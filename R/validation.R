## Validation Functions
##
## This file consolidates all validation functions from:
## - validation_stages.R (6 functions)
## - validation_target_functions.R (4 functions)
## - validation_report_helpers.R (6 functions)
## - data_quality_monitor_v2.R (1 function)
##
## Total functions: 17

library(validate)
library(data.table)
library(knitr)

# ===== VALIDATION STAGE FUNCTIONS =====

validate_import_stage <- function(data, stage_name, expected_cols = NULL, min_rows = 1) {
  # Validates data immediately after import to catch loading errors
  
  # Create import-specific rules
  base_rules <- list(
    # Basic structure checks  
    has_rows = substitute(nrow(.) >= min_rows_val, list(min_rows_val = min_rows)),
    not_empty = quote(nrow(.) > 0)
  )
  
  # Add column checks if expected columns provided
  if (!is.null(expected_cols)) {
    for (col in expected_cols) {
      rule_name <- paste0("has_column_", col)
      base_rules[[rule_name]] <- substitute(col %in% names(.), list(col = col))
    }
  }
  
  rules <- do.call(validator, base_rules)
  
  # Run validation
  result <- confront(data, rules)
  
  # Create metadata
  metadata <- list(
    stage = stage_name,
    type = "import",
    timestamp = Sys.time(),
    n_rows = nrow(data),
    n_cols = ncol(data),
    columns = names(data)
  )
  
  # Return structured result
  structure(
    list(
      result = result,
      metadata = metadata,
      stage = stage_name,
      passed = all(result$passes, na.rm = TRUE)
    ),
    class = "validation_result"
  )
}

validate_cleaning_stage <- function(data, stage_name, original_data = NULL) {
  # Validates data after cleaning transformations
  
  # Create cleaning-specific rules
  rules <- validator(
    # Basic integrity
    has_rows = nrow(.) > 0,
    
    # Data quality improvements expected after cleaning
    # These are soft checks - we expect improvement but don't require perfection
    no_duplicate_rows = nrow(.) == length(unique(.))
  )
  
  # Run validation
  result <- confront(data, rules)
  
  # Create metadata
  metadata <- list(
    stage = stage_name,
    type = "cleaning",
    timestamp = Sys.time(),
    n_rows = nrow(data),
    n_cols = ncol(data)
  )
  
  # If original data provided, add comparison metrics
  if (!is.null(original_data)) {
    metadata$rows_removed <- nrow(original_data) - nrow(data)
    metadata$removal_rate <- (nrow(original_data) - nrow(data)) / nrow(original_data)
  }
  
  # Return structured result
  structure(
    list(
      result = result,
      metadata = metadata,
      stage = stage_name,
      passed = all(result$passes, na.rm = TRUE)
    ),
    class = "validation_result"
  )
}

validate_string_match_stage <- function(match_result, stage_name, min_match_rate = 0.0) {
  # Validates string matching results
  
  # Ensure we're working with a data.table
  if (!is.data.table(match_result)) {
    match_result <- as.data.table(match_result)
  }
  
  # Create string-matching specific rules
  rules <- validator(
    has_rows = nrow(.) > 0,
    has_local_id = "local_id" %in% names(.),
    
    # Check that we have match columns
    has_match_columns = any(grepl("^match_", names(.))),
    has_distance_columns = any(grepl("^mindist_", names(.)))
  )
  
  # Run validation
  result <- confront(match_result, rules)
  
  # Calculate match statistics
  match_cols <- names(match_result)[grepl("^match_", names(match_result))]
  dist_cols <- names(match_result)[grepl("^mindist_", names(match_result))]
  
  match_stats <- list()
  for (col in match_cols) {
    if (col %in% names(match_result)) {
      match_stats[[col]] <- list(
        n_matched = sum(!is.na(match_result[[col]])),
        match_rate = mean(!is.na(match_result[[col]]))
      )
    }
  }
  
  # Create metadata
  metadata <- list(
    stage = stage_name,
    type = "string_match",
    timestamp = Sys.time(),
    n_rows = nrow(match_result),
    match_columns = match_cols,
    distance_columns = dist_cols,
    match_statistics = match_stats
  )
  
  # Return structured result
  structure(
    list(
      result = result,
      metadata = metadata,
      stage = stage_name,
      passed = all(result$passes, na.rm = TRUE)
    ),
    class = "validation_result"
  )
}

validate_merge_stage <- function(merged_data, stage_name, left_data = NULL, right_data = NULL) {
  # Validates data after merge operations
  
  # Create merge-specific rules
  rules <- validator(
    has_rows = nrow(.) > 0,
    no_complete_duplicates = nrow(.) == nrow(unique(.))
  )
  
  # Run validation
  result <- confront(merged_data, rules)
  
  # Create metadata
  metadata <- list(
    stage = stage_name,
    type = "merge",
    timestamp = Sys.time(),
    n_rows = nrow(merged_data),
    n_cols = ncol(merged_data)
  )
  
  # If input data provided, calculate merge statistics
  if (!is.null(left_data) && !is.null(right_data)) {
    metadata$left_rows <- nrow(left_data)
    metadata$right_rows <- nrow(right_data)
    metadata$output_rows <- nrow(merged_data)
    metadata$merge_type <- case_when(
      nrow(merged_data) == nrow(left_data) ~ "left join",
      nrow(merged_data) == nrow(right_data) ~ "right join",
      nrow(merged_data) == max(nrow(left_data), nrow(right_data)) ~ "full join",
      nrow(merged_data) < min(nrow(left_data), nrow(right_data)) ~ "inner join",
      TRUE ~ "unknown"
    )
  }
  
  # Return structured result
  structure(
    list(
      result = result,
      metadata = metadata,
      stage = stage_name,
      passed = all(result$passes, na.rm = TRUE)
    ),
    class = "validation_result"
  )
}

validate_prediction_stage <- function(predictions, stage_name, min_prediction_rate = 0.8) {
  # Validates model prediction results
  
  # Create prediction-specific rules
  rules <- validator(
    has_rows = nrow(.) > 0,
    has_predictions = any(c("pred_long", "pred_lat", "long", "lat") %in% names(.)),
    
    # Check prediction coverage
    sufficient_predictions = mean(!is.na(.$long) | !is.na(.$lat)) >= min_prediction_rate
  )
  
  # Run validation
  result <- confront(predictions, rules)
  
  # Calculate prediction statistics
  n_predictions <- sum(!is.na(predictions$long) | !is.na(predictions$lat))
  prediction_rate <- n_predictions / nrow(predictions)
  
  # Create metadata
  metadata <- list(
    stage = stage_name,
    type = "prediction",
    timestamp = Sys.time(),
    n_rows = nrow(predictions),
    n_predictions = n_predictions,
    prediction_rate = prediction_rate,
    min_required_rate = min_prediction_rate
  )
  
  # Return structured result
  structure(
    list(
      result = result,
      metadata = metadata,
      stage = stage_name,
      passed = all(result$passes, na.rm = TRUE)
    ),
    class = "validation_result"
  )
}

validate_output_stage <- function(output_data, stage_name, required_cols = NULL) {
  # Validates final output data before export
  
  # Default required columns for geocoded output
  if (is.null(required_cols)) {
    required_cols <- c("local_id", "ano", "nr_zona", "nr_locvot")
  }
  
  # Create output-specific rules
  base_rules <- list(
    has_rows = quote(nrow(.) > 0),
    has_coordinates = quote(any(c("final_long", "final_lat", "pred_long", "pred_lat") %in% names(.)))
  )
  
  # Add required column checks
  for (col in required_cols) {
    rule_name <- paste0("has_", col)
    base_rules[[rule_name]] <- substitute(col %in% names(.), list(col = col))
  }
  
  rules <- do.call(validator, base_rules)
  
  # Run validation
  result <- confront(output_data, rules)
  
  # Calculate output statistics
  coord_cols <- intersect(c("final_long", "final_lat", "pred_long", "pred_lat"), names(output_data))
  n_geocoded <- 0
  
  if ("final_long" %in% names(output_data) && "final_lat" %in% names(output_data)) {
    n_geocoded <- sum(!is.na(output_data$final_long) & !is.na(output_data$final_lat))
  } else if ("pred_long" %in% names(output_data) && "pred_lat" %in% names(output_data)) {
    n_geocoded <- sum(!is.na(output_data$pred_long) & !is.na(output_data$pred_lat))
  }
  
  geocoding_rate <- n_geocoded / nrow(output_data)
  
  # Create metadata
  metadata <- list(
    stage = stage_name,
    type = "output",
    timestamp = Sys.time(),
    n_rows = nrow(output_data),
    n_cols = ncol(output_data),
    n_geocoded = n_geocoded,
    geocoding_rate = geocoding_rate,
    coordinate_columns = coord_cols
  )
  
  # Return structured result
  structure(
    list(
      result = result,
      metadata = metadata,
      stage = stage_name,
      passed = all(result$passes, na.rm = TRUE)
    ),
    class = "validation_result"
  )
}

# ===== VALIDATION TARGET FUNCTIONS =====

validate_merge_simple <- function(merged_data, left_data, right_data, merge_key) {
  # Simple validation for merge operations used in targets
  
  # Basic checks
  n_left <- nrow(left_data)
  n_right <- nrow(right_data)
  n_merged <- nrow(merged_data)
  
  # Check for unexpected row count changes
  if (n_merged > n_left * n_right) {
    warning("Merge resulted in more rows than expected - possible many-to-many join")
  }
  
  # Check for key preservation
  if (merge_key %in% names(merged_data)) {
    n_unique_keys <- length(unique(merged_data[[merge_key]]))
    if (n_unique_keys < n_left) {
      message(sprintf("Merge key coverage: %d/%d unique keys preserved", 
                      n_unique_keys, n_left))
    }
  }
  
  # Return validation summary
  list(
    passed = n_merged > 0,
    n_rows = n_merged,
    merge_efficiency = n_merged / n_left,
    timestamp = Sys.time()
  )
}

validate_predictions_simple <- function(predictions) {
  # Simple validation for prediction results
  
  n_total <- nrow(predictions)
  n_predicted <- sum(!is.na(predictions$long) & !is.na(predictions$lat))
  prediction_rate <- n_predicted / n_total
  
  list(
    passed = prediction_rate > 0.5,
    n_predictions = n_predicted,
    prediction_rate = prediction_rate,
    timestamp = Sys.time()
  )
}

validate_final_output <- function(geocoded_data) {
  # Validate final geocoded output
  
  # Check required columns
  required_cols <- c("local_id", "ano", "final_long", "final_lat")
  has_required <- all(required_cols %in% names(geocoded_data))
  
  # Calculate statistics
  n_total <- nrow(geocoded_data)
  n_geocoded <- sum(!is.na(geocoded_data$final_long) & !is.na(geocoded_data$final_lat))
  geocoding_rate <- n_geocoded / n_total
  
  # Check for duplicates
  n_duplicates <- sum(duplicated(geocoded_data[, .(local_id, ano)]))
  
  list(
    passed = has_required && geocoding_rate > 0.7 && n_duplicates == 0,
    has_required_columns = has_required,
    n_rows = n_total,
    n_geocoded = n_geocoded,
    geocoding_rate = geocoding_rate,
    n_duplicates = n_duplicates,
    timestamp = Sys.time()
  )
}

validate_inputs_consolidated <- function(muni_ids, locais, inep_data) {
  # Consolidated validation for all input data
  
  results <- list()
  
  # Validate muni_ids
  results$muni_ids <- list(
    has_data = nrow(muni_ids) > 0,
    has_required_cols = all(c("id_munic_7", "id_TSE", "estado_abrev") %in% names(muni_ids)),
    n_municipalities = length(unique(muni_ids$id_munic_7)),
    n_states = length(unique(muni_ids$estado_abrev))
  )
  
  # Validate locais
  results$locais <- list(
    has_data = nrow(locais) > 0,
    has_required_cols = all(c("local_id", "ano", "nr_zona", "nr_locvot") %in% names(locais)),
    n_locations = length(unique(locais$local_id)),
    years = sort(unique(locais$ano))
  )
  
  # Validate INEP data
  results$inep <- list(
    has_data = nrow(inep_data) > 0,
    has_coordinates = all(c("latitude", "longitude") %in% names(inep_data)),
    n_schools = nrow(inep_data),
    n_with_coords = sum(!is.na(inep_data$latitude) & !is.na(inep_data$longitude))
  )
  
  # Overall pass/fail
  results$passed <- all(
    results$muni_ids$has_data,
    results$muni_ids$has_required_cols,
    results$locais$has_data,
    results$locais$has_required_cols,
    results$inep$has_data,
    results$inep$has_coordinates
  )
  
  results$timestamp <- Sys.time()
  
  return(results)
}

# ===== VALIDATION REPORT HELPERS =====

ensure_quarto_path <- function() {
  # Ensure QUARTO_PATH is set for crew workers
  
  if (Sys.getenv("QUARTO_PATH") == "") {
    quarto_bin <- Sys.which("quarto")
    if (nzchar(quarto_bin)) {
      Sys.setenv(QUARTO_PATH = as.character(quarto_bin))
      message("Setting QUARTO_PATH to: ", quarto_bin)
      return(TRUE)
    } else {
      warning("Quarto not found in PATH")
      return(FALSE)
    }
  }
  return(TRUE)
}

render_sanity_check_report <- function(geocoded_data, output_file = NULL, 
                                     output_format = "html_document") {
  # Render sanity check report for geocoded data
  
  # Ensure quarto is available
  ensure_quarto_path()
  
  if (is.null(output_file)) {
    output_file <- paste0("sanity_check_", Sys.Date(), ".html")
  }
  
  # Prepare data for report
  report_data <- geocoded_data[, .(
    ano,
    estado = sg_uf,
    municipio = nm_localidade,
    n_locations = .N,
    n_geocoded = sum(!is.na(final_long) & !is.na(final_lat)),
    geocoding_rate = mean(!is.na(final_long) & !is.na(final_lat))
  ), by = .(ano, sg_uf, nm_localidade)]
  
  # Create simple markdown report
  report_content <- c(
    "# Polling Station Geocoding Sanity Check",
    paste("Generated:", Sys.Date()),
    "",
    "## Summary Statistics",
    "",
    knitr::kable(summary(report_data)),
    "",
    "## Geocoding Rates by State",
    "",
    knitr::kable(report_data[, .(
      n_municipalities = .N,
      total_locations = sum(n_locations),
      total_geocoded = sum(n_geocoded),
      avg_geocoding_rate = mean(geocoding_rate)
    ), by = estado])
  )
  
  # Write and render
  temp_rmd <- tempfile(fileext = ".Rmd")
  writeLines(report_content, temp_rmd)
  
  rmarkdown::render(
    input = temp_rmd,
    output_file = output_file,
    output_format = output_format,
    quiet = TRUE
  )
  
  unlink(temp_rmd)
  
  return(output_file)
}

get_report_output_files <- function(base_name = "validation_report") {
  # Generate output file names for validation reports
  
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  
  list(
    rds = paste0(base_name, "_", timestamp, ".rds"),
    summary = paste0(base_name, "_", timestamp, "_summary.txt"),
    details = paste0(base_name, "_", timestamp, "_details.csv")
  )
}

create_validation_report_target <- function(validation_results, output_dir = "output/validation_reports") {
  # Create validation report as a targets object
  
  # Ensure output directory exists
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
  
  # Get output file names
  files <- get_report_output_files()
  
  # Save full results
  saveRDS(validation_results, file.path(output_dir, files$rds))
  
  # Create summary
  summary_text <- capture.output({
    cat("Validation Report Summary\n")
    cat("========================\n")
    cat("Generated:", format(Sys.time()), "\n\n")
    
    for (stage in names(validation_results)) {
      result <- validation_results[[stage]]
      cat(sprintf("Stage: %s - %s\n", stage, ifelse(result$passed, "PASSED", "FAILED")))
    }
  })
  
  writeLines(summary_text, file.path(output_dir, files$summary))
  
  # Return file paths
  list(
    rds = file.path(output_dir, files$rds),
    summary = file.path(output_dir, files$summary)
  )
}

create_validation_report <- function(validation_results, output_dir = "output/validation_reports") {
  # Create comprehensive validation report
  
  report <- create_validation_report_target(validation_results, output_dir)
  
  # Add detailed analysis if needed
  if (length(validation_results) > 0) {
    # Extract key metrics
    metrics <- lapply(validation_results, function(x) {
      if (!is.null(x$metadata)) {
        x$metadata
      } else {
        list(stage = "unknown", passed = x$passed)
      }
    })
    
    # Save as CSV for easy analysis
    metrics_df <- do.call(rbind, lapply(names(metrics), function(name) {
      data.frame(
        stage = name,
        type = metrics[[name]]$type %||% NA,
        passed = validation_results[[name]]$passed,
        n_rows = metrics[[name]]$n_rows %||% NA,
        stringsAsFactors = FALSE
      )
    }))
    
    csv_file <- file.path(output_dir, gsub("_summary.txt", "_metrics.csv", basename(report$summary)))
    write.csv(metrics_df, csv_file, row.names = FALSE)
    
    report$metrics <- csv_file
  }
  
  return(report)
}

generate_validation_report_simplified <- function(validation_results, output_path = NULL) {
  # Simplified report generation for quick checks
  
  if (is.null(output_path)) {
    output_path <- paste0("validation_summary_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".txt")
  }
  
  # Create simple text report
  report_lines <- c(
    "VALIDATION SUMMARY",
    "==================",
    paste("Generated:", Sys.time()),
    "",
    "Results by Stage:"
  )
  
  # Add results for each stage
  for (stage in names(validation_results)) {
    result <- validation_results[[stage]]
    status <- ifelse(result$passed, "PASS", "FAIL")
    
    stage_info <- sprintf("  %s: %s", stage, status)
    
    if (!is.null(result$metadata$n_rows)) {
      stage_info <- paste(stage_info, sprintf("(n=%d)", result$metadata$n_rows))
    }
    
    report_lines <- c(report_lines, stage_info)
  }
  
  # Write report
  writeLines(report_lines, output_path)
  
  return(output_path)
}

# ===== DATA QUALITY MONITOR =====

create_data_quality_monitor <- function(initial_config = list()) {
  # Create a data quality monitoring object
  
  # Default configuration
  default_config <- list(
    track_memory = TRUE,
    track_time = TRUE,
    log_warnings = TRUE,
    save_intermediate = FALSE,
    output_dir = "output/data_quality"
  )
  
  # Merge with user config
  config <- modifyList(default_config, initial_config)
  
  # Initialize monitor
  monitor <- list(
    config = config,
    stages = list(),
    start_time = Sys.time(),
    warnings = list()
  )
  
  # Add methods
  monitor$log_stage <- function(stage_name, data, notes = NULL) {
    stage_info <- list(
      name = stage_name,
      timestamp = Sys.time(),
      n_rows = nrow(data),
      n_cols = ncol(data),
      memory_used = if (config$track_memory) pryr::object_size(data) else NA,
      notes = notes
    )
    
    monitor$stages[[stage_name]] <- stage_info
  }
  
  monitor$log_warning <- function(stage_name, warning_msg) {
    if (config$log_warnings) {
      monitor$warnings[[length(monitor$warnings) + 1]] <- list(
        stage = stage_name,
        message = warning_msg,
        timestamp = Sys.time()
      )
    }
  }
  
  monitor$get_summary <- function() {
    list(
      total_stages = length(monitor$stages),
      total_warnings = length(monitor$warnings),
      runtime = difftime(Sys.time(), monitor$start_time, units = "mins"),
      stages = monitor$stages,
      warnings = monitor$warnings
    )
  }
  
  monitor$save_report <- function(filename = NULL) {
    if (is.null(filename)) {
      filename <- file.path(
        config$output_dir,
        paste0("quality_monitoring_", format(Sys.time(), "%Y-%m-%d"), ".rds")
      )
    }
    
    dir.create(dirname(filename), showWarnings = FALSE, recursive = TRUE)
    saveRDS(monitor$get_summary(), filename)
    
    return(filename)
  }
  
  class(monitor) <- c("data_quality_monitor", "list")
  
  return(monitor)
}