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
      passed = all(result)
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
      passed = all(result)
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
      passed = all(result)
    ),
    class = "validation_result"
  )
}

validate_merge_stage <- function(merged_data, left_data, right_data = NULL,
                                stage_name, merge_keys, join_type = "left") {
  # Validates data after merge operations
  
  # Create merge-specific rules
  base_rules <- list(
    # Should have results
    has_rows = quote(nrow(.) > 0),
    no_complete_duplicates = quote(nrow(.) == nrow(unique(.)))
  )
  
  # Add merge key checks
  if (!is.null(merge_keys)) {
    for (key in merge_keys) {
      base_rules[[paste0("has_key_", key)]] <- parse(text = sprintf(
        "'%s' %%in%% names(.)", key
      ))[[1]]
    }
  }
  
  # Create validator
  rules <- do.call(validator, base_rules)
  
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
      passed = all(result)
    ),
    class = "validation_result"
  )
}

validate_prediction_stage <- function(predictions, stage_name,
                                     pred_col = "prediction", prob_col = NULL) {
  # Validates model prediction results
  
  # Create prediction-specific rules
  base_rules <- list(
    # Should have predictions
    has_rows = quote(nrow(.) > 0),
    # Check for prediction column
    has_pred_col = parse(text = sprintf("'%s' %%in%% names(.)", pred_col))[[1]]
  )
  
  # Add prediction column validation
  if (pred_col %in% names(predictions)) {
    base_rules$predictions_numeric <- parse(text = sprintf(
      "is.numeric(.[[%s]])", deparse(pred_col)
    ))[[1]]
    base_rules$predictions_valid <- parse(text = sprintf(
      "all(!is.na(.[[%s]]))", deparse(pred_col)
    ))[[1]]
  }
  
  # Add probability column checks if specified
  if (!is.null(prob_col)) {
    base_rules$has_prob_col <- parse(text = sprintf(
      "'%s' %%in%% names(.)", prob_col
    ))[[1]]
    if (prob_col %in% names(predictions)) {
      base_rules$probs_valid <- parse(text = sprintf(
        "all(.[[%s]] >= 0 & .[[%s]] <= 1, na.rm = TRUE)", 
        deparse(prob_col), deparse(prob_col)
      ))[[1]]
    }
  }
  
  # For backward compatibility, also check for coordinate predictions
  if (any(c("pred_long", "pred_lat", "long", "lat") %in% names(predictions))) {
    base_rules$has_coordinate_predictions <- quote(
      any(c("pred_long", "pred_lat", "long", "lat") %in% names(.))
    )
  }
  
  # Create validator
  rules <- do.call(validator, base_rules)
  
  # Run validation
  result <- confront(predictions, rules)
  
  # Calculate prediction statistics based on pred_col
  if (pred_col %in% names(predictions)) {
    n_predictions <- sum(!is.na(predictions[[pred_col]]))
    prediction_rate <- n_predictions / nrow(predictions)
  } else if ("long" %in% names(predictions) && "lat" %in% names(predictions)) {
    # Fallback for coordinate predictions
    n_predictions <- sum(!is.na(predictions$long) | !is.na(predictions$lat))
    prediction_rate <- n_predictions / nrow(predictions)
  } else {
    n_predictions <- 0
    prediction_rate <- 0
  }
  
  # Create metadata
  metadata <- list(
    stage = stage_name,
    type = "prediction",
    timestamp = Sys.time(),
    n_rows = nrow(predictions),
    n_predictions = n_predictions,
    prediction_rate = prediction_rate,
    pred_col = pred_col,
    prob_col = prob_col
  )
  
  # Return structured result
  structure(
    list(
      result = result,
      metadata = metadata,
      stage = stage_name,
      passed = all(result)
    ),
    class = "validation_result"
  )
}

validate_output_stage <- function(output_data, stage_name, required_cols = NULL, unique_keys = NULL) {
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
  
  # Check uniqueness if keys specified
  if (!is.null(unique_keys) && all(unique_keys %in% names(output_data))) {
    n_unique <- nrow(unique(output_data[, ..unique_keys]))
    metadata$n_unique_keys <- n_unique
    metadata$duplicate_keys <- nrow(output_data) - n_unique
    
    if (metadata$duplicate_keys > 0) {
      warning(paste("Found", metadata$duplicate_keys, "duplicate key combinations"))
    }
  }
  
  # Check if all rules passed
  summary_df <- summary(result)
  passed <- all(summary_df$fails == 0, na.rm = TRUE)
  
  # Additional check for duplicates
  if (!is.null(unique_keys) && metadata$duplicate_keys > 0) {
    passed <- FALSE
  }
  
  # Return structured result
  structure(
    list(
      result = result,
      metadata = metadata,
      stage = stage_name,
      passed = passed
    ),
    class = "validation_result"
  )
}

# ===== VALIDATION TARGET FUNCTIONS =====

validate_merge_simple <- function(merged_data,
                                 left_data,
                                 stage_name,
                                 merge_keys,
                                 join_type = "left_many",
                                 warning_message = NULL) {
  # Validate merge operation
  
  result <- validate_merge_stage(
    merged_data = merged_data,
    left_data = left_data,
    right_data = NULL,  # Multiple sources merged
    stage_name = stage_name,
    merge_keys = merge_keys,
    join_type = join_type
  )
  
  if (!result$passed && !is.null(warning_message)) {
    warning(warning_message)
  }
  
  return(result)
}

validate_predictions_simple <- function(predictions,
                                       stage_name = "model_predictions",
                                       pred_col = "pred_dist",
                                       stop_on_failure = TRUE) {
  # Validate predictions
  
  result <- validate_prediction_stage(
    predictions = predictions,
    stage_name = stage_name,
    pred_col = pred_col,
    prob_col = NULL  # No probability column
  )
  
  if (!result$passed && stop_on_failure) {
    stop("Model predictions validation failed")
  }
  
  return(result)
}

validate_final_output <- function(output_data,
                                 stage_name = "geocoded_locais",
                                 required_cols,
                                 unique_keys,
                                 stop_on_failure = TRUE) {
  # Validate final geocoded output
  
  result <- validate_output_stage(
    output_data = output_data,
    stage_name = stage_name,
    required_cols = required_cols,
    unique_keys = unique_keys
  )
  
  # Final quality check
  if (!result$passed && stop_on_failure) {
    stop("Final output validation failed - do not export!")
  }
  
  message(sprintf(
    "Geocoding complete: %d polling stations geocoded",
    nrow(output_data)
  ))
  
  return(result)
}

validate_inputs_consolidated <- function(muni_ids, inep_codes, locais_filtered, pipeline_config) {
  # Validate all input datasets
  # Consolidated validation for all input datasets, focusing on size checks
  
  # Define expected sizes based on mode
  expected_sizes <- if (pipeline_config$dev_mode) {
    list(
      muni_ids = list(min = 30, max = 100, name = "municipalities"),
      inep_codes = list(min = 1000, max = 50000, name = "INEP schools"),
      locais = list(min = 1000, max = 20000, name = "polling stations")
    )
  } else {
    list(
      muni_ids = list(min = 5000, max = 6000, name = "municipalities"),
      inep_codes = list(min = 100000, max = 300000, name = "INEP schools"), 
      locais = list(min = 100000, max = 1000000, name = "polling stations")
    )
  }
  
  # Collect validation results
  checks <- list()
  messages <- list()
  all_passed <- TRUE
  
  # Check municipality data
  muni_count <- nrow(muni_ids)
  checks$muni_ids_size <- muni_count >= expected_sizes$muni_ids$min && 
                          muni_count <= expected_sizes$muni_ids$max
  messages$muni_ids <- sprintf("%s: %d (expected %d-%d)", 
                               expected_sizes$muni_ids$name,
                               muni_count,
                               expected_sizes$muni_ids$min,
                               expected_sizes$muni_ids$max)
  all_passed <- all_passed && checks$muni_ids_size
  
  # Check INEP codes
  inep_count <- nrow(inep_codes)
  checks$inep_codes_size <- inep_count >= expected_sizes$inep_codes$min &&
                            inep_count <= expected_sizes$inep_codes$max
  messages$inep_codes <- sprintf("%s: %d (expected %d-%d)",
                                 expected_sizes$inep_codes$name,
                                 inep_count,
                                 expected_sizes$inep_codes$min,
                                 expected_sizes$inep_codes$max)
  all_passed <- all_passed && checks$inep_codes_size
  
  # Check polling stations
  locais_count <- nrow(locais_filtered)
  checks$locais_size <- locais_count >= expected_sizes$locais$min &&
                        locais_count <= expected_sizes$locais$max
  messages$locais <- sprintf("%s: %d (expected %d-%d)",
                             expected_sizes$locais$name,
                             locais_count,
                             expected_sizes$locais$min,
                             expected_sizes$locais$max)
  all_passed <- all_passed && checks$locais_size
  
  # Create metadata
  metadata <- list(
    stage = "input_validation",
    type = "consolidated",
    timestamp = Sys.time(),
    mode = ifelse(pipeline_config$dev_mode, "DEVELOPMENT", "PRODUCTION"),
    counts = list(
      municipalities = muni_count,
      inep_schools = inep_count,
      polling_stations = locais_count
    ),
    messages = messages
  )
  
  # Print summary
  cat("\n=== INPUT DATA VALIDATION ===\n")
  cat("Mode:", metadata$mode, "\n")
  for (msg in messages) {
    cat("-", msg, ifelse(grepl("expected", msg) && !all_passed, "❌", "✓"), "\n")
  }
  cat("=============================\n\n")
  
  # Return validation result
  validation_output <- list(
    result = NULL, # No detailed rules, just size checks
    metadata = metadata,
    passed = all_passed,
    checks = checks
  )
  
  class(validation_output) <- "validation_result"
  
  if (!all_passed) {
    warning("Input data validation failed - check dataset sizes")
  }
  
  return(validation_output)
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

generate_validation_report_simplified <- function(
  validate_inputs, validate_model_data, validate_predictions,
  validate_geocoded_output, locais_filtered, model_data, 
  model_predictions, geocoded_locais, pipeline_config
) {
  # Simplified version focusing on critical validations only
  
  # Collect critical validation results only
  validation_results <- list(
    inputs = validate_inputs,
    model_data_merge = validate_model_data,
    model_predictions = validate_predictions,
    geocoded_output = validate_geocoded_output
  )
  
  # Create focused data source mapping
  data_sources <- list(
    inputs = NULL, # Input validation doesn't need data export
    model_data_merge = model_data,
    model_predictions = model_predictions,
    geocoded_output = geocoded_locais
  )
  
  # Generate comprehensive report
  summary_stats <- create_validation_report(
    validation_results,
    output_dir = "output/validation_reports"
  )
  
  # Save summary
  saveRDS(summary_stats, "output/validation_summary.rds")
  
  # Print focused summary to console
  cat("\n========== VALIDATION REPORT SUMMARY ==========\n")
  cat("Report generated:", summary_stats$rds, "\n")
  cat("Critical validations checked:", length(validation_results), "\n")
  cat("Passed:", sum(sapply(validation_results, function(x) x$passed)), "\n")
  cat("Failed:", sum(sapply(validation_results, function(x) !x$passed)), "\n")
  cat(
    "Mode:",
    ifelse(pipeline_config$dev_mode, "DEVELOPMENT", "PRODUCTION"),
    "\n"
  )
  
  # Print specific validation results
  cat("\nValidation Results:\n")
  cat("- Input data sizes:", ifelse(validation_results$inputs$passed, "✅", "❌"), "\n")
  cat("- Model data merge:", ifelse(validation_results$model_data_merge$passed, "✅", "❌"), "\n")
  cat("- Model predictions:", ifelse(validation_results$model_predictions$passed, "✅", "❌"), "\n")
  cat("- Final output:", ifelse(validation_results$geocoded_output$passed, "✅", "❌"), "\n")
  
  cat(
    "\nOverall status:",
    ifelse(sum(sapply(validation_results, function(x) !x$passed)) == 0, "✅ SUCCESS", "❌ FAILURES DETECTED"),
    "\n"
  )
  cat("===============================================\n\n")
  
  return(summary_stats)
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