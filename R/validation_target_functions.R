# validation_target_functions.R
#
# Helper functions for validation targets in _targets.R
# Extracts inline validation logic to reduce pipeline file size

#' Validate data with memory-efficient sampling
#' 
#' For large datasets, samples a subset for validation to avoid memory issues
#' 
#' @param data The data to validate (will sample from nrow(data))
#' @param stage_name Name of the validation stage
#' @param expected_cols Expected column names
#' @param min_rows_dev Minimum rows expected in dev mode
#' @param min_rows_prod Minimum rows expected in production mode
#' @param pipeline_config Pipeline configuration object
#' @param max_sample_size Maximum sample size (default 100000)
#' @param stop_on_failure Whether to stop pipeline on validation failure
#' @return Validation result object
#' @export
validate_with_sampling <- function(data, 
                                  stage_name, 
                                  expected_cols,
                                  min_rows_dev,
                                  min_rows_prod,
                                  pipeline_config,
                                  max_sample_size = 100000,
                                  stop_on_failure = TRUE) {
  
  # Get total row count without loading full dataset
  n_rows <- nrow(data)
  
  # Sample a subset for validation
  sample_size <- min(max_sample_size, n_rows)
  sample_indices <- sample.int(n_rows, sample_size)
  
  # Load only the sample
  data_sample <- data[sample_indices, ]
  
  # Validate the sample
  result <- validate_import_stage(
    data = data_sample,
    stage_name = stage_name,
    expected_cols = expected_cols,
    min_rows = ifelse(pipeline_config$dev_mode, min_rows_dev, min_rows_prod)
  )
  
  # Add actual row count to metadata
  result$metadata$total_rows <- n_rows
  result$metadata$sample_size <- sample_size
  
  if (!result$passed && stop_on_failure) {
    stop(paste(stage_name, "validation failed - pipeline halted"))
  }
  
  return(result)
}

#' Simple validation with optional warning
#' 
#' @param data Data to validate
#' @param stage_name Name of the validation stage
#' @param expected_cols Expected column names
#' @param min_rows_dev Minimum rows expected in dev mode
#' @param min_rows_prod Minimum rows expected in production mode
#' @param pipeline_config Pipeline configuration object
#' @param warning_message Custom warning message on failure
#' @return Validation result object
#' @export
validate_simple <- function(data,
                           stage_name,
                           expected_cols,
                           min_rows_dev,
                           min_rows_prod,
                           pipeline_config,
                           warning_message = NULL) {
  
  result <- validate_import_stage(
    data = data,
    stage_name = stage_name,
    expected_cols = expected_cols,
    min_rows = ifelse(pipeline_config$dev_mode, min_rows_dev, min_rows_prod)
  )
  
  if (!result$passed && !is.null(warning_message)) {
    warning(warning_message)
  }
  
  return(result)
}

#' Validate cleaning stage
#' 
#' @param cleaned_data Cleaned data
#' @param original_data Original data for comparison
#' @param stage_name Name of the validation stage
#' @param key_cols Key columns to validate
#' @param warning_message Custom warning message on failure
#' @return Validation result object
#' @export
validate_cleaning <- function(cleaned_data,
                             original_data,
                             stage_name,
                             key_cols,
                             warning_message = NULL) {
  
  result <- validate_cleaning_stage(
    cleaned_data = cleaned_data,
    original_data = original_data,
    stage_name = stage_name,
    key_cols = key_cols
  )
  
  if (!result$passed && !is.null(warning_message)) {
    warning(warning_message)
  }
  
  return(result)
}

#' Validate string match with statistics reporting
#' 
#' @param match_data String match results
#' @param stage_name Name of the validation stage
#' @param id_col Column name for IDs
#' @param score_col Column name for match scores
#' @param lat_col Column name for latitude (for match rate calculation)
#' @param custom_message Optional custom message format string with %s placeholders
#' @param warning_message Warning message on validation failure
#' @return Validation result object
#' @export
validate_string_match_with_stats <- function(match_data,
                                            stage_name,
                                            id_col,
                                            score_col,
                                            lat_col = "lat",
                                            custom_message = NULL,
                                            warning_message = NULL) {
  
  result <- validate_string_match_stage(
    match_data = match_data,
    stage_name = stage_name,
    id_col = id_col,
    score_col = score_col
  )
  
  # Report custom statistics if provided
  if (!is.null(custom_message)) {
    message(sprintf(
      custom_message,
      result$metadata$match_rate,
      sum(!is.na(match_data[[lat_col]]), na.rm = TRUE),
      nrow(match_data)
    ))
  }
  
  if (!result$passed && !is.null(warning_message)) {
    warning(warning_message)
  }
  
  return(result)
}

#' Validate geocodebr match with precision breakdown
#' 
#' @param match_data Geocodebr match results
#' @param stage_name Name of the validation stage
#' @param id_col Column name for IDs
#' @param score_col Column name for match scores
#' @param precision_col Column name for precision information
#' @return Validation result object
#' @export
validate_geocodebr_precision <- function(match_data,
                                        stage_name = "geocodebr_match",
                                        id_col = "local_id",
                                        score_col = "mindist_geocodebr",
                                        precision_col = "precisao_geocodebr") {
  
  result <- validate_string_match_stage(
    match_data = match_data,
    stage_name = stage_name,
    id_col = id_col,
    score_col = score_col
  )
  
  # Report precision breakdown
  if (nrow(match_data) > 0) {
    precision_summary <- match_data[, .N, by = get(precision_col)]
    names(precision_summary)[1] <- precision_col
    message("geocodebr precision breakdown:")
    print(precision_summary)
    
    # Calculate street-level precision percentage
    total_matches <- nrow(match_data)
    street_level <- precision_summary[get(precision_col) == "logradouro", N]
    if (length(street_level) > 0) {
      pct_street <- 100 * street_level / total_matches
      message(sprintf("Street-level precision: %.1f%%", pct_street))
    }
  }
  
  return(result)
}

#' Validate merge operation
#' 
#' @param merged_data Merged dataset
#' @param left_data Left dataset in merge
#' @param stage_name Name of the validation stage
#' @param merge_keys Keys used for merging
#' @param join_type Type of join performed
#' @param warning_message Warning message on failure
#' @return Validation result object
#' @export
validate_merge_simple <- function(merged_data,
                                 left_data,
                                 stage_name,
                                 merge_keys,
                                 join_type = "left_many",
                                 warning_message = NULL) {
  
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

#' Validate predictions
#' 
#' @param predictions Model predictions
#' @param stage_name Name of the validation stage
#' @param pred_col Prediction column name
#' @param stop_on_failure Whether to stop on validation failure
#' @return Validation result object
#' @export
validate_predictions_simple <- function(predictions,
                                       stage_name = "model_predictions",
                                       pred_col = "pred_dist",
                                       stop_on_failure = TRUE) {
  
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

#' Validate final geocoded output
#' 
#' @param output_data Final geocoded data
#' @param stage_name Name of the validation stage
#' @param required_cols Required columns in output
#' @param unique_keys Columns that should form unique combinations
#' @param stop_on_failure Whether to stop on validation failure
#' @return Validation result object
#' @export
validate_final_output <- function(output_data,
                                 stage_name = "geocoded_locais",
                                 required_cols,
                                 unique_keys,
                                 stop_on_failure = TRUE) {
  
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

#' Validate all input datasets
#' 
#' Consolidated validation for all input datasets, focusing on size checks
#' 
#' @param muni_ids Municipality identifiers data
#' @param inep_codes INEP codes data  
#' @param locais_filtered Filtered polling stations data
#' @param pipeline_config Pipeline configuration object
#' @return Validation result object with summary of all input checks
#' @export
validate_inputs_consolidated <- function(muni_ids, inep_codes, locais_filtered, pipeline_config) {
  
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