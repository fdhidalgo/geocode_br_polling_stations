# Note: 5 unused functions were moved to backup/unused_functions/
# Date: 2025-06-20
# Functions removed: validate_cleaning, validate_geocodebr_precision, validate_simple, validate_string_match_with_stats, validate_with_sampling

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