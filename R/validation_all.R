# validation_all.R
#
# Consolidated validation functions for the geocoding pipeline.
# This file sources all validation-related files and exports their functions
# to reduce the number of source() calls in _targets.R

# Load validation modules
source("R/functions_validate.R")
source("R/validation_reporting.R") 
source("R/validation_report_renderer.R")
source("R/validation_pipeline_stages.R")
source("R/validate_brasilia_filtering.R")

# All validation functions are now available through the sourced files above
# No need to re-export since tar_source() loads everything globally

# Export validation configuration
validation_config <- list(
  # Default thresholds for validation
  min_match_rate = 0.7,
  max_na_rate = 0.3,
  min_coordinate_precision = 4,
  
  # Validation stages
  stages = c(
    "data_import",
    "string_matching", 
    "geocoding",
    "coordinate_validation",
    "panel_creation"
  ),
  
  # Report settings
  report = list(
    max_failed_records = 1000,
    include_plots = TRUE,
    output_format = "html"
  )
)

# Helper function to run all validation stages
#' Run comprehensive validation pipeline
#' 
#' @param data List of datasets to validate
#' @param config Validation configuration (defaults to validation_config)
#' @return List of validation results
#' @export
run_full_validation <- function(data, config = validation_config) {
  results <- list()
  
  # Run each validation stage
  for (stage in config$stages) {
    stage_function <- switch(stage,
      "data_import" = validate_data_import,
      "string_matching" = validate_string_match_stage,
      "geocoding" = validate_geocoding_stage,
      "coordinate_validation" = validate_coordinates,
      "panel_creation" = validate_panel_creation,
      stop("Unknown validation stage: ", stage)
    )
    
    # Run validation if data exists for this stage
    stage_data <- data[[stage]]
    if (!is.null(stage_data)) {
      results[[stage]] <- stage_function(stage_data)
    }
  }
  
  return(results)
}

# Define null coalescing operator if not already defined
if (!exists("%||%")) {
  `%||%` <- function(x, y) if (is.null(x)) y else x
}