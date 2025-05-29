# Validation Functions using the validate package
# 
# This file contains functions to validate data quality at each stage of the
# geocoding pipeline using the validate package infrastructure.

library(validate)

#' Define validation rules for municipal identifiers
#' 
#' @return A validator object with rules for muni_ids
create_muni_ids_rules <- function() {
  validator(
    # Structure validation - check each column separately to avoid syntax issues
    has_id_munic_7 = "id_munic_7" %in% names(.),
    has_id_munic_6 = "id_munic_6" %in% names(.),
    has_id_estado = "id_estado" %in% names(.),
    has_estado_abrev = "estado_abrev" %in% names(.),
    has_municipio = "municipio" %in% names(.),
    
    # ID format validation
    no_missing_muni7 = !is.na(id_munic_7),
    no_missing_muni6 = !is.na(id_munic_6),
    
    # State code validation (Brazilian state codes range from 11-53)
    valid_state_codes = in_range(id_estado, min = 11, max = 53),
    
    # Uniqueness validation
    unique_muni7_ids = is_unique(id_munic_7),
    unique_muni6_ids = is_unique(id_munic_6),
    
    # Text field validation
    no_empty_names = !is.na(municipio) & nchar(as.character(municipio)) > 0,
    valid_state_abbrev = nchar(as.character(estado_abrev)) == 2
  )
}

#' Validate municipal identifiers
#' 
#' @param data Municipal identifier data
#' @return A validation report (confrontation object)
validate_muni_ids_data <- function(data) {
  rules <- create_muni_ids_rules()
  result <- confront(data, rules)
  
  # Create metadata separately
  metadata <- list(
    target = "muni_ids",
    timestamp = Sys.time()
  )
  
  # Check if all rules passed
  summary_df <- summary(result)
  passed <- all(summary_df$fails == 0, na.rm = TRUE)
  
  # Return a list with result and metadata
  validation_output <- list(
    result = result,
    metadata = metadata,
    passed = passed
  )
  
  class(validation_output) <- "validation_result"
  
  validation_output
}

#' Print method for validation results
#' @param x A validation result object
#' @param ... Additional arguments
#' @export
print.validation_result <- function(x, ...) {
  cat("Validation Result for:", x$metadata$target, "\n")
  cat("Timestamp:", format(x$metadata$timestamp), "\n")
  cat("Overall Status:", ifelse(x$passed, "PASSED", "FAILED"), "\n\n")
  
  # Print the summary
  summary_df <- summary(x$result)
  failed_rules <- summary_df[summary_df$fails > 0 | is.na(summary_df$fails), ]
  
  if (nrow(failed_rules) > 0) {
    cat("Failed Rules:\n")
    print(failed_rules[, c("name", "items", "passes", "fails", "nNA")])
  } else {
    cat("All validation rules passed!\n")
  }
  
  cat("\nSummary Statistics:\n")
  cat("Total rules:", nrow(summary_df), "\n")
  cat("Passed rules:", sum(summary_df$fails == 0, na.rm = TRUE), "\n")
  cat("Failed rules:", sum(summary_df$fails > 0, na.rm = TRUE), "\n")
  
  invisible(x)
}

#' Define validation rules for INEP codes
#' 
#' @return A validator object with rules for INEP codes
create_inep_codes_rules <- function() {
  validator(
    # Structure validation - check each column separately
    has_codigo_inep = "codigo_inep" %in% names(.),
    has_id_munic_7 = "id_munic_7" %in% names(.),
    
    # INEP code format validation (should be 8 digits)
    no_missing_inep = !is.na(codigo_inep),
    no_missing_muni = !is.na(id_munic_7),
    
    # INEP codes should be 8 digits when converted to character
    valid_inep_length = nchar(as.character(codigo_inep)) == 8,
    
    # Municipal codes should be 7 digits
    valid_muni_codes = nchar(as.character(id_munic_7)) == 7,
    
    # Check for duplicates - each INEP code should be unique
    unique_inep_codes = is_unique(codigo_inep)
  )
}

#' Validate INEP codes data
#' 
#' @param data INEP codes data
#' @return A validation report (confrontation object)
validate_inep_codes_data <- function(data) {
  rules <- create_inep_codes_rules()
  result <- confront(data, rules)
  
  # Create metadata separately
  metadata <- list(
    target = "inep_codes",
    timestamp = Sys.time()
  )
  
  # Check if all rules passed
  summary_df <- summary(result)
  passed <- all(summary_df$fails == 0, na.rm = TRUE)
  
  # Return a list with result and metadata
  validation_output <- list(
    result = result,
    metadata = metadata,
    passed = passed
  )
  
  class(validation_output) <- "validation_result"
  
  validation_output
}