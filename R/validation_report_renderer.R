# Validation Report Renderer Functions
#
# Functions to generate text-based validation reports as part of the pipeline

library(data.table)

#' Generate Text Validation Report
#' 
#' @description Creates a simple text summary of validation results
#' @param validation_results List of validation result objects from pipeline stages
#' @param output_dir Directory for the report (will be created if needed)
#' @param export_failures Whether to export failed records to CSV files
#' @return Path to the generated text report
#' @export
generate_text_validation_report <- function(validation_results,
                                          output_dir = "output/validation_reports",
                                          export_failures = TRUE) {
  
  # Check inputs
  if (length(validation_results) == 0) {
    stop("No validation results provided")
  }
  
  # Create output directory if needed
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  # Generate output filename
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  output_file <- file.path(output_dir, paste0("validation_summary_", timestamp, ".txt"))
  
  # Source the reporting functions
  source("R/validation_reporting.R")
  
  # Export failed records if requested
  if (export_failures) {
    message("Exporting failed records to CSV...")
    failure_dir <- file.path(output_dir, "failed_records")
    
    # Export each stage's failures
    for (stage_name in names(validation_results)) {
      result <- validation_results[[stage_name]]
      if (!result$passed && !is.null(result$metadata$data)) {
        # Extract and export failed records
        failed <- extract_failed_records(result, result$metadata$data)
        if (nrow(failed) > 0) {
          export_failed_records(failed, failure_dir, stage_name)
        }
      }
    }
  }
  
  # Create text report
  report_lines <- c(
    "VALIDATION REPORT SUMMARY",
    "========================",
    "",
    paste("Generated:", Sys.time()),
    ""
  )
  
  # Add summary for each stage
  for (stage_name in names(validation_results)) {
    result <- validation_results[[stage_name]]
    
    # Add stage header
    report_lines <- c(report_lines,
      paste("Stage:", stage_name),
      paste("  Type:", result$metadata$type),
      paste("  Passed:", result$passed),
      paste("  Rows:", result$metadata$n_rows),
      ""
    )
  }
  
  # Write report
  writeLines(report_lines, output_file)
  
  message("Validation report generated: ", output_file)
  
  # Add failure export location if applicable
  if (export_failures) {
    report_lines <- c(report_lines,
      "",
      paste("Failed records exported to:", file.path(output_dir, "failed_records/")),
      ""
    )
    writeLines(report_lines, output_file)
  }
  
  # Return the path to the generated report
  invisible(output_file)
}

#' Quick Validation Report Generation
#' 
#' @description Convenience function to generate report from saved validation results
#' @param validation_files Character vector of paths to saved validation .rds files
#' @param ... Additional arguments passed to generate_text_validation_report
#' @return Path to generated report
#' @export
quick_validation_report <- function(validation_files, ...) {
  # Load validation results
  validation_results <- list()
  
  for (file in validation_files) {
    if (file.exists(file)) {
      result <- readRDS(file)
      if (inherits(result, "validation_result")) {
        stage_name <- result$metadata$stage
        validation_results[[stage_name]] <- result
      }
    }
  }
  
  if (length(validation_results) == 0) {
    stop("No valid validation results found in provided files")
  }
  
  # Generate report
  generate_text_validation_report(validation_results, ...)
}