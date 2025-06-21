# validation_report_helpers.R
#
# Helper functions for generating validation reports.
# This extracts inline report generation logic from _targets.R to make it more maintainable.

#' Ensure Quarto Path is Set
#' 
#' Sets the QUARTO_PATH environment variable if not already set.
#' This is needed for crew workers to find the quarto binary.
#' 
#' @return Logical indicating if quarto was found and path was set
#' @export
# Note: 3 unused functions were moved to backup/unused_functions/
# Date: 2025-06-20
# Functions removed: configure_report_targets, generate_validation_report_complete, render_methodology_report

ensure_quarto_path <- function() {
  if (Sys.getenv("QUARTO_PATH") == "") {
    quarto_bin <- Sys.which("quarto")
    if (!nzchar(quarto_bin)) {
      # Try common installation paths
      possible_paths <- c(
        "/usr/local/bin/quarto",
        "/usr/bin/quarto",
        "/opt/quarto/bin/quarto"
      )
      
      for (path in possible_paths) {
        if (file.exists(path)) {
          quarto_bin <- path
          break
        }
      }
    }
    
    if (nzchar(quarto_bin)) {
      Sys.setenv(QUARTO_PATH = quarto_bin)
      message("Setting QUARTO_PATH to: ", quarto_bin)
      return(TRUE)
    } else {
      warning("Quarto binary not found")
      return(FALSE)
    }
  }
  return(TRUE)
}

#' Render Sanity Check Report
#' 
#' Renders the polling station sanity check report with proper dependency handling.
#' 
#' @param input_file Path to the quarto document (default: "reports/polling_station_sanity_check.qmd")
#' @param dependencies Named list of data dependencies that should be available
#' @return Character vector of output files created
#' @export
render_sanity_check_report <- function(input_file = "reports/polling_station_sanity_check.qmd",
                                       dependencies = list()) {
  
  # Ensure dependencies are loaded (this forces evaluation in targets)
  for (dep_name in names(dependencies)) {
    dep_value <- dependencies[[dep_name]]
    if (is.null(dep_value)) {
      warning("Dependency '", dep_name, "' is NULL")
    }
  }
  
  # Ensure quarto can be found
  if (!ensure_quarto_path()) {
    stop("Cannot render report: quarto binary not found")
  }
  
  # Check input file exists
  if (!file.exists(input_file)) {
    stop("Input file not found: ", input_file)
  }
  
  # Render the report
  message("Rendering sanity check report: ", input_file)
  quarto::quarto_render(
    input = input_file,
    execute_dir = getwd()
  )
  
  # Determine output files
  output_files <- get_report_output_files(input_file)
  
  message("Report rendered successfully. Output files: ", 
          paste(output_files, collapse = ", "))
  
  return(output_files)
}

#' Get Report Output Files
#' 
#' Determines the output files created by rendering a quarto document.
#' 
#' @param input_file Path to the input quarto document
#' @return Character vector of output file paths
#' @export
get_report_output_files <- function(input_file) {
  # Determine base name and directory
  input_dir <- dirname(input_file)
  input_base <- tools::file_path_sans_ext(basename(input_file))
  
  # Main HTML output
  html_file <- file.path(input_dir, paste0(input_base, ".html"))
  output_files <- html_file
  
  # Support directory (for figures, etc.)
  support_dir <- file.path(input_dir, paste0(input_base, "_files"))
  if (dir.exists(support_dir)) {
    output_files <- c(output_files, support_dir)
  }
  
  # Only return files that actually exist
  output_files[file.exists(output_files)]
}
#' Create Validation Report Target
#' 
#' Factory function to create a validation report target with proper dependencies.
#' 
#' @param target_name Name of the target
#' @param input_file Path to the report source file
#' @param dependencies Character vector of dependency target names
#' @param report_type Type of report ("sanity_check" or "methodology")
#' @param dev_mode_only Logical indicating if report should only render in dev mode
#' @return A tar_target object for the report
#' @export
create_validation_report_target <- function(target_name,
                                           input_file,
                                           dependencies = c(),
                                           report_type = "sanity_check",
                                           dev_mode_only = FALSE) {
  
  # For sanity check report, use the simpler direct approach
  if (report_type == "sanity_check" && target_name == "sanity_check_report") {
    # Return a simple target that matches what the original code did
    return(tar_target(
      name = sanity_check_report,
      command = render_sanity_check_report(
        input_file = "reports/polling_station_sanity_check.qmd",
        dependencies = list(
          geocoded_locais = geocoded_locais,
          panel_ids = panel_ids
        )
      ),
      format = "file",
      deployment = "main"
    ))
  }
  
  # For other report types, create appropriate targets
  if (report_type == "methodology") {
    return(tar_render(
      name = as.name(target_name),
      path = input_file,
      cue = tar_cue(mode = "thorough")
    ))
  }
  
  stop("Unsupported report configuration")
}
#' Create comprehensive validation report
#'
#' Collects all validation results, manages data references, and generates reports
#' @param validation_list Named list of validation results
#' @param data_sources Named list mapping validation names to their data sources
#' @param locais_filtered Filtered polling stations data (for row count)
#' @param pipeline_config Pipeline configuration object
#' @return Validation summary with report path and statistics
#' @export
create_validation_report <- function(validation_list, data_sources, 
                                   locais_filtered, pipeline_config) {
  # Store original data references for failed record export
  # ONLY for smaller datasets to avoid memory exhaustion
  for (name in names(validation_list)) {
    # Skip CNEFE datasets which are too large to load into memory
    if (name %in% c("cnefe10_cleaned", "cnefe22_cleaned")) {
      validation_list[[name]]$metadata$data <- NULL
      validation_list[[name]]$metadata$skip_export <- TRUE
      next
    }
    
    # Assign data from sources if available
    if (name %in% names(data_sources)) {
      validation_list[[name]]$metadata$data <- data_sources[[name]]
    }
  }
  
  # Add Brasília filtering report path
  validation_list$brasilia_filtering <- list(
    passed = TRUE,
    metadata = list(
      type = "preprocessing",
      stage = "brasilia_filtering",
      n_rows = nrow(locais_filtered),
      report_path = "output/brasilia_filtering_report.rds",
      note = "Brasília records filtered from municipal election years"
    )
  )
  
  # Generate text validation report
  # Simple implementation - just save the validation list as RDS
  output_dir <- "output/validation_reports"
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  report_path <- file.path(output_dir, paste0("validation_report_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".rds"))
  saveRDS(validation_list, report_path)
  
  # Create a simple text summary
  summary_path <- sub("\\.rds$", "_summary.txt", report_path)
  sink(summary_path)
  cat("=== VALIDATION REPORT SUMMARY ===\n")
  cat("Generated:", format(Sys.time()), "\n")
  cat("Pipeline mode:", ifelse(pipeline_config$dev_mode, "DEVELOPMENT", "PRODUCTION"), "\n\n")
  
  # Summary of validation results
  passed <- 0
  failed <- 0
  for (name in names(validation_list)) {
    if (!is.null(validation_list[[name]]$passed)) {
      if (validation_list[[name]]$passed) {
        passed <- passed + 1
      } else {
        failed <- failed + 1
      }
    }
  }
  
  cat("Total validations:", length(validation_list), "\n")
  cat("Passed:", passed, "\n")
  cat("Failed:", failed, "\n\n")
  
  # Details for each validation
  cat("=== VALIDATION DETAILS ===\n")
  for (name in names(validation_list)) {
    result <- validation_list[[name]]
    cat("\n", name, ":\n", sep = "")
    cat("  Status:", ifelse(result$passed, "PASSED", "FAILED"), "\n")
    if (!is.null(result$metadata$message)) {
      cat("  Message:", result$metadata$message, "\n")
    }
  }
  
  sink()
  
  cat("Validation report saved to:", report_path, "\n")
  cat("Summary saved to:", summary_path, "\n")
  
  # Save validation summary for future reference
  summary_stats <- list(
    report_path = report_path,
    timestamp = Sys.time(),
    pipeline_version = "1.0.0",
    total_validations = length(validation_list),
    passed = sum(sapply(validation_list, function(x) x$passed)),
    failed = sum(sapply(validation_list, function(x) !x$passed)),
    dev_mode = pipeline_config$dev_mode,
    stage_summary = sapply(validation_list, function(x) {
      list(
        passed = x$passed,
        type = x$metadata$type,
        n_rows = x$metadata$n_rows %||% NA
      )
    })
  )
  
  summary_stats
}

#' Generate simplified validation report
#'
#' Simplified version focusing on critical validations only
#' @param validate_inputs Consolidated input validation result
#' @param validate_model_data Model data merge validation
#' @param validate_predictions Model predictions validation
#' @param validate_geocoded_output Final output validation
#' @param locais_filtered Filtered polling stations data
#' @param model_data Model training data
#' @param model_predictions Model predictions
#' @param geocoded_locais Final geocoded data
#' @param pipeline_config Pipeline configuration
#' @return Validation summary statistics
#' @export
generate_validation_report_simplified <- function(
  validate_inputs, validate_model_data, validate_predictions,
  validate_geocoded_output, locais_filtered, model_data, 
  model_predictions, geocoded_locais, pipeline_config
) {
  
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
    validation_list = validation_results,
    data_sources = data_sources,
    locais_filtered = locais_filtered,
    pipeline_config = pipeline_config
  )
  
  # Save summary
  saveRDS(summary_stats, "output/validation_summary.rds")
  
  # Print focused summary to console
  cat("\n========== VALIDATION REPORT SUMMARY ==========\n")
  cat("Report generated:", summary_stats$report_path, "\n")
  cat("Critical validations checked:", summary_stats$total_validations, "\n")
  cat("Passed:", summary_stats$passed, "\n")
  cat("Failed:", summary_stats$failed, "\n")
  cat(
    "Mode:",
    ifelse(summary_stats$dev_mode, "DEVELOPMENT", "PRODUCTION"),
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
    ifelse(summary_stats$failed == 0, "✅ SUCCESS", "❌ FAILURES DETECTED"),
    "\n"
  )
  cat("===============================================\n\n")
  
  summary_stats
}