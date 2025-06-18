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

#' Render Methodology Report
#' 
#' Renders the methodology/writeup report using tar_render approach.
#' 
#' @param input_file Path to the R Markdown document
#' @return Character vector of output files
#' @export
render_methodology_report <- function(input_file = "doc/geocoding_procedure.Rmd") {
  
  if (!file.exists(input_file)) {
    stop("Input file not found: ", input_file)
  }
  
  # Ensure quarto/pandoc can be found
  ensure_quarto_path()
  
  message("Rendering methodology report: ", input_file)
  
  # Use rmarkdown to render (since this is .Rmd not .qmd)
  output_file <- rmarkdown::render(
    input = input_file,
    quiet = FALSE
  )
  
  message("Methodology report rendered: ", output_file)
  return(output_file)
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

#' Configure Report Targets
#' 
#' Creates all standard report targets for the pipeline.
#' 
#' @param dev_mode Logical indicating if running in development mode
#' @return List of report targets
#' @export
configure_report_targets <- function(dev_mode = FALSE) {
  targets <- list()
  
  # Sanity check report (always included)
  targets$sanity_check <- create_validation_report_target(
    target_name = "sanity_check_report",
    input_file = "reports/polling_station_sanity_check.qmd",
    dependencies = c("geocoded_locais", "panel_ids"),
    report_type = "sanity_check"
  )
  
  # Methodology report (only in production mode)
  if (!dev_mode) {
    targets$methodology <- tar_render(
      name = geocode_writeup,
      path = "doc/geocoding_procedure.Rmd",
      cue = tar_cue(mode = "thorough")
    )
  }
  
  return(targets)
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
  report_path <- generate_text_validation_report(
    validation_results = validation_list,
    output_dir = "output/validation_reports",
    export_failures = TRUE
  )
  
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

#' Generate and save validation report with summary
#'
#' Complete wrapper function that collects validation results, generates report,
#' saves summary, and prints console output
#' @param validate_muni_ids Validation result for muni_ids
#' @param validate_inep_codes Validation result for inep_codes
#' @param validate_inep_clean Validation result for inep data
#' @param validate_locais Validation result for locais
#' @param validate_inep_match Validation result for inep matches
#' @param validate_model_data Validation result for model data
#' @param validate_predictions Validation result for predictions
#' @param validate_geocoded_output Validation result for geocoded output
#' @param muni_ids Municipality IDs data
#' @param inep_codes INEP codes data
#' @param inep_data INEP data
#' @param locais_filtered Filtered locais data
#' @param inep_string_match INEP string match results
#' @param model_data Model data
#' @param model_predictions Model predictions
#' @param geocoded_locais Geocoded locais
#' @param pipeline_config Pipeline configuration
#' @return Validation summary statistics
#' @export
generate_validation_report_complete <- function(
  validate_muni_ids, validate_inep_codes, validate_inep_clean, validate_locais,
  validate_inep_match, validate_model_data, validate_predictions,
  validate_geocoded_output, muni_ids, inep_codes, inep_data,
  locais_filtered, inep_string_match, model_data, model_predictions,
  geocoded_locais, pipeline_config
) {
  
  # Collect all validation results
  validation_results <- list(
    muni_ids = validate_muni_ids,
    inep_codes = validate_inep_codes,
    inep_cleaned = validate_inep_clean,
    locais = validate_locais,
    inep_string_match = validate_inep_match,
    model_data_merge = validate_model_data,
    model_predictions = validate_predictions,
    geocoded_output = validate_geocoded_output
  )
  
  # Create data source mapping
  data_sources <- list(
    muni_ids = muni_ids,
    inep_codes = inep_codes,
    inep_cleaned = inep_data,
    locais = locais_filtered,
    inep_string_match = inep_string_match,
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
  
  # Print summary to console
  cat("\n========== VALIDATION REPORT SUMMARY ==========\n")
  cat("Report generated:", summary_stats$report_path, "\n")
  cat("Total stages validated:", summary_stats$total_validations, "\n")
  cat("Passed:", summary_stats$passed, "\n")
  cat("Failed:", summary_stats$failed, "\n")
  cat(
    "Mode:",
    ifelse(summary_stats$dev_mode, "DEVELOPMENT", "PRODUCTION"),
    "\n"
  )
  cat(
    "Overall status:",
    ifelse(summary_stats$failed == 0, "✅ SUCCESS", "❌ FAILURES DETECTED"),
    "\n"
  )
  cat("===============================================\n\n")
  
  summary_stats
}