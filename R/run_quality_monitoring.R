# Standalone Quality Monitoring Runner
# Can be run independently or integrated into targets pipeline

source("R/data_quality_monitor.R")

#' Run quality monitoring with simplified Quarto handling
#' 
#' @param skip_quarto logical, whether to skip the Quarto report generation
#' @return monitoring results
run_quality_monitoring_standalone <- function(skip_quarto = FALSE) {
  
  # Temporarily override the Quarto function if needed
  if (skip_quarto) {
    # Store original function
    orig_func <- run_quarto_sanity_check
    
    # Replace with stub
    assign("run_quarto_sanity_check", function() {
      list(
        success = FALSE,
        error = "Quarto report generation skipped",
        generated = Sys.time()
      )
    }, envir = .GlobalEnv)
    
    # Run monitoring
    results <- run_data_quality_monitoring(generate_alerts = TRUE)
    
    # Restore original function
    assign("run_quarto_sanity_check", orig_func, envir = .GlobalEnv)
    
  } else {
    results <- run_data_quality_monitoring(generate_alerts = TRUE)
  }
  
  return(results)
}

# Function to generate a simple text report
generate_text_report <- function(results, output_file = NULL) {
  
  if (is.null(output_file)) {
    output_file <- paste0("output/monitoring/quality_report_", Sys.Date(), ".txt")
  }
  
  # Ensure directory exists
  dir.create(dirname(output_file), recursive = TRUE, showWarnings = FALSE)
  
  # Start report
  report_lines <- c(
    "========================================",
    "DATA QUALITY MONITORING REPORT",
    paste("Generated:", Sys.time()),
    "========================================",
    ""
  )
  
  # Overall status
  if (!is.null(results$overall_assessment)) {
    report_lines <- c(report_lines,
      "OVERALL STATUS:",
      paste("  ", results$overall_assessment$overall_status),
      ""
    )
  }
  
  # Key metrics
  if (!is.null(results$metrics)) {
    report_lines <- c(report_lines,
      "KEY METRICS:",
      paste("  Total Municipalities:", results$metrics$total_municipalities),
      paste("  Total Polling Stations:", results$metrics$total_stations),
      paste("  Years Covered:", paste(results$metrics$years_covered, collapse = ", ")),
      paste("  Geocoding Coverage:", round(results$metrics$geocoding_coverage, 1), "%")
    )
    
    if (!is.null(results$metrics$panel_coverage)) {
      report_lines <- c(report_lines,
        paste("  Panel Coverage:", round(results$metrics$panel_coverage, 1), "%")
      )
    }
    
    report_lines <- c(report_lines, "")
  }
  
  # Historical comparison
  if (!is.null(results$historical_comparison) && 
      results$historical_comparison$status != "insufficient_data") {
    
    report_lines <- c(report_lines,
      "HISTORICAL COMPARISON:",
      paste("  Years analyzed:", paste(results$historical_comparison$summary$years_analyzed, collapse = ", "))
    )
    
    # Count extreme changes
    extreme_count <- 0
    for (period in names(results$historical_comparison)) {
      if (period != "summary" && is.list(results$historical_comparison[[period]])) {
        trend <- results$historical_comparison[[period]]
        if ((trend$extreme_increases + trend$extreme_decreases) > 0) {
          extreme_count <- extreme_count + 1
          report_lines <- c(report_lines,
            paste("  ", period, ": ", 
                  trend$extreme_increases, " extreme increases, ",
                  trend$extreme_decreases, " extreme decreases", sep = "")
          )
        }
      }
    }
    
    if (extreme_count == 0) {
      report_lines <- c(report_lines, "  No extreme changes detected")
    }
    
    report_lines <- c(report_lines, "")
  }
  
  # Duplicate detection
  if (!is.null(results$duplicate_detection)) {
    report_lines <- c(report_lines, "DUPLICATE DETECTION:")
    
    if (!is.null(results$duplicate_detection$coordinate_duplicates)) {
      report_lines <- c(report_lines,
        paste("  Coordinate duplicates:", 
              results$duplicate_detection$coordinate_duplicates$total_duplicate_groups,
              "groups affecting",
              results$duplicate_detection$coordinate_duplicates$total_duplicate_stations,
              "stations")
      )
    }
    
    if (!is.null(results$duplicate_detection$id_duplicates)) {
      report_lines <- c(report_lines,
        paste("  ID duplicates:", 
              results$duplicate_detection$id_duplicates$total_duplicate_ids,
              "IDs affecting",
              results$duplicate_detection$id_duplicates$total_affected_records,
              "records")
      )
    }
    
    if (results$duplicate_detection$summary$total_issues == 0) {
      report_lines <- c(report_lines, "  No duplicates detected")
    }
    
    report_lines <- c(report_lines, "")
  }
  
  # Alerts
  if (!is.null(results$alerts) && length(results$alerts) > 0) {
    report_lines <- c(report_lines,
      "ACTIVE ALERTS:",
      paste("  -", results$alerts),
      ""
    )
  }
  
  # Failed checks
  if (!is.null(results$overall_assessment$failed_checks) && 
      length(results$overall_assessment$failed_checks) > 0) {
    report_lines <- c(report_lines,
      "FAILED CHECKS:",
      paste("  -", results$overall_assessment$failed_checks),
      ""
    )
  }
  
  # Warnings
  if (!is.null(results$overall_assessment$warnings) && 
      length(results$overall_assessment$warnings) > 0) {
    report_lines <- c(report_lines,
      "WARNINGS:",
      paste("  -", results$overall_assessment$warnings),
      ""
    )
  }
  
  # Write report
  writeLines(report_lines, output_file)
  message("Text report saved to: ", output_file)
  
  return(invisible(output_file))
}

# If running as script
if (!interactive()) {
  message("Running data quality monitoring...")
  results <- run_quality_monitoring_standalone(skip_quarto = TRUE)
  generate_text_report(results)
  
  # Exit with appropriate code
  if (results$overall_assessment$overall_status == "FAIL") {
    quit(status = 1)
  } else {
    quit(status = 0)
  }
}