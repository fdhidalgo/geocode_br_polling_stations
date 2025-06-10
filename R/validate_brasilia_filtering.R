#' Validate Brasília Filtering
#'
#' Validates that Brasília (DF) records have been correctly filtered from
#' municipal election years and that federal/state election years are preserved.
#'
#' @param dt A data.table containing filtered polling station data
#' @param stop_on_failure Logical; if TRUE, stops execution on validation failure
#' @return A list with validation results or stops execution if validation fails
#' @export
validate_brasilia_filtering <- function(dt, stop_on_failure = TRUE) {
  # Define municipal and federal/state election years
  municipal_years <- c(2008, 2012, 2016, 2020, 2024)
  federal_state_years <- c(2006, 2010, 2014, 2018, 2022)
  
  # Handle different column naming conventions
  uf_col <- if ("sg_uf" %in% names(dt)) "sg_uf" else "uf"
  year_col <- if ("ano" %in% names(dt)) "ano" else "ano_eleicao"
  
  # Initialize validation results
  validation_results <- list(
    passed = TRUE,
    checks = list(),
    summary = list()
  )
  
  # Check if DF is in the dataset at all
  has_df_data <- any(dt[[uf_col]] == "DF")
  
  if (!has_df_data) {
    # No DF data in dataset (e.g., dev mode with only AC/RR)
    validation_results$checks$no_df_data <- list(
      passed = TRUE,
      message = "SKIP: No Brasília (DF) data in dataset - validation not applicable",
      details = NULL
    )
    validation_results$summary$has_df_data <- FALSE
    validation_results$summary$total_records <- nrow(dt)
    validation_results$summary$states_present <- unique(dt[[uf_col]])
    return(validation_results)
  }
  
  # Check 1: No DF records in municipal years
  df_municipal <- dt[get(uf_col) == "DF" & get(year_col) %in% municipal_years]
  check1_passed <- nrow(df_municipal) == 0
  
  validation_results$checks$no_df_municipal <- list(
    passed = check1_passed,
    message = if (check1_passed) {
      "PASS: No Brasília records found in municipal election years"
    } else {
      sprintf("FAIL: Found %d Brasília records in municipal election years", nrow(df_municipal))
    },
    details = if (!check1_passed) {
      df_municipal[, .N, by = get(year_col)][order(get)]
    } else {
      NULL
    }
  )
  
  # Check 2: DF records exist in federal/state years (if any of those years are in the data)
  available_federal_years <- intersect(unique(dt[[year_col]]), federal_state_years)
  
  if (length(available_federal_years) > 0) {
    df_federal <- dt[get(uf_col) == "DF" & get(year_col) %in% available_federal_years]
    check2_passed <- nrow(df_federal) > 0
    
    validation_results$checks$df_federal_exists <- list(
      passed = check2_passed,
      message = if (check2_passed) {
        sprintf("PASS: Found %d Brasília records in federal/state election years", nrow(df_federal))
      } else {
        "FAIL: No Brasília records found in federal/state election years"
      },
      details = if (check2_passed) {
        df_federal[, .N, by = get(year_col)][order(get)]
      } else {
        NULL
      }
    )
  } else {
    validation_results$checks$df_federal_exists <- list(
      passed = NA,
      message = "SKIP: No federal/state election years in dataset",
      details = NULL
    )
  }
  
  # Check 3: Other states are not affected
  # Count records by state and year
  state_year_before <- dt[, .N, by = c(uf_col, year_col)]
  
  # This check ensures the filtering didn't accidentally remove non-DF records
  non_df_municipal <- dt[get(uf_col) != "DF" & get(year_col) %in% municipal_years]
  check3_passed <- nrow(non_df_municipal) > 0 || !any(dt[[year_col]] %in% municipal_years)
  
  validation_results$checks$other_states_preserved <- list(
    passed = check3_passed,
    message = if (check3_passed) {
      "PASS: Other states' records are preserved"
    } else {
      "FAIL: Non-DF records may have been incorrectly filtered"
    },
    details = NULL
  )
  
  # Summary statistics
  validation_results$summary <- list(
    total_records = nrow(dt),
    df_records_total = nrow(dt[get(uf_col) == "DF"]),
    df_records_by_year = dt[get(uf_col) == "DF", .N, by = get(year_col)][order(get)],
    unique_states = uniqueN(dt[[uf_col]]),
    years_in_data = sort(unique(dt[[year_col]]))
  )
  
  # Overall pass/fail
  validation_results$passed <- all(
    sapply(validation_results$checks, function(x) is.na(x$passed) || x$passed)
  )
  
  # Print results
  cat("\n=== BRASÍLIA FILTERING VALIDATION ===\n")
  cat(sprintf("Overall Status: %s\n\n", 
              ifelse(validation_results$passed, "PASSED", "FAILED")))
  
  for (check_name in names(validation_results$checks)) {
    check <- validation_results$checks[[check_name]]
    cat(sprintf("• %s\n", check$message))
    
    if (!is.null(check$details) && nrow(check$details) > 0) {
      print(check$details)
      cat("\n")
    }
  }
  
  cat("\nSummary:\n")
  cat(sprintf("- Total records: %s\n", format(validation_results$summary$total_records, big.mark = ",")))
  cat(sprintf("- DF records: %s\n", format(validation_results$summary$df_records_total, big.mark = ",")))
  cat(sprintf("- States in data: %d\n", validation_results$summary$unique_states))
  cat(sprintf("- Years in data: %s\n", paste(validation_results$summary$years_in_data, collapse = ", ")))
  
  # Stop if validation failed and stop_on_failure is TRUE
  if (!validation_results$passed && stop_on_failure) {
    stop("Brasília filtering validation failed! See details above.")
  }
  
  return(invisible(validation_results))
}

#' Generate Brasília Filtering Report
#'
#' Creates a detailed report of Brasília records before and after filtering
#' to help understand the impact of the filtering operation.
#'
#' @param dt_before Data.table before filtering
#' @param dt_after Data.table after filtering
#' @param output_file Optional file path to save the report
#' @return A list containing the report data
#' @export
generate_brasilia_filtering_report <- function(dt_before, dt_after, output_file = NULL) {
  # Handle different column naming conventions
  uf_col <- if ("sg_uf" %in% names(dt_before)) "sg_uf" else "uf"
  year_col <- if ("ano" %in% names(dt_before)) "ano" else "ano_eleicao"
  
  # Municipal years
  municipal_years <- c(2008, 2012, 2016, 2020, 2024)
  
  # Calculate differences
  df_before <- dt_before[get(uf_col) == "DF"]
  df_after <- dt_after[get(uf_col) == "DF"]
  
  # Records removed
  records_removed <- nrow(df_before) - nrow(df_after)
  
  # Detailed breakdown
  df_before_summary <- df_before[, .(
    records_before = .N,
    election_type = ifelse(get(year_col) %in% municipal_years, "Municipal", "Federal/State")
  ), by = get(year_col)]
  
  df_after_summary <- df_after[, .(
    records_after = .N
  ), by = get(year_col)]
  
  # Merge summaries
  setnames(df_before_summary, "get", "year")
  setnames(df_after_summary, "get", "year")
  
  report_data <- merge(df_before_summary, df_after_summary, 
                      by = "year", all.x = TRUE)
  report_data[is.na(records_after), records_after := 0]
  report_data[, records_removed := records_before - records_after]
  
  # Create report
  report <- list(
    summary = list(
      total_records_before = nrow(dt_before),
      total_records_after = nrow(dt_after),
      df_records_before = nrow(df_before),
      df_records_after = nrow(df_after),
      df_records_removed = records_removed,
      removal_percentage = round(records_removed / nrow(df_before) * 100, 1)
    ),
    details_by_year = report_data[order(year)],
    validation = validate_brasilia_filtering(dt_after, stop_on_failure = FALSE)
  )
  
  # Print report
  cat("\n=== BRASÍLIA FILTERING REPORT ===\n")
  cat(sprintf("Date: %s\n\n", Sys.time()))
  
  cat("Overall Impact:\n")
  cat(sprintf("- Total records: %s → %s\n", 
              format(report$summary$total_records_before, big.mark = ","),
              format(report$summary$total_records_after, big.mark = ",")))
  cat(sprintf("- DF records: %s → %s (removed %d, %.1f%%)\n",
              format(report$summary$df_records_before, big.mark = ","),
              format(report$summary$df_records_after, big.mark = ","),
              report$summary$df_records_removed,
              report$summary$removal_percentage))
  
  cat("\nDF Records by Year:\n")
  print(report$details_by_year)
  
  # Save to file if requested
  if (!is.null(output_file)) {
    saveRDS(report, output_file)
    cat(sprintf("\nReport saved to: %s\n", output_file))
  }
  
  return(invisible(report))
}