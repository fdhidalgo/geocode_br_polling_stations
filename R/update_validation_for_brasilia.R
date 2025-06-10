#' Update Validation Rules for Brasília Filtering
#'
#' Updates validation expectations to account for Brasília being filtered
#' from municipal election years.
#'
#' @param validation_result The original validation result
#' @param year The election year being validated
#' @return Updated validation result with Brasília-aware expectations
#' @export
update_validation_for_brasilia <- function(validation_result, year) {
  if (!inherits(validation_result, "validation_result")) {
    return(validation_result)
  }
  
  # Municipal years where Brasília is filtered
  municipal_years <- c(2008, 2012, 2016, 2020, 2024)
  
  # Add metadata about Brasília filtering
  if (!is.null(validation_result$metadata)) {
    validation_result$metadata$brasilia_filtered <- year %in% municipal_years
    validation_result$metadata$brasilia_note <- if (year %in% municipal_years) {
      "Brasília (DF) records have been filtered from this municipal election year"
    } else {
      "Brasília (DF) records are included (federal/state election year)"
    }
  }
  
  return(validation_result)
}

#' Add Brasília Filtering Summary to Report
#'
#' Creates a summary section for Brasília filtering to be included
#' in validation reports.
#'
#' @param data The filtered dataset
#' @param year The election year
#' @return A list with summary information
#' @export
create_brasilia_filtering_summary <- function(data, year) {
  municipal_years <- c(2008, 2012, 2016, 2020, 2024)
  federal_years <- c(2006, 2010, 2014, 2018, 2022)
  
  # Check DF presence
  df_records <- data[sg_uf == "DF", .N]
  df_stations <- data[sg_uf == "DF", uniqueN(local_id)]
  
  summary <- list(
    year = year,
    election_type = if (year %in% municipal_years) "Municipal" else if (year %in% federal_years) "Federal/State" else "Other",
    brasilia_filtered = year %in% municipal_years,
    df_records = df_records,
    df_stations = df_stations,
    total_states = uniqueN(data$sg_uf),
    has_df = "DF" %in% unique(data$sg_uf)
  )
  
  # Create status message
  if (year %in% municipal_years) {
    if (df_records > 0) {
      summary$status <- "WARNING"
      summary$message <- sprintf("Found %d Brasília records in municipal election year %d. These should have been filtered.", 
                                df_records, year)
    } else {
      summary$status <- "OK"
      summary$message <- sprintf("Brasília correctly filtered from municipal election year %d.", year)
    }
  } else if (year %in% federal_years) {
    if (df_records == 0) {
      summary$status <- "WARNING"
      summary$message <- sprintf("No Brasília records found in federal/state election year %d. Some may be missing.", year)
    } else {
      summary$status <- "OK"  
      summary$message <- sprintf("Brasília correctly included in federal/state election year %d (%d stations).", 
                                year, df_stations)
    }
  } else {
    summary$status <- "INFO"
    summary$message <- sprintf("Year %d is not a standard election year.", year)
  }
  
  return(summary)
}

#' Format Brasília Summary for Display
#'
#' Formats the Brasília filtering summary for inclusion in reports.
#'
#' @param summary The summary from create_brasilia_filtering_summary
#' @return Formatted text for display
#' @export
format_brasilia_summary <- function(summary) {
  status_icon <- switch(summary$status,
    "OK" = "✅",
    "WARNING" = "⚠️",
    "INFO" = "ℹ️",
    "❓"
  )
  
  lines <- c(
    paste("## Brasília (DF) Filtering Status"),
    "",
    paste("**Year:**", summary$year),
    paste("**Election Type:**", summary$election_type),
    paste("**Filtering Applied:**", ifelse(summary$brasilia_filtered, "Yes", "No")),
    paste("**DF Records:**", format(summary$df_records, big.mark = ",")),
    paste("**DF Stations:**", format(summary$df_stations, big.mark = ",")),
    "",
    paste(status_icon, summary$message),
    "",
    "### Background",
    "- Brasília (Federal District) does not hold municipal elections",
    "- It only participates in federal and state elections (every 4 years)",
    "- Records are filtered from years: 2008, 2012, 2016, 2020, 2024"
  )
  
  return(paste(lines, collapse = "\n"))
}