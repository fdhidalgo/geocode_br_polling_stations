#' Filter Brasília Polling Stations from Municipal Election Years
#'
#' Removes Brasília (DF) polling station records from municipal election years
#' (2008, 2012, 2016, 2020, 2024) since the Federal District does not hold
#' municipal elections.
#'
#' @param dt A data.table containing polling station data with columns:
#'   - sg_uf or uf: State code
#'   - ano or ano_eleicao: Election year
#' @return A data.table with Brasília records removed from municipal election years
#' @export
filter_brasilia_municipal_elections <- function(dt) {
  # Make a copy to avoid modifying the original
  dt_filtered <- copy(dt)
  
  # Define municipal election years
  municipal_years <- c(2008, 2012, 2016, 2020, 2024)
  
  # Handle different column naming conventions
  uf_col <- if ("sg_uf" %in% names(dt_filtered)) "sg_uf" else "uf"
  year_col <- if ("ano" %in% names(dt_filtered)) "ano" else "ano_eleicao"
  
  # Count records before filtering
  initial_count <- nrow(dt_filtered)
  df_municipal_count <- nrow(dt_filtered[get(uf_col) == "DF" & get(year_col) %in% municipal_years])
  
  # Remove Brasília (DF) records from municipal election years
  dt_filtered <- dt_filtered[!(get(uf_col) == "DF" & get(year_col) %in% municipal_years)]
  
  # Log filtering results
  removed_count <- initial_count - nrow(dt_filtered)
  
  if (removed_count > 0) {
    message(sprintf(
      "Removed %d Brasília polling stations from municipal election years (%d records)",
      removed_count,
      df_municipal_count
    ))
    
    # Log details by year
    removed_by_year <- dt[get(uf_col) == "DF" & get(year_col) %in% municipal_years, 
                          .N, by = get(year_col)][order(get)]
    if (nrow(removed_by_year) > 0) {
      message("Removed records by year:")
      for (i in 1:nrow(removed_by_year)) {
        message(sprintf("  - %d: %d records", 
                       removed_by_year[i, get], 
                       removed_by_year[i, N]))
      }
    }
  }
  
  return(dt_filtered)
}

#' Get Brasília Filtering Summary
#'
#' Provides a detailed summary of Brasília records across all years
#' to help understand the filtering impact.
#'
#' @param dt A data.table containing polling station data
#' @return A data.table with summary statistics
#' @export
summarize_brasilia_records <- function(dt) {
  # Handle different column naming conventions
  uf_col <- if ("sg_uf" %in% names(dt)) "sg_uf" else "uf"
  year_col <- if ("ano" %in% names(dt)) "ano" else "ano_eleicao"
  
  # Define election types
  municipal_years <- c(2008, 2012, 2016, 2020, 2024)
  federal_state_years <- c(2006, 2010, 2014, 2018, 2022)
  
  # Get DF records summary - first group by year
  df_data <- dt[get(uf_col) == "DF"]
  
  if (nrow(df_data) == 0) {
    return(data.table(year = character(), n_records = integer(), 
                     n_stations = integer(), election_type = character()))
  }
  
  df_summary <- df_data[, .(
    n_records = .N,
    n_stations = ifelse("local_id" %in% names(.SD), uniqueN(local_id), .N)
  ), by = get(year_col)]
  
  setnames(df_summary, "get", "year")
  
  # Add election type
  df_summary[, election_type := ifelse(year %in% municipal_years, 
                                      "Municipal", 
                                      "Federal/State")]
  
  setorder(df_summary, year)
  
  # Add totals
  totals <- df_summary[, .(
    year = "TOTAL",
    n_records = sum(n_records),
    n_stations = sum(n_stations),
    election_type = "All"
  )]
  
  df_summary <- rbind(df_summary, totals)
  
  return(df_summary)
}