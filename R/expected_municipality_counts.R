#' Get Expected Municipality Count for a Given Year
#'
#' Returns the expected number of municipalities for a given election year,
#' accounting for Brasília's special status and municipality creation/extinction.
#'
#' @param year The election year
#' @param include_df Logical; whether to include Brasília (DF) in the count
#' @return Expected municipality count
#' @export
get_expected_municipality_count <- function(year, include_df = TRUE) {
  # Base municipality count (as of 2022)
  base_count <- 5570
  
  # Municipal election years where DF should be excluded
  municipal_years <- c(2008, 2012, 2016, 2020, 2024)
  
  # Adjustments for municipality creation/extinction by year
  # These are approximate based on historical data
  adjustments <- list(
    "2006" = -5,   # Fewer municipalities in earlier years
    "2008" = -5,
    "2010" = -3,   # Mojuí dos Campos and others created
    "2012" = -1,
    "2013" = 0,    # Several municipalities created (Pescaria Brava, Balneário Rincão, Paraíso das Águas)
    "2014" = 0,
    "2016" = 0,
    "2018" = 0,
    "2020" = 0,
    "2022" = 0,
    "2024" = 0
  )
  
  # Get adjustment for the year
  year_adjustment <- adjustments[[as.character(year)]] %||% 0
  expected <- base_count + year_adjustment
  
  # Subtract 1 for DF in municipal election years
  # (Brasília doesn't participate in municipal elections)
  if (year %in% municipal_years && !include_df) {
    expected <- expected - 1
  }
  
  return(expected)
}

#' Get Expected Municipality Range
#'
#' Returns a reasonable range for municipality counts, accounting for
#' data quality issues and administrative changes.
#'
#' @param year The election year
#' @param tolerance_pct Percentage tolerance (default 2%)
#' @return A list with min and max expected values
#' @export
get_expected_municipality_range <- function(year, tolerance_pct = 2) {
  expected <- get_expected_municipality_count(year)
  tolerance <- ceiling(expected * tolerance_pct / 100)
  
  list(
    min = expected - tolerance,
    max = expected + tolerance,
    expected = expected
  )
}

#' Validate Municipality Count
#'
#' Checks if the observed municipality count is within expected range.
#'
#' @param observed_count The observed number of municipalities
#' @param year The election year
#' @param tolerance_pct Percentage tolerance for the check
#' @return A list with validation results
#' @export
validate_municipality_count <- function(observed_count, year, tolerance_pct = 2) {
  range <- get_expected_municipality_range(year, tolerance_pct)
  
  list(
    passed = observed_count >= range$min && observed_count <= range$max,
    observed = observed_count,
    expected = range$expected,
    min = range$min,
    max = range$max,
    message = if (observed_count < range$min) {
      sprintf("Municipality count too low: %d (expected %d-%d)", 
              observed_count, range$min, range$max)
    } else if (observed_count > range$max) {
      sprintf("Municipality count too high: %d (expected %d-%d)", 
              observed_count, range$min, range$max)
    } else {
      sprintf("Municipality count OK: %d (within expected range %d-%d)", 
              observed_count, range$min, range$max)
    }
  )
}