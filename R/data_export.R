#' Data Export Functions
#' 
#' Consolidated file containing all data export functions.
#' This file combines export functions from:
#' - target_helpers.R
#' - data_cleaning_fns.R
#' - panel_id_fns.R

library(data.table)

#' Export geocoded locations to file
#' 
#' @param geocoded_locais Geocoded locations data
#' @return Path to exported file
#' @export
export_geocoded_locais <- function(geocoded_locais) {
  fwrite(geocoded_locais, "./output/geocoded_polling_stations.csv.gz")
  "./output/geocoded_polling_stations.csv.gz"
}

#' Export panel IDs to file
#' 
#' @param panel_ids Panel ID data to export
#' @return Path to exported file
#' @export
export_panel_ids <- function(panel_ids) {
  fwrite(panel_ids, "./output/panel_ids.csv.gz")
  "./output/panel_ids.csv.gz"
}

#' Export geocoded data with validation dependency
#'
#' @param geocoded_locais Geocoded locations data
#' @param validation_report Validation report (ensures it runs first)
#' @return File path of exported data
#' @export
export_geocoded_with_validation <- function(geocoded_locais, validation_report) {
  # validation_report is passed to ensure dependency
  export_geocoded_locais(geocoded_locais)
}

#' Export panel IDs with validation dependency
#'
#' @param panel_ids Panel ID data
#' @param validation_report Validation report (ensures it runs first)
#' @return File path of exported data
#' @export
export_panel_ids_with_validation <- function(panel_ids, validation_report) {
  # validation_report is passed to ensure dependency
  export_panel_ids(panel_ids)
}