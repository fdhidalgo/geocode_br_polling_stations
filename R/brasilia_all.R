# brasilia_all.R
#
# Consolidated Brasília-specific functions for the geocoding pipeline.
# This file sources all Brasília-related files and provides a unified interface
# to reduce the number of source() calls in _targets.R

# Load Brasília-specific modules
source("R/filter_brasilia_municipal.R")
source("R/validate_brasilia_filtering.R")
source("R/update_validation_for_brasilia.R")

# Brasília configuration
brasilia_config <- list(
  # Brasília municipality codes
  codes = list(
    ibge_7digit = 5300108,  # 7-digit IBGE code for Brasília
    tse_code = 97271       # TSE code for Brasília
  ),
  
  # Administrative regions (RAs) that should be treated separately
  admin_regions = c(
    "Plano Piloto", "Gama", "Taguatinga", "Brazlândia", "Sobradinho",
    "Planaltina", "Paranoá", "Núcleo Bandeirante", "Ceilândia",
    "Guará", "Cruzeiro", "Samambaia", "Santa Maria", "São Sebastião",
    "Recanto das Emas", "Lago Sul", "Riacho Fundo", "Lago Norte",
    "Candangolândia", "Águas Claras", "Riacho Fundo II", "Sudoeste/Octogonal",
    "Varjão", "Park Way", "SCIA", "Sobradinho II", "Jardim Botânico",
    "Itapoã", "SIA", "Vicente Pires", "Fercal"
  ),
  
  # Validation thresholds specific to Brasília
  validation = list(
    min_polling_stations = 500,  # Minimum expected polling stations
    max_polling_stations = 2000, # Maximum expected polling stations
    min_geocoded_rate = 0.8,     # Minimum geocoding success rate
    coordinate_bounds = list(
      lat_min = -16.05, lat_max = -15.50,  # Brasília latitude bounds
      lon_min = -48.30, lon_max = -47.25   # Brasília longitude bounds
    )
  )
)

#' Get Brasília Municipality Codes
#' 
#' @return Named list with IBGE and TSE codes for Brasília
#' @export
get_brasilia_codes <- function() {
  brasilia_config$codes
}

#' Check if municipality is Brasília
#' 
#' @param muni_code Municipality code (7-digit IBGE or TSE)
#' @return Logical indicating if municipality is Brasília
#' @export
is_brasilia <- function(muni_code) {
  codes <- get_brasilia_codes()
  muni_code %in% c(codes$ibge_7digit, codes$tse_code)
}

#' Filter data for Brasília municipality
#' 
#' @param data Data.table with municipality codes
#' @param code_col Column name containing municipality codes
#' @return Filtered data.table containing only Brasília records
#' @export
filter_brasilia_data <- function(data, code_col = "cod_localidade_ibge") {
  if (!code_col %in% names(data)) {
    stop("Column '", code_col, "' not found in data")
  }
  
  codes <- get_brasilia_codes()
  brasilia_codes <- c(codes$ibge_7digit, codes$tse_code)
  
  data[get(code_col) %in% brasilia_codes]
}

#' Validate Brasília coordinates
#' 
#' @param data Data.table with latitude and longitude columns
#' @param lat_col Column name for latitude (default: "latitude")
#' @param lon_col Column name for longitude (default: "longitude")
#' @return Validation result list
#' @export
validate_brasilia_coordinates <- function(data, lat_col = "latitude", lon_col = "longitude") {
  bounds <- brasilia_config$validation$coordinate_bounds
  
  if (!all(c(lat_col, lon_col) %in% names(data))) {
    stop("Coordinate columns not found in data")
  }
  
  # Check coordinate bounds
  valid_coords <- data[
    !is.na(get(lat_col)) & !is.na(get(lon_col)) &
    get(lat_col) >= bounds$lat_min & get(lat_col) <= bounds$lat_max &
    get(lon_col) >= bounds$lon_min & get(lon_col) <= bounds$lon_max
  ]
  
  total_coords <- data[!is.na(get(lat_col)) & !is.na(get(lon_col))]
  
  validation_rate <- if (nrow(total_coords) > 0) {
    nrow(valid_coords) / nrow(total_coords)
  } else {
    0
  }
  
  list(
    passed = validation_rate >= 0.95,  # 95% should be within bounds
    validation_rate = validation_rate,
    total_records = nrow(data),
    coords_available = nrow(total_coords),
    coords_in_bounds = nrow(valid_coords),
    out_of_bounds = nrow(total_coords) - nrow(valid_coords),
    metadata = list(
      bounds_used = bounds,
      validation_date = Sys.Date()
    )
  )
}

#' Comprehensive Brasília data validation
#' 
#' @param polling_data Data.table with polling station data
#' @return Comprehensive validation result
#' @export
validate_brasilia_data <- function(polling_data) {
  results <- list()
  
  # Basic count validation
  n_stations <- nrow(polling_data)
  min_expected <- brasilia_config$validation$min_polling_stations
  max_expected <- brasilia_config$validation$max_polling_stations
  
  results$count_validation <- list(
    passed = n_stations >= min_expected & n_stations <= max_expected,
    n_stations = n_stations,
    expected_range = c(min_expected, max_expected),
    message = if (n_stations < min_expected) {
      "Too few polling stations"
    } else if (n_stations > max_expected) {
      "Too many polling stations" 
    } else {
      "Station count within expected range"
    }
  )
  
  # Coordinate validation
  if (all(c("latitude", "longitude") %in% names(polling_data))) {
    results$coordinate_validation <- validate_brasilia_coordinates(polling_data)
  }
  
  # Geocoding rate validation
  if ("latitude" %in% names(polling_data)) {
    geocoded_count <- sum(!is.na(polling_data$latitude))
    geocoding_rate <- geocoded_count / n_stations
    min_rate <- brasilia_config$validation$min_geocoded_rate
    
    results$geocoding_validation <- list(
      passed = geocoding_rate >= min_rate,
      geocoding_rate = geocoding_rate,
      geocoded_count = geocoded_count,
      total_count = n_stations,
      min_required_rate = min_rate
    )
  }
  
  # Overall validation
  all_passed <- all(sapply(results, function(x) x$passed %||% TRUE))
  
  results$overall <- list(
    passed = all_passed,
    summary = paste(
      "Brasília validation:",
      if (all_passed) "PASSED" else "FAILED"
    ),
    validation_date = Sys.Date()
  )
  
  return(results)
}

#' Process Brasília polling stations with special handling
#' 
#' @param data Raw polling station data for Brasília
#' @return Processed data.table with Brasília-specific adjustments
#' @export
process_brasilia_polling_data <- function(data) {
  if (nrow(data) == 0) {
    warning("No Brasília data to process")
    return(data)
  }
  
  # Apply Brasília-specific filtering and validation
  processed_data <- filter_brasilia_municipal(data)
  
  # Validate the processed data
  validation_result <- validate_brasilia_data(processed_data)
  
  if (!validation_result$overall$passed) {
    warning("Brasília data validation failed: ", 
            validation_result$overall$summary)
  }
  
  # Add metadata
  attr(processed_data, "brasilia_validation") <- validation_result
  attr(processed_data, "processing_date") <- Sys.Date()
  
  return(processed_data)
}

# Helper function to define null coalescing operator if not available
if (!exists("%||%")) {
  `%||%` <- function(x, y) if (is.null(x)) y else x
}