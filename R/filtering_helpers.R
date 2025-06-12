# filtering_helpers.R
#
# Common filtering operations used throughout the geocoding pipeline.
# This consolidates repetitive filtering logic to reduce code duplication in _targets.R

#' Filter Data by Development Mode
#' 
#' Applies development mode filtering to limit data to a subset of states.
#' This is the main entry point for dev mode filtering throughout the pipeline.
#' 
#' @param data Data.table to filter
#' @param pipeline_config Pipeline configuration object with dev_mode and dev_states
#' @param state_col Column name containing state codes (default: "uf")
#' @return Filtered data.table
#' @export
filter_by_dev_mode <- function(data, pipeline_config, state_col = "uf") {
  if (!pipeline_config$dev_mode) {
    return(data)
  }
  
  filter_data_by_state(data, pipeline_config$dev_states, state_col)
}

#' Filter Data by State
#' 
#' Filters data.table to include only specified states.
#' 
#' @param data Data.table to filter
#' @param states Character vector of state codes to include
#' @param state_col Column name containing state codes
#' @return Filtered data.table
#' @export
filter_data_by_state <- function(data, states, state_col = "uf") {
  if (!state_col %in% names(data)) {
    stop("Column '", state_col, "' not found in data")
  }
  
  if (length(states) == 0) {
    warning("No states specified for filtering")
    return(data)
  }
  
  data[get(state_col) %in% states]
}

#' Filter Data by Year
#' 
#' Filters data.table to include only specified years.
#' 
#' @param data Data.table to filter
#' @param years Numeric vector of years to include
#' @param year_col Column name containing years (default: "ano")
#' @return Filtered data.table
#' @export
filter_data_by_year <- function(data, years, year_col = "ano") {
  if (!year_col %in% names(data)) {
    stop("Column '", year_col, "' not found in data")
  }
  
  if (length(years) == 0) {
    warning("No years specified for filtering")
    return(data)
  }
  
  data[get(year_col) %in% years]
}

#' Filter Data by Municipality Codes
#' 
#' Filters data to include only specified municipalities.
#' 
#' @param data Data.table to filter
#' @param muni_codes Numeric vector of municipality codes to include
#' @param muni_col Column name containing municipality codes (default: "id_munic_7")
#' @return Filtered data.table
#' @export
filter_data_by_municipalities <- function(data, muni_codes, muni_col = "id_munic_7") {
  if (!muni_col %in% names(data)) {
    stop("Column '", muni_col, "' not found in data")
  }
  
  if (length(muni_codes) == 0) {
    warning("No municipality codes specified for filtering")
    return(data)
  }
  
  data[get(muni_col) %in% muni_codes]
}

#' Filter Data with Valid Coordinates
#' 
#' Removes records with missing or invalid latitude/longitude coordinates.
#' 
#' @param data Data.table to filter
#' @param lat_col Column name for latitude (default: "latitude")
#' @param lon_col Column name for longitude (default: "longitude")
#' @param strict Logical indicating strict validation (removes zeros and extreme values)
#' @return Filtered data.table with valid coordinates only
#' @export
filter_valid_coordinates <- function(data, lat_col = "latitude", lon_col = "longitude", strict = FALSE) {
  if (!all(c(lat_col, lon_col) %in% names(data))) {
    missing_cols <- setdiff(c(lat_col, lon_col), names(data))
    stop("Coordinate columns not found: ", paste(missing_cols, collapse = ", "))
  }
  
  # Basic filtering: remove NA values
  filtered <- data[!is.na(get(lat_col)) & !is.na(get(lon_col))]
  
  if (strict) {
    # Strict filtering: remove zeros and extreme values
    # Brazilian coordinates: lat ~= -35 to 5, lon ~= -75 to -30
    filtered <- filtered[
      get(lat_col) != 0 & get(lon_col) != 0 &
      get(lat_col) >= -35 & get(lat_col) <= 5 &
      get(lon_col) >= -75 & get(lon_col) <= -30
    ]
  }
  
  return(filtered)
}

#' Get Municipality Codes for States
#' 
#' Extracts municipality codes that belong to specified states.
#' 
#' @param muni_data Data.table with municipality information
#' @param states Character vector of state codes
#' @param state_col Column name containing state codes (default: "uf")
#' @param muni_col Column name containing municipality codes (default: "id_munic_7")
#' @return Numeric vector of municipality codes
#' @export
get_muni_codes_for_states <- function(muni_data, states, state_col = "uf", muni_col = "id_munic_7") {
  if (!all(c(state_col, muni_col) %in% names(muni_data))) {
    missing_cols <- setdiff(c(state_col, muni_col), names(muni_data))
    stop("Required columns not found: ", paste(missing_cols, collapse = ", "))
  }
  
  filtered_munis <- muni_data[get(state_col) %in% states]
  unique(filtered_munis[[muni_col]])
}

#' Filter Geographic Data by State Codes
#' 
#' Filters geographic data (like census tracts) by extracting state codes from
#' longer geographic codes and matching against specified states.
#' 
#' @param data Data.table or sf object to filter
#' @param states Character vector of 2-digit state codes
#' @param code_col Column name containing the geographic codes
#' @param code_start Starting position for state code extraction (default: 1)
#' @param code_length Length of state code (default: 2)
#' @return Filtered data
#' @export
filter_geographic_by_state <- function(data, states, code_col, code_start = 1, code_length = 2) {
  if (!code_col %in% names(data)) {
    stop("Column '", code_col, "' not found in data")
  }
  
  # Extract state codes from the geographic codes
  state_codes <- substr(as.character(data[[code_col]]), code_start, code_start + code_length - 1)
  
  # Filter by state
  data[state_codes %in% states]
}

#' Filter CNEFE Data by States
#' 
#' Specialized function for filtering CNEFE data by state codes.
#' CNEFE data typically has codes where the first 2 digits represent the state.
#' 
#' @param cnefe_data Data.table with CNEFE data
#' @param states Character vector of 2-digit state codes
#' @param setor_col Column name containing setor codes (default: "setor_code")
#' @return Filtered CNEFE data
#' @export
filter_cnefe_by_states <- function(cnefe_data, states, setor_col = "setor_code") {
  filter_geographic_by_state(cnefe_data, states, setor_col)
}

#' Apply Development Mode Configuration
#' 
#' Helper function that applies all necessary development mode filters to a dataset.
#' This is a higher-level function that combines multiple filtering operations.
#' 
#' @param data Data.table to filter
#' @param pipeline_config Pipeline configuration object
#' @param muni_ids Municipality ID reference data (optional)
#' @param filter_type Type of filtering to apply ("state", "municipality", "geographic")
#' @param ... Additional arguments passed to specific filter functions
#' @return Filtered data.table
#' @export
apply_dev_mode_filters <- function(data, pipeline_config, muni_ids = NULL, filter_type = "state", ...) {
  if (!pipeline_config$dev_mode) {
    return(data)
  }
  
  switch(filter_type,
    "state" = filter_data_by_state(data, pipeline_config$dev_states, ...),
    "municipality" = {
      if (is.null(muni_ids)) {
        stop("muni_ids required for municipality filtering")
      }
      dev_muni_codes <- get_muni_codes_for_states(muni_ids, pipeline_config$dev_states)
      filter_data_by_municipalities(data, dev_muni_codes, ...)
    },
    "geographic" = filter_geographic_by_state(data, pipeline_config$dev_states, ...),
    stop("Unknown filter_type: ", filter_type)
  )
}

#' Create State File List for Development Mode
#' 
#' Helper function to create file lists based on development mode configuration.
#' Commonly used for data files organized by state.
#' 
#' @param all_files Character vector of all available files
#' @param state_file_map Named vector mapping state codes to file names
#' @param pipeline_config Pipeline configuration object
#' @return Character vector of files to process
#' @export
create_dev_mode_file_list <- function(all_files, state_file_map, pipeline_config) {
  if (!pipeline_config$dev_mode) {
    return(all_files)
  }
  
  dev_files <- state_file_map[pipeline_config$dev_states]
  dev_files[!is.na(dev_files)]
}

#' Filter Brasília Data
#' 
#' Specialized filtering for Brasília municipality data.
#' This is a wrapper around the Brasília-specific filtering functions.
#' 
#' @param data Data.table with polling station data
#' @return Filtered data.table with Brasília-specific processing applied
#' @export
apply_brasilia_filters <- function(data) {
  # Apply Brasília filtering
  dt_filtered <- filter_brasilia_municipal_elections(data)
  
  # Generate filtering report
  report <- generate_brasilia_filtering_report(
    dt_before = data,
    dt_after = dt_filtered,
    output_file = "output/brasilia_filtering_report.rds"
  )
  
  # Validate the filtering
  validation <- validate_brasilia_filtering(dt_filtered, stop_on_failure = FALSE)
  
  if (!validation$passed) {
    warning("Brasília filtering validation failed")
  }
  
  # Add metadata
  attr(dt_filtered, "brasilia_report") <- report
  attr(dt_filtered, "brasilia_validation") <- validation
  
  return(dt_filtered)
}

# Helper function to define null coalescing operator if not available
if (!exists("%||%")) {
  `%||%` <- function(x, y) if (is.null(x)) y else x
}