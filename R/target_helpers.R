#' Target Helper Functions
#'
#' This file contains helper functions to reduce repetition in targets pipeline
#' and make the _targets.R file more concise and readable.

#' Filter data by development mode
#'
#' Generic function to filter data based on development mode settings
#' @param data The data to filter
#' @param config Pipeline configuration list
#' @param filter_col Column to filter on (default: "estado_abrev")
#' @return Filtered data
#' @export
filter_by_dev_mode <- function(data, config, filter_col = "estado_abrev") {
  if (!config$dev_mode) {
    return(data)
  }
  
  if (filter_col %in% names(data)) {
    return(data[get(filter_col) %in% config$dev_states])
  } else {
    # If column doesn't exist, return data unchanged with warning
    warning(paste("Column", filter_col, "not found in data. Returning unfiltered."))
    return(data)
  }
}

#' Process string match batch
#'
#' Generic function to process string matching in batches
#' @param batch_ids Current batch ID being processed
#' @param batch_assignments Municipality batch assignments table
#' @param locais_data Filtered polling stations data
#' @param match_data Data to match against (e.g., INEP, CNEFE)
#' @param match_fn Matching function to use
#' @param id_col Column name for municipality ID (default: "id_munic_7")
#' @return Combined match results for the batch
#' @export
process_string_match_batch <- function(batch_ids, batch_assignments, locais_data, 
                                     match_data, match_fn, id_col = "id_munic_7") {
  # Get municipalities for this batch
  batch_munis <- batch_assignments[batch_id == batch_ids]$muni_code
  
  # Process all municipalities in this batch
  batch_results <- lapply(batch_munis, function(muni_code) {
    # Special handling for different matching functions
    if (deparse(substitute(match_fn)) == "match_inep_muni") {
      match_fn(
        locais_muni = locais_data[cod_localidade_ibge == muni_code],
        inep_muni = match_data[get(id_col) == muni_code]
      )
    } else if (deparse(substitute(match_fn)) == "match_schools_cnefe_muni") {
      match_fn(
        locais_muni = locais_data[cod_localidade_ibge == muni_code],
        schools_cnefe_muni = match_data[get(id_col) == muni_code]
      )
    } else if (deparse(substitute(match_fn)) == "match_geocodebr_muni") {
      match_fn(
        locais_muni = locais_data[cod_localidade_ibge == muni_code],
        muni_ids = match_data[get(id_col) == muni_code]
      )
    } else {
      # Generic case - assumes standard interface
      match_fn(
        locais_muni = locais_data[cod_localidade_ibge == muni_code],
        match_muni = match_data[get(id_col) == muni_code]
      )
    }
  })
  
  # Remove NULL results and combine
  batch_results <- batch_results[!sapply(batch_results, is.null)]
  if (length(batch_results) > 0) {
    rbindlist(batch_results, use.names = TRUE, fill = TRUE)
  } else {
    data.table()
  }
}

#' Process street/neighborhood match batch
#'
#' Specialized function for street and neighborhood matching
#' @param batch_ids Current batch ID
#' @param batch_assignments Municipality batch assignments
#' @param locais_data Filtered polling stations
#' @param st_data Street-level data
#' @param bairro_data Neighborhood-level data
#' @param match_fn Matching function (e.g., match_stbairro_cnefe_muni)
#' @return Combined match results
#' @export
process_stbairro_match_batch <- function(batch_ids, batch_assignments, locais_data,
                                       st_data, bairro_data, match_fn) {
  # Get municipalities for this batch
  batch_munis <- batch_assignments[batch_id == batch_ids]$muni_code
  
  # Process all municipalities in this batch
  batch_results <- lapply(batch_munis, function(muni_code) {
    if (deparse(substitute(match_fn)) == "match_stbairro_agrocnefe_muni") {
      match_fn(
        locais_muni = locais_data[cod_localidade_ibge == muni_code],
        agrocnefe_st_muni = st_data[id_munic_7 == muni_code],
        agrocnefe_bairro_muni = bairro_data[id_munic_7 == muni_code]
      )
    } else {
      match_fn(
        locais_muni = locais_data[cod_localidade_ibge == muni_code],
        cnefe_st_muni = st_data[id_munic_7 == muni_code],
        cnefe_bairro_muni = bairro_data[id_munic_7 == muni_code]
      )
    }
  })
  
  # Remove NULL results and combine
  batch_results <- batch_results[!sapply(batch_results, is.null)]
  if (length(batch_results) > 0) {
    rbindlist(batch_results, use.names = TRUE, fill = TRUE)
  } else {
    data.table()
  }
}

#' Process CNEFE by state
#'
#' Generic function to process CNEFE data state by state
#' @param state Current state being processed
#' @param year CNEFE year (2010 or 2022)
#' @param muni_ids Municipality identifiers
#' @param tract_centroids Tract centroids (for 2010 only)
#' @param extract_schools Whether to extract schools (2010 only)
#' @return Cleaned CNEFE data for the state
#' @export
process_cnefe_state <- function(state, year, muni_ids, tract_centroids = NULL, 
                              extract_schools = FALSE) {
  # Construct file path
  state_file <- file.path(
    "data",
    paste0("cnefe_", year),
    paste0("cnefe_", year, "_", state, ".csv.gz")
  )
  
  # Get municipality IDs for this state
  state_muni_ids <- muni_ids[estado_abrev == state]
  
  if (year == 2010) {
    # Read state data
    state_data <- fread(
      state_file,
      sep = ",",
      encoding = "UTF-8",
      verbose = FALSE,
      showProgress = FALSE
    )
    
    # Get tract centroids for this state
    state_codes <- unique(substr(
      as.character(state_muni_ids$id_munic_7),
      1, 2
    ))
    state_tract_centroids <- tract_centroids[
      substr(setor_code, 1, 2) %in% state_codes
    ]
    
    # Clean the data
    result <- clean_cnefe10(
      cnefe_file = state_data,
      muni_ids = state_muni_ids,
      tract_centroids = state_tract_centroids,
      extract_schools = extract_schools
    )
    
    # Force garbage collection
    gc(verbose = FALSE)
    
    # Return based on what was requested
    if (extract_schools) {
      return(result$schools)
    } else {
      return(result$data)
    }
  } else {
    # CNEFE 2022 processing
    result <- clean_cnefe22(
      cnefe22_file = state_file,
      muni_ids = state_muni_ids
    )
    
    gc(verbose = FALSE)
    return(result)
  }
}

#' Create validation summary
#'
#' Helper to create consistent validation summaries
#' @param validation_results List of validation results
#' @param pipeline_config Pipeline configuration
#' @return Summary statistics list
#' @export
create_validation_summary <- function(validation_results, pipeline_config) {
  summary_stats <- list(
    timestamp = Sys.time(),
    pipeline_version = "1.0.0",
    total_validations = length(validation_results),
    passed = sum(sapply(validation_results, function(x) x$passed)),
    failed = sum(sapply(validation_results, function(x) !x$passed)),
    dev_mode = pipeline_config$dev_mode,
    stage_summary = sapply(validation_results, function(x) {
      list(
        passed = x$passed,
        type = x$metadata$type,
        n_rows = x$metadata$n_rows %||% NA
      )
    })
  )
  
  summary_stats
}