## Utility Functions
##
## Helper functions for common operations throughout the pipeline:
## - State/municipality filtering for development mode
## - File I/O operations with compression support
## - Parallel processing configuration
## - Memory monitoring and management
## - Data export utilities

library(data.table)

# ===== FILTERING HELPERS =====
# Functions to subset data for development mode testing

# Define the null coalescing operator
`%||%` <- function(x, y) if (is.null(x)) y else x

filter_by_dev_mode <- function(data, dev_states, id_column = "estado_abrev") {
  # Filter data by development mode states.
  # If dev_states is NULL or empty (production), return all data.
  if (is.null(dev_states) || length(dev_states) == 0) {
    return(data)
  }

  # Fail loud when the filter column is absent (cleanup phase 3, finding H2):
  # silently returning unfiltered data would run the full pipeline in dev mode.
  if (!id_column %in% names(data)) {
    stop(sprintf("filter_by_dev_mode(): column '%s' not found in data.", id_column))
  }

  data[get(id_column) %in% dev_states]
}

filter_data_by_state <- function(data, states, state_col = "estado_abrev") {
  # Generic function to filter any data by state (data.table or data.frame).
  if (is.null(states) || length(states) == 0) {
    return(data)
  }

  # Fail loud when the state column is absent (cleanup phase 3, finding H2).
  if (!state_col %in% names(data)) {
    stop(sprintf("filter_data_by_state(): state column '%s' not found in data.", state_col))
  }

  if (is.data.table(data)) {
    data[get(state_col) %in% states]
  } else {
    data[data[[state_col]] %in% states, ]
  }
}

filter_data_by_municipalities <- function(data, muni_codes, muni_col = "id_munic_7") {
  # Filter data by municipality codes.
  if (is.null(muni_codes) || length(muni_codes) == 0) {
    return(data)
  }

  # Fail loud when the named column is absent (cleanup phase 3, finding H2). The
  # former four-way probing of alternative ID columns could filter on the wrong
  # ID system; the caller must name the column it means.
  if (!muni_col %in% names(data)) {
    stop(sprintf("filter_data_by_municipalities(): municipality column '%s' not found in data.", muni_col))
  }

  if (is.data.table(data)) {
    data[get(muni_col) %in% muni_codes]
  } else {
    data[data[[muni_col]] %in% muni_codes, ]
  }
}

apply_dev_mode_filters <- function(data, config, filter_type = NULL, state_col = NULL, muni_col = NULL) {
  # Apply development mode filters based on configuration
  # Supports both old and new call signatures for backward compatibility
  
  # Handle old-style calls with named parameters
  if (!is.null(filter_type)) {
    if (filter_type == "state" && !is.null(state_col)) {
      # Old-style state filtering
      if (!is.null(config$dev_states)) {
        return(filter_data_by_state(data, config$dev_states, state_col))
      } else {
        return(data)
      }
    } else if (filter_type == "municipality" && !is.null(muni_col)) {
      # Municipality-based filtering for development mode
      # Filter by municipalities from development states only
      if (config$dev_mode && !is.null(config$dev_states)) {
        # For municipality filtering in dev mode, we need to filter by state
        # since we don't have a predefined list of municipalities
        if ("estado_abrev" %in% names(data)) {
          return(filter_data_by_state(data, config$dev_states, "estado_abrev"))
        } else if ("sg_uf" %in% names(data)) {
          return(filter_data_by_state(data, config$dev_states, "sg_uf"))
        }
      }
      return(data)
    }
  }
  
  # Apply filtering based on configuration
  if (!is.null(config$dev_states)) {
    data <- filter_data_by_state(data, config$dev_states)
  }
  
  if (!is.null(config$dev_municipalities)) {
    data <- filter_data_by_municipalities(data, config$dev_municipalities)
  }
  
  return(data)
}

apply_brasilia_filters <- function(data, state_col = "sg_uf", remove_brasilia = TRUE) {
  # Apply special filtering for Brasília (DF), which had municipal elections in
  # years that differ from other states.
  if (!remove_brasilia) {
    return(data)
  }

  # Fail loud when the named state column is absent (cleanup phase 3, finding
  # H2). The former stacked fallbacks (four state-column names, then three
  # municipality-code columns filtering on a "^53" prefix, then a warn-and-pass)
  # could silently skip the filter or use the wrong column; the caller names it.
  if (!state_col %in% names(data)) {
    stop(sprintf("apply_brasilia_filters(): state column '%s' not found in data.", state_col))
  }

  if (is.data.table(data)) {
    data[get(state_col) != "DF"]
  } else {
    data[data[[state_col]] != "DF", ]
  }
}

# ===== PIPELINE HELPERS =====

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
  batch_info <- batch_assignments[batch_id == batch_ids]
  batch_munis <- batch_info$muni_code

  # Log batch start
  message(sprintf(
    "[%s] Processing string match batch %s: %d municipalities",
    format(Sys.time(), "%H:%M:%S"),
    batch_ids,
    length(batch_munis)
  ))

  # Process all municipalities in this batch
  batch_results <- lapply(seq_along(batch_munis), function(i) {
    muni_code <- batch_munis[i]

    # Log progress for large batches
    if (length(batch_munis) > 5 && i %% 5 == 0) {
      message(sprintf(
        "  - Batch %s progress: %d/%d municipalities",
        batch_ids,
        i,
        length(batch_munis)
      ))
    }
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
  batch_info <- batch_assignments[batch_id == batch_ids]
  batch_munis <- batch_info$muni_code

  # Log batch start with size info
  total_size <- sum(batch_info$muni_size)
  message(sprintf(
    "[%s] Processing stbairro match batch %s: %d municipalities, %s total items",
    format(Sys.time(), "%H:%M:%S"),
    batch_ids,
    length(batch_munis),
    format(total_size, big.mark = ",")
  ))

  # Process all municipalities in this batch
  batch_results <- lapply(seq_along(batch_munis), function(i) {
    muni_code <- batch_munis[i]

    # Log progress
    if (i %% 2 == 0 || i == length(batch_munis)) {
      message(sprintf(
        "  - Batch %s: processing municipality %d/%d (code: %s)",
        batch_ids,
        i,
        length(batch_munis),
        muni_code
      ))
    }
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

    # Return based on what was requested
    if (extract_schools) {
      return(result$schools)
    } else {
      return(result)
    }
  } else {
    # CNEFE 2022 processing
    result <- clean_cnefe22(
      cnefe22_file = state_file,
      muni_ids = state_muni_ids
    )

    return(result)
  }
}

#' Process INEP string matching in batches
#'
#' @param batch_ids Current batch ID
#' @param municipality_batch_assignments Batch assignments
#' @param locais_filtered Filtered polling stations
#' @param inep_data INEP data
#' @return Combined match results
#' @export
process_inep_batch <- function(batch_ids, municipality_batch_assignments,
                               locais_filtered, inep_data) {
  # Get municipalities for this batch
  batch_munis <- municipality_batch_assignments[
    batch_id == batch_ids
  ]$cod_localidade_ibge

  # Process all municipalities in this batch
  batch_results <- lapply(batch_munis, function(muni_code) {
    match_inep_muni(
      locais_muni = locais_filtered[cod_localidade_ibge == muni_code],
      inep_muni = inep_data[id_munic_7 == muni_code]
    )
  })

  # Remove NULL results and combine
  batch_results <- batch_results[!sapply(batch_results, is.null)]
  if (length(batch_results) > 0) {
    rbindlist(batch_results, use.names = TRUE, fill = TRUE)
  } else {
    data.table()
  }
}

#' Process schools CNEFE string matching in batches
#'
#' @param batch_ids Current batch ID
#' @param municipality_batch_assignments Batch assignments
#' @param locais_filtered Filtered polling stations
#' @param schools_cnefe Schools CNEFE data
#' @return Combined match results
#' @export
process_schools_cnefe_batch <- function(batch_ids, municipality_batch_assignments,
                                        locais_filtered, schools_cnefe) {
  # Get municipalities for this batch
  batch_munis <- municipality_batch_assignments[
    batch_id == batch_ids
  ]$cod_localidade_ibge

  # Process all municipalities in this batch
  batch_results <- lapply(batch_munis, function(muni_code) {
    match_schools_cnefe_muni(
      locais_muni = locais_filtered[cod_localidade_ibge == muni_code],
      schools_cnefe_muni = schools_cnefe[id_munic_7 == muni_code]
    )
  })

  # Remove NULL results and combine
  batch_results <- batch_results[!sapply(batch_results, is.null)]
  if (length(batch_results) > 0) {
    rbindlist(batch_results, use.names = TRUE, fill = TRUE)
  } else {
    data.table()
  }
}

#' Process GeocodeR string matching in batches
#'
#' @param batch_ids Current batch ID
#' @param municipality_batch_assignments Batch assignments
#' @param locais_filtered Filtered polling stations
#' @param muni_ids Municipality IDs data
#' @return Combined match results
#' @export
process_geocodebr_batch <- function(batch_ids, municipality_batch_assignments,
                                    locais_filtered, muni_ids) {
  # Get municipalities for this batch
  batch_munis <- municipality_batch_assignments[
    batch_id == batch_ids
  ]$cod_localidade_ibge

  # Collect-and-stop (cleanup phase 3, finding C5): a municipality that errors is
  # recorded and the batch continues; at batch end any accumulated failures raise
  # a single error naming every failing municipality, so no municipality is ever
  # silently dropped from geocodebr coverage. A NULL result (no polling stations
  # or no geocoding hits) is a legitimate empty case and is filtered, not failed.
  results <- lapply(batch_munis, function(muni_code) {
    tryCatch(
      match_geocodebr_muni(
        locais_muni = locais_filtered[cod_localidade_ibge == muni_code],
        muni_ids = muni_ids[id_munic_7 == muni_code]
      ),
      error = function(e) {
        structure(
          list(muni_code = muni_code, message = conditionMessage(e)),
          class = "geocodebr_muni_failure"
        )
      }
    )
  })

  failures <- Filter(function(x) inherits(x, "geocodebr_muni_failure"), results)
  if (length(failures) > 0) {
    msgs <- vapply(
      failures,
      function(f) sprintf("  %s: %s", f$muni_code, f$message),
      character(1)
    )
    stop(sprintf(
      "geocodebr matching failed for %d municipalit%s:\n%s",
      length(failures),
      if (length(failures) == 1L) "y" else "ies",
      paste(msgs, collapse = "\n")
    ))
  }

  batch_results <- Filter(Negate(is.null), results)
  if (length(batch_results) > 0) {
    rbindlist(batch_results, use.names = TRUE, fill = TRUE)
  } else {
    data.table()
  }
}

#' Process CNEFE street/neighborhood matching in batches
#'
#' @param batch_ids Current batch ID
#' @param municipality_batch_assignments Batch assignments
#' @param locais_filtered Filtered polling stations
#' @param cnefe_st Street-level CNEFE data
#' @param cnefe_bairro Neighborhood-level CNEFE data
#' @return Combined match results
#' @export
process_cnefe_stbairro_batch <- function(batch_ids, municipality_batch_assignments,
                                         locais_filtered, cnefe_st, cnefe_bairro) {
  # Get municipalities for this batch
  batch_munis <- municipality_batch_assignments[
    batch_id == batch_ids
  ]$cod_localidade_ibge

  # Log batch start
  message(sprintf(
    "[Batch %d] Starting CNEFE street/neighborhood matching for %d municipalities",
    batch_ids, length(batch_munis)
  ))

  # Process all municipalities in this batch with progress tracking
  batch_results <- lapply(seq_along(batch_munis), function(i) {
    muni_code <- batch_munis[i]

    # Get data sizes for logging
    n_locais <- nrow(locais_filtered[cod_localidade_ibge == muni_code])
    n_streets <- nrow(cnefe_st[id_munic_7 == muni_code])
    n_bairros <- nrow(cnefe_bairro[id_munic_7 == muni_code])

    message(sprintf(
      "[Batch %d - %d/%d] Processing municipality %s: %d polling stations, %d streets, %d neighborhoods",
      batch_ids, i, length(batch_munis), muni_code, n_locais, n_streets, n_bairros
    ))

    # Perform matching
    result <- match_stbairro_cnefe_muni(
      locais_muni = locais_filtered[cod_localidade_ibge == muni_code],
      cnefe_st_muni = cnefe_st[id_munic_7 == muni_code],
      cnefe_bairro_muni = cnefe_bairro[id_munic_7 == muni_code]
    )

    # Log completion
    if (!is.null(result)) {
      message(sprintf(
        "[Batch %d - %d/%d] Completed municipality %s: %d matches",
        batch_ids, i, length(batch_munis), muni_code, nrow(result)
      ))
    }

    result
  })

  # Remove NULL results and combine
  batch_results <- batch_results[!sapply(batch_results, is.null)]

  # Log batch completion
  total_matches <- if (length(batch_results) > 0) {
    sum(sapply(batch_results, nrow))
  } else {
    0
  }

  message(sprintf(
    "[Batch %d] Completed with %d total matches from %d municipalities",
    batch_ids, total_matches, length(batch_results)
  ))

  if (length(batch_results) > 0) {
    rbindlist(batch_results, use.names = TRUE, fill = TRUE)
  } else {
    data.table()
  }
}

#' Process Agro CNEFE street/neighborhood matching in batches
#'
#' @param batch_ids Current batch ID
#' @param municipality_batch_assignments Batch assignments
#' @param locais_filtered Filtered polling stations
#' @param agrocnefe_st Street-level Agro CNEFE data
#' @param agrocnefe_bairro Neighborhood-level Agro CNEFE data
#' @return Combined match results
#' @export
process_agrocnefe_stbairro_batch <- function(batch_ids, municipality_batch_assignments,
                                             locais_filtered, agrocnefe_st, agrocnefe_bairro) {
  # Get municipalities for this batch
  batch_munis <- municipality_batch_assignments[
    batch_id == batch_ids
  ]$cod_localidade_ibge

  # Log batch start
  message(sprintf(
    "[Batch %d] Starting Agro CNEFE street/neighborhood matching for %d municipalities",
    batch_ids, length(batch_munis)
  ))

  # Process all municipalities in this batch with progress tracking
  batch_results <- lapply(seq_along(batch_munis), function(i) {
    muni_code <- batch_munis[i]

    # Get data sizes for logging
    n_locais <- nrow(locais_filtered[cod_localidade_ibge == muni_code])
    n_streets <- nrow(agrocnefe_st[id_munic_7 == muni_code])
    n_bairros <- nrow(agrocnefe_bairro[id_munic_7 == muni_code])

    message(sprintf(
      "[Batch %d - %d/%d] Processing municipality %s: %d polling stations, %d streets, %d neighborhoods",
      batch_ids, i, length(batch_munis), muni_code, n_locais, n_streets, n_bairros
    ))

    # Perform matching
    result <- match_stbairro_agrocnefe_muni(
      locais_muni = locais_filtered[cod_localidade_ibge == muni_code],
      agrocnefe_st_muni = agrocnefe_st[id_munic_7 == muni_code],
      agrocnefe_bairro_muni = agrocnefe_bairro[id_munic_7 == muni_code]
    )

    # Log completion
    if (!is.null(result)) {
      message(sprintf(
        "[Batch %d - %d/%d] Completed municipality %s: %d matches",
        batch_ids, i, length(batch_munis), muni_code, nrow(result)
      ))
    }

    result
  })

  # Remove NULL results and combine
  batch_results <- batch_results[!sapply(batch_results, is.null)]

  # Log batch completion
  total_matches <- if (length(batch_results) > 0) {
    sum(sapply(batch_results, nrow))
  } else {
    0
  }

  message(sprintf(
    "[Batch %d] Completed with %d total matches from %d municipalities",
    batch_ids, total_matches, length(batch_results)
  ))

  if (length(batch_results) > 0) {
    rbindlist(batch_results, use.names = TRUE, fill = TRUE)
  } else {
    data.table()
  }
}
# ===== PARALLEL PROCESSING =====

#' Create municipality batch assignments for balanced parallel processing
#'
#' This function creates balanced batch assignments for municipality codes to reduce
#' the number of dynamic branches in the targets pipeline. It assigns each municipality
#' to a specific batch number, which helps prevent bottlenecks when crew dispatches
#' thousands of fine-grained tasks.
#'
#' Unlike create_municipality_batches (which returns a list of batches), this function
#' returns a data.table mapping each municipality code to its batch number. This is
#' more efficient for joining operations in the pipeline.
#'
#' @param muni_codes Vector of municipality codes to batch
#' @param batch_size Target size for each batch (default: 50)
#' @param muni_sizes Optional named vector of municipality sizes for load balancing
#' @return data.table with columns: cod_localidade_ibge (municipality code) and batch_id
#' @examples
#' assignments <- create_municipality_batch_assignments(unique(locais$cod_localidade_ibge))
#' @export
create_municipality_batch_assignments <- function(muni_codes, batch_size = 50, muni_sizes = NULL) {
  n_munis <- length(muni_codes)
  
  # Simple sequential batching if no size information provided
  if (is.null(muni_sizes)) {
    n_batches <- ceiling(n_munis / batch_size)
    batch_nums <- rep(seq_len(n_batches), each = batch_size, length.out = n_munis)
    
    result <- data.table::data.table(
      cod_localidade_ibge = muni_codes,
      batch_id = batch_nums
    )
  } else {
    # Load-balanced batching based on municipality sizes
    # Handle both data.table and vector inputs for muni_sizes
    if (inherits(muni_sizes, "data.table")) {
      # If muni_sizes is a data.table, join properly
      muni_df <- data.table::data.table(
        cod_localidade_ibge = muni_codes
      )
      # Join with the sizes data
      muni_df <- merge(muni_df, muni_sizes, 
                       by.x = "cod_localidade_ibge", 
                       by.y = "muni_code", 
                       all.x = TRUE)
    } else {
      # If muni_sizes is a named vector
      muni_df <- data.table::data.table(
        cod_localidade_ibge = muni_codes,
        size = muni_sizes[as.character(muni_codes)]
      )
    }
    # Fail loud on a municipality with no size entry (cleanup phase 3, Medium):
    # median-imputing a missing size masks a municipality-code key mismatch
    # between muni_codes and muni_sizes.
    missing_size <- muni_df[is.na(size), cod_localidade_ibge]
    if (length(missing_size) > 0) {
      stop(sprintf(
        "Municipality sizes missing for %d municipalit%s (key mismatch): %s",
        length(missing_size),
        if (length(missing_size) == 1L) "y" else "ies",
        paste(utils::head(missing_size, 10), collapse = ", ")
      ))
    }
    data.table::setorder(muni_df, -size)
    
    # Assign to batches using round-robin for load balancing
    n_batches <- ceiling(n_munis / batch_size)
    muni_df[, batch_id := rep_len(seq_len(n_batches), .N)]
    
    result <- muni_df[, .(cod_localidade_ibge, batch_id)]
  }
  
  # Log batch statistics
  batch_stats <- result[, .N, by = batch_id]
  message(sprintf(
    "Created %d batches for %d municipalities (min: %d, max: %d, avg: %.1f per batch)",
    length(unique(result$batch_id)), 
    n_munis,
    min(batch_stats$N),
    max(batch_stats$N),
    mean(batch_stats$N)
  ))
  
  result
}

# ===== DATA EXPORT FUNCTIONS =====
# These functions were moved from data_export.R

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