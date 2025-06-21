#' Target Helper Functions
#'
#' This file contains helper functions to reduce repetition in targets pipeline
#' and make the _targets.R file more concise and readable.

#' Get crew controllers for the pipeline
#'
#' Creates and configures crew controllers based on development mode
#' @param dev_mode Logical indicating if in development mode
#' @return crew controller group
#' @export
# Note: 1 unused functions were moved to backup/unused_functions/
# Date: 2025-06-20
# Functions removed: create_validation_summary

get_crew_controllers <- function(dev_mode = FALSE) {
  # Standard controller for most tasks - optimized for 32-core machine
  controller_standard <- crew::crew_controller_local(
    name = "standard",
    workers = if (dev_mode) 8 else 28,
    seconds_idle = 30,
    seconds_wall = 3600,
    seconds_timeout = 300,
    reset_globals = TRUE,
    reset_packages = FALSE,
    garbage_collection = TRUE
  )

  # Memory-limited controller for CNEFE operations
  # Fewer workers but more memory per worker
  controller_memory <- crew::crew_controller_local(
    name = "memory_limited",
    workers = if (dev_mode) 4 else 8,
    seconds_idle = 60,
    seconds_wall = 7200, # 2 hours for memory-intensive tasks
    seconds_timeout = 600, # 10 minutes timeout
    reset_globals = TRUE,
    reset_packages = FALSE,
    garbage_collection = TRUE
  )

  # Return controller group
  crew::crew_controller_group(
    controller_standard,
    controller_memory
  )
}

#' Configure targets options with crew controller
#'
#' Sets up targets options with the provided controller
#' @param controller_group Crew controller group
#' @export
configure_targets_options <- function(controller_group) {
  tar_option_set(
    packages = c("data.table", "stringr", "stringdist", "validate", "sf", "reclin2"),
    format = "qs",
    controller = controller_group,
    storage = "worker",
    retrieval = "worker",
    memory = "transient",
    garbage_collection = TRUE,
    resources = tar_resources(
      crew = tar_resources_crew(controller = "standard") # Default to standard controller
    )
  )
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
  ]$muni_code

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
  ]$muni_code

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
  ]$muni_code

  # Process all municipalities in this batch
  batch_results <- lapply(batch_munis, function(muni_code) {
    match_geocodebr_muni(
      locais_muni = locais_filtered[cod_localidade_ibge == muni_code],
      muni_ids = muni_ids[id_munic_7 == muni_code]
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
  ]$muni_code

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
  ]$muni_code

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
