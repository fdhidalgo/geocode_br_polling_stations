# Functions for state-based data filtering in the targets pipeline

library(data.table)

#' Read state-specific CNEFE files
#'
#' This function reads CNEFE data for specified states from pre-partitioned
#' compressed files and combines them into a single data.table
#'
#' @param year Year of CNEFE data (2010 or 2022)
#' @param states Character vector of state abbreviations (e.g., c("AC", "RR"))
#' @param base_dir Base directory containing CNEFE files
#' @return Combined data.table with CNEFE data for specified states
read_state_cnefe <- function(year, states, base_dir = "data") {
  # Construct directory path
  year_dir <- file.path(base_dir, paste0("cnefe_", year))
  
  # Check if directory exists
  if (!dir.exists(year_dir)) {
    stop(sprintf("CNEFE directory not found: %s\nRun partition_cnefe_by_state() first.", 
                 year_dir))
  }
  
  # Build file paths for requested states
  state_files <- paste0("cnefe_", year, "_", states, ".csv.gz")
  file_paths <- file.path(year_dir, state_files)
  
  # Check which files exist
  existing_files <- file_paths[file.exists(file_paths)]
  missing_states <- states[!file.exists(file_paths)]
  
  if (length(missing_states) > 0) {
    warning(sprintf("Files not found for states: %s", 
                    paste(missing_states, collapse = ", ")))
  }
  
  if (length(existing_files) == 0) {
    stop("No CNEFE files found for specified states")
  }
  
  message(sprintf("Reading CNEFE %d data for %d state(s): %s", 
                  year, 
                  length(existing_files),
                  paste(states[file.exists(file_paths)], collapse = ", ")))
  
  # Read and combine state files
  cnefe_list <- lapply(existing_files, function(f) {
    state <- gsub(".*_([A-Z]{2})\\.csv\\.gz$", "\\1", basename(f))
    message(sprintf("  Reading %s...", state))
    fread(f, encoding = "UTF-8", showProgress = FALSE)
  })
  
  # Combine all state data
  cnefe_combined <- rbindlist(cnefe_list, use.names = TRUE, fill = TRUE)
  
  message(sprintf("Total rows read: %s", 
                  format(nrow(cnefe_combined), big.mark = ",")))
  
  return(cnefe_combined)
}

#' Get pipeline configuration based on development mode
#'
#' @param dev_mode Logical indicating development mode
#' @return List with configuration settings
get_pipeline_config <- function(dev_mode = FALSE) {
  list(
    # Development mode flag
    dev_mode = dev_mode,
    
    # Small states for rapid development (reduced to 2 for faster testing)
    dev_states = c("AC", "RR"),
    
    # All Brazilian states
    prod_states = c("AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", 
                    "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", 
                    "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", 
                    "SP", "SE", "TO"),
    
    # Memory limits by mode
    memory_limit = ifelse(dev_mode, "2GB", "16GB"),
    
    # Number of parallel workers
    n_workers = ifelse(dev_mode, 2, 4),
    
    # Memory-efficient string matching configuration
    use_memory_efficient = TRUE,  # Enable by default to prevent OOM
    memory_efficient_threshold_mb = 500,  # Use memory-efficient mode for matrices > 500MB
    chunk_sizes = list(
      small = 2000,   # For municipalities < 5000 polling stations
      medium = 1000,  # For municipalities 5000-10000
      large = 500     # For municipalities > 10000
    )
  )
}

#' Filter other data sources by state
#'
#' Helper function to filter non-CNEFE data sources to match selected states
#'
#' @param data Data.table to filter
#' @param states Character vector of state abbreviations
#' @param state_col Name of the state column (default "sg_uf")
#' @return Filtered data.table
filter_data_by_state <- function(data, states, state_col = "sg_uf") {
  if (!state_col %in% names(data)) {
    warning(sprintf("State column '%s' not found in data. Returning unfiltered data.", 
                    state_col))
    return(data)
  }
  
  # Filter to selected states
  filtered <- data[get(state_col) %in% states]
  
  message(sprintf("Filtered from %s to %s rows (%d states)", 
                  format(nrow(data), big.mark = ","),
                  format(nrow(filtered), big.mark = ","),
                  length(states)))
  
  return(filtered)
}

#' Get municipality codes for selected states
#'
#' @param muni_ids Municipality identifiers table
#' @param states Character vector of state abbreviations
#' @return Vector of municipality codes
get_muni_codes_for_states <- function(muni_ids, states) {
  muni_codes <- muni_ids[estado_abrev %in% states, id_munic_7]
  
  message(sprintf("Found %d municipalities in %d states", 
                  length(muni_codes), 
                  length(states)))
  
  return(muni_codes)
}

#' Get states to process based on context and mode
#' 
#' Centralized function to determine which states to process based on the
#' pipeline context and development mode setting. This replaces repetitive
#' if/else blocks throughout the pipeline.
#' 
#' @param context Character string indicating the context (e.g., "cnefe10", "cnefe22", "panel")
#' @param pipeline_config Pipeline configuration object with dev_mode and dev_states
#' @param custom_states Optional custom state list for production mode (used for panel context)
#' @return Character vector of state abbreviations
#' @export
get_states_for_processing <- function(context, pipeline_config, custom_states = NULL) {
  if (pipeline_config$dev_mode) {
    return(pipeline_config$dev_states)
  }
  
  # Production mode - determine states based on context
  switch(context,
    cnefe10 = {
      state_files <- list.files("data/cnefe_2010", pattern = "cnefe_2010_.*\\.csv\\.gz$")
      gsub("cnefe_2010_(.+)\\.csv\\.gz", "\\1", state_files)
    },
    cnefe22 = {
      state_files <- list.files("data/cnefe_2022", pattern = "cnefe_2022_.*\\.csv\\.gz$")
      gsub("cnefe_2022_(.+)\\.csv\\.gz", "\\1", state_files)
    },
    panel = {
      # Use custom states or default to all Brazilian states
      custom_states %||% c("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", 
                          "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", 
                          "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", 
                          "SE", "SP", "TO")
    },
    stop("Unknown context: ", context)
  )
}

#' Get agro CNEFE files based on mode
#' 
#' Special handler for agro_cnefe_files which returns file paths not states.
#' In dev mode, maps state abbreviations to specific file names.
#' In production mode, returns all files in the agro_censo directory.
#' 
#' @param pipeline_config Pipeline configuration object with dev_mode and dev_states
#' @return Character vector of file paths
#' @export
get_agro_cnefe_files <- function(pipeline_config) {
  if (pipeline_config$dev_mode) {
    state_file_map <- c(
      "AC" = "12_ACRE.csv.gz",
      "RR" = "14_RORAIMA.csv.gz",
      "AP" = "16_AMAPA.csv.gz",
      "RO" = "11_RONDONIA.csv.gz"
    )
    dev_files <- state_file_map[pipeline_config$dev_states]
    file.path("data/agro_censo", dev_files[!is.na(dev_files)])
  } else {
    dir("data/agro_censo/", full.names = TRUE)
  }
}