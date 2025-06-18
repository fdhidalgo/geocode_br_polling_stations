# Pipeline configuration functions used in _targets.R

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