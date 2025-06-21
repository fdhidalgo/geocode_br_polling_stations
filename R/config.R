## Configuration Functions
##
## This file consolidates configuration functions from:
## - pipeline_config.R (3 functions)
## - expected_municipality_counts.R (2 functions)
## - target_helpers.R (2 config functions)
##
## Total functions: 7

library(data.table)
library(crew)

# ===== PIPELINE CONFIGURATION =====

get_pipeline_config <- function(dev_mode = FALSE) {
  # Get pipeline configuration based on development mode
  
  if (dev_mode) {
    config <- list(
      dev_mode = TRUE,
      dev_states = c("AC", "RR"),  # Small states for testing
      batch_size = 10,
      max_workers = 2,
      use_memory_efficient = TRUE,
      cache_dir = "_targets/cache",
      log_level = "DEBUG"
    )
  } else {
    config <- list(
      dev_mode = FALSE,
      dev_states = NULL,  # Process all states
      batch_size = 100,
      max_workers = parallel::detectCores() - 1,
      use_memory_efficient = TRUE,
      cache_dir = "_targets/cache",
      log_level = "INFO"
    )
  }
  
  # Add computed values
  config$n_cores <- config$max_workers
  config$states <- if (is.null(config$dev_states)) {
    c("AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA",
      "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN",
      "RO", "RR", "RS", "SC", "SE", "SP", "TO")
  } else {
    config$dev_states
  }
  
  return(config)
}

get_states_for_processing <- function(config) {
  # Get list of states to process based on configuration
  
  if (is.null(config)) {
    config <- get_pipeline_config()
  }
  
  return(config$states)
}

get_agro_cnefe_files <- function(states = NULL) {
  # Get list of agro CNEFE files for specified states
  
  if (is.null(states)) {
    # Get all available files
    pattern <- "*.csv.gz"
  } else {
    # Create pattern for specific states
    state_patterns <- paste0("(", paste(states, collapse = "|"), ")\\.")
    pattern <- paste0(state_patterns, "csv.gz$")
  }
  
  # Find files in agro_censo directory
  files <- list.files(
    path = "data/agro_censo",
    pattern = pattern,
    full.names = TRUE,
    ignore.case = TRUE
  )
  
  return(files)
}

# ===== EXPECTED MUNICIPALITY COUNTS =====

get_expected_municipality_count <- function(state_abbrev) {
  # Expected number of municipalities per state (2022 data)
  # Source: IBGE
  
  expected_counts <- list(
    AC = 22,
    AL = 102,
    AM = 62,
    AP = 16,
    BA = 417,
    CE = 184,
    DF = 1,
    ES = 78,
    GO = 246,
    MA = 217,
    MG = 853,
    MS = 79,
    MT = 141,
    PA = 144,
    PB = 223,
    PE = 185,
    PI = 224,
    PR = 399,
    RJ = 92,
    RN = 167,
    RO = 52,
    RR = 15,
    RS = 497,
    SC = 295,
    SE = 75,
    SP = 645,
    TO = 139
  )
  
  count <- expected_counts[[state_abbrev]]
  
  if (is.null(count)) {
    warning(paste("Unknown state abbreviation:", state_abbrev))
    return(NA)
  }
  
  return(count)
}

get_expected_municipality_range <- function(state_abbrev, tolerance = 0.05) {
  # Get expected range of municipalities allowing for some tolerance
  # Useful for validation as municipality counts can change slightly
  
  expected <- get_expected_municipality_count(state_abbrev)
  
  if (is.na(expected)) {
    return(list(min = NA, max = NA, expected = NA))
  }
  
  # Calculate range with tolerance
  min_count <- floor(expected * (1 - tolerance))
  max_count <- ceiling(expected * (1 + tolerance))
  
  return(list(
    min = min_count,
    max = max_count,
    expected = expected
  ))
}

# ===== CREW CONTROLLER CONFIGURATION =====

get_crew_controllers <- function(dev_mode = FALSE) {
  # Create crew controller group for parallel processing
  
  # Get configuration
  config <- get_pipeline_config(dev_mode)
  
  # Create controllers for different task types
  controller_io <- crew_controller_local(
    name = "io",
    workers = min(2, config$max_workers),
    seconds_idle = 30
  )
  
  controller_compute <- crew_controller_local(
    name = "compute",
    workers = config$max_workers,
    seconds_idle = 60
  )
  
  controller_memory <- crew_controller_local(
    name = "memory",
    workers = max(1, config$max_workers / 2),
    seconds_idle = 30
  )
  
  # Create controller group
  controller_group <- crew_controller_group(
    controller_io,
    controller_compute,
    controller_memory
  )
  
  return(controller_group)
}

configure_targets_options <- function(controller_group) {
  # Configure targets options with the controller group
  
  tar_option_set(
    packages = c(
      "data.table",
      "stringr",
      "stringdist",
      "sf",
      "janitor",
      "reclin2",
      "future",
      "lightgbm"
    ),
    format = "qs",
    controller = controller_group,
    memory = "transient",
    garbage_collection = TRUE,
    deployment = "worker",
    # Set a reasonable timeout for workers
    seconds_timeout = 3600  # 1 hour timeout
  )
}