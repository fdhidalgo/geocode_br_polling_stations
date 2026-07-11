## Configuration Functions
##
## Functions for managing pipeline configuration, including:
## - Development vs production mode settings
## - Parallel processing configuration
## - Memory management thresholds
## - Expected data validation counts

library(data.table)
library(crew)

# ===== PIPELINE CONFIGURATION =====
# Core configuration settings that control pipeline behavior

# The 27 Brazilian federative units (26 states + the Federal District), used to
# enumerate all states in production. Defined once and reused (cleanup phase 4
# dedup).
BRAZIL_STATES <- c(
  "AC",
  "AL",
  "AM",
  "AP",
  "BA",
  "CE",
  "DF",
  "ES",
  "GO",
  "MA",
  "MG",
  "MS",
  "MT",
  "PA",
  "PB",
  "PE",
  "PI",
  "PR",
  "RJ",
  "RN",
  "RO",
  "RR",
  "RS",
  "SC",
  "SE",
  "SP",
  "TO"
)

get_pipeline_config <- function(dev_mode = FALSE) {
  # Get pipeline configuration based on development mode

  if (dev_mode) {
    config <- list(
      dev_mode = TRUE,
      dev_states = c("AC", "RR"), # Acre and Roraima - smallest states by population for fast testing
      batch_size = 10,
      max_workers = 2,
      cache_dir = "_targets/cache",
      log_level = "DEBUG"
    )
  } else {
    config <- list(
      dev_mode = FALSE,
      dev_states = NULL, # Process all states
      batch_size = 100,
      max_workers = parallel::detectCores() - 1,
      cache_dir = "_targets/cache",
      log_level = "INFO"
    )
  }

  # Panel record-linkage weight threshold (Fellegi-Sunter). An explicit,
  # tracked config field replaces the former untracked
  # getOption("geocode_br.panel_weight_threshold") (cleanup phase 3, Medium).
  # Kept at 0; whether ~0.5 is intended is an evaluation question (ticket #25).
  config$panel_weight_threshold <- 0

  # Add computed values
  config$n_cores <- config$max_workers
  config$states <- if (is.null(config$dev_states)) {
    BRAZIL_STATES
  } else {
    config$dev_states
  }

  return(config)
}

get_states_for_processing <- function(context, pipeline_config, custom_states = NULL) {
  # Get states to process based on context and mode
  # Centralized function to determine which states to process based on the
  # pipeline context and development mode setting

  if (pipeline_config$dev_mode) {
    return(pipeline_config$dev_states)
  }

  # Production mode - determine states based on context
  switch(
    context,
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
      custom_states %||% BRAZIL_STATES
    },
    stop("Unknown context: ", context)
  )
}

get_agro_cnefe_files <- function(pipeline_config) {
  # Get agro CNEFE files based on mode
  # Special handler for agro_cnefe_files which returns file paths not states
  # In dev mode, maps state abbreviations to specific file names
  # In production mode, returns all files in the agro_censo directory

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

  # Fail loud on an unknown state (cleanup phase 3, Medium): returning NA with a
  # warning let a typo'd or unexpected state code flow through validation as NA.
  if (is.null(count)) {
    stop(paste("Unknown state abbreviation:", state_abbrev))
  }

  return(count)
}

get_expected_municipality_count_for_config <- function(pipeline_config) {
  # Total expected municipality count for the states this run actually processes:
  # the sum of per-state IBGE counts for the dev subset, or the national 5570 in
  # production. Keeps the data-quality monitor's municipality-count check (and its
  # CRITICAL-status stop) from firing on a legitimately dev-filtered output
  # (cleanup phase 3, finding H4 wiring; Codex triage).
  if (pipeline_config$dev_mode) {
    sum(vapply(pipeline_config$dev_states, get_expected_municipality_count, numeric(1)))
  } else {
    5570
  }
}

# ===== CREW CONTROLLER CONFIGURATION =====

get_crew_controllers <- function() {
  # Create crew controller group for parallel processing
  # Uses same configuration for both dev and production modes
  # Crew will only spawn workers as needed, so this is efficient

  # Capture each worker's stdout/stderr to per-worker log files so a worker that
  # dies mid-task (a crew "crash") leaves its actual dying error on disk, instead
  # of only surfacing the opaque "worker crashed N consecutive times" message
  # (issue #48 debugging). Created eagerly because crew does not mkdir it.
  crew_log_dir <- "crew_logs"
  dir.create(crew_log_dir, showWarnings = FALSE, recursive = TRUE)

  # Standard controller for most tasks - optimized for 32-core machine
  controller_standard <- crew::crew_controller_local(
    name = "standard",
    workers = 28, # Max workers - crew only spawns as needed
    seconds_idle = 30,
    seconds_wall = 3600,
    seconds_timeout = 300,
    reset_globals = TRUE,
    reset_packages = FALSE,
    garbage_collection = TRUE,
    options_local = crew::crew_options_local(log_directory = crew_log_dir)
  )

  # Memory-limited controller for CNEFE operations. Fewer workers but more memory
  # per worker. Capped at 4 (was 8): the standard (28) and memory_limited pools
  # run concurrently, so the census-cleaning stage could put up to 36 heavy R
  # processes on a 32-core / 125 GB machine at once and exhaust memory, killing a
  # worker. The victim was agro_cnefe (now also pinned here, see _targets.R); the
  # target itself builds fine in isolation, so the fix is capping peak concurrency
  # rather than the target (issue #48).
  controller_memory <- crew::crew_controller_local(
    name = "memory_limited",
    workers = 4, # Max workers for memory-intensive tasks
    seconds_idle = 60,
    seconds_wall = 7200, # 2 hours for memory-intensive tasks
    seconds_timeout = 600, # 10 minutes timeout
    reset_globals = TRUE,
    reset_packages = FALSE,
    garbage_collection = TRUE,
    options_local = crew::crew_options_local(log_directory = crew_log_dir)
  )

  # Return controller group
  controller_group <- crew::crew_controller_group(
    controller_standard,
    controller_memory
  )

  return(controller_group)
}

configure_targets_options <- function(controller_group) {
  # Configure targets options with the controller group

  tar_option_set(
    packages = c(
      "data.table",
      # R.utils is required by data.table::fread() to read the project's .csv.gz
      # inputs; fread loads it by string, so it must be named explicitly here (both
      # so workers can read gzipped data and so renv discovers/locks it).
      "R.utils",
      "stringr",
      "stringdist",
      "validate",
      "sf",
      "reclin2",
      "bonsai",
      # lightgbm is the bonsai engine dispatched to during model training; it is
      # referenced only as the string "lightgbm" in set_engine(), so it must be
      # named explicitly here (both to load it on workers and so renv discovers it).
      "lightgbm",
      # qs2 backs format = "qs"; with retrieval = "worker" the workers deserialize
      # qs2-formatted targets, so it is a genuine worker dependency (and, being a
      # targets Suggests invoked by string config, is otherwise undiscoverable).
      "qs2",
      "geosphere",
      "rsample",
      "recipes",
      "parsnip",
      "workflows",
      "yardstick",
      "finetune",
      # lme4 is required by finetune::tune_race_anova(): the racing procedure fits a
      # mixed-effects ANOVA model (via lme4) to eliminate hyperparameter candidates.
      # finetune loads it by string, so it must be named explicitly for renv.
      "lme4",
      "tune"
    ),
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
