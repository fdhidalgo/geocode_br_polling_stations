#' Pipeline Configuration Management
#'
#' This file centralizes all configuration for the targets pipeline,
#' including development mode settings, controller configurations,
#' and pipeline parameters.

#' Get crew controller configuration
#'
#' Creates and configures crew controllers for parallel processing
#' @param dev_mode Logical indicating if running in development mode
#' @return crew controller group
#' @export
get_crew_controllers <- function(dev_mode = FALSE) {
  # Adjust worker counts based on mode
  memory_workers <- ifelse(dev_mode, 2, 3)
  standard_workers <- ifelse(dev_mode, 4, 28)
  
  # Memory-limited controller for CNEFE operations
  controller_memory <- crew::crew_controller_local(
    name = "memory_limited",
    workers = memory_workers,
    seconds_idle = 300,
    seconds_wall = 36000,  # 10 hours
    seconds_timeout = 32400,  # 9 hours
    tasks_max = 1,
    tasks_timers = 1,
    seconds_interval = 0.1,
    reset_globals = TRUE,
    reset_packages = TRUE,
    reset_options = TRUE,
    garbage_collection = TRUE
  )
  
  # Standard controller for all other operations
  controller_standard <- crew::crew_controller_local(
    name = "standard",
    workers = standard_workers,
    seconds_idle = 60,
    seconds_wall = 14400,  # 4 hours
    seconds_timeout = 3600,  # 1 hour
    tasks_max = 50,
    tasks_timers = 1,
    seconds_interval = 0.05,
    reset_globals = FALSE,
    reset_packages = FALSE,
    reset_options = FALSE,
    garbage_collection = FALSE
  )
  
  # Create controller group
  crew::crew_controller_group(
    controller_memory,
    controller_standard
  )
}

#' Get batch size configuration
#'
#' Returns appropriate batch sizes for parallel processing
#' @param dev_mode Logical indicating if running in development mode
#' @return List with batch size configurations
#' @export
get_batch_sizes <- function(dev_mode = FALSE) {
  list(
    municipalities = ifelse(dev_mode, 10, 50),
    string_matching = ifelse(dev_mode, 5, 20),
    validation = ifelse(dev_mode, 100000, 1000000)
  )
}

#' Configure targets options
#'
#' Sets up targets options with appropriate settings
#' @param controller_group The crew controller group to use
#' @export
configure_targets_options <- function(controller_group) {
  tar_option_set(
    packages = c(
      "targets",
      "data.table",
      "stringr",
      "bonsai",
      "reclin2",
      "validate",
      "geosphere",
      "sf",
      "geocodebr"
    ),
    format = "qs",
    memory = "transient",
    garbage_collection = TRUE,
    storage = "main",
    retrieval = "main",
    controller = controller_group,
    resources = tar_resources(
      crew = tar_resources_crew(controller = "standard")
    )
  )
}

#' Get source files to load
#'
#' Returns list of R files to source for the pipeline
#' @return Character vector of file paths
#' @export
get_source_files <- function() {
  c(
    # Configuration and helpers
    "./R/pipeline_config.R",
    "./R/target_factory.R",
    "./R/target_helpers.R",
    
    # Core functionality
    "./R/geocodebr_matching.R",
    "./R/parallel_integration_fns.R",
    "./R/memory_efficient_cnefe.R",
    "./R/column_mapping.R",
    "./R/state_filtering.R",
    "./R/data_table_utils.R",
    
    # Validation
    "./R/functions_validate.R",
    "./R/validation_pipeline_stages.R",
    "./R/validation_reporting.R",
    "./R/validation_report_renderer.R",
    
    # Brasília filtering
    "./R/filter_brasilia_municipal.R",
    "./R/validate_brasilia_filtering.R",
    "./R/expected_municipality_counts.R",
    "./R/update_validation_for_brasilia.R",
    
    # Function files (loaded via pattern)
    list.files("./R", full.names = TRUE, pattern = "fns$")
  )
}