#' Wrapper for CNEFE processing with memory-optimized routing
#'
#' These functions wrap existing CNEFE processing functions to use
#' the single-threaded controller for memory efficiency
#'

#' Clean CNEFE 2010 data with memory-optimized processing
#' 
#' @param cnefe_file CNEFE raw data
#' @param muni_ids Municipality identifiers
#' @param tract_centroids Tract centroids for geocoding
#' @return Cleaned CNEFE data
#' @export
clean_cnefe10_routed <- function(cnefe_file, muni_ids, tract_centroids) {
  # Check if controllers exist
  if (!exists("crew_controllers", envir = .GlobalEnv)) {
    # Fall back to original function if no controllers
    return(clean_cnefe10(cnefe_file, muni_ids, tract_centroids))
  }
  
  # Use memory-optimized controller
  controllers <- get("crew_controllers", envir = .GlobalEnv)
  
  # Submit task to CNEFE controller
  controllers$cnefe$push(
    command = clean_cnefe10(cnefe_file, muni_ids, tract_centroids),
    data = list(
      cnefe_file = cnefe_file,
      muni_ids = muni_ids,
      tract_centroids = tract_centroids
    ),
    packages = c("data.table", "stringr")
  )
  
  # Wait for result
  controllers$cnefe$wait()
  controllers$cnefe$pop()$result[[1]]
}

#' Clean CNEFE 2022 data with memory-optimized processing
#'
#' @param cnefe22_file Path to CNEFE 2022 file
#' @param muni_ids Municipality identifiers for the state
#' @return Cleaned CNEFE data
#' @export
clean_cnefe22_routed <- function(cnefe22_file, muni_ids) {
  # Check if controllers exist
  if (!exists("crew_controllers", envir = .GlobalEnv)) {
    # Fall back to original function if no controllers
    return(clean_cnefe22(cnefe22_file, muni_ids))
  }
  
  # Use memory-optimized controller
  controllers <- get("crew_controllers", envir = .GlobalEnv)
  
  # Submit task to CNEFE controller
  controllers$cnefe$push(
    command = clean_cnefe22(cnefe22_file, muni_ids),
    data = list(
      cnefe22_file = cnefe22_file,
      muni_ids = muni_ids
    ),
    packages = c("data.table", "stringr")
  )
  
  # Wait for result
  controllers$cnefe$wait()
  controllers$cnefe$pop()$result[[1]]
}

#' Process string matching with parallel controller
#'
#' @param locais_muni Polling stations for a municipality
#' @param match_data Data to match against (INEP, CNEFE schools, etc.)
#' @param match_fn Matching function to use
#' @return Match results
#' @export
process_string_match_routed <- function(locais_muni, match_data, match_fn) {
  # Check if controllers exist
  if (!exists("crew_controllers", envir = .GlobalEnv)) {
    # Fall back to direct function call if no controllers
    return(match_fn(locais_muni, match_data))
  }
  
  # Use light controller for string matching
  controllers <- get("crew_controllers", envir = .GlobalEnv)
  
  # Submit task to light controller
  controllers$light$push(
    command = match_fn(locais_muni, match_data),
    data = list(
      locais_muni = locais_muni,
      match_data = match_data
    ),
    packages = c("data.table", "stringdist", "stringr")
  )
  
  # Return immediately - targets will handle the result
  controllers$light
}

#' Process validation with parallel controller
#'
#' @param data Data to validate
#' @param validation_fn Validation function to use
#' @param ... Additional arguments for validation function
#' @return Validation results
#' @export
process_validation_routed <- function(data, validation_fn, ...) {
  # Check if controllers exist
  if (!exists("crew_controllers", envir = .GlobalEnv)) {
    # Fall back to direct function call if no controllers
    return(validation_fn(data, ...))
  }
  
  # Use light controller for validation
  controllers <- get("crew_controllers", envir = .GlobalEnv)
  
  # Submit task to light controller
  controllers$light$push(
    command = validation_fn(data, ...),
    data = list(
      data = data,
      ... = list(...)
    ),
    packages = c("validate", "data.table")
  )
  
  # Wait for result
  controllers$light$wait()
  controllers$light$pop()$result[[1]]
}

#' Check and adjust controller resources based on current load
#'
#' This function should be called periodically during pipeline execution
#' to adjust resources based on system load
#'
#' @export
check_and_adjust_resources <- function() {
  if (!exists("crew_controllers", envir = .GlobalEnv)) {
    return(invisible(NULL))
  }
  
  controllers <- get("crew_controllers", envir = .GlobalEnv)
  
  # Monitor current status
  status <- monitor_resource_usage(controllers)
  
  # Adjust if needed
  if (!is.na(status$system_memory_pct)) {
    adjust_workers(controllers, status$system_memory_pct)
  }
  
  # Log status
  message(sprintf(
    "[%s] Memory: %.1f%% | Active workers: %d | CNEFE queue: %d | Light queue: %d",
    format(status$timestamp, "%H:%M:%S"),
    (status$system_memory_pct %||% 0) * 100,
    status$active_workers,
    status$cnefe_status$queue %||% 0,
    status$light_status$queue %||% 0
  ))
  
  invisible(status)
}