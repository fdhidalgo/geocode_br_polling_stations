#' String Matching Function Selector
#' 
#' This file provides a mechanism to switch between original and memory-efficient
#' string matching implementations based on municipality size or global settings.

#' Check if memory-efficient mode should be used
#'
#' @param n_queries Number of query strings
#' @param n_targets Number of target strings
#' @param force_memory_efficient Force memory-efficient mode regardless of size
#' @return Logical indicating whether to use memory-efficient mode
#' @export
should_use_memory_efficient <- function(n_queries, n_targets, force_memory_efficient = FALSE) {
  # Global option to force memory-efficient mode
  if (isTRUE(getOption("geocode_br.use_memory_efficient", FALSE))) {
    return(TRUE)
  }
  
  if (force_memory_efficient) {
    return(TRUE)
  }
  
  # Use memory-efficient mode for large datasets
  # Threshold: when matrix would exceed ~500MB
  matrix_size_mb <- (n_queries * n_targets * 8) / 1024^2
  
  if (matrix_size_mb > 500) {
    message(sprintf(
      "Using memory-efficient mode for %d x %d matrix (%.1f MB)",
      n_queries, n_targets, matrix_size_mb
    ))
    return(TRUE)
  }
  
  FALSE
}

#' Get appropriate string matching function
#'
#' @param function_name Name of the string matching function
#' @param n_queries Number of queries (optional)
#' @param n_targets Number of targets (optional)
#' @return The appropriate function to use
#' @export
get_string_matching_function <- function(function_name, n_queries = NULL, n_targets = NULL) {
  # Check if we should use memory-efficient version
  use_memory_efficient <- FALSE
  
  if (!is.null(n_queries) && !is.null(n_targets)) {
    use_memory_efficient <- should_use_memory_efficient(n_queries, n_targets)
  } else {
    # If sizes not provided, check global option
    use_memory_efficient <- isTRUE(getOption("geocode_br.use_memory_efficient", FALSE))
  }
  
  if (use_memory_efficient) {
    # Source memory-efficient versions
    if (!exists("match_strings_memory_efficient")) {
      source("R/memory_efficient_string_matching.R")
      source("R/string_matching_geocode_fns_memory_efficient.R")
    }
    
    # Return memory-efficient version
    switch(function_name,
      "match_inep_muni" = get("match_inep_muni", envir = globalenv()),
      "match_schools_cnefe_muni" = get("match_schools_cnefe_muni", envir = globalenv()),
      "match_stbairro_cnefe_muni" = get("match_stbairro_cnefe_muni", envir = globalenv()),
      "match_stbairro_agrocnefe_muni" = get("match_stbairro_agrocnefe_muni", envir = globalenv()),
      stop("Unknown function: ", function_name)
    )
  } else {
    # Use original versions
    if (!exists(function_name)) {
      source("R/string_matching_geocode_fns.R")
    }
    get(function_name)
  }
}

#' Enable memory-efficient string matching globally
#' 
#' @param enable Logical indicating whether to enable
#' @export
set_memory_efficient_mode <- function(enable = TRUE) {
  options(geocode_br.use_memory_efficient = enable)
  if (enable) {
    message("Memory-efficient string matching enabled globally")
  } else {
    message("Memory-efficient string matching disabled")
  }
}

#' Get current memory-efficient mode status
#' 
#' @return Logical indicating if memory-efficient mode is enabled
#' @export
get_memory_efficient_mode <- function() {
  isTRUE(getOption("geocode_br.use_memory_efficient", FALSE))
}