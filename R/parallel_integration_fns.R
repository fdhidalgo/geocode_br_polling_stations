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

#' Create balanced batches of municipalities for parallel processing
#'
#' This function creates balanced batches of municipality codes to reduce
#' the number of dynamic branches in the targets pipeline. This helps
#' prevent bottlenecks when crew dispatches thousands of fine-grained tasks.
#'
#' @param muni_codes Vector of municipality codes to batch
#' @param batch_size Target size for each batch (default: 50)
#' @return Named list of municipality code batches
#' @export
#' @examples
#' # Create batches of 50 municipalities each
#' batches <- create_municipality_batches(unique(locais$cod_localidade_ibge))
#' length(batches)  # Will be ~112 for 5571 municipalities
create_municipality_batches <- function(muni_codes, batch_size = 50) {
  # Ensure we have unique municipality codes
  muni_codes <- unique(muni_codes)
  
  n_munis <- length(muni_codes)
  n_batches <- ceiling(n_munis / batch_size)
  
  # Create balanced batches using cut
  batch_indices <- cut(seq_along(muni_codes), 
                      breaks = n_batches, 
                      labels = FALSE)
  
  # Split municipalities into batches
  batches <- split(muni_codes, batch_indices)
  
  # Remove names to ensure clean iteration
  batches <- unname(batches)
  
  # Log batch creation
  message(sprintf(
    "Created %d batches from %d municipalities (avg size: %.1f)",
    n_batches, n_munis, n_munis / n_batches
  ))
  
  batches
}

#' Create municipality batch assignments for flattened parallel processing
#'
#' This function creates a data.table with municipality codes and their
#' corresponding batch IDs. This flattened structure avoids vector length
#' issues when using pattern = map() in targets.
#'
#' @param muni_codes Vector of municipality codes to batch
#' @param batch_size Target size for each batch (default: 50)
#' @return data.table with columns muni_code and batch_id
#' @export
#' @examples
#' # Create batch assignments for municipalities
#' assignments <- create_municipality_batch_assignments(unique(locais$cod_localidade_ibge))
#' # Use with targets: pattern = map(unique(assignments$batch_id))
create_municipality_batch_assignments <- function(muni_codes, batch_size = 50, muni_sizes = NULL) {
  # Ensure we have unique municipality codes
  muni_codes <- unique(muni_codes)
  n_munis <- length(muni_codes)
  
  # If muni_sizes not provided, use simple batching
  if (is.null(muni_sizes)) {
    n_batches <- ceiling(n_munis / batch_size)
    
    # Create batch IDs using rep with appropriate lengths
    batch_ids <- rep(seq_len(n_batches), 
                     each = batch_size, 
                     length.out = n_munis)
    
    # Create the assignment table
    batch_assignments <- data.table(
      muni_code = muni_codes,
      batch_id = batch_ids
    )
    
    # Log batch creation
    message(sprintf(
      "Created %d batch assignments for %d municipalities (avg size: %.1f)",
      n_batches, n_munis, n_munis / n_batches
    ))
    
    return(batch_assignments)
  }
  
  # Smart batching based on municipality size
  # muni_sizes should be a data.table with columns: muni_code, size
  muni_sizes <- copy(muni_sizes)
  setnames(muni_sizes, old = names(muni_sizes), new = c("muni_code", "size"))
  
  # Categorize municipalities by size
  muni_sizes[, size_category := cut(
    size,
    breaks = c(0, 500, 2000, 5000, Inf),
    labels = c("small", "medium", "large", "mega"),
    include.lowest = TRUE
  )]
  
  # Set batch sizes based on category
  muni_sizes[, target_batch_size := fcase(
    size_category == "small", 100,
    size_category == "medium", 50,
    size_category == "large", 20,
    size_category == "mega", 5,
    default = 50
  )]
  
  # Sort by size (largest first) to ensure balanced batches
  setorder(muni_sizes, -size)
  
  # Create batches for each size category
  batch_counter <- 1
  muni_sizes[, batch_id := NA_integer_]
  
  for (cat in c("mega", "large", "medium", "small")) {
    cat_data <- muni_sizes[size_category == cat]
    if (nrow(cat_data) == 0) next
    
    target_size <- cat_data$target_batch_size[1]
    n_cat <- nrow(cat_data)
    n_batches_cat <- ceiling(n_cat / target_size)
    
    # Assign batch IDs
    batch_ids_cat <- rep(
      seq(from = batch_counter, length.out = n_batches_cat),
      each = target_size,
      length.out = n_cat
    )
    
    muni_sizes[size_category == cat, batch_id := batch_ids_cat]
    batch_counter <- batch_counter + n_batches_cat
  }
  
  # Create final assignment table
  batch_assignments <- muni_sizes[, .(muni_code, batch_id, size, size_category)]
  
  # Log batch creation with details
  batch_summary <- batch_assignments[, .(
    n_munis = .N,
    total_size = sum(size),
    avg_size = mean(size),
    size_categories = paste(unique(size_category), collapse = ", ")
  ), by = batch_id]
  
  message(sprintf(
    "Created %d smart batch assignments for %d municipalities:",
    max(batch_assignments$batch_id), n_munis
  ))
  message(sprintf(
    "  Mega (>5000): %d municipalities in %d batches",
    nrow(muni_sizes[size_category == "mega"]),
    length(unique(muni_sizes[size_category == "mega"]$batch_id))
  ))
  message(sprintf(
    "  Large (2000-5000): %d municipalities in %d batches",
    nrow(muni_sizes[size_category == "large"]),
    length(unique(muni_sizes[size_category == "large"]$batch_id))
  ))
  message(sprintf(
    "  Medium (500-2000): %d municipalities in %d batches",
    nrow(muni_sizes[size_category == "medium"]),
    length(unique(muni_sizes[size_category == "medium"]$batch_id))
  ))
  message(sprintf(
    "  Small (<500): %d municipalities in %d batches",
    nrow(muni_sizes[size_category == "small"]),
    length(unique(muni_sizes[size_category == "small"]$batch_id))
  ))
  
  # Return only muni_code and batch_id for compatibility
  batch_assignments[, .(muni_code, batch_id)]
}