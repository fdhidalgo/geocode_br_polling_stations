#' Parallel Processing Functions
#' 
#' Consolidated file containing all parallel processing and batching functions.
#' This file combines functions from:
#' - parallel_processing_fns.R
#' - parallel_integration_fns.R

library(data.table)
library(crew)
library(parallel)

# ============================================================================
# Crew Controller Functions (from parallel_processing_fns.R)
# ============================================================================

#' Create differentiated crew controllers for parallel processing
#'
#' Sets up three separate crew controllers with different resource profiles:
#' - cnefe_heavy: Limited workers for memory-intensive CNEFE operations
#' - string_match: Optimized for CPU-intensive string matching operations
#' - light_tasks: High-concurrency controller for lightweight tasks
#'
#' @return List with three crew controllers: cnefe, string_match, and light
#' @export
create_crew_controllers <- function() {
  # Get system info
  n_cores <- parallel::detectCores()
  
  # Controller for memory-intensive CNEFE operations
  # Each CNEFE state can use 20-40GB, so limit to 2 workers max
  cnefe_controller <- crew::crew_controller_local(
    name = "cnefe_heavy",
    workers = 2,  # Keep limited to prevent memory allocation errors
    seconds_idle = 30,  # Quick cleanup to free memory
    seconds_wall = 7200,  # 2 hour wall time limit per worker
    seconds_timeout = 600,  # 10 minute timeout for dispatcher communications
    tasks_max = 1,  # Process one task at a time per worker
    reset_globals = TRUE,  # Clean memory between tasks
    reset_packages = TRUE,  # Also reset packages to free more memory
    reset_options = FALSE,
    garbage_collection = TRUE  # Force garbage collection between tasks
  )
  
  # Controller for string matching operations
  # CPU-intensive but not memory-heavy, so we can use more workers
  string_match_workers <- max(4, min(n_cores - 2, floor(n_cores * 0.9)))
  
  string_match_controller <- crew::crew_controller_local(
    name = "string_match",
    workers = string_match_workers,  # Use most cores for string matching
    seconds_idle = 60,  # Keep workers warm for batch processing
    seconds_wall = 3600,  # 1 hour wall time limit
    seconds_timeout = 300,  # 5 minute timeout
    tasks_max = 10000,  # High limit for many small tasks
    reset_globals = FALSE,  # Keep globals for performance
    reset_packages = FALSE,
    reset_options = FALSE,
    garbage_collection = FALSE  # Skip GC for speed
  )
  
  # Controller for general light tasks (validation, small operations)
  # Balance between parallelism and system responsiveness
  light_workers <- max(2, min(n_cores - 4, floor(n_cores * 0.5)))
  
  light_controller <- crew::crew_controller_local(
    name = "light_tasks",
    workers = light_workers,
    seconds_idle = 5,  # Very quick idle timeout for dynamic tasks
    seconds_wall = 1800,  # 30 minute wall time limit
    seconds_timeout = 180,  # 3 minute timeout
    tasks_max = 5000,  # Medium limit
    reset_globals = FALSE,  # Keep globals for performance
    reset_packages = FALSE,
    reset_options = FALSE,
    garbage_collection = FALSE  # Skip GC for light tasks
  )
  
  # Return all three controllers
  list(
    cnefe = cnefe_controller,
    string_match = string_match_controller,
    light = light_controller
  )
}

#' Route task to appropriate controller based on task type
#'
#' @param task_type Character string: "cnefe", "string_match", or "light"
#' @param task_function Function to execute
#' @param ... Additional arguments to pass to task_function
#' @param packages Character vector of packages required by the task
#' @return Task submission result
#' @export
route_task <- function(task_type, task_function, ..., packages = c("data.table")) {
  # Get controllers from global environment
  if (!exists("crew_controllers", envir = .GlobalEnv)) {
    stop("Crew controllers not initialized. Run create_crew_controllers() first.")
  }
  
  controllers <- get("crew_controllers", envir = .GlobalEnv)
  
  # Select appropriate controller
  controller <- if (task_type == "cnefe") {
    controllers$cnefe
  } else if (task_type == "string_match") {
    controllers$string_match
  } else if (task_type == "light") {
    controllers$light
  } else {
    stop("Unknown task type: ", task_type, ". Use 'cnefe', 'string_match', or 'light'.")
  }
  
  # Push task to controller
  controller$push(
    command = task_function(...),
    data = list(...),
    packages = packages
  )
}

#' Monitor resource usage across controllers
#'
#' @param controllers List of crew controllers
#' @return List with status information
#' @export
monitor_resource_usage <- function(controllers) {
  # Get memory usage
  mem_info <- gc()
  
  # Get controller status
  cnefe_status <- controllers$cnefe$summary()
  string_match_status <- if (!is.null(controllers$string_match)) {
    controllers$string_match$summary()
  } else {
    list(active = 0, queue = 0, complete = 0)
  }
  light_status <- controllers$light$summary()
  
  # Get system memory
  if (Sys.info()["sysname"] == "Linux") {
    total_mem <- as.numeric(system("free -m | awk 'NR==2{print $2}'", intern = TRUE))
    used_mem <- as.numeric(system("free -m | awk 'NR==2{print $3}'", intern = TRUE))
    mem_pct <- used_mem / total_mem
  } else {
    mem_pct <- NA
  }
  
  list(
    cnefe_status = cnefe_status,
    string_match_status = string_match_status,
    light_status = light_status,
    memory_usage_mb = sum(mem_info[, "used"]),
    system_memory_pct = mem_pct,
    active_workers = sum(
      cnefe_status$active %||% 0,
      string_match_status$active %||% 0,
      light_status$active %||% 0
    ),
    timestamp = Sys.time()
  )
}

#' Adjust worker count based on system load
#'
#' @param controllers List of crew controllers
#' @param system_memory_pct Current system memory usage percentage
#' @return Updated controllers
#' @export
adjust_workers <- function(controllers, system_memory_pct = NULL) {
  # Get current memory usage if not provided
  if (is.null(system_memory_pct)) {
    status <- monitor_resource_usage(controllers)
    system_memory_pct <- status$system_memory_pct
  }
  
  # Only adjust if we have valid memory info
  if (!is.na(system_memory_pct)) {
    current_workers <- controllers$light$workers
    
    if (system_memory_pct > 0.8 && current_workers > 1) {
      # Reduce workers if memory pressure is high
      new_workers <- max(1, current_workers - 1)
      message(sprintf(
        "High memory usage (%.1f%%). Reducing light workers from %d to %d",
        system_memory_pct * 100, current_workers, new_workers
      ))
      controllers$light$scale(workers = new_workers)
    } else if (system_memory_pct < 0.6 && current_workers < (parallel::detectCores() - 2)) {
      # Increase workers if memory usage is low
      new_workers <- min(parallel::detectCores() - 2, current_workers + 1)
      message(sprintf(
        "Low memory usage (%.1f%%). Increasing light workers from %d to %d",
        system_memory_pct * 100, current_workers, new_workers
      ))
      controllers$light$scale(workers = new_workers)
    }
  }
  
  controllers
}

#' Cleanup all controllers and release resources
#'
#' @param controllers List of crew controllers
#' @export
cleanup_controllers <- function(controllers) {
  message("Cleaning up crew controllers...")
  
  # Terminate CNEFE controller
  if (!is.null(controllers$cnefe)) {
    controllers$cnefe$terminate()
    message("CNEFE controller terminated")
  }
  
  # Terminate string match controller
  if (!is.null(controllers$string_match)) {
    controllers$string_match$terminate()
    message("String match controller terminated")
  }
  
  # Terminate light controller
  if (!is.null(controllers$light)) {
    controllers$light$terminate()
    message("Light controller terminated")
  }
  
  # Force garbage collection
  gc()
  
  # Remove from global environment
  if (exists("crew_controllers", envir = .GlobalEnv)) {
    rm("crew_controllers", envir = .GlobalEnv)
  }
  
  message("Cleanup complete")
}

#' Initialize crew controllers and store in global environment
#'
#' @return List of controllers
#' @export
initialize_crew_controllers <- function() {
  # Create controllers
  controllers <- create_crew_controllers()
  
  # Store in global environment for access
  assign("crew_controllers", controllers, envir = .GlobalEnv)
  
  # Print detailed configuration
  message("\n========== CREW CONTROLLERS INITIALIZED ==========")
  message("System info:")
  message(sprintf("  - Total cores: %d", parallel::detectCores()))
  message(sprintf("  - Total memory: %.1f GB", 
                  as.numeric(system("free -g | awk 'NR==2{print $2}'", intern = TRUE))))
  
  message("\nCNEFE Controller (memory-intensive tasks):")
  message(sprintf("  - Workers: %d (memory-optimized)", controllers$cnefe$workers))
  message(sprintf("  - Tasks per worker: %d", controllers$cnefe$tasks_max))
  message("  - Memory management: reset_globals=TRUE, garbage_collection=TRUE")
  message("  - Idle timeout: 30 seconds")
  
  message("\nString Match Controller (CPU-intensive tasks):")
  message(sprintf("  - Workers: %d (%.0f%% of cores)", 
                  controllers$string_match$workers,
                  (controllers$string_match$workers / parallel::detectCores()) * 100))
  message(sprintf("  - Tasks per worker: %d", controllers$string_match$tasks_max))
  message("  - Memory management: reset_globals=FALSE, garbage_collection=FALSE")
  message("  - Idle timeout: 60 seconds")
  
  message("\nLight Controller (general parallel tasks):")
  message(sprintf("  - Workers: %d (%.0f%% of cores)", 
                  controllers$light$workers,
                  (controllers$light$workers / parallel::detectCores()) * 100))
  message(sprintf("  - Tasks per worker: %d", controllers$light$tasks_max))
  message("  - Memory management: reset_globals=FALSE, garbage_collection=FALSE")
  message("  - Idle timeout: 5 seconds")
  message("================================================\n")
  
  invisible(controllers)
}

#' Print current controller status
#'
#' @param controllers List of crew controllers (optional, uses global if not provided)
#' @export
print_controller_status <- function(controllers = NULL) {
  if (is.null(controllers)) {
    if (!exists("crew_controllers", envir = .GlobalEnv)) {
      message("No crew controllers found in global environment")
      return(invisible(NULL))
    }
    controllers <- get("crew_controllers", envir = .GlobalEnv)
  }
  
  status <- monitor_resource_usage(controllers)
  
  message("\n========== CONTROLLER STATUS ==========")
  message(sprintf("Timestamp: %s", format(status$timestamp, "%Y-%m-%d %H:%M:%S")))
  message(sprintf("System memory usage: %.1f%%", 
                  (status$system_memory_pct %||% 0) * 100))
  message(sprintf("R memory usage: %.1f MB", status$memory_usage_mb))
  
  message("\nCNEFE Controller:")
  message(sprintf("  - Active: %s", status$cnefe_status$active %||% FALSE))
  message(sprintf("  - Queue: %d tasks", status$cnefe_status$queue %||% 0))
  message(sprintf("  - Complete: %d tasks", status$cnefe_status$complete %||% 0))
  
  message("\nString Match Controller:")
  message(sprintf("  - Active: %s", status$string_match_status$active %||% FALSE))
  message(sprintf("  - Queue: %d tasks", status$string_match_status$queue %||% 0))
  message(sprintf("  - Complete: %d tasks", status$string_match_status$complete %||% 0))
  
  message("\nLight Controller:")
  message(sprintf("  - Active: %s", status$light_status$active %||% FALSE))
  message(sprintf("  - Queue: %d tasks", status$light_status$queue %||% 0))
  message(sprintf("  - Complete: %d tasks", status$light_status$complete %||% 0))
  message("=====================================\n")
  
  invisible(status)
}

# ============================================================================
# Batching Functions (from parallel_integration_fns.R)
# ============================================================================

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
#' @param muni_sizes Optional data with municipality sizes for smart batching
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

# ============================================================================
# Helper function for default operator
# ============================================================================

#' Default value operator
#' 
#' @param x Value to check
#' @param y Default value if x is NULL
#' @return x if not NULL, otherwise y
#' @export
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}