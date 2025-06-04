#' Create differentiated crew controllers for parallel processing
#'
#' Sets up two separate crew controllers with different resource profiles:
#' - cnefe_heavy: Single-threaded controller for memory-intensive CNEFE operations
#' - light_tasks: Multi-core controller for lighter parallel tasks
#'
#' @return List with two crew controllers: cnefe and light
#' @export
create_crew_controllers <- function() {
  # Controller for memory-intensive CNEFE operations
  # With 116GB available memory, we can safely run 4 CNEFE workers
  # Each state typically uses 10-20GB, so 4 workers = 40-80GB peak
  cnefe_controller <- crew::crew_controller_local(
    name = "cnefe_heavy",
    workers = 4,  # Limited parallelism for memory efficiency
    seconds_idle = 60,  # Keep worker alive for 60 seconds when idle
    seconds_wall = 7200,  # 2 hour wall time limit per worker
    seconds_timeout = 600,  # 10 minute timeout for dispatcher communications
    tasks_max = 1,  # Process one task at a time per worker
    reset_globals = TRUE,  # Clean memory between tasks
    reset_packages = FALSE,
    reset_options = FALSE,
    garbage_collection = TRUE  # Force garbage collection between tasks
  )
  
  # Controller for lighter parallel tasks
  # Use most cores for light tasks (leave 2 for system + CNEFE controller)
  n_cores <- parallel::detectCores()
  light_workers <- max(1, floor(n_cores * 0.75))  # Use 75% of cores
  
  light_controller <- crew::crew_controller_local(
    name = "light_tasks",
    workers = light_workers,
    seconds_idle = 10,  # Quick idle timeout for dynamic scaling
    seconds_wall = 3600,  # 1 hour wall time limit per worker
    seconds_timeout = 300,  # 5 minute timeout
    tasks_max = 1000,  # Higher limit but not infinite to prevent runaway
    reset_globals = FALSE,  # Keep globals for performance
    reset_packages = FALSE,
    reset_options = FALSE,
    garbage_collection = FALSE,  # Skip GC for light tasks to improve speed
    launch_max = light_workers * 2  # Allow relaunching workers if they fail
  )
  
  # Return both controllers
  list(
    cnefe = cnefe_controller,
    light = light_controller
  )
}

#' Route task to appropriate controller based on task type
#'
#' @param task_type Character string: "cnefe" or "light"
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
  } else if (task_type == "light") {
    controllers$light
  } else {
    stop("Unknown task type: ", task_type, ". Use 'cnefe' or 'light'.")
  }
  
  # Push task to controller
  controller$push(
    command = task_function(...),
    data = list(...),
    packages = packages
  )
}

#' Process CNEFE data chunk with memory-optimized controller
#'
#' @param chunk_data CNEFE data chunk to process
#' @param operation Character string describing the operation
#' @return Processed data
#' @export
process_cnefe_chunk <- function(chunk_data, operation = "clean") {
  route_task(
    task_type = "cnefe",
    task_function = function(data) {
      # Ensure data.table
      setDT(data)
      
      # Perform operation based on type
      if (operation == "clean") {
        # Example cleaning operations
        data[, norm_address := normalize_address(address)]
        data[, norm_street := normalize_street(street)]
      } else if (operation == "geocode") {
        # Geocoding operations
        data[, `:=`(
          lat_processed = process_latitude(latitude),
          long_processed = process_longitude(longitude)
        )]
      }
      
      # Force garbage collection before returning
      gc()
      
      data
    },
    data = chunk_data,
    packages = c("data.table", "stringr")
  )
}

#' Process validation batch with parallel controller
#'
#' @param data_batch Data to validate
#' @param validation_rules List of validation rules
#' @return Validation results
#' @export
process_validation <- function(data_batch, validation_rules = NULL) {
  route_task(
    task_type = "light",
    task_function = function(data, rules) {
      # Default validation rules if not provided
      if (is.null(rules)) {
        rules <- list(
          field_checks = quote(is.character(field)),
          numeric_checks = quote(is.numeric(value))
        )
      }
      
      # Apply validation
      validator <- validate::validator(.list = rules)
      results <- validate::confront(data, validator)
      
      validate::summary(results)
    },
    data = data_batch,
    rules = validation_rules,
    packages = c("validate", "data.table")
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
    light_status = light_status,
    memory_usage_mb = sum(mem_info[, "used"]),
    system_memory_pct = mem_pct,
    active_workers = sum(
      cnefe_status$active %||% 0,
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
  message(sprintf("  - Workers: %d (single-threaded)", controllers$cnefe$workers))
  message(sprintf("  - Tasks per worker: %d", controllers$cnefe$tasks_max))
  message("  - Memory management: reset_globals=TRUE, garbage_collection=TRUE")
  message("  - Idle timeout: 60 seconds")
  
  message("\nLight Controller (parallel tasks):")
  message(sprintf("  - Workers: %d (%.0f%% of cores)", 
                  controllers$light$workers,
                  (controllers$light$workers / parallel::detectCores()) * 100))
  message(sprintf("  - Tasks per worker: %d", controllers$light$tasks_max))
  message("  - Memory management: reset_globals=FALSE, garbage_collection=FALSE")
  message("  - Idle timeout: 10 seconds")
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
  
  message("\nLight Controller:")
  message(sprintf("  - Active: %s", status$light_status$active %||% FALSE))
  message(sprintf("  - Queue: %d tasks", status$light_status$queue %||% 0))
  message(sprintf("  - Complete: %d tasks", status$light_status$complete %||% 0))
  message("=====================================\n")
  
  invisible(status)
}