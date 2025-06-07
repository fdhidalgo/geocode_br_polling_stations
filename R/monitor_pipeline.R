#' Monitor targets pipeline execution status
#'
#' This script provides functions to monitor the targets pipeline execution,
#' including branch progress, worker utilization, and system resources.

#' Monitor pipeline status with detailed branch information
#'
#' @param target_names Optional vector of target names to monitor
#' @return List with pipeline status information
#' @export
monitor_pipeline_status <- function(target_names = NULL) {
  library(targets)
  
  # Overall progress
  overall <- tar_progress_summary()
  
  # Branch progress for key targets
  branch_targets <- c(
    "inep_string_match_batch",
    "schools_cnefe10_match_batch", 
    "schools_cnefe22_match_batch",
    "cnefe10_stbairro_match_batch",
    "cnefe22_stbairro_match_batch",
    "agrocnefe_stbairro_match_batch"
  )
  
  if (!is.null(target_names)) {
    branch_targets <- intersect(branch_targets, target_names)
  }
  
  branch_progress <- list()
  for (target in branch_targets) {
    tryCatch({
      progress <- tar_progress_branches(names = target)
      if (!is.null(progress) && nrow(progress) > 0) {
        # Ensure progress column exists
        if (!"progress" %in% names(progress)) {
          progress$progress <- NA_character_
        }
        branch_progress[[target]] <- progress
      }
    }, error = function(e) {
      # Target may not exist yet
      NULL
    })
  }
  
  # Check outdated targets
  outdated <- tryCatch({
    tar_outdated(callr_function = NULL)
  }, error = function(e) {
    character(0)
  })
  
  # Summary
  list(
    timestamp = Sys.time(),
    overall = overall,
    branch_progress = branch_progress,
    outdated_count = length(outdated),
    outdated_targets = head(outdated, 20)  # Show first 20
  )
}

#' Monitor crew controller status
#'
#' @return List with controller status information
#' @export
monitor_crew_status <- function() {
  # Check if controllers exist in global environment
  if (!exists("controller_group", envir = .GlobalEnv)) {
    return(list(
      status = "Controllers not found in global environment",
      timestamp = Sys.time()
    ))
  }
  
  controller_group <- get("controller_group", envir = .GlobalEnv)
  
  # Get controller summaries
  memory_summary <- tryCatch({
    controller_group$controllers$memory_limited$summary()
  }, error = function(e) NULL)
  
  standard_summary <- tryCatch({
    controller_group$controllers$standard$summary()
  }, error = function(e) NULL)
  
  list(
    timestamp = Sys.time(),
    memory_controller = memory_summary,
    standard_controller = standard_summary
  )
}

#' Print formatted pipeline status
#'
#' @param status Pipeline status from monitor_pipeline_status()
#' @export
print_pipeline_status <- function(status = NULL) {
  if (is.null(status)) {
    status <- monitor_pipeline_status()
  }
  
  cat("\n========== PIPELINE STATUS ==========\n")
  cat("Timestamp:", format(status$timestamp, "%Y-%m-%d %H:%M:%S"), "\n\n")
  
  # Overall progress
  if (!is.null(status$overall)) {
    cat("Overall Progress:\n")
    print(status$overall)
    cat("\n")
  }
  
  # Branch progress
  if (length(status$branch_progress) > 0) {
    cat("Branch Progress:\n")
    for (target in names(status$branch_progress)) {
      progress <- status$branch_progress[[target]]
      if (!is.null(progress) && nrow(progress) > 0) {
        total <- nrow(progress)
        dispatched <- sum(progress$progress == "dispatched", na.rm = TRUE)
        completed <- sum(progress$progress == "completed", na.rm = TRUE)
        skipped <- sum(progress$progress == "skipped", na.rm = TRUE)
        
        cat(sprintf(
          "  %s: %d branches (dispatched: %d, completed: %d, skipped: %d)\n",
          target, total, dispatched, completed, skipped
        ))
      }
    }
    cat("\n")
  }
  
  # Outdated targets
  cat("Outdated targets:", status$outdated_count, "\n")
  if (length(status$outdated_targets) > 0) {
    cat("  First few:", paste(head(status$outdated_targets, 5), collapse = ", "), "\n")
  }
  
  cat("=====================================\n\n")
}

#' Continuous pipeline monitoring
#'
#' @param interval Seconds between status updates (default: 30)
#' @param duration Total monitoring duration in seconds (default: NULL for continuous)
#' @export
monitor_pipeline_continuous <- function(interval = 30, duration = NULL) {
  start_time <- Sys.time()
  
  cat("Starting continuous pipeline monitoring...\n")
  cat("Press Ctrl+C to stop\n\n")
  
  while (TRUE) {
    # Clear console for fresh display
    cat("\014")  # Form feed character
    
    # Get and print status
    status <- monitor_pipeline_status()
    print_pipeline_status(status)
    
    # Also print crew status
    crew_status <- monitor_crew_status()
    if (!is.null(crew_status$memory_controller)) {
      cat("Memory Controller Workers:", 
          crew_status$memory_controller$workers, "\n")
    }
    if (!is.null(crew_status$standard_controller)) {
      cat("Standard Controller Workers:", 
          crew_status$standard_controller$workers, "\n")
    }
    
    # Check duration
    if (!is.null(duration)) {
      elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
      if (elapsed >= duration) {
        cat("\nMonitoring duration reached. Stopping.\n")
        break
      }
    }
    
    # Wait for next update
    Sys.sleep(interval)
  }
}

# Example usage:
# monitor_pipeline_continuous(interval = 10)  # Update every 10 seconds