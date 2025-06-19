#' Batch Monitoring Utilities
#'
#' Functions to monitor and analyze batch processing performance

#' Monitor batch processing with enhanced logging
#'
#' Wrapper for batch processing functions that adds timing and progress logging
#' @param batch_ids Current batch ID
#' @param batch_assignments Municipality batch assignments
#' @param batch_fn Function to execute for the batch
#' @param ... Additional arguments passed to batch_fn
#' @return Result from batch_fn with timing information
#' @export
monitor_batch_processing <- function(batch_ids, batch_assignments, batch_fn, ...) {
  # Get batch info
  batch_info <- batch_assignments[batch_id == batch_ids]
  n_munis <- nrow(batch_info)
  total_size <- sum(batch_info$muni_size)
  
  # Log start
  start_time <- Sys.time()
  message(sprintf(
    "[%s] Starting batch %s: %d municipalities, %s total items",
    format(start_time, "%H:%M:%S"),
    batch_ids,
    n_munis,
    format(total_size, big.mark = ",")
  ))
  
  # Execute batch function
  result <- batch_fn(batch_ids, batch_assignments, ...)
  
  # Log completion
  end_time <- Sys.time()
  duration <- difftime(end_time, start_time, units = "mins")
  
  message(sprintf(
    "[%s] Completed batch %s in %.1f minutes (%.0f items/min)",
    format(end_time, "%H:%M:%S"),
    batch_ids,
    duration,
    total_size / as.numeric(duration)
  ))
  
  # Add timing metadata if result is a data.table
  if (is.data.table(result)) {
    attr(result, "batch_timing") <- list(
      batch_id = batch_ids,
      start_time = start_time,
      end_time = end_time,
      duration_mins = as.numeric(duration),
      n_munis = n_munis,
      total_size = total_size,
      items_per_minute = total_size / as.numeric(duration)
    )
  }
  
  result
}

#' Analyze municipality complexity for batch optimization
#'
#' @param locais_data Polling stations data
#' @param cnefe_data CNEFE data (streets/neighborhoods)
#' @return Data.table with complexity metrics by municipality
#' @export
analyze_municipality_complexity <- function(locais_data, cnefe_data) {
  # Count items per municipality
  locais_counts <- locais_data[, .(n_polling_stations = .N), by = cod_localidade_ibge]
  cnefe_counts <- cnefe_data[, .(n_cnefe_items = .N), by = id_munic_7]
  
  # Merge and calculate complexity score
  complexity <- merge(
    locais_counts,
    cnefe_counts,
    by.x = "cod_localidade_ibge",
    by.y = "id_munic_7",
    all = TRUE
  )
  
  # Replace NAs with 0
  complexity[is.na(n_polling_stations), n_polling_stations := 0]
  complexity[is.na(n_cnefe_items), n_cnefe_items := 0]
  
  # Calculate complexity score (number of comparisons)
  complexity[, complexity_score := n_polling_stations * n_cnefe_items]
  
  # Add complexity category
  complexity[, complexity_category := cut(
    complexity_score,
    breaks = c(0, 1e5, 1e6, 1e7, Inf),
    labels = c("low", "medium", "high", "very_high"),
    include.lowest = TRUE
  )]
  
  complexity[order(-complexity_score)]
}

#' Generate batch performance report
#'
#' @param meta_data Targets metadata
#' @param pattern Pattern to match batch targets
#' @return Summary statistics of batch performance
#' @export
generate_batch_performance_report <- function(meta_data, pattern = "_match_batch") {
  # Filter batch targets
  batch_targets <- meta_data[grepl(pattern, name)]
  
  if (nrow(batch_targets) == 0) {
    message("No batch targets found matching pattern: ", pattern)
    return(NULL)
  }
  
  # Calculate duration in hours
  batch_targets[, duration_hours := seconds / 3600]
  
  # Summary statistics
  summary_stats <- list(
    total_batches = nrow(batch_targets),
    total_time_hours = sum(batch_targets$duration_hours, na.rm = TRUE),
    mean_time_hours = mean(batch_targets$duration_hours, na.rm = TRUE),
    median_time_hours = median(batch_targets$duration_hours, na.rm = TRUE),
    slowest_batches = batch_targets[order(-duration_hours)][1:10, .(name, duration_hours)],
    time_distribution = quantile(
      batch_targets$duration_hours,
      probs = c(0, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 1),
      na.rm = TRUE
    )
  )
  
  # Print report
  cat("\n=== Batch Performance Report ===\n")
  cat(sprintf("Total batches: %d\n", summary_stats$total_batches))
  cat(sprintf("Total time: %.1f hours\n", summary_stats$total_time_hours))
  cat(sprintf("Mean time per batch: %.2f hours\n", summary_stats$mean_time_hours))
  cat(sprintf("Median time per batch: %.2f hours\n", summary_stats$median_time_hours))
  cat("\nTime distribution (hours):\n")
  print(summary_stats$time_distribution)
  cat("\nSlowest 10 batches:\n")
  print(summary_stats$slowest_batches)
  
  invisible(summary_stats)
}

#' Monitor active pipeline execution
#'
#' @param interval_seconds Seconds between status checks
#' @param max_duration_mins Maximum monitoring duration in minutes
#' @export
monitor_pipeline_execution <- function(interval_seconds = 60, max_duration_mins = 60) {
  library(targets)
  
  start_time <- Sys.time()
  max_time <- start_time + (max_duration_mins * 60)
  
  message("Starting pipeline monitoring...")
  message("Press Ctrl+C to stop monitoring\n")
  
  while (Sys.time() < max_time) {
    # Get current progress
    progress <- tar_progress()
    
    if (nrow(progress) > 0) {
      # Count by status
      status_counts <- table(progress$progress)
      
      # Calculate rates
      elapsed_mins <- as.numeric(difftime(Sys.time(), start_time, units = "mins"))
      completed <- sum(progress$progress %in% c("completed", "skipped"))
      rate <- if (elapsed_mins > 0) completed / elapsed_mins else 0
      
      # Display status
      cat(sprintf(
        "\n[%s] Pipeline Status (%.1f min elapsed):\n",
        format(Sys.time(), "%H:%M:%S"),
        elapsed_mins
      ))
      print(status_counts)
      cat(sprintf("Completion rate: %.1f targets/min\n", rate))
      
      # Show currently running targets
      running <- progress[progress$progress == "running", "name"]
      if (nrow(running) > 0) {
        cat("\nCurrently running:\n")
        print(head(running, 10))
      }
    }
    
    # Wait for next check
    Sys.sleep(interval_seconds)
  }
  
  message("\nMonitoring completed.")
}