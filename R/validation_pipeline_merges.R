# Pipeline-Specific Merge Validations
#
# This file contains validation wrappers for specific merge operations
# used throughout the geocoding pipeline, with configurations tailored
# to each merge's expected behavior.

source("R/validation_join_operations.R")

#' Validate and merge CNEFE data with municipality identifiers
#' 
#' @param cnefe_dt CNEFE data.table
#' @param muni_ids Municipality identifiers data.table
#' @param stop_on_error Whether to stop execution on validation failure
#' @return Merged data.table with validation report
validate_merge_cnefe_muni <- function(cnefe_dt, muni_ids, stop_on_error = TRUE) {
  message("Validating CNEFE-Municipality merge...")
  
  # CNEFE has many records per municipality, muni_ids has one per municipality
  result <- safe_merge(
    dt1 = cnefe_dt,
    dt2 = muni_ids,
    keys = "id_munic_7",
    join_type = "many-to-one",
    merge_type = "left",
    stop_on_error = stop_on_error
  )
  
  # Additional specific validation
  validation_report <- attr(result, "validation_report")
  
  # Check that all CNEFE records got municipality info
  na_muni_info <- result[is.na(id_estado), .N]
  if (na_muni_info > 0) {
    warning(sprintf("%d CNEFE records did not match to municipality info", na_muni_info))
  }
  
  result
}

#' Validate and merge TSE geocoded data with municipality identifiers
#' 
#' @param tse_dt TSE geocoded data.table
#' @param muni_ids Municipality identifiers data.table
#' @param stop_on_error Whether to stop execution on validation failure
#' @return Merged data.table with validation report
validate_merge_tse_muni <- function(tse_dt, muni_ids, stop_on_error = TRUE) {
  message("Validating TSE-Municipality merge...")
  
  # Rename key in muni_ids for merge
  muni_ids_renamed <- copy(muni_ids)
  setnames(muni_ids_renamed, "id_TSE", "cd_municipio", skip_absent = TRUE)
  
  result <- safe_merge(
    dt1 = tse_dt,
    dt2 = muni_ids_renamed,
    keys = "cd_municipio",
    join_type = "many-to-one", 
    merge_type = "left",
    stop_on_error = stop_on_error
  )
  
  # Check coverage
  na_muni <- result[is.na(id_munic_7), .N]
  if (na_muni > 0) {
    warning(sprintf("%d TSE records did not match to municipality identifiers", na_muni))
  }
  
  result
}

#' Validate and merge locais with coordinates
#' 
#' @param locais_dt Locais data.table
#' @param coords_dt Coordinates data.table
#' @param keys Join keys (default: "local_id")
#' @param stop_on_error Whether to stop execution on validation failure
#' @return Merged data.table with validation report
validate_merge_locais_coords <- function(locais_dt, coords_dt, 
                                       keys = "local_id",
                                       stop_on_error = TRUE) {
  message("Validating Locais-Coordinates merge...")
  
  # This should be a one-to-one merge
  result <- safe_merge(
    dt1 = locais_dt,
    dt2 = coords_dt,
    keys = keys,
    join_type = "one-to-one",
    merge_type = "left",
    stop_on_error = stop_on_error
  )
  
  # Additional validation: check coordinate quality
  validation_report <- attr(result, "validation_report")
  
  # Check for missing coordinates
  na_coords <- result[is.na(latitude) | is.na(longitude), .N]
  total_rows <- nrow(result)
  coverage_pct <- (total_rows - na_coords) / total_rows * 100
  
  message(sprintf("Coordinate coverage: %.1f%% (%d of %d locations)", 
                  coverage_pct, total_rows - na_coords, total_rows))
  
  # Check coordinate bounds for Brazil
  invalid_coords <- result[
    !is.na(latitude) & !is.na(longitude) & 
    (latitude < -33.75 | latitude > 5.27 | 
     longitude < -73.99 | longitude > -34.79), .N
  ]
  
  if (invalid_coords > 0) {
    warning(sprintf("%d locations have coordinates outside Brazil bounds", invalid_coords))
  }
  
  result
}

#' Validate and merge using composite keys (e.g., polling station identification)
#' 
#' @param dt1 First data.table
#' @param dt2 Second data.table
#' @param keys Composite key columns
#' @param merge_name Name for this merge (for reporting)
#' @param stop_on_error Whether to stop execution on validation failure
#' @return Merged data.table with validation report
validate_merge_composite_keys <- function(dt1, dt2, keys, 
                                        merge_name = "composite",
                                        stop_on_error = TRUE) {
  message(sprintf("Validating %s merge with composite keys: %s", 
                  merge_name, paste(keys, collapse = ", ")))
  
  # Check for any missing values in composite keys
  dt1_missing <- dt1[, any(is.na(.SD)), .SDcols = keys, by = 1:nrow(dt1)][V1 == TRUE, .N]
  dt2_missing <- dt2[, any(is.na(.SD)), .SDcols = keys, by = 1:nrow(dt2)][V1 == TRUE, .N]
  
  if (dt1_missing > 0 || dt2_missing > 0) {
    warning(sprintf("Missing values in composite keys: %d in dt1, %d in dt2", 
                    dt1_missing, dt2_missing))
  }
  
  # Composite keys often should be unique
  result <- safe_merge(
    dt1 = dt1,
    dt2 = dt2,
    keys = keys,
    join_type = "one-to-one",
    merge_type = "left",
    stop_on_error = stop_on_error
  )
  
  result
}

#' Validate panel ID merge operations
#' 
#' @param geocoded_dt Geocoded locais data.table
#' @param panel_dt Panel IDs data.table
#' @param stop_on_error Whether to stop execution on validation failure
#' @return Merged data.table with validation report
validate_merge_panel_ids <- function(geocoded_dt, panel_dt, stop_on_error = TRUE) {
  message("Validating Panel ID merge...")
  
  # Panel IDs should be unique per local_id
  result <- safe_merge(
    dt1 = geocoded_dt,
    dt2 = panel_dt,
    keys = "local_id",
    join_type = "one-to-one",
    merge_type = "left",
    stop_on_error = stop_on_error
  )
  
  # Check panel consistency
  if ("panel_id" %in% names(result)) {
    # Check that panel_ids are not duplicated within years
    panel_year_dups <- result[!is.na(panel_id), .(n = .N), by = .(ano, panel_id)][n > 1]
    if (nrow(panel_year_dups) > 0) {
      warning(sprintf("Found %d duplicate panel_id-year combinations", nrow(panel_year_dups)))
      print(head(panel_year_dups))
    }
  }
  
  result
}

#' Batch validate all merges in a pipeline stage
#' 
#' @param merge_results List of merge results with validation reports
#' @param stage_name Name of the pipeline stage
#' @param stop_on_error Whether to stop if any merge failed validation
#' @return Summary report of all validations
validate_pipeline_stage <- function(merge_results, stage_name, stop_on_error = TRUE) {
  message(sprintf("\nValidating pipeline stage: %s", stage_name))
  message(paste(rep("=", 50), collapse = ""))
  
  all_valid <- TRUE
  summary_report <- list()
  
  for (i in seq_along(merge_results)) {
    merge_name <- names(merge_results)[i]
    if (is.null(merge_name)) merge_name <- paste0("merge_", i)
    
    result <- merge_results[[i]]
    validation <- attr(result, "validation_report")
    
    if (!is.null(validation)) {
      pre_valid <- is.null(validation$pre_merge) || validation$pre_merge$valid
      post_valid <- is.null(validation$post_merge) || validation$post_merge$valid
      
      merge_valid <- pre_valid && post_valid
      all_valid <- all_valid && merge_valid
      
      summary_report[[merge_name]] <- list(
        valid = merge_valid,
        pre_merge_valid = pre_valid,
        post_merge_valid = post_valid,
        rows_before = validation$post_merge$diagnostics$rows_before,
        rows_after = validation$post_merge$diagnostics$rows_after,
        issues = c(validation$pre_merge$issues, validation$post_merge$issues)
      )
      
      cat(sprintf("\n%s: %s\n", merge_name, ifelse(merge_valid, "PASSED", "FAILED")))
      if (!merge_valid && length(summary_report[[merge_name]]$issues) > 0) {
        cat("  Issues:\n")
        for (issue in summary_report[[merge_name]]$issues) {
          cat("    -", issue, "\n")
        }
      }
    }
  }
  
  cat(sprintf("\n\nStage Summary: %s\n", ifelse(all_valid, "ALL PASSED", "SOME FAILED")))
  
  if (!all_valid && stop_on_error) {
    stop(sprintf("Pipeline stage '%s' failed validation", stage_name))
  }
  
  invisible(summary_report)
}

#' Create a validation checkpoint for use in targets pipeline
#' 
#' @param data Data to validate
#' @param validation_fn Validation function to apply
#' @param target_name Name of the target being validated
#' @param ... Additional arguments passed to validation function
#' @return The input data if validation passes
#' @export
create_validation_checkpoint <- function(data, validation_fn, target_name, ...) {
  message(sprintf("\n🔍 Validation checkpoint: %s", target_name))
  
  # Apply validation
  validation_result <- validation_fn(data, ...)
  
  # Handle different validation result types
  if (inherits(validation_result, "validation_result")) {
    if (!validation_result$passed) {
      print(validation_result)
      stop(sprintf("Validation failed for target: %s", target_name))
    }
  } else if (is.list(validation_result) && !is.null(validation_result$valid)) {
    if (!validation_result$valid) {
      print(validation_result)
      stop(sprintf("Validation failed for target: %s", target_name))
    }
  }
  
  message(sprintf("✅ Validation passed for: %s", target_name))
  
  # Return the data unchanged
  data
}