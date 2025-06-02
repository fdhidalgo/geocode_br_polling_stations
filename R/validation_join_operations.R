# Join Operation Validation Functions
# 
# This file contains functions to validate data integrity before and after
# join operations in the geocoding pipeline. These validations are critical
# to prevent data corruption from merge mistakes.

library(data.table)
# Note: validate package is loaded if available but not required for core functionality
if (requireNamespace("validate", quietly = TRUE)) {
  library(validate)
}

#' Validate join keys before merge operation
#' 
#' @param dt1 First data.table
#' @param dt2 Second data.table  
#' @param keys Character vector of join keys
#' @param join_type Type of join: "one-to-one", "one-to-many", "many-to-one"
#' @param allow_missing Logical, whether to allow missing keys
#' @return List with validation results and detailed diagnostics
validate_join_keys <- function(dt1, dt2, keys, join_type = "one-to-one", allow_missing = FALSE) {
  stopifnot(is.data.table(dt1), is.data.table(dt2))
  stopifnot(all(keys %in% names(dt1)), all(keys %in% names(dt2)))
  
  results <- list(
    valid = TRUE,
    issues = character(),
    diagnostics = list()
  )
  
  # Check for missing keys
  dt1_missing <- dt1[, any(is.na(.SD)), .SDcols = keys]
  dt2_missing <- dt2[, any(is.na(.SD)), .SDcols = keys]
  
  if (!allow_missing && (dt1_missing || dt2_missing)) {
    results$valid <- FALSE
    if (dt1_missing) {
      missing_count <- dt1[, sum(rowSums(is.na(.SD)) > 0), .SDcols = keys]
      results$issues <- c(results$issues, 
        sprintf("dt1 has %d rows with missing join keys", missing_count))
    }
    if (dt2_missing) {
      missing_count <- dt2[, sum(rowSums(is.na(.SD)) > 0), .SDcols = keys]
      results$issues <- c(results$issues, 
        sprintf("dt2 has %d rows with missing join keys", missing_count))
    }
  }
  
  # Check key uniqueness based on join type
  dt1_unique <- nrow(unique(dt1[, ..keys])) == nrow(dt1)
  dt2_unique <- nrow(unique(dt2[, ..keys])) == nrow(dt2)
  
  if (join_type == "one-to-one") {
    if (!dt1_unique) {
      results$valid <- FALSE
      dup_count <- dt1[, .N, by = keys][N > 1, sum(N) - .N]
      results$issues <- c(results$issues, 
        sprintf("dt1 has non-unique keys (%d duplicate rows) for one-to-one join", dup_count))
    }
    if (!dt2_unique) {
      results$valid <- FALSE
      dup_count <- dt2[, .N, by = keys][N > 1, sum(N) - .N]
      results$issues <- c(results$issues, 
        sprintf("dt2 has non-unique keys (%d duplicate rows) for one-to-one join", dup_count))
    }
  } else if (join_type == "one-to-many") {
    if (!dt1_unique) {
      results$valid <- FALSE
      dup_count <- dt1[, .N, by = keys][N > 1, sum(N) - .N]
      results$issues <- c(results$issues, 
        sprintf("dt1 (left side) has non-unique keys (%d duplicate rows) for one-to-many join", dup_count))
    }
  } else if (join_type == "many-to-one") {
    if (!dt2_unique) {
      results$valid <- FALSE
      dup_count <- dt2[, .N, by = keys][N > 1, sum(N) - .N]
      results$issues <- c(results$issues, 
        sprintf("dt2 (right side) has non-unique keys (%d duplicate rows) for many-to-one join", dup_count))
    }
  }
  
  # Collect diagnostic information
  results$diagnostics$dt1_rows <- nrow(dt1)
  results$diagnostics$dt2_rows <- nrow(dt2)
  results$diagnostics$dt1_unique_keys <- nrow(unique(dt1[, ..keys]))
  results$diagnostics$dt2_unique_keys <- nrow(unique(dt2[, ..keys]))
  results$diagnostics$common_keys <- nrow(fintersect(unique(dt1[, ..keys]), unique(dt2[, ..keys])))
  results$diagnostics$dt1_only_keys <- results$diagnostics$dt1_unique_keys - results$diagnostics$common_keys
  results$diagnostics$dt2_only_keys <- results$diagnostics$dt2_unique_keys - results$diagnostics$common_keys
  
  # Add key distribution information
  if (!dt1_unique) {
    results$diagnostics$dt1_key_distribution <- dt1[, .N, by = keys][N > 1][order(-N)][1:min(10, .N)]
  }
  if (!dt2_unique) {
    results$diagnostics$dt2_key_distribution <- dt2[, .N, by = keys][N > 1][order(-N)][1:min(10, .N)]
  }
  
  class(results) <- "join_validation"
  results
}

#' Print method for join validation results
#' @param x A join validation object
#' @param ... Additional arguments
#' @export
print.join_validation <- function(x, ...) {
  cat("Join Key Validation Result\n")
  cat("==========================\n")
  cat("Status:", ifelse(x$valid, "VALID", "INVALID"), "\n\n")
  
  if (length(x$issues) > 0) {
    cat("Issues Found:\n")
    for (issue in x$issues) {
      cat("  - ", issue, "\n")
    }
    cat("\n")
  }
  
  cat("Diagnostics:\n")
  cat("  Left table rows:", x$diagnostics$dt1_rows, "\n")
  cat("  Right table rows:", x$diagnostics$dt2_rows, "\n")
  cat("  Left unique keys:", x$diagnostics$dt1_unique_keys, "\n")
  cat("  Right unique keys:", x$diagnostics$dt2_unique_keys, "\n")
  cat("  Common keys:", x$diagnostics$common_keys, "\n")
  cat("  Left-only keys:", x$diagnostics$dt1_only_keys, "\n")
  cat("  Right-only keys:", x$diagnostics$dt2_only_keys, "\n")
  
  if (!is.null(x$diagnostics$dt1_key_distribution)) {
    cat("\nTop duplicate keys in left table:\n")
    print(x$diagnostics$dt1_key_distribution)
  }
  
  if (!is.null(x$diagnostics$dt2_key_distribution)) {
    cat("\nTop duplicate keys in right table:\n")
    print(x$diagnostics$dt2_key_distribution)
  }
  
  invisible(x)
}

#' Validate merge results
#' 
#' @param dt_before Data.table before merge
#' @param dt_after Data.table after merge
#' @param dt2 Second data.table that was merged
#' @param keys Character vector of join keys
#' @param merge_type Type of merge: "left", "right", "inner", "full"
#' @return List with validation results
validate_merge_result <- function(dt_before, dt_after, dt2, keys, merge_type = "left") {
  results <- list(
    valid = TRUE,
    issues = character(),
    diagnostics = list()
  )
  
  # Basic row count validation
  results$diagnostics$rows_before <- nrow(dt_before)
  results$diagnostics$rows_after <- nrow(dt_after)
  results$diagnostics$row_difference <- results$diagnostics$rows_after - results$diagnostics$rows_before
  
  # Check for unexpected row changes
  if (merge_type == "left") {
    if (results$diagnostics$rows_after < results$diagnostics$rows_before) {
      results$valid <- FALSE
      results$issues <- c(results$issues, 
        sprintf("Left merge lost %d rows", results$diagnostics$rows_before - results$diagnostics$rows_after))
    }
  } else if (merge_type == "inner") {
    if (results$diagnostics$rows_after > results$diagnostics$rows_before) {
      results$valid <- FALSE
      results$issues <- c(results$issues, 
        sprintf("Inner merge created %d extra rows (possible duplicate keys)", results$diagnostics$row_difference))
    }
  }
  
  # Check for duplicate rows created by merge
  if (results$diagnostics$row_difference > 0) {
    # Check if original keys are still unique
    original_key_cols <- intersect(names(dt_before), keys)
    if (length(original_key_cols) > 0) {
      dt_after_unique <- nrow(unique(dt_after[, ..original_key_cols])) == nrow(dt_after)
      if (!dt_after_unique) {
        results$valid <- FALSE
        dup_count <- dt_after[, .N, by = original_key_cols][N > 1, sum(N) - .N]
        results$issues <- c(results$issues, 
          sprintf("Merge created %d duplicate rows based on original keys", dup_count))
      }
    }
  }
  
  # Check for new NA values introduced
  cols_to_check <- setdiff(names(dt_after), names(dt_before))
  if (length(cols_to_check) > 0) {
    na_counts <- dt_after[, lapply(.SD, function(x) sum(is.na(x))), .SDcols = cols_to_check]
    results$diagnostics$new_na_counts <- na_counts
    
    # Flag if many NAs introduced
    total_nas <- sum(unlist(na_counts))
    if (total_nas > 0.1 * nrow(dt_after) * length(cols_to_check)) {
      results$issues <- c(results$issues, 
        sprintf("Merge introduced significant NAs: %d total across %d columns", total_nas, length(cols_to_check)))
    }
  }
  
  # Check merge completeness
  if (merge_type %in% c("left", "full")) {
    # All original keys should still be present
    original_keys <- unique(dt_before[, ..keys])
    merged_keys <- unique(dt_after[, ..keys])
    missing_keys <- nrow(fsetdiff(original_keys, merged_keys))
    
    if (missing_keys > 0) {
      results$valid <- FALSE
      results$issues <- c(results$issues, 
        sprintf("Merge lost %d unique key combinations from original data", missing_keys))
    }
  }
  
  class(results) <- "merge_validation"
  results
}

#' Print method for merge validation results
#' @param x A merge validation object
#' @param ... Additional arguments
#' @export
print.merge_validation <- function(x, ...) {
  cat("Merge Result Validation\n")
  cat("======================\n")
  cat("Status:", ifelse(x$valid, "VALID", "INVALID"), "\n\n")
  
  cat("Row Count Changes:\n")
  cat("  Before merge:", x$diagnostics$rows_before, "\n")
  cat("  After merge:", x$diagnostics$rows_after, "\n")
  cat("  Difference:", x$diagnostics$row_difference, "\n\n")
  
  if (length(x$issues) > 0) {
    cat("Issues Found:\n")
    for (issue in x$issues) {
      cat("  - ", issue, "\n")
    }
    cat("\n")
  }
  
  if (!is.null(x$diagnostics$new_na_counts)) {
    cat("NA values in new columns:\n")
    print(x$diagnostics$new_na_counts)
  }
  
  invisible(x)
}

#' Create a safe merge wrapper with validation
#' 
#' @param dt1 First data.table
#' @param dt2 Second data.table
#' @param keys Character vector of join keys
#' @param join_type Expected join type for validation
#' @param merge_type Type of merge to perform
#' @param validate_before Logical, whether to validate before merge
#' @param validate_after Logical, whether to validate after merge
#' @param stop_on_error Logical, whether to stop on validation errors
#' @param ... Additional arguments passed to merge
#' @return Merged data.table with validation report as attribute
safe_merge <- function(dt1, dt2, keys, 
                      join_type = "one-to-one",
                      merge_type = "left",
                      validate_before = TRUE,
                      validate_after = TRUE,
                      stop_on_error = TRUE,
                      ...) {
  
  validation_report <- list()
  
  # Pre-merge validation
  if (validate_before) {
    pre_validation <- validate_join_keys(dt1, dt2, keys, join_type)
    validation_report$pre_merge <- pre_validation
    
    if (!pre_validation$valid && stop_on_error) {
      print(pre_validation)
      stop("Pre-merge validation failed. See issues above.")
    }
  }
  
  # Perform merge
  dt1_copy <- copy(dt1)  # Preserve original for comparison
  
  if (merge_type == "left") {
    result <- merge(dt1, dt2, by = keys, all.x = TRUE, ...)
  } else if (merge_type == "right") {
    result <- merge(dt1, dt2, by = keys, all.y = TRUE, ...)
  } else if (merge_type == "inner") {
    result <- merge(dt1, dt2, by = keys, all = FALSE, ...)
  } else if (merge_type == "full") {
    result <- merge(dt1, dt2, by = keys, all = TRUE, ...)
  } else {
    stop("Unknown merge_type. Use 'left', 'right', 'inner', or 'full'.")
  }
  
  # Post-merge validation
  if (validate_after) {
    post_validation <- validate_merge_result(dt1_copy, result, dt2, keys, merge_type)
    validation_report$post_merge <- post_validation
    
    if (!post_validation$valid && stop_on_error) {
      print(post_validation)
      stop("Post-merge validation failed. See issues above.")
    }
  }
  
  # Attach validation report as attribute
  attr(result, "validation_report") <- validation_report
  
  # Print summary if any issues found
  if (validate_before && !validation_report$pre_merge$valid) {
    message("Warning: Pre-merge validation found issues (see validation_report attribute)")
  }
  if (validate_after && !validation_report$post_merge$valid) {
    message("Warning: Post-merge validation found issues (see validation_report attribute)")
  }
  
  result
}

#' Extract and print validation report from a merge result
#' 
#' @param dt Data.table with validation_report attribute
#' @export
get_merge_validation_report <- function(dt) {
  report <- attr(dt, "validation_report")
  if (is.null(report)) {
    cat("No validation report found for this data.table\n")
    return(invisible(NULL))
  }
  
  if (!is.null(report$pre_merge)) {
    cat("\n")
    print(report$pre_merge)
  }
  
  if (!is.null(report$post_merge)) {
    cat("\n")
    print(report$post_merge)
  }
  
  invisible(report)
}

#' Define validation rules for specific merge operations in the pipeline
#' 
#' @param merge_name Name of the merge operation
#' @return List with expected join configuration
get_merge_validation_config <- function(merge_name) {
  configs <- list(
    # CNEFE with municipality IDs
    cnefe_muni = list(
      keys = "id_munic_7",
      join_type = "many-to-one",
      critical_columns = c("id_munic_6", "id_estado", "estado_abrev")
    ),
    
    # TSE geocoded with municipality IDs  
    tse_muni = list(
      keys = c("cd_municipio" = "id_TSE"),
      join_type = "many-to-one",
      critical_columns = c("id_munic_7", "municipio")
    ),
    
    # Locais with coordinates
    locais_coords = list(
      keys = "local_id",
      join_type = "one-to-one",
      critical_columns = c("latitude", "longitude", "match_type")
    ),
    
    # Panel ID joins
    panel_join = list(
      keys = "local_id",
      join_type = "one-to-one", 
      critical_columns = c("panel_id")
    ),
    
    # Composite key example (polling station identification)
    polling_station = list(
      keys = c("ano", "cod_localidade_ibge", "nr_zona", "nr_locvot"),
      join_type = "one-to-one",
      critical_columns = c("nm_local", "ds_endereco")
    )
  )
  
  if (merge_name %in% names(configs)) {
    configs[[merge_name]]
  } else {
    warning(sprintf("No validation config found for merge '%s'. Using defaults.", merge_name))
    list(
      keys = NULL,
      join_type = "one-to-one",
      critical_columns = character()
    )
  }
}

#' Create validation rules for merged datasets
#' 
#' @param merge_name Name of the merge operation
#' @return A validator object
create_merge_validation_rules <- function(merge_name) {
  config <- get_merge_validation_config(merge_name)
  
  # Base rules that apply to all merges
  base_rules <- validator(
    # No complete duplicate rows
    no_complete_duplicates = !any(duplicated(.))
  )
  
  # Add merge-specific rules based on config
  if (!is.null(config$critical_columns)) {
    critical_col_rules <- validator(
      # Critical columns should not be mostly NA
      critical_cols_not_mostly_na = all(colMeans(is.na(.SD)) < 0.9, na.rm = TRUE),
      .SDcols = config$critical_columns
    )
  }
  
  # Combine rules (would need to be done programmatically in practice)
  base_rules
}