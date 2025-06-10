# Modular Validation Functions for Pipeline Stages
#
# This file contains validation functions that can be invoked after each
# stage of the data pipeline to ensure data integrity and quality.

library(validate)
library(data.table)

#' Validate Data Import Stage
#' 
#' @description Validates data immediately after import to catch loading errors
#' @param data The imported data.table
#' @param stage_name Name of the import stage (e.g., "cnefe10_raw", "locais")
#' @param expected_cols Character vector of expected column names
#' @param min_rows Minimum expected number of rows
#' @return A validation result object
#' @export
validate_import_stage <- function(data, stage_name, expected_cols = NULL, min_rows = 1) {
  # Create import-specific rules
  base_rules <- list(
    # Basic structure checks  
    has_rows = substitute(nrow(.) >= min_rows_val, list(min_rows_val = min_rows)),
    not_empty = quote(nrow(.) > 0)
  )
  
  # Add column checks if expected columns provided
  if (!is.null(expected_cols)) {
    for (col in expected_cols) {
      rule_name <- paste0("has_column_", col)
      base_rules[[rule_name]] <- substitute(col %in% names(.), list(col = col))
    }
  }
  
  rules <- do.call(validator, base_rules)
  
  # Run validation
  result <- confront(data, rules)
  
  # Create metadata
  metadata <- list(
    stage = stage_name,
    type = "import",
    timestamp = Sys.time(),
    n_rows = nrow(data),
    n_cols = ncol(data),
    columns = names(data)
  )
  
  # Additional checks outside validate DSL
  additional_checks <- list()
  additional_checks$is_data_table <- inherits(data, "data.table")
  
  # Check if all rules passed
  summary_df <- summary(result)
  rule_passed <- all(summary_df$fails == 0, na.rm = TRUE)
  additional_passed <- all(unlist(additional_checks), na.rm = TRUE)
  passed <- rule_passed && additional_passed
  
  # Return validation result
  validation_output <- list(
    result = result,
    metadata = metadata,
    passed = passed,
    additional_checks = additional_checks
  )
  
  class(validation_output) <- "validation_result"
  validation_output
}

#' Validate Data Cleaning Stage
#' 
#' @description Validates data after cleaning transformations
#' @param cleaned_data The cleaned data.table
#' @param original_data The original data.table (for comparison)
#' @param stage_name Name of the cleaning stage
#' @param key_cols Character vector of key columns that should be preserved
#' @return A validation result object
#' @export
validate_cleaning_stage <- function(cleaned_data, original_data = NULL, 
                                  stage_name, key_cols = NULL) {
  # Create cleaning-specific rules
  base_rules <- list(
    # No complete loss of data
    has_rows = quote(nrow(.) > 0),
    
    # Check for normalized columns (common in cleaning)
    has_normalized_cols = quote(any(grepl("^norm_|_norm$|normalized", names(.))))
  )
  
  # Add key column preservation checks
  if (!is.null(key_cols)) {
    for (col in key_cols) {
      rule_name <- paste0("preserved_", col)
      # Use simple column existence check that validate DSL supports
      base_rules[[rule_name]] <- substitute(
        col %in% names(.) && any(!is.na(get(col))), 
        list(col = col)
      )
    }
  }
  
  rules <- do.call(validator, base_rules)
  
  # Run validation
  result <- confront(cleaned_data, rules)
  
  # Create metadata
  metadata <- list(
    stage = stage_name,
    type = "cleaning",
    timestamp = Sys.time(),
    n_rows = nrow(cleaned_data),
    n_cols = ncol(cleaned_data)
  )
  
  # Add comparison metrics if original data provided
  if (!is.null(original_data)) {
    metadata$original_rows <- nrow(original_data)
    metadata$rows_removed <- nrow(original_data) - nrow(cleaned_data)
    metadata$removal_rate <- (metadata$rows_removed / metadata$original_rows) * 100
  }
  
  # Additional checks outside validate DSL
  additional_checks <- list()
  additional_checks$no_empty_columns <- !any(sapply(cleaned_data, function(x) all(is.na(x))))
  
  # Check if all rules passed
  summary_df <- summary(result)
  rule_passed <- all(summary_df$fails == 0, na.rm = TRUE)
  additional_passed <- all(unlist(additional_checks), na.rm = TRUE)
  passed <- rule_passed && additional_passed
  
  # Return validation result
  validation_output <- list(
    result = result,
    metadata = metadata,
    passed = passed,
    additional_checks = additional_checks
  )
  
  class(validation_output) <- "validation_result"
  validation_output
}

#' Validate String Matching Stage
#' 
#' @description Validates results from string matching operations
#' @param match_data The string match results data.table
#' @param stage_name Name of the matching stage
#' @param id_col Column name for the ID being matched
#' @param score_col Column name for match scores (if applicable)
#' @return A validation result object
#' @export
validate_string_match_stage <- function(match_data, stage_name, 
                                      id_col = "local_id", 
                                      score_col = NULL) {
  # Create string matching-specific rules
  base_rules <- list(
    # Basic structure
    has_rows = quote(nrow(.) > 0),
    
    # ID column checks
    has_id_column = substitute(id_col %in% names(.), list(id_col = id_col)),
    
    # Should have coordinate columns from matches
    has_match_coords = quote(any(grepl("lat|long|longitude", names(.), ignore.case = TRUE)))
  )
  
  # Add score validation if score column provided
  if (!is.null(score_col) && score_col %in% names(match_data)) {
    base_rules$has_score_column <- substitute(
      score_col %in% names(.), 
      list(score_col = score_col)
    )
    # Note: validate DSL doesn't support get() in rules, score validation done in additional_checks
  }
  
  rules <- do.call(validator, base_rules)
  
  # Run validation
  result <- confront(match_data, rules)
  
  # Create metadata
  metadata <- list(
    stage = stage_name,
    type = "string_matching",
    timestamp = Sys.time(),
    n_rows = nrow(match_data),
    n_unique_ids = ifelse(id_col %in% names(match_data), 
                         length(unique(match_data[[id_col]])), 
                         NA),
    match_rate = {
      lat_col <- intersect(names(match_data), c("lat", "latitude"))
      if (length(lat_col) > 0) {
        (sum(!is.na(match_data[[lat_col[1]]]), na.rm = TRUE) / nrow(match_data)) * 100
      } else {
        0
      }
    }
  )
  
  # Additional checks outside validate DSL
  additional_checks <- list()
  
  # Check for missing IDs
  if (id_col %in% names(match_data)) {
    additional_checks$no_missing_ids <- !any(is.na(match_data[[id_col]]))
  }
  
  # Check score validity if score column exists
  if (!is.null(score_col) && score_col %in% names(match_data)) {
    scores <- match_data[[score_col]]
    # Check that non-NA scores are in valid range
    non_na_scores <- scores[!is.na(scores)]
    if (length(non_na_scores) > 0) {
      additional_checks$valid_scores <- all(non_na_scores >= 0 & non_na_scores <= 1)
    } else {
      additional_checks$valid_scores <- TRUE  # No non-NA scores to check
    }
  }
  
  # Check if all rules passed
  summary_df <- summary(result)
  rule_passed <- all(summary_df$fails == 0, na.rm = TRUE)
  
  # Include additional checks in pass/fail determination
  additional_passed <- if (length(additional_checks) > 0) {
    all(unlist(additional_checks), na.rm = TRUE)
  } else {
    TRUE
  }
  
  passed <- rule_passed && additional_passed
  
  # Return validation result
  validation_output <- list(
    result = result,
    metadata = metadata,
    passed = passed,
    additional_checks = additional_checks
  )
  
  class(validation_output) <- "validation_result"
  validation_output
}

#' Validate Merge Operation
#' 
#' @description Validates data after merge operations
#' @param merged_data The merged data.table
#' @param left_data Original left data.table
#' @param right_data Original right data.table
#' @param stage_name Name of the merge stage
#' @param merge_keys Character vector of merge key columns
#' @param join_type Type of join performed ("left", "inner", "full")
#' @return A validation result object
#' @export
validate_merge_stage <- function(merged_data, left_data, right_data,
                               stage_name, merge_keys, 
                               join_type = "left") {
  # Create merge-specific rules
  base_rules <- list(
    # Basic checks
    has_rows = quote(nrow(.) > 0)
  )
  
  # Add merge key checks
  if (!is.null(merge_keys)) {
    for (key in merge_keys) {
      rule_name <- paste0("has_key_", key)
      base_rules[[rule_name]] <- substitute(
        key %in% names(.),
        list(key = key)
      )
    }
  }
  
  rules <- do.call(validator, base_rules)
  
  # Run validation
  result <- confront(merged_data, rules)
  
  # Calculate merge statistics
  n_left <- nrow(left_data)
  n_right <- if (!is.null(right_data)) nrow(right_data) else NA
  n_merged <- nrow(merged_data)
  
  # Expected rows based on join type
  if (join_type == "left") {
    expected_range <- c(n_left, n_left)
  } else if (join_type == "left_many") {
    # For one-to-many joins (e.g., fuzzy matching), expect at least n_left rows
    expected_range <- c(n_left, Inf)
  } else if (join_type == "inner") {
    expected_range <- c(0, if (!is.na(n_right)) min(n_left, n_right) else n_left)
  } else if (join_type == "full") {
    expected_range <- c(
      if (!is.na(n_right)) max(n_left, n_right) else n_left,
      if (!is.na(n_right)) n_left + n_right else n_left
    )
  } else {
    expected_range <- c(NA, NA)
  }
  
  # Create metadata
  metadata <- list(
    stage = stage_name,
    type = "merge",
    timestamp = Sys.time(),
    join_type = join_type,
    merge_keys = merge_keys,
    n_left = n_left,
    n_right = n_right,
    n_merged = n_merged,
    expected_range = expected_range,
    within_expected = n_merged >= expected_range[1] & n_merged <= expected_range[2]
  )
  
  # Additional checks outside validate DSL
  additional_checks <- list()
  
  # Check merge keys for NAs
  if (!is.null(merge_keys) && all(merge_keys %in% names(merged_data))) {
    for (key in merge_keys) {
      check_name <- paste0("no_na_", key)
      additional_checks[[check_name]] <- !any(is.na(merged_data[[key]]))
    }
  }
  
  # Check if all rules passed and merge produced expected results
  summary_df <- summary(result)
  rule_passed <- all(summary_df$fails == 0, na.rm = TRUE)
  additional_passed <- all(unlist(additional_checks), na.rm = TRUE)
  passed <- rule_passed && additional_passed && metadata$within_expected
  
  # Return validation result
  validation_output <- list(
    result = result,
    metadata = metadata,
    passed = passed,
    additional_checks = additional_checks
  )
  
  class(validation_output) <- "validation_result"
  validation_output
}

#' Validate Model Prediction Stage
#' 
#' @description Validates model predictions
#' @param predictions The predictions data.table
#' @param stage_name Name of the prediction stage
#' @param pred_col Column name for predictions
#' @param prob_col Column name for probabilities (optional)
#' @return A validation result object
#' @export
validate_prediction_stage <- function(predictions, stage_name,
                                    pred_col = "prediction",
                                    prob_col = NULL) {
  # Create prediction-specific rules
  base_rules <- list(
    # Basic checks
    has_rows = quote(nrow(.) > 0),
    
    # Prediction column checks
    has_pred_column = substitute(pred_col %in% names(.), list(pred_col = pred_col))
  )
  
  # Add probability checks if applicable
  if (!is.null(prob_col) && prob_col %in% names(predictions)) {
    base_rules$has_prob_column <- substitute(
      prob_col %in% names(.), 
      list(prob_col = prob_col)
    )
    # Note: validate DSL doesn't support get() in rules, probability validation done in additional_checks
  }
  
  rules <- do.call(validator, base_rules)
  
  # Run validation
  result <- confront(predictions, rules)
  
  # Create metadata
  metadata <- list(
    stage = stage_name,
    type = "prediction",
    timestamp = Sys.time(),
    n_rows = nrow(predictions),
    n_predictions = sum(!is.na(predictions[[pred_col]]))
  )
  
  if (!is.null(prob_col) && prob_col %in% names(predictions)) {
    metadata$mean_probability <- mean(predictions[[prob_col]], na.rm = TRUE)
    metadata$median_probability <- median(predictions[[prob_col]], na.rm = TRUE)
  }
  
  # Additional checks outside validate DSL
  additional_checks <- list()
  
  # Check prediction completeness
  if (pred_col %in% names(predictions)) {
    additional_checks$has_predictions <- !all(is.na(predictions[[pred_col]]))
  }
  
  # Check probability validity
  if (!is.null(prob_col) && prob_col %in% names(predictions)) {
    probs <- predictions[[prob_col]]
    additional_checks$valid_probabilities <- all(probs >= 0 & probs <= 1, na.rm = TRUE)
  }
  
  # Check if all rules passed
  summary_df <- summary(result)
  rule_passed <- all(summary_df$fails == 0, na.rm = TRUE)
  additional_passed <- all(unlist(additional_checks), na.rm = TRUE)
  passed <- rule_passed && additional_passed
  
  # Return validation result
  validation_output <- list(
    result = result,
    metadata = metadata,
    passed = passed,
    additional_checks = additional_checks
  )
  
  class(validation_output) <- "validation_result"
  validation_output
}

#' Validate Final Output Stage
#' 
#' @description Validates final output data before export
#' @param output_data The final output data.table
#' @param stage_name Name of the output stage
#' @param required_cols Character vector of required columns
#' @param unique_keys Character vector of columns that should form unique keys
#' @return A validation result object
#' @export
validate_output_stage <- function(output_data, stage_name,
                                required_cols = NULL,
                                unique_keys = NULL) {
  # Create output-specific rules
  base_rules <- list(
    # Basic checks
    has_rows = quote(nrow(.) > 0),
    
    # Basic completeness check
    has_data = quote(ncol(.) > 0)
  )
  
  # Add required column checks
  if (!is.null(required_cols)) {
    for (col in required_cols) {
      rule_name <- paste0("has_required_", col)
      base_rules[[rule_name]] <- substitute(
        col %in% names(.),
        list(col = col)
      )
    }
  }
  
  # Add uniqueness checks - validate DSL doesn't support .SD, so we check columns exist
  if (!is.null(unique_keys)) {
    for (key in unique_keys) {
      rule_name <- paste0("has_unique_key_", key)
      base_rules[[rule_name]] <- substitute(key %in% names(.), list(key = key))
    }
  }
  
  rules <- do.call(validator, base_rules)
  
  # Run validation
  result <- confront(output_data, rules)
  
  # Create metadata
  metadata <- list(
    stage = stage_name,
    type = "output",
    timestamp = Sys.time(),
    n_rows = nrow(output_data),
    n_cols = ncol(output_data),
    columns = names(output_data)
  )
  
  # Additional checks outside validate DSL
  additional_checks <- list()
  
  # Check if ALL columns are NA (not just some)
  all_cols_na <- all(sapply(output_data, function(x) all(is.na(x))))
  additional_checks$not_entirely_empty <- !all_cols_na
  
  # Check required columns have data
  if (!is.null(required_cols)) {
    for (col in required_cols) {
      if (col %in% names(output_data)) {
        check_name <- paste0("has_data_", col)
        additional_checks[[check_name]] <- !all(is.na(output_data[[col]]))
      }
    }
  }
  
  if (!is.null(unique_keys) && all(unique_keys %in% names(output_data))) {
    metadata$n_unique_keys <- nrow(unique(output_data[, .SD, .SDcols = unique_keys]))
    metadata$has_duplicates <- metadata$n_unique_keys < metadata$n_rows
    additional_checks$unique_records <- !metadata$has_duplicates
  }
  
  # Check if all rules passed
  summary_df <- summary(result)
  rule_passed <- all(summary_df$fails == 0, na.rm = TRUE)
  additional_passed <- all(unlist(additional_checks), na.rm = TRUE)
  passed <- rule_passed && additional_passed
  
  # Return validation result
  validation_output <- list(
    result = result,
    metadata = metadata,
    passed = passed,
    additional_checks = additional_checks
  )
  
  class(validation_output) <- "validation_result"
  validation_output
}

#' Create Pipeline Validation Report
#' 
#' @description Creates a comprehensive report of all validation results
#' @param validation_results List of validation result objects
#' @param output_file Optional file path to save report
#' @return NULL (prints report and optionally saves to file)
#' @export
create_pipeline_validation_report <- function(validation_results, 
                                            output_file = NULL) {
  report_lines <- character()
  
  # Header
  report_lines <- c(report_lines,
    "================================================================================",
    "PIPELINE VALIDATION REPORT",
    paste("Generated:", Sys.time()),
    "================================================================================",
    ""
  )
  
  # Summary statistics
  n_stages <- length(validation_results)
  n_passed <- sum(sapply(validation_results, function(x) x$passed))
  n_failed <- n_stages - n_passed
  
  report_lines <- c(report_lines,
    "SUMMARY",
    "-------",
    paste("Total stages validated:", n_stages),
    paste("Passed:", n_passed),
    paste("Failed:", n_failed),
    paste("Success rate:", sprintf("%.1f%%", (n_passed/n_stages)*100)),
    ""
  )
  
  # Stage details
  report_lines <- c(report_lines,
    "STAGE DETAILS",
    "-------------"
  )
  
  for (i in seq_along(validation_results)) {
    val_result <- validation_results[[i]]
    
    report_lines <- c(report_lines,
      "",
      paste0("Stage ", i, ": ", val_result$metadata$stage),
      paste("  Type:", val_result$metadata$type),
      paste("  Status:", ifelse(val_result$passed, "✓ PASSED", "✗ FAILED")),
      paste("  Rows:", format(val_result$metadata$n_rows, big.mark = ","))
    )
    
    # Add type-specific information
    if (val_result$metadata$type == "merge") {
      report_lines <- c(report_lines,
        paste("  Join type:", val_result$metadata$join_type),
        paste("  Left rows:", format(val_result$metadata$n_left, big.mark = ",")),
        paste("  Right rows:", format(val_result$metadata$n_right, big.mark = ","))
      )
    } else if (val_result$metadata$type == "string_matching") {
      report_lines <- c(report_lines,
        paste("  Match rate:", sprintf("%.1f%%", val_result$metadata$match_rate))
      )
    } else if (val_result$metadata$type == "cleaning" && !is.null(val_result$metadata$removal_rate)) {
      report_lines <- c(report_lines,
        paste("  Rows removed:", format(val_result$metadata$rows_removed, big.mark = ",")),
        paste("  Removal rate:", sprintf("%.1f%%", val_result$metadata$removal_rate))
      )
    }
    
    # Show failed rules if any
    if (!val_result$passed) {
      # Try to get summary, handle if it doesn't exist (e.g., mock objects)
      summary_df <- tryCatch({
        summary(val_result$result)
      }, error = function(e) {
        NULL
      })
      
      if (!is.null(summary_df) && is.data.frame(summary_df)) {
        failed_rules <- summary_df[summary_df$fails > 0 | is.na(summary_df$fails), ]
        
        if (nrow(failed_rules) > 0) {
          report_lines <- c(report_lines, "  Failed rules:")
          for (j in 1:nrow(failed_rules)) {
            report_lines <- c(report_lines,
              paste0("    - ", failed_rules$name[j], 
                     " (", failed_rules$fails[j], " failures)")
            )
          }
        }
      }
      
      # Show failed additional checks if any
      if (!is.null(val_result$additional_checks)) {
        failed_checks <- val_result$additional_checks[!unlist(val_result$additional_checks)]
        if (length(failed_checks) > 0) {
          report_lines <- c(report_lines, "  Failed additional checks:")
          for (check_name in names(failed_checks)) {
            report_lines <- c(report_lines, paste0("    - ", check_name))
          }
        }
      }
    }
  }
  
  # Footer
  report_lines <- c(report_lines,
    "",
    "================================================================================",
    "END OF REPORT",
    "================================================================================"
  )
  
  # Print report
  cat(paste(report_lines, collapse = "\n"))
  
  # Save to file if requested
  if (!is.null(output_file)) {
    writeLines(report_lines, output_file)
    cat("\n\nReport saved to:", output_file, "\n")
  }
  
  invisible(NULL)
}

#' Run Full Pipeline Validation
#' 
#' @description Convenience function to run all validations for a pipeline
#' @param stages List of stage data and metadata
#' @return List of validation results
#' @export
run_pipeline_validation <- function(stages) {
  validation_results <- list()
  
  for (stage in stages) {
    # Determine validation function based on stage type
    val_result <- switch(stage$type,
      "import" = validate_import_stage(
        data = stage$data,
        stage_name = stage$name,
        expected_cols = stage$expected_cols,
        min_rows = stage$min_rows %||% 1
      ),
      "cleaning" = validate_cleaning_stage(
        cleaned_data = stage$data,
        original_data = stage$original_data,
        stage_name = stage$name,
        key_cols = stage$key_cols
      ),
      "string_match" = validate_string_match_stage(
        match_data = stage$data,
        stage_name = stage$name,
        id_col = stage$id_col %||% "local_id",
        score_col = stage$score_col
      ),
      "merge" = validate_merge_stage(
        merged_data = stage$data,
        left_data = stage$left_data,
        right_data = stage$right_data,
        stage_name = stage$name,
        merge_keys = stage$merge_keys,
        join_type = stage$join_type %||% "left"
      ),
      "prediction" = validate_prediction_stage(
        predictions = stage$data,
        stage_name = stage$name,
        pred_col = stage$pred_col %||% "prediction",
        prob_col = stage$prob_col
      ),
      "output" = validate_output_stage(
        output_data = stage$data,
        stage_name = stage$name,
        required_cols = stage$required_cols,
        unique_keys = stage$unique_keys
      )
    )
    
    validation_results[[stage$name]] <- val_result
  }
  
  validation_results
}

#' Validate rbindlist Merge Operation
#' 
#' @description Validates data after rbindlist operations (common in parallel processing)
#' @param list_of_results List of data.tables to be merged
#' @param merged_data The result of rbindlist (if already computed)
#' @param stage_name Name of the merge stage
#' @param expected_cols Character vector of expected columns
#' @param fill Whether rbindlist was called with fill=TRUE
#' @return A validation result object with merged data
#' @export
validate_rbindlist_stage <- function(list_of_results = NULL, 
                                   merged_data = NULL,
                                   stage_name, 
                                   expected_cols = NULL,
                                   fill = TRUE) {
  
  # Either provide list_of_results or merged_data, not both
  if (!is.null(list_of_results) && !is.null(merged_data)) {
    stop("Provide either list_of_results or merged_data, not both")
  }
  
  if (is.null(list_of_results) && is.null(merged_data)) {
    stop("Must provide either list_of_results or merged_data")
  }
  
  # If we have the list, perform the merge
  if (!is.null(list_of_results)) {
    # Validation checks before merge
    if (length(list_of_results) == 0) {
      stop(paste(stage_name, ": No results to merge"))
    }
    
    # Check all elements are data.tables or data.frames
    if (!all(sapply(list_of_results, function(x) is.data.frame(x) || is.data.table(x)))) {
      stop(paste(stage_name, ": All elements must be data.frames or data.tables"))
    }
    
    # Calculate pre-merge statistics
    n_chunks <- length(list_of_results)
    rows_per_chunk <- sapply(list_of_results, nrow)
    expected_rows <- sum(rows_per_chunk)
    
    # Perform merge
    merged_data <- data.table::rbindlist(list_of_results, fill = fill)
    actual_rows <- nrow(merged_data)
  } else {
    # We already have merged data, just validate it
    actual_rows <- nrow(merged_data)
    expected_rows <- actual_rows  # We don't know the original
    n_chunks <- NA
    rows_per_chunk <- NA
  }
  
  # Create validation rules
  base_rules <- list(
    # Basic checks
    has_rows = quote(nrow(.) > 0)
  )
  
  # Add column checks if expected
  if (!is.null(expected_cols)) {
    for (col in expected_cols) {
      rule_name <- paste0("has_column_", col)
      base_rules[[rule_name]] <- substitute(col %in% names(.), list(col = col))
    }
  }
  
  rules <- do.call(validator, base_rules)
  
  # Run validation
  result <- confront(merged_data, rules)
  
  # Create metadata
  metadata <- list(
    stage = stage_name,
    type = "rbindlist",
    timestamp = Sys.time(),
    n_chunks = n_chunks,
    expected_rows = expected_rows,
    actual_rows = actual_rows,
    row_difference = actual_rows - expected_rows,
    fill_used = fill
  )
  
  if (!is.null(list_of_results) && length(rows_per_chunk) > 0) {
    # Handle empty results gracefully
    non_na_rows <- rows_per_chunk[!is.na(rows_per_chunk)]
    if (length(non_na_rows) > 0) {
      metadata$min_chunk_rows <- min(non_na_rows)
      metadata$max_chunk_rows <- max(non_na_rows)
      metadata$mean_chunk_rows <- mean(non_na_rows)
    } else {
      # All chunks were empty or NA
      metadata$min_chunk_rows <- 0
      metadata$max_chunk_rows <- 0
      metadata$mean_chunk_rows <- 0
    }
  }
  
  # Additional checks
  additional_checks <- list()
  
  # Check data.table structure
  additional_checks$is_data_table <- inherits(merged_data, "data.table")
  
  # Check we didn't lose rows (only if we have the original list)
  if (!is.null(list_of_results)) {
    additional_checks$no_row_loss <- actual_rows == expected_rows
  }
  
  # Check for duplicate columns (can happen with fill=TRUE)
  dup_cols <- names(merged_data)[duplicated(names(merged_data))]
  additional_checks$no_duplicate_columns <- length(dup_cols) == 0
  if (length(dup_cols) > 0) {
    metadata$duplicate_columns <- dup_cols
  }
  
  # Check if all rules passed
  summary_df <- summary(result)
  rule_passed <- all(summary_df$fails == 0, na.rm = TRUE)
  additional_passed <- all(unlist(additional_checks), na.rm = TRUE)
  passed <- rule_passed && additional_passed
  
  # Return validation result with merged data
  validation_output <- list(
    result = result,
    metadata = metadata,
    passed = passed,
    additional_checks = additional_checks,
    merged_data = merged_data  # Include the merged data for convenience
  )
  
  class(validation_output) <- "validation_result"
  validation_output
}

# Helper function for NULL default
`%||%` <- function(x, y) if (is.null(x)) y else x