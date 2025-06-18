# Core validation stage functions
# Extracted from validation_pipeline_stages.R - contains only the functions used in the pipeline

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
  
  # Additional comparisons if original data provided
  if (!is.null(original_data)) {
    metadata$original_rows <- nrow(original_data)
    metadata$rows_change_pct <- round(100 * (nrow(cleaned_data) - nrow(original_data)) / nrow(original_data), 2)
  }
  
  # Check if all rules passed
  summary_df <- summary(result)
  passed <- all(summary_df$fails == 0, na.rm = TRUE)
  
  # Return validation result
  validation_output <- list(
    result = result,
    metadata = metadata,
    passed = passed
  )
  
  class(validation_output) <- "validation_result"
  validation_output
}

#' Validate String Matching Stage
#' 
#' @description Validates string matching results
#' @param match_data The string match results data.table
#' @param stage_name Name of the string matching stage  
#' @param id_col Column name containing IDs
#' @param score_col Column name containing match scores
#' @param max_score Maximum acceptable score (lower is better for distance metrics)
#' @return A validation result object
#' @export
validate_string_match_stage <- function(match_data, stage_name, 
                                       id_col = "local_id", score_col = "dist",
                                       max_score = Inf) {
  # Create match-specific rules
  base_rules <- list(
    # Should have match results
    has_matches = quote(nrow(.) > 0),
    
    # ID column should exist and be non-empty
    has_id_col = substitute(id_col %in% names(.), list(id_col = id_col)),
    
    # Score column should exist
    has_score_col = substitute(score_col %in% names(.), list(score_col = score_col))
  )
  
  # Add score validation if max_score specified
  if (is.finite(max_score)) {
    base_rules$valid_scores <- substitute(
      all(get(score_col) <= max_val | is.na(get(score_col))),
      list(score_col = score_col, max_val = max_score)
    )
  }
  
  rules <- do.call(validator, base_rules)
  
  # Run validation
  result <- confront(match_data, rules)
  
  # Calculate match statistics
  if (nrow(match_data) > 0 && id_col %in% names(match_data)) {
    n_unique_matches <- length(unique(match_data[[id_col]]))
    match_rate <- round(100 * n_unique_matches / n_unique_matches, 2) # This will be 100 if there are matches
  } else {
    n_unique_matches <- 0
    match_rate <- 0
  }
  
  # Create metadata
  metadata <- list(
    stage = stage_name,
    type = "string_match",
    timestamp = Sys.time(),
    n_rows = nrow(match_data),
    n_unique_matches = n_unique_matches,
    match_rate = match_rate
  )
  
  # Add score statistics if score column exists
  if (score_col %in% names(match_data) && nrow(match_data) > 0) {
    scores <- match_data[[score_col]]
    scores <- scores[!is.na(scores)]
    if (length(scores) > 0) {
      metadata$score_stats <- list(
        min = min(scores),
        median = median(scores),
        mean = mean(scores),
        max = max(scores)
      )
    }
  }
  
  # Check if all rules passed
  summary_df <- summary(result)
  passed <- all(summary_df$fails == 0, na.rm = TRUE)
  
  # Return validation result
  validation_output <- list(
    result = result,
    metadata = metadata,
    passed = passed
  )
  
  class(validation_output) <- "validation_result"
  validation_output
}

#' Validate Merge Stage
#' 
#' @description Validates data after merge operations
#' @param merged_data The merged data.table
#' @param left_data The left data.table in the merge
#' @param right_data The right data.table in the merge (optional)
#' @param stage_name Name of the merge stage
#' @param merge_keys Character vector of merge keys
#' @param join_type Type of join performed ("inner", "left", "right", "full", "left_many")
#' @return A validation result object
#' @export
validate_merge_stage <- function(merged_data, left_data, right_data = NULL,
                                stage_name, merge_keys, join_type = "left") {
  # Create merge-specific rules
  base_rules <- list(
    # Should have results
    has_rows = quote(nrow(.) > 0)
  )
  
  # Add merge key checks
  if (!is.null(merge_keys)) {
    for (key in merge_keys) {
      rule_name <- paste0("has_key_", key)
      base_rules[[rule_name]] <- substitute(key %in% names(.), list(key = key))
    }
  }
  
  rules <- do.call(validator, base_rules)
  
  # Run validation
  result <- confront(merged_data, rules)
  
  # Create metadata
  metadata <- list(
    stage = stage_name,
    type = "merge",
    timestamp = Sys.time(),
    n_rows = nrow(merged_data),
    left_rows = nrow(left_data),
    join_type = join_type,
    merge_keys = merge_keys
  )
  
  if (!is.null(right_data)) {
    metadata$right_rows <- nrow(right_data)
  }
  
  # Calculate merge statistics based on join type
  if (join_type == "inner") {
    metadata$expected_max_rows <- min(nrow(left_data), 
                                     ifelse(is.null(right_data), nrow(left_data), nrow(right_data)))
  } else if (join_type == "left") {
    metadata$expected_rows <- nrow(left_data)
    metadata$match_rate <- round(100 * nrow(merged_data) / nrow(left_data), 2)
  } else if (join_type == "left_many") {
    # For one-to-many joins, we expect at least as many rows as the left table
    metadata$expected_min_rows <- nrow(left_data)
    metadata$expansion_factor <- round(nrow(merged_data) / nrow(left_data), 2)
  }
  
  # Check if all rules passed
  summary_df <- summary(result)
  passed <- all(summary_df$fails == 0, na.rm = TRUE)
  
  # Additional checks for merge quality
  if (join_type == "left" && nrow(merged_data) < nrow(left_data)) {
    warning(paste("Left join resulted in fewer rows than left table:", 
                 nrow(merged_data), "vs", nrow(left_data)))
    passed <- FALSE
  }
  
  # Return validation result
  validation_output <- list(
    result = result,
    metadata = metadata,
    passed = passed
  )
  
  class(validation_output) <- "validation_result"
  validation_output
}

#' Validate Prediction Stage
#' 
#' @description Validates model predictions
#' @param predictions The predictions data.table
#' @param stage_name Name of the prediction stage
#' @param pred_col Column name containing predictions
#' @param prob_col Column name containing probabilities (optional)
#' @return A validation result object
#' @export
validate_prediction_stage <- function(predictions, stage_name,
                                     pred_col = "prediction", prob_col = NULL) {
  # Create prediction-specific rules
  base_rules <- list(
    # Should have predictions
    has_rows = quote(nrow(.) > 0),
    
    # Prediction column should exist
    has_pred_col = substitute(pred_col %in% names(.), list(pred_col = pred_col)),
    
    # No missing predictions
    no_missing_preds = substitute(!any(is.na(get(pred_col))), list(pred_col = pred_col))
  )
  
  # Add probability checks if specified
  if (!is.null(prob_col)) {
    base_rules$has_prob_col <- substitute(prob_col %in% names(.), list(prob_col = prob_col))
    base_rules$valid_probs <- substitute(
      all(get(prob_col) >= 0 & get(prob_col) <= 1, na.rm = TRUE),
      list(prob_col = prob_col)
    )
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
  
  # Add prediction statistics
  if (pred_col %in% names(predictions)) {
    pred_values <- predictions[[pred_col]]
    pred_values <- pred_values[!is.na(pred_values)]
    
    if (is.numeric(pred_values) && length(pred_values) > 0) {
      metadata$pred_stats <- list(
        min = min(pred_values),
        median = median(pred_values),
        mean = mean(pred_values),
        max = max(pred_values)
      )
    }
  }
  
  # Check if all rules passed
  summary_df <- summary(result)
  passed <- all(summary_df$fails == 0, na.rm = TRUE)
  
  # Return validation result
  validation_output <- list(
    result = result,
    metadata = metadata,
    passed = passed
  )
  
  class(validation_output) <- "validation_result"
  validation_output
}

#' Validate Output Stage
#' 
#' @description Validates final output data
#' @param output_data The final output data.table
#' @param stage_name Name of the output stage
#' @param required_cols Character vector of required columns
#' @param unique_keys Character vector of columns that should form unique combinations
#' @return A validation result object
#' @export
validate_output_stage <- function(output_data, stage_name, 
                                 required_cols = NULL, unique_keys = NULL) {
  # Create output-specific rules
  base_rules <- list(
    # Should have data
    has_rows = quote(nrow(.) > 0),
    
    # No complete NA columns
    no_empty_cols = quote(!any(sapply(., function(x) all(is.na(x)))))
  )
  
  # Add required column checks
  if (!is.null(required_cols)) {
    for (col in required_cols) {
      rule_name <- paste0("has_", col)
      base_rules[[rule_name]] <- substitute(col %in% names(.), list(col = col))
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
  
  # Check uniqueness if keys specified
  if (!is.null(unique_keys) && all(unique_keys %in% names(output_data))) {
    n_unique <- nrow(unique(output_data[, ..unique_keys]))
    metadata$n_unique_keys <- n_unique
    metadata$duplicate_keys <- nrow(output_data) - n_unique
    
    if (metadata$duplicate_keys > 0) {
      warning(paste("Found", metadata$duplicate_keys, "duplicate key combinations"))
    }
  }
  
  # Check if all rules passed
  summary_df <- summary(result)
  passed <- all(summary_df$fails == 0, na.rm = TRUE)
  
  # Additional check for duplicates
  if (!is.null(unique_keys) && metadata$duplicate_keys > 0) {
    passed <- FALSE
  }
  
  # Return validation result
  validation_output <- list(
    result = result,
    metadata = metadata,
    passed = passed
  )
  
  class(validation_output) <- "validation_result"
  validation_output
}

#' Print method for validation results
#' @param x A validation result object
#' @param ... Additional arguments
#' @export
print.validation_result <- function(x, ...) {
  cat("Validation Result for:", x$metadata$stage, "\n")
  cat("Type:", x$metadata$type, "\n")
  cat("Timestamp:", format(x$metadata$timestamp), "\n")
  cat("Overall Status:", ifelse(x$passed, "PASSED", "FAILED"), "\n\n")
  
  # Print the summary
  summary_df <- summary(x$result)
  failed_rules <- summary_df[summary_df$fails > 0 | is.na(summary_df$fails), ]
  
  if (nrow(failed_rules) > 0) {
    cat("Failed Rules:\n")
    print(failed_rules[, c("name", "items", "passes", "fails", "nNA")])
  } else {
    cat("All validation rules passed!\n")
  }
  
  cat("\nMetadata:\n")
  # Print relevant metadata excluding result and timestamp
  meta_to_print <- x$metadata[!names(x$metadata) %in% c("timestamp", "stage", "type")]
  for (name in names(meta_to_print)) {
    value <- meta_to_print[[name]]
    if (is.list(value)) {
      cat(" ", name, ":\n", sep = "")
      for (subname in names(value)) {
        cat("   ", subname, ": ", value[[subname]], "\n", sep = "")
      }
    } else {
      cat(" ", name, ": ", value, "\n", sep = "")
    }
  }
  
  invisible(x)
}