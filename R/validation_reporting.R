# Validation Reporting Functions
#
# This file contains functions to process validation results and
# export failed records for analysis

library(data.table)

#' Extract Failed Records from Validation Result
#' 
#' @description Extracts records that failed validation rules with details
#' @param validation_result A validation result object from validate_*_stage()
#' @param data The original data that was validated
#' @param max_records Maximum number of failed records to return (default: 1000)
#' @return A data.table with failed records and failure details
#' @export
extract_failed_records <- function(validation_result, data, max_records = 1000) {
  if (!inherits(validation_result, "validation_result")) {
    stop("Input must be a validation_result object")
  }
  
  # Get validation summary
  summary_df <- summary(validation_result$result)
  failed_rules <- summary_df[summary_df$fails > 0, ]
  
  if (nrow(failed_rules) == 0) {
    return(data.table())
  }
  
  # Get validation values matrix
  values_mat <- values(validation_result$result)
  
  # Initialize results
  all_failed_records <- list()
  
  for (i in 1:nrow(failed_rules)) {
    rule_name <- failed_rules$name[i]
    rule_expr <- failed_rules$expression[i]
    
    # Find records that failed this rule
    # values_mat is a matrix with rules as columns
    if (rule_name %in% colnames(values_mat)) {
      rule_values <- values_mat[, rule_name]
      failed_idx <- which(rule_values == FALSE)
      
      if (length(failed_idx) > 0) {
        # Limit number of records
        if (length(failed_idx) > max_records) {
          failed_idx <- failed_idx[1:max_records]
        }
        
        # Extract failed records
        failed_data <- data[failed_idx, ]
        failed_data$failed_rule <- rule_name
        failed_data$rule_expression <- rule_expr
        failed_data$record_index <- failed_idx
        
        all_failed_records[[rule_name]] <- failed_data
      }
    }
  }
  
  if (length(all_failed_records) > 0) {
    result <- rbindlist(all_failed_records, fill = TRUE)
    
    # Reorder columns
    rule_cols <- c("failed_rule", "rule_expression", "record_index")
    data_cols <- setdiff(names(result), rule_cols)
    setcolorder(result, c(rule_cols, data_cols))
    
    return(result)
  } else {
    return(data.table())
  }
}

#' Summarize Validation Results for Reporting
#' 
#' @description Creates summary statistics for a validation result
#' @param validation_result A validation result object
#' @return A list with summary statistics
#' @export
summarize_validation_result <- function(validation_result) {
  if (!inherits(validation_result, "validation_result")) {
    stop("Input must be a validation_result object")
  }
  
  summary_df <- summary(validation_result$result)
  
  # Calculate overall statistics
  total_rules <- nrow(summary_df)
  failed_rules <- sum(summary_df$fails > 0, na.rm = TRUE)
  passed_rules <- sum(summary_df$fails == 0 & !is.na(summary_df$fails))
  na_rules <- sum(is.na(summary_df$fails))
  
  # Get rule details
  rule_details <- data.table(
    rule = summary_df$name,
    expression = summary_df$expression,
    items_checked = summary_df$items,
    failures = summary_df$fails,
    passes = summary_df$passes,
    na_values = summary_df$nNA,
    error = summary_df$error,
    warning = summary_df$warning
  )
  
  # Additional check results
  additional_summary <- NULL
  if (!is.null(validation_result$additional_checks)) {
    additional_summary <- data.table(
      check_name = names(validation_result$additional_checks),
      passed = unlist(validation_result$additional_checks)
    )
  }
  
  # Create summary object
  summary_obj <- list(
    stage_name = validation_result$metadata$stage,
    stage_type = validation_result$metadata$type,
    timestamp = validation_result$metadata$timestamp,
    overall_passed = validation_result$passed,
    total_rules = total_rules,
    passed_rules = passed_rules,
    failed_rules = failed_rules,
    na_rules = na_rules,
    pass_rate = ifelse(total_rules > 0, 
                      (passed_rules / total_rules) * 100, 
                      100),
    rule_details = rule_details,
    additional_checks = additional_summary,
    metadata = validation_result$metadata
  )
  
  class(summary_obj) <- c("validation_summary", "list")
  return(summary_obj)
}

#' Create Validation Failure Pattern Analysis
#' 
#' @description Analyzes patterns in validation failures
#' @param failed_records Data.table of failed records from extract_failed_records
#' @param group_by Column(s) to group failures by
#' @return A data.table with failure pattern analysis
#' @export
analyze_failure_patterns <- function(failed_records, group_by = NULL) {
  if (nrow(failed_records) == 0) {
    return(data.table())
  }
  
  # Overall failure patterns
  overall_patterns <- failed_records[, .(
    n_failures = .N,
    pct_of_total = (.N / nrow(failed_records)) * 100
  ), by = failed_rule][order(-n_failures)]
  
  # Group-specific patterns if requested
  grouped_patterns <- NULL
  if (!is.null(group_by) && all(group_by %in% names(failed_records))) {
    grouped_patterns <- failed_records[, .(
      n_failures = .N,
      unique_records = length(unique(record_index))
    ), by = c("failed_rule", group_by)][order(-n_failures)]
  }
  
  return(list(
    overall = overall_patterns,
    grouped = grouped_patterns
  ))
}

#' Generate Action Items from Validation Failures
#' 
#' @description Creates actionable recommendations based on validation failures
#' @param validation_summary Summary object from summarize_validation_result
#' @param failed_records Failed records from extract_failed_records
#' @return A data.table with prioritized action items
#' @export
generate_action_items <- function(validation_summary, failed_records = NULL) {
  action_items <- list()
  
  # Get failed rules
  failed_rules <- validation_summary$rule_details[failures > 0]
  
  if (nrow(failed_rules) == 0) {
    return(data.table(
      priority = character(),
      rule = character(),
      action = character(),
      details = character()
    ))
  }
  
  # Generate actions based on rule names and types
  for (i in 1:nrow(failed_rules)) {
    rule <- failed_rules[i]
    
    # Determine priority based on failure count
    if (rule$failures > rule$items_checked * 0.5) {
      priority <- "HIGH"
    } else if (rule$failures > rule$items_checked * 0.1) {
      priority <- "MEDIUM"
    } else {
      priority <- "LOW"
    }
    
    # Generate specific actions based on rule patterns
    action <- generate_rule_specific_action(rule$rule, rule$expression)
    
    action_items[[i]] <- data.table(
      priority = priority,
      rule = rule$rule,
      failures = rule$failures,
      failure_rate = sprintf("%.1f%%", (rule$failures / rule$items_checked) * 100),
      action = action$action,
      details = action$details,
      fix_command = action$fix_command
    )
  }
  
  # Combine and sort by priority
  result <- rbindlist(action_items)
  priority_order <- c("HIGH", "MEDIUM", "LOW")
  result[, priority := factor(priority, levels = priority_order)]
  setorder(result, priority, -failures)
  
  return(result)
}

#' Generate Rule-Specific Actions
#' 
#' @description Helper function to generate specific actions based on rule name
#' @param rule_name Name of the failed rule
#' @param rule_expr Expression of the rule
#' @return List with action, details, and fix command
generate_rule_specific_action <- function(rule_name, rule_expr) {
  # Default action
  action <- list(
    action = "Review and fix data",
    details = paste("Records failed rule:", rule_expr),
    fix_command = ""
  )
  
  # Specific actions based on common patterns
  if (grepl("has_column_|has_required_", rule_name)) {
    col_name <- gsub("has_column_|has_required_", "", rule_name)
    action <- list(
      action = paste("Add missing column:", col_name),
      details = paste("Required column", col_name, "is missing from the dataset"),
      fix_command = paste0("data[, ", col_name, " := NA]  # Add column with appropriate values")
    )
  } else if (grepl("valid_lat|valid_lon|valid_coords", rule_name, ignore.case = TRUE)) {
    action <- list(
      action = "Fix coordinate values",
      details = "Coordinates are outside valid Brazilian boundaries",
      fix_command = "data[!between(lat, -33.75, 5.27) | !between(long, -73.99, -34.79), `:=`(lat = NA, long = NA)]"
    )
  } else if (grepl("no_na_|no_missing_", rule_name)) {
    col_name <- gsub("no_na_|no_missing_", "", rule_name)
    action <- list(
      action = paste("Handle missing values in:", col_name),
      details = paste("Column", col_name, "contains NA values"),
      fix_command = paste0("# Option 1: Remove rows with NA\ndata <- data[!is.na(", col_name, ")]\n",
                          "# Option 2: Fill with default\ndata[is.na(", col_name, "), ", col_name, " := 'default_value']")
    )
  } else if (grepl("unique_|no_duplicates", rule_name)) {
    action <- list(
      action = "Remove duplicate records",
      details = "Dataset contains duplicate key values",
      fix_command = "data <- unique(data, by = c('key_columns'))"
    )
  } else if (grepl("match_rate|has_matches", rule_name)) {
    action <- list(
      action = "Improve string matching parameters",
      details = "Low match rate in string matching stage",
      fix_command = "# Consider adjusting string distance threshold or matching algorithm"
    )
  }
  
  return(action)
}

#' Export Failed Records to CSV
#' 
#' @description Exports failed records to CSV files for manual review
#' @param failed_records Data.table of failed records from extract_failed_records
#' @param output_dir Directory to save CSV files
#' @param stage_name Name of the validation stage
#' @return Path to created CSV file
#' @export
export_failed_records <- function(failed_records, output_dir = "output/validation_failures", stage_name = "records") {
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  if (nrow(failed_records) > 0) {
    filename <- file.path(output_dir, 
                         paste0("failed_", stage_name, "_", 
                               format(Sys.Date(), "%Y%m%d"), ".csv"))
    
    fwrite(failed_records, filename)
    return(filename)
  }
  
  return(NULL)
}