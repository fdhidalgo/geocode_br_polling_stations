#' Column Mapping Functions for Data Standardization
#' 
#' These functions provide a centralized way to handle column name variations
#' across different data sources in the Brazilian polling stations project.

#' Load column mapping configuration
#' 
#' @param config_path Path to the YAML configuration file
#' @return List containing column mapping configuration
#' @export
load_column_mappings <- function(config_path = "config/column_mappings.yaml") {
  if (!requireNamespace("yaml", quietly = TRUE)) {
    stop("Package 'yaml' is required. Please install it with: install.packages('yaml')")
  }
  
  if (file.exists(config_path)) {
    config <- yaml::read_yaml(config_path)
    message(sprintf("Loaded column mappings v%s from %s", config$version, config_path))
    return(config)
  } else {
    warning(sprintf("Column mapping config not found at %s, using defaults", config_path))
    return(list())
  }
}

#' Apply column mappings to a data.table
#' 
#' Standardizes column names based on the configuration for a specific data source
#' 
#' @param dt data.table to standardize
#' @param source_type Type of data source (e.g., "inep_catalog", "muni_identifiers")
#' @param config Column mapping configuration (loaded if NULL)
#' @param verbose Print detailed mapping information
#' @return data.table with standardized column names
#' @export
standardize_columns <- function(dt, source_type, config = NULL, verbose = FALSE) {
  if (!inherits(dt, "data.table")) {
    stop("Input must be a data.table")
  }
  
  # Load config if not provided
  if (is.null(config)) {
    config <- load_column_mappings()
  }
  
  # Get mappings for this source type
  source_config <- config$data_sources[[source_type]]
  if (is.null(source_config)) {
    warning(sprintf("No column mappings found for source type: %s", source_type))
    return(dt)
  }
  
  if (verbose) {
    message(sprintf("\nStandardizing columns for: %s", source_type))
    message(sprintf("Description: %s", source_config$description))
  }
  
  # Store original column names for reporting
  original_names <- names(dt)
  
  # Apply global transformations
  if (!is.null(config$global$standardize_case)) {
    if (config$global$standardize_case == "lowercase") {
      setnames(dt, names(dt), tolower(names(dt)))
      if (verbose) message("Applied: Convert to lowercase")
    }
  }
  
  # Remove accents if configured
  if (isTRUE(config$global$remove_accents)) {
    current_names <- names(dt)
    new_names <- iconv(current_names, from = "UTF-8", to = "ASCII//TRANSLIT")
    new_names <- gsub("[^[:alnum:]_]", "_", new_names)
    setnames(dt, current_names, new_names)
    if (verbose) message("Applied: Remove accents from column names")
  }
  
  # Apply specific column mappings
  mappings <- source_config$column_mappings
  if (!is.null(mappings)) {
    current_names <- names(dt)
    mapped_count <- 0
    
    for (old_name in names(mappings)) {
      new_name <- mappings[[old_name]]
      # Check both original case and lowercase
      if (old_name %in% current_names) {
        if (old_name != new_name) {
          setnames(dt, old_name, new_name)
          mapped_count <- mapped_count + 1
          if (verbose) message(sprintf("  Renamed: '%s' -> '%s'", old_name, new_name))
        }
      } else if (tolower(old_name) %in% current_names) {
        if (tolower(old_name) != new_name) {
          setnames(dt, tolower(old_name), new_name)
          mapped_count <- mapped_count + 1
          if (verbose) message(sprintf("  Renamed: '%s' -> '%s'", tolower(old_name), new_name))
        }
      }
    }
    
    if (verbose) message(sprintf("Mapped %d column names", mapped_count))
  }
  
  # Check required columns exist
  required <- source_config$required_columns
  if (!is.null(required)) {
    missing <- setdiff(required, names(dt))
    if (length(missing) > 0) {
      warning(sprintf("Missing required columns for %s: %s", 
                      source_type, paste(missing, collapse = ", ")))
    }
  }
  
  # Report on optional columns
  if (verbose && !is.null(source_config$optional_columns)) {
    optional <- source_config$optional_columns
    present_optional <- intersect(optional, names(dt))
    if (length(present_optional) > 0) {
      message(sprintf("Optional columns present: %s", paste(present_optional, collapse = ", ")))
    }
  }
  
  return(dt)
}

#' Validate columns with flexible alternatives
#' 
#' Checks if expected columns exist, considering alternative names
#' 
#' @param dt data.table to validate
#' @param expected_cols Character vector of expected column names
#' @param alternatives Named list of alternative column names
#' @param source_type Optional source type for config-based validation
#' @return List with validation results
#' @export
validate_columns_flexible <- function(dt, expected_cols = NULL, alternatives = NULL, 
                                    source_type = NULL, config = NULL) {
  
  # If source_type is provided, load from config
  if (!is.null(source_type) && is.null(expected_cols)) {
    if (is.null(config)) {
      config <- load_column_mappings()
    }
    
    val_rules <- config$validation_rules[[source_type]]
    if (!is.null(val_rules)) {
      expected_cols <- val_rules$expected_columns
      alternatives <- val_rules$column_alternatives
    }
  }
  
  if (is.null(expected_cols)) {
    stop("Must provide either expected_cols or valid source_type")
  }
  
  actual_cols <- names(dt)
  missing <- setdiff(expected_cols, actual_cols)
  found_alternatives <- list()
  
  # Check if alternatives exist for missing columns
  if (length(missing) > 0 && !is.null(alternatives)) {
    still_missing <- character()
    
    for (col in missing) {
      if (col %in% names(alternatives)) {
        # Check each alternative
        alt_found <- FALSE
        for (alt in alternatives[[col]]) {
          if (alt %in% actual_cols) {
            found_alternatives[[col]] <- alt
            alt_found <- TRUE
            break
          }
        }
        if (!alt_found) {
          still_missing <- c(still_missing, col)
        }
      } else {
        still_missing <- c(still_missing, col)
      }
    }
    missing <- still_missing
  }
  
  validation_passed <- length(missing) == 0
  
  result <- list(
    valid = validation_passed,
    missing = missing,
    actual = actual_cols,
    expected = expected_cols,
    found_alternatives = found_alternatives
  )
  
  # Add detailed message
  if (!validation_passed) {
    result$message <- sprintf("Missing columns: %s. Available columns: %s",
                            paste(missing, collapse = ", "),
                            paste(actual_cols, collapse = ", "))
  } else if (length(found_alternatives) > 0) {
    alt_msg <- paste(sprintf("%s->%s", names(found_alternatives), 
                           unlist(found_alternatives)), collapse = ", ")
    result$message <- sprintf("Validation passed using alternatives: %s", alt_msg)
  } else {
    result$message <- "All expected columns present"
  }
  
  return(result)
}

#' Update validation in targets pipeline to use flexible validation
#' 
#' This is a wrapper function that can be used in _targets.R to perform
#' validation with column alternatives
#' 
#' @param data The data to validate
#' @param stage_name Name of the validation stage
#' @param min_rows Minimum expected rows
#' @return Validation result object
#' @export
validate_import_stage_flexible <- function(data, stage_name, min_rows = 1) {
  # Load configuration
  config <- load_column_mappings()
  
  # Validate columns using flexible approach
  col_validation <- validate_columns_flexible(
    dt = data,
    source_type = stage_name,
    config = config
  )
  
  # Check row count
  row_validation <- nrow(data) >= min_rows
  
  # Combine results
  result <- list(
    passed = col_validation$valid && row_validation,
    stage = stage_name,
    column_validation = col_validation,
    row_count = nrow(data),
    min_rows = min_rows,
    timestamp = Sys.time()
  )
  
  # Add failure reasons if any
  if (!result$passed) {
    reasons <- character()
    if (!col_validation$valid) {
      reasons <- c(reasons, sprintf("Missing columns: %s", 
                                  paste(col_validation$missing, collapse = ", ")))
    }
    if (!row_validation) {
      reasons <- c(reasons, sprintf("Too few rows: %d < %d", nrow(data), min_rows))
    }
    result$failure_reasons <- reasons
  }
  
  return(result)
}

#' Get column mapping report
#' 
#' Generates a report of all column mappings for documentation
#' 
#' @param config Column mapping configuration
#' @return data.table with mapping details
#' @export
get_column_mapping_report <- function(config = NULL) {
  if (is.null(config)) {
    config <- load_column_mappings()
  }
  
  # Create report table
  report_data <- list()
  
  for (source in names(config$data_sources)) {
    source_config <- config$data_sources[[source]]
    mappings <- source_config$column_mappings
    
    if (!is.null(mappings)) {
      for (old_name in names(mappings)) {
        report_data[[length(report_data) + 1]] <- list(
          source_type = source,
          original_column = old_name,
          mapped_column = mappings[[old_name]],
          is_required = mappings[[old_name]] %in% source_config$required_columns
        )
      }
    }
  }
  
  if (length(report_data) > 0) {
    report_dt <- rbindlist(report_data)
    return(report_dt[order(source_type, original_column)])
  } else {
    return(data.table())
  }
}