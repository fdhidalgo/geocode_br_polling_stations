## Utility Functions
##
## This file consolidates utility functions from:
## - data_table_utils.R (2 functions)
## - filtering_helpers.R (8 functions + 1 operator)
##
## Total functions: 10 (including %||% operator)

library(data.table)

# ===== DATA TABLE UTILITIES =====

# Note: standardize_column_names was moved to data_cleaning.R to avoid circular dependencies

apply_padding_batch <- function(ids, target_size = 100) {
  # Apply padding to create balanced batches
  # Used in parallel processing to ensure even distribution
  
  n_ids <- length(ids)
  n_batches <- ceiling(n_ids / target_size)
  ideal_total <- n_batches * target_size
  n_padding <- ideal_total - n_ids
  
  if (n_padding > 0) {
    # Add padding by repeating some IDs
    padding_ids <- sample(ids, n_padding, replace = TRUE)
    ids <- c(ids, padding_ids)
  }
  
  return(ids)
}

# ===== FILTERING HELPERS =====

# Define the null coalescing operator
`%||%` <- function(x, y) if (is.null(x)) y else x

filter_by_dev_mode <- function(data, dev_states, id_column = "estado_abrev") {
  # Filter data by development mode states
  # If dev_states is NULL or empty, return all data
  
  if (is.null(dev_states) || length(dev_states) == 0) {
    return(data)
  }
  
  # Filter by state abbreviation
  if (id_column %in% names(data)) {
    filtered <- data[get(id_column) %in% dev_states]
  } else {
    warning(paste("Column", id_column, "not found in data. Returning unfiltered data."))
    filtered <- data
  }
  
  return(filtered)
}

filter_data_by_state <- function(data, states, state_col = "estado_abrev") {
  # Generic function to filter any data by state
  # Works with data.table or data.frame
  
  if (is.null(states) || length(states) == 0) {
    return(data)
  }
  
  if (!state_col %in% names(data)) {
    warning(paste("State column", state_col, "not found. Returning unfiltered data."))
    return(data)
  }
  
  if (is.data.table(data)) {
    return(data[get(state_col) %in% states])
  } else {
    return(data[data[[state_col]] %in% states, ])
  }
}

filter_data_by_municipalities <- function(data, muni_codes, muni_col = "id_munic_7") {
  # Filter data by municipality codes
  
  if (is.null(muni_codes) || length(muni_codes) == 0) {
    return(data)
  }
  
  if (!muni_col %in% names(data)) {
    # Try alternative column names
    alt_cols <- c("cod_localidade_ibge", "id_TSE", "cd_municipio")
    for (col in alt_cols) {
      if (col %in% names(data)) {
        muni_col <- col
        break
      }
    }
    
    if (!muni_col %in% names(data)) {
      warning("No municipality column found. Returning unfiltered data.")
      return(data)
    }
  }
  
  if (is.data.table(data)) {
    return(data[get(muni_col) %in% muni_codes])
  } else {
    return(data[data[[muni_col]] %in% muni_codes, ])
  }
}

get_muni_codes_for_states <- function(muni_ids, states) {
  # Get municipality codes for specific states
  
  if (is.null(states) || length(states) == 0) {
    return(unique(muni_ids$id_munic_7))
  }
  
  # Filter muni_ids by state and return codes
  filtered <- muni_ids[estado_abrev %in% states]
  return(unique(filtered$id_munic_7))
}

filter_geographic_by_state <- function(geo_data, states) {
  # Filter geographic data (sf objects) by state
  # Handles both data.frame and sf objects
  
  if (is.null(states) || length(states) == 0) {
    return(geo_data)
  }
  
  # Check for state columns
  state_cols <- c("abbrev_state", "estado_abrev", "sigla_uf", "uf")
  state_col <- NULL
  
  for (col in state_cols) {
    if (col %in% names(geo_data)) {
      state_col <- col
      break
    }
  }
  
  if (is.null(state_col)) {
    warning("No state column found in geographic data. Returning unfiltered.")
    return(geo_data)
  }
  
  # Filter by state
  if ("sf" %in% class(geo_data)) {
    return(geo_data[geo_data[[state_col]] %in% states, ])
  } else {
    return(filter_data_by_state(geo_data, states, state_col))
  }
}

apply_dev_mode_filters <- function(data, config, filter_type = NULL, state_col = NULL, muni_col = NULL) {
  # Apply development mode filters based on configuration
  # Supports both old and new call signatures for backward compatibility
  
  # Handle old-style calls with named parameters
  if (!is.null(filter_type)) {
    if (filter_type == "state" && !is.null(state_col)) {
      # Old-style state filtering
      if (!is.null(config$dev_states)) {
        return(filter_data_by_state(data, config$dev_states, state_col))
      } else {
        return(data)
      }
    } else if (filter_type == "municipality" && !is.null(muni_col)) {
      # Old-style municipality filtering
      if (!is.null(config$dev_municipalities)) {
        return(filter_data_by_municipalities(data, config$dev_municipalities, muni_col))
      } else {
        return(data)
      }
    }
  }
  
  # New-style filtering based on config
  if (!is.null(config$dev_states)) {
    data <- filter_data_by_state(data, config$dev_states)
  }
  
  if (!is.null(config$dev_municipalities)) {
    data <- filter_data_by_municipalities(data, config$dev_municipalities)
  }
  
  return(data)
}

apply_brasilia_filters <- function(data, remove_brasilia = TRUE) {
  # Apply special filtering for Brasília (DF)
  # Brasília has unique characteristics that can affect analysis
  
  if (!remove_brasilia) {
    return(data)
  }
  
  # Identify columns that might contain state information
  state_cols <- c("estado_abrev", "sg_uf", "uf", "sigla_uf")
  state_col <- NULL
  
  for (col in state_cols) {
    if (col %in% names(data)) {
      state_col <- col
      break
    }
  }
  
  if (is.null(state_col)) {
    # Try municipality code approach
    muni_cols <- c("id_munic_7", "cod_localidade_ibge", "cd_municipio")
    muni_col <- NULL
    
    for (col in muni_cols) {
      if (col %in% names(data)) {
        muni_col <- col
        break
      }
    }
    
    if (!is.null(muni_col)) {
      # Brasília municipality codes start with 53
      if (is.data.table(data)) {
        return(data[!grepl("^53", get(muni_col))])
      } else {
        return(data[!grepl("^53", data[[muni_col]]), ])
      }
    }
    
    warning("No state or municipality column found. Cannot filter Brasília.")
    return(data)
  }
  
  # Filter out DF
  if (is.data.table(data)) {
    return(data[get(state_col) != "DF"])
  } else {
    return(data[data[[state_col]] != "DF", ])
  }
}