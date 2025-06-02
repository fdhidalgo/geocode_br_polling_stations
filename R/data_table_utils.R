# Data.table utility functions

library(data.table)
library(stringr)

# Function to standardize column names
standardize_column_names <- function(dt, inplace = FALSE) {
  if (!inplace) dt <- copy(dt)
  
  old_names <- names(dt)
  new_names <- tolower(old_names)
  new_names <- gsub("([a-z])([A-Z])", "\\1_\\2", new_names)
  new_names <- gsub("[^a-z0-9]+", "_", new_names)
  new_names <- gsub("^_|_$", "", new_names)
  new_names <- gsub("__+", "_", new_names)
  
  setnames(dt, old_names, new_names)
  
  if (!inplace) return(dt) else invisible(dt)
}

# Apply padding to multiple columns
apply_padding_batch <- function(dt, pad_specs) {
  for (col in names(pad_specs)) {
    if (col %in% names(dt)) {
      dt[, (col) := str_pad(get(col), width = pad_specs[[col]], side = "left", pad = "0")]
    }
  }
  invisible(dt)
}

# Normalize multiple columns using batch operations
normalize_columns_batch <- function(dt, columns, normalize_fn) {
  for (col in columns) {
    if (col %in% names(dt)) {
      norm_col <- paste0("norm_", col)
      dt[, (norm_col) := normalize_fn(get(col))]
    }
  }
  invisible(dt)
}

# Create address columns by combining components
create_address_columns <- function(dt, address_components) {
  for (addr_type in names(address_components)) {
    components <- address_components[[addr_type]]
    valid_components <- components[components %in% names(dt)]
    
    if (length(valid_components) > 0) {
      dt[, (addr_type) := do.call(paste, c(.SD, sep = " ")), .SDcols = valid_components]
      dt[, (addr_type) := trimws(get(addr_type))]
    }
  }
  invisible(dt)
}

# Calculate string distances
calculate_string_distances <- function(vec1, vec2, method = "lv") {
  dist_matrix <- stringdist::stringdistmatrix(vec1, vec2, method = method)
  
  # Normalize by maximum string length
  len_matrix <- outer(nchar(vec1), nchar(vec2), FUN = "pmax")
  normalized_dist <- dist_matrix / len_matrix
  
  return(normalized_dist)
}