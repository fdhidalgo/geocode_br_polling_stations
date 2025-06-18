# Data.table utility functions

library(data.table)
library(stringr)

#' Standardize column names to lowercase with underscores
#' 
#' @param dt data.table to process
#' @param inplace Whether to modify in place or return a copy
#' @return Modified data.table
#' @export
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

#' Apply zero-padding to multiple columns
#' 
#' @param dt data.table to process
#' @param pad_specs Named list with column names and padding widths
#' @return Modified data.table by reference
#' @export
apply_padding_batch <- function(dt, pad_specs) {
  for (col in names(pad_specs)) {
    if (col %in% names(dt)) {
      dt[, (col) := str_pad(get(col), width = pad_specs[[col]], side = "left", pad = "0")]
    }
  }
  invisible(dt)
}