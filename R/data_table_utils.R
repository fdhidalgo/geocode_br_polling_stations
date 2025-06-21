## Data table utility functions
## This file has been replaced by data_cleaning.R and utilities.R
## Kept for backward compatibility - sources the new consolidated files

# standardize_column_names is in data_cleaning.R
source("R/data_cleaning.R")

# apply_padding_batch will be in utilities.R when created
# For now, define it here until utilities.R is created
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