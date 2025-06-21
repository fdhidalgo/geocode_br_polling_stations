#' Create balanced batches of municipalities for parallel processing
#'
#' This function creates balanced batches of municipality codes to reduce
#' the number of dynamic branches in the targets pipeline. This helps
#' prevent bottlenecks when crew dispatches thousands of fine-grained tasks.
#'
#' @param muni_codes Vector of municipality codes to batch
#' @param batch_size Target size for each batch (default: 50)
#' @return Named list of municipality code batches
#' @export
#' @examples
#' # Create batches of 50 municipalities each
#' batches <- create_municipality_batches(unique(locais$cod_localidade_ibge))
#' length(batches)  # Will be ~112 for 5571 municipalities
# Note: 5 unused functions were moved to backup/unused_functions/
# Date: 2025-06-20
# Functions removed: check_and_adjust_resources, clean_cnefe10_routed, clean_cnefe22_routed, process_string_match_routed, process_validation_routed

create_municipality_batches <- function(muni_codes, batch_size = 50) {
  # Ensure we have unique municipality codes
  muni_codes <- unique(muni_codes)
  
  n_munis <- length(muni_codes)
  n_batches <- ceiling(n_munis / batch_size)
  
  # Create balanced batches using cut
  batch_indices <- cut(seq_along(muni_codes), 
                      breaks = n_batches, 
                      labels = FALSE)
  
  # Split municipalities into batches
  batches <- split(muni_codes, batch_indices)
  
  # Remove names to ensure clean iteration
  batches <- unname(batches)
  
  # Log batch creation
  message(sprintf(
    "Created %d batches from %d municipalities (avg size: %.1f)",
    n_batches, n_munis, n_munis / n_batches
  ))
  
  batches
}

#' Create municipality batch assignments for flattened parallel processing
#'
#' This function creates a data.table with municipality codes and their
#' corresponding batch IDs. This flattened structure avoids vector length
#' issues when using pattern = map() in targets.
#'
#' @param muni_codes Vector of municipality codes to batch
#' @param batch_size Target size for each batch (default: 50)
#' @return data.table with columns muni_code and batch_id
#' @export
#' @examples
#' # Create batch assignments for municipalities
#' assignments <- create_municipality_batch_assignments(unique(locais$cod_localidade_ibge))
#' # Use with targets: pattern = map(unique(assignments$batch_id))
create_municipality_batch_assignments <- function(muni_codes, batch_size = 50, muni_sizes = NULL) {
  # Ensure we have unique municipality codes
  muni_codes <- unique(muni_codes)
  n_munis <- length(muni_codes)
  
  # If muni_sizes not provided, use simple batching
  if (is.null(muni_sizes)) {
    n_batches <- ceiling(n_munis / batch_size)
    
    # Create batch IDs using rep with appropriate lengths
    batch_ids <- rep(seq_len(n_batches), 
                     each = batch_size, 
                     length.out = n_munis)
    
    # Create the assignment table
    batch_assignments <- data.table(
      muni_code = muni_codes,
      batch_id = batch_ids
    )
    
    # Log batch creation
    message(sprintf(
      "Created %d batch assignments for %d municipalities (avg size: %.1f)",
      n_batches, n_munis, n_munis / n_batches
    ))
    
    return(batch_assignments)
  }
  
  # Smart batching based on municipality size
  # muni_sizes should be a data.table with columns: muni_code, size
  muni_sizes <- copy(muni_sizes)
  setnames(muni_sizes, old = names(muni_sizes), new = c("muni_code", "size"))
  
  # Categorize municipalities by size
  muni_sizes[, size_category := cut(
    size,
    breaks = c(0, 500, 2000, 5000, Inf),
    labels = c("small", "medium", "large", "mega"),
    include.lowest = TRUE
  )]
  
  # Set batch sizes based on category
  muni_sizes[, target_batch_size := fcase(
    size_category == "small", 100,
    size_category == "medium", 50,
    size_category == "large", 20,
    size_category == "mega", 5,
    default = 50
  )]
  
  # Sort by size (largest first) to ensure balanced batches
  setorder(muni_sizes, -size)
  
  # Create batches for each size category
  batch_counter <- 1
  muni_sizes[, batch_id := NA_integer_]
  
  for (cat in c("mega", "large", "medium", "small")) {
    cat_data <- muni_sizes[size_category == cat]
    if (nrow(cat_data) == 0) next
    
    target_size <- cat_data$target_batch_size[1]
    n_cat <- nrow(cat_data)
    n_batches_cat <- ceiling(n_cat / target_size)
    
    # Assign batch IDs
    batch_ids_cat <- rep(
      seq(from = batch_counter, length.out = n_batches_cat),
      each = target_size,
      length.out = n_cat
    )
    
    muni_sizes[size_category == cat, batch_id := batch_ids_cat]
    batch_counter <- batch_counter + n_batches_cat
  }
  
  # Create final assignment table
  batch_assignments <- muni_sizes[, .(muni_code, batch_id, size, size_category)]
  
  # Log batch creation with details
  batch_summary <- batch_assignments[, .(
    n_munis = .N,
    total_size = sum(size),
    avg_size = mean(size),
    size_categories = paste(unique(size_category), collapse = ", ")
  ), by = batch_id]
  
  message(sprintf(
    "Created %d smart batch assignments for %d municipalities:",
    max(batch_assignments$batch_id), n_munis
  ))
  message(sprintf(
    "  Mega (>5000): %d municipalities in %d batches",
    nrow(muni_sizes[size_category == "mega"]),
    length(unique(muni_sizes[size_category == "mega"]$batch_id))
  ))
  message(sprintf(
    "  Large (2000-5000): %d municipalities in %d batches",
    nrow(muni_sizes[size_category == "large"]),
    length(unique(muni_sizes[size_category == "large"]$batch_id))
  ))
  message(sprintf(
    "  Medium (500-2000): %d municipalities in %d batches",
    nrow(muni_sizes[size_category == "medium"]),
    length(unique(muni_sizes[size_category == "medium"]$batch_id))
  ))
  message(sprintf(
    "  Small (<500): %d municipalities in %d batches",
    nrow(muni_sizes[size_category == "small"]),
    length(unique(muni_sizes[size_category == "small"]$batch_id))
  ))
  
  # Return only muni_code and batch_id for compatibility
  batch_assignments[, .(muni_code, batch_id)]
}