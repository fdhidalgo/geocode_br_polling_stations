#' Optimize Batch Distribution
#'
#' Functions to analyze and optimize batch distribution for better parallelization

#' Analyze current batch distribution
#'
#' @param locais_data Polling stations data
#' @param cnefe_data CNEFE data (can be streets or neighborhoods)
#' @param n_workers Number of parallel workers
#' @return Analysis of batch distribution with recommendations
#' @export
analyze_batch_distribution <- function(locais_data, cnefe_data = NULL, n_workers = 30) {
  # Count polling stations per municipality
  muni_sizes <- locais_data[, .(
    n_polling_stations = .N
  ), by = cod_localidade_ibge]
  
  # If CNEFE data provided, calculate complexity
  if (!is.null(cnefe_data)) {
    cnefe_sizes <- cnefe_data[, .(
      n_cnefe_items = .N
    ), by = id_munic_7]
    
    muni_sizes <- merge(
      muni_sizes,
      cnefe_sizes,
      by.x = "cod_localidade_ibge",
      by.y = "id_munic_7",
      all.x = TRUE
    )
    
    muni_sizes[is.na(n_cnefe_items), n_cnefe_items := 0]
    muni_sizes[, complexity := n_polling_stations * n_cnefe_items]
  } else {
    muni_sizes[, complexity := n_polling_stations]
  }
  
  # Sort by complexity
  setorder(muni_sizes, -complexity)
  
  # Calculate optimal batch size
  total_complexity <- sum(muni_sizes$complexity)
  target_complexity_per_batch <- total_complexity / (n_workers * 10) # Aim for 10 batches per worker
  
  # Identify problematic municipalities
  large_munis <- muni_sizes[complexity > target_complexity_per_batch * 2]
  
  # Summary statistics
  cat("\n=== Batch Distribution Analysis ===\n")
  cat(sprintf("Total municipalities: %d\n", nrow(muni_sizes)))
  cat(sprintf("Total complexity score: %s\n", format(total_complexity, big.mark = ",")))
  cat(sprintf("Target complexity per batch: %s\n", format(round(target_complexity_per_batch), big.mark = ",")))
  cat(sprintf("Number of workers: %d\n", n_workers))
  
  cat("\nComplexity distribution:\n")
  print(summary(muni_sizes$complexity))
  
  if (nrow(large_munis) > 0) {
    cat(sprintf("\n%d municipalities exceed 2x target batch complexity:\n", nrow(large_munis)))
    print(head(large_munis, 10))
    
    cat("\nRecommendations:\n")
    cat("1. Split these large municipalities into smaller chunks\n")
    cat("2. Process them with dedicated memory-limited workers\n")
    cat("3. Consider pre-filtering to reduce comparisons\n")
  }
  
  invisible(list(
    muni_sizes = muni_sizes,
    target_complexity = target_complexity_per_batch,
    large_munis = large_munis
  ))
}

#' Create optimized batch assignments
#'
#' @param muni_sizes Data.table with municipality sizes and complexity
#' @param n_batches Target number of batches
#' @param max_complexity_per_batch Maximum complexity allowed per batch
#' @return Data.table with optimized batch assignments
#' @export
create_optimized_batches <- function(muni_sizes, n_batches = 300, max_complexity_per_batch = NULL) {
  # Copy to avoid modifying original
  munis <- copy(muni_sizes)
  setorder(munis, -complexity)
  
  # Calculate target complexity if not provided
  if (is.null(max_complexity_per_batch)) {
    total_complexity <- sum(munis$complexity)
    max_complexity_per_batch <- total_complexity / n_batches * 1.2 # Allow 20% overhead
  }
  
  # Initialize batch assignments
  munis[, batch_id := NA_integer_]
  munis[, batch_complexity := 0.0]
  
  # Create batches
  current_batch <- 1
  current_complexity <- 0
  
  for (i in 1:nrow(munis)) {
    muni_complexity <- munis[i, complexity]
    
    # Check if adding this municipality would exceed limit
    if (current_complexity + muni_complexity > max_complexity_per_batch && current_complexity > 0) {
      # Start new batch
      current_batch <- current_batch + 1
      current_complexity <- 0
    }
    
    # Assign to current batch
    munis[i, batch_id := current_batch]
    current_complexity <- current_complexity + muni_complexity
    munis[batch_id == current_batch, batch_complexity := current_complexity]
  }
  
  # Summary
  batch_summary <- munis[, .(
    n_munis = .N,
    total_complexity = sum(complexity),
    min_complexity = min(complexity),
    max_complexity = max(complexity)
  ), by = batch_id]
  
  cat("\n=== Optimized Batch Assignments ===\n")
  cat(sprintf("Created %d batches\n", max(munis$batch_id)))
  cat("\nBatch complexity distribution:\n")
  print(summary(batch_summary$total_complexity))
  
  # Check balance
  cv <- sd(batch_summary$total_complexity) / mean(batch_summary$total_complexity)
  cat(sprintf("\nCoefficient of variation: %.2f\n", cv))
  if (cv > 0.5) {
    cat("WARNING: Batch sizes are highly imbalanced. Consider adjusting parameters.\n")
  }
  
  munis
}

#' Split large municipalities for parallel processing
#'
#' @param locais_data Polling stations for a large municipality
#' @param n_chunks Number of chunks to split into
#' @return List of data.table chunks
#' @export
split_large_municipality <- function(locais_data, n_chunks = 4) {
  n_rows <- nrow(locais_data)
  
  if (n_rows < n_chunks * 10) {
    # Too small to split effectively
    return(list(locais_data))
  }
  
  # Create random chunks for even distribution
  locais_data[, chunk_id := sample(1:n_chunks, .N, replace = TRUE)]
  
  # Split by chunk
  chunks <- split(locais_data, by = "chunk_id", keep.by = FALSE)
  
  message(sprintf(
    "Split municipality with %d polling stations into %d chunks",
    n_rows,
    length(chunks)
  ))
  
  chunks
}