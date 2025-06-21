#' Memory-Efficient String Matching Functions
#' 
#' This file contains optimized string matching functions that use chunked processing
#' and pre-filtering to reduce memory usage for large municipalities.

#' Pre-filter targets that share at least one word with query
#'
#' @param query_string Single query string
#' @param target_strings Vector of target strings to filter
#' @param min_word_length Minimum word length to consider (default: 3)
#' @return Indices of targets that have at least one word in common with query
#' @export
prefilter_by_common_words <- function(query_string, target_strings, min_word_length = 3) {
  # Split query into words, removing punctuation
  query_words <- unlist(strsplit(gsub("[[:punct:]]", " ", query_string), "\\s+"))
  query_words <- unique(query_words[nchar(query_words) >= min_word_length])
  
  if (length(query_words) == 0) {
    return(seq_along(target_strings))  # No filtering if no valid words
  }
  
  # Vectorized approach for efficiency
  # Create pattern for any of the query words
  pattern <- paste0("\\b(", paste(query_words, collapse = "|"), ")\\b")
  
  # Find targets with at least one word in common
  has_common_word <- grepl(pattern, target_strings, ignore.case = TRUE)
  
  indices <- which(has_common_word)
  
  # If no matches found, return all indices (fallback)
  if (length(indices) == 0) {
    return(seq_along(target_strings))
  }
  
  indices
}

#' Process string matching in chunks to manage memory
#'
#' @param query_strings Vector of query strings
#' @param target_strings Vector of target strings
#' @param chunk_size Number of queries to process at once
#' @param method String distance method (default: "jw" for Jaro-Winkler)
#' @param normalize Whether to normalize by string length
#' @return List with distances, matches, and indices
#' @export
chunk_string_match <- function(query_strings, target_strings, chunk_size = 1000, 
                              method = "jw", normalize = TRUE) {
  n_queries <- length(query_strings)
  n_chunks <- ceiling(n_queries / chunk_size)
  
  # Pre-allocate results
  min_dists <- numeric(n_queries)
  best_matches <- character(n_queries)
  best_indices <- integer(n_queries)
  
  # Get string lengths once if normalizing
  if (normalize) {
    query_lengths <- nchar(query_strings)
    target_lengths <- nchar(target_strings)
  }
  
  for (i in seq_len(n_chunks)) {
    start_idx <- (i - 1) * chunk_size + 1
    end_idx <- min(i * chunk_size, n_queries)
    chunk_indices <- start_idx:end_idx
    
    # Process chunk
    chunk_dists <- stringdist::stringdistmatrix(
      query_strings[chunk_indices], 
      target_strings, 
      method = method
    )
    
    # Normalize if requested
    if (normalize) {
      # Normalize by max length of each pair
      norm_matrix <- outer(
        query_lengths[chunk_indices],
        target_lengths,
        FUN = pmax
      )
      chunk_dists <- chunk_dists / norm_matrix
    }
    
    # Find best matches for chunk
    for (j in seq_len(nrow(chunk_dists))) {
      min_idx <- which.min(chunk_dists[j, ])
      global_idx <- start_idx + j - 1
      min_dists[global_idx] <- chunk_dists[j, min_idx]
      best_matches[global_idx] <- target_strings[min_idx]
      best_indices[global_idx] <- min_idx
    }
    
    # Clean up memory after each chunk
    rm(chunk_dists)
    if (normalize) rm(norm_matrix)
    if (i %% 5 == 0) gc(verbose = FALSE)
  }
  
  list(
    distances = min_dists, 
    matches = best_matches, 
    indices = best_indices
  )
}

#' Memory-efficient string matching with pre-filtering
#'
#' @param query_strings Vector of query strings
#' @param target_strings Vector of target strings  
#' @param use_prefilter Whether to use common word pre-filtering
#' @param chunk_size Number of queries to process at once
#' @param method String distance method
#' @param normalize Whether to normalize by string length
#' @return List with match results for each query
#' @export
match_strings_memory_efficient <- function(query_strings, target_strings,
                                         use_prefilter = TRUE,
                                         chunk_size = NULL,
                                         method = "jw",
                                         normalize = TRUE) {
  
  n_queries <- length(query_strings)
  n_targets <- length(target_strings)
  
  # Adaptive chunk size based on data size
  if (is.null(chunk_size)) {
    chunk_size <- get_adaptive_chunk_size(n_queries, n_targets)
  }
  
  # If pre-filtering is disabled or data is small, use chunked processing
  if (!use_prefilter || n_queries * n_targets < 1e6) {
    result <- chunk_string_match(query_strings, target_strings, 
                                chunk_size, method, normalize)
    return(list(
      local_id = seq_along(query_strings),
      match = result$matches,
      mindist = result$distances,
      match_idx = result$indices
    ))
  }
  
  # Use pre-filtering for large datasets
  # Pre-allocate results
  min_dists <- numeric(n_queries)
  best_matches <- character(n_queries)
  best_indices <- integer(n_queries)
  
  # Process in batches for memory efficiency
  batch_size <- 100  # Process 100 queries at a time with filtering
  n_batches <- ceiling(n_queries / batch_size)
  
  for (b in seq_len(n_batches)) {
    batch_start <- (b - 1) * batch_size + 1
    batch_end <- min(b * batch_size, n_queries)
    batch_indices <- batch_start:batch_end
    
    for (i in batch_indices) {
      # Pre-filter targets
      candidate_indices <- prefilter_by_common_words(query_strings[i], target_strings)
      
      # Skip if no candidates (shouldn't happen with fallback)
      if (length(candidate_indices) == 0) {
        min_dists[i] <- NA
        best_matches[i] <- NA_character_
        best_indices[i] <- NA_integer_
        next
      }
      
      # Calculate distances only for filtered candidates
      if (length(candidate_indices) == 1) {
        # Only one candidate
        dists <- stringdist::stringdist(
          query_strings[i],
          target_strings[candidate_indices],
          method = method
        )
      } else {
        # Multiple candidates
        dists <- stringdist::stringdist(
          query_strings[i],
          target_strings[candidate_indices],
          method = method
        )
      }
      
      # Normalize if requested
      if (normalize) {
        query_len <- nchar(query_strings[i])
        target_lens <- nchar(target_strings[candidate_indices])
        dists <- dists / pmax(query_len, target_lens)
      }
      
      # Find best match
      min_idx <- which.min(dists)
      min_dists[i] <- dists[min_idx]
      best_indices[i] <- candidate_indices[min_idx]
      best_matches[i] <- target_strings[best_indices[i]]
    }
    
    # Periodic garbage collection
    if (b %% 10 == 0) gc(verbose = FALSE)
  }
  
  list(
    local_id = seq_along(query_strings),
    match = best_matches,
    mindist = min_dists,
    match_idx = best_indices
  )
}

#' Get adaptive chunk size based on data dimensions
#'
#' @param n_queries Number of query strings
#' @param n_targets Number of target strings
#' @return Recommended chunk size
#' @export
get_adaptive_chunk_size <- function(n_queries, n_targets) {
  # Estimate memory usage in GB (8 bytes per distance value)
  memory_estimate <- (n_queries * n_targets * 8) / 1024^3
  
  if (memory_estimate > 4) {
    500   # Very large - small chunks
  } else if (memory_estimate > 2) {
    1000  # Large - medium chunks
  } else if (memory_estimate > 1) {
    2000  # Medium - larger chunks
  } else {
    5000  # Small - large chunks OK
  }
}