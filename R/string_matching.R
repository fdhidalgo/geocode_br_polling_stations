## String Matching Functions
## 
## This file consolidates all string matching functions from:
## - string_matching_geocode_fns.R (4 match functions)
## - string_matching_geocode_fns_memory_efficient.R (4 duplicate functions) 
## - memory_efficient_string_matching.R (4 functions)
## - geocodebr_matching.R (1 function)
##
## The duplicate functions are unified with a memory_efficient parameter
## Total functions: 9

library(data.table)
library(stringr)
library(stringdist)

# Check if memory-efficient mode is enabled globally
USE_MEMORY_EFFICIENT <- isTRUE(getOption("geocode_br.use_memory_efficient", FALSE))

# ===== MEMORY EFFICIENT HELPER FUNCTIONS =====

prefilter_by_common_words <- function(query_strings, target_strings, min_common_words = 1) {
  # Pre-filter strings based on common words to reduce comparison space
  # Returns indices of target_strings that share at least min_common_words with query
  
  # Extract words from strings
  query_words <- strsplit(tolower(query_strings), "\\s+")
  target_words <- strsplit(tolower(target_strings), "\\s+")
  
  # Create a matrix to store which targets match each query
  matches <- matrix(FALSE, nrow = length(query_strings), ncol = length(target_strings))
  
  for (i in seq_along(query_strings)) {
    query_word_set <- unique(query_words[[i]])
    
    for (j in seq_along(target_strings)) {
      target_word_set <- unique(target_words[[j]])
      common_words <- length(intersect(query_word_set, target_word_set))
      
      if (common_words >= min_common_words) {
        matches[i, j] <- TRUE
      }
    }
  }
  
  return(matches)
}

chunk_string_match <- function(query_chunk, target_strings, method = "jw", 
                              normalize_by_length = TRUE) {
  # Perform string matching on a chunk of query strings
  
  # Calculate distance matrix for this chunk
  dist_matrix <- stringdist::stringdistmatrix(
    query_chunk,
    target_strings,
    method = method
  )
  
  # Normalize by string length if requested
  if (normalize_by_length) {
    len_matrix <- outer(
      nchar(query_chunk),
      nchar(target_strings),
      FUN = "pmax"
    )
    dist_matrix <- dist_matrix / len_matrix
  }
  
  return(dist_matrix)
}

match_strings_memory_efficient <- function(query_strings, target_strings, 
                                         method = "jw", chunk_size = 1000,
                                         normalize_by_length = TRUE,
                                         prefilter = TRUE,
                                         min_common_words = 1) {
  # Memory-efficient version of string matching
  # Process queries in chunks and optionally pre-filter
  
  n_queries <- length(query_strings)
  n_targets <- length(target_strings)
  
  # Initialize result vectors
  min_dists <- rep(Inf, n_queries)
  best_matches <- rep(NA_character_, n_queries)
  best_indices <- rep(NA_integer_, n_queries)
  
  # Pre-filter if requested
  if (prefilter && min_common_words > 0) {
    filter_matrix <- prefilter_by_common_words(query_strings, target_strings, min_common_words)
  } else {
    filter_matrix <- matrix(TRUE, nrow = n_queries, ncol = n_targets)
  }
  
  # Process in chunks
  n_chunks <- ceiling(n_queries / chunk_size)
  
  for (chunk_i in seq_len(n_chunks)) {
    start_idx <- (chunk_i - 1) * chunk_size + 1
    end_idx <- min(chunk_i * chunk_size, n_queries)
    chunk_indices <- start_idx:end_idx
    
    query_chunk <- query_strings[chunk_indices]
    filter_chunk <- filter_matrix[chunk_indices, , drop = FALSE]
    
    # For each query in the chunk
    for (i in seq_along(query_chunk)) {
      global_idx <- chunk_indices[i]
      
      # Get filtered targets for this query
      valid_targets <- which(filter_chunk[i, ])
      
      if (length(valid_targets) == 0) {
        next
      }
      
      # Calculate distances only for valid targets
      if (length(valid_targets) < 1000) {
        # Small set, calculate directly
        dists <- stringdist::stringdist(
          query_chunk[i],
          target_strings[valid_targets],
          method = method
        )
        
        if (normalize_by_length) {
          lens <- pmax(nchar(query_chunk[i]), nchar(target_strings[valid_targets]))
          dists <- dists / lens
        }
      } else {
        # Large set, use chunked calculation
        dists <- numeric(length(valid_targets))
        sub_chunk_size <- 1000
        
        for (j in seq(1, length(valid_targets), by = sub_chunk_size)) {
          sub_end <- min(j + sub_chunk_size - 1, length(valid_targets))
          sub_indices <- j:sub_end
          
          sub_dists <- stringdist::stringdist(
            query_chunk[i],
            target_strings[valid_targets[sub_indices]],
            method = method
          )
          
          if (normalize_by_length) {
            lens <- pmax(nchar(query_chunk[i]), nchar(target_strings[valid_targets[sub_indices]]))
            sub_dists <- sub_dists / lens
          }
          
          dists[sub_indices] <- sub_dists
        }
      }
      
      # Find minimum
      min_idx <- which.min(dists)
      if (length(min_idx) > 0 && dists[min_idx] < min_dists[global_idx]) {
        min_dists[global_idx] <- dists[min_idx]
        best_indices[global_idx] <- valid_targets[min_idx]
        best_matches[global_idx] <- target_strings[valid_targets[min_idx]]
      }
    }
    
    # Garbage collection after each chunk
    if (chunk_i %% 10 == 0) {
      gc(verbose = FALSE)
    }
  }
  
  return(list(
    min_dist = min_dists,
    best_match = best_matches,
    best_index = best_indices
  ))
}

get_adaptive_chunk_size <- function(n_items, available_memory_gb = 4) {
  # Determine optimal chunk size based on data size and available memory
  # Assumes each comparison uses roughly 8 bytes
  
  bytes_per_comparison <- 8
  safety_factor <- 0.5  # Use only 50% of available memory
  
  available_bytes <- available_memory_gb * 1e9 * safety_factor
  max_comparisons <- available_bytes / bytes_per_comparison
  
  # Chunk size is sqrt of max comparisons (for square distance matrix)
  chunk_size <- floor(sqrt(max_comparisons))
  
  # Apply reasonable bounds
  chunk_size <- max(100, min(chunk_size, 10000))
  
  return(chunk_size)
}

# ===== UNIFIED STRING MATCHING FUNCTIONS =====

match_inep_muni <- function(locais_muni, inep_muni, memory_efficient = USE_MEMORY_EFFICIENT) {
  # Match polling stations with INEP school data for a single municipality
  
  if (nrow(inep_muni) == 0) {
    return(NULL)
  }
  
  if (memory_efficient) {
    # Memory-efficient version
    
    # Match on name
    name_results <- match_strings_memory_efficient(
      locais_muni$normalized_name,
      inep_muni$norm_school,
      method = "jw",
      chunk_size = get_adaptive_chunk_size(nrow(locais_muni)),
      normalize_by_length = TRUE,
      prefilter = TRUE,
      min_common_words = 1
    )
    
    # Match on address
    addr_results <- match_strings_memory_efficient(
      locais_muni$normalized_addr,
      inep_muni$norm_addr,
      method = "jw",
      chunk_size = get_adaptive_chunk_size(nrow(locais_muni)),
      normalize_by_length = TRUE,
      prefilter = TRUE,
      min_common_words = 1
    )
    
    # Get coordinates for best matches
    match_long_inep_name <- ifelse(
      is.na(name_results$best_index),
      NA_real_,
      inep_muni$longitude[name_results$best_index]
    )
    match_lat_inep_name <- ifelse(
      is.na(name_results$best_index),
      NA_real_,
      inep_muni$latitude[name_results$best_index]
    )
    
    match_long_inep_addr <- ifelse(
      is.na(addr_results$best_index),
      NA_real_,
      inep_muni$longitude[addr_results$best_index]
    )
    match_lat_inep_addr <- ifelse(
      is.na(addr_results$best_index),
      NA_real_,
      inep_muni$latitude[addr_results$best_index]
    )
    
    # Create output
    output <- data.table(
      local_id = locais_muni$local_id,
      match_inep_name = name_results$best_match,
      mindist_inep_name = name_results$min_dist,
      match_long_inep_name = match_long_inep_name,
      match_lat_inep_name = match_lat_inep_name,
      match_inep_addr = addr_results$best_match,
      mindist_inep_addr = addr_results$min_dist,
      match_long_inep_addr = match_long_inep_addr,
      match_lat_inep_addr = match_lat_inep_addr
    )
    
  } else {
    # Original version
    
    # Match on inep name
    name_inep_dists <- stringdist::stringdistmatrix(
      locais_muni$normalized_name,
      inep_muni$norm_school,
      method = "jw"
    )
    # Normalize for string length
    name_inep_dists <- name_inep_dists /
      outer(
        nchar(locais_muni$normalized_name),
        nchar(inep_muni$norm_school),
        FUN = "pmax"
      )
    
    mindist_name_inep <- apply(name_inep_dists, 1, min)
    match_inep_name <- inep_muni$norm_school[apply(name_inep_dists, 1, which.min)]
    match_long_inep_name <- inep_muni$longitude[apply(name_inep_dists, 1, which.min)]
    match_lat_inep_name <- inep_muni$latitude[apply(name_inep_dists, 1, which.min)]
    
    # Match on inep address
    addr_inep_dists <- stringdist::stringdistmatrix(
      locais_muni$normalized_addr,
      inep_muni$norm_addr,
      method = "jw"
    )
    
    mindist_addr_inep <- apply(addr_inep_dists, 1, min)
    match_inep_addr <- inep_muni$norm_addr[apply(addr_inep_dists, 1, which.min)]
    match_long_inep_addr <- inep_muni$longitude[apply(addr_inep_dists, 1, which.min)]
    match_lat_inep_addr <- inep_muni$latitude[apply(addr_inep_dists, 1, which.min)]
    
    # Create output
    output <- data.table(
      local_id = locais_muni$local_id,
      match_inep_name = match_inep_name,
      mindist_inep_name = mindist_name_inep,
      match_long_inep_name = match_long_inep_name,
      match_lat_inep_name = match_lat_inep_name,
      match_inep_addr = match_inep_addr,
      mindist_inep_addr = mindist_addr_inep,
      match_long_inep_addr = match_long_inep_addr,
      match_lat_inep_addr = match_lat_inep_addr
    )
  }
  
  return(output)
}

match_schools_cnefe_muni <- function(locais_muni, schools_cnefe_muni, 
                                   memory_efficient = USE_MEMORY_EFFICIENT) {
  # Match polling stations with CNEFE school data
  
  if (nrow(schools_cnefe_muni) == 0) {
    return(NULL)
  }
  
  if (memory_efficient) {
    # Memory-efficient version
    
    # Match on name
    name_results <- match_strings_memory_efficient(
      locais_muni$normalized_name,
      schools_cnefe_muni$norm_desc,
      method = "jw",
      chunk_size = get_adaptive_chunk_size(nrow(locais_muni)),
      normalize_by_length = TRUE,
      prefilter = TRUE,
      min_common_words = 1
    )
    
    # Get coordinates for best matches
    match_long_schools_cnefe <- ifelse(
      is.na(name_results$best_index),
      NA_real_,
      schools_cnefe_muni$cnefe_long[name_results$best_index]
    )
    match_lat_schools_cnefe <- ifelse(
      is.na(name_results$best_index),
      NA_real_,
      schools_cnefe_muni$cnefe_lat[name_results$best_index]
    )
    match_bairro_schools_cnefe <- ifelse(
      is.na(name_results$best_index),
      NA_character_,
      schools_cnefe_muni$norm_bairro[name_results$best_index]
    )
    
    # Create output
    output <- data.table(
      local_id = locais_muni$local_id,
      match_schools_cnefe = name_results$best_match,
      mindist_schools_cnefe = name_results$min_dist,
      match_long_schools_cnefe = match_long_schools_cnefe,
      match_lat_schools_cnefe = match_lat_schools_cnefe,
      match_bairro_schools_cnefe = match_bairro_schools_cnefe
    )
    
  } else {
    # Original version
    
    # Match on school name
    name_schools_cnefe_dists <- stringdist::stringdistmatrix(
      locais_muni$normalized_name,
      schools_cnefe_muni$norm_desc,
      method = "jw"
    )
    
    mindist_schools_cnefe <- apply(name_schools_cnefe_dists, 1, min)
    idx_schools_cnefe <- apply(name_schools_cnefe_dists, 1, which.min)
    
    match_schools_cnefe <- schools_cnefe_muni$norm_desc[idx_schools_cnefe]
    match_long_schools_cnefe <- schools_cnefe_muni$cnefe_long[idx_schools_cnefe]
    match_lat_schools_cnefe <- schools_cnefe_muni$cnefe_lat[idx_schools_cnefe]
    match_bairro_schools_cnefe <- schools_cnefe_muni$norm_bairro[idx_schools_cnefe]
    
    # Create output
    output <- data.table(
      local_id = locais_muni$local_id,
      match_schools_cnefe = match_schools_cnefe,
      mindist_schools_cnefe = mindist_schools_cnefe,
      match_long_schools_cnefe = match_long_schools_cnefe,
      match_lat_schools_cnefe = match_lat_schools_cnefe,
      match_bairro_schools_cnefe = match_bairro_schools_cnefe
    )
  }
  
  return(output)
}

match_stbairro_cnefe_muni <- function(locais_muni, cnefe_st_muni, cnefe_bairro_muni,
                                    memory_efficient = USE_MEMORY_EFFICIENT) {
  # Match polling stations with CNEFE street and neighborhood data
  
  if (nrow(cnefe_st_muni) == 0) {
    return(NULL)
  }
  
  if (memory_efficient) {
    # Memory-efficient version
    
    # Match on street
    st_results <- match_strings_memory_efficient(
      locais_muni$normalized_st,
      cnefe_st_muni$norm_street,
      method = "jw",
      chunk_size = get_adaptive_chunk_size(nrow(locais_muni)),
      normalize_by_length = TRUE,
      prefilter = TRUE,
      min_common_words = 1
    )
    
    # Match on neighborhood
    bairro_results <- match_strings_memory_efficient(
      locais_muni$normalized_bairro,
      cnefe_bairro_muni$norm_bairro,
      method = "jw", 
      chunk_size = get_adaptive_chunk_size(nrow(locais_muni)),
      normalize_by_length = FALSE,  # Don't normalize for neighborhoods
      prefilter = TRUE,
      min_common_words = 1
    )
    
    # Get coordinates for best matches
    match_long_cnefe_st <- ifelse(
      is.na(st_results$best_index),
      NA_real_,
      cnefe_st_muni$cnefe_long[st_results$best_index]
    )
    match_lat_cnefe_st <- ifelse(
      is.na(st_results$best_index),
      NA_real_,
      cnefe_st_muni$cnefe_lat[st_results$best_index]
    )
    
    match_long_cnefe_bairro <- ifelse(
      is.na(bairro_results$best_index),
      NA_real_,
      cnefe_bairro_muni$cnefe_long[bairro_results$best_index]
    )
    match_lat_cnefe_bairro <- ifelse(
      is.na(bairro_results$best_index),
      NA_real_,
      cnefe_bairro_muni$cnefe_lat[bairro_results$best_index]
    )
    
    # Create output
    output <- data.table(
      local_id = locais_muni$local_id,
      match_cnefe_st = st_results$best_match,
      mindist_cnefe_st = st_results$min_dist,
      match_long_cnefe_st = match_long_cnefe_st,
      match_lat_cnefe_st = match_lat_cnefe_st,
      match_cnefe_bairro = bairro_results$best_match,
      mindist_cnefe_bairro = bairro_results$min_dist,
      match_long_cnefe_bairro = match_long_cnefe_bairro,
      match_lat_cnefe_bairro = match_lat_cnefe_bairro
    )
    
  } else {
    # Original version
    
    # Street matching
    st_cnefe_dists <- stringdist::stringdistmatrix(
      locais_muni$normalized_st,
      cnefe_st_muni$norm_street,
      method = "jw"
    )
    st_cnefe_dists <- st_cnefe_dists /
      outer(
        nchar(locais_muni$normalized_st),
        nchar(cnefe_st_muni$norm_street),
        FUN = "pmax"
      )
    
    mindist_cnefe_st <- apply(st_cnefe_dists, 1, min)
    idx_cnefe_st <- apply(st_cnefe_dists, 1, which.min)
    
    match_cnefe_st <- cnefe_st_muni$norm_street[idx_cnefe_st]
    match_long_cnefe_st <- cnefe_st_muni$cnefe_long[idx_cnefe_st]
    match_lat_cnefe_st <- cnefe_st_muni$cnefe_lat[idx_cnefe_st]
    
    # Neighborhood matching
    bairro_cnefe_dists <- stringdist::stringdistmatrix(
      locais_muni$normalized_bairro,
      cnefe_bairro_muni$norm_bairro,
      method = "jw"
    )
    
    mindist_cnefe_bairro <- apply(bairro_cnefe_dists, 1, min)
    idx_cnefe_bairro <- apply(bairro_cnefe_dists, 1, which.min)
    
    match_cnefe_bairro <- cnefe_bairro_muni$norm_bairro[idx_cnefe_bairro]
    match_long_cnefe_bairro <- cnefe_bairro_muni$cnefe_long[idx_cnefe_bairro]
    match_lat_cnefe_bairro <- cnefe_bairro_muni$cnefe_lat[idx_cnefe_bairro]
    
    # Create output
    output <- data.table(
      local_id = locais_muni$local_id,
      match_cnefe_st = match_cnefe_st,
      mindist_cnefe_st = mindist_cnefe_st,
      match_long_cnefe_st = match_long_cnefe_st,
      match_lat_cnefe_st = match_lat_cnefe_st,
      match_cnefe_bairro = match_cnefe_bairro,
      mindist_cnefe_bairro = mindist_cnefe_bairro,
      match_long_cnefe_bairro = match_long_cnefe_bairro,
      match_lat_cnefe_bairro = match_lat_cnefe_bairro
    )
  }
  
  return(output)
}

match_stbairro_agrocnefe_muni <- function(locais_muni, agrocnefe_st_muni, agrocnefe_bairro_muni,
                                        memory_efficient = USE_MEMORY_EFFICIENT) {
  # Match polling stations with Agro CNEFE data
  # This follows the same pattern as match_stbairro_cnefe_muni
  
  if (nrow(agrocnefe_st_muni) == 0) {
    return(NULL)
  }
  
  if (memory_efficient) {
    # Memory-efficient version
    
    # Match on street
    st_results <- match_strings_memory_efficient(
      locais_muni$normalized_st,
      agrocnefe_st_muni$norm_street,
      method = "jw",
      chunk_size = get_adaptive_chunk_size(nrow(locais_muni)),
      normalize_by_length = TRUE,
      prefilter = TRUE,
      min_common_words = 1
    )
    
    # Match on neighborhood
    bairro_results <- match_strings_memory_efficient(
      locais_muni$normalized_bairro,
      agrocnefe_bairro_muni$norm_bairro,
      method = "jw",
      chunk_size = get_adaptive_chunk_size(nrow(locais_muni)),
      normalize_by_length = FALSE,
      prefilter = TRUE,
      min_common_words = 1
    )
    
    # Get coordinates for best matches
    match_long_agrocnefe_st <- ifelse(
      is.na(st_results$best_index),
      NA_real_,
      agrocnefe_st_muni$longitude[st_results$best_index]
    )
    match_lat_agrocnefe_st <- ifelse(
      is.na(st_results$best_index),
      NA_real_,
      agrocnefe_st_muni$latitude[st_results$best_index]
    )
    
    match_long_agrocnefe_bairro <- ifelse(
      is.na(bairro_results$best_index),
      NA_real_,
      agrocnefe_bairro_muni$longitude[bairro_results$best_index]
    )
    match_lat_agrocnefe_bairro <- ifelse(
      is.na(bairro_results$best_index),
      NA_real_,
      agrocnefe_bairro_muni$latitude[bairro_results$best_index]
    )
    
    # Create output
    output <- data.table(
      local_id = locais_muni$local_id,
      match_agrocnefe_st = st_results$best_match,
      mindist_agrocnefe_st = st_results$min_dist,
      match_long_agrocnefe_st = match_long_agrocnefe_st,
      match_lat_agrocnefe_st = match_lat_agrocnefe_st,
      match_agrocnefe_bairro = bairro_results$best_match,
      mindist_agrocnefe_bairro = bairro_results$min_dist,
      match_long_agrocnefe_bairro = match_long_agrocnefe_bairro,
      match_lat_agrocnefe_bairro = match_lat_agrocnefe_bairro
    )
    
  } else {
    # Original version
    
    # Street matching
    st_agrocnefe_dists <- stringdist::stringdistmatrix(
      locais_muni$normalized_st,
      agrocnefe_st_muni$norm_street,
      method = "jw"
    )
    st_agrocnefe_dists <- st_agrocnefe_dists /
      outer(
        nchar(locais_muni$normalized_st),
        nchar(agrocnefe_st_muni$norm_street),
        FUN = "pmax"
      )
    
    mindist_agrocnefe_st <- apply(st_agrocnefe_dists, 1, min)
    idx_agrocnefe_st <- apply(st_agrocnefe_dists, 1, which.min)
    
    match_agrocnefe_st <- agrocnefe_st_muni$norm_street[idx_agrocnefe_st]
    match_long_agrocnefe_st <- agrocnefe_st_muni$longitude[idx_agrocnefe_st]
    match_lat_agrocnefe_st <- agrocnefe_st_muni$latitude[idx_agrocnefe_st]
    
    # Neighborhood matching
    bairro_agrocnefe_dists <- stringdist::stringdistmatrix(
      locais_muni$normalized_bairro,
      agrocnefe_bairro_muni$norm_bairro,
      method = "jw"
    )
    
    mindist_agrocnefe_bairro <- apply(bairro_agrocnefe_dists, 1, min)
    idx_agrocnefe_bairro <- apply(bairro_agrocnefe_dists, 1, which.min)
    
    match_agrocnefe_bairro <- agrocnefe_bairro_muni$norm_bairro[idx_agrocnefe_bairro]
    match_long_agrocnefe_bairro <- agrocnefe_bairro_muni$longitude[idx_agrocnefe_bairro]
    match_lat_agrocnefe_bairro <- agrocnefe_bairro_muni$latitude[idx_agrocnefe_bairro]
    
    # Create output
    output <- data.table(
      local_id = locais_muni$local_id,
      match_agrocnefe_st = match_agrocnefe_st,
      mindist_agrocnefe_st = mindist_agrocnefe_st,
      match_long_agrocnefe_st = match_long_agrocnefe_st,
      match_lat_agrocnefe_st = match_lat_agrocnefe_st,
      match_agrocnefe_bairro = match_agrocnefe_bairro,
      mindist_agrocnefe_bairro = mindist_agrocnefe_bairro,
      match_long_agrocnefe_bairro = match_long_agrocnefe_bairro,
      match_lat_agrocnefe_bairro = match_lat_agrocnefe_bairro
    )
  }
  
  return(output)
}

# ===== GEOCODEBR MATCHING FUNCTION =====

match_geocodebr_muni <- function(locais_muni, muni_ids = NULL) {
  # Match polling stations with geocodebr for a single municipality
  # Moved from geocodebr_matching.R
  
  # Wrap entire function in tryCatch to prevent pipeline crashes
  tryCatch({
    # Load geocodebr if not already loaded
    if (!requireNamespace("geocodebr", quietly = TRUE)) {
      warning("geocodebr package not installed. Returning NULL.")
      return(NULL)
    }
    
    # Check if we have data
    if (nrow(locais_muni) == 0) {
      return(NULL)
    }
    
    # Debug info
    muni_code <- unique(locais_muni$cod_localidade_ibge)
    muni_name <- unique(locais_muni$nm_localidade)
    message(sprintf("Processing municipality: %s (%s)", muni_name[1], muni_code[1]))
  
  # Prepare data for geocodebr
  dt_geocode <- locais_muni[, .(
    local_id = local_id,
    estado = sg_uf,
    municipio = nm_localidade,
    logradouro = ds_endereco,
    localidade = ds_bairro
  )]
  
  # Clean text fields - use simplified addresses for better matching
  # Process each column separately to isolate errors
  dt_geocode[, municipio := clean_text_for_geocodebr(municipio)]
  dt_geocode[, logradouro := simplify_address_for_geocodebr(logradouro)]
  dt_geocode[, localidade := clean_text_for_geocodebr(localidade)]
  
  # Remove rows with missing essential fields
  dt_geocode <- dt_geocode[!is.na(municipio) & !is.na(estado) & !is.na(logradouro)]
  
  if (nrow(dt_geocode) == 0) {
    return(NULL)
  }
  
  # Attempt geocoding with error handling
  geocoded_result <- tryCatch({
    # Ensure all text is properly encoded
    geocode_data <- dt_geocode[, .(estado, municipio, logradouro)]
    
    # Force UTF-8 encoding on all character columns
    char_cols <- names(geocode_data)[sapply(geocode_data, is.character)]
    for (col in char_cols) {
      set(geocode_data, j = col, value = enc2utf8(geocode_data[[col]]))
    }
    
    # Only use estado, municipio, logradouro to avoid database errors
    geocodebr::geocode(
      geocode_data,
      campos_endereco = geocodebr::definir_campos(
        estado = "estado",
        municipio = "municipio",
        logradouro = "logradouro"
      ),
      resolver_empates = TRUE,
      verboso = FALSE,
      cache = TRUE,
      n_cores = 1  # Single core for stability
    )
  }, error = function(e) {
    warning(paste("geocodebr error for municipality", 
                  unique(dt_geocode$municipio), ":", e$message))
    # Return empty result with correct structure
    data.table(
      estado = character(),
      municipio = character(),
      logradouro = character(),
      lat = numeric(),
      lon = numeric(),
      tipo_resultado = character(),
      precisao = character(),
      endereco_encontrado = character(),
      contagem_cnefe = integer()
    )
  })
  
  # If we got results, format them
  if (nrow(geocoded_result) > 0) {
    # Add local_id back
    geocoded_result[, local_id := dt_geocode$local_id]
    
    # Create output in format consistent with other matching functions
    output <- data.table(
      local_id = geocoded_result$local_id,
      match_geocodebr = geocoded_result$endereco_encontrado,
      mindist_geocodebr = 0,  # geocodebr doesn't provide distance metric
      match_long_geocodebr = geocoded_result$lon,
      match_lat_geocodebr = geocoded_result$lat,
      precisao_geocodebr = geocoded_result$precisao,
      tipo_resultado_geocodebr = geocoded_result$tipo_resultado,
      contagem_cnefe_geocodebr = geocoded_result$contagem_cnefe
    )
    
    return(output)
  } else {
    return(NULL)
  }
  }, error = function(e) {
    # If any error occurs, log it and return NULL
    warning(sprintf("Error in match_geocodebr_muni for municipality %s: %s", 
                    unique(locais_muni$cod_localidade_ibge), e$message))
    return(NULL)
  })
}