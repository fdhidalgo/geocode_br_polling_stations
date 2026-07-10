## String Matching Functions
## 
## Functions for fuzzy string matching between polling station addresses and
## reference datasets (CNEFE, INEP schools, geocodebr). Uses Jaro-Winkler
## distance for name matching and Levenshtein distance for addresses.
## All functions implement memory-efficient chunked processing to handle
## large datasets without exhausting system memory.

library(data.table)
library(stringr)
library(stringdist)

# ===== MEMORY EFFICIENT HELPER FUNCTIONS =====
# These functions enable processing of large string matching tasks
# by breaking them into manageable chunks and pre-filtering candidates

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

  # Never chunk larger than the number of query items: chunking beyond n_items
  # yields a single chunk of n_items anyway, so cap here for an honest chunk
  # size. This cap wins over the lower bound when n_items < 100.
  chunk_size <- min(chunk_size, n_items)

  return(chunk_size)
}

# ===== UNIFIED STRING MATCHING FUNCTIONS =====

match_inep_muni <- function(locais_muni, inep_muni) {
  # Match polling stations with INEP school data for a single municipality
  
  if (nrow(inep_muni) == 0) {
    return(NULL)
  }
  
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
  
  return(output)
}

match_schools_cnefe_muni <- function(locais_muni, schools_cnefe_muni) {
  # Match polling stations with CNEFE school data
  
  if (nrow(schools_cnefe_muni) == 0) {
    return(NULL)
  }
  
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
  
  return(output)
}

match_stbairro_cnefe_muni <- function(locais_muni, cnefe_st_muni, cnefe_bairro_muni) {
  # Match polling stations with CNEFE street and neighborhood data
  
  if (nrow(cnefe_st_muni) == 0) {
    return(NULL)
  }
  
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
    cnefe_st_muni$long[st_results$best_index]
  )
  match_lat_cnefe_st <- ifelse(
    is.na(st_results$best_index),
    NA_real_,
    cnefe_st_muni$lat[st_results$best_index]
  )
  
  match_long_cnefe_bairro <- ifelse(
    is.na(bairro_results$best_index),
    NA_real_,
    cnefe_bairro_muni$long[bairro_results$best_index]
  )
  match_lat_cnefe_bairro <- ifelse(
    is.na(bairro_results$best_index),
    NA_real_,
    cnefe_bairro_muni$lat[bairro_results$best_index]
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
  
  return(output)
}

match_stbairro_agrocnefe_muni <- function(locais_muni, agrocnefe_st_muni, agrocnefe_bairro_muni) {
  # Match polling stations with Agro CNEFE data
  # This follows the same pattern as match_stbairro_cnefe_muni
  
  if (nrow(agrocnefe_st_muni) == 0) {
    return(NULL)
  }
  
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
    agrocnefe_st_muni$long[st_results$best_index]
  )
  match_lat_agrocnefe_st <- ifelse(
    is.na(st_results$best_index),
    NA_real_,
    agrocnefe_st_muni$lat[st_results$best_index]
  )
  
  match_long_agrocnefe_bairro <- ifelse(
    is.na(bairro_results$best_index),
    NA_real_,
    agrocnefe_bairro_muni$long[bairro_results$best_index]
  )
  match_lat_agrocnefe_bairro <- ifelse(
    is.na(bairro_results$best_index),
    NA_real_,
    agrocnefe_bairro_muni$lat[bairro_results$best_index]
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
  
  return(output)
}

# ===== GEOCODEBR MATCHING FUNCTION =====

match_geocodebr_muni <- function(locais_muni, muni_ids = NULL) {
  # Match polling stations with geocodebr for a single municipality.
  #
  # Fail-loud contract (cleanup phase 3, finding C5): geocodebr must be
  # installed, and any geocoding error propagates to the caller rather than
  # being converted to a warning + NULL or an empty result. The batch driver
  # (process_geocodebr_batch) applies the collect-and-stop convention, so a
  # failing municipality is surfaced instead of silently dropped from coverage.
  # A missing package is a structural precondition and stops immediately here.
  if (!requireNamespace("geocodebr", quietly = TRUE)) {
    stop("geocodebr package not installed; it is required for match_geocodebr_muni().")
  }

  # No polling stations to geocode is a legitimate empty case, not an error.
  if (nrow(locais_muni) == 0) {
    return(NULL)
  }

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
  dt_geocode[, municipio := clean_text_for_geocodebr(municipio)]
  dt_geocode[, logradouro := simplify_address_for_geocodebr(logradouro)]
  dt_geocode[, localidade := clean_text_for_geocodebr(localidade)]

  # Remove rows with missing essential fields
  dt_geocode <- dt_geocode[!is.na(municipio) & !is.na(estado) & !is.na(logradouro)]

  if (nrow(dt_geocode) == 0) {
    return(NULL)
  }

  # Carry local_id through as a passthrough column so geocodebr reattaches it to
  # each result via its internal row id (see the row-count assertion below),
  # rather than us reassigning it by position afterward. Only estado, municipio,
  # logradouro are address fields; local_id is returned unchanged in the result.
  geocode_data <- dt_geocode[, .(local_id, estado, municipio, logradouro)]
  char_cols <- names(geocode_data)[sapply(geocode_data, is.character)]
  for (col in char_cols) {
    set(geocode_data, j = col, value = enc2utf8(geocode_data[[col]]))
  }

  geocoded_result <- geocodebr::geocode(
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

  # No geocoding hits is a legitimate empty result, not an error.
  if (nrow(geocoded_result) == 0) {
    return(NULL)
  }

  # geocodebr returns one row per input row (ties resolved) with all input
  # columns preserved, so local_id is already attached to the correct result.
  # Assert the invariant so a coordinate can never be tied to the wrong polling
  # station: local_id must survive the round-trip and the row count must match.
  stopifnot(
    "local_id" %in% names(geocoded_result),
    nrow(geocoded_result) == nrow(dt_geocode),
    !anyNA(geocoded_result$local_id)
  )

  # Create output in format consistent with other matching functions
  data.table(
    local_id = geocoded_result$local_id,
    match_geocodebr = geocoded_result$endereco_encontrado,
    mindist_geocodebr = 0,  # geocodebr doesn't provide distance metric
    match_long_geocodebr = geocoded_result$lon,
    match_lat_geocodebr = geocoded_result$lat,
    precisao_geocodebr = geocoded_result$precisao,
    tipo_resultado_geocodebr = geocoded_result$tipo_resultado,
    contagem_cnefe_geocodebr = geocoded_result$contagem_cnefe
  )
}