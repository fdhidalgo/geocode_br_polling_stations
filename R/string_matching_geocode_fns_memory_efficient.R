#' Memory-Efficient String Matching Functions for Geocoding
#' 
#' These functions replace the original string matching functions with
#' memory-efficient versions that use chunked processing and pre-filtering.

# Source the memory-efficient helper functions
source("R/memory_efficient_string_matching.R")

match_inep_muni <- function(locais_muni, inep_muni) {
  # this function operates on a single municipality
  
  if (nrow(inep_muni) == 0) {
    return(NULL) # Return NULL if inep_muni is empty
  }
  
  # Determine chunk size based on data size
  n_locais <- nrow(locais_muni)
  n_inep <- nrow(inep_muni)
  chunk_size <- get_adaptive_chunk_size(n_locais, n_inep)
  
  # Match on inep name with chunked processing
  name_result <- match_strings_memory_efficient(
    query_strings = locais_muni$normalized_name,
    target_strings = inep_muni$norm_school,
    use_prefilter = TRUE,
    chunk_size = chunk_size,
    method = "jw",
    normalize = TRUE
  )
  
  # Match on inep address with chunked processing  
  addr_result <- match_strings_memory_efficient(
    query_strings = locais_muni$normalized_addr,
    target_strings = inep_muni$norm_addr,
    use_prefilter = TRUE,
    chunk_size = chunk_size,
    method = "jw",
    normalize = TRUE
  )
  
  # Extract coordinates based on matches
  match_long_inep_name <- inep_muni$longitude[name_result$match_idx]
  match_lat_inep_name <- inep_muni$latitude[name_result$match_idx]
  match_long_inep_addr <- inep_muni$longitude[addr_result$match_idx]
  match_lat_inep_addr <- inep_muni$latitude[addr_result$match_idx]
  
  local_id <- locais_muni$local_id
  
  data.table(
    local_id,
    match_inep_name = name_result$match,
    mindist_name_inep = name_result$mindist,
    match_long_inep_name,
    match_lat_inep_name,
    match_inep_addr = addr_result$match,
    mindist_addr_inep = addr_result$mindist,
    match_long_inep_addr,
    match_lat_inep_addr
  )
}

match_schools_cnefe_muni <- function(locais_muni, schools_cnefe_muni) {
  # this function operates on a single municipality
  
  if (nrow(schools_cnefe_muni) == 0) {
    return(NULL) # Return NULL if there are no school data in that municipality
  }
  
  # Determine chunk size based on data size
  n_locais <- nrow(locais_muni)
  n_schools <- nrow(schools_cnefe_muni)
  chunk_size <- get_adaptive_chunk_size(n_locais, n_schools)
  
  # Match on school name with chunked processing
  name_result <- match_strings_memory_efficient(
    query_strings = locais_muni$normalized_name,
    target_strings = schools_cnefe_muni$norm_desc,
    use_prefilter = TRUE,
    chunk_size = chunk_size,
    method = "jw",
    normalize = TRUE
  )
  
  # Match on school address with chunked processing
  addr_result <- match_strings_memory_efficient(
    query_strings = locais_muni$normalized_addr,
    target_strings = schools_cnefe_muni$norm_addr,
    use_prefilter = TRUE,
    chunk_size = chunk_size,
    method = "jw",
    normalize = TRUE
  )
  
  # Extract coordinates based on matches
  match_long_schools_cnefe_name <- schools_cnefe_muni$cnefe_long[name_result$match_idx]
  match_lat_schools_cnefe_name <- schools_cnefe_muni$cnefe_lat[name_result$match_idx]
  match_long_schools_cnefe_addr <- schools_cnefe_muni$cnefe_long[addr_result$match_idx]
  match_lat_schools_cnefe_addr <- schools_cnefe_muni$cnefe_lat[addr_result$match_idx]
  
  local_id <- locais_muni$local_id
  
  data.table(
    local_id,
    match_schools_cnefe_name = name_result$match,
    mindist_name_schools_cnefe = name_result$mindist,
    match_long_schools_cnefe_name,
    match_lat_schools_cnefe_name,
    match_schools_cnefe_addr = addr_result$match,
    mindist_addr_schools_cnefe = addr_result$mindist,
    match_long_schools_cnefe_addr,
    match_lat_schools_cnefe_addr
  )
}

match_stbairro_cnefe_muni <- function(
  locais_muni,
  cnefe_st_muni,
  cnefe_bairro_muni
) {
  # this function operates on a single municipality
  
  if (nrow(cnefe_st_muni) == 0) {
    return(NULL)
  }
  
  locais_muni$normalized_st <- str_replace(
    locais_muni$normalized_st,
    ",.*$| sn$",
    ""
  )
  
  # Determine chunk size based on data size
  n_locais <- nrow(locais_muni)
  n_streets <- nrow(cnefe_st_muni)
  n_bairros <- nrow(cnefe_bairro_muni)
  chunk_size_st <- get_adaptive_chunk_size(n_locais, n_streets)
  chunk_size_bairro <- get_adaptive_chunk_size(n_locais, n_bairros)
  
  # Calculate the string distance between normalized street names
  st_result <- match_strings_memory_efficient(
    query_strings = locais_muni$normalized_st,
    target_strings = cnefe_st_muni$norm_street,
    use_prefilter = TRUE,
    chunk_size = chunk_size_st,
    method = "jw",
    normalize = TRUE
  )
  
  # Extract street match results
  match_st_cnefe <- st_result$match
  mindist_st_cnefe <- st_result$mindist
  match_long_st_cnefe <- cnefe_st_muni$long[st_result$match_idx]
  match_lat_st_cnefe <- cnefe_st_muni$lat[st_result$match_idx]
  
  local_id <- locais_muni$local_id
  
  locais_muni[normalized_bairro == "", normalized_bairro := normalized_st]
  
  # Calculate the string distance between normalized bairro names
  bairro_result <- match_strings_memory_efficient(
    query_strings = locais_muni$normalized_bairro,
    target_strings = cnefe_bairro_muni$norm_bairro,
    use_prefilter = TRUE,
    chunk_size = chunk_size_bairro,
    method = "jw",
    normalize = TRUE
  )
  
  # Extract bairro match results
  match_bairro_cnefe <- bairro_result$match
  mindist_bairro_cnefe <- bairro_result$mindist
  match_long_bairro_cnefe <- cnefe_bairro_muni$long[bairro_result$match_idx]
  match_lat_bairro_cnefe <- cnefe_bairro_muni$lat[bairro_result$match_idx]
  
  data.table(
    local_id,
    mindist_st_cnefe,
    match_st_cnefe,
    match_long_st_cnefe,
    match_lat_st_cnefe,
    mindist_bairro_cnefe,
    match_bairro_cnefe,
    match_long_bairro_cnefe,
    match_lat_bairro_cnefe
  )
}

match_stbairro_agrocnefe_muni <- function(
  locais_muni,
  agrocnefe_st_muni,
  agrocnefe_bairro_muni
) {
  # this function operates on a single municipality
  
  if (nrow(agrocnefe_st_muni) == 0) {
    return(NULL)
  }
  
  locais_muni$normalized_st <- str_replace(
    locais_muni$normalized_st,
    ",.*$| sn$",
    ""
  )
  
  # Determine chunk size based on data size
  n_locais <- nrow(locais_muni)
  n_streets <- nrow(agrocnefe_st_muni)
  n_bairros <- nrow(agrocnefe_bairro_muni)
  chunk_size_st <- get_adaptive_chunk_size(n_locais, n_streets)
  chunk_size_bairro <- get_adaptive_chunk_size(n_locais, n_bairros)
  
  # Calculate the string distance between normalized street names
  st_result <- match_strings_memory_efficient(
    query_strings = locais_muni$normalized_st,
    target_strings = agrocnefe_st_muni$norm_street,
    use_prefilter = TRUE,
    chunk_size = chunk_size_st,
    method = "jw",
    normalize = TRUE
  )
  
  # Extract street match results
  match_st_agrocnefe <- st_result$match
  mindist_st_agrocnefe <- st_result$mindist
  match_long_st_agrocnefe <- agrocnefe_st_muni$long[st_result$match_idx]
  match_lat_st_agrocnefe <- agrocnefe_st_muni$lat[st_result$match_idx]
  
  local_id <- locais_muni$local_id
  
  locais_muni[normalized_bairro == "", normalized_bairro := normalized_st]
  
  # Calculate the string distance between normalized bairro names
  bairro_result <- match_strings_memory_efficient(
    query_strings = locais_muni$normalized_bairro,
    target_strings = agrocnefe_bairro_muni$norm_bairro,
    use_prefilter = TRUE,
    chunk_size = chunk_size_bairro,
    method = "jw",
    normalize = TRUE
  )
  
  # Extract bairro match results
  match_bairro_agrocnefe <- bairro_result$match
  mindist_bairro_agrocnefe <- bairro_result$mindist
  match_long_bairro_agrocnefe <- agrocnefe_bairro_muni$long[bairro_result$match_idx]
  match_lat_bairro_agrocnefe <- agrocnefe_bairro_muni$lat[bairro_result$match_idx]
  
  out_data <- data.table(
    local_id,
    mindist_st_agrocnefe,
    match_st_agrocnefe,
    match_long_st_agrocnefe,
    match_lat_st_agrocnefe,
    mindist_bairro_agrocnefe,
    match_bairro_agrocnefe,
    match_long_bairro_agrocnefe,
    match_lat_bairro_agrocnefe
  )
  # Remove "agro" prefix
  setnames(out_data, names(out_data), gsub("agro", "", names(out_data)))
  out_data
}