## Fuzzy matching of polling station names/addresses to reference datasets (CNEFE, INEP, geocodebr).

library(data.table)
library(stringr)
library(stringdist)

# Nearest target string for each query, by Jaro-Winkler distance, considering only targets
# that share at least one whitespace-delimited word with the query. A query with no such
# target gets min_dist = Inf and an NA match. normalize_by_length divides the distance by
# the longer of the two strings, which favours longer targets; neighbourhood matching turns
# it off. Ties go to the lowest target index.
match_strings <- function(query_strings, target_strings, normalize_by_length = TRUE) {
  # An inverted word -> target index makes each query's candidate set a lookup, so no
  # query x target matrix is ever built.
  target_words <- strsplit(tolower(target_strings), "\\s+")
  word_index <- split(
    rep.int(seq_along(target_words), lengths(target_words)),
    unlist(target_words, use.names = FALSE)
  )
  target_nchar <- nchar(target_strings)

  # A municipality's rows are station-years, so the same address recurs across elections
  # (roughly 6 times nationally). Matching is pure, so each distinct query is matched once
  # and the results are expanded back over the input.
  queries <- unique(query_strings)
  query_words <- strsplit(tolower(queries), "\\s+")

  n <- length(queries)
  min_dist <- rep(Inf, n)
  best_match <- rep(NA_character_, n)
  best_index <- rep(NA_integer_, n)

  for (i in seq_len(n)) {
    candidates <- sort(unique(unlist(
      word_index[unique(query_words[[i]])],
      use.names = FALSE
    )))
    if (length(candidates) == 0L) {
      next
    }

    dists <- stringdist::stringdist(queries[i], target_strings[candidates], method = "jw")
    if (normalize_by_length) {
      dists <- dists / pmax(nchar(queries[i]), target_nchar[candidates])
    }

    best <- which.min(dists)
    min_dist[i] <- dists[best]
    best_index[i] <- candidates[best]
    best_match[i] <- target_strings[candidates[best]]
  }

  expand <- match(query_strings, queries)
  list(
    min_dist = min_dist[expand],
    best_match = best_match[expand],
    best_index = best_index[expand]
  )
}

# Jaro-Winkler distance between two already-paired string vectors, for the per-field
# similarity features the selection model consumes. Unlike match_strings() it chooses
# nothing: the caller has already picked the reference row. It also never divides by
# string length, so the features stay comparable across sources -- the length division in
# match_strings() is a ranking quirk, not a distance. NA on either side (no match at all,
# or a reference table without that field) yields NA, which lightgbm consumes directly.
field_distance <- function(x, y) {
  stringdist::stringdist(x, y, method = "jw")
}

match_inep_muni <- function(locais_muni, inep_muni) {
  # Match polling stations with INEP school data for a single municipality

  if (nrow(inep_muni) == 0) {
    return(NULL)
  }

  # Match on name
  name_results <- match_strings(
    locais_muni$normalized_name,
    inep_muni$norm_school
  )

  # Match on address
  addr_results <- match_strings(
    locais_muni$normalized_addr,
    inep_muni$norm_addr
  )

  name_idx <- name_results$best_index
  addr_idx <- addr_results$best_index

  # Each candidate is scored on *both* INEP fields, not just the one it was selected on:
  # a row that wins on the school name but whose address disagrees is now visible to the
  # selection model. INEP has no separate street or neighborhood column -- `norm_addr` is a
  # whole address line -- so sim_street and sim_bairro have nothing to compare against.
  # An unmatched station has best_index NA, and indexing by NA yields NA coordinates.
  data.table(
    local_id = locais_muni$local_id,
    match_inep_name = name_results$best_match,
    mindist_inep_name = name_results$min_dist,
    match_long_inep_name = inep_muni$longitude[name_idx],
    match_lat_inep_name = inep_muni$latitude[name_idx],
    sim_name_inep_name = field_distance(locais_muni$normalized_name, inep_muni$norm_school[name_idx]),
    sim_street_inep_name = NA_real_,
    sim_bairro_inep_name = NA_real_,
    sim_addr_inep_name = field_distance(locais_muni$normalized_addr, inep_muni$norm_addr[name_idx]),
    match_inep_addr = addr_results$best_match,
    mindist_inep_addr = addr_results$min_dist,
    match_long_inep_addr = inep_muni$longitude[addr_idx],
    match_lat_inep_addr = inep_muni$latitude[addr_idx],
    sim_name_inep_addr = field_distance(locais_muni$normalized_name, inep_muni$norm_school[addr_idx]),
    sim_street_inep_addr = NA_real_,
    sim_bairro_inep_addr = NA_real_,
    sim_addr_inep_addr = field_distance(locais_muni$normalized_addr, inep_muni$norm_addr[addr_idx])
  )
}

match_schools_cnefe_muni <- function(locais_muni, schools_cnefe_muni) {
  # Match polling stations with CNEFE school data

  if (nrow(schools_cnefe_muni) == 0) {
    return(NULL)
  }

  # Match on name
  name_results <- match_strings(
    locais_muni$normalized_name,
    schools_cnefe_muni$norm_desc
  )

  idx <- name_results$best_index

  # A CNEFE school row is a single establishment, so it carries a street and a
  # neighborhood alongside its name. Scoring all three is the point of the decomposition:
  # a school matched on name but sitting in the wrong bairro was previously indistinguishable
  # from one in the right place. There is no whole address line to compare, hence sim_addr NA.
  data.table(
    local_id = locais_muni$local_id,
    match_schools_cnefe = name_results$best_match,
    mindist_schools_cnefe = name_results$min_dist,
    match_long_schools_cnefe = schools_cnefe_muni$cnefe_long[idx],
    match_lat_schools_cnefe = schools_cnefe_muni$cnefe_lat[idx],
    match_bairro_schools_cnefe = schools_cnefe_muni$norm_bairro[idx],
    sim_name_schools_cnefe = field_distance(locais_muni$normalized_name, schools_cnefe_muni$norm_desc[idx]),
    sim_street_schools_cnefe = field_distance(locais_muni$normalized_st, schools_cnefe_muni$norm_street[idx]),
    sim_bairro_schools_cnefe = field_distance(locais_muni$normalized_bairro, schools_cnefe_muni$norm_bairro[idx]),
    sim_addr_schools_cnefe = NA_real_
  )
}

match_stbairro_muni <- function(locais_muni, st_muni, bairro_muni) {
  # Match polling stations against one census vintage's street and neighborhood
  # aggregates. Which vintage is named by the caller, not by these columns.

  if (nrow(st_muni) == 0) {
    return(NULL)
  }

  # Match on street
  st_results <- match_strings(
    locais_muni$normalized_st,
    st_muni$norm_street
  )

  # Match on neighborhood
  bairro_results <- match_strings(
    locais_muni$normalized_bairro,
    bairro_muni$norm_bairro,
    normalize_by_length = FALSE # Don't normalize for neighborhoods
  )

  st_idx <- st_results$best_index
  bairro_idx <- bairro_results$best_index

  # These two references are coordinate medians over a whole street or neighborhood, so
  # each knows exactly one field and the other three similarities have nothing to compare
  # against. They are still emitted: melt_match_candidates() needs one column per
  # (similarity field, candidate type) to line the candidates up.
  data.table(
    local_id = locais_muni$local_id,
    match_st = st_results$best_match,
    mindist_st = st_results$min_dist,
    match_long_st = st_muni$long[st_idx],
    match_lat_st = st_muni$lat[st_idx],
    sim_name_st = NA_real_,
    sim_street_st = field_distance(locais_muni$normalized_st, st_muni$norm_street[st_idx]),
    sim_bairro_st = NA_real_,
    sim_addr_st = NA_real_,
    match_bairro = bairro_results$best_match,
    mindist_bairro = bairro_results$min_dist,
    match_long_bairro = bairro_muni$long[bairro_idx],
    match_lat_bairro = bairro_muni$lat[bairro_idx],
    sim_name_bairro = NA_real_,
    sim_street_bairro = NA_real_,
    sim_bairro_bairro = field_distance(locais_muni$normalized_bairro, bairro_muni$norm_bairro[bairro_idx]),
    sim_addr_bairro = NA_real_
  )
}

match_geocodebr_muni <- function(locais_muni) {
  # Match polling stations with geocodebr for a single municipality.

  # Geocoding errors propagate to the caller; a municipality never drops out silently.
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

  # A station with no street, municipality, or state has nothing to geocode. Report the
  # count rather than let a silent drop look like a geocoding miss.
  n_before <- nrow(dt_geocode)
  dt_geocode <- dt_geocode[!is.na(municipio) & !is.na(estado) & !is.na(logradouro)]
  if (nrow(dt_geocode) < n_before) {
    message(sprintf(
      "  %d of %d stations have no address to geocode and are dropped",
      n_before - nrow(dt_geocode),
      n_before
    ))
  }

  if (nrow(dt_geocode) == 0) {
    return(NULL)
  }

  # local_id rides along as a non-address column so geocodebr reattaches it to each
  # result itself, rather than us reassigning coordinates by position afterward.
  geocode_data <- dt_geocode[, .(local_id, estado, municipio, logradouro)]

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
    n_cores = 1 # Single core for stability
  )

  # No geocoding hits is a legitimate empty result, not an error.
  if (nrow(geocoded_result) == 0) {
    return(NULL)
  }

  # Assert the round-trip so a coordinate can never be tied to the wrong station.
  stopifnot(
    "local_id" %in% names(geocoded_result),
    nrow(geocoded_result) == nrow(dt_geocode),
    !anyNA(geocoded_result$local_id)
  )

  # geocodebr returns one formatted string, "STREET - BAIRRO, MUNICIPIO - UF", rather than
  # structured fields, so the only field feature available is a whole-address-line
  # similarity: the station's street+neighborhood against the found address with its
  # municipality/state tail dropped. Municipality-precision rows resolved no street at all,
  # so their address line is just the municipality and carries no similarity signal.
  found_addr <- normalize_address(sub(",.*$", "", geocoded_result$endereco_encontrado))
  found_addr[geocoded_result$precisao == "municipio"] <- NA_character_
  station_addr <- locais_muni$normalized_addr[
    match(geocoded_result$local_id, locais_muni$local_id)
  ]

  # Create output in format consistent with other matching functions
  data.table(
    local_id = geocoded_result$local_id,
    match_geocodebr = geocoded_result$endereco_encontrado,
    mindist_geocodebr = 0, # geocodebr doesn't provide distance metric
    match_long_geocodebr = geocoded_result$lon,
    match_lat_geocodebr = geocoded_result$lat,
    sim_addr_geocodebr = field_distance(station_addr, found_addr),
    precisao_geocodebr = geocoded_result$precisao,
    tipo_resultado_geocodebr = geocoded_result$tipo_resultado,
    contagem_cnefe_geocodebr = geocoded_result$contagem_cnefe
  )
}
