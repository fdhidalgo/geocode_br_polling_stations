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

  # An unmatched station has best_index NA, and indexing by NA yields NA coordinates.
  data.table(
    local_id = locais_muni$local_id,
    match_inep_name = name_results$best_match,
    mindist_inep_name = name_results$min_dist,
    match_long_inep_name = inep_muni$longitude[name_results$best_index],
    match_lat_inep_name = inep_muni$latitude[name_results$best_index],
    match_inep_addr = addr_results$best_match,
    mindist_inep_addr = addr_results$min_dist,
    match_long_inep_addr = inep_muni$longitude[addr_results$best_index],
    match_lat_inep_addr = inep_muni$latitude[addr_results$best_index]
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

  data.table(
    local_id = locais_muni$local_id,
    match_schools_cnefe = name_results$best_match,
    mindist_schools_cnefe = name_results$min_dist,
    match_long_schools_cnefe = schools_cnefe_muni$cnefe_long[name_results$best_index],
    match_lat_schools_cnefe = schools_cnefe_muni$cnefe_lat[name_results$best_index],
    match_bairro_schools_cnefe = schools_cnefe_muni$norm_bairro[name_results$best_index]
  )
}

match_stbairro_cnefe_muni <- function(locais_muni, st_muni, bairro_muni, source) {
  # Match polling stations against one CNEFE vintage's street and neighborhood
  # aggregates. The regular and agricultural censuses are matched identically;
  # `source` ("cnefe" / "agrocnefe") only names the output columns, which is how
  # the model tells the two vintages' candidates apart once they are stacked.

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

  # Four columns per component, in the order they are built below.
  col_names <- function(component) {
    paste0(c("match", "mindist", "match_long", "match_lat"), "_", source, "_", component)
  }

  out <- data.table(
    local_id = locais_muni$local_id,
    st_results$best_match,
    st_results$min_dist,
    st_muni$long[st_results$best_index],
    st_muni$lat[st_results$best_index],
    bairro_results$best_match,
    bairro_results$min_dist,
    bairro_muni$long[bairro_results$best_index],
    bairro_muni$lat[bairro_results$best_index]
  )
  setnames(out, c("local_id", col_names("st"), col_names("bairro")))
  out
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

  # Create output in format consistent with other matching functions
  data.table(
    local_id = geocoded_result$local_id,
    match_geocodebr = geocoded_result$endereco_encontrado,
    mindist_geocodebr = 0, # geocodebr doesn't provide distance metric
    match_long_geocodebr = geocoded_result$lon,
    match_lat_geocodebr = geocoded_result$lat,
    precisao_geocodebr = geocoded_result$precisao,
    tipo_resultado_geocodebr = geocoded_result$tipo_resultado,
    contagem_cnefe_geocodebr = geocoded_result$contagem_cnefe
  )
}
