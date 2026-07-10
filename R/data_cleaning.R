## Data Import and Cleaning Functions
## 
## Functions for importing and cleaning polling station data from multiple sources:
## - TSE geocoded polling stations (2018-2024)
## - CNEFE census data (2010, 2017, 2022) 
## - INEP school census data
## - Municipal demographic and geographic data
## Includes memory-efficient processing for large CNEFE files (>40GB)

library(data.table)
library(stringr)

# ===== COLUMN STANDARDIZATION =====
# Handles variations in column naming across different data sources
# to ensure consistent processing throughout the pipeline

standardize_column_names <- function(dt, inplace = FALSE) {
  # Standardize column names across different data sources
  # This function handles variations in column naming conventions
  
  if (!inplace) {
    dt <- copy(dt)
  }
  
  # Get current names
  old_names <- names(dt)
  
  # Common replacements
  replacements <- c(
    # TSE naming variations
    "cd_municipio" = "cd_localidade_tse",
    "cod_municipio" = "cd_localidade_tse",
    "sg_uf" = "estado_abrev",
    "nm_locvot" = "nm_local_votacao",
    "ds_endereco" = "nm_endereco",
    "ds_bairro" = "nm_bairro",
    "nr_locvot" = "nr_local_votacao",
    
    # CNEFE naming variations
    "dsc_estabelecimento" = "desc_estabelecimento",
    "nom_seglogr" = "nome_logradouro",
    "nom_tipo_seglogr" = "tipo_logradouro",
    "nom_titulo_seglogr" = "titulo_logradouro",
    "dsc_localidade" = "nome_localidade",
    
    # Panel ID naming
    ".x_local_id" = "x_local_id",
    ".y_local_id" = "y_local_id"
  )
  
  # Apply replacements
  new_names <- old_names
  for (old in names(replacements)) {
    idx <- which(new_names == old)
    if (length(idx) > 0) {
      new_names[idx] <- replacements[old]
    }
  }
  
  # Set new names
  data.table::setnames(dt, old_names, new_names)
  
  if (!inplace) {
    return(dt)
  }
}

# ===== DATA CLEANING FUNCTIONS =====

# Human-readable labels for CNEFE "espécie de endereço" codes, used by
# clean_cnefe10() and clean_cnefe22(). The two censuses code establishment types
# differently: 2010 uses 7 codes (keyed on `especie`); 2022 uses 8 codes (keyed
# on `cod_especie`), widening code 7's label to include reforms and adding a
# distinct "estabelecimento religioso" (code 8). Defined once here (cleanup phase
# 4 dedup) with the former "estabeleciemento" typo corrected to "estabelecimento"
# — an output-visible label change noted for the 2024 release.
cnefe_especie_labels <- function(year) {
  common <- c(
    "domicílio particular",
    "domicílio coletivo",
    "estabelecimento agropecuário",
    "estabelecimento de ensino",
    "estabelecimento de saúde",
    "estabelecimento de outras finalidades"
  )
  if (year == 2010) {
    data.table(
      especie = 1:7,
      especie_lab = c(common, "edificação em construção")
    )
  } else if (year == 2022) {
    data.table(
      cod_especie = 1:8,
      especie_lab = c(
        common,
        "edificação em construção ou reforma",
        "estabelecimento religioso"
      )
    )
  } else {
    stop(sprintf("cnefe_especie_labels(): unsupported CNEFE year %s.", year))
  }
}

clean_cnefe22 <- function(cnefe22_file, muni_ids) {
  # Accept either file path or data.table
  if (is.character(cnefe22_file)) {
    cnefe22 <- fread(
      cnefe22_file,
      drop = c(
      "nom_comp_elem1",
      "val_comp_elem1",
      "nom_comp_elem2",
      "val_comp_elem2",
      "nom_comp_elem3",
      "val_comp_elem3",
      "nom_comp_elem4",
      "val_comp_elem4",
      "nom_comp_elem5",
      "val_comp_elem5",
      "num_quadra",
      "num_face",
      "cod_unico_endereco"
    )
  )
  } else {
    # If already a data.table, use it directly
    cnefe22 <- cnefe22_file
  }

  setnames(cnefe22, names(cnefe22), tolower(names(cnefe22)))

  # Create address variable
  cnefe22[,
    num_endereco_char := fifelse(
      num_endereco == 0,
      dsc_modificador,
      as.character(num_endereco)
    )
  ]
  # Remove "SN" (sem número/no number) - a placeholder that interferes with address matching
  cnefe22[, num_endereco_char := str_remove(num_endereco_char, "SN")]
  cnefe22[,
    dsc_modificador_nosn := fifelse(
      dsc_modificador != "SN",
      dsc_modificador,
      ""
    )
  ]

  cnefe22[,
    address := str_squish(paste(
      nom_tipo_seglogr,
      nom_titulo_seglogr,
      nom_seglogr,
      num_endereco_char,
      dsc_modificador_nosn
    ))
  ]
  cnefe22[,
    street := str_squish(paste(
      nom_tipo_seglogr,
      nom_titulo_seglogr,
      nom_seglogr
    ))
  ]
  cnefe22[,
    c(
      "nom_tipo_seglogr",
      "nom_titulo_seglogr",
      "num_endereco_char",
      "num_endereco",
      "nom_seglogr",
      "dsc_modificador_nosn",
      "dsc_modificador"
    ) := NULL
  ]

  # Add NAs where data is missing
  cnefe22[dsc_estabelecimento == "", dsc_estabelecimento := NA]
  # Remove extraneous white space from dsc_estabelecimento
  cnefe22[, dsc_estabelecimento := str_squish(dsc_estabelecimento)]

  setnames(cnefe22, "cod_municipio", "id_munic_7")

  # muni_ids is the municipality crosswalk; an empty table is a structural
  # failure, not a state without municipalities. Stop rather than filling the
  # id_TSE/municipio/estado_abrev columns with NA (cleanup phase 3, finding H1).
  if (nrow(muni_ids) == 0) {
    stop("clean_cnefe22(): muni_ids is empty; cannot attach municipality identifiers.")
  }
  cnefe22 <- merge(
    cnefe22,
    muni_ids[, .(id_munic_7, id_TSE, municipio, estado_abrev)],
    by.x = "id_munic_7",
    by.y = "id_munic_7",
    all.x = TRUE
  )
  # Add human-readable labels for CNEFE establishment types (especie codes)
  especie_labs <- cnefe_especie_labels(2022)

  cnefe22 <- merge(
    cnefe22,
    especie_labs,
    by = "cod_especie",
    all.x = TRUE
  )

  # Make smaller CNEFE dataset
  addr <- cnefe22[, .(
    id_munic_7,
    id_TSE,
    municipio,
    especie_lab,
    street,
    address,
    bairro = dsc_localidade,
    desc = dsc_estabelecimento,
    cnefe_long = longitude,
    cnefe_lat = latitude
  )]

  # Normalize addresses
  addr[, norm_address := normalize_address(address)]
  addr[, norm_bairro := normalize_address(bairro)]
  addr[, norm_street := normalize_address(street)]

  addr
}

clean_tsegeocoded_locais <- function(tse_files, muni_ids, locais) {
  # tse_files lists the TSE geocoded ground-truth files (2018, 2020, 2022, 2024).
  # The 2024 file was re-wired in for the 2006-2024 re-release (#48); assert the
  # expected count so a missing file fails loud here rather than being silently
  # dropped by an existence guard (cleanup phase 3, finding H1).
  expected_tse_files <- 4L
  if (length(tse_files) != expected_tse_files) {
    stop(sprintf(
      "Expected %d TSE geocoded files, got %d: %s",
      expected_tse_files, length(tse_files),
      paste(tse_files, collapse = ", ")
    ))
  }

  # Read each year. Encoding/parse warnings are allowed to surface once rather
  # than being masked by a suppressWarnings() re-read (Medium: TSE reads).
  loc_list <- lapply(tse_files, function(f) fread(f, encoding = "Latin-1"))

  # Keep only the columns present in every year (a schema change in any year is
  # not papered over by rbindlist(fill = TRUE)); assert the survivors are
  # non-empty (cleanup phase 3, finding H1).
  common_cols <- Reduce(intersect, lapply(loc_list, names))
  if (length(common_cols) == 0) {
    stop("No columns are common to all TSE geocoded files.")
  }
  loc_list <- lapply(loc_list, function(x) x[, common_cols, with = FALSE])
  locs <- rbindlist(loc_list, use.names = TRUE)

  # Convert column names to lowercase
  setnames(locs, names(locs), tolower(names(locs)))

  # Remove duplicate rows and filter out polling stations out of the country
  locs <- unique(locs[
    sg_uf != "ZZ",
    .(
      aa_eleicao,
      sg_uf,
      cd_municipio,
      nm_municipio,
      nr_zona,
      nr_local_votacao,
      nm_local_votacao,
      ds_endereco,
      nm_bairro,
      nr_cep,
      nr_latitude,
      nr_longitude
    )
  ])

  # Set 'nr_latitude' and 'nr_longitude' to NA if they are equal to -1
  locs[, nr_latitude := ifelse(nr_latitude == -1, NA, nr_latitude)]
  locs[, nr_longitude := ifelse(nr_longitude == -1, NA, nr_longitude)]

  # Remove rows with NA values in 'nr_latitude'
  locs <- locs[!is.na(nr_latitude)]

  # Merge 'locs' with 'muni_ids' based on 'cd_municipio' and 'id_TSE'
  locs <- merge(
    locs,
    muni_ids[, .(id_munic_7, id_TSE)],
    by.x = c("cd_municipio"),
    by.y = c("id_TSE"),
    all.x = TRUE
  )

  # Rename columns in 'locs'
  setnames(
    locs,
    c(
      "aa_eleicao",
      "id_munic_7",
      "nr_local_votacao",
      "nr_latitude",
      "nr_longitude"
    ),
    c("ano", "cod_localidade_ibge", "nr_locvot", "tse_lat", "tse_long")
  )

  # Merge 'locs' with 'locais' based on 'ano', 'cod_localidade_ibge', 'nr_zona', and 'nr_locvot'
  locs <- merge(
    locs,
    locais[, .(
      local_id,
      ano,
      cod_localidade_ibge,
      nr_zona,
      nr_locvot
    )],
    all.x = TRUE,
    all.y = FALSE,
    by = c("ano", "cod_localidade_ibge", "nr_zona", "nr_locvot")
  )

  # Remove rows with NA values in 'local_id'
  locs <- locs[!is.na(local_id)]

  # Group by 'local_id' and keep only the most recent year
  locs <- locs[locs[, .I[which.max(ano)], by = local_id]$V1]

  locs
}

clean_agro_cnefe <- function(agro_cnefe_files, muni_ids) {
  # Read and combine all agro census files
  agro_list <- lapply(agro_cnefe_files, function(file) {
    fread(file, encoding = "UTF-8", sep = ";")
  })
  agro_cnefe <- rbindlist(agro_list, fill = TRUE)

  # Convert column names to lowercase first (agro files have uppercase columns)
  setnames(agro_cnefe, names(agro_cnefe), tolower(names(agro_cnefe)))
  
  # Create street column combining type and name BEFORE standardization
  agro_cnefe[,
    street := str_squish(paste(
      nom_tipo_seglogr,
      nom_titulo_seglogr,
      nom_seglogr
    ))
  ]
  
  # Create id_munic_7 from cod_uf and cod_municipio BEFORE standardization
  agro_cnefe[, id_munic_7 := as.numeric(paste0(cod_uf, cod_municipio))]
  
  # Now standardize column names
  standardize_column_names(agro_cnefe, inplace = TRUE)
  
  # Create normalized street and neighborhood columns
  agro_cnefe[, norm_street := normalize_address(street)]
  agro_cnefe[, norm_bairro := normalize_address(nome_localidade)]
  
  # Convert latitude and longitude to numeric
  agro_cnefe[, latitude := as.numeric(latitude)]
  agro_cnefe[, longitude := as.numeric(longitude)]

  # Join with municipality IDs
  agro_cnefe <- muni_ids[
    agro_cnefe,
    on = .(id_munic_7),
    nomatch = NA
  ]

  return(agro_cnefe)
}

repair_mixed_utf8 <- function(x) {
  # Repair a character vector that mixes valid UTF-8 and Latin-1 bytes to
  # uniformly valid UTF-8. The consolidated polling-station export concatenates
  # source years with inconsistent encodings, so a single declared encoding on
  # read mislabels some strings (e.g. the "Nº" ordinal as the lone Latin-1 byte
  # 0xBA, invalid UTF-8). Reinterpreting the whole column as Latin-1 would instead
  # corrupt the genuinely-UTF-8 rows, so repair per string: only strings that fail
  # UTF-8 validation have their raw bytes reinterpreted as Latin-1; valid UTF-8 is
  # left untouched. Downstream string ops (stringi::stri_trans_general in
  # clean_text_for_geocodebr) require valid UTF-8 and error loudly on invalid input.
  # Edge case: a Latin-1 string whose bytes happen to form valid UTF-8 is left as
  # is (the per-string heuristic can't detect it); this is inherent to repairing a
  # mixed-encoding column without per-row source metadata, and does not arise for
  # the observed lone-high-byte case (e.g. 0xBA "Nº").
  # which() drops NAs: stri_enc_isutf8(NA) is NA, and a logical index containing
  # NA would error in the subassignment ("NAs are not allowed in subscripted
  # assignments"). NA strings need no repair and pass through enc2utf8() unchanged.
  bad <- which(!stringi::stri_enc_isutf8(x))
  x[bad] <- iconv(x[bad], from = "latin1", to = "UTF-8")
  enc2utf8(x)
}

import_locais <- function(locais_file, muni_ids) {
  # Read raw bytes (encoding = "unknown"), then repair per-string. The file mixes
  # UTF-8 and Latin-1 across source years, so neither a blanket "UTF-8" nor a
  # blanket "Latin-1" fread is correct; repair_mixed_utf8() fixes each string by
  # its own byte validity. This replaces a fragile filename-based encoding guess.
  locais_data <- fread(locais_file, encoding = "unknown")

  char_cols <- names(locais_data)[vapply(locais_data, is.character, logical(1))]
  for (col in char_cols) {
    set(locais_data, j = col, value = repair_mixed_utf8(locais_data[[col]]))
  }

  # Clean column names
  setnames(locais_data, janitor::make_clean_names(names(locais_data)))

  # Replace NA values with empty strings
  locais_data[, ds_bairro := ifelse(is.na(ds_bairro), "", ds_bairro)]
  locais_data[, ds_endereco := ifelse(is.na(ds_endereco), "", ds_endereco)]

  # Normalize and mutate columns
  locais_data[, normalized_name := normalize_school(nm_locvot)]
  locais_data[,
    normalized_addr := paste(
      normalize_address(ds_endereco),
      normalize_address(ds_bairro)
    )
  ]
  locais_data[, normalized_st := normalize_address(ds_endereco)]
  locais_data[, normalized_bairro := normalize_address(ds_bairro)]

  # Load muni_ids and merge
  locais_data <- merge(
    locais_data,
    muni_ids[, .(cod_localidade_ibge = id_munic_7, cd_localidade_tse = id_TSE)],
    by = "cd_localidade_tse",
    all.x = TRUE
  )

  # Remove polling stations abroad
  locais_data <- locais_data[sg_uf != "ZZ"]

  # Deterministic local_id (H6, #36/#48). Order the rows by the natural
  # station-year key and assign local_id from that ordering, so the id no longer
  # depends on the input file's row order (the former `local_id := .I` was
  # run-order-dependent, breaking reproducibility). The key
  # (ano, cod_localidade_ibge, nr_zona, nr_locvot) is verified unique across the
  # full 2006-2024 input; the stop() below makes that a permanent invariant. As a
  # consequence local_id values are reproducible within a release but are NOT
  # comparable across releases (documented in the v0.15 release notes).
  id_keys <- c("ano", "cod_localidade_ibge", "nr_zona", "nr_locvot")
  n_dup <- sum(duplicated(locais_data, by = id_keys))
  if (n_dup > 0L) {
    stop(sprintf(
      paste0(
        "local_id key not unique: %d duplicate rows on ",
        "(ano, cod_localidade_ibge, nr_zona, nr_locvot) in the polling-station ",
        "input. A deterministic local_id requires this key to be unique."
      ),
      n_dup
    ))
  }
  setorderv(locais_data, id_keys, na.last = TRUE)
  locais_data[, local_id := .I]

  return(locais_data)
}

finalize_coords <- function(locais, model_predictions, tsegeocoded_locais) {
  # Order within local_id by pred_dist and pick the first one in each group
  best_match <- model_predictions[
    order(local_id, pred_dist),
    .(local_id, long, lat, pred_dist)
  ]
  best_match <- best_match[, .SD[1], by = local_id]

  geocoded_locais <- merge(
    locais,
    best_match,
    by = "local_id",
    all.x = TRUE
  )
  geocoded_locais[,
    c(
      "normalized_name",
      "normalized_addr",
      "normalized_st",
      "normalized_bairro"
    ) := NULL
  ]
  setnames(geocoded_locais, c("long", "lat"), c("pred_long", "pred_lat"))

  # Merge with TSE geocoded data
  geocoded_locais <- merge(
    geocoded_locais,
    tsegeocoded_locais[, .(local_id, tse_lat, tse_long)],
    by = "local_id",
    all.x = TRUE
  )

  # Use TSE coordinates when available, otherwise use predicted
  geocoded_locais[, final_long := ifelse(is.na(tse_long), pred_long, tse_long)]
  geocoded_locais[, final_lat := ifelse(is.na(tse_lat), pred_lat, tse_lat)]

  return(geocoded_locais)
}

make_tract_centroids <- function(tracts) {
  # Transform to appropriate CRS and calculate centroids
  tracts$centroid <- sf::st_transform(tracts, 4674) |>
    sf::st_centroid() |>
    sf::st_geometry()

  # Extract coordinates
  tracts$tract_centroid_long <- sf::st_coordinates(tracts$centroid)[, 1]
  tracts$tract_centroid_lat <- sf::st_coordinates(tracts$centroid)[, 2]

  # Convert to data.table and keep only needed columns
  tracts <- sf::st_drop_geometry(tracts)
  tracts <- data.table(tracts)[, .(
    setor_code = code_tract,
    zone,
    tract_centroid_lat,
    tract_centroid_long
  )]

  return(tracts)
}

normalize_address <- function(x) {
  # Normalize addresses for consistent matching across datasets
  # The order of operations matters: lowercase -> transliterate -> remove punctuation
  # This ensures maximum consistency when comparing addresses from different sources
  result <- str_to_lower(x)
  result <- stringi::stri_trans_general(result, "Latin-ASCII")
  result <- str_remove_all(result, "[[:punct:]]")
  # Remove generic location descriptors that add noise without improving matches
  # These terms are inconsistently used across datasets
  result <- str_remove(result, "\\bzona rural\\b")
  result <- str_remove(result, "\\bpovoado\\b")
  result <- str_remove(result, "\\blocalidade\\b")
  result <- str_remove_all(result, "\\.|\\/")
  result <- str_replace_all(result, "^av\\b", "avenida")
  result <- str_replace_all(result, "^r\\b", "rua")
  result <- str_replace_all(result, "\\bs n\\b", "sn")
  result <- str_squish(result)

  return(result)
}

## Common school-related terms, stripped from school names for better matching in
## normalize_school() and used as a school-detection feature by make_model_data()
## in R/model.R. Defined once here and reused in both places (cleanup phase 4
## dedup). Order is irrelevant — both uses build one regex alternation.
school_synonyms <- c(
  "e m e i",
  "esc inf",
  "esc mun",
  "unidade escolar",
  "centro educacional",
  "escola municipal",
  "colegio estadual",
  "cmei",
  "emeif",
  "emeief",
  "grupo escolar",
  "escola estadual",
  "erem",
  "colegio municipal",
  "centro de ensino infantil",
  "escola mul",
  "e m",
  "grupo municipal",
  "e e",
  "creche",
  "escola",
  "colegio",
  "em",
  "de referencia",
  "centro comunitario",
  "grupo",
  "de referencia em ensino medio",
  "intermediaria",
  "ginasio municipal",
  "ginasio",
  "emef",
  "centro de educacao infantil",
  "esc",
  "ee",
  "e f",
  "cei",
  "emei",
  "ensino fundamental",
  "ensino medio",
  "eeief",
  "eef",
  "ens fun",
  "eem",
  "eeem",
  "est ens med",
  "est ens fund",
  "ens fund",
  "mul",
  "professora",
  "professor",
  "eepg",
  "eemg",
  "prof"
)

normalize_school <- function(x) {
  ## Normalize school names by removing generic terms and standardizing format
  result <- stringi::stri_trans_general(x, "Latin-ASCII")
  result <- str_to_lower(result)
  result <- str_remove_all(result, "\\.")
  result <- str_remove_all(result, "[[:punct:]]")
  result <- str_squish(result)
  result <- str_remove_all(
    result,
    paste0("\\b", school_synonyms, "\\b", collapse = "|")
  )
  result <- str_squish(result)

  return(result)
}

clean_inep <- function(inep_data, inep_codes) {
  # Standardize column names - remove diacritics and spaces
  setnames(
    inep_data,
    names(inep_data),
    str_replace_all(
      stringi::stri_trans_general(tolower(names(inep_data)), "Latin-ASCII"),
      " ",
      "_"
    )
  )

  # Filter and select relevant columns
  inep_data <- inep_data[
    !is.na(latitude),
    .(
      escola,
      codigo_inep,
      uf,
      municipio,
      endereco,
      latitude,
      longitude
    )
  ]

  # Merge with inep_codes
  inep_data <- inep_codes[inep_data, on = "codigo_inep"]

  # Normalize name and address
  inep_data[, norm_school := normalize_school(escola)]
  inep_data[, norm_addr := normalize_address(endereco)]

  # Remove CEP and municipality from address
  inep_data[, norm_addr := str_remove(norm_addr, " ([0-9]{5}).*")]

  return(inep_data)
}

calc_muni_area <- function(muni_shp) {
  # Calculate the area of each municipality
  area <- sf::st_area(muni_shp)
  muni_shp$area <- area
  muni_shp <- sf::st_drop_geometry(muni_shp)
  setDT(muni_shp)
  muni_shp[, .(cod_localidade_ibge = code_muni, area)]
}

get_cnefe22_schools <- function(cnefe22) {
  # Extract schools from the CNEFE 2022 data
  schools_cnefe22 <- cnefe22[especie_lab == "estabelecimento de ensino"]
  schools_cnefe22[, norm_desc := normalize_school(desc)]
  schools_cnefe22[norm_desc != ""]
}

convert_coord <- function(coord) {
  # Function to convert a single coordinate to decimal degrees
  # Handle potential errors in coordinate parsing
  
  tryCatch({
    parts <- unlist(strsplit(coord, " "))
    
    # Check if we have enough parts
    if (length(parts) < 4) {
      return(NA_real_)
    }
    
    degrees <- suppressWarnings(as.numeric(parts[1]))
    minutes <- suppressWarnings(as.numeric(parts[2]))
    seconds <- suppressWarnings(as.numeric(parts[3]))
    direction <- gsub("[^NSWO]", "", parts[4])
    
    # Check for NA values from conversion
    if (is.na(degrees) || is.na(minutes) || is.na(seconds)) {
      return(NA_real_)
    }
    
    decimal_degrees <- degrees + (minutes / 60) + (seconds / 3600)
    
    if (direction %in% c("S", "W", "O")) {
      decimal_degrees <- -decimal_degrees
    }
    
    return(decimal_degrees)
  }, error = function(e) {
    return(NA_real_)
  })
}

convert_coords_checked <- function(coord_strings, coord_name = "coordinate") {
  # Convert a vector of DMS coordinate strings to decimal degrees, accounting for
  # parse failures (cleanup phase 3, Medium). convert_coord() silently returns NA
  # on a malformed value; here we count those NAs, stop if EVERY value failed (a
  # systematic parse failure, e.g. a format change), and surface the NA rate as a
  # condition so a high failure rate is visible rather than silent.
  converted <- vapply(coord_strings, convert_coord, numeric(1), USE.NAMES = FALSE)

  n <- length(converted)
  if (n > 0) {
    n_na <- sum(is.na(converted))
    if (n_na == n) {
      stop(sprintf(
        "All %d %s values failed to parse to decimal degrees.",
        n, coord_name
      ))
    }
    if (n_na > 0) {
      message(sprintf(
        "%s: %d/%d (%.1f%%) values failed to parse and are NA.",
        coord_name, n_na, n, 100 * n_na / n
      ))
    }
  }

  converted
}


clean_cnefe10 <- function(cnefe_file, muni_ids, tract_centroids, extract_schools = FALSE) {
  # Memory-efficient processing of CNEFE 2010 data
  
  # Monitor memory before processing
  mem_before <- gc()[2, 2]
  message(sprintf("Memory before CNEFE processing: %.1f GB", mem_before / 1024))
  
  # Define the processing function for each chunk
  process_chunk <- function(cnefe_chunk) {
    # Standardize column names
    setnames(cnefe_chunk, names(cnefe_chunk), tolower(names(cnefe_chunk)))
    
    # Drop unnecessary columns early to save memory
    cols_to_drop <- c(
      "situacao_setor", "nom_comp_elem1", "val_comp_elem1",
      "nom_comp_elem2", "val_comp_elem2", "nom_comp_elem3", 
      "val_comp_elem3", "nom_comp_elem4", "val_comp_elem4",
      "nom_comp_elem5", "val_comp_elem5", "indicador_endereco",
      "num_quadra", "num_face", "cep_face", "cod_unico_endereco"
    )
    
    cols_to_drop <- cols_to_drop[cols_to_drop %in% names(cnefe_chunk)]
    if (length(cols_to_drop) > 0) {
      cnefe_chunk[, (cols_to_drop) := NULL]
    }
    
    # Pad administrative codes
    cnefe_chunk[, cod_municipio := str_pad(cod_municipio, width = 5, side = "left", pad = "0")]
    cnefe_chunk[, cod_distrito := str_pad(cod_distrito, width = 2, side = "left", pad = "0")]
    cnefe_chunk[, cod_subdistrito := str_pad(cod_subdistrito, width = 2, side = "left", pad = "0")]
    cnefe_chunk[, cod_setor := str_pad(cod_setor, width = 4, side = "left", pad = "0")]
    cnefe_chunk[, setor_code := paste0(cod_uf, cod_municipio, cod_distrito, cod_subdistrito, cod_setor)]
    cnefe_chunk[, c("cod_distrito", "cod_subdistrito", "cod_setor") := NULL]
    
    # Create address variables efficiently
    cnefe_chunk[, num_endereco_char := fifelse(num_endereco == 0, dsc_modificador, as.character(num_endereco))]
    cnefe_chunk[, dsc_modificador_nosn := fifelse(dsc_modificador != "SN", dsc_modificador, "")]
    
    # Create address and street in one operation
    cnefe_chunk[, `:=`(
      address = str_squish(paste(nom_tipo_seglogr, nom_titulo_seglogr, nom_seglogr, 
                                 num_endereco_char, dsc_modificador_nosn)),
      street = str_squish(paste(nom_tipo_seglogr, nom_titulo_seglogr, nom_seglogr))
    )]
    
    # Remove intermediate columns
    cnefe_chunk[, c("nom_tipo_seglogr", "nom_titulo_seglogr", "num_endereco_char",
                    "num_endereco", "nom_seglogr", "dsc_modificador_nosn", 
                    "dsc_modificador") := NULL]
    
    # Handle missing values
    cnefe_chunk[val_longitude == "", val_longitude := NA]
    cnefe_chunk[val_latitude == "", val_latitude := NA]
    cnefe_chunk[dsc_estabelecimento == "", dsc_estabelecimento := NA]
    cnefe_chunk[, dsc_estabelecimento := str_squish(dsc_estabelecimento)]
    
    # Add municipality code
    cnefe_chunk[, id_munic_7 := as.numeric(paste0(cod_uf, cod_municipio))]
    
    # Keep only essential columns early
    essential_cols <- c("id_munic_7", "setor_code", "especie", "street", "address",
                        "dsc_localidade", "dsc_estabelecimento", "val_longitude", "val_latitude")
    cnefe_chunk <- cnefe_chunk[, ..essential_cols]
    
    gc(verbose = FALSE)
    return(cnefe_chunk)
  }
  
  # Read and process CNEFE data
  if (is.character(cnefe_file)) {
    cnefe <- fread(cnefe_file, sep = ";", encoding = "UTF-8")
    cnefe <- process_chunk(cnefe)
  } else {
    cnefe <- process_chunk(copy(cnefe_file))
  }
  
  # Continue with rest of processing
  message("Merging municipality identifiers...")
  cnefe <- muni_ids[, .(id_munic_7, id_TSE, municipio, estado_abrev)][
    cnefe, on = "id_munic_7"
  ]
  
  gc(verbose = FALSE)
  
  # Merge especie labels
  message("Adding especie labels...")
  especie_labs <- cnefe_especie_labels(2010)
  cnefe <- especie_labs[cnefe, on = "especie"]
  
  # Create final dataset with renamed columns
  message("Creating final dataset...")
  addr <- cnefe[, .(
    id_munic_7, id_TSE, municipio, setor_code, especie_lab,
    street, address,
    bairro = dsc_localidade,
    desc = dsc_estabelecimento,
    val_longitude = val_longitude,  # Keep as character for convert_coord
    val_latitude = val_latitude      # Keep as character for convert_coord
  )]
  
  # Convert coordinates
  message("Converting coordinates...")
  # Initialize numeric columns
  addr[, `:=`(cnefe_long = NA_real_, cnefe_lat = NA_real_)]
  
  # Convert coordinates for non-empty values, accounting for parse failures
  # (stops if every value fails; reports the NA rate otherwise).
  addr[val_longitude != "" & val_latitude != "",
       `:=`(cnefe_long = convert_coords_checked(val_longitude, "CNEFE longitude"),
            cnefe_lat = convert_coords_checked(val_latitude, "CNEFE latitude"))]
  
  # Remove the character columns
  addr[, c("val_longitude", "val_latitude") := NULL]
  
  # Merge tract centroids
  message("Merging tract centroids...")
  addr <- tract_centroids[addr, on = .(setor_code)]
  
  # Replace NA coordinates with tract centroids
  addr[is.na(cnefe_long), cnefe_long := tract_centroid_long]
  addr[is.na(cnefe_lat), cnefe_lat := tract_centroid_lat]
  
  # Remove tract centroid columns
  addr[, c("tract_centroid_long", "tract_centroid_lat") := NULL]
  
  # Normalize addresses
  message("Normalizing addresses...")
  addr[, norm_address := normalize_address(address)]
  addr[, norm_bairro := normalize_address(bairro)]
  addr[, norm_street := normalize_address(street)]
  
  gc(verbose = FALSE)
  
  # Extract schools if requested
  if (extract_schools) {
    message("Extracting schools...")
    schools <- addr[especie_lab == "estabelecimento de ensino"]
    
    if (nrow(schools) > 0) {
      # Normalize school descriptions
      schools[, norm_desc := normalize_school(desc)]
      message(sprintf("Extracted %s schools", format(nrow(schools), big.mark = ",")))
    } else {
      message("No schools found in this dataset")
    }
    
    gc(verbose = FALSE)
    message("CNEFE processing complete")
    
    # Return both datasets as a list
    return(list(
      data = addr,
      schools = schools
    ))
  }
  
  # Monitor memory after processing
  mem_after <- gc()[2, 2]
  message(sprintf("Memory after CNEFE processing: %.1f GB", mem_after / 1024))
  message("CNEFE processing complete")
  
  return(addr)
}


clean_text_for_geocodebr <- function(text) {
  # Clean text for geocodebr matching
  # Remove special characters and normalize
  
  text <- tolower(text)
  text <- stringi::stri_trans_general(text, "Latin-ASCII")
  text <- gsub("[^a-z0-9 ]", " ", text)
  text <- gsub("\\s+", " ", text)
  text <- trimws(text)
  
  return(text)
}

simplify_address_for_geocodebr <- function(address) {
  # Simplify address for better geocodebr matching
  # GeocodeR API works best with street names only - numbers, prefixes, and 
  # suffixes often cause failed matches or incorrect results
  
  # Remove common prefixes
  address <- gsub("^(rua|avenida|av|r|travessa|tv|praca|pc|alameda|al)\\s+", "", address)
  
  # Remove numbers and common suffixes
  address <- gsub("\\b\\d+\\b", "", address)
  address <- gsub("\\b(sn|s n|sem numero)\\b", "", address)
  address <- gsub("\\b(lote|lt|quadra|qd|bloco|bl|casa|cs|apartamento|apto|ap)\\s*\\w*", "", address)
  
  # Clean up
  address <- clean_text_for_geocodebr(address)
  
  return(address)
}