## Import and cleaning functions for TSE polling stations, CNEFE, INEP, and municipal data.

library(data.table)
library(stringr)

# Renames source-specific column variants (TSE, CNEFE, panel) to the pipeline's names,
# in place.
standardize_column_names <- function(dt) {
  old_names <- names(dt)

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

  new_names <- old_names
  for (old in names(replacements)) {
    idx <- which(new_names == old)
    if (length(idx) > 0) {
      new_names[idx] <- replacements[old]
    }
  }

  data.table::setnames(dt, old_names, new_names)
  invisible(dt)
}

# Labels for the CNEFE "espécie de endereço" codes. 2010 uses 7 codes keyed on `especie`;
# 2022 uses 8 keyed on `cod_especie`, widening code 7 and adding a religious-establishment code.
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

# Cleans CNEFE 2022 into an address table with municipality ids and normalized street/bairro.
clean_cnefe22 <- function(cnefe22_file, muni_ids) {
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

  setnames(cnefe22, names(cnefe22), tolower(names(cnefe22)))

  # Street address used by matching; the house-number/modifier fields feed nothing downstream.
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
      "num_endereco",
      "nom_seglogr",
      "dsc_modificador"
    ) := NULL
  ]

  cnefe22[dsc_estabelecimento == "", dsc_estabelecimento := NA]
  cnefe22[, dsc_estabelecimento := str_squish(dsc_estabelecimento)]

  setnames(cnefe22, "cod_municipio", "id_munic_7")

  # muni_ids is the municipality crosswalk; an empty table is a structural failure.
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
  especie_labs <- cnefe_especie_labels(2022)

  cnefe22 <- merge(
    cnefe22,
    especie_labs,
    by = "cod_especie",
    all.x = TRUE
  )

  addr <- cnefe22[, .(
    id_munic_7,
    id_TSE,
    municipio,
    especie_lab,
    street,
    bairro = dsc_localidade,
    desc = dsc_estabelecimento,
    cnefe_long = longitude,
    cnefe_lat = latitude
  )]

  addr[, norm_bairro := normalize_address(bairro)]
  addr[, norm_street := normalize_address(street)]

  addr
}

# Reads one TSE geocoded file (Latin-1) to a lowercased data.table, dropping out-of-country
# (sg_uf == "ZZ") rows. `cols` optionally restricts the columns read.
read_tse_locais_file <- function(f, cols = NULL) {
  d <- fread(f, encoding = "Latin-1", select = cols)
  setnames(d, tolower(names(d)))
  d[sg_uf != "ZZ"]
}

# Combines the TSE geocoded ground-truth files into one row per local_id, keeping the latest year.
clean_tsegeocoded_locais <- function(tse_files, muni_ids, locais) {
  # The TSE geocoded ground-truth files are 2018, 2020, 2022, and 2024.
  expected_tse_files <- 4L
  if (length(tse_files) != expected_tse_files) {
    stop(sprintf(
      "Expected %d TSE geocoded files, got %d: %s",
      expected_tse_files,
      length(tse_files),
      paste(tse_files, collapse = ", ")
    ))
  }

  loc_list <- lapply(tse_files, read_tse_locais_file)

  # Keep only columns present in every year, so a schema change fails loud instead of being filled.
  common_cols <- Reduce(intersect, lapply(loc_list, names))
  if (length(common_cols) == 0) {
    stop("No columns are common to all TSE geocoded files.")
  }
  loc_list <- lapply(loc_list, function(x) x[, common_cols, with = FALSE])
  locs <- rbindlist(loc_list, use.names = TRUE)

  # Remove duplicate rows (out-of-country rows already dropped on read).
  locs <- unique(locs[,
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

  # TSE codes a missing coordinate as -1.
  locs[, nr_latitude := ifelse(nr_latitude == -1, NA, nr_latitude)]
  locs[, nr_longitude := ifelse(nr_longitude == -1, NA, nr_longitude)]

  locs <- locs[!is.na(nr_latitude)]

  locs <- merge(
    locs,
    muni_ids[, .(id_munic_7, id_TSE)],
    by.x = c("cd_municipio"),
    by.y = c("id_TSE"),
    all.x = TRUE
  )

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

  locs <- locs[!is.na(local_id)]

  # Group by 'local_id' and keep only the most recent year
  locs <- locs[locs[, .I[which.max(ano)], by = local_id]$V1]

  locs
}

# Builds the 7-digit IBGE municipality key. cod_municipio must be zero-padded to width 5
# first, or "12" + "401" collapses to 12401 instead of 1200401.
make_id_munic_7 <- function(cod_uf, cod_municipio) {
  cod_municipio <- str_pad(cod_municipio, width = 5, side = "left", pad = "0")
  id_munic_7 <- as.numeric(paste0(cod_uf, cod_municipio))
  if (any(is.na(id_munic_7) | id_munic_7 < 1e6 | id_munic_7 >= 1e7)) {
    stop(
      "make_id_munic_7(): produced invalid (NA or non-7-digit) municipality codes. ",
      "cod_uf/cod_municipio were missing or lost their leading zeros upstream (see #75). ",
      "Sample id_munic_7: ",
      paste(utils::head(sort(unique(id_munic_7))), collapse = ", ")
    )
  }
  id_munic_7
}

# Cleans the agricultural CNEFE files into an address table joined to muni_ids.
clean_agro_cnefe <- function(agro_cnefe_files, muni_ids) {
  # COD_UF/COD_MUNICIPIO are zero-padded strings in the source; read them as character so
  # fread does not strip the leading zeros the 7-digit IBGE code depends on.
  agro_list <- lapply(agro_cnefe_files, function(file) {
    fread(
      file,
      encoding = "UTF-8",
      sep = ";",
      colClasses = list(character = c("COD_UF", "COD_MUNICIPIO"))
    )
  })
  agro_cnefe <- rbindlist(agro_list, fill = TRUE)

  # Agro files ship uppercase column names.
  setnames(agro_cnefe, names(agro_cnefe), tolower(names(agro_cnefe)))

  # street and id_munic_7 are built before standardize_column_names() renames their sources.
  agro_cnefe[,
    street := str_squish(paste(
      nom_tipo_seglogr,
      nom_titulo_seglogr,
      nom_seglogr
    ))
  ]

  agro_cnefe[, id_munic_7 := make_id_munic_7(cod_uf, cod_municipio)]

  # Zero overlap means a schema/vintage mismatch; every downstream lookup would match nothing.
  if (!any(agro_cnefe$id_munic_7 %in% muni_ids$id_munic_7)) {
    stop(
      "clean_agro_cnefe(): id_munic_7 has zero overlap with muni_ids$id_munic_7. ",
      "The agro COD_UF/COD_MUNICIPIO codes do not match any known municipality. ",
      "Sample agro id_munic_7: ",
      paste(utils::head(sort(unique(agro_cnefe$id_munic_7))), collapse = ", ")
    )
  }

  standardize_column_names(agro_cnefe)

  agro_cnefe[, norm_street := normalize_address(street)]
  agro_cnefe[, norm_bairro := normalize_address(nome_localidade)]

  agro_cnefe[, latitude := as.numeric(latitude)]
  agro_cnefe[, longitude := as.numeric(longitude)]

  agro_cnefe <- muni_ids[
    agro_cnefe,
    on = .(id_munic_7),
    nomatch = NA
  ]

  return(agro_cnefe)
}

repair_mixed_utf8 <- function(x) {
  # The export mixes UTF-8 and Latin-1 across source years, so repair per string: only bytes
  # failing UTF-8 validation are reinterpreted as Latin-1. which() drops NAs, which a logical
  # index could not (NAs are illegal in subassignment).
  bad <- which(!stringi::stri_enc_isutf8(x))
  x[bad] <- iconv(x[bad], from = "latin1", to = "UTF-8")
  enc2utf8(x)
}

# Imports the polling-station file: encoding repair, normalized columns, deterministic local_id.
import_locais <- function(locais_file, muni_ids) {
  # Read raw bytes and repair per string; neither a blanket UTF-8 nor Latin-1 read is correct.
  locais_data <- fread(locais_file, encoding = "unknown")

  char_cols <- names(locais_data)[vapply(locais_data, is.character, logical(1))]
  for (col in char_cols) {
    set(locais_data, j = col, value = repair_mixed_utf8(locais_data[[col]]))
  }

  setnames(locais_data, janitor::make_clean_names(names(locais_data)))

  # The source writes "no address recorded" as an empty string. Keep it NA, so downstream
  # code cannot confuse it with an address that normalized away to nothing.
  locais_data[ds_endereco == "", ds_endereco := NA_character_]
  locais_data[ds_bairro == "", ds_bairro := NA_character_]

  locais_data[, normalized_name := normalize_school(nm_locvot)]
  locais_data[, normalized_st := normalize_address(ds_endereco)]
  locais_data[, normalized_bairro := normalize_address(ds_bairro)]
  # The combined address keeps whichever half is recorded, and is NA only if neither is.
  locais_data[,
    normalized_addr := str_squish(paste(
      fcoalesce(normalized_st, ""),
      fcoalesce(normalized_bairro, "")
    ))
  ]
  locais_data[
    is.na(normalized_st) & is.na(normalized_bairro),
    normalized_addr := NA_character_
  ]

  locais_data <- merge(
    locais_data,
    muni_ids[, .(cod_localidade_ibge = id_munic_7, cd_localidade_tse = id_TSE)],
    by = "cd_localidade_tse",
    all.x = TRUE
  )

  # Remove polling stations abroad
  locais_data <- locais_data[sg_uf != "ZZ"]

  # local_id is assigned after ordering by the natural station-year key, so it does not depend
  # on input row order. Values are reproducible within a release, not across releases.
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

# Attaches the best model prediction and TSE ground truth, then picks each station's final coords.
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

  geocoded_locais <- merge(
    geocoded_locais,
    tsegeocoded_locais[, .(local_id, tse_lat, tse_long)],
    by = "local_id",
    all.x = TRUE
  )

  geocoded_locais[, final_long := ifelse(is.na(tse_long), pred_long, tse_long)]
  geocoded_locais[, final_lat := ifelse(is.na(tse_lat), pred_lat, tse_lat)]

  # pred_dist is the predicted error of the chosen coordinate; TSE coordinates are exact.
  geocoded_locais[!is.na(tse_long) & !is.na(tse_lat), pred_dist := 0]

  return(geocoded_locais)
}

# Centroid coordinates for each census tract.
make_tract_centroids <- function(tracts) {
  tracts$centroid <- sf::st_transform(tracts, 4674) |>
    sf::st_centroid() |>
    sf::st_geometry()

  tracts$tract_centroid_long <- sf::st_coordinates(tracts$centroid)[, 1]
  tracts$tract_centroid_lat <- sf::st_coordinates(tracts$centroid)[, 2]

  tracts <- sf::st_drop_geometry(tracts)
  tracts <- data.table(tracts)[, .(
    setor_code = code_tract,
    zone,
    tract_centroid_lat,
    tract_centroid_long
  )]

  return(tracts)
}

# Normalizes an address string for cross-dataset matching.
normalize_address <- function(x) {
  # normalize_name() squishes whitespace first, so the single-space patterns below
  # still match sources that write "zona  rural" or "s / n".
  result <- normalize_name(x)
  # Generic location descriptors are used inconsistently across datasets, so drop them.
  result <- str_remove(result, "\\bzona rural\\b")
  result <- str_remove(result, "\\bpovoado\\b")
  result <- str_remove(result, "\\blocalidade\\b")
  result <- str_replace_all(result, "^av\\b", "avenida")
  result <- str_replace_all(result, "^r\\b", "rua")
  result <- str_replace_all(result, "\\bs n\\b", "sn")
  # Squish again: the descriptor removals leave a doubled space behind.
  str_squish(result)
}

## Generic school terms stripped by normalize_school() and used as a school-detection feature
## by make_model_data() in R/model.R.
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

# Lowercase, ASCII, punctuation-free form of a name.
normalize_name <- function(x) {
  result <- stringi::stri_trans_general(x, "Latin-ASCII")
  result <- str_to_lower(result)
  result <- str_remove_all(result, "[[:punct:]]")
  str_squish(result)
}

# normalize_name() plus removal of the generic school terms, so two records for the same
# school match on what distinguishes it rather than on "escola municipal".
SCHOOL_SYNONYM_PATTERN <- paste0("\\b", school_synonyms, "\\b", collapse = "|")

normalize_school <- function(x) {
  str_squish(str_remove_all(normalize_name(x), SCHOOL_SYNONYM_PATTERN))
}

# Cleans the INEP school census and normalizes its school names and addresses.
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

  inep_data <- inep_codes[inep_data, on = "codigo_inep"]

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

# School rows of a cleaned CNEFE table (either year). An empty norm_desc has no name
# to match on, so those rows are dropped.
get_cnefe_schools <- function(cnefe) {
  schools <- cnefe[especie_lab == "estabelecimento de ensino"]
  schools[, norm_desc := normalize_school(desc)]
  schools[norm_desc != ""]
}

convert_coords_dms <- function(coord_strings) {
  # Vectorized "degrees minutes seconds direction" -> decimal degrees, for CNEFE 2010 only.
  # Malformed values (fewer than 4 tokens, non-numeric D/M/S) become NA; S, W, or O negates.
  n <- length(coord_strings)
  tokens <- data.table::tstrsplit(coord_strings, " ", fixed = TRUE, fill = NA)

  # With fewer than 4 token columns every row has under 4 tokens, so every value is NA.
  if (length(tokens) < 4) {
    return(rep(NA_real_, n))
  }

  degrees <- suppressWarnings(as.numeric(tokens[[1]]))
  minutes <- suppressWarnings(as.numeric(tokens[[2]]))
  seconds <- suppressWarnings(as.numeric(tokens[[3]]))
  decimal <- degrees + (minutes / 60) + (seconds / 3600)

  # Negate S/W/O. The direction token is low-cardinality, so map distinct values back rather
  # than running the regex over ~80M rows.
  direction <- tokens[[4]]
  levels <- unique(direction)
  negate_level <- gsub("[^NSWO]", "", levels) %in% c("S", "W", "O")
  negate <- negate_level[match(direction, levels)]
  decimal[negate] <- -decimal[negate]

  # A row with fewer than 4 tokens has an NA direction token; force those to NA.
  decimal[is.na(direction)] <- NA_real_
  decimal
}

convert_coords_checked <- function(coord_strings, coord_name = "coordinate") {
  # DMS -> decimal degrees, stopping if EVERY value failed to parse (a systematic format
  # change) and reporting the NA rate otherwise.
  converted <- convert_coords_dms(coord_strings)

  n <- length(converted)
  if (n > 0) {
    n_na <- sum(is.na(converted))
    if (n_na == n) {
      stop(sprintf(
        "All %d %s values failed to parse to decimal degrees.",
        n,
        coord_name
      ))
    }
    if (n_na > 0) {
      message(sprintf(
        "%s: %d/%d (%.1f%%) values failed to parse and are NA.",
        coord_name,
        n_na,
        n,
        100 * n_na / n
      ))
    }
  }

  converted
}


# Cleans one CNEFE 2010 state file into an address table with municipality ids and
# normalized street/bairro. The 2010 state files are comma-separated.
clean_cnefe10 <- function(cnefe10_file, muni_ids, tract_centroids) {
  mem_before <- gc()[2, 2]
  message(sprintf("Memory before CNEFE processing: %.1f GB", mem_before / 1024))

  cnefe <- fread(cnefe10_file, sep = ",", encoding = "UTF-8", showProgress = FALSE)
  setnames(cnefe, names(cnefe), tolower(names(cnefe)))

  # Drop unnecessary columns early to save memory
  cols_to_drop <- c(
    "situacao_setor",
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
    "indicador_endereco",
    "num_quadra",
    "num_face",
    "cep_face",
    "cod_unico_endereco"
  )

  cnefe[, (cols_to_drop) := NULL]

  cnefe[, cod_municipio := str_pad(cod_municipio, width = 5, side = "left", pad = "0")]
  cnefe[, cod_distrito := str_pad(cod_distrito, width = 2, side = "left", pad = "0")]
  cnefe[, cod_subdistrito := str_pad(cod_subdistrito, width = 2, side = "left", pad = "0")]
  cnefe[, cod_setor := str_pad(cod_setor, width = 4, side = "left", pad = "0")]
  cnefe[, setor_code := paste0(cod_uf, cod_municipio, cod_distrito, cod_subdistrito, cod_setor)]
  cnefe[, c("cod_distrito", "cod_subdistrito", "cod_setor") := NULL]

  # Street address used by matching; the house-number/modifier fields feed nothing downstream.
  cnefe[, street := str_squish(paste(nom_tipo_seglogr, nom_titulo_seglogr, nom_seglogr))]

  cnefe[, c("nom_tipo_seglogr", "nom_titulo_seglogr", "num_endereco", "nom_seglogr", "dsc_modificador") := NULL]

  cnefe[val_longitude == "", val_longitude := NA]
  cnefe[val_latitude == "", val_latitude := NA]
  cnefe[dsc_estabelecimento == "", dsc_estabelecimento := NA]
  cnefe[, dsc_estabelecimento := str_squish(dsc_estabelecimento)]

  # cod_municipio was padded above for setor_code; make_id_munic_7 re-pads idempotently.
  cnefe[, id_munic_7 := make_id_munic_7(cod_uf, cod_municipio)]

  essential_cols <- c(
    "id_munic_7",
    "setor_code",
    "especie",
    "street",
    "dsc_localidade",
    "dsc_estabelecimento",
    "val_longitude",
    "val_latitude"
  )
  cnefe <- cnefe[, ..essential_cols]

  gc(verbose = FALSE)

  message("Merging municipality identifiers...")
  cnefe <- muni_ids[, .(id_munic_7, id_TSE, municipio, estado_abrev)][
    cnefe,
    on = "id_munic_7"
  ]

  gc(verbose = FALSE)

  message("Adding especie labels...")
  especie_labs <- cnefe_especie_labels(2010)
  cnefe <- especie_labs[cnefe, on = "especie"]

  message("Creating final dataset...")
  addr <- cnefe[, .(
    id_munic_7,
    id_TSE,
    municipio,
    setor_code,
    especie_lab,
    street,
    bairro = dsc_localidade,
    desc = dsc_estabelecimento,
    val_longitude = val_longitude, # Keep as character for convert_coords_dms
    val_latitude = val_latitude # Keep as character for convert_coords_dms
  )]

  message("Converting coordinates...")
  addr[, `:=`(cnefe_long = NA_real_, cnefe_lat = NA_real_)]

  addr[
    val_longitude != "" & val_latitude != "",
    `:=`(
      cnefe_long = convert_coords_checked(val_longitude, "CNEFE longitude"),
      cnefe_lat = convert_coords_checked(val_latitude, "CNEFE latitude")
    )
  ]

  addr[, c("val_longitude", "val_latitude") := NULL]

  message("Merging tract centroids...")
  addr <- tract_centroids[addr, on = .(setor_code)]

  addr[is.na(cnefe_long), cnefe_long := tract_centroid_long]
  addr[is.na(cnefe_lat), cnefe_lat := tract_centroid_lat]

  addr[, c("tract_centroid_long", "tract_centroid_lat") := NULL]

  message("Normalizing addresses...")
  addr[, norm_bairro := normalize_address(bairro)]
  addr[, norm_street := normalize_address(street)]

  mem_after <- gc()[2, 2]
  message(sprintf("Memory after CNEFE processing: %.1f GB", mem_after / 1024))

  addr
}


clean_text_for_geocodebr <- function(text) {
  # Lowercase, transliterate to ASCII, and reduce to single-spaced alphanumerics.
  text <- tolower(text)
  text <- stringi::stri_trans_general(text, "Latin-ASCII")
  text <- gsub("[^a-z0-9 ]", " ", text)
  text <- gsub("\\s+", " ", text)
  text <- trimws(text)

  # A field with nothing left to match on is missing, not empty. geocodebr reads NA as
  # "no such field for this row" and falls back down its cascade; "" is a value it tries
  # and fails to match.
  text[!nzchar(text)] <- NA_character_

  return(text)
}

# Splits a TSE address line into the street name and house number geocodebr wants as
# separate fields. Returns both, so callers parse once.
split_street_number <- function(address) {
  x <- stringi::stri_trans_general(toupper(address), "Latin-ASCII")

  # Phone numbers and unit complements ride along in this field, and to a
  # trailing-number rule both look exactly like a house number.
  x <- gsub("\\b(FONE|TELEFONE|TEL)\\b.*$", " ", x)
  # The apartment and block abbreviations are left out on purpose. "AP" never means
  # apartment in this data: across all 940k addresses its 35 occurrences are Amapa
  # highways ("RODOVIA AP 070"), streets named AP-3, and "AP" short for Aparecida inside
  # a person's name. "APARTAMENTO" occurs zero times and "APTO" once.
  x <- gsub("\\b(LOTE|LT|QUADRA|QD|BLOCO|CASA)\\b\\s*\\S*", " ", x)

  # "s/n" says there is no house number. Drop the marker so it cannot survive as street text.
  x <- gsub("\\bS/?\\s?N\\b|\\bSEM\\s+NUMERO\\b", " ", x)
  x <- trimws(gsub("\\s+", " ", x))
  # Removing a complement can leave the separator that preceded it ("RUA A, 123, CASA 2"),
  # which would hide the house number from a rule that reads the end of the string.
  x <- gsub("[^A-Z0-9]+$", "", x)

  marker <- "\\bN[O\u00ba\u00b0]?\\.?\\s*(\\d{1,6})\\b"
  trailing <- "[ ,]\\s*(\\d{1,6})\\s*$"

  numero <- rep(NA_integer_, length(x))
  street <- x

  # An explicit "N 123" marker names the house number wherever it sits; without one, a
  # house number is only ever in trailing position.
  marked <- grepl(marker, x)
  numero[marked] <- as.integer(sub(paste0("^.*?", marker, ".*$"), "\\1", x[marked]))
  street[marked] <- sub(marker, " ", x[marked])

  # A number after "km" is a highway milepost — the location itself on rural addresses,
  # not a house number.
  trailed <- !marked & grepl(trailing, x) & !grepl("\\bKM\\s*-?\\s*\\d{1,6}\\s*$", x)
  numero[trailed] <- as.integer(sub(paste0("^.*", trailing), "\\1", x[trailed]))
  street[trailed] <- sub(trailing, " ", x[trailed])

  # Pulling the number out of "RUA 15" leaves only words naming a kind of street, never an
  # individual one, which means the number was the name. Put it back.
  street_types <- paste0(
    "\\b(RUA|R|AVENIDA|AV|TRAVESSA|TV|PRACA|PC|ALAMEDA|AL|RODOVIA|ESTRADA|VIA|LARGO|",
    "VIELA|RAMAL|LINHA|BR|KM)\\b"
  )
  numbered <- which(!is.na(numero))
  named_only_by_number <- numbered[
    !grepl("[A-Z0-9]", gsub(street_types, " ", street[numbered]))
  ]
  street[named_only_by_number] <- x[named_only_by_number]
  numero[named_only_by_number] <- NA_integer_

  list(logradouro = clean_text_for_geocodebr(street), numero = numero)
}

# TSE stores the CEP as a number, so the leading zero of every Sao Paulo-range CEP
# (01000-000 upward) is gone; 0 is its missing-value sentinel, and a few rows carry
# truncated 5-digit CEPs from the pre-1992 scheme.
cep_to_string <- function(nr_cep) {
  valid <- !is.na(nr_cep) & nr_cep >= 1000000 & nr_cep <= 99999999
  data.table::fifelse(valid, sprintf("%08.0f", nr_cep), NA_character_)
}
