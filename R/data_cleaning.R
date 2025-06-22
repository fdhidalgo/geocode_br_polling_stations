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
  
  # Check if muni_ids has data for this state
  if (nrow(muni_ids) > 0) {
    cnefe22 <- merge(
      cnefe22,
      muni_ids[, .(id_munic_7, id_TSE, municipio, estado_abrev)],
      by.x = "id_munic_7",
      by.y = "id_munic_7",
      all.x = TRUE
    )
  } else {
    # If no muni_ids for this state, add empty columns
    cnefe22[, `:=`(id_TSE = NA_integer_, municipio = NA_character_, estado_abrev = NA_character_)]
  }
  # Add human-readable labels for CNEFE establishment types (especie codes)
  especie_labs <- data.table(
    cod_especie = 1:8,
    especie_lab = c(
      "domicílio particular",
      "domicílio coletivo",
      "estabeleciemento agropecuário",
      "estabelecimento de ensino",
      "estabelecimento de saúde",
      "estabeleciemento de outras finalidades",
      "edificação em construção ou reforma",
      "estabeleciemento religioso"
    )
  )

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
  # Read the data from the 2018, 2020, 2022, and 2024 election files
  # Wrap in tryCatch to handle encoding warnings better
  loc18 <- tryCatch({
    fread(tse_files[1], encoding = "Latin-1")
  }, warning = function(w) {
    message("Warning reading 2018 TSE file: ", conditionMessage(w))
    suppressWarnings(fread(tse_files[1], encoding = "Latin-1"))
  })
  
  loc20 <- tryCatch({
    fread(tse_files[2], encoding = "Latin-1")
  }, warning = function(w) {
    message("Warning reading 2020 TSE file: ", conditionMessage(w))
    suppressWarnings(fread(tse_files[2], encoding = "Latin-1"))
  })
  
  loc22 <- tryCatch({
    fread(tse_files[3], encoding = "Latin-1")
  }, warning = function(w) {
    message("Warning reading 2022 TSE file: ", conditionMessage(w))
    suppressWarnings(fread(tse_files[3], encoding = "Latin-1"))
  })
  
  # Check if 2024 file exists (may not be available in all environments)
  if (length(tse_files) >= 4 && file.exists(tse_files[4])) {
    loc24 <- tryCatch({
      fread(tse_files[4], encoding = "Latin-1")
    }, warning = function(w) {
      message("Warning reading 2024 TSE file: ", conditionMessage(w))
      suppressWarnings(fread(tse_files[4], encoding = "Latin-1"))
    })
    # Combine the data from all four years into a single data frame
    locs <- rbindlist(list(loc18, loc20, loc22, loc24), fill = TRUE)
  } else {
    # Combine the data from three years into a single data frame
    locs <- rbindlist(list(loc18, loc20, loc22), fill = TRUE)
  }

  # Only keep columns that are present in all data set
  locs <- locs[,
    names(loc22)[names(loc22) %in% names(loc18) == TRUE],
    with = FALSE
  ]

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

import_locais <- function(locais_file, muni_ids) {
  # Try to detect encoding by checking if it's a TSE file
  is_tse_file <- grepl("eleitorado_local_votacao", locais_file)
  
  if (is_tse_file) {
    # TSE files are Latin-1 encoded
    locais_data <- fread(locais_file, encoding = "Latin-1")
    
    # Convert all character columns to UTF-8
    char_cols <- names(locais_data)[sapply(locais_data, is.character)]
    for (col in char_cols) {
      locais_data[, (col) := iconv(get(col), from = "latin1", to = "UTF-8", sub = "")]
    }
  } else {
    # Other files should be UTF-8
    locais_data <- fread(locais_file, encoding = "UTF-8")
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

  # Filter and add local_id
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

normalize_school <- function(x) {
  ## Common school-related terms to remove for better name matching
  school_syns <- c(
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
    "e f",
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
    "prof",
    "ensino fundamental"
  )

  ## Normalize school names by removing generic terms and standardizing format
  result <- stringi::stri_trans_general(x, "Latin-ASCII")
  result <- str_to_lower(result)
  result <- str_remove_all(result, "\\.")
  result <- str_remove_all(result, "[[:punct:]]")
  result <- str_squish(result)
  result <- str_remove_all(
    result,
    paste0("\\b", school_syns, "\\b", collapse = "|")
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


read_cnefe_chunked <- function(file_path, chunk_size = 5e6, process_fn = NULL) {
  # Read large CNEFE files in chunks to avoid memory exhaustion
  # CNEFE 2022 files can exceed 40GB and contain >100M rows
  # Processing in 5M row chunks keeps memory usage manageable
  message(sprintf("Reading %s in chunks of %s rows", 
                  basename(file_path), 
                  format(chunk_size, big.mark = ",")))
  
  # First, get total number of rows
  total_rows <- fread(file_path, select = 1L, sep = ";")[, .N]
  n_chunks <- ceiling(total_rows / chunk_size)
  
  message(sprintf("Total rows: %s, will process in %d chunks", 
                  format(total_rows, big.mark = ","), 
                  n_chunks))
  
  # Process each chunk
  results <- list()
  
  for (i in seq_len(n_chunks)) {
    skip_rows <- (i - 1) * chunk_size
    
    message(sprintf("Processing chunk %d/%d (rows %s-%s)...", 
                    i, n_chunks, 
                    format(skip_rows + 1, big.mark = ","),
                    format(min(skip_rows + chunk_size, total_rows), big.mark = ",")))
    
    # Read chunk
    chunk <- fread(
      file_path,
      sep = ";",
      nrows = chunk_size,
      skip = skip_rows,
      header = (i == 1),  # Only read header for first chunk
      col.names = if (i > 1) names(results[[1]]) else NULL,
      encoding = "UTF-8",
      showProgress = FALSE
    )
    
    # Apply processing function if provided
    if (!is.null(process_fn)) {
      chunk <- process_fn(chunk)
    }
    
    results[[i]] <- chunk
    
    # Force garbage collection after each chunk
    rm(chunk)
    gc(verbose = FALSE)
    
    # Check memory usage
    mem_usage <- gc()[2, 2]  # Current memory usage in MB
    message(sprintf("  Memory usage: %.1f GB", mem_usage / 1024))
    
    # If memory usage is high, combine results so far and clear list
    if (mem_usage > 50000) {  # If using more than 50GB
      message("  High memory usage detected, combining chunks...")
      if (length(results) > 1) {
        combined <- rbindlist(results, use.names = TRUE, fill = TRUE)
        results <- list(combined)
        gc(verbose = FALSE)
      }
    }
  }
  
  # Combine all results
  message("Combining all chunks...")
  final_result <- rbindlist(results, use.names = TRUE, fill = TRUE)
  
  message(sprintf("Finished reading %s rows", 
                  format(nrow(final_result), big.mark = ",")))
  
  return(final_result)
}


monitor_memory <- function() {
  gc_info <- gc()
  
  # Get system memory info on Linux
  if (Sys.info()["sysname"] == "Linux") {
    mem_info <- system("free -m", intern = TRUE)
    total_mem <- as.numeric(gsub(".*Mem:\\s+(\\d+).*", "\\1", mem_info[2]))
    used_mem <- as.numeric(gsub(".*Mem:\\s+\\d+\\s+(\\d+).*", "\\1", mem_info[2]))
    free_mem <- total_mem - used_mem
    
    list(
      r_memory_mb = sum(gc_info[, "used"]),
      system_total_gb = total_mem / 1024,
      system_used_gb = used_mem / 1024,
      system_free_gb = free_mem / 1024,
      system_used_pct = used_mem / total_mem * 100
    )
  } else {
    list(
      r_memory_mb = sum(gc_info[, "used"]),
      system_info = "Not available on this platform"
    )
  }
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
    cnefe_chunk[, cod_setor := str_pad(cod_setor, width = 6, side = "left", pad = "0")]
    cnefe_chunk[, setor_code := paste0(cod_uf, cod_municipio, cod_distrito, cod_setor)]
    cnefe_chunk[, c("cod_distrito", "cod_setor") := NULL]
    
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
    if (file.size(cnefe_file) > 1e9) {  # Use chunks for files > 1GB
      cnefe <- read_cnefe_chunked(cnefe_file, chunk_size = 5e6, process_fn = process_chunk)
    } else {
      cnefe <- fread(cnefe_file, sep = ";", encoding = "UTF-8")
      cnefe <- process_chunk(cnefe)
    }
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
  especie_labs <- data.table(
    especie = 1:7,
    especie_lab = c(
      "domicílio particular", "domicílio coletivo",
      "estabeleciemento agropecuário", "estabelecimento de ensino",
      "estabelecimento de saúde", "estabeleciemento de outras finalidades",
      "edificação em construção"
    )
  )
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
  
  # Convert coordinates for non-empty values
  # Use sapply to vectorize convert_coord function
  addr[val_longitude != "" & val_latitude != "", 
       `:=`(cnefe_long = sapply(val_longitude, convert_coord), 
            cnefe_lat = sapply(val_latitude, convert_coord))]
  
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