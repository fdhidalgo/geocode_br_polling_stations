# Memory-efficient CNEFE processing functions

library(data.table)
library(stringr)

# Source required functions
source("R/data_cleaning_fns.R")  # For normalize_address and convert_coord

#' Read CNEFE data in chunks with memory management
#'
#' This function reads CNEFE data in smaller chunks to avoid memory allocation errors
#' 
#' @param file_path Path to CNEFE file
#' @param chunk_size Number of rows to read per chunk (default: 5 million)
#' @param process_fn Function to apply to each chunk
#' @return Combined processed data
read_cnefe_chunked <- function(file_path, chunk_size = 5e6, process_fn = NULL) {
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

#' Memory-efficient version of clean_cnefe10
#'
#' Processes CNEFE 2010 data with better memory management
#'
#' @param cnefe_file Path to CNEFE file or data.table
#' @param muni_ids Municipality identifiers
#' @param tract_centroids Census tract centroids
#' @param use_chunks Whether to use chunked reading for large files
#' @param extract_schools Whether to extract schools as a separate dataset
#' @return Cleaned CNEFE data (list if extract_schools=TRUE, data.table otherwise)
clean_cnefe10_efficient <- function(cnefe_file, muni_ids, tract_centroids, use_chunks = TRUE, extract_schools = FALSE) {
  
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
    if (use_chunks && file.size(cnefe_file) > 1e9) {  # Use chunks for files > 1GB
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
    cnefe_long = val_longitude,
    cnefe_lat = val_latitude
  )]
  
  rm(cnefe)
  gc(verbose = FALSE)
  
  # Convert coordinates
  message("Converting coordinates...")
  addr[!is.na(cnefe_long), cnefe_long := sapply(cnefe_long, convert_coord)]
  addr[, cnefe_long := as.numeric(cnefe_long)]
  addr[!is.na(cnefe_lat), cnefe_lat := sapply(cnefe_lat, convert_coord)]
  addr[, cnefe_lat := as.numeric(cnefe_lat)]
  
  # Merge tract centroids
  message("Merging tract centroids...")
  addr <- tract_centroids[addr, on = "setor_code"]
  
  # Impute missing coordinates
  addr$cnefe_impute_tract_centroid <- as.numeric(
    is.na(addr$cnefe_lat) & !is.na(addr$tract_centroid_lat)
  )
  
  # Filter out rows with no coordinates
  addr <- addr[!(is.na(cnefe_lat) & is.na(tract_centroid_lat))]
  
  # Impute coordinates
  addr[cnefe_impute_tract_centroid == 1, `:=`(
    cnefe_long = tract_centroid_long,
    cnefe_lat = tract_centroid_lat
  )]
  
  # Normalize addresses
  message("Normalizing addresses...")
  addr[, `:=`(
    norm_address = normalize_address(address),
    norm_bairro = normalize_address(bairro),
    norm_street = normalize_address(street)
  )]
  
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
  
  gc(verbose = FALSE)
  message("CNEFE processing complete")
  
  return(addr)
}

#' Monitor memory usage during pipeline execution
#'
#' @return List with memory statistics
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

#' Wrapper to use memory-efficient version in targets pipeline
#'
#' This ensures backward compatibility while using the efficient version
#'
#' @param cnefe_file CNEFE file or data
#' @param muni_ids Municipality identifiers  
#' @param tract_centroids Tract centroids
#' @param extract_schools Whether to extract schools as a separate attribute
#' @return Cleaned CNEFE data (list with 'data' and optionally 'schools' if extract_schools=TRUE)
clean_cnefe10 <- function(cnefe_file, muni_ids, tract_centroids, extract_schools = FALSE) {
  # Monitor memory before processing
  mem_before <- monitor_memory()
  message(sprintf("Memory before CNEFE processing: R=%.1f GB, System=%.1f%% used",
                  mem_before$r_memory_mb / 1024,
                  mem_before$system_used_pct))
  
  # Use memory-efficient version
  result <- clean_cnefe10_efficient(
    cnefe_file = cnefe_file,
    muni_ids = muni_ids,
    tract_centroids = tract_centroids,
    use_chunks = TRUE,
    extract_schools = extract_schools
  )
  
  # Monitor memory after processing
  mem_after <- monitor_memory()
  message(sprintf("Memory after CNEFE processing: R=%.1f GB, System=%.1f%% used",
                  mem_after$r_memory_mb / 1024,
                  mem_after$system_used_pct))
  
  return(result)
}