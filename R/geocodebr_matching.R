# geocodebr integration functions for polling station geocoding

#' Clean text for geocodebr compatibility
#' 
#' geocodebr is sensitive to encoding and special characters.
#' This function cleans text to maximize success rate.
#' 
#' @param text Character vector to clean
#' @return Cleaned character vector
clean_text_for_geocodebr <- function(text) {
  if (is.null(text) || length(text) == 0) return(text)
  
  tryCatch({
    # Use stringi to transliterate to ASCII
    cleaned <- stringi::stri_trans_general(text, "Latin-ASCII")
    
    # Remove remaining non-ASCII characters
    cleaned <- gsub("[^[:alnum:][:space:],.-]", " ", cleaned)
    
    # Clean up whitespace
    cleaned <- gsub("\\s+", " ", cleaned)
    cleaned <- trimws(cleaned)
    
    # Convert to uppercase
    toupper(cleaned)
  }, error = function(e) {
    # Fallback: basic cleaning without stringi
    warning(paste("Encoding error in clean_text_for_geocodebr:", e$message))
    text_clean <- iconv(text, from = "UTF-8", to = "ASCII//TRANSLIT", sub = " ")
    text_clean[is.na(text_clean)] <- text[is.na(text_clean)]
    toupper(gsub("[^[:alnum:][:space:],.-]", " ", text_clean))
  })
}

#' Simplify address for better geocodebr matching
#' 
#' Removes numbers and everything after commas/dashes to match
#' geocodebr's CNEFE data format better.
#' 
#' @param address Character vector of addresses
#' @return Simplified addresses
simplify_address_for_geocodebr <- function(address) {
  if (is.null(address) || length(address) == 0) return(address)
  
  # Wrap in tryCatch to handle encoding issues
  tryCatch({
    # Remove everything after comma or dash
    simplified <- stringr::str_extract(address, "^[^,-]+")
    
    # Remove common suffixes that might confuse matching
    simplified <- gsub("\\s+(ANEXO|SALA|ANDAR|BLOCO|LOTE|QUADRA|QD|LT|N|NO|NUM|NUMERO).*$", "", simplified, ignore.case = TRUE)
    
    # Apply standard cleaning
    clean_text_for_geocodebr(simplified)
  }, error = function(e) {
    # If encoding error, try basic cleaning
    warning(paste("Encoding error in simplify_address_for_geocodebr:", e$message))
    # Return uppercase version without special processing
    toupper(gsub("[^[:alnum:][:space:]]", " ", address))
  })
}

#' Match polling stations with geocodebr for a single municipality
#' 
#' @param locais_muni Data.table of polling stations for one municipality
#' @param muni_ids Municipality identifiers (not used but kept for consistency)
#' @return Data.table with geocodebr matches and precision levels
match_geocodebr_muni <- function(locais_muni, muni_ids = NULL) {
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

#' Create geocodebr precision features for ML model
#' 
#' @param geocodebr_match Data.table with geocodebr matches
#' @return Data.table with precision features
create_geocodebr_features <- function(geocodebr_match) {
  if (is.null(geocodebr_match) || nrow(geocodebr_match) == 0) {
    return(NULL)
  }
  
  # Create precision indicator features
  precision_features <- geocodebr_match[, .(
    local_id,
    # One-hot encoding
    geocodebr_precisao_municipio = as.integer(precisao_geocodebr == "municipio"),
    geocodebr_precisao_logradouro = as.integer(precisao_geocodebr == "logradouro"),
    geocodebr_precisao_numero = as.integer(precisao_geocodebr == "numero"),
    # Ordinal encoding (higher = more precise)
    geocodebr_precisao_score = fcase(
      precisao_geocodebr == "municipio", 1L,
      precisao_geocodebr == "logradouro", 2L,
      precisao_geocodebr == "numero", 3L,
      default = 0L
    ),
    # Additional features
    geocodebr_has_match = 1L,
    geocodebr_cnefe_count = contagem_cnefe_geocodebr
  )]
  
  return(precision_features)
}

#' Process geocodebr matches for model data
#' 
#' Convert geocodebr matches to long format for make_model_data()
#' 
#' @param geocodebr_match Data.table with geocodebr matches
#' @return Data.table in long format
process_geocodebr_for_model <- function(geocodebr_match) {
  if (is.null(geocodebr_match) || nrow(geocodebr_match) == 0) {
    return(NULL)
  }
  
  # Create long format data
  geocodebr_long <- geocodebr_match[!is.na(match_lat_geocodebr), .(
    local_id,
    type = "geocodebr",
    value.long = match_long_geocodebr,
    value.lat = match_lat_geocodebr,
    value.mindist = mindist_geocodebr,  # Always 0 for geocodebr
    # Keep precision info
    precisao = precisao_geocodebr,
    tipo_resultado = tipo_resultado_geocodebr
  )]
  
  return(geocodebr_long)
}