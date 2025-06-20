#' Panel ID Blocking Functions for Two-Level Optimization
#' 
#' Functions to implement two-level blocking (municipality + shared words) 
#' for efficient panel ID processing using reclin2
#'
#' @import data.table
#' @import stringr
#' @import reclin2

# Portuguese stopwords for polling station contexts
PORTUGUESE_STOPWORDS <- c(
  # General stopwords
  "DE", "DA", "DO", "DOS", "DAS", "E", "EM", "NA", "NO", "NAS", "NOS",
  "A", "O", "AS", "OS", "UM", "UMA", "UNS", "UMAS",
  
  # School-related terms
  "ESCOLA", "MUNICIPAL", "ESTADUAL", "FEDERAL", "PUBLICA", "PRIVADA",
  "COLEGIO", "INSTITUTO", "CENTRO", "UNIDADE", "EDUCACIONAL", "ENSINO",
  "FUNDAMENTAL", "MEDIO", "INFANTIL", "CRECHE", "CEI", "EMEF", "EMEI",
  "EE", "EM", "EC", "EP", "ESC", "COL", "INST",
  
  # Location terms
  "RUA", "AVENIDA", "PRACA", "ALAMEDA", "TRAVESSA", "ESTRADA", "RODOVIA",
  "AV", "R", "PC", "AL", "TR", "EST", "ROD", "VIA", "LARGO", "BECO",
  
  # Building/zone terms
  "PREDIO", "EDIFICIO", "BLOCO", "ANDAR", "SALA", "QUADRA", "LOTE",
  "ZONA", "BAIRRO", "DISTRITO", "REGIAO", "SETOR", "AREA",
  
  # Common abbreviations
  "S", "N", "SN", "C", "CONJ", "QD", "LT", "BL", "AP", "APTO",
  
  # Numbers and single letters (often not meaningful for matching)
  "1", "2", "3", "4", "5", "6", "7", "8", "9", "0",
  "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X"
)

#' Extract significant words from text
#' 
#' Tokenizes text and removes Portuguese stopwords to extract significant words
#' for blocking purposes
#' 
#' @param text Character vector of text to process
#' @param min_word_length Minimum word length to consider (default: 3)
#' @return List of character vectors, each containing significant words
#' @export
extract_significant_words <- function(text, min_word_length = 3) {
  # Handle NA and empty strings
  if (length(text) == 0) return(list())
  
  # Convert to uppercase for consistent matching
  text <- toupper(as.character(text))
  text[is.na(text)] <- ""
  
  # Split into words, removing punctuation
  word_lists <- strsplit(gsub("[[:punct:]]", " ", text), "\\s+")
  
  # Process each text entry
  lapply(word_lists, function(words) {
    # Remove empty strings
    words <- words[words != ""]
    
    # Filter by minimum length
    words <- words[nchar(words) >= min_word_length]
    
    # Remove stopwords
    words <- words[!words %in% PORTUGUESE_STOPWORDS]
    
    # Return unique words
    unique(words)
  })
}

#' Create blocking keys from significant words
#' 
#' Generates blocking keys by combining significant words from name and address fields
#' 
#' @param name_words List of significant words from normalized names
#' @param addr_words List of significant words from normalized addresses
#' @param max_combinations Maximum number of word combinations to generate (default: 10)
#' @return Character vector of blocking keys
#' @export
create_word_blocking_keys <- function(name_words, addr_words, max_combinations = 10) {
  n <- length(name_words)
  if (n == 0) return(character(0))
  
  blocking_keys <- character(n)
  
  for (i in seq_len(n)) {
    # Combine words from both name and address
    all_words <- unique(c(name_words[[i]], addr_words[[i]]))
    
    if (length(all_words) == 0) {
      blocking_keys[i] <- ""
      next
    }
    
    # For efficiency, limit the number of words used
    if (length(all_words) > 5) {
      all_words <- all_words[1:5]
    }
    
    # Create a single blocking key by concatenating sorted words
    # This ensures "SANTOS DUMONT" and "DUMONT SANTOS" get the same key
    blocking_keys[i] <- paste(sort(all_words), collapse = "_")
  }
  
  blocking_keys
}

#' Find records that share at least one significant word
#' 
#' Efficient function to find pairs of records that share at least one significant word
#' 
#' @param words1 List of word vectors for first dataset
#' @param words2 List of word vectors for second dataset
#' @param min_shared_words Minimum number of shared words (default: 1)
#' @return Data.table with columns x (index in words1) and y (index in words2)
#' @export
find_shared_word_pairs <- function(words1, words2, min_shared_words = 1) {
  # Create word-to-index mappings for efficient lookup
  # For dataset 1
  word_to_idx1 <- data.table()
  for (i in seq_along(words1)) {
    if (length(words1[[i]]) > 0) {
      word_to_idx1 <- rbind(word_to_idx1, 
                            data.table(word = words1[[i]], idx = i))
    }
  }
  
  # For dataset 2
  word_to_idx2 <- data.table()
  for (i in seq_along(words2)) {
    if (length(words2[[i]]) > 0) {
      word_to_idx2 <- rbind(word_to_idx2, 
                            data.table(word = words2[[i]], idx = i))
    }
  }
  
  # If either dataset has no words, return empty pairs
  if (nrow(word_to_idx1) == 0 || nrow(word_to_idx2) == 0) {
    return(data.table(x = integer(0), y = integer(0)))
  }
  
  # Find shared words
  setkey(word_to_idx1, word)
  setkey(word_to_idx2, word)
  
  # Inner join on words to find potential pairs
  shared <- word_to_idx1[word_to_idx2, on = "word", allow.cartesian = TRUE]
  setnames(shared, c("idx", "i.idx"), c("x", "y"))
  
  if (min_shared_words == 1) {
    # Remove duplicates and return
    pairs <- unique(shared[, .(x, y)])
  } else {
    # Count shared words per pair
    pair_counts <- shared[, .N, by = .(x, y)]
    pairs <- pair_counts[N >= min_shared_words, .(x, y)]
  }
  
  pairs
}

#' Apply two-level blocking for panel ID matching
#' 
#' Uses municipality as primary blocking and shared words as secondary blocking
#' 
#' @param data1 First dataset (data.table)
#' @param data2 Second dataset (data.table)
#' @param municipality_col Column name for municipality code
#' @param name_col Column name for normalized name
#' @param addr_col Column name for normalized address
#' @param fallback_on_empty Whether to fall back to all comparisons if no shared words
#' @param min_words_threshold Minimum significant words required to apply blocking (default: 2)
#' @return reclin2 pairs object with blocked pairs
#' @export
create_two_level_blocked_pairs <- function(data1, data2, 
                                         municipality_col = "cod_localidade_ibge",
                                         name_col = "normalized_name", 
                                         addr_col = "normalized_addr",
                                         fallback_on_empty = TRUE,
                                         min_words_threshold = 2) {
  
  # First apply municipality blocking using reclin2
  pairs <- pair_blocking(data1, data2, municipality_col)
  
  if (nrow(pairs) == 0) {
    return(pairs)
  }
  
  # Extract the actual records for word processing
  x_indices <- pairs$.x
  y_indices <- pairs$.y
  
  # Get unique indices to avoid redundant word extraction
  unique_x <- unique(x_indices)
  unique_y <- unique(y_indices)
  
  # Extract words for unique records only
  x_name_words <- extract_significant_words(data1[[name_col]][unique_x])
  x_addr_words <- extract_significant_words(data1[[addr_col]][unique_x])
  y_name_words <- extract_significant_words(data2[[name_col]][unique_y])
  y_addr_words <- extract_significant_words(data2[[addr_col]][unique_y])
  
  # Combine words from name and address for each record
  x_all_words <- mapply(function(n, a) unique(c(n, a)), 
                       x_name_words, x_addr_words, 
                       SIMPLIFY = FALSE)
  y_all_words <- mapply(function(n, a) unique(c(n, a)), 
                       y_name_words, y_addr_words, 
                       SIMPLIFY = FALSE)
  
  # Create lookup tables to map from original indices to unique indices
  x_lookup <- match(x_indices, unique_x)
  y_lookup <- match(y_indices, unique_y)
  
  # Check which pairs share words
  keep_pair <- logical(nrow(pairs))
  no_words_count <- 0
  no_match_count <- 0
  
  for (i in seq_len(nrow(pairs))) {
    x_words <- x_all_words[[x_lookup[i]]]
    y_words <- y_all_words[[y_lookup[i]]]
    
    # Check if they share any words
    if (length(x_words) == 0 || length(y_words) == 0) {
      # If either has no significant words, keep the pair (conservative)
      keep_pair[i] <- fallback_on_empty
      if (fallback_on_empty) no_words_count <- no_words_count + 1
    } else if (length(x_words) < min_words_threshold || length(y_words) < min_words_threshold) {
      # If either has very few words, be conservative and keep the pair
      # This handles cases where stopword filtering was too aggressive
      keep_pair[i] <- TRUE
      no_words_count <- no_words_count + 1
    } else {
      # Both have sufficient words - check for overlap
      has_match <- any(x_words %in% y_words)
      keep_pair[i] <- has_match
      if (!has_match) no_match_count <- no_match_count + 1
    }
  }
  
  if (no_match_count > 0) {
    cat("    Note: ", no_match_count, " pairs excluded (no shared words)\n", sep = "")
  }
  if (no_words_count > 0) {
    cat("    Note: ", no_words_count, " pairs kept (fallback - no significant words)\n", sep = "")
  }
  
  # Filter pairs to keep only those with shared words
  filtered_pairs <- pairs[keep_pair]
  
  cat("Two-level blocking: ", nrow(pairs), " municipality pairs -> ", 
      nrow(filtered_pairs), " pairs with shared words (", 
      round(100 * nrow(filtered_pairs) / nrow(pairs), 1), "% retained)\n", sep = "")
  
  filtered_pairs
}

#' Generate blocking statistics for monitoring
#' 
#' Provides statistics about the effectiveness of two-level blocking
#' 
#' @param original_pairs Number of pairs without word blocking
#' @param blocked_pairs Number of pairs after word blocking
#' @param municipality_code Municipality code for logging
#' @return List with blocking statistics
#' @export
calculate_blocking_stats <- function(original_pairs, blocked_pairs, municipality_code = NULL) {
  reduction_pct <- round(100 * (1 - blocked_pairs / original_pairs), 1)
  
  stats <- list(
    municipality = municipality_code,
    original_pairs = original_pairs,
    blocked_pairs = blocked_pairs,
    reduction_pct = reduction_pct,
    speedup_factor = round(original_pairs / blocked_pairs, 1)
  )
  
  if (!is.null(municipality_code)) {
    cat("Municipality", municipality_code, ": ", 
        format(original_pairs, big.mark = ","), " -> ", 
        format(blocked_pairs, big.mark = ","), 
        " pairs (", reduction_pct, "% reduction, ", 
        stats$speedup_factor, "x speedup)\n", sep = "")
  }
  
  stats
}