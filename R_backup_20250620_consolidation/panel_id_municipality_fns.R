## Panel ID Municipality-based Processing Functions
## Optimized for parallel processing at municipality level

# Note: 2 unused functions were moved to backup/unused_functions/
# Date: 2025-06-20
# Functions removed: get_batch_controller, monitor_panel_progress

library(data.table)
library(reclin2)

#' Create municipality batches for panel ID processing
#' 
#' Groups municipalities into batches based on polling station count
#' to ensure balanced workloads across workers
#' 
#' @param locais_data Data table with polling station data
#' @param target_batch_size Target number of polling stations per batch
#' @return Data table with municipality codes and batch assignments
create_panel_municipality_batches <- function(locais_data, target_batch_size = 5000) {
  # Count polling stations per municipality
  muni_counts <- locais_data[
    !is.na(cod_localidade_ibge), 
    .(n_stations = uniqueN(local_id)), 
    by = .(cod_localidade_ibge, sg_uf)
  ][order(-n_stations)]
  
  # Classify municipalities by size
  muni_counts[, size_class := fcase(
    n_stations > 10000, "mega",
    n_stations > 5000, "large", 
    n_stations > 1000, "medium",
    default = "small"
  )]
  
  # Assign batches
  muni_counts[, batch_id := integer()]
  
  # Mega cities get individual batches
  mega_cities <- muni_counts[size_class == "mega"]
  if (nrow(mega_cities) > 0) {
    mega_cities[, batch_id := seq_len(.N)]
    muni_counts[size_class == "mega", batch_id := mega_cities$batch_id]
  }
  
  # Large cities get individual or paired batches
  large_cities <- muni_counts[size_class == "large"]
  if (nrow(large_cities) > 0) {
    current_batch <- max(c(0, muni_counts$batch_id), na.rm = TRUE) + 1
    large_cities[, batch_id := current_batch + ((seq_len(.N) - 1) %/% 2)]
    muni_counts[size_class == "large", batch_id := large_cities$batch_id]
  }
  
  # Group medium and small municipalities
  remaining <- muni_counts[is.na(batch_id) | batch_id == 0]
  if (nrow(remaining) > 0) {
    current_batch <- max(c(0, muni_counts$batch_id), na.rm = TRUE) + 1
    remaining[, cumsum_stations := cumsum(n_stations)]
    remaining[, batch_id := current_batch + (cumsum_stations - 1) %/% target_batch_size]
    muni_counts[is.na(batch_id) | batch_id == 0, batch_id := remaining$batch_id]
  }
  
  # Add batch type for controller selection
  muni_counts[, batch_type := fcase(
    size_class == "mega", "mega_cities",
    size_class == "large", "memory_limited",
    default = "standard"
  )]
  
  return(muni_counts[, .(cod_localidade_ibge, sg_uf, n_stations, batch_id, batch_type, size_class)])
}
#' Process panel IDs for a batch of municipalities
#' 
#' @param locais_full Full polling station dataset
#' @param municipality_batch Data table with municipality codes for this batch
#' @param years Vector of years to process
#' @param blocking_column Column to use for blocking
#' @param scoring_columns Columns to use for scoring
#' @return Data table with panel IDs for the batch
process_panel_ids_municipality_batch <- function(
  locais_full, 
  municipality_batch,
  years,
  blocking_column,
  scoring_columns,
  use_word_blocking = FALSE
) {
  # Extract municipality codes for this batch
  muni_codes <- municipality_batch$cod_localidade_ibge
  
  # Filter data for municipalities in this batch
  batch_data <- locais_full[cod_localidade_ibge %in% muni_codes]
  
  if (nrow(batch_data) == 0) {
    cat("No data found for municipality batch\n")
    return(data.table())
  }
  
  # Process each municipality separately to enable better memory management
  results_list <- lapply(muni_codes, function(muni_code) {
    muni_data <- batch_data[cod_localidade_ibge == muni_code]
    
    if (nrow(muni_data) == 0) {
      return(NULL)
    }
    
    # Count unique stations properly
    n_stations <- length(unique(muni_data$local_id))
    
    cat("Processing municipality:", muni_code, 
        "- Stations:", n_stations,
        "- Years:", length(unique(muni_data$ano)), "\n")
    
    # Check for DF special case
    state <- unique(muni_data$sg_uf)[1]
    if (state == "DF") {
      years_to_use <- c(2006, 2008, 2010, 2012, 2014, 2018, 2022, 2024)
    } else {
      years_to_use <- years
    }
    
    # Filter years to those available in the data
    available_years <- sort(unique(muni_data$ano))
    years_to_use <- intersect(years_to_use, available_years)
    
    if (length(years_to_use) < 2) {
      cat("  Insufficient years for panel creation\n")
      return(NULL)
    }
    
    # Process panel IDs for this municipality
    result <- tryCatch({
      make_panel_1block(
        block = muni_data,
        years = years_to_use,
        blocking_column = blocking_column,
        scoring_columns = scoring_columns,
        use_word_blocking = use_word_blocking
      )
    }, error = function(e) {
      cat("  Error processing municipality", muni_code, ":", e$message, "\n")
      return(NULL)
    })
    
    # Add municipality identifier for tracking
    if (!is.null(result) && nrow(result) > 0) {
      result[, cod_localidade_ibge := muni_code]
    }
    
    return(result)
  })
  
  # Remove NULL results and combine
  valid_results <- results_list[!sapply(results_list, is.null)]
  
  if (length(valid_results) == 0) {
    return(data.table())
  }
  
  # Combine results
  combined <- rbindlist(valid_results, fill = TRUE)
  
  # Remove municipality identifier (was only for tracking)
  if ("cod_localidade_ibge" %in% names(combined)) {
    combined[, cod_localidade_ibge := NULL]
  }
  
  cat("Batch complete - Total panel IDs:", nrow(combined), "\n")
  
  return(combined)
}
#' Create and select best pairs with optimizations
#' 
#' Optimized version of create_and_select_best_pairs that processes more efficiently
#' using two-level blocking (municipality + shared words)
#' 
#' @param data Data table with polling station data
#' @param years Vector of years to process
#' @param blocking_column Column to use for blocking
#' @param scoring_columns Columns to use for scoring
#' @param use_word_blocking Whether to use two-level word blocking (default: FALSE)
#' @return List of best pairs for each year transition
create_and_select_best_pairs_optimized <- function(data, years, blocking_column, scoring_columns, 
                                                  use_word_blocking = FALSE) {
  pairs_list <- list()
  
  # Load blocking functions if using word blocking
  if (use_word_blocking && !exists("create_two_level_blocked_pairs")) {
    source("R/panel_id_blocking_fns.R")
  }
  
  # Standardize column names in input data
  standardize_column_names(data, inplace = TRUE)
  
  # Sort years to ensure correct order
  years <- sort(years)
  
  # Pre-compute data subsets for all years to avoid repeated filtering
  year_data <- lapply(years, function(y) {
    subset <- data[ano == y]
    if (nrow(subset) > 0) {
      # Keep only necessary columns to reduce memory usage
      keep_cols <- c("local_id", "ano", "sg_uf", blocking_column, scoring_columns)
      subset[, .SD, .SDcols = intersect(names(subset), keep_cols)]
    } else {
      NULL
    }
  })
  names(year_data) <- as.character(years)
  
  # Initialize blocking statistics
  blocking_stats <- list()
  
  # Process year pairs
  for (i in seq_along(years)[-length(years)]) {
    year1 <- years[i]
    year2 <- years[i + 1]
    
    linkexample1 <- year_data[[as.character(year1)]]
    linkexample2 <- year_data[[as.character(year2)]]
    
    if (is.null(linkexample1) || is.null(linkexample2) || 
        nrow(linkexample1) == 0 || nrow(linkexample2) == 0) {
      cat("  Skipping year pair", year1, "->", year2, "- no data\n")
      next
    }
    
    cat("  Processing year pair:", year1, "->", year2, 
        "(", nrow(linkexample1), "x", nrow(linkexample2), "records)\n")
    
    # Create pairs with appropriate blocking strategy
    if (use_word_blocking) {
      # Calculate potential comparisons without word blocking
      pairs_no_word <- pair_blocking(linkexample1, linkexample2, blocking_column)
      original_pairs <- nrow(pairs_no_word)
      
      # Apply two-level blocking
      pairs <- create_two_level_blocked_pairs(
        linkexample1, linkexample2,
        municipality_col = blocking_column,
        name_col = scoring_columns[1],  # normalized_name
        addr_col = scoring_columns[2],  # normalized_addr
        fallback_on_empty = TRUE
      )
      
      # Store blocking statistics
      blocking_stats[[paste0(year1, "_", year2)]] <- list(
        original = original_pairs,
        blocked = nrow(pairs),
        reduction_pct = round(100 * (1 - nrow(pairs) / original_pairs), 1)
      )
      
      rm(pairs_no_word)
      gc(verbose = FALSE)
    } else {
      # Original blocking on municipality only
      pairs <- pair_blocking(linkexample1, linkexample2, blocking_column)
    }
    
    if (nrow(pairs) == 0) {
      cat("    No pairs found after blocking\n")
      next
    }
    
    cat("    Comparing", format(nrow(pairs), big.mark = ","), "pairs\n")
    
    # Compare pairs using standardized scoring columns
    pairs <- compare_pairs(pairs,
      on = scoring_columns,
      default_comparator = cmp_jarowinkler(0.9), 
      inplace = TRUE
    )
    
    # Rename variables
    match_scoring_columns <- paste0("match_", scoring_columns)
    setnames(pairs, scoring_columns, match_scoring_columns)
    
    # Compute the Machine Learning Fellegi and Sunter Weights
    formula <- as.formula(paste("~", paste(match_scoring_columns, collapse = " + ")))
    m <- problink_em(formula, data = pairs)
    pairs <- predict(m, pairs, add = TRUE)
    
    # Add local_id to the pairs
    pairs[, `:=`(
      x_local_id = linkexample1$local_id[.x],
      y_local_id = linkexample2$local_id[.y]
    )]
    
    # Select the best match for each observation
    # Using a small positive threshold (e.g., 0.5) can help filter out very low confidence matches
    weight_threshold <- getOption("geocode_br.panel_weight_threshold", 0)
    best_pairs <- select_n_to_m(pairs, threshold = weight_threshold, score = "weights", var = "match", n = 1, m = 1)
    
    # Keep only the pairs where match is TRUE
    best_pairs <- best_pairs[match == TRUE]
    
    # Store the final matched pairs in the list
    pairs_list[[paste0(year1, "_", year2)]] <- best_pairs
    
    cat("    Found", format(nrow(best_pairs), big.mark = ","), "matches\n")
    
    # Clean up memory
    rm(pairs)
    gc(verbose = FALSE)
  }
  
  # Report overall blocking statistics if using word blocking
  if (use_word_blocking && length(blocking_stats) > 0) {
    total_original <- sum(sapply(blocking_stats, function(x) x$original))
    total_blocked <- sum(sapply(blocking_stats, function(x) x$blocked))
    overall_reduction <- round(100 * (1 - total_blocked / total_original), 1)
    
    cat("\n  Overall blocking statistics:\n")
    cat("    Total pairs without word blocking: ", format(total_original, big.mark = ","), "\n")
    cat("    Total pairs with word blocking: ", format(total_blocked, big.mark = ","), "\n")
    cat("    Overall reduction: ", overall_reduction, "%\n")
    cat("    Estimated speedup: ", round(total_original / total_blocked, 1), "x\n\n")
  }
  
  return(pairs_list)
}

