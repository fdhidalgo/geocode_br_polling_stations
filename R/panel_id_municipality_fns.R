## Panel ID Municipality-based Processing Functions
## Optimized for parallel processing at municipality level

library(data.table)

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

#' Get controller type for a batch
#' 
#' Helper function to determine which controller to use for a batch
#' 
#' @param batch_id The batch ID to check
#' @param municipality_batches The municipality batches data table
#' @return Character string with controller name
get_batch_controller <- function(batch_id_val, municipality_batches) {
  batch_type <- unique(municipality_batches[batch_id == batch_id_val]$batch_type)[1]
  
  if (is.na(batch_type)) {
    return("standard")
  }
  
  switch(batch_type,
    "mega_cities" = "mega_cities",
    "memory_limited" = "memory_limited",
    "standard"
  )
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
  scoring_columns
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
    
    cat("Processing municipality:", muni_code, 
        "- Stations:", nrow(muni_data[, .N, by = local_id][, .N]),
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
        scoring_columns = scoring_columns
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

#' Monitor panel ID processing progress
#' 
#' Provides summary statistics about panel ID processing
#' 
#' @param municipality_batches The municipality batch assignments
#' @param completed_batches Vector of completed batch IDs
#' @return NULL (prints progress information)
monitor_panel_progress <- function(municipality_batches, completed_batches = NULL) {
  total_batches <- length(unique(municipality_batches$batch_id))
  total_municipalities <- nrow(municipality_batches)
  total_stations <- sum(municipality_batches$n_stations)
  
  cat("\n=== Panel ID Processing Progress ===\n")
  cat("Total municipalities:", total_municipalities, "\n")
  cat("Total polling stations:", format(total_stations, big.mark = ","), "\n")
  cat("Total batches:", total_batches, "\n")
  
  # Batch type breakdown
  batch_summary <- municipality_batches[, .(
    n_batches = length(unique(batch_id)),
    n_municipalities = .N,
    n_stations = sum(n_stations)
  ), by = batch_type]
  
  cat("\nBatch breakdown:\n")
  print(batch_summary)
  
  if (!is.null(completed_batches)) {
    n_completed <- length(completed_batches)
    pct_complete <- round(100 * n_completed / total_batches, 1)
    
    completed_data <- municipality_batches[batch_id %in% completed_batches]
    stations_complete <- sum(completed_data$n_stations)
    pct_stations_complete <- round(100 * stations_complete / total_stations, 1)
    
    cat("\nProgress:\n")
    cat("Batches completed:", n_completed, "/", total_batches, 
        "(", pct_complete, "%)\n")
    cat("Stations processed:", format(stations_complete, big.mark = ","), "/", 
        format(total_stations, big.mark = ","), 
        "(", pct_stations_complete, "%)\n")
  }
  
  cat("\n")
  invisible(NULL)
}

#' Create and select best pairs with optimizations
#' 
#' Optimized version of create_and_select_best_pairs that processes more efficiently
#' 
#' @param data Data table with polling station data
#' @param years Vector of years to process
#' @param blocking_column Column to use for blocking
#' @param scoring_columns Columns to use for scoring
#' @return List of best pairs for each year transition
create_and_select_best_pairs_optimized <- function(data, years, blocking_column, scoring_columns) {
  pairs_list <- list()
  
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
        "(", nrow(linkexample1), "x", nrow(linkexample2), "comparisons)\n")
    
    # For very large comparisons, consider sampling or chunking
    n_comparisons <- nrow(linkexample1) * nrow(linkexample2)
    if (n_comparisons > 10000000) {  # 10 million comparisons
      cat("    Warning: Large comparison set. Consider additional blocking.\n")
    }
    
    # Create pairs using pair_blocking
    pairs <- pair_blocking(linkexample1, linkexample2, blocking_column)
    
    if (nrow(pairs) == 0) {
      cat("    No pairs found after blocking\n")
      next
    }
    
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
    best_pairs <- select_n_to_m(pairs, threshold = 0, score = "weights", var = "match", n = 1, m = 1)
    
    # Keep only the pairs where match is TRUE
    best_pairs <- best_pairs[match == TRUE]
    
    # Store the final matched pairs in the list
    pairs_list[[paste0(year1, "_", year2)]] <- best_pairs
    
    cat("    Found", nrow(best_pairs), "matches\n")
    
    # Clean up memory
    rm(pairs)
    gc(verbose = FALSE)
  }
  
  return(pairs_list)
}