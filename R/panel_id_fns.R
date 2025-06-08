## Panel ID functions

library(data.table)
source("R/data_table_utils.R")

process_year_pairs <- function(panel, best_pairs, year_from, year_to) {
  # Standardize column names in best_pairs
  standardize_column_names(best_pairs, inplace = TRUE)
  
  # Rename the columns in best_pairs to match the years
  clean_pairs <- best_pairs[, .(local_id_from = x_local_id, local_id_to = y_local_id)]
  setnames(
    clean_pairs, c("local_id_from", "local_id_to"),
    c(paste0("local_id_", year_from), paste0("local_id_", year_to))
  )
  
  # Identify missing ids from the previous year that are not in the current panel
  missing_ids <- setdiff(
    clean_pairs[[paste0("local_id_", year_from)]],
    panel[[paste0("local_id_", year_from)]]
  )
  
  # Create rows for missing ids
  if (length(missing_ids) > 0) {
    missing_rows <- data.table(matrix(NA, nrow = length(missing_ids), ncol = ncol(panel)))
    setnames(missing_rows, names(panel))
    missing_rows[[paste0("local_id_", year_from)]] <- missing_ids
    
    # Add missing rows to the panel
    panel <- rbindlist(list(panel, missing_rows), fill = TRUE)
  }
  
  # Join with clean_pairs
  panel <- clean_pairs[
    panel,
    on = paste0("local_id_", year_from),
    nomatch = NA
  ]
  
  return(panel)
}

make_panel_ids <- function(panel_ids_df, panel_ids_states, geocoded_locais) {
  # Standardize column names
  standardize_column_names(panel_ids_df, inplace = TRUE)
  standardize_column_names(panel_ids_states, inplace = TRUE)
  standardize_column_names(geocoded_locais, inplace = TRUE)
  
  panel_ids <- rbindlist(list(panel_ids_df, panel_ids_states))
  
  # Join with geocoded locais using data.table syntax
  panel_ids <- geocoded_locais[
    panel_ids,
    on = .(local_id),
    nomatch = NA
  ][, .(local_id, panel_id, ano, 
        long = tse_long, lat = tse_lat)]
  
  # For each panel_id, get coordinates from most recent year
  # (TSE data doesn't have pred_dist)
  panel_ids_best <- panel_ids[
    order(panel_id, -ano)
  ][, .SD[1], by = .(panel_id)
  ][, .(panel_id, long, lat)]
  
  # Remove coordinates from panel_ids
  panel_ids[, c("long", "lat", "ano") := NULL]
  
  # Join with best coordinates using data.table syntax
  panel_ids <- panel_ids_best[
    panel_ids,
    on = .(panel_id),
    nomatch = NA
  ]
  
  return(panel_ids)
}

create_panel_dataset <- function(final_pairs_list, years) {
  # Ensure the years are sorted
  years <- sort(years)
  
  # Extract the best pairs for the first two years
  first_year <- years[1]
  second_year <- years[2]
  best_pairs_first <- final_pairs_list[[paste0(first_year, "_", second_year)]]
  
  # Standardize column names
  standardize_column_names(best_pairs_first, inplace = TRUE)
  
  # Create the initial panel dataset with standardized column names
  panel <- best_pairs_first[, .(
    local_id_first = x_local_id,
    local_id_second = y_local_id
  )]
  setnames(
    panel, c("local_id_first", "local_id_second"),
    c(paste0("local_id_", first_year), paste0("local_id_", second_year))
  )
  
  # Process each subsequent pair of years
  for (i in seq(2, length(years) - 1)) {
    year_from <- years[i]
    year_to <- years[i + 1]
    best_pairs <- final_pairs_list[[paste0(year_from, "_", year_to)]]
    panel <- process_year_pairs(panel, best_pairs, year_from, year_to)
  }
  
  # Add panel_id
  panel[, panel_id := apply(.SD, 1, min, na.rm = TRUE), .SDcols = patterns("local_id_")]
  
  # Create the final dataset
  panel_long <- melt(panel,
    id.vars = "panel_id", 
    measure.vars = patterns("local_id_"),
    variable.name = "year", 
    value.name = "local_id"
  )[, .(local_id, panel_id)]
  
  # Remove rows with NA local_id
  panel_clean <- panel_long[!is.na(local_id)]
  
  return(panel_clean)
}

create_and_select_best_pairs <- function(data, years, blocking_column, scoring_columns) {
  pairs_list <- list()
  
  # Standardize column names in input data
  standardize_column_names(data, inplace = TRUE)
  
  # Sort years to ensure correct order
  years <- sort(years)
  
  for (i in seq_along(years)[-length(years)]) {
    year1 <- years[i]
    year2 <- years[i + 1]
    cat("Processing year pair:", year1, "->", year2, "\n")
    
    # Subset the data for the two consecutive years
    linkexample1 <- data[ano == year1]
    linkexample2 <- data[ano == year2]
    
    if (nrow(linkexample1) == 0 || nrow(linkexample2) == 0) {
      cat("  Skipping - no data for one or both years\n")
      next
    }
    
    # Create pairs using pair_blocking
    pairs <- pair_blocking(linkexample1, linkexample2, blocking_column)
    
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
    
    cat("  Found", nrow(best_pairs), "matches\n")
  }
  
  return(pairs_list)
}

make_panel_1block <- function(block, years, blocking_column, scoring_columns) {
  # Standardize column names in block
  standardize_column_names(block, inplace = TRUE)
  
  cat("Processing state:", unique(block$sg_uf), "\n")
  
  pairs_list <- create_and_select_best_pairs(block, years, blocking_column, scoring_columns)
  
  if (length(pairs_list) == 0) {
    cat("  No pairs found for this block\n")
    return(NULL)
  }
  
  panel <- create_panel_dataset(pairs_list, years)
  
  cat("  Final panel has", nrow(panel), "observations\n")
  
  return(panel)
}

# Function to validate panel consistency
validate_panel_consistency <- function(panel_data, years) {
  cat("=== Panel Consistency Validation ===\n\n")
  
  # Check for duplicates
  duplicates <- panel_data[, .N, by = local_id][N > 1]
  cat("Duplicate local_ids:", nrow(duplicates), "\n")
  
  # Check panel_id distribution
  panel_sizes <- panel_data[, .N, by = panel_id]
  cat("Panel size distribution:\n")
  print(panel_sizes[, .N, by = N][order(N)])
  
  # Check for missing years in panels
  if ("year" %in% names(panel_data)) {
    year_coverage <- panel_data[, .(years_present = length(unique(year))), by = panel_id]
    cat("Year coverage distribution:\n")
    print(year_coverage[, .N, by = years_present][order(years_present)])
  }
  
  invisible(list(
    duplicates = duplicates,
    panel_sizes = panel_sizes,
    year_coverage = if("year" %in% names(panel_data)) year_coverage else NULL
  ))
}

# Batch processing function for multiple states/blocks
# Note: This function is kept for backwards compatibility but is not used
# when targets dynamic branching is enabled (which is the default)
process_multiple_blocks <- function(data_list, years, blocking_column, scoring_columns, parallel = TRUE) {
  cat("Processing", length(data_list), "blocks/states\n")
  
  # Process blocks sequentially
  results <- lapply(seq_along(data_list), function(i) {
    cat("Block", i, "of", length(data_list), "\n")
    make_panel_1block(data_list[[i]], years, blocking_column, scoring_columns)
  })
  
  # Remove NULL results and combine
  valid_results <- results[!sapply(results, is.null)]
  
  if (length(valid_results) > 0) {
    combined <- rbindlist(valid_results, fill = TRUE)
    cat("Combined panel has", nrow(combined), "total observations\n")
    return(combined)
  } else {
    cat("No valid results found\n")
    return(NULL)
  }
}

export_panel_ids <- function(panel_ids) {
  fwrite(panel_ids, "./output/panel_ids.csv.gz")
  "./output/panel_ids.csv.gz"
}

# Wrapper function for single-state panel ID processing (for targets dynamic branching)
process_panel_ids_single_state <- function(locais_full, state_code, years, blocking_column, scoring_columns) {
  # Filter data for the specific state
  state_data <- locais_full[sg_uf == state_code]
  
  # Check if there's data for this state
  if (nrow(state_data) == 0) {
    cat("No data found for state:", state_code, "\n")
    return(data.table())
  }
  
  # Process panel IDs for this state
  result <- make_panel_1block(
    block = state_data,
    years = years,
    blocking_column = blocking_column,
    scoring_columns = scoring_columns
  )
  
  # Add state identifier to the result for tracking
  if (!is.null(result) && nrow(result) > 0) {
    result[, sg_uf := state_code]
  }
  
  return(result)
}

# Function to combine panel IDs from multiple states
combine_state_panel_ids <- function(panel_ids_list) {
  # Remove NULL or empty results
  valid_results <- panel_ids_list[!sapply(panel_ids_list, function(x) is.null(x) || nrow(x) == 0)]
  
  if (length(valid_results) == 0) {
    cat("No valid panel ID results to combine\n")
    return(data.table())
  }
  
  # Combine all state results
  combined <- rbindlist(valid_results, fill = TRUE)
  
  # Remove state identifier if present (was only for tracking)
  if ("sg_uf" %in% names(combined)) {
    combined[, sg_uf := NULL]
  }
  
  cat("Combined panel IDs from", length(valid_results), "states\n")
  cat("Total panel IDs:", nrow(combined), "\n")
  
  return(combined)
}

