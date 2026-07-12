#' Panel Creation Functions
#'
#' Functions for creating temporal panel identifiers that link polling stations
#' across election years using Fellegi-Sunter record linkage methodology.
#' Supports both standard and memory-efficient municipality-based processing
#' strategies with optional two-level blocking for large datasets.

library(data.table)
library(reclin2)
library(stringr)

# ============================================================================
# Core Panel Creation Functions
# ============================================================================

#' Process year pairs for panel creation
#'
#' @param panel Current panel data
#' @param best_pairs Best matching pairs for year transition
#' @param year_from Source year
#' @param year_to Target year
#' @return Updated panel with new year connections
#' @export
process_year_pairs <- function(panel, best_pairs, year_from, year_to) {
  # Check if best_pairs is NULL or empty
  if (is.null(best_pairs) || nrow(best_pairs) == 0) {
    # If no pairs for this year combination, return panel unchanged
    return(panel)
  }

  # Each transition must extend the panel into a year it does not yet hold. If
  # the target column already exists, this transition is being processed twice:
  # the join below would silently emit a duplicate i.local_id_<year_to> column
  # that then inflates panel_id and the melt (the #53 duplicate-row bug). Fail
  # loud rather than corrupt the panel.
  year_to_col <- paste0("local_id_", year_to)
  if (year_to_col %in% names(panel)) {
    stop(
      "process_year_pairs: panel already has column '",
      year_to_col,
      "'; transition ",
      year_from,
      " -> ",
      year_to,
      " is being processed more than once."
    )
  }

  # Standardize column names in best_pairs
  standardize_column_names(best_pairs, inplace = TRUE)

  # Rename the columns in best_pairs to match the years
  clean_pairs <- best_pairs[, .(local_id_from = x_local_id, local_id_to = y_local_id)]
  setnames(
    clean_pairs,
    c("local_id_from", "local_id_to"),
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

#' Create panel IDs from matched pairs
#'
#' @param panel_ids_df Panel IDs from main processing
#' @param panel_ids_states Panel IDs from state-specific processing
#' @param geocoded_locais Geocoded polling station data
#' @return Data table with panel IDs and coordinates
#' @export
make_panel_ids <- function(panel_ids_df, panel_ids_states, geocoded_locais) {
  # Standardize the panel-id tables' column names. geocoded_locais is deliberately
  # NOT standardized: it is the shared canonical target, so an inplace rename here
  # would corrupt the columns other consumers (export, release gates) read from
  # the same in-memory object - and the columns used below (local_id, ano,
  # final_long, final_lat, pred_dist) are already in their final form anyway.
  standardize_column_names(panel_ids_df, inplace = TRUE)
  standardize_column_names(panel_ids_states, inplace = TRUE)

  panel_ids <- rbindlist(list(panel_ids_df, panel_ids_states))

  # Attach each station-year's final coordinate (the TSE official coordinate when
  # available, otherwise the model's selection) and its predicted error. pred_dist
  # is 0 for TSE-covered rows and the model's predicted distance otherwise, so
  # ordering a panel's years by pred_dist puts ground-truth coordinates first and
  # the most confident model coordinate next.
  panel_ids <- geocoded_locais[
    panel_ids,
    on = .(local_id),
    nomatch = NA
  ][, .(local_id, panel_id, ano, long = final_long, lat = final_lat, pred_dist)]

  # For each panel, choose the single most accurate coordinate: smallest
  # pred_dist, ties broken toward the most recent year. Only station-years that
  # actually carry a coordinate compete, so a panel is left uncoordinated only
  # when every one of its years failed to geocode (rare). This is why the model
  # coordinates matter here: without them, panels whose years predate TSE ground
  # truth (2018) would all come out blank.
  # unique(by=) keeps the first row per panel on the already-sorted table, which
  # is the smallest-pred_dist / most-recent row - the same pick as .SD[1] by
  # group, without the per-group allocation.
  panel_ids_best <- unique(
    panel_ids[!is.na(long) & !is.na(lat)][order(panel_id, pred_dist, -ano)],
    by = "panel_id"
  )[, .(panel_id, long, lat, pred_dist)]

  # Replace the per-station coordinates with the chosen panel-level coordinate.
  panel_ids[, c("long", "lat", "pred_dist", "ano") := NULL]
  panel_ids <- panel_ids_best[
    panel_ids,
    on = .(panel_id),
    nomatch = NA
  ]
  setcolorder(panel_ids, c("panel_id", "local_id", "long", "lat", "pred_dist"))
  panel_ids[]
}

#' Create panel dataset from matched pairs
#'
#' @param final_pairs_list List of matched pairs for each year transition
#' @param years Vector of years to process
#' @return Data table with panel IDs
#' @export
create_panel_dataset <- function(final_pairs_list, years) {
  # Ensure the years are sorted
  years <- sort(years)

  # Walk the year-transitions once. Transition i links years[i] -> years[i + 1],
  # for i in 1..(length(years) - 1). The first transition that has pairs seeds
  # the panel; every later transition with pairs extends it by one year. Each
  # transition is visited exactly once, so a two-year block never reprocesses
  # its only pair (the source of the duplicate-row bug this replaced).
  panel <- NULL
  for (i in seq_len(length(years) - 1L)) {
    best_pairs <- final_pairs_list[[paste0(years[i], "_", years[i + 1L])]]
    if (is.null(best_pairs) || nrow(best_pairs) == 0) {
      next
    }

    if (is.null(panel)) {
      # Seed the panel from the first non-empty transition.
      standardize_column_names(best_pairs, inplace = TRUE)
      panel <- best_pairs[, .(
        local_id_first = x_local_id,
        local_id_second = y_local_id
      )]
      setnames(
        panel,
        c("local_id_first", "local_id_second"),
        c(paste0("local_id_", years[i]), paste0("local_id_", years[i + 1L]))
      )
    } else {
      panel <- process_year_pairs(panel, best_pairs, years[i], years[i + 1L])
    }
  }

  if (is.null(panel)) {
    # No valid pairs found at all
    return(data.table())
  }

  # Add panel_id
  panel[, panel_id := apply(.SD, 1, min, na.rm = TRUE), .SDcols = patterns("local_id_")]

  # Create the final dataset
  panel_long <- melt(
    panel,
    id.vars = "panel_id",
    measure.vars = patterns("local_id_"),
    variable.name = "year",
    value.name = "local_id"
  )[, .(local_id, panel_id)]

  # Remove rows with NA local_id
  panel_clean <- panel_long[!is.na(local_id)]

  return(panel_clean)
}


#' Process panel IDs for one block
#'
#' @param block Data for one municipality/block
#' @param years Vector of years to process
#' @param blocking_column Column to use for blocking
#' @param scoring_columns Columns to use for scoring
#' @param use_word_blocking Whether to use two-level word blocking
#' @return Panel data for the block
#' @export
make_panel_1block <- function(
  block,
  years,
  blocking_column,
  scoring_columns,
  use_word_blocking = FALSE,
  panel_weight_threshold = 0
) {
  # Standardize column names in block
  standardize_column_names(block, inplace = TRUE)

  cat("Processing block with", nrow(block), "rows\n")

  # Use optimized version
  pairs_list <- create_and_select_best_pairs_optimized(
    block,
    years,
    blocking_column,
    scoring_columns,
    use_word_blocking,
    panel_weight_threshold
  )

  if (length(pairs_list) == 0) {
    cat("  No pairs found for this block\n")
    return(NULL)
  }

  panel <- create_panel_dataset(pairs_list, years)

  cat("  Final panel has", nrow(panel), "observations\n")

  return(panel)
}


#' Combine panel IDs from multiple states
#'
#' @param panel_ids_list List of panel ID results from different states
#' @return Combined panel ID data
#' @export
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

# ============================================================================
# Municipality-based Processing Functions
# ============================================================================

#' Create municipality batches for panel ID processing
#'
#' Groups municipalities into batches based on polling station count
#' to ensure balanced workloads across workers
#'
#' @param locais_data Data table with polling station data
#' @param target_batch_size Target number of polling stations per batch
#' @return Data table with municipality codes and batch assignments
#' @export
create_panel_municipality_batches <- function(locais_data, target_batch_size = 5000) {
  # Count polling stations per municipality
  muni_counts <- locais_data[
    !is.na(cod_localidade_ibge),
    .(n_stations = uniqueN(local_id)),
    by = .(cod_localidade_ibge, sg_uf)
  ][order(-n_stations)]

  # Classify municipalities by size
  muni_counts[,
    size_class := fcase(
      n_stations > 10000 , "mega"   ,
      n_stations > 5000  , "large"  ,
      n_stations > 1000  , "medium" ,
      default = "small"
    )
  ]

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

  return(muni_counts[, .(cod_localidade_ibge, sg_uf, n_stations, batch_id, size_class)])
}

#' Process panel IDs for a batch of municipalities
#'
#' @param locais_full Full polling station dataset
#' @param municipality_batch Data table with municipality codes for this batch
#' @param years Vector of years to process
#' @param blocking_column Column to use for blocking
#' @param scoring_columns Columns to use for scoring
#' @param use_word_blocking Whether to use two-level word blocking
#' @return Data table with panel IDs for the batch
#' @export
process_panel_ids_municipality_batch <- function(
  locais_full,
  municipality_batch,
  years,
  blocking_column,
  scoring_columns,
  use_word_blocking = FALSE,
  panel_weight_threshold = 0
) {
  # Extract municipality codes for this batch
  muni_codes <- municipality_batch$cod_localidade_ibge

  # Filter data for municipalities in this batch
  batch_data <- locais_full[cod_localidade_ibge %in% muni_codes]

  if (nrow(batch_data) == 0) {
    cat("No data found for municipality batch\n")
    return(data.table())
  }

  # Process each municipality separately to enable better memory management.
  # This approach prevents memory exhaustion when processing large states like SP
  # or MG by keeping only one municipality's data in memory at a time.
  #
  # Collect-and-stop (cleanup phase 3, finding H3): a genuine per-municipality
  # error is surfaced at batch end, never swallowed to NULL and filtered out
  # (which would silently exclude the municipality from published panel IDs). A
  # NULL result (no polling stations, too few years, or no cross-year pairs) is a
  # legitimate empty case, distinct from a failure.
  valid_results <- collect_batch_or_stop(
    muni_codes,
    function(muni_code) {
      muni_data <- batch_data[cod_localidade_ibge == muni_code]

      if (nrow(muni_data) == 0) {
        return(NULL)
      }

      n_stations <- length(unique(muni_data$local_id))
      cat(
        "Processing municipality:",
        muni_code,
        "- Stations:",
        n_stations,
        "- Years:",
        length(unique(muni_data$ano)),
        "\n"
      )

      # Distrito Federal (Brasília) requires special handling because it had
      # municipal elections in years that differ from other Brazilian states.
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

      result <- make_panel_1block(
        block = muni_data,
        years = years_to_use,
        blocking_column = blocking_column,
        scoring_columns = scoring_columns,
        use_word_blocking = use_word_blocking,
        panel_weight_threshold = panel_weight_threshold
      )

      # Add municipality identifier for tracking
      if (!is.null(result) && nrow(result) > 0) {
        result[, cod_localidade_ibge := muni_code]
      }

      result
    },
    task_label = "Panel ID creation"
  )

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
#' @export
create_and_select_best_pairs_optimized <- function(
  data,
  years,
  blocking_column,
  scoring_columns,
  use_word_blocking = FALSE,
  panel_weight_threshold = 0
) {
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

  # Initialize blocking statistics
  blocking_stats <- list()

  # Process year pairs
  for (i in seq_along(years)[-length(years)]) {
    year1 <- years[i]
    year2 <- years[i + 1]

    linkexample1 <- year_data[[as.character(year1)]]
    linkexample2 <- year_data[[as.character(year2)]]

    if (is.null(linkexample1) || is.null(linkexample2) || nrow(linkexample1) == 0 || nrow(linkexample2) == 0) {
      cat("  Skipping year pair", year1, "->", year2, "- no data\n")
      next
    }

    cat("  Processing year pair:", year1, "->", year2, "(", nrow(linkexample1), "x", nrow(linkexample2), "records)\n")

    # Create pairs with appropriate blocking strategy
    if (use_word_blocking) {
      # Calculate potential comparisons without word blocking
      pairs_no_word <- pair_blocking(linkexample1, linkexample2, blocking_column)
      original_pairs <- nrow(pairs_no_word)

      # Apply two-level blocking
      pairs <- create_two_level_blocked_pairs(
        linkexample1,
        linkexample2,
        municipality_col = blocking_column,
        name_col = scoring_columns[1], # normalized_name
        addr_col = scoring_columns[2], # normalized_addr
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
    pairs <- compare_pairs(pairs, on = scoring_columns, default_comparator = cmp_jarowinkler(0.9), inplace = TRUE)

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

    # Select the best match for each observation. The weight threshold is an
    # explicit argument wired through pipeline config (cleanup phase 3, Medium),
    # replacing the former untracked getOption(). NOTE: the effective default is
    # kept at 0; the adjacent comment's ~0.5 is an open evaluation question
    # (ticket #25), deliberately not changed here.
    best_pairs <- select_n_to_m(
      pairs,
      threshold = panel_weight_threshold,
      score = "weights",
      var = "match",
      n = 1,
      m = 1
    )

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

# ============================================================================
# Two-level Blocking Functions
# ============================================================================

# Portuguese stopwords for polling station contexts
# These common words appear in most polling station names (e.g., "escola municipal")
# and are poor discriminators for matching. Removing them improves the
# signal-to-noise ratio in string matching operations.
PORTUGUESE_STOPWORDS <- c(
  # General stopwords
  "DE",
  "DA",
  "DO",
  "DOS",
  "DAS",
  "E",
  "EM",
  "NA",
  "NO",
  "NAS",
  "NOS",
  "A",
  "O",
  "AS",
  "OS",
  "UM",
  "UMA",
  "UNS",
  "UMAS",

  # School-related terms
  "ESCOLA",
  "MUNICIPAL",
  "ESTADUAL",
  "FEDERAL",
  "PUBLICA",
  "PRIVADA",
  "COLEGIO",
  "INSTITUTO",
  "CENTRO",
  "UNIDADE",
  "EDUCACIONAL",
  "ENSINO",
  "FUNDAMENTAL",
  "MEDIO",
  "INFANTIL",
  "CRECHE",
  "CEI",
  "EMEF",
  "EMEI",
  "EE",
  "EM",
  "EC",
  "EP",
  "ESC",
  "COL",
  "INST",

  # Location terms
  "RUA",
  "AVENIDA",
  "PRACA",
  "ALAMEDA",
  "TRAVESSA",
  "ESTRADA",
  "RODOVIA",
  "AV",
  "R",
  "PC",
  "AL",
  "TR",
  "EST",
  "ROD",
  "VIA",
  "LARGO",
  "BECO",

  # Building/zone terms
  "PREDIO",
  "EDIFICIO",
  "BLOCO",
  "ANDAR",
  "SALA",
  "QUADRA",
  "LOTE",
  "ZONA",
  "BAIRRO",
  "DISTRITO",
  "REGIAO",
  "SETOR",
  "AREA",

  # Common abbreviations
  "S",
  "N",
  "SN",
  "C",
  "CONJ",
  "QD",
  "LT",
  "BL",
  "AP",
  "APTO",

  # Numbers and single letters (often not meaningful for matching)
  "1",
  "2",
  "3",
  "4",
  "5",
  "6",
  "7",
  "8",
  "9",
  "0",
  "I",
  "II",
  "III",
  "IV",
  "V",
  "VI",
  "VII",
  "VIII",
  "IX",
  "X"
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
  if (length(text) == 0) {
    return(list())
  }

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

#' Apply two-level blocking for panel ID matching
#'
#' Uses municipality as primary blocking and shared words as secondary blocking.
#' This dramatically reduces the number of comparisons needed (often 90%+ reduction)
#' while maintaining high recall by only excluding pairs with no shared significant words.
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
create_two_level_blocked_pairs <- function(
  data1,
  data2,
  municipality_col = "cod_localidade_ibge",
  name_col = "normalized_name",
  addr_col = "normalized_addr",
  fallback_on_empty = TRUE,
  min_words_threshold = 2
) {
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
  x_all_words <- mapply(function(n, a) unique(c(n, a)), x_name_words, x_addr_words, SIMPLIFY = FALSE)
  y_all_words <- mapply(function(n, a) unique(c(n, a)), y_name_words, y_addr_words, SIMPLIFY = FALSE)

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

  cat(
    "Two-level blocking: ",
    nrow(pairs),
    " municipality pairs -> ",
    nrow(filtered_pairs),
    " pairs with shared words (",
    round(100 * nrow(filtered_pairs) / nrow(pairs), 1),
    "% retained)\n",
    sep = ""
  )

  filtered_pairs
}

# ============================================================================
# Section-Panel Mapping Functions
# ============================================================================

#' Create section-to-panel mapping for easy user joins
#'
#' Creates a comprehensive mapping from election sections to panel IDs,
#' enabling users to directly join section-level election data with panel
#' identifiers without complex multi-file operations.
#'
#' @param secc_loc_map Section-to-location mapping data (from secc_loc_map files)
#' @param geocoded_locais Geocoded polling station data with local_id
#' @param panel_ids Panel IDs data with panel_id and local_id
#' @return Data table with columns: nr_secao, nr_zona, nr_local_votacao, ano, estado_abrev, nm_localidade, panel_id
#' @export
create_section_panel_mapping <- function(secc_loc_map, geocoded_locais, panel_ids) {
  cat("Creating section-to-panel mapping...\n")

  # Fail loud on empty inputs (cleanup phase 3, Medium): returning an empty
  # data.table hid an upstream failure to produce sections, geocoded locations,
  # or panel IDs.
  if (nrow(secc_loc_map) == 0) {
    stop("create_section_panel_mapping(): empty section-location mapping.")
  }
  if (nrow(geocoded_locais) == 0) {
    stop("create_section_panel_mapping(): empty geocoded locations.")
  }
  if (nrow(panel_ids) == 0) {
    stop("create_section_panel_mapping(): empty panel IDs.")
  }

  # Standardize column names
  standardize_column_names(secc_loc_map, inplace = TRUE)
  standardize_column_names(geocoded_locais, inplace = TRUE)
  standardize_column_names(panel_ids, inplace = TRUE)

  cat("Input data sizes:\n")
  cat("  Sections:", format(nrow(secc_loc_map), big.mark = ","), "\n")
  cat("  Geocoded locations:", format(nrow(geocoded_locais), big.mark = ","), "\n")
  cat("  Panel IDs:", format(nrow(panel_ids), big.mark = ","), "\n")

  # Step 1: Get unique location identifiers from geocoded data
  # Using the correct join keys: (nr_zona, nr_local_votacao, ano, cd_localidade_tse)
  # Note: Adding cd_localidade_tse to prevent cross-municipality contamination
  # This is critical because nr_locvot values are reused across different municipalities
  geocoded_locations <- unique(geocoded_locais[, .(nr_zona, nr_local_votacao, ano, cd_localidade_tse, local_id)])

  cat("  Unique geocoded locations:", format(nrow(geocoded_locations), big.mark = ","), "\n")

  # Step 2: Join sections with geocoded locations to get local_id
  cat("Joining sections with geocoded locations...\n")

  # Use merge() to maintain proper 1:1 relationship (avoid cartesian explosion from data.table join syntax)
  sections_with_local_id <- merge(
    secc_loc_map,
    geocoded_locations,
    by = c("nr_zona", "nr_local_votacao", "ano", "cd_localidade_tse"),
    all.x = TRUE
  )

  # Check join success rate
  success_count <- sum(!is.na(sections_with_local_id$local_id))
  success_rate <- success_count / nrow(secc_loc_map)

  cat("  Join success rate:", round(success_rate * 100, 1), "%\n")
  cat("  Sections with local_id:", format(success_count, big.mark = ","), "\n")

  if (success_rate < 0.8) {
    warning(sprintf(
      "Low section-to-location join success rate: %.1f%% (<80%%). Check data quality.",
      success_rate * 100
    ))
  }

  # Step 3: Join with panel IDs to get panel_id
  cat("Joining with panel IDs...\n")

  sections_with_local_id_clean <- sections_with_local_id[!is.na(local_id)]

  # Use merge() to maintain proper 1:1 relationship
  final_mapping <- merge(
    sections_with_local_id_clean,
    panel_ids,
    by = "local_id",
    all.x = TRUE
  )

  # Check final join success rate
  final_success_count <- sum(!is.na(final_mapping$panel_id))
  final_success_rate <- final_success_count / nrow(sections_with_local_id_clean)

  cat("  Final join success rate:", round(final_success_rate * 100, 1), "%\n")
  cat("  Final mapping records:", format(final_success_count, big.mark = ","), "\n")

  if (final_success_rate < 0.9) {
    warning(sprintf(
      "Low section-to-panel join success rate: %.1f%% (<90%%). Check panel ID coverage.",
      final_success_rate * 100
    ))
  }

  # Step 4: Clean and select final columns
  final_clean <- final_mapping[!is.na(panel_id)]

  # Select and order columns for user convenience
  # Note: standardized column names after processing
  output_columns <- c("nr_secao", "nr_zona", "nr_local_votacao", "ano", "estado_abrev", "nm_localidade", "panel_id")
  available_columns <- intersect(output_columns, colnames(final_clean))

  if (length(available_columns) < length(output_columns)) {
    missing_cols <- setdiff(output_columns, available_columns)
    warning("Missing columns in section-panel output: ", paste(missing_cols, collapse = ", "))
  }

  result <- final_clean[, .SD, .SDcols = available_columns]

  # Validate result
  cat("Validating final mapping...\n")

  # Check for duplicate section identifiers
  duplicate_sections <- result[, .N, by = .(nr_secao, nr_zona, ano, estado_abrev)][N > 1]
  if (nrow(duplicate_sections) > 0) {
    warning(nrow(duplicate_sections), " duplicate section identifiers found")
  }

  # Check panel ID distribution
  panel_distribution <- result[, .N, by = panel_id][order(-N)]
  max_sections_per_panel <- max(panel_distribution$N)
  cat("  Max sections per panel:", max_sections_per_panel, "\n")

  if (max_sections_per_panel > 500) {
    cat("Warning: Some panels have >500 sections. This may indicate data quality issues.\n")
  }

  # Summary statistics
  cat("\nFinal mapping summary:\n")
  cat("  Total records:", format(nrow(result), big.mark = ","), "\n")
  cat("  Unique sections:", format(nrow(result[, .(nr_secao, nr_zona, ano, estado_abrev)]), big.mark = ","), "\n")
  cat("  Unique panels:", format(length(unique(result$panel_id)), big.mark = ","), "\n")
  cat("  Years covered:", paste(sort(unique(result$ano)), collapse = ", "), "\n")
  cat("  States covered:", length(unique(result$estado_abrev)), "\n")

  # Sort for user convenience
  setorder(result, ano, estado_abrev, nr_zona, nr_local_votacao, nr_secao)

  cat("Section-to-panel mapping created successfully!\n\n")

  result
}

# ============================================================================
# Conservative Blocking Functions
# ============================================================================
