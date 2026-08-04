## Temporal panel identifiers linking polling stations across election years, via
## Fellegi-Sunter record linkage blocked by municipality.

library(data.table)
library(reclin2)
library(stringr)

# Extend the panel with one year transition's matched pairs.
process_year_pairs <- function(panel, best_pairs, year_from, year_to) {
  if (is.null(best_pairs) || nrow(best_pairs) == 0) {
    return(panel)
  }

  # A transition must extend the panel into a year it does not yet hold: a duplicate
  # local_id_<year_to> column would silently duplicate rows in the join and melt below.
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

  standardize_column_names(best_pairs, inplace = TRUE)

  clean_pairs <- best_pairs[, .(local_id_from = x_local_id, local_id_to = y_local_id)]
  setnames(
    clean_pairs,
    c("local_id_from", "local_id_to"),
    c(paste0("local_id_", year_from), paste0("local_id_", year_to))
  )

  # Source-year ids the panel has never seen need their own rows before the join.
  missing_ids <- setdiff(
    clean_pairs[[paste0("local_id_", year_from)]],
    panel[[paste0("local_id_", year_from)]]
  )

  if (length(missing_ids) > 0) {
    missing_rows <- data.table(matrix(NA, nrow = length(missing_ids), ncol = ncol(panel)))
    setnames(missing_rows, names(panel))
    missing_rows[[paste0("local_id_", year_from)]] <- missing_ids

    panel <- rbindlist(list(panel, missing_rows), fill = TRUE)
  }

  panel <- clean_pairs[
    panel,
    on = paste0("local_id_", year_from),
    nomatch = NA
  ]

  return(panel)
}

# Attach one coordinate per panel to its member station-years.
make_panel_ids <- function(panel_ids_df, panel_ids_states, geocoded_locais) {
  # geocoded_locais is deliberately not standardized: an inplace rename would corrupt the
  # shared in-memory object other consumers (export, release gates) read.
  standardize_column_names(panel_ids_df, inplace = TRUE)
  standardize_column_names(panel_ids_states, inplace = TRUE)

  panel_ids <- rbindlist(list(panel_ids_df, panel_ids_states))

  # Attach each station-year's final coordinate and predicted error. pred_dist is 0 for
  # TSE-covered rows, so ordering by it puts ground-truth coordinates ahead of model ones.
  panel_ids <- geocoded_locais[
    panel_ids,
    on = .(local_id),
    nomatch = NA
  ][, .(local_id, panel_id, ano, long = final_long, lat = final_lat, pred_dist)]

  # One coordinate per panel: smallest pred_dist, ties to the most recent year. Only
  # station-years carrying a coordinate compete, so a panel comes out blank only when
  # every year failed to geocode. unique(by=) takes the first row of the sorted table.
  panel_ids_best <- unique(
    panel_ids[!is.na(long) & !is.na(lat)][order(panel_id, pred_dist, -ano)],
    by = "panel_id"
  )[, .(panel_id, long, lat, pred_dist)]

  # Swap the per-station coordinates for the chosen panel-level one.
  panel_ids[, c("long", "lat", "pred_dist", "ano") := NULL]
  panel_ids <- panel_ids_best[
    panel_ids,
    on = .(panel_id),
    nomatch = NA
  ]
  setcolorder(panel_ids, c("panel_id", "local_id", "long", "lat", "pred_dist"))
  panel_ids[]
}

# Chain a block's year-to-year matched pairs into one long table of (local_id, panel_id).
create_panel_dataset <- function(final_pairs_list, years) {
  years <- sort(years)

  # Walk transitions in order, each visited once: the first with pairs seeds the panel,
  # later ones extend it by a year.
  panel <- NULL
  for (i in seq_len(length(years) - 1L)) {
    best_pairs <- final_pairs_list[[paste0(years[i], "_", years[i + 1L])]]
    if (is.null(best_pairs) || nrow(best_pairs) == 0) {
      next
    }

    if (is.null(panel)) {
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
    return(data.table())
  }

  # A panel is identified by the smallest local_id it ever holds.
  panel[, panel_id := apply(.SD, 1, min, na.rm = TRUE), .SDcols = patterns("local_id_")]

  panel_long <- melt(
    panel,
    id.vars = "panel_id",
    measure.vars = patterns("local_id_"),
    variable.name = "year",
    value.name = "local_id"
  )[, .(local_id, panel_id)]

  panel_clean <- panel_long[!is.na(local_id)]

  return(panel_clean)
}


# Build the panel for one block (a municipality): match pairs across years, then chain them.
make_panel_1block <- function(
  block,
  years,
  blocking_column,
  scoring_columns,
  use_word_blocking = FALSE,
  panel_weight_threshold = 0
) {
  standardize_column_names(block, inplace = TRUE)

  cat("Processing block with", nrow(block), "rows\n")

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


# Stack per-state panel ID tables into one.
combine_state_panel_ids <- function(panel_ids_list) {
  valid_results <- panel_ids_list[!sapply(panel_ids_list, function(x) is.null(x) || nrow(x) == 0)]

  if (length(valid_results) == 0) {
    cat("No valid panel ID results to combine\n")
    return(data.table())
  }

  combined <- rbindlist(valid_results, fill = TRUE)

  # sg_uf was carried only for progress reporting.
  if ("sg_uf" %in% names(combined)) {
    combined[, sg_uf := NULL]
  }

  cat("Combined panel IDs from", length(valid_results), "states\n")
  cat("Total panel IDs:", nrow(combined), "\n")

  return(combined)
}

# Group municipalities into batches of roughly equal polling-station count, so workers
# get balanced workloads and the largest cities are not batched with anything else.
create_panel_municipality_batches <- function(locais_data, target_batch_size = 5000) {
  muni_counts <- locais_data[
    !is.na(cod_localidade_ibge),
    .(n_stations = uniqueN(local_id)),
    by = .(cod_localidade_ibge, sg_uf)
  ][order(-n_stations)]

  muni_counts[,
    size_class := fcase(
      n_stations > 10000 , "mega"   ,
      n_stations > 5000  , "large"  ,
      n_stations > 1000  , "medium" ,
      default = "small"
    )
  ]

  muni_counts[, batch_id := integer()]

  # Mega cities get a batch each.
  mega_cities <- muni_counts[size_class == "mega"]
  if (nrow(mega_cities) > 0) {
    mega_cities[, batch_id := seq_len(.N)]
    muni_counts[size_class == "mega", batch_id := mega_cities$batch_id]
  }

  # Large cities are paired.
  large_cities <- muni_counts[size_class == "large"]
  if (nrow(large_cities) > 0) {
    current_batch <- max(c(0, muni_counts$batch_id), na.rm = TRUE) + 1
    large_cities[, batch_id := current_batch + ((seq_len(.N) - 1) %/% 2)]
    muni_counts[size_class == "large", batch_id := large_cities$batch_id]
  }

  # Medium and small ones are packed up to target_batch_size stations per batch.
  remaining <- muni_counts[is.na(batch_id) | batch_id == 0]
  if (nrow(remaining) > 0) {
    current_batch <- max(c(0, muni_counts$batch_id), na.rm = TRUE) + 1
    remaining[, cumsum_stations := cumsum(n_stations)]
    remaining[, batch_id := current_batch + (cumsum_stations - 1) %/% target_batch_size]
    muni_counts[is.na(batch_id) | batch_id == 0, batch_id := remaining$batch_id]
  }

  return(muni_counts[, .(cod_localidade_ibge, sg_uf, n_stations, batch_id, size_class)])
}

# Build panel IDs for one batch of municipalities, one municipality at a time.
process_panel_ids_municipality_batch <- function(
  locais_full,
  municipality_batch,
  years,
  blocking_column,
  scoring_columns,
  use_word_blocking = FALSE,
  panel_weight_threshold = 0
) {
  muni_codes <- municipality_batch$cod_localidade_ibge

  batch_data <- locais_full[cod_localidade_ibge %in% muni_codes]

  if (nrow(batch_data) == 0) {
    cat("No data found for municipality batch\n")
    return(data.table())
  }

  # One municipality at a time keeps peak memory bounded in large states like SP and MG.
  # A NULL result is a legitimate empty case (no stations, too few years, no cross-year
  # pairs); a real error is surfaced at batch end rather than dropping a municipality.
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

      # Distrito Federal held elections in different years than the other states.
      state <- unique(muni_data$sg_uf)[1]
      if (state == "DF") {
        years_to_use <- c(2006, 2008, 2010, 2012, 2014, 2018, 2022, 2024)
      } else {
        years_to_use <- years
      }

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

  combined <- rbindlist(valid_results, fill = TRUE)

  # cod_localidade_ibge was carried only for progress reporting.
  if ("cod_localidade_ibge" %in% names(combined)) {
    combined[, cod_localidade_ibge := NULL]
  }

  cat("Batch complete - Total panel IDs:", nrow(combined), "\n")

  return(combined)
}

# For each consecutive year pair, block candidates, score them with Fellegi-Sunter
# weights, and return the best 1-to-1 matches per transition.
create_and_select_best_pairs_optimized <- function(
  data,
  years,
  blocking_column,
  scoring_columns,
  use_word_blocking = FALSE,
  panel_weight_threshold = 0
) {
  pairs_list <- list()

  standardize_column_names(data, inplace = TRUE)

  years <- sort(years)

  # Slice once per year, keeping only the columns used, to avoid repeated filtering.
  year_data <- lapply(years, function(y) {
    subset <- data[ano == y]
    if (nrow(subset) > 0) {
      keep_cols <- c("local_id", "ano", "sg_uf", blocking_column, scoring_columns)
      subset[, .SD, .SDcols = intersect(names(subset), keep_cols)]
    } else {
      NULL
    }
  })
  names(year_data) <- as.character(years)

  blocking_stats <- list()

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

    if (use_word_blocking) {
      # Municipality-only pairs, kept solely as the denominator for the reduction stat.
      pairs_no_word <- pair_blocking(linkexample1, linkexample2, blocking_column)
      original_pairs <- nrow(pairs_no_word)

      pairs <- create_two_level_blocked_pairs(
        linkexample1,
        linkexample2,
        municipality_col = blocking_column,
        name_col = scoring_columns[1], # normalized_name
        addr_col = scoring_columns[2], # normalized_addr
        fallback_on_empty = TRUE
      )

      blocking_stats[[paste0(year1, "_", year2)]] <- list(
        original = original_pairs,
        blocked = nrow(pairs),
        reduction_pct = round(100 * (1 - nrow(pairs) / original_pairs), 1)
      )

      rm(pairs_no_word)
      gc(verbose = FALSE)
    } else {
      pairs <- pair_blocking(linkexample1, linkexample2, blocking_column)
    }

    if (nrow(pairs) == 0) {
      cat("    No pairs found after blocking\n")
      next
    }

    cat("    Comparing", format(nrow(pairs), big.mark = ","), "pairs\n")

    pairs <- compare_pairs(pairs, on = scoring_columns, default_comparator = cmp_jarowinkler(0.9), inplace = TRUE)

    match_scoring_columns <- paste0("match_", scoring_columns)
    setnames(pairs, scoring_columns, match_scoring_columns)

    # Fellegi-Sunter weights, with m/u probabilities estimated by EM on these pairs.
    formula <- as.formula(paste("~", paste(match_scoring_columns, collapse = " + ")))
    m <- problink_em(formula, data = pairs)
    pairs <- predict(m, pairs, add = TRUE)

    # pairs carries only row indices into the two year slices; recover the ids.
    pairs[, `:=`(
      x_local_id = linkexample1$local_id[.x],
      y_local_id = linkexample2$local_id[.y]
    )]

    # One-to-one assignment of the highest-weight pairs above panel_weight_threshold.
    best_pairs <- select_n_to_m(
      pairs,
      threshold = panel_weight_threshold,
      score = "weights",
      var = "match",
      n = 1,
      m = 1
    )

    best_pairs <- best_pairs[match == TRUE]

    pairs_list[[paste0(year1, "_", year2)]] <- best_pairs

    cat("    Found", format(nrow(best_pairs), big.mark = ","), "matches\n")

    rm(pairs)
    gc(verbose = FALSE)
  }

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

# Words appearing in most polling station names and addresses (e.g. "escola municipal"),
# so useless as blocking keys: articles, school and street terms, and their abbreviations.
PORTUGUESE_STOPWORDS <- c(
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

# Uppercase tokens of each string, minus stopwords and short words, deduplicated.
extract_significant_words <- function(text, min_word_length = 3) {
  if (length(text) == 0) {
    return(list())
  }

  text <- toupper(as.character(text))
  text[is.na(text)] <- ""

  word_lists <- strsplit(gsub("[[:punct:]]", " ", text), "\\s+")

  lapply(word_lists, function(words) {
    words <- words[words != ""]

    words <- words[nchar(words) >= min_word_length]

    words <- words[!words %in% PORTUGUESE_STOPWORDS]

    unique(words)
  })
}

# Block on municipality, then drop pairs sharing no significant word in name or address.
# This cuts comparisons by 90%+ while keeping recall high, since two records for the same
# station almost always share a word.
create_two_level_blocked_pairs <- function(
  data1,
  data2,
  municipality_col = "cod_localidade_ibge",
  name_col = "normalized_name",
  addr_col = "normalized_addr",
  fallback_on_empty = TRUE,
  min_words_threshold = 2
) {
  pairs <- pair_blocking(data1, data2, municipality_col)

  if (nrow(pairs) == 0) {
    return(pairs)
  }

  x_indices <- pairs$.x
  y_indices <- pairs$.y

  # A record appears in many pairs; tokenize each one only once.
  unique_x <- unique(x_indices)
  unique_y <- unique(y_indices)

  x_name_words <- extract_significant_words(data1[[name_col]][unique_x])
  x_addr_words <- extract_significant_words(data1[[addr_col]][unique_x])
  y_name_words <- extract_significant_words(data2[[name_col]][unique_y])
  y_addr_words <- extract_significant_words(data2[[addr_col]][unique_y])

  x_all_words <- mapply(function(n, a) unique(c(n, a)), x_name_words, x_addr_words, SIMPLIFY = FALSE)
  y_all_words <- mapply(function(n, a) unique(c(n, a)), y_name_words, y_addr_words, SIMPLIFY = FALSE)

  x_lookup <- match(x_indices, unique_x)
  y_lookup <- match(y_indices, unique_y)

  keep_pair <- logical(nrow(pairs))
  no_words_count <- 0
  no_match_count <- 0

  for (i in seq_len(nrow(pairs))) {
    x_words <- x_all_words[[x_lookup[i]]]
    y_words <- y_all_words[[y_lookup[i]]]

    # Too few words to judge (stopword filtering can strip a name bare): keep the pair.
    if (length(x_words) == 0 || length(y_words) == 0) {
      keep_pair[i] <- fallback_on_empty
      if (fallback_on_empty) no_words_count <- no_words_count + 1
    } else if (length(x_words) < min_words_threshold || length(y_words) < min_words_threshold) {
      keep_pair[i] <- TRUE
      no_words_count <- no_words_count + 1
    } else {
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

# Map election sections to panel IDs, so users can join section-level election results to
# the panel without going through local_id themselves.
create_section_panel_mapping <- function(secc_loc_map, geocoded_locais, panel_ids) {
  cat("Creating section-to-panel mapping...\n")

  if (nrow(secc_loc_map) == 0) {
    stop("create_section_panel_mapping(): empty section-location mapping.")
  }
  if (nrow(geocoded_locais) == 0) {
    stop("create_section_panel_mapping(): empty geocoded locations.")
  }
  if (nrow(panel_ids) == 0) {
    stop("create_section_panel_mapping(): empty panel IDs.")
  }

  standardize_column_names(secc_loc_map, inplace = TRUE)
  standardize_column_names(geocoded_locais, inplace = TRUE)
  standardize_column_names(panel_ids, inplace = TRUE)

  cat("Input data sizes:\n")
  cat("  Sections:", format(nrow(secc_loc_map), big.mark = ","), "\n")
  cat("  Geocoded locations:", format(nrow(geocoded_locais), big.mark = ","), "\n")
  cat("  Panel IDs:", format(nrow(panel_ids), big.mark = ","), "\n")

  # cd_localidade_tse belongs in the key: nr_local_votacao is reused across municipalities.
  geocoded_locations <- unique(geocoded_locais[, .(nr_zona, nr_local_votacao, ano, cd_localidade_tse, local_id)])

  cat("  Unique geocoded locations:", format(nrow(geocoded_locations), big.mark = ","), "\n")

  cat("Joining sections with geocoded locations...\n")

  sections_with_local_id <- merge(
    secc_loc_map,
    geocoded_locations,
    by = c("nr_zona", "nr_local_votacao", "ano", "cd_localidade_tse"),
    all.x = TRUE
  )

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

  cat("Joining with panel IDs...\n")

  sections_with_local_id_clean <- sections_with_local_id[!is.na(local_id)]

  final_mapping <- merge(
    sections_with_local_id_clean,
    panel_ids,
    by = "local_id",
    all.x = TRUE
  )

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

  final_clean <- final_mapping[!is.na(panel_id)]

  output_columns <- c("nr_secao", "nr_zona", "nr_local_votacao", "ano", "estado_abrev", "nm_localidade", "panel_id")
  available_columns <- intersect(output_columns, colnames(final_clean))

  if (length(available_columns) < length(output_columns)) {
    missing_cols <- setdiff(output_columns, available_columns)
    warning("Missing columns in section-panel output: ", paste(missing_cols, collapse = ", "))
  }

  result <- final_clean[, .SD, .SDcols = available_columns]

  cat("Validating final mapping...\n")

  duplicate_sections <- result[, .N, by = .(nr_secao, nr_zona, ano, estado_abrev)][N > 1]
  if (nrow(duplicate_sections) > 0) {
    warning(nrow(duplicate_sections), " duplicate section identifiers found")
  }

  panel_distribution <- result[, .N, by = panel_id][order(-N)]
  max_sections_per_panel <- max(panel_distribution$N)
  cat("  Max sections per panel:", max_sections_per_panel, "\n")

  if (max_sections_per_panel > 500) {
    cat("Warning: Some panels have >500 sections. This may indicate data quality issues.\n")
  }

  cat("\nFinal mapping summary:\n")
  cat("  Total records:", format(nrow(result), big.mark = ","), "\n")
  cat("  Unique sections:", format(nrow(result[, .(nr_secao, nr_zona, ano, estado_abrev)]), big.mark = ","), "\n")
  cat("  Unique panels:", format(length(unique(result$panel_id)), big.mark = ","), "\n")
  cat("  Years covered:", paste(sort(unique(result$ano)), collapse = ", "), "\n")
  cat("  States covered:", length(unique(result$estado_abrev)), "\n")

  setorder(result, ano, estado_abrev, nr_zona, nr_local_votacao, nr_secao)

  cat("Section-to-panel mapping created successfully!\n\n")

  result
}
