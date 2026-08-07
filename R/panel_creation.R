## Temporal panel identifiers linking polling stations across election years, via
## Fellegi-Sunter record linkage blocked by municipality.

library(data.table)
library(reclin2)
library(stringr)

# The fields a candidate pair is scored on when linking a station to its counterpart in
# the next election year.
PANEL_SCORING_COLUMNS <- c("normalized_name", "normalized_addr")

# Extend the panel with one year transition's matched pairs.
process_year_pairs <- function(panel, best_pairs, year_from, year_to) {
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

  standardize_column_names(best_pairs)

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

# Attach one coordinate per panel to its member station-years. geocoded_locais is the full
# geocoded output, not the TSE-only table, so panels whose years predate TSE ground truth
# still get the model's coordinate and its pred_dist.
make_panel_ids <- function(panel_ids_combined, geocoded_locais) {
  # geocoded_locais is deliberately not standardized: an inplace rename would corrupt the
  # shared in-memory object other consumers (export, release gates) read.
  standardize_column_names(panel_ids_combined)

  # Attach each station-year's final coordinate and predicted error. pred_dist is 0 for
  # TSE-covered rows, so ordering by it puts ground-truth coordinates ahead of model ones.
  panel_ids <- geocoded_locais[
    panel_ids_combined,
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
      standardize_column_names(best_pairs)
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
make_panel_1block <- function(block) {
  standardize_column_names(block)

  years <- sort(unique(block$ano))
  cat("Processing block with", nrow(block), "rows across", length(years), "years\n")

  pairs_list <- select_best_pairs_by_year(block, years)

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
create_panel_municipality_batches <- function(locais, target_batch_size = 5000) {
  muni_counts <- locais[
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
process_panel_ids_municipality_batch <- function(locais, municipality_batch) {
  muni_codes <- municipality_batch$cod_localidade_ibge

  batch_data <- locais[cod_localidade_ibge %in% muni_codes]

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

      if (uniqueN(muni_data$ano) < 2) {
        cat("  Insufficient years for panel creation\n")
        return(NULL)
      }

      result <- make_panel_1block(muni_data)

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
select_best_pairs_by_year <- function(data, years) {
  pairs_list <- list()

  standardize_column_names(data)

  years <- sort(years)

  # Slice once per year, keeping only the columns used, to avoid repeated filtering.
  year_data <- lapply(years, function(y) {
    subset <- data[ano == y]
    keep_cols <- c("local_id", "ano", "sg_uf", "cod_localidade_ibge", PANEL_SCORING_COLUMNS)
    subset[, .SD, .SDcols = intersect(names(subset), keep_cols)]
  })
  names(year_data) <- as.character(years)

  for (i in seq_along(years)[-length(years)]) {
    year1 <- years[i]
    year2 <- years[i + 1]

    linkexample1 <- year_data[[as.character(year1)]]
    linkexample2 <- year_data[[as.character(year2)]]

    cat("  Processing year pair:", year1, "->", year2, "(", nrow(linkexample1), "x", nrow(linkexample2), "records)\n")

    pairs <- create_two_level_blocked_pairs(linkexample1, linkexample2)

    if (nrow(pairs) == 0) {
      cat("    No pairs found after blocking\n")
      next
    }

    cat("    Comparing", format(nrow(pairs), big.mark = ","), "pairs\n")

    pairs <- compare_pairs(
      pairs,
      on = PANEL_SCORING_COLUMNS,
      default_comparator = cmp_jarowinkler(0.9),
      inplace = TRUE
    )

    match_scoring_columns <- paste0("match_", PANEL_SCORING_COLUMNS)
    setnames(pairs, PANEL_SCORING_COLUMNS, match_scoring_columns)

    # Fellegi-Sunter weights, with m/u probabilities estimated by EM on these pairs.
    formula <- as.formula(paste("~", paste(match_scoring_columns, collapse = " + ")))
    m <- problink_em(formula, data = pairs)
    pairs <- predict(m, pairs, add = TRUE)

    # pairs carries only row indices into the two year slices; recover the ids.
    pairs[, `:=`(
      x_local_id = linkexample1$local_id[.x],
      y_local_id = linkexample2$local_id[.y]
    )]

    # One-to-one assignment of the pairs whose Fellegi-Sunter weight favours a match at
    # all. Whether a stricter threshold (~0.5) would be better is an open question.
    best_pairs <- select_n_to_m(
      pairs,
      threshold = 0,
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
extract_significant_words <- function(text) {
  if (length(text) == 0) {
    return(list())
  }

  text <- toupper(as.character(text))
  text[is.na(text)] <- ""

  word_lists <- strsplit(gsub("[[:punct:]]", " ", text), "\\s+")

  lapply(word_lists, function(words) {
    words <- words[words != ""]

    words <- words[nchar(words) >= 3]

    words <- words[!words %in% PORTUGUESE_STOPWORDS]

    unique(words)
  })
}

# Block on municipality, then drop pairs sharing no significant word in name or address.
# This cuts comparisons by 90%+ while keeping recall high, since two records for the same
# station almost always share a word.
create_two_level_blocked_pairs <- function(data1, data2) {
  # A record with fewer than this many significant words is too thin to judge on word
  # overlap (stopword filtering can strip a name bare), so its pairs are all kept.
  min_words <- 2

  pairs <- pair_blocking(data1, data2, "cod_localidade_ibge")

  if (nrow(pairs) == 0) {
    return(pairs)
  }

  x_indices <- pairs$.x
  y_indices <- pairs$.y

  # A record appears in many pairs; tokenize each one only once. Name and address are
  # tokenized together, since only the union of their words is ever used.
  unique_x <- unique(x_indices)
  unique_y <- unique(y_indices)

  scored_text <- function(data, rows) {
    do.call(paste, data[rows, .SD, .SDcols = PANEL_SCORING_COLUMNS])
  }
  x_all_words <- extract_significant_words(scored_text(data1, unique_x))
  y_all_words <- extract_significant_words(scored_text(data2, unique_y))

  x_thin <- lengths(x_all_words) < min_words
  y_thin <- lengths(y_all_words) < min_words

  # Word -> record tables driving the overlap test. Municipality rides along as part of the
  # join key, so the join can only produce pairs municipality blocking already allows —
  # without it a word common across the state would cross-join every record holding it.
  # Thin records are left out: their pairs are kept whatever the overlap.
  word_table <- function(word_lists, thin, muni_by_slot) {
    slot <- rep.int(seq_along(word_lists), lengths(word_lists))
    keep <- !thin[slot]
    data.table(
      slot = slot[keep],
      word = unlist(word_lists, use.names = FALSE)[keep],
      muni = muni_by_slot[slot[keep]]
    )
  }
  x_words <- word_table(x_all_words, x_thin, data1$cod_localidade_ibge[unique_x])
  y_words <- word_table(y_all_words, y_thin, data2$cod_localidade_ibge[unique_y])
  setnames(x_words, "slot", "x_slot")
  setnames(y_words, "slot", "y_slot")

  # "Shares at least one significant word" as a set intersection over the word tables,
  # rather than a scan over the (millions of) blocked pairs.
  shared <- unique(
    merge(x_words, y_words, by = c("muni", "word"), allow.cartesian = TRUE)[, .(x_slot, y_slot)]
  )

  pair_slots <- data.table(
    x_slot = match(x_indices, unique_x),
    y_slot = match(y_indices, unique_y),
    shares_word = FALSE
  )
  pair_slots[shared, shares_word := TRUE, on = c("x_slot", "y_slot")]

  thin_pair <- x_thin[pair_slots$x_slot] | y_thin[pair_slots$y_slot]
  keep_pair <- thin_pair | pair_slots$shares_word

  no_words_count <- sum(thin_pair)
  no_match_count <- sum(!keep_pair)

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

  standardize_column_names(secc_loc_map)
  standardize_column_names(geocoded_locais)
  standardize_column_names(panel_ids)

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
    stop(sprintf(
      paste0(
        "Section-to-location join matched only %.1f%% of sections (expected >=80%%); ",
        "the section and polling-station keys have drifted apart."
      ),
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
    stop(sprintf(
      paste0(
        "Section-to-panel join matched only %.1f%% of located sections (expected >=90%%); ",
        "panel ID coverage is incomplete."
      ),
      final_success_rate * 100
    ))
  }

  final_clean <- final_mapping[!is.na(panel_id)]

  # cd_localidade_tse ships too, for the same reason it is in the key above: RS zona 20
  # alone spans 14 municipalities, each restarting its section numbers from 1.
  output_columns <- c(
    "nr_secao",
    "nr_zona",
    "nr_local_votacao",
    "ano",
    "estado_abrev",
    "cd_localidade_tse",
    "nm_localidade",
    "panel_id"
  )
  missing_cols <- setdiff(output_columns, colnames(final_clean))
  if (length(missing_cols) > 0) {
    stop(
      "create_section_panel_mapping(): missing output column(s): ",
      paste(missing_cols, collapse = ", ")
    )
  }

  # The source lists some stations twice under different name/address spellings; those rows
  # are identical once the descriptive columns are dropped.
  result <- unique(final_clean[, ..output_columns])

  cat("Validating final mapping...\n")

  section_key <- c("nr_secao", "nr_zona", "ano", "estado_abrev", "cd_localidade_tse")

  # A section votes at one polling place per election, but the source carries no round
  # column, so a section relocated between rounds appears twice with nothing to say which
  # assignment belongs to which round. Publishing two panels for one section is worse than
  # publishing neither, so these are dropped -- but only where the defect is known to live:
  # RS 2012, 39 sections out of 4.33M rows. Ambiguity anywhere else is a new problem.
  ambiguous <- result[, .N, by = section_key][N > 1][, ..section_key]
  if (nrow(ambiguous) > 0) {
    unexpected <- ambiguous[!(estado_abrev == "RS" & ano == 2012)]
    if (nrow(unexpected) > 0) {
      stop(
        "create_section_panel_mapping(): ",
        nrow(unexpected),
        " sections outside RS 2012 sit at more than one polling place; ",
        "first: ",
        paste(unexpected[1], collapse = " ")
      )
    }
    cat("  Dropping", nrow(ambiguous), "RS 2012 sections listed at more than one polling place\n")
    result <- result[!ambiguous, on = section_key]
  }

  panel_distribution <- result[, .N, by = panel_id][order(-N)]
  max_sections_per_panel <- max(panel_distribution$N)
  cat("  Max sections per panel:", max_sections_per_panel, "\n")

  if (max_sections_per_panel > 500) {
    cat("Warning: Some panels have >500 sections. This may indicate data quality issues.\n")
  }

  cat("\nFinal mapping summary:\n")
  cat("  Total records:", format(nrow(result), big.mark = ","), "\n")
  cat("  Unique panels:", format(length(unique(result$panel_id)), big.mark = ","), "\n")
  cat("  Years covered:", paste(sort(unique(result$ano)), collapse = ", "), "\n")
  cat("  States covered:", length(unique(result$estado_abrev)), "\n")

  setorder(result, ano, estado_abrev, nr_zona, nr_local_votacao, nr_secao)

  cat("Section-to-panel mapping created successfully!\n\n")

  result
}
