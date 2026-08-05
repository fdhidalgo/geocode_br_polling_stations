## Utility helpers used throughout the pipeline.

library(data.table)

# Run fn(item) over items, recording per-item errors and stopping once at the
# end with every failure named. A NULL result is a legitimate empty item, not a
# failure, and is dropped from the returned list.
collect_batch_or_stop <- function(items, fn, task_label) {
  results <- lapply(items, function(item) {
    tryCatch(
      fn(item),
      error = function(e) {
        structure(
          list(item = item, message = conditionMessage(e)),
          class = "batch_item_failure"
        )
      }
    )
  })

  failures <- Filter(function(x) inherits(x, "batch_item_failure"), results)
  if (length(failures) > 0) {
    n <- length(failures)
    msgs <- vapply(
      failures,
      function(f) sprintf("  %s: %s", f$item, f$message),
      character(1)
    )
    stop(sprintf(
      "%s failed for %d %s:\n%s",
      task_label,
      n,
      ngettext(n, "municipality", "municipalities"),
      paste(msgs, collapse = "\n")
    ))
  }

  Filter(Negate(is.null), results)
}

# Keep only the development-mode states; NULL dev_states means production, which
# passes all data through. state_col names the table's state-abbreviation column,
# which differs by source (estado_abrev, sg_uf, uf).
filter_by_dev_mode <- function(data, dev_states, state_col) {
  if (is.null(dev_states)) {
    return(data)
  }

  # A missing filter column would silently return unfiltered data, running the
  # full pipeline in dev mode.
  if (!state_col %in% names(data)) {
    stop(sprintf("filter_by_dev_mode(): column '%s' not found in data.", state_col))
  }

  data[get(state_col) %in% dev_states]
}

# Restrict a municipality-keyed table to the municipalities this run processes. muni_col
# holds 7-digit IBGE codes, or (for census tracts) a longer code whose first 7 digits are
# the municipality. Production passes everything through rather than filtering on the full
# crosswalk, which would silently drop the two muni_shp codes the crosswalk lacks.
filter_to_run_munis <- function(data, muni_col, muni_ids, dev_mode) {
  if (!dev_mode) {
    return(data)
  }

  if (!muni_col %in% names(data)) {
    stop(sprintf("filter_to_run_munis(): column '%s' not found in data.", muni_col))
  }

  keys <- substr(as.character(data[[muni_col]]), 1, 7)
  data[keys %in% as.character(muni_ids$id_munic_7), ]
}

# Drop Brasília (DF), which holds municipal elections in different years from
# the other states.
apply_brasilia_filters <- function(data) {
  data[sg_uf != "DF"]
}

# Read and clean one state's CNEFE file in memory and return only the small
# summaries the pipeline consumes: street- and neighborhood-level coordinate
# aggregates plus the school rows. The full cleaned address table has no other
# consumer, so it is never returned or persisted.
process_cnefe_state <- function(state_file, year, muni_ids, tract_centroids = NULL) {
  # The state is derived from the filename (e.g. "cnefe_2010_AC.csv.gz") rather
  # than passed separately, so file content stays the single tracked dependency
  # and a re-downloaded state file invalidates exactly its branch.
  state <- cnefe_state_from_file(state_file, year)

  state_muni_ids <- muni_ids[estado_abrev == state]
  if (nrow(state_muni_ids) == 0L) {
    stop(sprintf(
      "process_cnefe_state(): no muni_ids rows for state '%s' derived from %s",
      state,
      state_file
    ))
  }

  if (year == 2010) {
    state_codes <- unique(substr(
      as.character(state_muni_ids$id_munic_7),
      1,
      2
    ))
    state_tract_centroids <- tract_centroids[
      substr(setor_code, 1, 2) %in% state_codes
    ]

    # Schools come out of the same in-memory pass, so the state file is read once.
    cleaned <- clean_cnefe10(
      cnefe10_file = state_file,
      muni_ids = state_muni_ids,
      tract_centroids = state_tract_centroids,
      extract_schools = TRUE
    )
    addr <- cleaned$data
    schools <- cleaned$schools
  } else {
    addr <- clean_cnefe22(
      cnefe22_file = state_file,
      muni_ids = state_muni_ids
    )
    schools <- get_cnefe22_schools(addr)
  }

  list(
    st = aggregate_cnefe_coords(addr, "norm_street"),
    bairro = aggregate_cnefe_coords(addr, "norm_bairro"),
    schools = schools
  )
}

# Collapse cleaned CNEFE rows to one row per (id_munic_7, group_col) with the
# median coordinate and group size n. Singleton groups are deliberately kept
# here; combine_cnefe_state_component() drops them after its duplicate check.
aggregate_cnefe_coords <- function(addr, group_col) {
  addr[,
    .(
      long = median(cnefe_long, na.rm = TRUE),
      lat = median(cnefe_lat, na.rm = TRUE),
      n = .N
    ),
    by = c("id_munic_7", group_col)
  ]
}

# Row-bind one component ("st", "bairro", or "schools") across the per-state
# CNEFE results. For the aggregates, unique_key asserts that no key appears in
# more than one state slice -- a duplicate means a municipality spanning two
# state files or a mis-assigned file, which is an error, not a row to merge.
combine_cnefe_state_component <- function(state_results, component, unique_key = NULL) {
  combined <- rbindlist(
    lapply(state_results, `[[`, component),
    use.names = TRUE,
    fill = TRUE
  )

  # anyDuplicated() is a single pass with a 0L fast path, so the expensive
  # per-key count table is only built on the rare error path.
  if (!is.null(unique_key) && anyDuplicated(combined, by = unique_key) > 0L) {
    dup_keys <- combined[, .N, by = unique_key][N > 1]
    example <- paste(
      unlist(dup_keys[1, ..unique_key]),
      collapse = ", "
    )
    stop(sprintf(
      paste0(
        "combine_cnefe_state_component(): %d '%s' key(s) duplicated across ",
        "state slices (e.g. %s). A municipality spanning two state files or ",
        "a mis-assigned state file produces this."
      ),
      nrow(dup_keys),
      component,
      example
    ))
  }

  # Singletons are dropped only after the duplicate check, so a key split
  # one-and-one across two state files cannot be filtered away unseen. Aggregate
  # components carry `n`; `schools` does not and passes through whole.
  if ("n" %in% names(combined)) {
    combined <- combined[n > 1]
  }

  combined
}

# Attach each reference row's batch_id plus a contiguous tar_group, so a match
# target can map() over the groups and receive only its batch's slice instead of
# the whole national reference table. tar_group is the dense rank of batch_id,
# so row order (and therefore match tie-breaks) is untouched.
make_ref_batch_groups <- function(ref, municipality_batch_assignments, copy = TRUE) {
  if (copy) {
    ref <- data.table::copy(ref)
  }
  ref[
    municipality_batch_assignments,
    batch_id := i.batch_id,
    on = c(id_munic_7 = "cod_localidade_ibge")
  ]
  # Inner-join semantics: a reference municipality with no polling-station batch
  # is never matched, so drop it rather than form an empty group for it.
  ref <- ref[!is.na(batch_id)]
  if (nrow(ref) == 0L) {
    stop(
      "make_ref_batch_groups(): reference has no municipality overlap with the batch ",
      "assignments, so it can contribute no matches at all. The reference's ",
      "id_munic_7 codes and the polling stations' cod_localidade_ibge disagree."
    )
  }
  ref[, tar_group := data.table::frank(batch_id, ties.method = "dense")]
  ref[]
}

# Group the street and neighborhood aggregates into per-batch slices. They are
# unioned into one table (tagged by `component`) first so both travel as a single
# grouped stem and cannot fall out of alignment when a batch has streets but no
# neighborhoods, or vice versa.
make_stbairro_batch_groups <- function(ref_st, ref_bairro, municipality_batch_assignments) {
  # rbindlist() returns a fresh table, so neither input is mutated and the
  # defensive copy inside make_ref_batch_groups() is unnecessary.
  ref <- data.table::rbindlist(
    list(st = ref_st, bairro = ref_bairro),
    use.names = TRUE,
    fill = TRUE,
    idcol = "component"
  )
  make_ref_batch_groups(ref, municipality_batch_assignments, copy = FALSE)
}

# Match one batch's polling stations against the INEP school catalog. inep_data
# is this batch's slice of the catalog; municipalities come from
# municipality_batch_assignments, which fixes the combined row order.
process_inep_batch <- function(municipality_batch_assignments, locais_filtered, inep_data) {
  this_batch <- inep_data$batch_id[1]
  batch_munis <- municipality_batch_assignments[
    batch_id == this_batch
  ]$cod_localidade_ibge

  data.table::setkey(inep_data, id_munic_7)
  data.table::setkey(locais_filtered, cod_localidade_ibge)

  batch_results <- lapply(batch_munis, function(muni_code) {
    match_inep_muni(
      locais_muni = locais_filtered[.(muni_code), nomatch = NULL],
      inep_muni = inep_data[.(muni_code), nomatch = NULL]
    )
  })

  batch_results <- batch_results[!sapply(batch_results, is.null)]
  if (length(batch_results) > 0) {
    rbindlist(batch_results, use.names = TRUE, fill = TRUE)
  } else {
    data.table()
  }
}

# Match one batch's polling stations against the CNEFE school rows.
process_schools_cnefe_batch <- function(municipality_batch_assignments, locais_filtered, schools_cnefe) {
  this_batch <- schools_cnefe$batch_id[1]
  batch_munis <- municipality_batch_assignments[
    batch_id == this_batch
  ]$cod_localidade_ibge

  data.table::setkey(schools_cnefe, id_munic_7)
  data.table::setkey(locais_filtered, cod_localidade_ibge)

  batch_results <- lapply(batch_munis, function(muni_code) {
    match_schools_cnefe_muni(
      locais_muni = locais_filtered[.(muni_code), nomatch = NULL],
      schools_cnefe_muni = schools_cnefe[.(muni_code), nomatch = NULL]
    )
  })

  batch_results <- batch_results[!sapply(batch_results, is.null)]
  if (length(batch_results) > 0) {
    rbindlist(batch_results, use.names = TRUE, fill = TRUE)
  } else {
    data.table()
  }
}

# Geocode one batch's polling stations with geocodebr.
process_geocodebr_batch <- function(batch_ids, municipality_batch_assignments, locais_filtered) {
  batch_munis <- municipality_batch_assignments[
    batch_id == batch_ids
  ]$cod_localidade_ibge

  data.table::setkey(locais_filtered, cod_localidade_ibge)

  # A NULL result (no polling stations, or no geocoding hits) is a legitimate
  # empty case and is filtered out; a municipality that errors is reported at
  # batch end rather than silently dropped.
  results <- collect_batch_or_stop(
    batch_munis,
    function(muni_code) {
      match_geocodebr_muni(locais_filtered[.(muni_code), nomatch = NULL])
    },
    task_label = "geocodebr matching"
  )

  if (length(results) > 0) {
    rbindlist(results, use.names = TRUE, fill = TRUE)
  } else {
    data.table()
  }
}

# Street/neighborhood match batch, shared by the CNEFE and Agro CNEFE vintages.
# stbairro is this batch's slice: the union of the street and neighborhood
# aggregates, tagged by `component`. `label` names the vintage in the log only.
process_stbairro_batch <- function(
  municipality_batch_assignments,
  locais_filtered,
  stbairro,
  label
) {
  this_batch <- stbairro$batch_id[1]
  batch_munis <- municipality_batch_assignments[
    batch_id == this_batch
  ]$cod_localidade_ibge

  st <- stbairro[component == "st"]
  bairro <- stbairro[component == "bairro"]
  data.table::setkey(st, id_munic_7)
  data.table::setkey(bairro, id_munic_7)
  data.table::setkey(locais_filtered, cod_localidade_ibge)

  message(sprintf(
    "[Batch %d] Starting %s street/neighborhood matching for %d municipalities",
    this_batch,
    label,
    length(batch_munis)
  ))

  batch_results <- lapply(seq_along(batch_munis), function(i) {
    muni_code <- batch_munis[i]

    locais_muni <- locais_filtered[.(muni_code), nomatch = NULL]
    st_muni <- st[.(muni_code), nomatch = NULL]
    bairro_muni <- bairro[.(muni_code), nomatch = NULL]

    message(sprintf(
      "[Batch %d - %d/%d] Processing municipality %s: %d polling stations, %d streets, %d neighborhoods",
      this_batch,
      i,
      length(batch_munis),
      muni_code,
      nrow(locais_muni),
      nrow(st_muni),
      nrow(bairro_muni)
    ))

    result <- match_stbairro_muni(locais_muni, st_muni, bairro_muni)

    if (!is.null(result)) {
      message(sprintf(
        "[Batch %d - %d/%d] Completed municipality %s: %d matches",
        this_batch,
        i,
        length(batch_munis),
        muni_code,
        nrow(result)
      ))
    }

    result
  })

  batch_results <- batch_results[!sapply(batch_results, is.null)]

  total_matches <- if (length(batch_results) > 0) {
    sum(sapply(batch_results, nrow))
  } else {
    0
  }

  message(sprintf(
    "[Batch %d] Completed with %d total matches from %d municipalities",
    this_batch,
    total_matches,
    length(batch_results)
  ))

  if (length(batch_results) > 0) {
    rbindlist(batch_results, use.names = TRUE, fill = TRUE)
  } else {
    data.table()
  }
}

# Size-balanced batch assignment for the polling-station municipalities, so the
# pipeline branches over a few hundred batches instead of thousands of
# per-municipality tasks. Dev mode uses smaller batches (two states only).
build_municipality_batches <- function(locais_filtered, dev_mode) {
  muni_df <- locais_filtered[, .(size = .N), by = .(cod_localidade_ibge)]
  # Sort by size with municipality code as tiebreak, then round-robin so the
  # large municipalities spread evenly across batches.
  data.table::setorder(muni_df, -size, cod_localidade_ibge)
  batch_size <- if (dev_mode) 5 else 15
  n_batches <- ceiling(nrow(muni_df) / batch_size)
  muni_df[, batch_id := rep_len(seq_len(n_batches), .N)]
  result <- muni_df[, .(cod_localidade_ibge, batch_id)]

  batch_stats <- result[, .N, by = batch_id]
  message(sprintf(
    "Created %d batches for %d municipalities (min: %d, max: %d, avg: %.1f per batch)",
    n_batches,
    nrow(result),
    min(batch_stats$N),
    max(batch_stats$N),
    mean(batch_stats$N)
  ))

  result
}

# Row-bind the per-batch results of a matching stage and assert the stage
# produced something: an empty combined table means matching broke, not data.
combine_match_batches <- function(batches, table_name) {
  out <- rbindlist(batches, use.names = TRUE, fill = TRUE)
  if (nrow(out) == 0L) {
    stop(sprintf("%s: matching stage produced no rows", table_name))
  }
  out
}

# Column names and order of the published geocoded file, preserved from the
# 0.141 release so downstream code that reads it keeps working. Internally the
# pipeline names the recommended coordinate final_long/final_lat (to
# disambiguate from pred_*/tse_*); the published file calls them long/lat.
GEOCODED_EXPORT_SCHEMA <- c(
  "local_id",
  "ano",
  "sg_uf",
  "cd_localidade_tse",
  "cod_localidade_ibge",
  "nr_zona",
  "nr_locvot",
  "nr_cep",
  "nm_localidade",
  "nm_locvot",
  "ds_endereco",
  "ds_bairro",
  "pred_long",
  "pred_lat",
  "pred_dist",
  "tse_long",
  "tse_lat",
  "long",
  "lat"
)

# Map the internal geocoded table to the published schema: rename
# final_long/final_lat to long/lat and reorder to GEOCODED_EXPORT_SCHEMA.
# Returns a new table; the input is not mutated.
to_geocoded_export_schema <- function(geocoded_locais) {
  as.data.table(geocoded_locais)[, .(
    local_id,
    ano,
    sg_uf,
    cd_localidade_tse,
    cod_localidade_ibge,
    nr_zona,
    nr_locvot,
    nr_cep,
    nm_localidade,
    nm_locvot,
    ds_endereco,
    ds_bairro,
    pred_long,
    pred_lat,
    pred_dist,
    tse_long,
    tse_lat,
    long = final_long,
    lat = final_lat
  )]
}

# Write the published geocoded file and return its path. gates is a
# dependency-only argument: passing the stage-validation targets makes the
# write depend on them, so a validation failure stops the export.
export_geocoded_locais <- function(geocoded_locais, gates) {
  fwrite(
    to_geocoded_export_schema(geocoded_locais),
    "./output/geocoded_polling_stations.csv.gz"
  )
  "./output/geocoded_polling_stations.csv.gz"
}

# Write the published panel-ID file and return its path. gates as above.
export_panel_ids <- function(panel_ids, gates) {
  fwrite(panel_ids, "./output/panel_ids.csv.gz")
  "./output/panel_ids.csv.gz"
}

# Write the section-to-panel mapping and return its path.
export_section_panel_mapping <- function(section_panel_mapping) {
  dir.create("output", showWarnings = FALSE)
  fwrite(section_panel_mapping, "./output/section_panel_mapping.csv.gz")
  "./output/section_panel_mapping.csv.gz"
}
