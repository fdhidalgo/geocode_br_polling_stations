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

    addr <- clean_cnefe10(
      cnefe10_file = state_file,
      muni_ids = state_muni_ids,
      tract_centroids = state_tract_centroids
    )
  } else {
    addr <- clean_cnefe22(
      cnefe22_file = state_file,
      muni_ids = state_muni_ids
    )
  }

  # Schools come out of the same in-memory pass, so the state file is read once.
  schools <- get_cnefe_schools(addr)
  message(sprintf("%s %s: extracted %s schools", year, state, format(nrow(schools), big.mark = ",")))

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
process_inep_batch <- function(municipality_batch_assignments, locais, inep_data) {
  this_batch <- inep_data$batch_id[1]
  batch_munis <- municipality_batch_assignments[
    batch_id == this_batch
  ]$cod_localidade_ibge

  data.table::setkey(inep_data, id_munic_7)
  data.table::setkey(locais, cod_localidade_ibge)

  batch_results <- collect_batch_or_stop(
    batch_munis,
    function(muni_code) {
      match_inep_muni(
        locais_muni = locais[.(muni_code), nomatch = NULL],
        inep_muni = inep_data[.(muni_code), nomatch = NULL]
      )
    },
    task_label = "INEP matching"
  )

  # A batch whose municipalities all matched nothing needs no special case:
  # rbindlist() of an empty list is an empty data.table.
  rbindlist(batch_results, use.names = TRUE, fill = TRUE)
}

# Match one batch's polling stations against the CNEFE school rows.
process_schools_cnefe_batch <- function(municipality_batch_assignments, locais, schools_cnefe) {
  this_batch <- schools_cnefe$batch_id[1]
  batch_munis <- municipality_batch_assignments[
    batch_id == this_batch
  ]$cod_localidade_ibge

  data.table::setkey(schools_cnefe, id_munic_7)
  data.table::setkey(locais, cod_localidade_ibge)

  batch_results <- collect_batch_or_stop(
    batch_munis,
    function(muni_code) {
      match_schools_cnefe_muni(
        locais_muni = locais[.(muni_code), nomatch = NULL],
        schools_cnefe_muni = schools_cnefe[.(muni_code), nomatch = NULL]
      )
    },
    task_label = "CNEFE school matching"
  )

  rbindlist(batch_results, use.names = TRUE, fill = TRUE)
}

# Geocode one batch's polling stations with geocodebr, in a single call. Unlike the other
# match_*_muni functions, geocodebr needs no per-municipality reference slice, and its cost
# is per call rather than per row -- so the batch goes to it whole.
process_geocodebr_batch <- function(batch_ids, municipality_batch_assignments, locais) {
  batch_munis <- municipality_batch_assignments[
    batch_id == batch_ids
  ]$cod_localidade_ibge

  data.table::setkey(locais, cod_localidade_ibge)

  # rbindlist so an empty batch is a zero-row table, as in the sibling batch helpers --
  # match_geocodebr() signals empty with NULL, and this is the only branched match target.
  rbindlist(list(match_geocodebr(locais[.(batch_munis), nomatch = NULL])))
}

# Street/neighborhood match batch, shared by the CNEFE and Agro CNEFE vintages.
# stbairro is this batch's slice: the union of the street and neighborhood
# aggregates, tagged by `component`. `label` names the vintage in the log and in
# any failure message.
process_stbairro_batch <- function(municipality_batch_assignments, locais, stbairro, label) {
  this_batch <- stbairro$batch_id[1]
  batch_munis <- municipality_batch_assignments[
    batch_id == this_batch
  ]$cod_localidade_ibge

  st <- stbairro[component == "st"]
  bairro <- stbairro[component == "bairro"]
  data.table::setkey(st, id_munic_7)
  data.table::setkey(bairro, id_munic_7)
  data.table::setkey(locais, cod_localidade_ibge)

  message(sprintf(
    "[Batch %d] Starting %s street/neighborhood matching for %d municipalities",
    this_batch,
    label,
    length(batch_munis)
  ))

  batch_results <- collect_batch_or_stop(
    batch_munis,
    function(muni_code) {
      locais_muni <- locais[.(muni_code), nomatch = NULL]
      st_muni <- st[.(muni_code), nomatch = NULL]
      bairro_muni <- bairro[.(muni_code), nomatch = NULL]

      # Logged before the match, not after: a municipality with a large distance
      # matrix can run for minutes, and this is the line that says which one.
      message(sprintf(
        "[Batch %d] Processing municipality %s: %d polling stations, %d streets, %d neighborhoods",
        this_batch,
        muni_code,
        nrow(locais_muni),
        nrow(st_muni),
        nrow(bairro_muni)
      ))

      match_stbairro_muni(locais_muni, st_muni, bairro_muni)
    },
    task_label = sprintf("%s street/neighborhood matching", label)
  )

  message(sprintf(
    "[Batch %d] Completed with %d total matches from %d municipalities",
    this_batch,
    sum(vapply(batch_results, nrow, integer(1))),
    length(batch_results)
  ))

  rbindlist(batch_results, use.names = TRUE, fill = TRUE)
}

# Size-balanced batch assignment for the polling-station municipalities, so the
# pipeline branches over a few hundred batches instead of thousands of
# per-municipality tasks. Dev mode uses smaller batches (two states only).
build_municipality_batches <- function(locais, dev_mode) {
  muni_df <- locais[, .(size = .N), by = .(cod_localidade_ibge)]
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

# Column names and order of the published geocoded file. Internally the pipeline
# names the recommended coordinate final_long/final_lat (to disambiguate from
# pred_*/tse_*); the published file calls them long/lat. conf_dist_km replaced the
# 0.141-era pred_dist, which measured something different -- see the README.
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
  "conf_dist_km",
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
    conf_dist_km,
    tse_long,
    tse_lat,
    long = final_long,
    lat = final_lat
  )]
}

# Directory the pipeline writes a given kind of output to, created if absent.
# Dev runs cover only AC/RR, so they write to a dev/ subdirectory and can never
# replace a released file. Nested inside the production directory because the
# worktree seed script clones output/ and reports/ whole.
run_output_dir <- function(base, dev_mode) {
  dir <- if (dev_mode) file.path(base, "dev") else base
  dir.create(dir, showWarnings = FALSE, recursive = TRUE)
  dir
}

# Destination for one written output file.
export_path <- function(filename, dev_mode) {
  file.path(run_output_dir("output", dev_mode), filename)
}

# Write the published geocoded file and return its path. gates is a
# dependency-only argument: passing the stage-validation targets makes the
# write depend on them, so a validation failure stops the export.
export_geocoded_locais <- function(geocoded_locais, gates, dev_mode) {
  path <- export_path("geocoded_polling_stations.csv.gz", dev_mode)
  fwrite(to_geocoded_export_schema(geocoded_locais), path)
  path
}

# Write the published panel-ID file and return its path. gates as above.
export_panel_ids <- function(panel_ids, gates, dev_mode) {
  path <- export_path("panel_ids.csv.gz", dev_mode)
  fwrite(panel_ids, path)
  path
}

# Write the section-to-panel mapping and return its path.
export_section_panel_mapping <- function(section_panel_mapping, dev_mode) {
  path <- export_path("section_panel_mapping.csv.gz", dev_mode)
  fwrite(section_panel_mapping, path)
  path
}
