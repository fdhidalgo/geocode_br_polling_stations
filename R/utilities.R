## Utility Functions
##
## Helper functions for common operations throughout the pipeline:
## - State/municipality filtering for development mode
## - File I/O operations with compression support
## - Parallel processing configuration
## - Memory monitoring and management
## - Data export utilities

library(data.table)

# ===== FILTERING HELPERS =====
# Functions to subset data for development mode testing

# Define the null coalescing operator
`%||%` <- function(x, y) if (is.null(x)) y else x

# Run fn(item) for each item under the collect-and-stop convention (cleanup
# phase 3): a per-item error is recorded and the batch continues; at batch end,
# if any items failed, stop() with a single message naming every failure. A NULL
# result is a legitimate empty item (not a failure) and is dropped from the
# returned list. `task_label` and the unit noun shape the error message.
collect_batch_or_stop <- function(
  items,
  fn,
  task_label,
  unit_singular = "municipality",
  unit_plural = "municipalities"
) {
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
      ngettext(n, unit_singular, unit_plural),
      paste(msgs, collapse = "\n")
    ))
  }

  Filter(Negate(is.null), results)
}

filter_by_dev_mode <- function(data, dev_states, id_column = "estado_abrev") {
  # Filter data by development mode states.
  # If dev_states is NULL or empty (production), return all data.
  if (is.null(dev_states) || length(dev_states) == 0) {
    return(data)
  }

  # Fail loud when the filter column is absent (cleanup phase 3, finding H2):
  # silently returning unfiltered data would run the full pipeline in dev mode.
  if (!id_column %in% names(data)) {
    stop(sprintf("filter_by_dev_mode(): column '%s' not found in data.", id_column))
  }

  data[get(id_column) %in% dev_states]
}

filter_data_by_state <- function(data, states, state_col = "estado_abrev") {
  # Generic function to filter any data by state (data.table or data.frame).
  if (is.null(states) || length(states) == 0) {
    return(data)
  }

  # Fail loud when the state column is absent (cleanup phase 3, finding H2).
  if (!state_col %in% names(data)) {
    stop(sprintf("filter_data_by_state(): state column '%s' not found in data.", state_col))
  }

  if (is.data.table(data)) {
    data[get(state_col) %in% states]
  } else {
    data[data[[state_col]] %in% states, ]
  }
}

filter_data_by_municipalities <- function(data, muni_codes, muni_col = "id_munic_7") {
  # Filter data by municipality codes.
  if (is.null(muni_codes) || length(muni_codes) == 0) {
    return(data)
  }

  # Fail loud when the named column is absent (cleanup phase 3, finding H2). The
  # former four-way probing of alternative ID columns could filter on the wrong
  # ID system; the caller must name the column it means.
  if (!muni_col %in% names(data)) {
    stop(sprintf("filter_data_by_municipalities(): municipality column '%s' not found in data.", muni_col))
  }

  if (is.data.table(data)) {
    data[get(muni_col) %in% muni_codes]
  } else {
    data[data[[muni_col]] %in% muni_codes, ]
  }
}

apply_dev_mode_filters <- function(data, config, state_col) {
  # Single named seam for "what dev mode restricts data to", so the pipeline file
  # reads intent and there is one place to re-expand if dev filtering ever regains
  # a second dimension (it previously also filtered by municipality). In dev mode
  # it keeps only the configured development states (config$dev_states) on the
  # named state column; in production dev_states is NULL and the data passes
  # through unchanged. The caller must name the state column (cleanup phase 4; H2
  # convention).
  filter_data_by_state(data, config$dev_states, state_col)
}

apply_brasilia_filters <- function(data, remove_brasilia = TRUE, state_col = "sg_uf") {
  # Apply special filtering for Brasília (DF), which had municipal elections in
  # years that differ from other states.
  if (!remove_brasilia) {
    return(data)
  }

  # Fail loud when the named state column is absent (cleanup phase 3, finding
  # H2). The former stacked fallbacks (four state-column names, then three
  # municipality-code columns filtering on a "^53" prefix, then a warn-and-pass)
  # could silently skip the filter or use the wrong column; the caller names it.
  if (!state_col %in% names(data)) {
    stop(sprintf("apply_brasilia_filters(): state column '%s' not found in data.", state_col))
  }

  if (is.data.table(data)) {
    data[get(state_col) != "DF"]
  } else {
    data[data[[state_col]] != "DF", ]
  }
}

# ===== PIPELINE HELPERS =====

#' Process one state's CNEFE data into per-municipality aggregates
#'
#' Reads and cleans one state's CNEFE file entirely in memory and returns only
#' the small summaries the pipeline consumes: street- and neighborhood-level
#' coordinate aggregates plus the school rows. The full cleaned address table is
#' never returned (and therefore never persisted); it has no consumer other than
#' these three aggregates (spec `2026-07-partition-reference-data-spec.md`, D5).
#'
#' @param state Current state being processed
#' @param year CNEFE year (2010 or 2022)
#' @param muni_ids Municipality identifiers
#' @param tract_centroids Tract centroids (for 2010 only)
#' @return A list with `st` (street aggregates), `bairro` (neighborhood
#'   aggregates), and `schools` (school rows) for this state
#' @export
process_cnefe_state <- function(state, year, muni_ids, tract_centroids = NULL) {
  # Construct file path
  state_file <- file.path(
    "data",
    paste0("cnefe_", year),
    paste0("cnefe_", year, "_", state, ".csv.gz")
  )

  # Get municipality IDs for this state
  state_muni_ids <- muni_ids[estado_abrev == state]

  if (year == 2010) {
    # Read state data
    state_data <- fread(
      state_file,
      sep = ",",
      encoding = "UTF-8",
      verbose = FALSE,
      showProgress = FALSE
    )

    # Get tract centroids for this state
    state_codes <- unique(substr(
      as.character(state_muni_ids$id_munic_7),
      1,
      2
    ))
    state_tract_centroids <- tract_centroids[
      substr(setor_code, 1, 2) %in% state_codes
    ]

    # Clean the data, extracting schools from the same in-memory pass (the
    # duplicate 2010 schools read/clean pass is deleted per spec D5).
    cleaned <- clean_cnefe10(
      cnefe_file = state_data,
      muni_ids = state_muni_ids,
      tract_centroids = state_tract_centroids,
      extract_schools = TRUE
    )
    addr <- cleaned$data
    schools <- cleaned$schools
  } else {
    # CNEFE 2022 processing
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

#' Aggregate CNEFE coordinates to per-municipality group medians
#'
#' Collapses cleaned CNEFE rows to one row per `(id_munic_7, <group_col>)` group,
#' taking the median coordinate and the group size `n`. This is the aggregation
#' formerly applied to the combined national `cnefe10`/`cnefe22` tables; because
#' each municipality lives in exactly one state file, computing it per state and
#' row-binding the results is equivalent to aggregating the national table.
#'
#' Singleton groups (`n == 1`) are intentionally NOT dropped here: the singleton
#' drop happens in [combine_cnefe_state_component()], *after* the cross-slice
#' duplicate check, so that a key accidentally split one-and-one across two state
#' files is caught by the D6 invariant instead of being filtered away unseen.
#'
#' @param addr Cleaned CNEFE address data.table with `id_munic_7`,
#'   `cnefe_long`, `cnefe_lat`, and the grouping column
#' @param group_col Grouping column name (`"norm_street"` or `"norm_bairro"`)
#' @return A data.table with columns `id_munic_7`, `<group_col>`, `long`, `lat`,
#'   `n` (one row per group, including `n == 1` groups)
#' @export
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

#' Combine one component of the per-state CNEFE results
#'
#' Row-binds a named component (`"st"`, `"bairro"`, or `"schools"`) across the
#' per-state result lists returned by [process_cnefe_state()]. For the street and
#' neighborhood aggregates, `unique_key` asserts the spec-D6 invariant: no
#' `(id_munic_7, <key>)` may appear in more than one state slice. A duplicate is
#' exactly what a municipality spanning two state files, or a mis-assigned state
#' file, would produce, so it is a hard error rather than a silently merged row.
#'
#' The duplicate check runs on the un-thinned aggregates (singleton `n == 1`
#' groups still present), so a key split one-and-one across two state files is
#' detected here; only afterwards are singleton groups dropped (`n > 1`), matching
#' the national aggregation's `[n > 1]` filter. `schools` has no `n` column and is
#' returned in full.
#'
#' @param state_results List of per-state result lists
#' @param component Component name to extract from each per-state list
#' @param unique_key Optional key columns whose uniqueness across slices is
#'   asserted; `NULL` (the default) skips the check (used for `schools`, which is
#'   legitimately many-per-municipality)
#' @return The row-bound data.table for the requested component (aggregate
#'   components thinned to `n > 1` groups)
#' @export
combine_cnefe_state_component <- function(state_results, component, unique_key = NULL) {
  combined <- rbindlist(
    lapply(state_results, `[[`, component),
    use.names = TRUE,
    fill = TRUE
  )

  # anyDuplicated() is a single pass with a 0L fast path, so the expensive
  # per-key count table is only materialized on the rare error path (to build
  # the diagnostic), not on every combine.
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

  # Drop singleton groups only now, after the cross-slice duplicate check, so a
  # 1+1 split cannot be filtered away before the invariant sees it. Aggregate
  # components carry an `n` column; `schools` does not and passes through whole.
  if ("n" %in% names(combined)) {
    combined <- combined[n > 1]
  }

  combined
}

#' Slice a reference table into per-batch groups for Option A branching (issue #68)
#'
#' Attaches each reference row's `batch_id` (via `municipality_batch_assignments`,
#' the single source of truth for the municipality->batch map) plus a contiguous
#' `tar_group`, so a match target can `pattern = map()` over the groups with
#' `retrieval = "main"` and receive only its batch's slice instead of the whole
#' national reference table.
#'
#' The join is an update-join (it does not reorder rows) followed by an
#' inner-join drop: reference municipalities absent from the batch assignments
#' have no polling stations and are never matched, so dropping them changes no
#' match output. `tar_group` is the dense rank of `batch_id`, which is a
#' contiguous 1..N in ascending `batch_id` order (the pre-existing branch order)
#' and leaves row order untouched -- so every per-municipality slice preserves the
#' reference's original row order and therefore identical match tie-breaks.
#'
#' @param ref data.table reference table (must have `id_munic_7`).
#' @param municipality_batch_assignments data.table with `cod_localidade_ibge`,
#'   `batch_id`.
#' @param copy Copy `ref` before the in-place update-join? Defaults to `TRUE` so a
#'   real target's cached value is never mutated. Callers that already hand in a
#'   freshly allocated table (e.g. [make_stbairro_batch_groups()] via `rbindlist`)
#'   pass `FALSE` to avoid a redundant copy of a large reference.
#' @return `ref` (inner-joined to the assignments) with added `batch_id` and
#'   `tar_group` columns, intended for `iteration = "group"`.
#' @export
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
    # Degenerate case: no reference municipality overlaps the batch assignments,
    # so this reference contributes no matches at all. `pattern = map()` cannot
    # branch over an empty target, so emit one placeholder group carrying no real
    # municipality (id_munic_7 = NA). The match batch then runs once, every
    # per-municipality lookup misses, and the combined result is the same empty
    # data.table the whole-table path produced -- no crash. This is not expected
    # for any reference in normal operation (Agro CNEFE used to hit it in dev via
    # bug #75, now fixed), so the warning keeps a genuinely empty reference from
    # passing silently.
    warning(
      "make_ref_batch_groups(): reference has no municipality overlap with the ",
      "batch assignments; emitting one empty placeholder group (no matches). ",
      "Not expected in normal operation -- investigate."
    )
    ref <- ref[NA_integer_]
    ref[, batch_id := municipality_batch_assignments$batch_id[1]]
  }
  ref[, tar_group := data.table::frank(batch_id, ties.method = "dense")]
  ref[]
}

#' Slice paired street + neighborhood reference tables into per-batch groups (#68)
#'
#' Unions the street and neighborhood aggregates (tagged by a `component` column)
#' into one table before grouping, so both travel as a single grouped stem and
#' cannot fall out of group alignment -- which mapping over two separate grouped
#' targets would risk whenever a batch has streets but no neighborhoods, or vice
#' versa. Grouping semantics are those of [make_ref_batch_groups()].
#'
#' @param ref_st Street-level aggregate (`id_munic_7`, `norm_street`, `long`,
#'   `lat`, `n`).
#' @param ref_bairro Neighborhood-level aggregate (`id_munic_7`, `norm_bairro`,
#'   `long`, `lat`, `n`).
#' @param municipality_batch_assignments data.table with `cod_localidade_ibge`,
#'   `batch_id`.
#' @return Unioned table with a `component` tag (`"st"`/`"bairro"`) plus
#'   `batch_id`/`tar_group`, intended for `iteration = "group"`.
#' @export
make_stbairro_batch_groups <- function(ref_st, ref_bairro, municipality_batch_assignments) {
  # rbindlist(idcol=) tags each source with its `component` and returns a fresh
  # table, so neither input is copied or mutated. The union is already fresh,
  # hence copy = FALSE below.
  ref <- data.table::rbindlist(
    list(st = ref_st, bairro = ref_bairro),
    use.names = TRUE,
    fill = TRUE,
    idcol = "component"
  )
  make_ref_batch_groups(ref, municipality_batch_assignments, copy = FALSE)
}

#' Process INEP string matching in batches
#'
#' `inep_data` is a per-batch grouped-stem slice (Option A, issue #68): the
#' production INEP catalog is ~61 MB, so slicing it (rather than broadcasting the
#' whole table to every worker) clears the D3 size bar. Municipalities are still
#' taken from `municipality_batch_assignments` so the combined row order is
#' byte-identical to the pre-reshape pipeline.
#'
#' @param municipality_batch_assignments Batch assignments
#' @param locais_filtered Filtered polling stations
#' @param inep_data Per-batch slice of the INEP catalog
#' @return Combined match results
#' @export
process_inep_batch <- function(municipality_batch_assignments, locais_filtered, inep_data) {
  this_batch <- inep_data$batch_id[1]
  batch_munis <- municipality_batch_assignments[
    batch_id == this_batch
  ]$cod_localidade_ibge

  data.table::setkey(inep_data, id_munic_7)

  # Process all municipalities in this batch
  batch_results <- lapply(batch_munis, function(muni_code) {
    match_inep_muni(
      locais_muni = locais_filtered[cod_localidade_ibge == muni_code],
      inep_muni = inep_data[.(muni_code), nomatch = NULL]
    )
  })

  # Remove NULL results and combine
  batch_results <- batch_results[!sapply(batch_results, is.null)]
  if (length(batch_results) > 0) {
    rbindlist(batch_results, use.names = TRUE, fill = TRUE)
  } else {
    data.table()
  }
}

#' Process schools CNEFE string matching in batches
#'
#' `schools_cnefe` is a per-batch grouped-stem slice (Option A, issue #68): it
#' holds only this batch's municipalities plus a constant `batch_id` column, so
#' the whole national table never crosses to the worker. Municipalities are still
#' taken from `municipality_batch_assignments` (the source of truth) so per-batch
#' iteration order -- and thus the combined row order -- is byte-identical to the
#' pre-reshape pipeline.
#'
#' @param municipality_batch_assignments Batch assignments
#' @param locais_filtered Filtered polling stations
#' @param schools_cnefe Per-batch slice of the schools CNEFE reference
#' @return Combined match results
#' @export
process_schools_cnefe_batch <- function(municipality_batch_assignments, locais_filtered, schools_cnefe) {
  this_batch <- schools_cnefe$batch_id[1]
  batch_munis <- municipality_batch_assignments[
    batch_id == this_batch
  ]$cod_localidade_ibge

  data.table::setkey(schools_cnefe, id_munic_7)

  batch_results <- lapply(batch_munis, function(muni_code) {
    match_schools_cnefe_muni(
      locais_muni = locais_filtered[cod_localidade_ibge == muni_code],
      schools_cnefe_muni = schools_cnefe[.(muni_code), nomatch = NULL]
    )
  })

  # Remove NULL results and combine
  batch_results <- batch_results[!sapply(batch_results, is.null)]
  if (length(batch_results) > 0) {
    rbindlist(batch_results, use.names = TRUE, fill = TRUE)
  } else {
    data.table()
  }
}

#' Process GeocodeR string matching in batches
#'
#' @param batch_ids Current batch ID
#' @param municipality_batch_assignments Batch assignments
#' @param locais_filtered Filtered polling stations
#' @param muni_ids Municipality IDs data
#' @return Combined match results
#' @export
process_geocodebr_batch <- function(batch_ids, municipality_batch_assignments, locais_filtered, muni_ids) {
  # Get municipalities for this batch
  batch_munis <- municipality_batch_assignments[
    batch_id == batch_ids
  ]$cod_localidade_ibge

  # Collect-and-stop (cleanup phase 3, finding C5): a NULL result (no polling
  # stations or no geocoding hits) is a legitimate empty case and is filtered;
  # a municipality that errors is surfaced at batch end, never silently dropped.
  results <- collect_batch_or_stop(
    batch_munis,
    function(muni_code) {
      match_geocodebr_muni(
        locais_muni = locais_filtered[cod_localidade_ibge == muni_code],
        muni_ids = muni_ids[id_munic_7 == muni_code]
      )
    },
    task_label = "geocodebr matching"
  )

  if (length(results) > 0) {
    rbindlist(results, use.names = TRUE, fill = TRUE)
  } else {
    data.table()
  }
}

#' Process street/neighborhood matching for one batch slice
#'
#' Shared engine for the CNEFE and Agro CNEFE street/neighborhood match batches,
#' which differ only in the per-municipality matcher and the log label. `stbairro`
#' is a per-batch grouped-stem slice (Option A, issue #68): the union of the
#' street and neighborhood aggregates for this batch's municipalities, tagged by
#' `component` and carrying a constant `batch_id`. The whole national tables never
#' cross to the worker; municipalities are still taken from
#' `municipality_batch_assignments` so the combined row order is byte-identical to
#' the pre-reshape pipeline.
#'
#' @param municipality_batch_assignments Batch assignments
#' @param locais_filtered Filtered polling stations
#' @param stbairro Per-batch slice: union of street + neighborhood aggregates,
#'   tagged by `component`
#' @param match_fn Per-municipality matcher called positionally as
#'   `match_fn(locais_muni, st_muni, bairro_muni)`
#' @param label Human-readable source label for the log lines (e.g. `"CNEFE"`)
#' @return Combined match results
#' @export
process_stbairro_batch <- function(
  municipality_batch_assignments,
  locais_filtered,
  stbairro,
  match_fn,
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

  message(sprintf(
    "[Batch %d] Starting %s street/neighborhood matching for %d municipalities",
    this_batch,
    label,
    length(batch_munis)
  ))

  batch_results <- lapply(seq_along(batch_munis), function(i) {
    muni_code <- batch_munis[i]

    locais_muni <- locais_filtered[cod_localidade_ibge == muni_code]
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

    result <- match_fn(locais_muni, st_muni, bairro_muni)

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

#' Process CNEFE street/neighborhood matching in batches
#'
#' Thin wrapper over [process_stbairro_batch()] with the CNEFE matcher.
#'
#' @param municipality_batch_assignments Batch assignments
#' @param locais_filtered Filtered polling stations
#' @param cnefe_stbairro Per-batch slice: union of street + neighborhood CNEFE
#'   aggregates, tagged by `component`
#' @return Combined match results
#' @export
process_cnefe_stbairro_batch <- function(
  municipality_batch_assignments,
  locais_filtered,
  cnefe_stbairro
) {
  process_stbairro_batch(
    municipality_batch_assignments,
    locais_filtered,
    cnefe_stbairro,
    match_fn = match_stbairro_cnefe_muni,
    label = "CNEFE"
  )
}

#' Process Agro CNEFE street/neighborhood matching in batches
#'
#' Thin wrapper over [process_stbairro_batch()] with the Agro CNEFE matcher.
#'
#' @param municipality_batch_assignments Batch assignments
#' @param locais_filtered Filtered polling stations
#' @param agrocnefe_stbairro Per-batch slice: union of street + neighborhood Agro
#'   CNEFE aggregates, tagged by `component`
#' @return Combined match results
#' @export
process_agrocnefe_stbairro_batch <- function(
  municipality_batch_assignments,
  locais_filtered,
  agrocnefe_stbairro
) {
  process_stbairro_batch(
    municipality_batch_assignments,
    locais_filtered,
    agrocnefe_stbairro,
    match_fn = match_stbairro_agrocnefe_muni,
    label = "Agro CNEFE"
  )
}
# ===== PARALLEL PROCESSING =====

#' Create municipality batch assignments for balanced parallel processing
#'
#' This function creates balanced batch assignments for municipality codes to reduce
#' the number of dynamic branches in the targets pipeline. It assigns each municipality
#' to a specific batch number, which helps prevent bottlenecks when crew dispatches
#' thousands of fine-grained tasks.
#'
#' Unlike create_municipality_batches (which returns a list of batches), this function
#' returns a data.table mapping each municipality code to its batch number. This is
#' more efficient for joining operations in the pipeline.
#'
#' @param muni_codes Vector of municipality codes to batch
#' @param batch_size Target size for each batch (default: 50)
#' @param muni_sizes Optional named vector of municipality sizes for load balancing
#' @return data.table with columns: cod_localidade_ibge (municipality code) and batch_id
#' @examples
#' assignments <- create_municipality_batch_assignments(unique(locais$cod_localidade_ibge))
#' @export
create_municipality_batch_assignments <- function(muni_codes, batch_size = 50, muni_sizes = NULL) {
  n_munis <- length(muni_codes)

  # Simple sequential batching if no size information provided
  if (is.null(muni_sizes)) {
    n_batches <- ceiling(n_munis / batch_size)
    batch_nums <- rep(seq_len(n_batches), each = batch_size, length.out = n_munis)

    result <- data.table::data.table(
      cod_localidade_ibge = muni_codes,
      batch_id = batch_nums
    )
  } else {
    # Load-balanced batching based on municipality sizes
    # Handle both data.table and vector inputs for muni_sizes
    if (inherits(muni_sizes, "data.table")) {
      # If muni_sizes is a data.table, join properly
      muni_df <- data.table::data.table(
        cod_localidade_ibge = muni_codes
      )
      # Join with the sizes data
      muni_df <- merge(muni_df, muni_sizes, by.x = "cod_localidade_ibge", by.y = "muni_code", all.x = TRUE)
    } else {
      # If muni_sizes is a named vector
      muni_df <- data.table::data.table(
        cod_localidade_ibge = muni_codes,
        size = muni_sizes[as.character(muni_codes)]
      )
    }
    # Fail loud on a municipality with no size entry (cleanup phase 3, Medium):
    # median-imputing a missing size masks a municipality-code key mismatch
    # between muni_codes and muni_sizes.
    missing_size <- muni_df[is.na(size), cod_localidade_ibge]
    if (length(missing_size) > 0) {
      stop(sprintf(
        "Municipality sizes missing for %d %s (key mismatch): %s",
        length(missing_size),
        ngettext(length(missing_size), "municipality", "municipalities"),
        paste(utils::head(missing_size, 10), collapse = ", ")
      ))
    }
    data.table::setorder(muni_df, -size)

    # Assign to batches using round-robin for load balancing
    n_batches <- ceiling(n_munis / batch_size)
    muni_df[, batch_id := rep_len(seq_len(n_batches), .N)]

    result <- muni_df[, .(cod_localidade_ibge, batch_id)]
  }

  # Log batch statistics
  batch_stats <- result[, .N, by = batch_id]
  message(sprintf(
    "Created %d batches for %d municipalities (min: %d, max: %d, avg: %.1f per batch)",
    length(unique(result$batch_id)),
    n_munis,
    min(batch_stats$N),
    max(batch_stats$N),
    mean(batch_stats$N)
  ))

  result
}

# ===== DATA EXPORT FUNCTIONS =====
# These functions were moved from data_export.R

#' Export geocoded locations to file
#'
#' @param geocoded_locais Geocoded locations data
#' @return Path to exported file
#' @export
export_geocoded_locais <- function(geocoded_locais) {
  fwrite(geocoded_locais, "./output/geocoded_polling_stations.csv.gz")
  "./output/geocoded_polling_stations.csv.gz"
}

#' Export panel IDs to file
#'
#' @param panel_ids Panel ID data to export
#' @return Path to exported file
#' @export
export_panel_ids <- function(panel_ids) {
  fwrite(panel_ids, "./output/panel_ids.csv.gz")
  "./output/panel_ids.csv.gz"
}

#' Export geocoded data with validation dependency
#'
#' @param geocoded_locais Geocoded locations data
#' @param validation_report Validation report (ensures it runs first)
#' @return File path of exported data
#' @export
export_geocoded_with_validation <- function(geocoded_locais, validation_report) {
  # validation_report is passed to ensure dependency
  export_geocoded_locais(geocoded_locais)
}

#' Export panel IDs with validation dependency
#'
#' @param panel_ids Panel ID data
#' @param validation_report Validation report (ensures it runs first)
#' @return File path of exported data
#' @export
export_panel_ids_with_validation <- function(panel_ids, validation_report) {
  # validation_report is passed to ensure dependency
  export_panel_ids(panel_ids)
}
