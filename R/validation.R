## Data-quality validation and release gates for the geocoding pipeline.

library(data.table)

# The exact geocoded_locais schema written to geocoded_polling_stations.csv.gz, in the
# order finalize_coords() produces it. Checked twice: validate_final_output() requires
# every column before the export runs, release Gate 2 asserts set equality after it.
RELEASE_EXPORT_COLS <- c(
  "cd_localidade_tse",
  "ano",
  "nr_zona",
  "nr_locvot",
  "nr_cep",
  "sg_uf",
  "nm_localidade",
  "nm_locvot",
  "ds_endereco",
  "ds_bairro",
  "cod_localidade_ibge",
  "local_id",
  "pred_long",
  "pred_lat",
  "pred_dist",
  "tse_lat",
  "tse_long",
  "final_long",
  "final_lat"
)

# Asserts the assembled model table is non-empty, keyed, and free of duplicate rows.
# anyDuplicated() rather than unique(), which would copy the ~950 MB national table.
validate_model_data_merge <- function(model_data) {
  stopifnot(
    nrow(model_data) > 0,
    "local_id" %in% names(model_data),
    anyDuplicated(model_data) == 0L
  )
  nrow(model_data)
}

# Asserts every polling station carries a usable predicted match distance.
validate_model_predictions <- function(predictions) {
  stopifnot(
    nrow(predictions) > 0,
    "pred_dist" %in% names(predictions),
    is.numeric(predictions$pred_dist),
    !anyNA(predictions$pred_dist)
  )
  nrow(predictions)
}

# Asserts the final geocoded table is shippable; stops before export on failure.
validate_final_output <- function(output_data) {
  stopifnot(nrow(output_data) > 0)
  missing_cols <- setdiff(RELEASE_EXPORT_COLS, names(output_data))
  if (length(missing_cols) > 0) {
    stop(sprintf("geocoded_locais is missing required columns: %s", paste(missing_cols, collapse = ", ")))
  }
  # The station-year key; a duplicate here duplicates rows in the published file.
  duplicate_keys <- sum(duplicated(output_data, by = c("local_id", "ano", "nr_zona", "nr_locvot")))
  if (duplicate_keys > 0) {
    stop(sprintf("Found %d duplicate key combinations in geocoded_locais", duplicate_keys))
  }

  message(sprintf(
    "Geocoding complete: %d polling stations geocoded",
    nrow(output_data)
  ))
  nrow(output_data)
}

# Checks municipality, INEP, and polling-station row counts against expected ranges.
validate_inputs_consolidated <- function(muni_ids, inep_codes, locais, pipeline_config) {
  # Dev mode restricts the pipeline to two states, so municipality and polling-station
  # counts shrink; inep_codes is never filtered, so its range is always national.
  checks <- list(
    muni_ids_size = list(
      name = "municipalities",
      count = nrow(muni_ids),
      range = if (pipeline_config$dev_mode) c(30L, 100L) else c(5000L, 6000L)
    ),
    inep_codes_size = list(
      name = "INEP schools",
      count = nrow(inep_codes),
      range = c(100000L, 300000L)
    ),
    locais_size = list(
      name = "polling stations",
      count = nrow(locais),
      range = if (pipeline_config$dev_mode) c(1000L, 20000L) else c(100000L, 1000000L)
    )
  )

  passed <- vapply(checks, function(ck) ck$count >= ck$range[1] && ck$count <= ck$range[2], logical(1))
  messages <- vapply(
    checks,
    function(ck) sprintf("%s: %d (expected %d-%d)", ck$name, ck$count, ck$range[1], ck$range[2]),
    character(1)
  )

  cat("\n=== INPUT DATA VALIDATION ===\n")
  cat("Mode:", if (pipeline_config$dev_mode) "DEVELOPMENT" else "PRODUCTION", "\n")
  for (i in seq_along(checks)) {
    cat("-", messages[i], if (passed[i]) "✓" else "❌", "\n")
  }
  cat("=============================\n\n")

  # Fail loud: a warning here would let the pipeline continue on inputs that failed validation.
  if (!all(passed)) {
    stop(sprintf(
      "Input data validation failed for: %s.\n%s",
      paste(names(checks)[!passed], collapse = ", "),
      paste(messages, collapse = "\n")
    ))
  }

  list(
    municipalities = checks$muni_ids_size$count,
    inep_schools = checks$inep_codes_size$count,
    polling_stations = checks$locais_size$count
  )
}

# Computes coverage and duplicate-coordinate quality metrics; stops on CRITICAL status.
# Export-file existence is Gate 4's job (validate_release_gates), not re-checked here.
create_data_quality_monitor <- function(geocoded_locais, panel_ids, expected_municipality_count = 5570) {
  cat("Running data quality monitoring...\n")

  alerts <- list()
  status <- "OK"

  n_municipalities <- length(unique(geocoded_locais$cd_localidade_tse))
  muni_diff <- abs(n_municipalities - expected_municipality_count)
  if (muni_diff > 50) {
    alerts <- append(
      alerts,
      sprintf(
        "Municipality count (%d) differs from expected (%d) by %d",
        n_municipalities,
        expected_municipality_count,
        muni_diff
      )
    )
    status <- if (muni_diff > 100) "CRITICAL" else "WARNING"
  }

  geocoding_rate <- mean(!is.na(geocoded_locais$final_long) & !is.na(geocoded_locais$final_lat)) * 100
  if (geocoding_rate < 95) {
    alerts <- append(
      alerts,
      sprintf("Geocoding coverage (%.1f%%) below threshold (95%%)", geocoding_rate)
    )
    if (status == "OK") {
      status <- "WARNING"
    }
  }

  panel_coverage <- (sum(geocoded_locais$local_id %in% panel_ids$local_id) / nrow(geocoded_locais)) * 100
  if (panel_coverage < 90) {
    alerts <- append(
      alerts,
      sprintf("Panel coverage (%.1f%%) below threshold (90%%)", panel_coverage)
    )
    if (status == "OK") {
      status <- "WARNING"
    }
  }

  coords_dt <- geocoded_locais[
    !is.na(final_long) & !is.na(final_lat),
    .(n = .N),
    by = .(final_long, final_lat)
  ]
  duplicate_groups <- nrow(coords_dt[n > 1])
  if (duplicate_groups > 10) {
    alerts <- append(
      alerts,
      sprintf("Found %d duplicate coordinate groups (threshold: 10)", duplicate_groups)
    )
    if (status == "OK") {
      status <- "WARNING"
    }
  }

  results <- list(
    status = status,
    metrics = list(
      n_geocoded = nrow(geocoded_locais),
      n_panel_ids = nrow(panel_ids),
      n_unique_stations = length(unique(geocoded_locais$local_id)),
      n_unique_panels = length(unique(panel_ids$panel_id)),
      n_municipalities = n_municipalities,
      geocoding_coverage = geocoding_rate,
      panel_coverage = panel_coverage,
      duplicate_coord_groups = duplicate_groups
    ),
    alerts = alerts
  )

  cat("Data quality monitoring completed.\n")
  cat("  Status:", status, "\n")
  cat("  Geocoded locations:", results$metrics$n_geocoded, "\n")
  cat("  Panel IDs:", results$metrics$n_panel_ids, "\n")
  cat("  Municipalities:", n_municipalities, "\n")
  cat("  Geocoding coverage:", sprintf("%.1f%%", geocoding_rate), "\n")
  if (length(alerts) > 0) {
    cat("  Alerts:", length(alerts), "\n")
  }

  # A CRITICAL status must stop the build, not merely be recorded in the result.
  if (identical(status, "CRITICAL")) {
    stop(sprintf(
      "Data quality monitoring reported CRITICAL status. Alerts:\n%s",
      paste(unlist(alerts), collapse = "\n")
    ))
  }

  results
}

# Release gates: fail-loud structural tripwires on the production rebuild.

# All election years the pipeline geocodes.
RELEASE_EXPECTED_YEARS <- seq(2006L, 2024L, by = 2L)

# Sane-scale band for the 2024 partition (2024 address count: 93,337).
RELEASE_N_2024_MIN <- 85000L
RELEASE_N_2024_MAX <- 100000L

# Plausible national band for any single year; counts sit around 90-96k per year.
RELEASE_N_YEAR_MIN <- 50000L
RELEASE_N_YEAR_MAX <- 120000L

# Landed 2024 TSE-coverage hard gate, in percent.
RELEASE_TSE_COVERAGE_GATE <- 92

# Election years for which TSE publishes field-collected coordinates. Coverage
# begins with the 2018 vintage; earlier years carry no TSE ground truth.
RELEASE_TSE_VINTAGES <- c(2018L, 2020L, 2022L, 2024L)

# Gate 7 tolerance in percentage points: observed legitimate join loss is <= 1.6 pt, while
# the merge bug this gate catches dropped ~36 pt.
RELEASE_TSE_JOIN_SLACK <- 5L

# Runs the release gates over the geocoded export; stops the build on any failure.
validate_release_gates <- function(
  geocoded_locais,
  tse_coverage,
  tse_raw_availability,
  export_paths,
  dev_mode,
  panel_gate = NULL
) {
  # panel_gate is a dependency-only argument: taking the panel_release_gates target makes
  # this check depend on it, so building release_gates also runs that gate.
  # Read-only, so skip a deep copy of the ~945k-row national table.
  dt <- if (is.data.table(geocoded_locais)) {
    geocoded_locais
  } else {
    as.data.table(geocoded_locais)
  }
  failures <- character(0)
  add_fail <- function(...) failures <<- c(failures, sprintf(...))

  # Gate 1: all election years present, with a non-empty 2024 partition.
  present_years <- sort(unique(dt$ano))
  missing_years <- setdiff(RELEASE_EXPECTED_YEARS, present_years)
  if (length(missing_years) > 0) {
    add_fail("Gate 1 (all years): missing election years: %s", paste(missing_years, collapse = ", "))
  }
  n_2024 <- dt[ano == 2024L, .N]
  if (n_2024 == 0L) {
    add_fail("Gate 1 (all years): 2024 partition is empty")
  }

  # Gate 2: exported schema exactly unchanged - no column removed or added.
  missing_cols <- setdiff(RELEASE_EXPORT_COLS, names(dt))
  extra_cols <- setdiff(names(dt), RELEASE_EXPORT_COLS)
  if (length(missing_cols) > 0) {
    add_fail("Gate 2 (schema): missing columns: %s", paste(missing_cols, collapse = ", "))
  }
  if (length(extra_cols) > 0) {
    add_fail("Gate 2 (schema): unexpected extra columns: %s", paste(extra_cols, collapse = ", "))
  }

  # Gate 3: coordinates not all-NA in any year-state cell. Grouping by year alone
  # missed a whole state shipping blank in every year, which is how the Distrito
  # Federal went out uncoordinated: 0.4% of national rows, invisible to a
  # coverage threshold and to a per-year check.
  coord_by_cell <- dt[,
    .(
      n = .N,
      n_coord = sum(!is.na(final_lat) & !is.na(final_long))
    ),
    by = .(ano, sg_uf)
  ][order(ano, sg_uf)]
  zero_coord_cells <- coord_by_cell[n_coord == 0L]
  if (nrow(zero_coord_cells) > 0) {
    add_fail(
      "Gate 3 (coords): year-state cells with zero non-NA coordinates: %s",
      paste(zero_coord_cells$sg_uf, zero_coord_cells$ano, sep = "-", collapse = ", ")
    )
  }

  # Gate 5 and the returned summary want national per-year counts, rolled up from
  # the same pass rather than re-scanning the table.
  coord_by_year <- coord_by_cell[,
    .(n = sum(n), n_coord = sum(n_coord)),
    by = ano
  ][order(ano)]

  # Gate 4: output files exist on disk.
  for (p in export_paths) {
    if (!file.exists(p)) {
      add_fail("Gate 4 (files): output file missing: %s", p)
    }
  }

  # Gate 5: sane per-year row counts, production only (dev mode processes only AC/RR).
  if (!isTRUE(dev_mode)) {
    if (n_2024 < RELEASE_N_2024_MIN || n_2024 > RELEASE_N_2024_MAX) {
      add_fail(
        "Gate 5 (counts): 2024 station count %d outside sane range [%d, %d]",
        n_2024,
        RELEASE_N_2024_MIN,
        RELEASE_N_2024_MAX
      )
    }
    off <- coord_by_year[
      ano != 2024L & (n < RELEASE_N_YEAR_MIN | n > RELEASE_N_YEAR_MAX)
    ]
    if (nrow(off) > 0) {
      add_fail(
        "Gate 5 (counts): years outside plausible national range [%d, %d]: %s",
        RELEASE_N_YEAR_MIN,
        RELEASE_N_YEAR_MAX,
        paste(sprintf("%d=%d", off$ano, off$n), collapse = ", ")
      )
    }
  }

  # Gates 6 & 7: landed TSE coverage, aggregated from per-year x state to per-year.
  cov <- as.data.table(tse_coverage)
  cov_year <- cov[,
    .(
      n_total = sum(n_total),
      n_covered = sum(n_covered)
    ),
    by = ano
  ]
  cov_year[, coverage_pct := 100 * n_covered / n_total]
  setorder(cov_year, ano)

  # Gate 6: landed 2024 TSE coverage >= 92% (hard gate).
  cov_2024 <- cov_year[ano == 2024L, coverage_pct]
  if (length(cov_2024) == 0) {
    add_fail("Gate 6 (2024 coverage): no 2024 rows in tse_coverage")
  } else if (cov_2024 < RELEASE_TSE_COVERAGE_GATE) {
    add_fail("Gate 6 (2024 coverage): landed 2024 TSE coverage %.2f%% < %d%% gate", cov_2024, RELEASE_TSE_COVERAGE_GATE)
  }

  # Gate 7: landed coverage must track each vintage's own raw TSE availability (~51% in 2018 to ~94% in 2024).
  raw <- as.data.table(tse_raw_availability)
  cov_vs_raw <- merge(
    cov_year[, .(ano, coverage_pct)],
    raw[, .(ano, raw_avail_pct)],
    by = "ano"
  )
  missing_vintages <- setdiff(RELEASE_TSE_VINTAGES, cov_vs_raw$ano)
  if (length(missing_vintages) > 0) {
    add_fail(
      "Gate 7 (coverage regression): TSE vintages absent from coverage/availability: %s",
      paste(missing_vintages, collapse = ", ")
    )
  }
  cov_vs_raw[, shortfall := raw_avail_pct - coverage_pct]
  regressed <- cov_vs_raw[shortfall > RELEASE_TSE_JOIN_SLACK]
  if (nrow(regressed) > 0) {
    add_fail(
      "Gate 7 (coverage regression): landed TSE coverage falls >%d pts below raw availability: %s",
      RELEASE_TSE_JOIN_SLACK,
      paste(
        sprintf("%d=%.1f%% landed vs %.1f%% raw", regressed$ano, regressed$coverage_pct, regressed$raw_avail_pct),
        collapse = ", "
      )
    )
  }

  # Gate 8: TSE-covered stations ship ground-truth coordinates, so pred_dist must be 0.
  # Column-guarded so a missing column stays Gate 2's failure rather than a raw error.
  if (all(c("tse_long", "tse_lat", "pred_dist") %in% names(dt))) {
    n_bad_preddist <- dt[
      !is.na(tse_long) & !is.na(tse_lat) & (is.na(pred_dist) | pred_dist != 0),
      .N
    ]
    if (n_bad_preddist > 0) {
      add_fail(
        "Gate 8 (pred_dist): %d TSE-covered rows have non-zero pred_dist (expected 0 for ground-truth coordinates)",
        n_bad_preddist
      )
    }
  }

  # Gate 9: the published header comes from to_geocoded_export_schema(); check it matches the 0.141 schema.
  published_cols <- names(to_geocoded_export_schema(head(dt, 1L)))
  if (!identical(published_cols, GEOCODED_EXPORT_SCHEMA)) {
    add_fail(
      "Gate 9 (published schema): export mapper output does not match the 0.141 schema.\n    expected: %s\n    got: %s",
      paste(GEOCODED_EXPORT_SCHEMA, collapse = ", "),
      paste(published_cols, collapse = ", ")
    )
  }

  summary <- list(
    passed = length(failures) == 0,
    failures = failures,
    dev_mode = isTRUE(dev_mode),
    years_present = present_years,
    n_2024 = n_2024,
    coverage_by_year = cov_year[],
    coverage_vs_raw = cov_vs_raw[],
    coord_by_year = coord_by_year[]
  )

  if (length(failures) > 0) {
    stop(sprintf(
      "Release gates FAILED (release spec Validation gates) - do not ship:\n  - %s",
      paste(failures, collapse = "\n  - ")
    ))
  }
  message(sprintf(
    "Release gates PASSED: %d years present (%s); 2024 n=%d; 2024 landed TSE coverage=%.2f%%",
    length(present_years),
    paste(present_years, collapse = ","),
    n_2024,
    cov_2024
  ))
  summary
}

# Max share of panel rows allowed to ship without a coordinate. A panel lacks one only
# when every one of its station-years failed to geocode, so the real rate sits well under 1%.
RELEASE_PANEL_COORD_NA_MAX_PCT <- 1

# Release gate for panel_ids.csv.gz: stops if pred_dist is missing or too many panels lack a coordinate.
validate_panel_release <- function(panel_ids) {
  dt <- if (is.data.table(panel_ids)) panel_ids else as.data.table(panel_ids)
  failures <- character(0)
  add_fail <- function(...) failures <<- c(failures, sprintf(...))

  # Gate P1: the accuracy-filter column must reach the output.
  if (!("pred_dist" %in% names(dt))) {
    add_fail("Gate P1 (schema): panel_ids is missing the pred_dist column")
  }

  # Gate P2: coordinates present for essentially every panel.
  has_coords <- all(c("long", "lat") %in% names(dt))
  coord_na_pct <- if (has_coords) 100 * mean(is.na(dt$long) | is.na(dt$lat)) else NA_real_
  if (!has_coords) {
    add_fail("Gate P2 (coords): panel_ids is missing long/lat columns")
  } else if (coord_na_pct > RELEASE_PANEL_COORD_NA_MAX_PCT) {
    add_fail(
      "Gate P2 (coords): %.2f%% of panel rows lack a coordinate (> %g%% max) - the panel step is likely ignoring model coordinates",
      coord_na_pct,
      RELEASE_PANEL_COORD_NA_MAX_PCT
    )
  }

  summary <- list(
    passed = length(failures) == 0,
    failures = failures,
    n_rows = nrow(dt),
    coord_na_pct = coord_na_pct
  )

  if (length(failures) > 0) {
    stop(sprintf(
      "Panel release gates FAILED (do not ship):\n  - %s",
      paste(failures, collapse = "\n  - ")
    ))
  }
  message(sprintf(
    "Panel release gates PASSED: %d rows; %.2f%% without a coordinate",
    summary$n_rows,
    coord_na_pct
  ))
  summary
}
