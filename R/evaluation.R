## Evaluation harness for honest geocoding accuracy + pred_dist calibration
##
## Implements the leakage-controlled evaluation spec
## (docs/specs/2026-07-evaluation-spec.md, issue #47). The pipeline's output is
## the TSE coordinate wherever TSE has one, and TSE coordinates are the model's
## training target, so accuracy can only be measured honestly on the TSE-covered
## subset with a strict held-out split. These helpers produce:
##   - out-of-fold (OOF) predictions over the covered set (station-grouped k-fold),
##   - stratified accuracy tables (metric ladder, joint with match rate),
##   - TSE coverage by year x state,
##   - a pred_dist calibration check (rank-and-filter + reliability/ENCE).
## Errors are haversine distances in kilometres (model_data$dist is already km,
## r = 6378.137 in make_model_data()).

library(data.table)

## Small-cell floor and fold count are execution parameters fixed by the spec
## (documented here so the chosen values live in one place).
EVAL_N_FOLDS <- 5L
EVAL_MIN_CELL_N <- 50L
EVAL_FOLD_SEED <- 20260710L

## Metric-ladder columns produced by accuracy_metrics(), suppressed together when
## a stratum falls below the held-out N floor.
EVAL_METRIC_COLS <- c(
  "median_km",
  "p90",
  "p95",
  "p99",
  "within_100m",
  "within_500m",
  "within_1km"
)

# ============================================================================
# Stratification helpers
# ============================================================================

state_to_region <- function(sg_uf) {
  # Map the 27 Brazilian UF (state) codes to the 5 IBGE macro-regions.
  region_map <- c(
    # Norte
    AC = "Norte",
    AP = "Norte",
    AM = "Norte",
    PA = "Norte",
    RO = "Norte",
    RR = "Norte",
    TO = "Norte",
    # Nordeste
    AL = "Nordeste",
    BA = "Nordeste",
    CE = "Nordeste",
    MA = "Nordeste",
    PB = "Nordeste",
    PE = "Nordeste",
    PI = "Nordeste",
    RN = "Nordeste",
    SE = "Nordeste",
    # Centro-Oeste
    DF = "Centro-Oeste",
    GO = "Centro-Oeste",
    MT = "Centro-Oeste",
    MS = "Centro-Oeste",
    # Sudeste
    ES = "Sudeste",
    MG = "Sudeste",
    RJ = "Sudeste",
    SP = "Sudeste",
    # Sul
    PR = "Sul",
    RS = "Sul",
    SC = "Sul"
  )
  out <- unname(region_map[as.character(sg_uf)])
  # Fail loud on an unknown UF: a missing region means a data problem upstream,
  # not something to silently paper over.
  if (anyNA(out) && !anyNA(sg_uf)) {
    stop(
      "state_to_region(): unmapped UF code(s): ",
      paste(unique(sg_uf[is.na(out)]), collapse = ", ")
    )
  }
  out
}

get_station_zone <- function(tse_points, tract_shp) {
  # Attach the census-tract zone (URBANO/RURAL) to each covered station by a
  # point-in-tract spatial join on its field-collected TSE coordinate. Mirrors
  # the join used in doc/geocoding_procedure.qmd. `tse_points` is a data.table
  # with local_id, tse_long, tse_lat.
  pts <- tse_points[!is.na(tse_long) & !is.na(tse_lat)]
  if (nrow(pts) == 0) {
    return(data.table(local_id = integer(), zone = character()))
  }
  pts_sf <- sf::st_as_sf(
    pts,
    coords = c("tse_long", "tse_lat"),
    crs = 4674
  )
  # Reproject to the tract CRS so the join is valid regardless of the tract
  # layer's stored CRS.
  pts_sf <- sf::st_transform(pts_sf, sf::st_crs(tract_shp))
  joined <- sf::st_join(pts_sf, tract_shp, left = TRUE, largest = TRUE)
  joined <- sf::st_drop_geometry(joined)
  unique(data.table(local_id = joined$local_id, zone = joined$zone))
}

# ============================================================================
# Out-of-fold prediction machinery
# ============================================================================

assign_eval_folds <- function(model_data, k = EVAL_N_FOLDS, seed = EVAL_FOLD_SEED) {
  # Assign every municipality that has at least one TSE-covered candidate to one
  # of k folds. Grouping by cod_localidade_ibge (municipality) mirrors the
  # production tuner and is a superset of the spec's station-grouping
  # requirement: no polling station's candidate rows are ever split across
  # folds, so the held-out fold never leaks its TSE target into training.
  covered_munis <- sort(unique(
    model_data[!is.na(dist), cod_localidade_ibge]
  ))
  n_muni <- length(covered_munis)
  if (n_muni < k) {
    stop(
      "assign_eval_folds(): only ",
      n_muni,
      " covered municipalities for k = ",
      k,
      " folds."
    )
  }
  # Deterministic, balanced assignment: a fixed seed makes the split reproducible
  # across pipeline runs (a standing project reproducibility constraint).
  set.seed(seed)
  folds <- sample(rep_len(seq_len(k), n_muni))
  data.table(cod_localidade_ibge = covered_munis, fold = folds)
}

compute_oof_predictions <- function(
  model_data,
  trained_model,
  fold_assignment,
  offset = GBM_LOG_OFFSET
) {
  # Produce honest out-of-fold pred_dist for every covered candidate row. For
  # each fold, refit the LightGBM workflow on the other k-1 folds and predict the
  # held-out fold, reusing the production model's tuned hyperparameters. The
  # refits themselves are leak-free: no station's prediction is ever trained on
  # that station's own fold (this is the primary leak the C4 fix / #33 controls).
  #
  # One residual, deliberately accepted channel remains: the hyperparameters were
  # chosen by the production tuner's cross-validation, whose training split can
  # include municipalities that later fall in an OOF evaluation fold. This is a
  # weak, low-dimensional channel (six hyperparameters selected by aggregate CV
  # score, not per-row TSE targets). Eliminating it fully would require nested
  # per-fold tuning (k x the already hours-long tune), judged not worth it; it is
  # documented here and in the reports rather than hidden.
  library(bonsai) # registers the lightgbm engine on the worker

  covered <- model_data[!is.na(dist) & !is.na(mindist)]
  covered <- merge(covered, fold_assignment, by = "cod_localidade_ibge")
  stopifnot(
    "every covered row must receive a fold" = !anyNA(covered$fold),
    "no covered rows to evaluate" = nrow(covered) > 0
  )

  best_params <- tune::select_best(trained_model$tune_out, metric = "rmse")

  fold_ids <- sort(unique(covered$fold))
  preds <- vector("list", length(fold_ids))
  for (i in seq_along(fold_ids)) {
    f <- fold_ids[i]
    # Drop the `fold` column before fitting: the recipe is dist ~ ., so any
    # stray column would become a model predictor. Fold membership is a
    # split key, never a feature.
    train_df <- covered[fold != f][, fold := NULL]
    test_df <- covered[fold == f]
    test_features <- test_df[, !"fold"]

    wf <- tune::finalize_workflow(build_gbm_workflow(train_df), best_params)
    fitted_wf <- generics::fit(wf, data = train_df)

    pred_logdist <- predict(fitted_wf, new_data = test_features)$.pred
    test_df[, pred_logdist := pred_logdist]
    test_df[, pred_dist := exp(pred_logdist) - offset]
    preds[[i]] <- test_df[, .(
      local_id,
      cod_localidade_ibge,
      match_type = type,
      mindist,
      long,
      lat,
      dist,
      pred_dist,
      pred_logdist,
      fold
    )]
  }
  rbindlist(preds)[order(local_id, pred_dist)]
}

select_oof_matches <- function(
  oof_predictions,
  locais,
  tsegeocoded_locais,
  tract_shp
) {
  # Per covered station, select the candidate with the smallest OOF pred_dist
  # (ties -> first, matching finalize_coords()), and left-join it onto the full
  # covered-station universe so non-geocoded covered stations survive with a
  # missing error (needed for honest per-stratum match rates). Attaches the four
  # stratification axes: vintage, region, match source, urban/rural.

  # Selected (geocoded) match per station. oof_predictions arrives sorted by
  # (local_id, pred_dist) from compute_oof_predictions(), so the first row per
  # station is its best (smallest predicted-error) candidate; unique(by=...)
  # takes it without a re-sort or per-group .SD allocation. The chosen row's
  # `dist` is the realized haversine error to TSE, in km.
  selected <- unique(oof_predictions, by = "local_id")[, .(
    local_id,
    match_source = match_type,
    error_km = dist,
    pred_dist,
    fold
  )]

  # Universe of covered stations with their (geocoding-independent) strata.
  covered_ids <- unique(tsegeocoded_locais$local_id)
  universe <- locais[
    local_id %in% covered_ids,
    .(local_id, cod_localidade_ibge, vintage = ano, sg_uf)
  ]
  universe[, region := state_to_region(sg_uf)]

  zones <- get_station_zone(
    tsegeocoded_locais[, .(local_id, tse_long, tse_lat)],
    tract_shp
  )
  universe <- merge(universe, zones, by = "local_id", all.x = TRUE)
  universe[,
    urban_rural := fcase(
      zone == "URBANO" , "urban" ,
      zone == "RURAL"  , "rural" ,
      default = NA_character_
    )
  ]
  universe[, zone := NULL]

  out <- merge(universe, selected, by = "local_id", all.x = TRUE)
  out[, geocoded := !is.na(error_km)]
  out[]
}

# ============================================================================
# TSE coverage (spec section 6)
# ============================================================================

compute_tse_coverage <- function(
  locais,
  tsegeocoded_locais,
  min_cell_n = EVAL_MIN_CELL_N
) {
  # Coverage of field-collected TSE coordinates by election year x state: the
  # ground-truth density each accuracy stratum is read against. Small cells are
  # flagged (not dropped) so downstream tables can suppress noisy medians.
  totals <- locais[, .(n_total = .N), by = .(ano, sg_uf)]
  covered_ids <- unique(tsegeocoded_locais$local_id)
  covered <- locais[
    local_id %in% covered_ids,
    .(n_covered = .N),
    by = .(ano, sg_uf)
  ]
  cov <- merge(totals, covered, by = c("ano", "sg_uf"), all.x = TRUE)
  cov[is.na(n_covered), n_covered := 0L]
  cov[, coverage_pct := 100 * n_covered / n_total]
  cov[, suppressed := n_covered < min_cell_n]
  setorder(cov, ano, sg_uf)
  cov[]
}

# Per-election-year share of polling stations for which the raw TSE file carries
# a usable coordinate. This is the ceiling the pipeline's landed TSE coverage is
# read against: TSE progressively geocoded stations over the vintages, so raw
# availability ramps from ~51% (2018) to ~94% (2024). The release regression
# tripwire (validate_release_gates() Gate 7) therefore compares each year's
# landed coverage to that year's own raw availability, not to a flat floor a
# pre-2024 vintage could never clear. A station counts as available if ANY of
# its rows carries a valid latitude (TSE encodes "no coordinate" as -1 or NA).
# Station identity mirrors clean_tsegeocoded_locais(): unique (cd_municipio,
# nr_zona, nr_local_votacao) within an election year.
#
# Availability is scoped to the states present in `locais` (the same universe
# compute_tse_coverage() aggregates over) so Gate 7 compares like with like in
# dev mode, where locais is the AC/RR subset but tse_files stay national. The
# scope is by state (a pre-join geography), so this stays the pre-join ceiling.
compute_tse_raw_availability <- function(tse_files, locais) {
  cols <- c("AA_ELEICAO", "SG_UF", "CD_MUNICIPIO", "NR_ZONA", "NR_LOCAL_VOTACAO", "NR_LATITUDE")
  locs <- rbindlist(lapply(tse_files, function(f) read_tse_locais_file(f, cols)), use.names = TRUE)
  locs <- locs[sg_uf %in% unique(locais$sg_uf)]
  locs[, has_coord := !is.na(nr_latitude) & nr_latitude != -1]
  by_station <- locs[,
    .(any_coord = any(has_coord)),
    by = .(ano = aa_eleicao, cd_municipio, nr_zona, nr_local_votacao)
  ]
  avail <- by_station[,
    .(n_stations = .N, n_with_coord = sum(any_coord)),
    by = ano
  ]
  avail[, raw_avail_pct := 100 * n_with_coord / n_stations]
  setorder(avail, ano)
  avail[]
}

# ============================================================================
# Accuracy tables (spec section 4)
# ============================================================================

accuracy_metrics <- function(error_km) {
  # The positional-accuracy metric ladder for one stratum. Positional error is a
  # right-skewed distribution, so we lead with the distribution (percentiles and
  # within-threshold shares), not a mean. Thresholds are 100 m / 500 m / 1 km.
  e <- error_km[!is.na(error_km)]
  if (length(e) == 0) {
    return(list(
      median_km = NA_real_,
      p90 = NA_real_,
      p95 = NA_real_,
      p99 = NA_real_,
      within_100m = NA_real_,
      within_500m = NA_real_,
      within_1km = NA_real_
    ))
  }
  qs <- stats::quantile(e, probs = c(.5, .9, .95, .99), names = FALSE)
  list(
    median_km = qs[1], # the 50th percentile / median
    p90 = qs[2],
    p95 = qs[3],
    p99 = qs[4],
    within_100m = 100 * mean(e <= 0.1),
    within_500m = 100 * mean(e <= 0.5),
    within_1km = 100 * mean(e <= 1.0)
  )
}

.accuracy_by <- function(dt, by_cols, min_cell_n) {
  # Compute the metric ladder + match rate for one grouping of the covered
  # universe. `by_cols` is a character vector of grouping columns (empty for the
  # overall row); the stratum label is derived from it. Accuracy is measured on
  # geocoded stations; match rate is the share of covered stations in the stratum
  # that were geocoded at all (accuracy and match rate always reported jointly,
  # per the spec).
  stratum <- if (length(by_cols) == 0L) "overall" else paste(by_cols, collapse = ":")
  # Copy only when adding the synthetic grouping column for the overall case;
  # the per-axis calls group by existing columns and need no copy.
  if (length(by_cols) == 0L) {
    dt <- copy(dt)[, .all := "all"]
    by_cols <- ".all"
  }
  res <- dt[,
    {
      ng <- sum(geocoded)
      c(
        list(
          n_total = .N,
          n_geocoded = ng,
          match_rate = 100 * ng / .N
        ),
        accuracy_metrics(error_km[geocoded])
      )
    },
    by = by_cols
  ]

  res[, level := do.call(paste, c(.SD, sep = ":")), .SDcols = by_cols]
  res[, stratum := stratum]
  # Suppress noisy accuracy metrics below the held-out N floor; keep counts and
  # match rate visible so the suppression itself is legible.
  res[n_geocoded < min_cell_n, (EVAL_METRIC_COLS) := NA_real_]
  res[, suppressed := n_geocoded < min_cell_n]
  res[, (by_cols) := NULL]
  setcolorder(res, c("stratum", "level", "n_total", "n_geocoded", "match_rate"))
  res[]
}

compute_accuracy_tables <- function(
  selected_matches,
  min_cell_n = EVAL_MIN_CELL_N
) {
  # Stratified accuracy tables over the covered set: overall, one table per axis
  # (urban/rural, region, vintage, match source), and the two urban/rural
  # crosses. Returned as one tidy long data.table keyed by (stratum, level).
  tabs <- list(
    .accuracy_by(selected_matches, character(0), min_cell_n),
    .accuracy_by(selected_matches, "urban_rural", min_cell_n),
    .accuracy_by(selected_matches, "region", min_cell_n),
    .accuracy_by(selected_matches, "vintage", min_cell_n),
    .accuracy_by(selected_matches, c("urban_rural", "vintage"), min_cell_n),
    .accuracy_by(selected_matches, c("urban_rural", "region"), min_cell_n)
  )

  # Match source is only defined for geocoded stations, so its match rate is not
  # meaningful (denominator would be geocoded-only). Report accuracy + N only.
  ms <- .accuracy_by(selected_matches[geocoded == TRUE], "match_source", min_cell_n)
  ms[, match_rate := NA_real_]
  tabs[[length(tabs) + 1L]] <- ms

  rbindlist(tabs, use.names = TRUE)
}

# ============================================================================
# pred_dist calibration check (spec section 7)
# ============================================================================

compute_calibration <- function(selected_matches, n_bins = 10L) {
  # Validate the predicted-distance ranking the pipeline already trusts for match
  # selection, on the OOF selected matches of covered stations. Two artifacts:
  #   1. rank-and-filter: dropping the worst-predicted tail should monotonically
  #      lower realized median error and raise %-within-500 m (the ranking
  #      carries information even if not calibrated in absolute metres);
  #   2. reliability + ENCE: bin by predicted error, compare predicted vs
  #      realized, and summarize the gap with Expected Normalized Calibration
  #      Error. Prediction-interval coverage is deferred to #44 (spec section 7).
  geo <- selected_matches[
    geocoded == TRUE & !is.na(pred_dist),
    .(pred_dist, error_km)
  ]
  setorder(geo, pred_dist)
  n <- nrow(geo)

  # Rank-and-filter: retain the best-predicted (1 - drop) share.
  drop_fracs <- seq(0, 0.5, by = 0.1)
  rank_filter <- rbindlist(lapply(drop_fracs, function(q) {
    keep <- seq_len(floor((1 - q) * n))
    e <- geo$error_km[keep]
    data.table(
      drop_frac = q,
      retained_frac = length(keep) / n,
      n_retained = length(keep),
      median_km = stats::median(e),
      within_500m = 100 * mean(e <= 0.5)
    )
  }))

  # Reliability: quantile bins of predicted error; predicted vs realized per bin.
  breaks <- unique(stats::quantile(
    geo$pred_dist,
    probs = seq(0, 1, length.out = n_bins + 1L),
    names = FALSE
  ))
  geo[, bin := cut(pred_dist, breaks = breaks, include.lowest = TRUE, labels = FALSE)]
  reliability <- geo[,
    .(
      n = .N,
      mean_pred = mean(pred_dist),
      mean_realized = mean(error_km)
    ),
    by = bin
  ][order(bin)]

  # Expected Normalized Calibration Error: N-weighted mean of the per-bin
  # absolute predicted-vs-realized gap, normalized by predicted error.
  ence <- reliability[, sum(n * abs(mean_pred - mean_realized) / mean_pred) / sum(n)]

  list(
    rank_filter = rank_filter,
    reliability = reliability,
    ence = ence
  )
}
