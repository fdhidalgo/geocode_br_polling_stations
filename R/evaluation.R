## Geocoding accuracy and pred_dist calibration, measured on the TSE-covered subset
## with out-of-fold predictions. Errors are haversine distances in kilometres.

library(data.table)

EVAL_N_FOLDS <- 5L
EVAL_MIN_CELL_N <- 50L
EVAL_FOLD_SEED <- 20260710L

## Metric-ladder columns from accuracy_metrics(), suppressed together below the cell-size floor.
EVAL_METRIC_COLS <- c(
  "median_km",
  "p90",
  "p95",
  "p99",
  "within_100m",
  "within_500m",
  "within_1km"
)

# Map the 27 Brazilian UF (state) codes to the 5 IBGE macro-regions.
state_to_region <- function(sg_uf) {
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
  if (anyNA(out) && !anyNA(sg_uf)) {
    stop(
      "state_to_region(): unmapped UF code(s): ",
      paste(unique(sg_uf[is.na(out)]), collapse = ", ")
    )
  }
  out
}

# Attach the census-tract zone (URBANO/RURAL) to each station by a point-in-tract join
# on its TSE coordinate. `tse_points` holds local_id, tse_long, tse_lat.
get_station_zone <- function(tse_points, tract_shp) {
  pts <- tse_points[!is.na(tse_long) & !is.na(tse_lat)]
  if (nrow(pts) == 0) {
    return(data.table(local_id = integer(), zone = character()))
  }
  pts_sf <- sf::st_as_sf(
    pts,
    coords = c("tse_long", "tse_lat"),
    crs = 4674
  )
  # Reproject to the tract CRS so the join is valid whatever CRS the tract layer stores.
  pts_sf <- sf::st_transform(pts_sf, sf::st_crs(tract_shp))
  # s2 off: spherical predicates over the ~316k-tract layer dominate the cost, and planar
  # GEOS containment is exact enough here. A boundary point can hit >1 tract, so
  # unique(by = "local_id") keeps one tract per station.
  old_s2 <- sf::sf_use_s2(FALSE)
  on.exit(sf::sf_use_s2(old_s2), add = TRUE)
  joined <- sf::st_join(pts_sf, tract_shp, left = TRUE)
  joined <- sf::st_drop_geometry(joined)
  unique(data.table(local_id = joined$local_id, zone = joined$zone), by = "local_id")
}

# Assign each TSE-covered municipality to one of k folds. Grouping by municipality keeps
# every station's candidate rows in one fold, so a held-out fold leaks no TSE target.
assign_eval_folds <- function(model_data, k = EVAL_N_FOLDS, seed = EVAL_FOLD_SEED) {
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
  # Fixed seed so the split is reproducible across pipeline runs.
  set.seed(seed)
  folds <- sample(rep_len(seq_len(k), n_muni))
  data.table(cod_localidade_ibge = covered_munis, fold = folds)
}

# Out-of-fold pred_dist for every covered candidate row: per fold, refit the LightGBM
# workflow on the other k-1 folds and predict the held-out one, reusing the production
# model's tuned hyperparameters. Those hyperparameters were tuned on all municipalities,
# a residual leakage channel deliberately accepted rather than paying for nested tuning.
compute_oof_predictions <- function(
  model_data,
  trained_model,
  fold_assignment,
  offset = GBM_LOG_OFFSET
) {
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
    # The recipe is dist ~ ., so `fold` must be dropped or it becomes a predictor.
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

# Per covered station, take the smallest-OOF-pred_dist candidate (ties -> first, matching
# finalize_coords()) and left-join it onto the full covered-station universe, so stations
# that never geocoded survive with a missing error and per-stratum match rates stay honest.
# Attaches the four stratification axes: vintage, region, match source, urban/rural.
select_oof_matches <- function(
  oof_predictions,
  locais,
  tsegeocoded_locais,
  tract_shp
) {
  # oof_predictions arrives sorted by (local_id, pred_dist), so the first row per station
  # is its best candidate. Its `dist` is the realized haversine error to TSE, in km.
  selected <- unique(oof_predictions, by = "local_id")[, .(
    local_id,
    match_source = match_type,
    error_km = dist,
    pred_dist,
    fold
  )]

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

# Coverage of field-collected TSE coordinates by election year x state, the ground-truth
# density each accuracy stratum is read against. Small cells are flagged, not dropped.
compute_tse_coverage <- function(
  locais,
  tsegeocoded_locais,
  min_cell_n = EVAL_MIN_CELL_N
) {
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

# Per-year ceiling on TSE coverage: the share of stations whose raw TSE file carries a
# usable coordinate. TSE geocoded stations progressively, so this ramps from ~51% (2018)
# to ~94% (2024) and each year must be read against its own ceiling. TSE encodes "no
# coordinate" as -1 or NA. Scoped to the states in `locais` so dev mode (AC/RR) compares
# like with like against national TSE files.
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

# Positional-accuracy metric ladder for one stratum. Error is right-skewed, so this
# reports percentiles and within-threshold shares (100 m / 500 m / 1 km), not a mean.
accuracy_metrics <- function(error_km) {
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
    median_km = qs[1],
    p90 = qs[2],
    p95 = qs[3],
    p99 = qs[4],
    within_100m = 100 * mean(e <= 0.1),
    within_500m = 100 * mean(e <= 0.5),
    within_1km = 100 * mean(e <= 1.0)
  )
}

# Metric ladder plus match rate for one grouping of the covered universe. `by_cols` is
# empty for the overall row. Accuracy is measured on geocoded stations; match rate is the
# share of covered stations geocoded at all, always reported alongside accuracy.
.accuracy_by <- function(dt, by_cols, min_cell_n) {
  stratum <- if (length(by_cols) == 0L) "overall" else paste(by_cols, collapse = ":")
  # Copy only for the overall case, which adds a synthetic grouping column.
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
  # Suppress noisy metrics below the cell-size floor; counts and match rate stay visible.
  res[n_geocoded < min_cell_n, (EVAL_METRIC_COLS) := NA_real_]
  res[, suppressed := n_geocoded < min_cell_n]
  res[, (by_cols) := NULL]
  setcolorder(res, c("stratum", "level", "n_total", "n_geocoded", "match_rate"))
  res[]
}

# Stratified accuracy tables over the covered set: overall, one per axis (urban/rural,
# region, vintage, match source), and two urban/rural crosses, stacked long by
# (stratum, level).
compute_accuracy_tables <- function(
  selected_matches,
  min_cell_n = EVAL_MIN_CELL_N
) {
  tabs <- list(
    .accuracy_by(selected_matches, character(0), min_cell_n),
    .accuracy_by(selected_matches, "urban_rural", min_cell_n),
    .accuracy_by(selected_matches, "region", min_cell_n),
    .accuracy_by(selected_matches, "vintage", min_cell_n),
    .accuracy_by(selected_matches, c("urban_rural", "vintage"), min_cell_n),
    .accuracy_by(selected_matches, c("urban_rural", "region"), min_cell_n)
  )

  # Match source exists only for geocoded stations, so its match rate has no denominator.
  ms <- .accuracy_by(selected_matches[geocoded == TRUE], "match_source", min_cell_n)
  ms[, match_rate := NA_real_]
  tabs[[length(tabs) + 1L]] <- ms

  rbindlist(tabs, use.names = TRUE)
}

# Check the predicted-distance ranking the pipeline trusts for match selection, two ways:
# rank-and-filter (dropping the worst-predicted tail should lower realized median error)
# and a reliability table summarized by Expected Normalized Calibration Error.
compute_calibration <- function(selected_matches, n_bins = 10L) {
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

  # N-weighted mean of the per-bin predicted-vs-realized gap, normalized by prediction.
  ence <- reliability[, sum(n * abs(mean_pred - mean_realized) / mean_pred) / sum(n)]

  list(
    rank_filter = rank_filter,
    reliability = reliability,
    ence = ence
  )
}
