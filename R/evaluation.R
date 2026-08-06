## Geocoding accuracy and pred_dist calibration, measured on the TSE-covered subset
## with out-of-fold predictions. Errors are haversine distances in kilometres.

library(data.table)

## Cells with fewer than this many geocoded stations have their accuracy metrics
## suppressed: the percentile ladder is too noisy to report at that size.
EVAL_MIN_CELL_N <- 50L

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

## Candidate-type precedence for the trivial baseline, most specific reference first:
## a school's own registered address point, then a school establishment in the census,
## then a school looked up by its address line, then a geocoded address, and last the
## two aggregates that stand in for an address rather than locating it (a street's
## median coordinate, a neighborhood's).
##
## Census vintages of the same reference share a rank -- they are the same kind of
## reference differing only in year, so the mindist tie-break decides between them, and
## within a rank that comparison is like-for-like (same matcher, same field, same
## normalization). Every type in the modeling table must appear here;
## select_baseline_candidates() errors if one does not.
BASELINE_SOURCE_RANK <- c(
  schools_inep_name = 1L,
  schools_cnefe_name_2022 = 2L,
  schools_cnefe_name_2010 = 2L,
  schools_inep_addr = 3L,
  geocodebr = 4L,
  st_cnefe_2022 = 5L,
  st_cnefe_2010 = 5L,
  st_agrocnefe_2017 = 5L,
  bairro_cnefe_2022 = 6L,
  bairro_cnefe_2010 = 6L,
  bairro_agrocnefe_2017 = 6L
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
assign_eval_folds <- function(model_data) {
  k <- 5L
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
  set.seed(20260710L)
  folds <- sample(rep_len(seq_len(k), n_muni))
  data.table(cod_localidade_ibge = covered_munis, fold = folds)
}

# Out-of-fold pred_dist for every covered candidate row: per fold, refit the LightGBM
# workflow on the other k-1 folds and predict the held-out one, reusing the production
# model's tuned hyperparameters. Those hyperparameters were tuned on all municipalities,
# a residual leakage channel deliberately accepted rather than paying for nested tuning.
compute_oof_predictions <- function(model_data, trained_model, fold_assignment) {
  covered <- model_data[!is.na(dist)]
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
    test_df[, pred_dist := exp(pred_logdist) - GBM_LOG_OFFSET]
    preds[[i]] <- test_df[, .(
      local_id,
      cod_localidade_ibge,
      match_type = type,
      mindist,
      desvio_km,
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

# Every TSE-covered station with the stratification axes accuracy is cut by: vintage,
# region, urban/rural. This is the denominator both selectors are scored against, built
# once because the urban/rural axis costs a point-in-tract join over the whole tract layer.
build_eval_universe <- function(locais, tsegeocoded_locais, tract_shp) {
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
  universe[]
}

# Left-join one selector's per-station picks onto the covered universe, so stations that
# never geocoded survive with a missing error and per-stratum match rates stay honest.
attach_eval_universe <- function(selected, eval_universe) {
  stopifnot("selector returned duplicate stations" = !anyDuplicated(selected$local_id))
  out <- merge(eval_universe, selected, by = "local_id", all.x = TRUE)
  out[, geocoded := !is.na(error_km)]
  out[]
}

# Per covered station, the smallest-OOF-pred_dist candidate (ties -> first, matching
# finalize_coords()).
select_oof_candidates <- function(oof_predictions) {
  # oof_predictions arrives sorted by (local_id, pred_dist), so the first row per station
  # is its best candidate. Its `dist` is the realized haversine error to TSE, in km.
  unique(oof_predictions, by = "local_id")[, .(
    local_id,
    match_source = match_type,
    error_km = dist,
    pred_dist,
    fold
  )]
}

# The trivial deterministic selector the model has to beat: per covered station, take the
# highest-precedence candidate available, breaking ties within a rank on the smallest
# string distance. Scored on the same covered candidate rows and the same station universe
# as the out-of-fold model picks, so the two are directly comparable. It trains on nothing,
# so it has no fold structure and nothing to hold out.
#
# The tie-break stays inside a rank on purpose: mindist is not comparable across ranks
# (length-normalized Jaro-Winkler for most, unnormalized for bairro, over different fields,
# and absent for geocodebr), so a cross-rank argmin would be comparing different scales.
select_baseline_candidates <- function(model_data) {
  covered <- model_data[
    !is.na(dist),
    .(local_id, match_source = type, error_km = dist, mindist)
  ]
  covered[, source_rank := unname(BASELINE_SOURCE_RANK[match_source])]
  stopifnot(
    "candidate type missing from BASELINE_SOURCE_RANK" = !anyNA(covered$source_rank)
  )
  # na.last keeps geocodebr's absent mindist from outranking a scored candidate of the
  # same type; it has one candidate per station, so in practice nothing is ordered by it.
  setorder(covered, local_id, source_rank, mindist, na.last = TRUE)
  unique(covered, by = "local_id")[, .(local_id, match_source, error_km)]
}

# Coverage of field-collected TSE coordinates by election year x state, the ground-truth
# density each accuracy stratum is read against. Small cells are flagged, not dropped.
compute_tse_coverage <- function(locais, tsegeocoded_locais) {
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
  cov[, suppressed := n_covered < EVAL_MIN_CELL_N]
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
.accuracy_by <- function(dt, by_cols) {
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
  res[n_geocoded < EVAL_MIN_CELL_N, (EVAL_METRIC_COLS) := NA_real_]
  res[, suppressed := n_geocoded < EVAL_MIN_CELL_N]
  res[, (by_cols) := NULL]
  setcolorder(res, c("stratum", "level", "n_total", "n_geocoded", "match_rate"))
  res[]
}

# Stratified accuracy tables over the covered set: overall, one per axis (urban/rural,
# region, vintage, match source), and two urban/rural crosses, stacked long by
# (stratum, level).
compute_accuracy_tables <- function(selected_matches) {
  tabs <- list(
    .accuracy_by(selected_matches, character(0)),
    .accuracy_by(selected_matches, "urban_rural"),
    .accuracy_by(selected_matches, "region"),
    .accuracy_by(selected_matches, "vintage"),
    .accuracy_by(selected_matches, c("urban_rural", "vintage")),
    .accuracy_by(selected_matches, c("urban_rural", "region"))
  )

  # Match source exists only for geocoded stations, so its match rate has no denominator.
  ms <- .accuracy_by(selected_matches[geocoded == TRUE], "match_source")
  ms[, match_rate := NA_real_]
  tabs[[length(tabs) + 1L]] <- ms

  rbindlist(tabs, use.names = TRUE)
}

# Model-vs-baseline accuracy on the spec's two headline metrics, stratum by stratum.
# Deltas are signed so that better-than-baseline is negative for median error and positive
# for %-within-500 m. This is the number that answers "does the selection model earn its
# keep over a fixed source precedence"; it changes no production behavior.
#
# The match_source cut is excluded: each selector partitions the stations by whichever
# source it picked, so its levels hold different stations under the two selectors and a
# per-level delta would not be a like-for-like comparison. Every other stratum is a fixed
# partition of the covered universe, which is what makes the alignment checks below hold.
compare_to_baseline <- function(accuracy_tables, baseline_accuracy_tables) {
  keep <- c("stratum", "level", "n_total", "n_geocoded", "median_km", "within_500m", "suppressed")
  model <- accuracy_tables[stratum != "match_source", ..keep]
  baseline <- baseline_accuracy_tables[stratum != "match_source", ..keep]

  cmp <- merge(model, baseline, by = c("stratum", "level"), suffixes = c("_model", "_baseline"))
  # Both selectors rank the same candidate rows, so a station geocodes under one exactly
  # when it geocodes under the other: the comparison is pure accuracy at a fixed match rate.
  stopifnot(
    "model and baseline strata do not align" = nrow(cmp) == nrow(model),
    "model and baseline disagree on which stations geocoded" = identical(cmp$n_geocoded_model, cmp$n_geocoded_baseline)
  )

  cmp[, delta_median_km := median_km_model - median_km_baseline]
  cmp[, delta_within_500m := within_500m_model - within_500m_baseline]
  cmp[, c("n_total_baseline", "n_geocoded_baseline", "suppressed_baseline") := NULL]
  setnames(
    cmp,
    c("n_total_model", "n_geocoded_model", "suppressed_model"),
    c("n_total", "n_geocoded", "suppressed")
  )
  setcolorder(
    cmp,
    c(
      "stratum",
      "level",
      "n_total",
      "n_geocoded",
      "median_km_baseline",
      "median_km_model",
      "delta_median_km",
      "within_500m_baseline",
      "within_500m_model",
      "delta_within_500m"
    )
  )
  setorder(cmp, stratum, level)
  cmp[]
}

# Check the predicted-distance ranking the pipeline trusts for match selection, two ways:
# rank-and-filter (dropping the worst-predicted tail should lower realized median error)
# and a reliability table summarized by Expected Normalized Calibration Error.
compute_calibration <- function(selected_matches) {
  n_bins <- 10L
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
