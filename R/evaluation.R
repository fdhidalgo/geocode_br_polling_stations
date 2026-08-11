## Geocoding accuracy and distance-bound calibration, measured on the TSE-covered subset
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

# Out-of-fold selection scores and distance bounds for every covered candidate row: per
# fold, refit both LightGBM workflows on the other k-1 folds and predict the held-out one,
# reusing each production model's tuned hyperparameters. Those hyperparameters were tuned
# on all municipalities, a residual leakage channel deliberately accepted rather than
# paying for nested tuning.
compute_oof_predictions <- function(model_data, trained_model, fold_assignment) {
  covered <- model_data[!is.na(dist)]
  covered <- merge(covered, fold_assignment, by = "cod_localidade_ibge")
  stopifnot(
    "every covered row must receive a fold" = !anyNA(covered$fold),
    "no covered rows to evaluate" = nrow(covered) > 0
  )

  selection_params <- tune::select_best(trained_model$selection$tune_out, metric = "rmse")
  bound_params <- tune::select_best(trained_model$bound$tune_out, metric = "pinball_loss")

  fold_ids <- sort(unique(covered$fold))
  preds <- vector("list", length(fold_ids))
  for (i in seq_along(fold_ids)) {
    f <- fold_ids[i]
    # The recipe is log_dist ~ ., so `fold` must be dropped or it becomes a predictor.
    pool <- covered[fold != f][, fold := NULL]
    test_features <- covered[fold == f, !"fold"]

    # A quarter of the fold's training municipalities calibrates the conformal offset. It
    # has to be data this fold's model never saw, or the coverage measured on the held-out
    # fold is a fit statistic rather than a test. Municipality-grouped, matching the outer
    # folds: a station's candidates must not straddle the fit/calibrate boundary either.
    inner <- rsample::group_initial_split(pool, group = cod_localidade_ibge, prop = 0.75)
    train_df <- as.data.table(rsample::training(inner))
    cal_df <- as.data.table(rsample::testing(inner))

    selection_wf <- generics::fit(
      tune::finalize_workflow(build_gbm_workflow(train_df, "regression"), selection_params),
      data = train_df
    )
    bound_wf <- generics::fit(
      tune::finalize_workflow(build_gbm_workflow(train_df, "quantile"), bound_params),
      data = train_df
    )
    offset <- conformal_log_offset(selection_wf, bound_wf, cal_df)
    message(sprintf(
      "OOF fold %d/%d: fit on %d rows, calibrated on %d (offset %.3f log-km)",
      i,
      length(fold_ids),
      nrow(train_df),
      nrow(cal_df),
      offset
    ))

    # test_features is this loop's own materialization, so it is scored in place.
    test_features[, pred_logmean := predict(selection_wf, new_data = test_features)$.pred]
    test_features[, pred_logq := predict(bound_wf, new_data = test_features)$.pred]
    test_features[, conf_dist_km := conformal_bound_km(pred_logq, offset)]
    preds[[i]] <- test_features[, .(
      local_id,
      cod_localidade_ibge,
      match_type = type,
      mindist,
      desvio_km,
      long,
      lat,
      dist,
      conf_dist_km,
      pred_logmean,
      pred_logq,
      fold = f
    )]
  }
  rbindlist(preds)
}

# Every TSE-covered station with the stratification axes accuracy is cut by: vintage,
# region, urban/rural. The denominator both selectors are scored against, built once so
# they cannot drift apart.
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

# Per covered station, the candidate the pipeline would have picked from its out-of-fold
# scores. `dist` is the realized haversine error to TSE, in km.
select_oof_candidates <- function(oof_predictions) {
  select_best_candidate(oof_predictions)[, .(
    local_id,
    match_source = match_type,
    error_km = dist,
    conf_dist_km,
    fold
  )]
}

# The trivial deterministic selector the model has to beat: per covered station, the
# highest-precedence candidate available, ties within a rank broken on the smallest
# string distance. mindist is on incomparable scales across ranks (length-normalized
# Jaro-Winkler for most sources, unnormalized for bairro, over different fields, absent
# for geocodebr), so it can only break ties inside one.
select_baseline_candidates <- function(model_data) {
  # Precedence, most specific reference first: references that locate the building, then
  # the address, then the aggregates that only stand in for it. Census vintages of the
  # same reference share a rank, so mindist picks between them.
  source_rank <- c(
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

  covered <- model_data[
    !is.na(dist),
    .(local_id, match_source = type, error_km = dist, mindist)
  ]
  covered[, rank := unname(source_rank[match_source])]
  stopifnot("unranked candidate type" = !anyNA(covered$rank))
  # na.last keeps an absent mindist from outranking a scored candidate of the same rank.
  setorder(covered, local_id, rank, mindist, na.last = TRUE)
  unique(covered, by = "local_id")[, .(local_id, match_source, error_km)]
}

# geocodebr's own coordinate for every covered station it resolved, on the error scale the
# other selectors are scored on. The precision tier is joined from geocodebr_match rather
# than carried in model_data: the model recipe is dist ~ ., so every model_data column
# becomes a predictor. It lands in `match_source` -- for a single-source selector, the
# cascade tier is the provenance label.
select_geocodebr_candidates <- function(model_data, geocodebr_match) {
  covered <- model_data[
    type == "geocodebr" & !is.na(dist),
    .(local_id, error_km = dist)
  ]
  out <- merge(
    covered,
    geocodebr_match[, .(local_id, match_source = precisao_geocodebr)],
    by = "local_id"
  )
  stopifnot(
    "geocodebr candidate lost its precision tier in the join" = nrow(out) == nrow(covered),
    "geocodebr candidate without a precision tier" = !anyNA(out$match_source)
  )
  out[]
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

# Scaffolding shared by the stratified tables below: run `cell_fn` over each level of
# `by_cols` (empty for the overall row) and stack the cells long, keyed by (stratum,
# level). `metric_cols` are blanked wherever `n_col` falls below the cell-size floor, so
# counts stay visible where the percentile ladder is too noisy to report.
.by_stratum <- function(dt, by_cols, cell_fn, metric_cols, n_col) {
  stratum <- if (length(by_cols) == 0L) "overall" else paste(by_cols, collapse = ":")
  # Copy only for the overall case, which adds a synthetic grouping column.
  if (length(by_cols) == 0L) {
    dt <- copy(dt)[, .all := "all"]
    by_cols <- ".all"
  }
  res <- dt[, cell_fn(.SD), by = by_cols]

  res[, level := do.call(paste, c(.SD, sep = ":")), .SDcols = by_cols]
  res[, stratum := stratum]
  res[get(n_col) < EVAL_MIN_CELL_N, (metric_cols) := NA_real_]
  res[, suppressed := get(n_col) < EVAL_MIN_CELL_N]
  res[, (by_cols) := NULL]
  setcolorder(res, c("stratum", "level"))
  res[]
}

# Metric ladder plus match rate for one grouping of the covered universe. Accuracy is
# measured on geocoded stations; match rate is the share of covered stations geocoded at
# all, always reported alongside accuracy.
.accuracy_by <- function(dt, by_cols) {
  res <- .by_stratum(
    dt,
    by_cols,
    function(cell) {
      ng <- sum(cell$geocoded)
      c(
        list(
          n_total = nrow(cell),
          n_geocoded = ng,
          match_rate = 100 * ng / nrow(cell)
        ),
        accuracy_metrics(cell$error_km[cell$geocoded])
      )
    },
    metric_cols = EVAL_METRIC_COLS,
    n_col = "n_geocoded"
  )
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

# Model minus baseline on the two headline metrics, stratum by stratum. Deltas are signed
# so the model's advantage reads as a negative median delta and a positive within-500 m
# delta. The match_source cut is dropped: its levels hold different stations under each
# selector, so a per-level delta would not be like-for-like.
compare_to_baseline <- function(accuracy_tables, baseline_accuracy_tables) {
  model <- accuracy_tables[
    stratum != "match_source",
    .(
      stratum,
      level,
      n_total,
      n_geocoded,
      suppressed,
      median_km_model = median_km,
      within_500m_model = within_500m
    )
  ]
  baseline <- baseline_accuracy_tables[
    stratum != "match_source",
    .(
      stratum,
      level,
      n_geocoded_baseline = n_geocoded,
      median_km_baseline = median_km,
      within_500m_baseline = within_500m
    )
  ]

  cmp <- merge(model, baseline, by = c("stratum", "level"))
  stopifnot(
    "model and baseline strata do not align" = nrow(cmp) == nrow(model),
    "model and baseline disagree on which stations geocoded" = identical(cmp$n_geocoded, cmp$n_geocoded_baseline)
  )

  cmp[, n_geocoded_baseline := NULL]
  cmp[, delta_median_km := median_km_model - median_km_baseline]
  cmp[, delta_within_500m := within_500m_model - within_500m_baseline]
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

# geocodebr's coordinates against the pipeline's selected coordinates, both measured to the
# TSE ground truth, over every covered station and over the subset the bespoke 2022 CNEFE
# street/neighborhood tables currently win. Each cell scores both selectors on the stations
# where both produced a coordinate, with each side's coverage of the whole cell alongside.
# Deltas are geocodebr minus model.
compare_geocodebr_to_model <- function(geocodebr_selected_matches, oof_selected_matches) {
  metric_cols <- c(
    "median_km_geocodebr",
    "median_km_model",
    "delta_median_km",
    "within_500m_geocodebr",
    "within_500m_model",
    "delta_within_500m"
  )

  paired <- merge(
    geocodebr_selected_matches[, .(
      local_id,
      urban_rural,
      region,
      # Stations geocodebr never resolved get a named tier, so they form a level of their own.
      geocodebr_tier = fifelse(is.na(match_source), "sem_resultado", match_source),
      geocoded_geocodebr = geocoded,
      error_km_geocodebr = error_km
    )],
    oof_selected_matches[, .(
      local_id,
      model_source = match_source,
      geocoded_model = geocoded,
      error_km_model = error_km
    )],
    by = "local_id"
  )
  stopifnot(
    "selectors were scored on different station universes" = nrow(paired) == nrow(geocodebr_selected_matches) &&
      nrow(paired) == nrow(oof_selected_matches)
  )

  cell <- function(dt) {
    both <- dt$geocoded_geocodebr & dt$geocoded_model
    g <- accuracy_metrics(dt$error_km_geocodebr[both])
    m <- accuracy_metrics(dt$error_km_model[both])
    list(
      n_stations = nrow(dt),
      n_geocodebr = sum(dt$geocoded_geocodebr),
      n_model = sum(dt$geocoded_model),
      n_both = sum(both),
      median_km_geocodebr = g$median_km,
      median_km_model = m$median_km,
      delta_median_km = g$median_km - m$median_km,
      within_500m_geocodebr = g$within_500m,
      within_500m_model = m$within_500m,
      delta_within_500m = g$within_500m - m$within_500m
    )
  }

  universes <- list(
    all_covered = paired,
    # CNEFE-2022 schools are matched on establishment name against a different table, so they
    # are not part of the proposed substitution. Every station here geocoded under the model,
    # so n_both is geocodebr's coverage of the tables it would replace.
    cnefe22_winner = paired[model_source %in% c("st_cnefe_2022", "bairro_cnefe_2022")]
  )
  strata <- list(
    character(0),
    "urban_rural",
    "region",
    c("urban_rural", "region"),
    "geocodebr_tier"
  )

  out <- rbindlist(
    lapply(names(universes), function(u) {
      rbindlist(
        lapply(strata, function(s) {
          .by_stratum(universes[[u]], s, cell, metric_cols = metric_cols, n_col = "n_both")
        }),
        use.names = TRUE
      )[, universe := u]
    }),
    use.names = TRUE
  )
  setcolorder(
    out,
    c("universe", "stratum", "level", "n_stations", "n_geocodebr", "n_model", "n_both")
  )
  out[]
}

# Metric columns of the panel-coordinate comparison, suppressed below the cell-size floor.
EVAL_PANEL_COLS <- c(
  "median_km_bound",
  "median_km_expected",
  "delta_median_km",
  "within_500m_bound",
  "within_500m_expected",
  "delta_within_500m"
)

# Panel coordinate accuracy under the two candidate ranking rules: lowest expected error
# (what the pipeline ships) against smallest published bound (what it shipped before).
# A panel gives one coordinate to all of its station-years, so the quantity measured here
# is that single coordinate against every member's TSE ground truth -- a different question
# from the station-level tables above, which score each station-year's own pick.
#
# Scored on out-of-fold model coordinates with no TSE substitution, deliberately: in
# production a panel holding any covered year ships ground truth under either rule, so the
# rules can only diverge on panels with no covered year at all -- exactly the panels that
# have no truth to be measured against. Withholding the substitution lets the covered
# panels stand in for them.
#
# Members outside the covered universe have no out-of-fold coordinate, so they neither
# compete for the panel coordinate nor are scored; a panel enters with the covered years
# it has. Deltas are expected-error minus bound, so the shipped rule's advantage reads as
# a negative median delta and a positive within-500 m delta.
compute_panel_coord_accuracy <- function(
  panel_ids_combined,
  oof_predictions,
  eval_station_universe,
  tsegeocoded_locais
) {
  picks <- select_best_candidate(oof_predictions)[, .(local_id, long, lat, pred_logmean, conf_dist_km)]

  members <- merge(
    as.data.table(panel_ids_combined)[, .(local_id, panel_id)],
    eval_station_universe[, .(local_id, vintage, urban_rural, region)],
    by = "local_id"
  )
  members <- merge(members, picks, by = "local_id")
  members <- merge(members, tsegeocoded_locais[, .(local_id, tse_long, tse_lat)], by = "local_id")
  stopifnot("no covered panel members to score" = nrow(members) > 0)

  # One rule's panel coordinate handed to every member, and each member's error under it.
  # The ordering mirrors make_panel_ids(): rank column first, ties to the most recent year.
  under_rule <- function(rank_col) {
    best <- unique(
      members[order(panel_id, get(rank_col), -vintage)],
      by = "panel_id"
    )[, .(panel_id, sel_long = long, sel_lat = lat)]
    out <- merge(members[, .(local_id, panel_id, tse_long, tse_lat)], best, by = "panel_id")
    out[, .(
      local_id,
      sel_long,
      sel_lat,
      error_km = geosphere::distHaversine(
        cbind(sel_long, sel_lat),
        cbind(tse_long, tse_lat),
        r = EARTH_RADIUS_KM
      )
    )]
  }

  expected <- under_rule("pred_logmean")
  bound <- under_rule("conf_dist_km")
  setnames(expected, c("sel_long", "sel_lat", "error_km"), c("long_e", "lat_e", "error_km_expected"))
  setnames(bound, c("sel_long", "sel_lat", "error_km"), c("long_b", "lat_b", "error_km_bound"))

  paired <- merge(
    members[, .(local_id, panel_id, urban_rural, region, vintage)],
    merge(expected, bound, by = "local_id"),
    by = "local_id"
  )
  paired[, changed := long_e != long_b | lat_e != lat_b]

  cell <- function(dt) {
    e <- accuracy_metrics(dt$error_km_expected)
    b <- accuracy_metrics(dt$error_km_bound)
    list(
      n_stations = nrow(dt),
      n_panels = uniqueN(dt$panel_id),
      # How often the two rules disagree at all: the metric deltas below are diluted by
      # every station-year where both rules ship the same coordinate.
      pct_changed = 100 * mean(dt$changed),
      median_km_bound = b$median_km,
      median_km_expected = e$median_km,
      delta_median_km = e$median_km - b$median_km,
      within_500m_bound = b$within_500m,
      within_500m_expected = e$within_500m,
      delta_within_500m = e$within_500m - b$within_500m
    )
  }

  out <- rbindlist(
    lapply(
      list(character(0), "urban_rural", "region", "vintage"),
      function(s) .by_stratum(paired, s, cell, metric_cols = EVAL_PANEL_COLS, n_col = "n_stations")
    ),
    use.names = TRUE
  )
  setcolorder(out, c("stratum", "level", "n_stations", "n_panels", "pct_changed"))
  out[]
}

# Metric columns of the coverage tables, suppressed together below the cell-size floor.
EVAL_COVERAGE_COLS <- c("coverage", "median_bound_km", "median_error_km")

# One coverage cell: the share inside the bound, plus the bound's own width and the realized
# error. Width is never omitted -- coverage alone can always be bought by widening.
.coverage_cell <- function(cell) {
  list(
    n = nrow(cell),
    coverage = 100 * mean(cell$error_km <= cell$conf_dist_km),
    median_bound_km = stats::median(cell$conf_dist_km),
    median_error_km = stats::median(cell$error_km)
  )
}

# Check the exported distance bound two ways: rank-and-filter (dropping the worst-scored
# tail should lower realized median error, so the ranking carries information) and coverage
# (does the bound hold as often as it claims, and how wide does it have to be to do it).
# Coverage is cut by stratum because the conformal guarantee is marginal, not conditional.
compute_calibration <- function(selected_matches) {
  n_bins <- 10L
  geo <- selected_matches[
    geocoded == TRUE,
    .(urban_rural, region, vintage, conf_dist_km, error_km)
  ]
  stopifnot("a geocoded station reached calibration without a bound" = !anyNA(geo$conf_dist_km))
  setorder(geo, conf_dist_km)
  n <- nrow(geo)

  # Rank-and-filter: retain the best-scored (1 - drop) share.
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

  coverage <- rbindlist(
    lapply(
      list(character(0), "urban_rural", "region", "vintage"),
      function(s) .by_stratum(geo, s, .coverage_cell, metric_cols = EVAL_COVERAGE_COLS, n_col = "n")
    ),
    use.names = TRUE
  )

  # Coverage across the range of the bound itself: an adaptive bound has to hold at both
  # ends, not only on average.
  breaks <- unique(stats::quantile(
    geo$conf_dist_km,
    probs = seq(0, 1, length.out = n_bins + 1L),
    names = FALSE
  ))
  geo[, bin := cut(conf_dist_km, breaks = breaks, include.lowest = TRUE, labels = FALSE)]
  coverage_by_bound <- geo[, .coverage_cell(.SD), by = bin][order(bin)]

  list(
    rank_filter = rank_filter,
    coverage = coverage,
    coverage_by_bound = coverage_by_bound,
    nominal = 100 * SELECTOR_QUANTILE
  )
}
