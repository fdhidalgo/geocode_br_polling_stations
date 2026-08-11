## Unit tests for the deterministic evaluation-harness helpers (R/evaluation.R).
## These avoid the heavy pipeline (no model fitting, no spatial joins): they check
## the metric ladder, region mapping, coverage counting, fold assignment, the
## trivial-heuristic baseline selector and its comparison table, the geocodebr
## selector and its head-to-head table, and the calibration rank-and-filter and
## coverage logic with tiny synthetic inputs.

library(testthat)
library(data.table)

test_that("state_to_region maps all 27 UFs and errors on unknown codes", {
  ufs <- c(
    "AC",
    "AP",
    "AM",
    "PA",
    "RO",
    "RR",
    "TO",
    "AL",
    "BA",
    "CE",
    "MA",
    "PB",
    "PE",
    "PI",
    "RN",
    "SE",
    "DF",
    "GO",
    "MT",
    "MS",
    "ES",
    "MG",
    "RJ",
    "SP",
    "PR",
    "RS",
    "SC"
  )
  regions <- state_to_region(ufs)
  expect_length(regions, 27L)
  expect_false(anyNA(regions))
  expect_setequal(
    unique(regions),
    c("Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul")
  )
  expect_equal(state_to_region("SP"), "Sudeste")
  expect_error(state_to_region("ZZ"), "unmapped UF")
})

test_that("accuracy_metrics computes the ladder and thresholds", {
  # errors in km: half within 100 m, all within 1 km
  e <- c(0.05, 0.05, 0.6, 1.0)
  m <- accuracy_metrics(e)
  expect_equal(m$median_km, median(e))
  expect_equal(m$within_100m, 50) # 2 of 4 <= 0.1 km
  expect_equal(m$within_500m, 50) # 2 of 4 <= 0.5 km
  expect_equal(m$within_1km, 100) # all <= 1 km
  # empty stratum -> all NA, no error
  empty <- accuracy_metrics(numeric(0))
  expect_true(all(vapply(empty, is.na, logical(1))))
})

test_that("assign_eval_folds is deterministic and never splits a municipality", {
  md <- data.table(
    cod_localidade_ibge = rep(1:20, each = 3),
    dist = 1 # all covered
  )
  f1 <- assign_eval_folds(md)
  f2 <- assign_eval_folds(md)
  expect_identical(f1, f2) # deterministic
  expect_equal(uniqueN(f1$cod_localidade_ibge), 20L)
  expect_equal(sort(unique(f1$fold)), 1:5) # every fold used
  # one fold per municipality => a municipality's rows can't be split
  expect_equal(nrow(f1), uniqueN(f1$cod_localidade_ibge))
  # fewer covered municipalities than folds is an error, not a silent short split
  too_few <- data.table(cod_localidade_ibge = 1:3, dist = 1)
  expect_error(assign_eval_folds(too_few), "covered municipalities")
})

test_that("assign_eval_folds ignores uncovered municipalities", {
  md <- data.table(
    cod_localidade_ibge = c(1:10, 100:105),
    dist = c(rep(1, 10), rep(NA_real_, 6)) # 100:105 uncovered
  )
  f <- assign_eval_folds(md)
  expect_equal(sort(unique(f$cod_localidade_ibge)), 1:10)
})

test_that("compute_tse_coverage counts and flags small cells", {
  locais <- data.table(
    local_id = 1:10,
    ano = c(rep(2018L, 6), rep(2020L, 4)),
    sg_uf = "AC"
  )
  tse <- data.table(local_id = c(1:4)) # 4 covered, all 2018
  cov <- compute_tse_coverage(locais, tse)
  y18 <- cov[ano == 2018L]
  y20 <- cov[ano == 2020L]
  expect_equal(y18$n_total, 6L)
  expect_equal(y18$n_covered, 4L)
  expect_equal(y20$n_covered, 0L) # no covered stations in 2020
  expect_true(y18$suppressed) # 4 < the 50-station suppression floor
  expect_equal(round(y18$coverage_pct, 2), round(100 * 4 / 6, 2))
})

test_that("compute_calibration rank-and-filter improves as tail is dropped", {
  # the bound perfectly ranks error: dropping the worst-scored tail lowers realized error
  n <- 200
  sel <- data.table(
    geocoded = TRUE,
    urban_rural = "urban",
    region = "Norte",
    vintage = 2018L,
    conf_dist_km = seq_len(n) / 100,
    error_km = seq_len(n) / 100
  )
  cal <- compute_calibration(sel)
  rf <- cal$rank_filter
  expect_true(all(diff(rf$median_km) <= 0)) # median monotonically down
  expect_true(all(diff(rf$within_500m) >= 0)) # within-500m monotonically up
})

test_that("compute_calibration measures coverage as the share inside the bound", {
  # 180 of 200 stations land inside their bound - exactly the nominal 90%.
  n <- 200L
  n_covered <- 180L
  sel <- data.table(
    geocoded = TRUE,
    urban_rural = rep(c("urban", "rural"), each = n / 2),
    region = "Norte",
    vintage = 2018L,
    # Two bound widths, both comfortably above the hits and below the misses, so the
    # coverage arithmetic stays hand-checkable while the bound still has a distribution.
    conf_dist_km = rep(c(1, 1.5), length.out = n),
    # The misses are all rural, so the strata must disagree: marginal coverage holding
    # while a stratum fails is the failure mode the cut exists to surface.
    error_km = c(rep(0.5, n / 2), rep(0.5, n_covered - n / 2), rep(2, n - n_covered))
  )
  cal <- compute_calibration(sel)

  expect_equal(cal$nominal, 90)
  overall <- cal$coverage[stratum == "overall"]
  expect_equal(overall$n, n)
  expect_equal(overall$coverage, 100 * n_covered / n)
  expect_equal(overall$median_bound_km, 1.25) # sharpness reported alongside coverage

  by_zone <- cal$coverage[stratum == "urban_rural"]
  expect_equal(by_zone[level == "urban"]$coverage, 100)
  expect_equal(by_zone[level == "rural"]$coverage, 80)
})

test_that("select_baseline_candidates prefers source precedence over string distance", {
  md <- data.table(
    local_id = c(1L, 1L, 2L, 2L, 3L),
    # station 1: a far-better neighborhood string match must still lose to the school match
    # station 2: no school candidate, so the street aggregate wins over the neighborhood
    # station 3: no TSE coordinate, so it is not scored at all
    type = c(
      "schools_inep_name",
      "bairro_cnefe_2022",
      "st_cnefe_2022",
      "bairro_cnefe_2022",
      "st_cnefe_2022"
    ),
    mindist = c(0.9, 0.01, 0.5, 0.1, 0.1),
    dist = c(0.2, 5.0, 0.4, 3.0, NA_real_)
  )
  sel <- select_baseline_candidates(md)
  expect_equal(sel$local_id, c(1L, 2L))
  expect_equal(sel[local_id == 1L]$match_source, "schools_inep_name")
  expect_equal(sel[local_id == 1L]$error_km, 0.2)
  expect_equal(sel[local_id == 2L]$match_source, "st_cnefe_2022")

  # a new candidate source must be ranked deliberately, not default to the bottom
  unknown <- data.table(local_id = 1L, type = "brand_new_source", mindist = 0.1, dist = 0.5)
  expect_error(select_baseline_candidates(unknown), "unranked candidate type")
})

test_that("select_baseline_candidates breaks ties within a rank on mindist", {
  # the three street aggregates share rank 5, so mindist decides between the vintages
  md <- data.table(
    local_id = 1L,
    type = c("st_cnefe_2010", "st_agrocnefe_2017", "st_cnefe_2022"),
    mindist = c(0.4, 0.05, NA_real_),
    dist = c(2.0, 0.3, 9.0)
  )
  sel <- select_baseline_candidates(md)
  expect_equal(sel$match_source, "st_agrocnefe_2017") # smallest mindist in the rank
  expect_equal(sel$error_km, 0.3) # unscored candidate ranks last
})

test_that("compare_to_baseline signs deltas so the model's advantage is visible", {
  # the match_source rows differ by selector on purpose: each names its own picks
  model <- data.table(
    stratum = c("overall", "match_source"),
    level = c("all", "schools_inep_name"),
    n_total = c(100L, 40L),
    n_geocoded = c(80L, 40L),
    median_km = c(0.2, 0.3),
    within_500m = c(70, 60),
    suppressed = FALSE
  )
  baseline <- copy(model)
  baseline[, level := c("all", "bairro_cnefe_2022")]
  baseline[, median_km := c(0.5, 0.3)]
  baseline[, within_500m := c(55, 60)]

  cmp <- compare_to_baseline(model, baseline)
  # the source cut is dropped, not compared across selectors that partition differently
  expect_equal(cmp$stratum, "overall")
  expect_equal(cmp$delta_median_km, -0.3) # model closer to truth
  expect_equal(cmp$delta_within_500m, 15) # model more often within 500 m

  # the two selectors rank the same candidates, so a differing geocoded count is a bug
  wrong <- copy(baseline)[stratum == "overall", n_geocoded := 79L]
  expect_error(compare_to_baseline(model, wrong), "which stations geocoded")
})

test_that("select_geocodebr_candidates keeps covered geocodebr rows with their tier", {
  md <- data.table(
    local_id = c(1L, 1L, 2L, 3L),
    type = c("geocodebr", "st_cnefe_2022", "geocodebr", "geocodebr"),
    # station 3 has no TSE coordinate, so it is not scored at all
    dist = c(0.3, 0.9, 1.2, NA_real_)
  )
  gb <- data.table(
    local_id = 1:3,
    precisao_geocodebr = c("numero", "localidade", "municipio")
  )
  sel <- select_geocodebr_candidates(md, gb)
  expect_equal(sel$local_id, c(1L, 2L))
  expect_equal(sel$error_km, c(0.3, 1.2)) # geocodebr's row, not the CNEFE candidate's
  expect_equal(sel$match_source, c("numero", "localidade"))

  # a scored candidate that lost its tier is a broken join, not a station to score anyway
  expect_error(select_geocodebr_candidates(md, gb[local_id != 1L]), "lost its precision tier")
  no_tier <- copy(gb)[local_id == 1L, precisao_geocodebr := NA_character_]
  expect_error(select_geocodebr_candidates(md, no_tier), "without a precision tier")
})

test_that("compare_geocodebr_to_model scores both selectors on both-geocoded stations", {
  n <- 60L
  ids <- seq_len(2L * n)
  # First block: the model's winning match comes from the 2022 CNEFE street table (the
  # subset the substitution decision turns on). Second block: it comes from INEP schools.
  model_source <- rep(c("st_cnefe_2022", "schools_inep_name"), each = n)
  # geocodebr resolves every station but the last of each block.
  gb_geocoded <- rep(TRUE, 2L * n)
  gb_geocoded[c(n, 2L * n)] <- FALSE

  gb <- data.table(
    local_id = ids,
    urban_rural = "urban",
    region = "Norte",
    match_source = fifelse(gb_geocoded, "numero", NA_character_),
    error_km = fifelse(gb_geocoded, 0.2, NA_real_),
    geocoded = gb_geocoded
  )
  model <- data.table(
    local_id = ids,
    match_source = model_source,
    error_km = 0.8,
    geocoded = TRUE
  )

  cmp <- compare_geocodebr_to_model(gb, model)

  overall <- cmp[universe == "all_covered" & stratum == "overall"]
  expect_equal(overall$n_stations, 120L)
  expect_equal(overall$n_geocodebr, 118L) # coverage reported, not folded into the metric
  expect_equal(overall$n_model, 120L)
  expect_equal(overall$n_both, 118L)
  # deltas are geocodebr minus model, so geocodebr's advantage is a negative median delta
  # and a positive within-500 m delta
  expect_equal(overall$delta_median_km, 0.2 - 0.8)
  expect_equal(overall$delta_within_500m, 100)

  # the gate subset holds only the stations the 2022 CNEFE tables currently win
  subset_overall <- cmp[universe == "cnefe22_winner" & stratum == "overall"]
  expect_equal(subset_overall$n_stations, 60L)
  expect_equal(subset_overall$n_both, 59L)

  # a station geocodebr never resolved gets a named tier rather than dropping out
  tiers <- cmp[universe == "all_covered" & stratum == "geocodebr_tier"]
  expect_setequal(tiers$level, c("numero", "sem_resultado"))
  expect_equal(tiers[level == "sem_resultado"]$n_both, 0L)
  expect_true(tiers[level == "sem_resultado"]$suppressed)

  # the two selectors must be scored on the same universe, or the pairing is meaningless
  expect_error(compare_geocodebr_to_model(gb[-1L], model), "different station universes")
})

## Fixture for compute_panel_coord_accuracy(): `n_panels` two-year panels, all covered.
## Both members sit at the same truth. The 2018 member's out-of-fold coordinate is exact
## but carries the wide bound; the 2022 member's is ~1.1 km off with a tight bound, so the
## two ranking rules pick opposite years unless `agree` flips the bounds.
panel_accuracy_fixture <- function(n_panels = 30L, agree = FALSE) {
  n <- 2L * n_panels
  ids <- seq_len(n)
  first <- rep(c(TRUE, FALSE), n_panels)
  truth_long <- rep(-60 + seq_len(n_panels) / 100, each = 2L)
  list(
    panel_ids_combined = data.table(
      local_id = ids,
      panel_id = rep(sprintf("p%02d", seq_len(n_panels)), each = 2L)
    ),
    oof_predictions = data.table(
      local_id = ids,
      long = truth_long,
      lat = fifelse(first, -9, -9 + 0.01),
      pred_logmean = fifelse(first, -2, -1),
      conf_dist_km = if (agree) fifelse(first, 0.5, 4) else fifelse(first, 4, 0.5)
    ),
    eval_station_universe = data.table(
      local_id = ids,
      vintage = fifelse(first, 2018L, 2022L),
      urban_rural = "urban",
      region = "Norte"
    ),
    tsegeocoded_locais = data.table(
      local_id = ids,
      tse_long = truth_long,
      tse_lat = -9
    )
  )
}

test_that("compute_panel_coord_accuracy scores both ranking rules on panel members", {
  fx <- panel_accuracy_fixture()
  out <- do.call(compute_panel_coord_accuracy, fx)

  overall <- out[stratum == "overall"]
  expect_equal(overall$n_stations, 60L)
  expect_equal(overall$n_panels, 30L)
  # Every panel ships a different coordinate under the two rules.
  expect_equal(overall$pct_changed, 100)

  # Expected error picks the exact 2018 coordinate for the whole panel; the bound picks
  # the 2022 one, which misses both members by ~1.1 km.
  expect_equal(overall$median_km_expected, 0)
  expect_equal(overall$median_km_bound, 1.11, tolerance = 0.01)
  expect_lt(overall$delta_median_km, 0) # shipped rule closer to truth
  expect_equal(overall$within_500m_expected, 100)
  expect_equal(overall$within_500m_bound, 0)
  expect_equal(overall$delta_within_500m, 100)

  # Every member is scored, including the year whose own coordinate did not win.
  expect_equal(out[stratum == "vintage", sum(n_stations)], 60L)
})

test_that("compute_panel_coord_accuracy reports no change when the rules agree", {
  out <- do.call(compute_panel_coord_accuracy, panel_accuracy_fixture(agree = TRUE))
  overall <- out[stratum == "overall"]
  expect_equal(overall$pct_changed, 0)
  expect_equal(overall$delta_median_km, 0)
})

test_that("compute_accuracy_tables reports match rate and suppresses small cells", {
  dt <- data.table(
    local_id = 1:8,
    urban_rural = rep(c("urban", "rural"), each = 4),
    region = "Norte",
    vintage = 2018L,
    match_source = c(rep("inep", 3), NA, rep("cnefe", 2), NA, NA),
    error_km = c(0.05, 0.2, 0.9, NA, 0.1, 0.4, NA, NA),
    geocoded = c(TRUE, TRUE, TRUE, FALSE, TRUE, TRUE, FALSE, FALSE)
  )
  tabs <- compute_accuracy_tables(dt)
  overall <- tabs[stratum == "overall"]
  expect_equal(overall$n_total, 8L)
  expect_equal(overall$n_geocoded, 5L)
  expect_equal(overall$match_rate, 100 * 5 / 8)
  expect_true(overall$suppressed) # 5 geocoded < the 50-station floor
  expect_true(is.na(overall$median_km)) # accuracy suppressed
  # match-source cut carries NA match rate (denominator is geocoded-only)
  ms <- tabs[stratum == "match_source"]
  expect_true(all(is.na(ms$match_rate)))
})
