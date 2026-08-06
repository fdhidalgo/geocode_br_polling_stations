## Unit tests for the deterministic evaluation-harness helpers (R/evaluation.R).
## These avoid the heavy pipeline (no model fitting, no spatial joins): they check
## the metric ladder, region mapping, coverage counting, fold assignment, the
## trivial-heuristic baseline selector and its comparison table, and the calibration
## rank-and-filter logic with tiny synthetic inputs.

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
  # pred_dist perfectly ranks error: dropping worst-predicted lowers realized error
  n <- 200
  sel <- data.table(
    geocoded = TRUE,
    pred_dist = seq_len(n) / 100,
    error_km = seq_len(n) / 100
  )
  cal <- compute_calibration(sel)
  rf <- cal$rank_filter
  expect_true(all(diff(rf$median_km) <= 0)) # median monotonically down
  expect_true(all(diff(rf$within_500m) >= 0)) # within-500m monotonically up
  expect_true(is.finite(cal$ence))
})

test_that("select_baseline_candidates prefers source precedence over string distance", {
  md <- data.table(
    local_id = c(1L, 1L, 2L, 2L),
    # station 1: a far-better neighborhood string match must still lose to the school match
    # station 2: no school candidate, so the street aggregate wins over the neighborhood
    type = c("schools_inep_name", "bairro_cnefe_2022", "st_cnefe_2022", "bairro_cnefe_2022"),
    mindist = c(0.9, 0.01, 0.5, 0.1),
    dist = c(0.2, 5.0, 0.4, 3.0)
  )
  sel <- select_baseline_candidates(md)
  expect_equal(nrow(sel), 2L)
  expect_equal(sel[local_id == 1L]$match_source, "schools_inep_name")
  expect_equal(sel[local_id == 1L]$error_km, 0.2)
  expect_equal(sel[local_id == 2L]$match_source, "st_cnefe_2022")
  expect_setequal(names(sel), c("local_id", "match_source", "error_km"))
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

test_that("select_baseline_candidates covers every type the modeling table emits", {
  # the rank table is the baseline's whole definition; a new candidate source must be
  # ranked deliberately rather than silently landing at the bottom
  expect_setequal(
    names(BASELINE_SOURCE_RANK),
    c(
      "schools_inep_name",
      "schools_inep_addr",
      "schools_cnefe_name_2010",
      "schools_cnefe_name_2022",
      "st_cnefe_2010",
      "st_cnefe_2022",
      "st_agrocnefe_2017",
      "bairro_cnefe_2010",
      "bairro_cnefe_2022",
      "bairro_agrocnefe_2017",
      "geocodebr"
    )
  )
})

test_that("select_baseline_candidates drops uncovered rows and rejects unknown types", {
  md <- data.table(
    local_id = c(1L, 2L),
    type = "st_cnefe_2022",
    mindist = 0.1,
    dist = c(0.5, NA_real_) # station 2 has no TSE coordinate
  )
  expect_equal(select_baseline_candidates(md)$local_id, 1L)

  unknown <- data.table(local_id = 1L, type = "brand_new_source", mindist = 0.1, dist = 0.5)
  expect_error(select_baseline_candidates(unknown), "BASELINE_SOURCE_RANK")
})

test_that("attach_eval_universe keeps ungeocoded covered stations", {
  universe <- data.table(
    local_id = 1:3,
    cod_localidade_ibge = 100L,
    vintage = 2018L,
    sg_uf = "AC",
    region = "Norte",
    urban_rural = "urban"
  )
  selected <- data.table(local_id = 1:2, match_source = "st", error_km = c(0.1, 0.2))
  out <- attach_eval_universe(selected, universe)
  expect_equal(nrow(out), 3L)
  expect_equal(out$geocoded, c(TRUE, TRUE, FALSE))
  expect_true(is.na(out[local_id == 3L]$error_km))

  dupes <- data.table(local_id = c(1L, 1L), match_source = "st", error_km = c(0.1, 0.2))
  expect_error(attach_eval_universe(dupes, universe), "duplicate stations")
})

test_that("compare_to_baseline signs deltas so the model's advantage is visible", {
  # the match_source rows differ by selector on purpose: each names its own picks
  mk <- function(median_km, within_500m, sources) {
    data.table(
      stratum = c("overall", "urban_rural", rep("match_source", length(sources))),
      level = c("all", "urban", sources),
      n_total = c(100L, 60L, rep(40L, length(sources))),
      n_geocoded = c(80L, 50L, rep(40L, length(sources))),
      median_km = c(median_km, rep(0.3, length(sources))),
      within_500m = c(within_500m, rep(60, length(sources))),
      suppressed = FALSE
    )
  }
  model <- mk(c(0.2, 0.1), c(70, 80), c("schools_inep_name", "st_cnefe_2022"))
  baseline <- mk(c(0.5, 0.4), c(55, 60), c("bairro_cnefe_2022"))
  cmp <- compare_to_baseline(model, baseline)

  # the source cut is dropped, not compared across selectors that partition differently
  expect_false("match_source" %in% cmp$stratum)
  expect_equal(nrow(cmp), 2L)

  expect_equal(cmp[level == "all"]$delta_median_km, -0.3) # model closer to truth
  expect_equal(cmp[level == "all"]$delta_within_500m, 15) # model more often within 500 m
  # counts and suppression are shared, not duplicated per selector
  expect_equal(cmp$n_geocoded, c(80L, 50L))
  expect_setequal(
    names(cmp),
    c(
      "stratum",
      "level",
      "n_total",
      "n_geocoded",
      "suppressed",
      "median_km_baseline",
      "median_km_model",
      "delta_median_km",
      "within_500m_baseline",
      "within_500m_model",
      "delta_within_500m"
    )
  )

  # the two selectors rank the same candidates, so a differing geocoded count is a bug
  wrong <- copy(baseline)[stratum == "overall", n_geocoded := 79L]
  expect_error(compare_to_baseline(model, wrong), "which stations geocoded")
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
