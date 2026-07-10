## Unit tests for the deterministic evaluation-harness helpers (R/evaluation.R).
## These avoid the heavy pipeline (no model fitting, no spatial joins): they check
## the metric ladder, region mapping, coverage counting, fold assignment, and the
## calibration rank-and-filter logic with tiny synthetic inputs.

library(testthat)
library(data.table)

if (!exists("accuracy_metrics")) {
  # Works whether run from the repo root or from tests/testthat (test_dir wd).
  eval_path <- if (file.exists("R/evaluation.R")) {
    "R/evaluation.R"
  } else {
    file.path("..", "..", "R", "evaluation.R")
  }
  source(eval_path)
}

test_that("state_to_region maps all 27 UFs and errors on unknown codes", {
  ufs <- c("AC", "AP", "AM", "PA", "RO", "RR", "TO",
           "AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE",
           "DF", "GO", "MT", "MS",
           "ES", "MG", "RJ", "SP",
           "PR", "RS", "SC")
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
  expect_equal(m$within_100m, 50)   # 2 of 4 <= 0.1 km
  expect_equal(m$within_500m, 50)   # 2 of 4 <= 0.5 km
  expect_equal(m$within_1km, 100)   # all <= 1 km
  # empty stratum -> all NA, no error
  empty <- accuracy_metrics(numeric(0))
  expect_true(all(vapply(empty, is.na, logical(1))))
})

test_that("assign_eval_folds is deterministic and never splits a municipality", {
  md <- data.table(
    cod_localidade_ibge = rep(1:20, each = 3),
    dist = 1  # all covered
  )
  f1 <- assign_eval_folds(md, k = 5L, seed = 123L)
  f2 <- assign_eval_folds(md, k = 5L, seed = 123L)
  expect_identical(f1, f2)                       # deterministic
  expect_equal(uniqueN(f1$cod_localidade_ibge), 20L)
  expect_equal(sort(unique(f1$fold)), 1:5)       # every fold used
  # one fold per municipality => a municipality's rows can't be split
  expect_equal(nrow(f1), uniqueN(f1$cod_localidade_ibge))
  expect_error(assign_eval_folds(md, k = 25L), "covered municipalities")
})

test_that("assign_eval_folds ignores uncovered municipalities", {
  md <- data.table(
    cod_localidade_ibge = c(1:10, 100:105),
    dist = c(rep(1, 10), rep(NA_real_, 6))  # 100:105 uncovered
  )
  f <- assign_eval_folds(md, k = 5L)
  expect_equal(sort(unique(f$cod_localidade_ibge)), 1:10)
})

test_that("compute_tse_coverage counts and flags small cells", {
  locais <- data.table(
    local_id = 1:10,
    ano = c(rep(2018L, 6), rep(2020L, 4)),
    sg_uf = "AC"
  )
  tse <- data.table(local_id = c(1:4))  # 4 covered, all 2018
  cov <- compute_tse_coverage(locais, tse, min_cell_n = 5L)
  y18 <- cov[ano == 2018L]
  y20 <- cov[ano == 2020L]
  expect_equal(y18$n_total, 6L)
  expect_equal(y18$n_covered, 4L)
  expect_equal(y20$n_covered, 0L)             # no covered stations in 2020
  expect_true(y18$suppressed)                 # 4 < floor of 5
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
  cal <- compute_calibration(sel, n_bins = 5L)
  rf <- cal$rank_filter
  expect_true(all(diff(rf$median_km) <= 0))       # median monotonically down
  expect_true(all(diff(rf$within_500m) >= 0))      # within-500m monotonically up
  expect_true(is.finite(cal$ence))
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
  tabs <- compute_accuracy_tables(dt, min_cell_n = 100L)
  overall <- tabs[stratum == "overall"]
  expect_equal(overall$n_total, 8L)
  expect_equal(overall$n_geocoded, 5L)
  expect_equal(overall$match_rate, 100 * 5 / 8)
  expect_true(overall$suppressed)                  # 5 geocoded < floor 100
  expect_true(is.na(overall$median_km))            # accuracy suppressed
  # match-source cut carries NA match rate (denominator is geocoded-only)
  ms <- tabs[stratum == "match_source"]
  expect_true(all(is.na(ms$match_rate)))
})
