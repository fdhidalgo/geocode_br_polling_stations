## Spec tests for validate_release_gates() (R/validation.R), the fail-loud
## structural tripwires on the 2006-2024 re-release rebuild (release spec
## docs/specs/2026-07-2024-release-spec.md, Validation gates; #48). The gate is
## the last thing standing between a broken build and shipping, so each gate must
## stop() loudly when its invariant is violated.

# A minimal but complete geocoded_locais with every election year present, a
# non-empty 2024 partition, the full documented schema, and real coordinates.
make_geocoded_fixture <- function() {
  years <- seq(2006L, 2024L, by = 2L)
  dt <- data.table::rbindlist(lapply(years, function(y) {
    data.table::data.table(
      cd_localidade_tse = 1120L,
      ano = y,
      nr_zona = 1L,
      nr_locvot = seq_len(3L),
      nr_cep = 69900000L,
      sg_uf = "AC",
      nm_localidade = "RIO BRANCO",
      nm_locvot = "ESCOLA",
      ds_endereco = "RUA X 100",
      ds_bairro = "CENTRO",
      cod_localidade_ibge = 1200013L,
      local_id = seq_len(3L),
      pred_long = -67.8,
      pred_lat = -9.97,
      pred_dist = 0.0,
      tse_lat = -9.97,
      tse_long = -67.8,
      final_long = -67.8,
      final_lat = -9.97
    )
  }))
  dt[, local_id := .I]
  dt[]
}

# Per year x state coverage in the shape compute_tse_coverage() returns, with the
# TSE-bearing vintages (2018+) landing within the Gate 7 slack of their raw
# availability and 2024 above the hard gate.
make_coverage_fixture <- function(cov_2024 = 95, cov_2018 = 90, cov_2020 = 90, cov_2022 = 92) {
  pct <- c("2018" = cov_2018, "2020" = cov_2020, "2022" = cov_2022, "2024" = cov_2024)
  data.table::rbindlist(lapply(names(pct), function(y) {
    data.table::data.table(
      ano = as.integer(y),
      sg_uf = "AC",
      n_total = 100L,
      n_covered = as.integer(round(pct[[y]])),
      coverage_pct = pct[[y]],
      suppressed = FALSE
    )
  }))
}

# Per-year raw TSE availability in the shape compute_tse_raw_availability()
# returns. Defaults sit just above the coverage fixture so the near-lossless
# join passes Gate 7 (shortfall within RELEASE_TSE_JOIN_SLACK).
make_raw_avail_fixture <- function(avail_2018 = 92, avail_2020 = 92, avail_2022 = 94, avail_2024 = 96) {
  pct <- c("2018" = avail_2018, "2020" = avail_2020, "2022" = avail_2022, "2024" = avail_2024)
  data.table::rbindlist(lapply(names(pct), function(y) {
    data.table::data.table(
      ano = as.integer(y),
      n_stations = 100L,
      n_with_coord = as.integer(round(pct[[y]])),
      raw_avail_pct = pct[[y]]
    )
  }))
}

# Existing files so gate 4 (output files exist) is satisfied.
make_export_paths <- function() {
  paths <- c(tempfile(fileext = ".csv.gz"), tempfile(fileext = ".csv.gz"))
  for (p in paths) {
    writeLines("x", p)
  }
  paths
}

test_that("all gates pass on a well-formed build (dev mode)", {
  res <- validate_release_gates(
    make_geocoded_fixture(),
    make_coverage_fixture(),
    make_raw_avail_fixture(),
    make_export_paths(),
    dev_mode = TRUE
  )
  expect_true(res$passed)
  expect_length(res$failures, 0)
})

test_that("Gate 1 fails when an election year is missing", {
  g <- make_geocoded_fixture()[ano != 2016L]
  expect_error(
    validate_release_gates(g, make_coverage_fixture(), make_raw_avail_fixture(), make_export_paths(), dev_mode = TRUE),
    "Gate 1.*2016"
  )
})

test_that("Gate 1 fails when the 2024 partition is empty", {
  g <- make_geocoded_fixture()[ano != 2024L]
  expect_error(
    validate_release_gates(g, make_coverage_fixture(), make_raw_avail_fixture(), make_export_paths(), dev_mode = TRUE),
    "2024 partition is empty|Gate 1"
  )
})

test_that("Gate 2 fails when a documented schema column is missing", {
  g <- make_geocoded_fixture()
  g[, pred_dist := NULL]
  expect_error(
    validate_release_gates(g, make_coverage_fixture(), make_raw_avail_fixture(), make_export_paths(), dev_mode = TRUE),
    "Gate 2.*pred_dist"
  )
})

test_that("Gate 2 fails when an unexpected extra column appears", {
  g <- make_geocoded_fixture()
  g[, surprise_col := 1L]
  expect_error(
    validate_release_gates(g, make_coverage_fixture(), make_raw_avail_fixture(), make_export_paths(), dev_mode = TRUE),
    "Gate 2.*surprise_col"
  )
})

test_that("Gate 3 fails when a year has zero non-NA coordinates", {
  g <- make_geocoded_fixture()
  g[ano == 2010L, c("final_lat", "final_long") := NA_real_]
  expect_error(
    validate_release_gates(g, make_coverage_fixture(), make_raw_avail_fixture(), make_export_paths(), dev_mode = TRUE),
    "Gate 3.*2010"
  )
})

test_that("Gate 4 fails when an output file is missing", {
  expect_error(
    validate_release_gates(
      make_geocoded_fixture(),
      make_coverage_fixture(),
      make_raw_avail_fixture(),
      c("/nonexistent/geocoded.csv.gz"),
      dev_mode = TRUE
    ),
    "Gate 4.*missing"
  )
})

test_that("Gate 6 fails when landed 2024 TSE coverage is below 92%", {
  expect_error(
    validate_release_gates(
      make_geocoded_fixture(),
      make_coverage_fixture(cov_2024 = 80),
      make_raw_avail_fixture(),
      make_export_paths(),
      dev_mode = TRUE
    ),
    "Gate 6.*coverage"
  )
})

test_that("Gate 7 fails when landed coverage falls far below raw availability", {
  # 2018 landed at 40% while the raw file offered 92% - a ~52 pt merge loss well
  # past the slack, the regression this tripwire exists to catch.
  expect_error(
    validate_release_gates(
      make_geocoded_fixture(),
      make_coverage_fixture(cov_2018 = 40),
      make_raw_avail_fixture(),
      make_export_paths(),
      dev_mode = TRUE
    ),
    "Gate 7.*2018"
  )
})

test_that("Gate 7 passes when a vintage's raw availability is genuinely low", {
  # 2018 landing at 49% is near-lossless when TSE only published 51% - not a
  # regression, so the gate must not fire on the sparse pre-2024 vintages.
  res <- validate_release_gates(
    make_geocoded_fixture(),
    make_coverage_fixture(cov_2018 = 49),
    make_raw_avail_fixture(avail_2018 = 51),
    make_export_paths(),
    dev_mode = TRUE
  )
  expect_true(res$passed)
})

test_that("Gate 5 (absolute counts) is enforced only in production mode", {
  g <- make_geocoded_fixture() # tiny per-year counts
  # dev mode: small counts are fine.
  expect_true(
    validate_release_gates(
      g,
      make_coverage_fixture(),
      make_raw_avail_fixture(),
      make_export_paths(),
      dev_mode = TRUE
    )$passed
  )
  # production mode: the tiny 2024 partition trips gate 5.
  expect_error(
    validate_release_gates(g, make_coverage_fixture(), make_raw_avail_fixture(), make_export_paths(), dev_mode = FALSE),
    "Gate 5"
  )
})

test_that("Gate 8 fails when a TSE-covered row has non-zero pred_dist", {
  # TSE-covered stations ship the ground-truth coordinate, so pred_dist must be
  # 0. A non-zero value is the finalize_coords regression this gate guards.
  g <- make_geocoded_fixture()
  g[ano == 2020L & tse_long == -67.8, pred_dist := 0.05]
  expect_error(
    validate_release_gates(g, make_coverage_fixture(), make_raw_avail_fixture(), make_export_paths(), dev_mode = TRUE),
    "Gate 8.*pred_dist"
  )
})

test_that("Gate 5 catches an exploded non-2024 year in production mode", {
  # Build a fixture at plausible national scale so only the exploded year trips.
  years <- seq(2006L, 2024L, by = 2L)
  g <- data.table::rbindlist(lapply(years, function(y) {
    n <- if (y == 2022L) 180000L else 90000L # 2022 doubled by a bad merge
    f <- make_geocoded_fixture()[ano == 2024L][1L]
    dt <- f[rep(1L, n)]
    dt[, ano := y]
    dt[, local_id := .I]
    dt[]
  }))
  g[, local_id := .I]
  expect_error(
    validate_release_gates(g, make_coverage_fixture(), make_raw_avail_fixture(), make_export_paths(), dev_mode = FALSE),
    "Gate 5.*2022"
  )
})
