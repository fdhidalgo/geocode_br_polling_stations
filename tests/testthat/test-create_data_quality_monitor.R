## Fail-loud spec test for create_data_quality_monitor() (R/validation.R),
## cleanup phase 3, finding H4. The monitor accumulated a "CRITICAL" status
## string but never raised a condition, so a CRITICAL data-quality state finished
## green. The fixed contract stops on CRITICAL; a non-CRITICAL run still returns
## its results object.

make_geocoded <- function(n_munis) {
  data.table::data.table(
    cd_localidade_tse = seq_len(n_munis),
    local_id = seq_len(n_munis),
    final_long = rep(-60, n_munis),
    final_lat = rep(-9, n_munis)
  )
}

test_that("create_data_quality_monitor stops on CRITICAL status", {
  exports <- replicate(2, { f <- tempfile(); file.create(f); f })
  # One municipality vs an expected 5570 is a discrepancy far past the CRITICAL
  # alert threshold (100), so status becomes CRITICAL.
  geocoded <- make_geocoded(1)
  panel <- data.table::data.table(panel_id = 1L, local_id = 1L)
  capture.output(
    expect_error(
      create_data_quality_monitor(exports[1], exports[2], geocoded, panel),
      "CRITICAL"
    )
  )
})

test_that("create_data_quality_monitor returns results when quality is acceptable", {
  exports <- replicate(2, { f <- tempfile(); file.create(f); f })
  geocoded <- make_geocoded(5)
  panel <- data.table::data.table(panel_id = 1:5, local_id = 1:5)
  # expected_municipality_count = 5 matches the data exactly, so no CRITICAL.
  out <- NULL
  capture.output(
    out <- create_data_quality_monitor(
      exports[1], exports[2], geocoded, panel,
      expected_municipality_count = 5
    )
  )
  expect_equal(out$status, "OK")
})
