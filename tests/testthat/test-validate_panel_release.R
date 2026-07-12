## Spec tests for validate_panel_release() (R/validation.R), the fail-loud gate
## on the panel-id output (panel_ids.csv.gz). It exists to catch the regression
## where make_panel_ids() stopped consulting model coordinates and left ~13% of
## panels blank (and dropped pred_dist). Each gate must stop() loudly.

make_panel_fixture <- function(n = 1000L) {
  data.table::data.table(
    panel_id = seq_len(n),
    local_id = seq_len(n),
    long = -60,
    lat = -9,
    pred_dist = 0.1
  )
}

test_that("panel gates pass on a well-formed panel file", {
  res <- validate_panel_release(make_panel_fixture())
  expect_true(res$passed)
  expect_length(res$failures, 0)
  expect_equal(res$coord_na_pct, 0)
})

test_that("Gate P1 fails when pred_dist is dropped", {
  p <- make_panel_fixture()
  p[, pred_dist := NULL]
  expect_error(validate_panel_release(p), "Gate P1.*pred_dist")
})

test_that("Gate P2 fails when too many panels lack a coordinate", {
  # Blank 13% of coordinates - the exact regression this gate guards against.
  p <- make_panel_fixture(1000L)
  p[seq_len(130L), c("long", "lat") := NA_real_]
  expect_error(validate_panel_release(p), "Gate P2.*ignoring model coordinates")
})

test_that("Gate P2 tolerates a tiny rate of genuinely ungeocoded panels", {
  # A handful of panels whose every year failed to geocode is legitimate.
  p <- make_panel_fixture(1000L)
  p[1L, c("long", "lat") := NA_real_]
  res <- validate_panel_release(p)
  expect_true(res$passed)
})

test_that("Gate P2 fails when long/lat columns are missing entirely", {
  p <- make_panel_fixture()
  p[, c("long", "lat") := NULL]
  expect_error(validate_panel_release(p), "Gate P2.*missing long/lat")
})
