## Spec tests for make_panel_ids() (R/panel_creation.R).
## Joins the final coordinate (TSE-when-available, otherwise the model's
## selection) from geocoded_locais onto the combined panel-id table,
## and assigns every station in a panel the single best coordinate the panel
## offers: lowest expected error (final_logmean), ties broken toward the most
## recent year. Assertions are keyed on local_id, not row order.

test_that("make_panel_ids picks each panel's lowest-expected-error coordinate", {
  panel_ids_combined <- data.table::data.table(
    local_id = c("l1", "l2", "l3"),
    panel_id = c("p1", "p1", "p2")
  )
  geocoded_locais <- data.table::data.table(
    local_id = c("l1", "l2", "l3"),
    ano = c(2018L, 2022L, 2020L),
    final_long = c(-60, -61, -62),
    final_lat = c(-9, -8, -7),
    conf_dist_km = c(0.2, 0.5, 3.0),
    # l1 is expected to be more accurate than the more recent l2.
    final_logmean = c(-2.0, -1.0, 0.5)
  )

  out <- make_panel_ids(copy(panel_ids_combined), copy(geocoded_locais))

  expect_equal(nrow(out), 3L)
  expect_true(all(c("local_id", "panel_id", "long", "lat", "conf_dist_km") %in% names(out)))

  # Panel p1 spans 2018 (l1) and 2022 (l2). The coordinate is chosen by expected
  # accuracy, not recency, so both stations take l1's 2018 coordinate, and the
  # published bound that ships is l1's.
  expect_equal(out[local_id == "l1", long], -60)
  expect_equal(out[local_id == "l1", lat], -9)
  expect_equal(out[local_id == "l2", long], -60)
  expect_equal(out[local_id == "l2", lat], -9)
  expect_equal(out[local_id == "l2", conf_dist_km], 0.2)
  # Panel p2 has a single station and keeps its own coordinates and conf_dist_km.
  expect_equal(out[local_id == "l3", long], -62)
  expect_equal(out[local_id == "l3", lat], -7)
  expect_equal(out[local_id == "l3", conf_dist_km], 3.0)
})

test_that("make_panel_ids ranks on expected error, not the published bound", {
  # Issue #142's inversion: the 2018 coordinate is expected to be far more accurate
  # but carries the wider calibrated bound (its features sit in a sparse region).
  # Ranking on the bound would ship the 2022 coordinate to the whole panel.
  panel_ids_combined <- data.table::data.table(
    local_id = c("w1", "w2"),
    panel_id = c("pw", "pw")
  )
  geocoded_locais <- data.table::data.table(
    local_id = c("w1", "w2"),
    ano = c(2018L, 2022L),
    final_long = c(-40, -41),
    final_lat = c(-20, -21),
    conf_dist_km = c(4.0, 0.9),
    final_logmean = c(log(0.08), log(0.9))
  )

  out <- make_panel_ids(copy(panel_ids_combined), copy(geocoded_locais))

  expect_equal(out[local_id == "w2", long], -40)
  expect_equal(out[local_id == "w2", lat], -20)
  # The winner's own bound ships with it.
  expect_equal(out[local_id == "w2", conf_dist_km], 4.0)
})

test_that("make_panel_ids keeps TSE ground truth ahead of any model coordinate", {
  # finalize_coords() gives a TSE-covered station-year final_logmean = -Inf. The model
  # prediction here is absurdly optimistic (log-km far below anything LightGBM produces)
  # precisely to show the precedence does not rest on the model's range.
  panel_ids_combined <- data.table::data.table(
    local_id = c("t1", "t2"),
    panel_id = c("pt", "pt")
  )
  geocoded_locais <- data.table::data.table(
    local_id = c("t1", "t2"),
    ano = c(2018L, 2022L),
    final_long = c(-30, -31),
    final_lat = c(-5, -6),
    conf_dist_km = c(0, 0.3),
    final_logmean = c(-Inf, -50)
  )

  out <- make_panel_ids(copy(panel_ids_combined), copy(geocoded_locais))

  expect_equal(out[local_id == "t2", long], -30)
  expect_equal(out[local_id == "t2", conf_dist_km], 0)
})

test_that("make_panel_ids uses model coordinates when a panel has no TSE year", {
  # Regression guard: a panel whose only coordinates come from the model (all
  # conf_dist_km > 0, i.e. no TSE ground truth) must still receive a coordinate.
  # The bug this replaced read tse_long/tse_lat only, blanking such panels.
  panel_ids_combined <- data.table::data.table(
    local_id = c("a1", "a2"),
    panel_id = c("pm", "pm")
  )
  geocoded_locais <- data.table::data.table(
    local_id = c("a1", "a2"),
    ano = c(2006L, 2008L),
    final_long = c(-50, -51),
    final_lat = c(-10, -11),
    conf_dist_km = c(1.5, 0.8),
    final_logmean = c(0.4, -0.3)
  )

  out <- make_panel_ids(copy(panel_ids_combined), copy(geocoded_locais))

  # Best expected error is a2; both stations take it. No NA coordinates.
  expect_equal(sum(is.na(out$long)), 0L)
  expect_equal(out[local_id == "a1", long], -51)
  expect_equal(out[local_id == "a2", lat], -11)
})

test_that("make_panel_ids leaves a panel uncoordinated only when every year is ungeocoded", {
  # The one legitimate NA case: a panel whose every station failed to geocode.
  panel_ids_combined <- data.table::data.table(
    local_id = c("n1", "n2"),
    panel_id = c("pn", "pn")
  )
  geocoded_locais <- data.table::data.table(
    local_id = c("n1", "n2"),
    ano = c(2010L, 2012L),
    final_long = c(NA_real_, NA_real_),
    final_lat = c(NA_real_, NA_real_),
    conf_dist_km = c(NA_real_, NA_real_),
    final_logmean = c(NA_real_, NA_real_)
  )

  out <- make_panel_ids(copy(panel_ids_combined), copy(geocoded_locais))

  expect_true(all(is.na(out$long)))
  expect_true(all(is.na(out$lat)))
})

test_that("make_panel_ids stops when a geocoded station-year carries no expected error", {
  panel_ids_combined <- data.table::data.table(
    local_id = "s1",
    panel_id = "ps"
  )
  geocoded_locais <- data.table::data.table(
    local_id = "s1",
    ano = 2018L,
    final_long = -45,
    final_lat = -12,
    conf_dist_km = 0.4,
    final_logmean = NA_real_
  )

  expect_error(
    make_panel_ids(copy(panel_ids_combined), copy(geocoded_locais)),
    "expected error"
  )
})
