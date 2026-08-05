## Spec tests for make_panel_ids() (R/panel_creation.R).
## Joins the final coordinate (TSE-when-available, otherwise the model's
## selection) from geocoded_locais onto the combined panel-id table,
## and assigns every station in a panel the single most accurate coordinate the
## panel offers: smallest pred_dist, ties broken toward the most recent year.
## Assertions are keyed on local_id, not row order.

test_that("make_panel_ids picks each panel's smallest-pred_dist coordinate", {
  panel_ids_combined <- data.table::data.table(
    local_id = c("l1", "l2", "l3"),
    panel_id = c("p1", "p1", "p2")
  )
  geocoded_locais <- data.table::data.table(
    local_id = c("l1", "l2", "l3"),
    ano = c(2018L, 2022L, 2020L),
    final_long = c(-60, -61, -62),
    final_lat = c(-9, -8, -7),
    # l1 is more accurate (smaller pred_dist) than the more recent l2.
    pred_dist = c(0.2, 0.5, 3.0)
  )

  out <- make_panel_ids(copy(panel_ids_combined), copy(geocoded_locais))

  expect_equal(nrow(out), 3L)
  expect_true(all(c("local_id", "panel_id", "long", "lat", "pred_dist") %in% names(out)))

  # Panel p1 spans 2018 (l1, pred_dist 0.2) and 2022 (l2, pred_dist 0.5). The
  # coordinate is chosen by accuracy, not recency, so both stations take l1's
  # 2018 coordinate - NOT l2's more recent one.
  expect_equal(out[local_id == "l1", long], -60)
  expect_equal(out[local_id == "l1", lat], -9)
  expect_equal(out[local_id == "l2", long], -60)
  expect_equal(out[local_id == "l2", lat], -9)
  expect_equal(out[local_id == "l2", pred_dist], 0.2)
  # Panel p2 has a single station and keeps its own coordinates and pred_dist.
  expect_equal(out[local_id == "l3", long], -62)
  expect_equal(out[local_id == "l3", lat], -7)
  expect_equal(out[local_id == "l3", pred_dist], 3.0)
})

test_that("make_panel_ids uses model coordinates when a panel has no TSE year", {
  # Regression guard: a panel whose only coordinates come from the model (all
  # pred_dist > 0, i.e. no TSE ground truth) must still receive a coordinate.
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
    pred_dist = c(1.5, 0.8)
  )

  out <- make_panel_ids(copy(panel_ids_combined), copy(geocoded_locais))

  # Best (smallest pred_dist) is a2; both stations take it. No NA coordinates.
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
    pred_dist = c(NA_real_, NA_real_)
  )

  out <- make_panel_ids(copy(panel_ids_combined), copy(geocoded_locais))

  expect_true(all(is.na(out$long)))
  expect_true(all(is.na(out$lat)))
})
