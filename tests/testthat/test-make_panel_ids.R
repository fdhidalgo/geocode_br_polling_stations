## Spec tests for make_panel_ids() (R/panel_creation.R).
## Stacks the main and state panel-id tables, joins TSE coordinates from
## geocoded_locais, and assigns every station in a panel the coordinates of the
## panel's most recent year. Assertions are keyed on local_id, not row order.

test_that("make_panel_ids gives every station in a panel its most-recent-year coordinates", {
  panel_ids_df <- data.table::data.table(
    local_id = c("l1", "l2"),
    panel_id = c("p1", "p1")
  )
  panel_ids_states <- data.table::data.table(local_id = "l3", panel_id = "p2")
  geocoded_locais <- data.table::data.table(
    local_id = c("l1", "l2", "l3"),
    ano = c(2018L, 2022L, 2020L),
    tse_long = c(-60, -61, -62),
    tse_lat = c(-9, -8, -7)
  )

  out <- make_panel_ids(copy(panel_ids_df), copy(panel_ids_states), copy(geocoded_locais))

  expect_equal(nrow(out), 3L)
  expect_true(all(c("local_id", "panel_id", "long", "lat") %in% names(out)))

  # Panel p1 spans 2018 (l1) and 2022 (l2); both stations take the 2022 coords.
  expect_equal(out[local_id == "l1", long], -61)
  expect_equal(out[local_id == "l1", lat], -8)
  expect_equal(out[local_id == "l2", long], -61)
  # Panel p2 has a single station and keeps its own coordinates.
  expect_equal(out[local_id == "l3", long], -62)
  expect_equal(out[local_id == "l3", lat], -7)
})
