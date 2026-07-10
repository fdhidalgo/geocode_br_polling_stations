## Fail-loud spec tests for create_section_panel_mapping() (R/panel_creation.R),
## cleanup phase 3, Medium. Empty inputs previously returned an empty data.table
## with a cat() "Warning:" (not even an R condition), hiding an upstream failure
## to produce sections, geocoded locations, or panel IDs. The fixed contract
## stops on any empty input.

test_that("create_section_panel_mapping stops on an empty section mapping", {
  empty <- data.table::data.table()
  nonempty <- data.table::data.table(x = 1L)
  expect_error(
    suppressWarnings(create_section_panel_mapping(empty, nonempty, nonempty)),
    "empty section-location mapping"
  )
})

test_that("create_section_panel_mapping stops on empty geocoded locations", {
  nonempty <- data.table::data.table(x = 1L)
  empty <- data.table::data.table()
  expect_error(
    suppressWarnings(create_section_panel_mapping(nonempty, empty, nonempty)),
    "empty geocoded locations"
  )
})

test_that("create_section_panel_mapping stops on empty panel IDs", {
  nonempty <- data.table::data.table(x = 1L)
  empty <- data.table::data.table()
  expect_error(
    suppressWarnings(create_section_panel_mapping(nonempty, nonempty, empty)),
    "empty panel IDs"
  )
})
