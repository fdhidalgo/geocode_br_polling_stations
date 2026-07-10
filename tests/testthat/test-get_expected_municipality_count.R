## Fail-loud spec test for get_expected_municipality_count() (R/config.R),
## cleanup phase 3, Medium. An unknown state code previously returned NA with a
## warning, letting a typo flow into validation as NA; the fixed contract stops.

test_that("get_expected_municipality_count returns the known count for a valid state", {
  expect_equal(get_expected_municipality_count("AC"), 22)
  expect_equal(get_expected_municipality_count("SP"), 645)
})

test_that("get_expected_municipality_count stops on an unknown state", {
  expect_error(
    get_expected_municipality_count("ZZ"),
    "Unknown state abbreviation: ZZ"
  )
})
