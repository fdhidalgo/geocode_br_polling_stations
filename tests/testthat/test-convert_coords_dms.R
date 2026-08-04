## Spec tests for convert_coords_dms() (R/data_cleaning.R).
## convert_coords_dms parses a vector of "degrees minutes seconds direction" DMS
## strings into decimal degrees, negating for S/W/O (Sul/West/Oeste), and returns
## NA_real_ for any malformed element (fewer than 4 tokens, or non-numeric D/M/S).

test_that("convert_coords_dms parses valid DMS strings to decimal degrees", {
  expect_equal(convert_coords_dms("23 30 0 S"), -23.5) # southern hemisphere is negative
  expect_equal(convert_coords_dms("10 0 0 N"), 10) # northern hemisphere stays positive
  expect_equal(convert_coords_dms("0 0 36 N"), 0.01) # seconds contribute 36/3600
})

test_that("convert_coords_dms negates for all western/southern direction codes", {
  expect_equal(convert_coords_dms("10 0 0 O"), -10) # Oeste
  expect_equal(convert_coords_dms("10 0 0 W"), -10) # West
  expect_equal(convert_coords_dms("10 0 0 S"), -10) # Sul
})

test_that("convert_coords_dms returns NA_real_ on malformed input", {
  expect_identical(convert_coords_dms("abc"), NA_real_) # single token
  expect_identical(convert_coords_dms("10 20"), NA_real_) # fewer than 4 parts
  expect_identical(convert_coords_dms("xx 20 30 N"), NA_real_) # non-numeric degrees
  expect_identical(convert_coords_dms("10 yy 30 N"), NA_real_) # non-numeric minutes
})

test_that("convert_coords_dms handles a mixed vector element-wise", {
  out <- convert_coords_dms(c("23 30 0 S", "bad", "10 0 0 N", "10 20"))
  expect_identical(out, c(-23.5, NA_real_, 10, NA_real_))
})

test_that("convert_coords_dms returns an empty numeric vector for empty input", {
  expect_identical(convert_coords_dms(character(0)), numeric(0))
})

test_that("convert_coords_dms maps every short row to NA when no row reaches 4 tokens", {
  # With no row reaching 4 tokens, tstrsplit yields < 4 columns; every element
  # must still map to NA (one per input), not collapse.
  expect_identical(convert_coords_dms(c("10 20", "30 40")), c(NA_real_, NA_real_))
})
