## Spec tests for convert_coord() (R/data_cleaning.R).
## convert_coord parses a "degrees minutes seconds direction" DMS string into
## decimal degrees, negating for S/W/O (Sul/West/Oeste), and returns NA_real_
## for any malformed input (fewer than 4 parts, or non-numeric D/M/S).

test_that("convert_coord parses valid DMS strings to decimal degrees", {
  expect_equal(convert_coord("23 30 0 S"), -23.5)   # southern hemisphere is negative
  expect_equal(convert_coord("10 0 0 N"), 10)       # northern hemisphere stays positive
  expect_equal(convert_coord("0 0 36 N"), 0.01)     # seconds contribute 36/3600
})

test_that("convert_coord negates for all western/southern direction codes", {
  expect_equal(convert_coord("10 0 0 O"), -10)   # Oeste
  expect_equal(convert_coord("10 0 0 W"), -10)   # West
  expect_equal(convert_coord("10 0 0 S"), -10)   # Sul
})

test_that("convert_coord returns NA_real_ on malformed input", {
  expect_identical(convert_coord("abc"), NA_real_)          # single token
  expect_identical(convert_coord("10 20"), NA_real_)        # fewer than 4 parts
  expect_identical(convert_coord("xx 20 30 N"), NA_real_)   # non-numeric degrees
  expect_identical(convert_coord("10 yy 30 N"), NA_real_)   # non-numeric minutes
})
