## Fail-loud spec tests for convert_coords_checked() (R/data_cleaning.R), cleanup
## phase 3, Medium. convert_coord() silently returns NA on a malformed value;
## this wrapper accounts for parse failures across a vector: it stops when every
## value fails (a systematic parse failure), reports the NA rate otherwise, and
## returns the converted values unchanged on the happy path.

test_that("convert_coords_checked converts a clean vector without error", {
  out <- suppressMessages(convert_coords_checked(c("23 30 0 S", "10 0 0 N")))
  expect_equal(out, c(-23.5, 10))
})

test_that("convert_coords_checked stops when every value fails to parse", {
  expect_error(
    convert_coords_checked(c("bad", "also bad"), "CNEFE longitude"),
    "All 2 CNEFE longitude values failed"
  )
})

test_that("convert_coords_checked reports the NA rate but returns partial results", {
  expect_message(
    out <- convert_coords_checked(c("23 30 0 S", "bad")),
    "1/2 \\(50.0%\\) values failed"
  )
  expect_equal(out, c(-23.5, NA_real_))
})
