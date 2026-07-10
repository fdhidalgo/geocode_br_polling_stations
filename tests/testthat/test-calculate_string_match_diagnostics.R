## Fail-loud spec tests for the string-match diagnostics helpers
## (R/string_match_diagnostics.R), cleanup phase 3, Medium. The former code
## guessed coordinate columns from fallback lists and returned an "error" string
## row when none matched, silently degrading the diagnostics. The fixed contract
## takes the coordinate columns explicitly (per match type, via
## string_match_coord_cols) and stops when a named column is absent.

test_that("string_match_coord_cols maps known targets and stops on unknown ones", {
  expect_equal(
    unname(string_match_coord_cols("inep_string_match")),
    c("match_long_inep_addr", "match_lat_inep_addr")
  )
  expect_equal(
    unname(string_match_coord_cols("schools_cnefe10_match")),
    c("match_long_schools_cnefe", "match_lat_schools_cnefe")
  )
  expect_error(
    string_match_coord_cols("not_a_real_match"),
    "No coordinate-column mapping"
  )
})

test_that("calculate_string_match_diagnostics computes NA rate on the named columns", {
  match_data <- data.table::data.table(
    match_long_inep_addr = c(-60, NA, -61, -62),
    match_lat_inep_addr = c(-9, NA, -8, -7)
  )
  out <- calculate_string_match_diagnostics(
    match_data, "inep_string_match", "match_long_inep_addr", "match_lat_inep_addr"
  )
  expect_equal(out$total_rows, 4L)
  expect_equal(out$na_coords, 1L)
  expect_equal(out$na_coords_pct, 25)
})

test_that("calculate_string_match_diagnostics stops when a coordinate column is absent", {
  match_data <- data.table::data.table(some_other_col = 1:3)
  expect_error(
    calculate_string_match_diagnostics(
      match_data, "inep_string_match", "match_long_inep_addr", "match_lat_inep_addr"
    ),
    "coordinate column\\(s\\) not found"
  )
})
