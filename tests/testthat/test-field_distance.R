## Spec tests for field_distance() (R/string_matching.R).
## Jaccard distance over character trigrams between paired strings, feeding the model's
## per-field sim_* features. Unlike match_strings(), the pairs are not gated on a shared
## word, so incomparable inputs reach it directly.

test_that("field_distance is 0 for identical strings and 1 for disjoint ones", {
  expect_equal(field_distance("rua das flores", "rua das flores"), 0)
  expect_equal(field_distance("abcdefg", "hijklmn"), 1)
})

test_that("field_distance is NA when either side is missing", {
  expect_true(is.na(field_distance(NA_character_, "rua das flores")))
  expect_true(is.na(field_distance("rua das flores", NA_character_)))
})

test_that("field_distance is NA when a string is too short to have a trigram", {
  # stringdist scores two sub-trigram strings as identical whatever they say, which would
  # hand the model a fabricated perfect match between two unrelated fields.
  expect_true(is.na(field_distance("sn", "ct")))
  expect_true(is.na(field_distance("sn", "sn")))
  expect_true(is.na(field_distance("sn", "rua das flores")))
})

test_that("field_distance is vectorized and preserves order", {
  out <- field_distance(c("rua das flores", "abcdefg"), c("rua das flores", "hijklmn"))
  expect_equal(out, c(0, 1))
})
