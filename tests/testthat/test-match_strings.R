## Spec tests for match_strings() (R/string_matching.R).
## For each query string it returns the nearest target by Jaro-Winkler distance, searching
## every target with no pre-filter. A query with nothing to compare against -- an empty
## target set, or an NA query -- gets min_dist = Inf and NA for best_match/best_index.
## Returns a list of three parallel vectors: min_dist, best_match, best_index.

test_that("match_strings finds exact matches at distance 0", {
  res <- match_strings(
    query_strings = c("escola central", "hospital norte"),
    target_strings = c("hospital norte", "escola central")
  )
  expect_equal(res$best_match, c("escola central", "hospital norte"))
  expect_equal(res$best_index, c(2L, 1L))
  expect_equal(res$min_dist, c(0, 0))
})

test_that("match_strings picks the closest of several candidates", {
  res <- match_strings(
    query_strings = "escola sao joao",
    target_strings = c("escola sao joao", "escola sao pedro")
  )
  expect_equal(res$best_index, 1L) # exact match beats the near one
  expect_equal(res$min_dist, 0)
})

test_that("match_strings matches a variant sharing no whole word with its target", {
  # Plural and singular forms share no whitespace-delimited word, so requiring one exact
  # shared word would drop this target before measuring anything -- yet the two strings
  # are nearly identical (Jaro-Winkler 0.08).
  res <- match_strings(
    query_strings = "escolas municipais",
    target_strings = c("hospital norte", "escola municipal")
  )
  expect_equal(res$best_index, 2L)
  expect_lt(res$min_dist, 0.1)
})

test_that("match_strings prefers the closer target over the merely longer one", {
  # Jaro-Winkler is not rescaled by string length, so a long unrelated target cannot
  # outrank a short accurate one.
  res <- match_strings(
    query_strings = "jose joaquim",
    target_strings = c(
      "centro indigena de formacao e cultura raposa serra do sol",
      "indigena jose joaquim"
    )
  )
  expect_equal(res$best_index, 2L)
})

test_that("match_strings breaks ties toward the lowest target index", {
  res <- match_strings("escola norte", c("escola norte", "escola norte"))
  expect_equal(res$best_index, 1L)
})

test_that("match_strings returns length-aligned vectors", {
  res <- match_strings(rep("escola norte", 5), "escola norte")
  expect_length(res$min_dist, 5L)
  expect_length(res$best_match, 5L)
  expect_length(res$best_index, 5L)
})

test_that("match_strings handles an empty target set and NA queries", {
  res <- match_strings(c("escola norte", NA_character_), character(0))
  expect_true(all(is.infinite(res$min_dist)))
  expect_true(all(is.na(res$best_index)))

  res_na <- match_strings(NA_character_, "escola norte")
  expect_true(is.na(res_na$best_index))
})
