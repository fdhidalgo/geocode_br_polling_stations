## Spec tests for match_strings() (R/string_matching.R).
## For each query string it returns the nearest target (Jaro-Winkler), but only among
## targets that share at least one whitespace-delimited word with the query. A query
## sharing no word with any target gets no candidate:
## min_dist stays Inf and best_match/best_index are NA. Returns a list of three
## parallel vectors: min_dist, best_match, best_index.

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

test_that("match_strings returns NA/Inf when no target shares a word", {
  res <- match_strings(
    query_strings = c("escola central", "zzz qqq"),
    target_strings = c("escola municipal", "hospital norte")
  )
  # query 1 shares "escola"; query 2 shares nothing -> no candidate
  expect_equal(res$best_index[1], 1L)
  expect_true(is.na(res$best_match[2]))
  expect_true(is.na(res$best_index[2]))
  expect_true(is.infinite(res$min_dist[2]))
})

test_that("match_strings ranks by similarity alone, not string length", {
  # Jaro-Winkler is already normalized to 0-1, so candidate length must not enter the
  # ranking. Here the sprawling name is the worse match (JW 0.43 vs 0.30), but dividing
  # by max(nchar) made its extra 64 characters win the comparison.
  res <- match_strings(
    query_strings = "jose joaquim",
    target_strings = c(
      "escola estadual indigena jose joaquim de sousa filho da comunidade raposa serra do sol",
      "indigena jose joaquim"
    )
  )
  expect_equal(res$best_index, 2L)
  expect_equal(res$min_dist, stringdist::stringdist("jose joaquim", "indigena jose joaquim", method = "jw"))
})

test_that("match_strings is case-insensitive when gating candidates", {
  res <- match_strings("ESCOLA Central", "escola norte")
  expect_equal(res$best_index, 1L)
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
