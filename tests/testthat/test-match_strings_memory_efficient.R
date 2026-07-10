## Spec tests for match_strings_memory_efficient() (R/string_matching.R).
## For each query string it returns the nearest target (Jaro-Winkler by default,
## length-normalized), but only among targets that survive the common-word
## prefilter. A query sharing no word with any target gets no candidate: min_dist
## stays Inf and best_match/best_index are NA. Returns a list of three parallel
## vectors: min_dist, best_match, best_index.

test_that("match_strings_memory_efficient finds exact matches at distance 0", {
  res <- match_strings_memory_efficient(
    query_strings  = c("escola central", "hospital norte"),
    target_strings = c("hospital norte", "escola central")
  )
  expect_equal(res$best_match, c("escola central", "hospital norte"))
  expect_equal(res$best_index, c(2L, 1L))
  expect_equal(res$min_dist, c(0, 0))
})

test_that("match_strings_memory_efficient picks the closest of several candidates", {
  res <- match_strings_memory_efficient(
    query_strings  = "escola sao joao",
    target_strings = c("escola sao joao", "escola sao pedro")
  )
  expect_equal(res$best_index, 1L)         # exact match beats the near one
  expect_equal(res$min_dist, 0)
})

test_that("match_strings_memory_efficient returns NA/Inf when the prefilter finds no candidate", {
  res <- match_strings_memory_efficient(
    query_strings  = c("escola central", "zzz qqq"),
    target_strings = c("escola municipal", "hospital norte")
  )
  # query 1 shares "escola"; query 2 shares nothing -> no candidate
  expect_equal(res$best_index[1], 1L)
  expect_true(is.na(res$best_match[2]))
  expect_true(is.na(res$best_index[2]))
  expect_true(is.infinite(res$min_dist[2]))
})

test_that("match_strings_memory_efficient returns length-aligned vectors", {
  res <- match_strings_memory_efficient(rep("escola norte", 5), "escola norte")
  expect_length(res$min_dist, 5L)
  expect_length(res$best_match, 5L)
  expect_length(res$best_index, 5L)
})
