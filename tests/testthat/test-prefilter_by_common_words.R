## Spec tests for prefilter_by_common_words() (R/string_matching.R).
## Returns a length(query) x length(target) logical matrix, TRUE where the query
## and target share at least `min_common_words` whitespace-delimited words
## (case-insensitive). This is the candidate gate that keeps string matching from
## comparing every query against every target.

test_that("prefilter flags targets sharing at least one word", {
  m <- prefilter_by_common_words(
    query_strings  = c("escola joao", "hospital central"),
    target_strings = c("escola maria", "banco central", "nada aqui")
  )
  expect_equal(dim(m), c(2L, 3L))
  expect_equal(m[1, ], c(TRUE, FALSE, FALSE))   # "escola" shared with target 1 only
  expect_equal(m[2, ], c(FALSE, TRUE, FALSE))   # "central" shared with target 2 only
})

test_that("prefilter is case-insensitive", {
  m <- prefilter_by_common_words("ESCOLA Central", "escola norte")
  expect_true(m[1, 1])
})

test_that("prefilter respects a higher min_common_words threshold", {
  q <- "escola joao maria"
  tg <- c("escola maria", "escola norte")
  # target 1 shares {escola, maria} = 2; target 2 shares {escola} = 1
  expect_equal(prefilter_by_common_words(q, tg, min_common_words = 2)[1, ], c(TRUE, FALSE))
  expect_equal(prefilter_by_common_words(q, tg, min_common_words = 1)[1, ], c(TRUE, TRUE))
})
