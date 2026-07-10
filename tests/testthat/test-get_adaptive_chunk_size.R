## Spec tests for get_adaptive_chunk_size() (R/string_matching.R).
## Chooses a chunk size = floor(sqrt(comparisons affordable in half the memory
## budget)), clamped to [100, 10000]. Larger memory budgets yield larger chunks.
##
## NOTE: the `n_items` argument is currently ignored by the implementation (the
## result depends only on available_memory_gb). That is a known smell tracked
## separately; these tests pin only the documented memory-scaling contract and
## the clamp bounds, not the n_items behavior.

test_that("get_adaptive_chunk_size clamps to the [100, 10000] range", {
  expect_equal(get_adaptive_chunk_size(1000, available_memory_gb = 4), 10000)     # hits upper clamp
  expect_equal(get_adaptive_chunk_size(1000, available_memory_gb = 1e-5), 100)    # hits lower clamp
})

test_that("get_adaptive_chunk_size grows with the memory budget", {
  small <- get_adaptive_chunk_size(1000, available_memory_gb = 0.01)
  big <- get_adaptive_chunk_size(1000, available_memory_gb = 0.1)
  expect_lt(small, big)
  expect_true(small >= 100 && big <= 10000)
})
