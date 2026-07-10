## Spec tests for get_adaptive_chunk_size() (R/string_matching.R).
## Chooses a chunk size = floor(sqrt(comparisons affordable in half the memory
## budget)), clamped to [100, 10000], then capped at n_items (chunking larger
## than the query set yields a single chunk of n_items anyway). Larger memory
## budgets yield larger chunks, up to the n_items cap.

test_that("get_adaptive_chunk_size clamps the memory-based size to [100, 10000]", {
  # n_items large enough not to bind, so the memory clamp is what shows through.
  expect_equal(get_adaptive_chunk_size(1e6, available_memory_gb = 4), 10000)     # hits upper clamp
  expect_equal(get_adaptive_chunk_size(1e6, available_memory_gb = 1e-5), 100)    # hits lower clamp
})

test_that("get_adaptive_chunk_size grows with the memory budget", {
  small <- get_adaptive_chunk_size(1e6, available_memory_gb = 0.01)
  big <- get_adaptive_chunk_size(1e6, available_memory_gb = 0.1)
  expect_lt(small, big)
  expect_true(small >= 100 && big <= 10000)
})

test_that("get_adaptive_chunk_size never exceeds n_items", {
  # A generous memory budget would give 10000, but there is no point chunking
  # more query rows than exist, so the result is capped at n_items.
  expect_equal(get_adaptive_chunk_size(500, available_memory_gb = 4), 500)
  # The cap wins even below the [100, ...] lower bound: 50 items -> one chunk of 50.
  expect_equal(get_adaptive_chunk_size(50, available_memory_gb = 4), 50)
})
