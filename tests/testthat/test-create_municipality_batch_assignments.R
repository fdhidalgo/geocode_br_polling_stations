## Fail-loud spec test for create_municipality_batch_assignments() (R/utilities.R),
## cleanup phase 3, Medium. When load-balancing by municipality size, a
## municipality with no size entry (a key mismatch between muni_codes and
## muni_sizes) was median-imputed, masking the mismatch. The fixed contract stops
## and names the municipalities with no size.

test_that("create_municipality_batch_assignments load-balances when all sizes are present", {
  sizes <- data.table::data.table(muni_code = c(1L, 2L, 3L), size = c(100L, 50L, 10L))
  out <- suppressMessages(
    create_municipality_batch_assignments(c(1L, 2L, 3L), batch_size = 2, muni_sizes = sizes)
  )
  expect_equal(sort(out$cod_localidade_ibge), c(1L, 2L, 3L))
  expect_true(all(c("cod_localidade_ibge", "batch_id") %in% names(out)))
})

test_that("create_municipality_batch_assignments stops on a municipality-size key mismatch", {
  # Municipality 3 has no size entry, so its size is NA after the join.
  sizes <- data.table::data.table(muni_code = c(1L, 2L), size = c(100L, 50L))
  expect_error(
    create_municipality_batch_assignments(c(1L, 2L, 3L), batch_size = 2, muni_sizes = sizes),
    "Municipality sizes missing for 1 municipality"
  )
})

test_that("create_municipality_batch_assignments still supports simple sequential batching", {
  out <- suppressMessages(create_municipality_batch_assignments(c(1L, 2L, 3L, 4L), batch_size = 2))
  expect_equal(nrow(out), 4L)
  expect_equal(length(unique(out$batch_id)), 2L)
})
