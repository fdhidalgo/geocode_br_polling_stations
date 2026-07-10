## Fail-loud spec tests for process_geocodebr_batch() (R/utilities.R), cleanup
## phase 3, finding C5. The batch driver applies the collect-and-stop
## convention: a municipality whose match_geocodebr_muni() errors is recorded and
## the batch continues, and at batch end any accumulated failures raise a single
## error naming every failing municipality. No errored municipality is ever
## filtered into the combined output as a silent gap.

test_that("process_geocodebr_batch collects per-municipality failures and stops", {
  assignments <- data.table::data.table(
    cod_localidade_ibge = c(1L, 2L),
    batch_id = c(99L, 99L)
  )
  # These rows carry cod_localidade_ibge but omit the address columns
  # match_geocodebr_muni() needs (sg_uf, nm_localidade, ds_endereco, ...), so
  # each municipality errors before ever reaching the external geocodebr DB.
  # Collect-and-stop must surface both failures rather than dropping them.
  locais <- data.table::data.table(
    cod_localidade_ibge = c(1L, 2L),
    local_id = c(10L, 20L)
  )
  muni_ids <- data.table::data.table(id_munic_7 = integer())

  err <- expect_error(
    process_geocodebr_batch(99L, assignments, locais, muni_ids),
    "geocodebr matching failed for"
  )
  # Both failing municipalities are named in the single collected error.
  expect_match(conditionMessage(err), "2 municipalities")
  expect_match(conditionMessage(err), "1:")
  expect_match(conditionMessage(err), "2:")
})

test_that("process_geocodebr_batch returns an empty table when a batch has no municipalities", {
  assignments <- data.table::data.table(
    cod_localidade_ibge = 1L,
    batch_id = 1L
  )
  # Batch 42 has no assigned municipalities, so there is nothing to geocode and
  # nothing to fail: an empty data.table, no error.
  out <- process_geocodebr_batch(
    42L, assignments,
    data.table::data.table(cod_localidade_ibge = integer(), local_id = integer()),
    data.table::data.table(id_munic_7 = integer())
  )
  expect_true(data.table::is.data.table(out))
  expect_equal(nrow(out), 0L)
})
