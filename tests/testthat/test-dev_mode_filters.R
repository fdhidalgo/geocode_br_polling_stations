## Fail-loud spec tests for the dev-mode filter helpers (R/utilities.R), cleanup
## phase 3, finding H2. Each helper used to return the FULL, unfiltered dataset
## with only a warning() when its expected column was absent — in dev mode that
## silently runs the multi-hour full pipeline. The fixed contract stops when the
## named column is missing, and filter_data_by_municipalities no longer probes
## alternative ID columns (which could filter on the wrong ID system).

test_that("filter_by_dev_mode filters on the named column and stops when it is absent", {
  data <- data.table::data.table(estado_abrev = c("AC", "RR", "SP"), x = 1:3)
  expect_equal(filter_by_dev_mode(data, c("AC", "RR"))$estado_abrev, c("AC", "RR"))
  # NULL dev_states is production: return everything, no error.
  expect_equal(nrow(filter_by_dev_mode(data, NULL)), 3L)
  # Missing column must stop, not silently return unfiltered data.
  expect_error(
    filter_by_dev_mode(data.table::data.table(sg_uf = "AC"), c("AC", "RR")),
    "'estado_abrev' not found"
  )
})

test_that("filter_data_by_state filters on the named column and stops when it is absent", {
  data <- data.table::data.table(sg_uf = c("AC", "RR", "SP"), x = 1:3)
  expect_equal(filter_data_by_state(data, c("AC", "RR"), "sg_uf")$sg_uf, c("AC", "RR"))
  expect_equal(nrow(filter_data_by_state(data, NULL, "sg_uf")), 3L)
  expect_error(
    filter_data_by_state(data, c("AC"), "estado_abrev"),
    "'estado_abrev' not found"
  )
})

test_that("filter_data_by_municipalities stops instead of probing alternative ID columns", {
  data <- data.table::data.table(id_munic_7 = c(1L, 2L, 3L), x = 1:3)
  expect_equal(filter_data_by_municipalities(data, c(1L, 2L))$id_munic_7, c(1L, 2L))
  expect_equal(nrow(filter_data_by_municipalities(data, NULL)), 3L)
  # A table with only an alternative ID column must now fail loud, rather than
  # silently filtering on cod_localidade_ibge (possibly the wrong ID system).
  alt <- data.table::data.table(cod_localidade_ibge = c(1L, 2L, 3L))
  expect_error(
    filter_data_by_municipalities(alt, c(1L, 2L)),
    "'id_munic_7' not found"
  )
})

test_that("apply_brasilia_filters drops DF on the named column and stops when it is absent", {
  data <- data.table::data.table(sg_uf = c("AC", "DF", "SP"), x = 1:3)
  out <- apply_brasilia_filters(data)
  expect_false("DF" %in% out$sg_uf)
  expect_equal(nrow(out), 2L)
  # remove_brasilia = FALSE is a pass-through, no column needed.
  expect_equal(nrow(apply_brasilia_filters(data, remove_brasilia = FALSE)), 3L)
  # Missing state column must stop rather than falling back to a municipality
  # code prefix or passing the data through unfiltered.
  expect_error(
    apply_brasilia_filters(data.table::data.table(id_munic_7 = 5300108L)),
    "'sg_uf' not found"
  )
})
