## Spec test for to_geocoded_export_schema() (R/utilities.R). The published
## geocoded file must keep the 0.141 column names and order - the recommended
## coordinate ships as long/lat, NOT the internal final_long/final_lat - so
## downstream code that read the 0.141 release keeps working.

test_that("to_geocoded_export_schema renames final_* to long/lat in 0.141 order", {
  internal <- data.table::data.table(
    # Deliberately in a different order than the export schema, with the internal
    # final_long/final_lat names, to prove the function renames AND reorders.
    cd_localidade_tse = 1L,
    ano = 2024L,
    local_id = 1L,
    nr_zona = 1L,
    nr_locvot = 1L,
    nr_cep = 1L,
    sg_uf = "AC",
    nm_localidade = "X",
    nm_locvot = "Y",
    ds_endereco = "Z",
    ds_bairro = "W",
    cod_localidade_ibge = 1L,
    pred_long = -67,
    pred_lat = -9,
    pred_dist = 0,
    tse_lat = -9,
    tse_long = -67,
    final_long = -67,
    final_lat = -9
  )

  out <- to_geocoded_export_schema(internal)

  # Exact published schema: 0.141 names (long/lat) in 0.141 order.
  expect_identical(names(out), GEOCODED_EXPORT_SCHEMA)
  expect_false(any(c("final_long", "final_lat") %in% names(out)))
  expect_equal(out$long, -67)
  expect_equal(out$lat, -9)

  # Pure: the input table is not mutated.
  expect_true("final_long" %in% names(internal))
})
