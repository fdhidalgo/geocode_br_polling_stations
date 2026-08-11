## Spec test for finalize_coords() (R/data_cleaning.R), covering the two columns that
## describe the shipped coordinate's accuracy: conf_dist_km (published upper bound) and
## final_logmean (expected error on the model's log scale, internal, and what
## make_panel_ids() ranks a panel's station-years on).

test_that("finalize_coords carries the winner's expected error and zeroes it for TSE rows", {
  locais <- data.table::data.table(
    local_id = c(1L, 2L),
    ano = c(2018L, 2022L),
    normalized_name = c("a", "b"),
    normalized_addr = c("a", "b"),
    normalized_st = c("a", "b"),
    normalized_bairro = c("a", "b")
  )
  model_predictions <- data.table::data.table(
    local_id = c(1L, 1L, 2L),
    long = c(-60, -60.5, -61),
    lat = c(-9, -9.5, -8),
    conf_dist_km = c(1.2, 0.3, 2.0),
    # Station 1's first candidate wins on expected error despite the wider bound.
    pred_logmean = c(-1.5, -0.2, 0.4)
  )
  # Only station 2 has field-collected TSE coordinates.
  tsegeocoded_locais <- data.table::data.table(
    local_id = 2L,
    tse_long = -61.5,
    tse_lat = -8.5
  )

  out <- finalize_coords(
    copy(locais),
    copy(model_predictions),
    copy(tsegeocoded_locais)
  )

  # Station 1 ships the model's pick, so both accuracy columns are the winner's own.
  expect_equal(out[local_id == 1L, final_long], -60)
  expect_equal(out[local_id == 1L, conf_dist_km], 1.2)
  expect_equal(out[local_id == 1L, final_logmean], -1.5)

  # Station 2 ships ground truth, so both take their zero-error values.
  expect_equal(out[local_id == 2L, final_long], -61.5)
  expect_equal(out[local_id == 2L, conf_dist_km], 0)
  expect_equal(out[local_id == 2L, final_logmean], log(GBM_LOG_OFFSET))

  # No model prediction can undercut the ground-truth value.
  expect_lt(out[local_id == 2L, final_logmean], min(model_predictions$pred_logmean))
})

test_that("finalize_coords keeps final_logmean out of the published schema", {
  geocoded <- data.table::data.table(
    local_id = 1L,
    ano = 2018L,
    sg_uf = "AC",
    cd_localidade_tse = 1L,
    cod_localidade_ibge = 1200013L,
    nr_zona = 1L,
    nr_locvot = 1L,
    nr_cep = "69900000",
    nm_localidade = "x",
    nm_locvot = "x",
    ds_endereco = "x",
    ds_bairro = "x",
    pred_long = -60,
    pred_lat = -9,
    conf_dist_km = 0.5,
    final_logmean = -1.5,
    tse_long = NA_real_,
    tse_lat = NA_real_,
    final_long = -60,
    final_lat = -9
  )

  expect_false("final_logmean" %in% names(to_geocoded_export_schema(geocoded)))
})
