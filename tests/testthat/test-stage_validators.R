# The stage validators are the gate the export targets depend on, so each has to
# stop the build rather than warn or pass a bad table through.

test_that("validate_model_data_merge rejects empty, unkeyed, or duplicated tables", {
  good <- data.table::data.table(local_id = 1:3, x = c(1, 2, 3))

  expect_error(validate_model_data_merge(good[0]))
  expect_error(validate_model_data_merge(data.table::data.table(x = 1:3)))
  expect_error(validate_model_data_merge(rbind(good, good[1])))
  expect_equal(validate_model_data_merge(good)$n_rows, 3L)
})

test_that("validate_model_predictions requires a numeric, complete pred_dist", {
  good <- data.table::data.table(local_id = 1:3, pred_dist = c(0, 1.5, 2))

  expect_error(validate_model_predictions(good[0]))
  expect_error(validate_model_predictions(data.table::data.table(local_id = 1:3)))
  expect_error(validate_model_predictions(data.table::data.table(local_id = 1:3, pred_dist = c(0, NA, 2))))
  expect_error(validate_model_predictions(data.table::data.table(local_id = 1:3, pred_dist = c("a", "b", "c"))))
  expect_equal(validate_model_predictions(good)$n_rows, 3L)
})

test_that("validate_final_output requires the published columns and a unique station-year key", {
  good <- data.table::data.table(
    local_id = 1:3,
    final_lat = 1,
    final_long = 2,
    ano = 2024L,
    nr_zona = 1:3,
    nr_locvot = 1:3,
    nm_locvot = "a",
    nm_localidade = "b"
  )

  expect_error(validate_final_output(good[0]))
  expect_error(validate_final_output(good[, .SD, .SDcols = -"final_lat"]), "missing required columns")
  expect_error(validate_final_output(rbind(good, good[1])), "duplicate key combinations")
  expect_equal(suppressMessages(validate_final_output(good))$n_rows, 3L)
})
