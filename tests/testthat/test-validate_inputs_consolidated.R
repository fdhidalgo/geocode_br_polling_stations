## Fail-loud spec tests for validate_inputs_consolidated() (R/validation.R),
## cleanup phase 3, finding H4. The validator previously only warning()'d when a
## dataset size fell outside its expected range, letting the pipeline proceed on
## inputs that failed validation. The fixed contract stops, naming the failed
## checks; the happy path still returns the validation result.

dev_config <- list(dev_mode = TRUE)

test_that("validate_inputs_consolidated stops when a dataset size is out of range", {
  # Empty inputs fall below every dev-mode minimum (muni >= 30, inep >= 1000,
  # locais >= 1000), so all three checks fail.
  empty <- data.table::data.table(x = integer())
  err <- expect_error(
    validate_inputs_consolidated(empty, empty, empty, dev_config),
    "Input data validation failed"
  )
  expect_match(conditionMessage(err), "muni_ids_size")
  expect_match(conditionMessage(err), "locais_size")
})

test_that("validate_inputs_consolidated returns a passing result for in-range inputs", {
  # Dev-mode ranges: muni 30-100, locais 1000-20000. inep_codes is the national
  # codes table in both modes (never filtered), so its range stays national
  # (100000-300000) even under dev_config.
  muni <- data.table::data.table(x = seq_len(50))
  inep <- data.table::data.table(x = seq_len(150000))
  locais <- data.table::data.table(x = seq_len(2000))
  out <- validate_inputs_consolidated(muni, inep, locais, dev_config)
  expect_true(out$passed)
  expect_s3_class(out, "validation_result")
})
