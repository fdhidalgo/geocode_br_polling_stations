## Spec tests for cep_to_string() (R/data_cleaning.R).
## TSE stores the CEP as a number, so every Sao Paulo-range CEP has lost its leading zero
## and has to be padded back to eight digits before geocodebr can match it.

test_that("cep_to_string restores the leading zero", {
  expect_equal(cep_to_string(1310100), "01310100")
  expect_equal(cep_to_string(78994000), "78994000")
})

test_that("cep_to_string reports unusable CEPs as missing", {
  # 0 is the missing-value sentinel; 502 is a truncated pre-1992 five-digit CEP.
  expect_true(is.na(cep_to_string(0)))
  expect_true(is.na(cep_to_string(502)))
  expect_true(is.na(cep_to_string(NA_real_)))
})

test_that("cep_to_string is vectorized over its input", {
  expect_equal(
    cep_to_string(c(78994000, 1310100, 0, NA)),
    c("78994000", "01310100", NA, NA)
  )
})
