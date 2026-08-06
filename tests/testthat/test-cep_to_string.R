## Spec test for cep_to_string() (R/data_cleaning.R).
## TSE stores the CEP as a number, so every Sao Paulo-range CEP has lost its leading zero
## and has to be padded back to eight digits. 0 is the missing-value sentinel and 502 is a
## truncated pre-1992 five-digit CEP; neither is usable.

test_that("cep_to_string pads to eight digits and rejects unusable CEPs", {
  expect_equal(
    cep_to_string(c(1310100, 78994000, 0, 502, NA)),
    c("01310100", "78994000", NA, NA, NA)
  )
})
