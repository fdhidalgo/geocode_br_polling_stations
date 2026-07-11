## Spec test for make_id_munic_7() (R/data_cleaning.R), the shared helper that
## unifies the divergent leading-zero guards in the CNEFE cleaners (#78). The
## invariant: cod_municipio is zero-padded to width 5, pasted onto the 2-digit
## cod_uf, and the result is a 7-digit IBGE code. A caller that lost the padding
## upstream (the #75 regression) must fail loud, not produce short codes.

test_that("make_id_munic_7 zero-pads a numeric cod_municipio to width 5", {
  # A numeric 401 (leading zeros already stripped) must pad back to "00401".
  expect_equal(make_id_munic_7("12", 401), 1200401)
})

test_that("make_id_munic_7 is vectorized over multiple municipalities", {
  expect_equal(
    make_id_munic_7(c("12", "35"), c("00401", "50308")),
    c(1200401, 3550308)
  )
})

test_that("make_id_munic_7 stops when a code is not 7 digits", {
  # A cod_uf that lost its leading structure yields a short code: fail loud.
  expect_error(
    make_id_munic_7("1", "00401"),
    "non-7-digit municipality codes"
  )
})
