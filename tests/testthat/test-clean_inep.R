## Spec tests for clean_inep() (R/data_cleaning.R).
## clean_inep standardizes column names (lowercase, de-accented, spaces ->
## underscores), drops schools with no latitude, joins the INEP-code -> IBGE-code
## crosswalk, and derives normalized name/address columns (stripping a trailing
## 5-digit CEP + everything after it from the address).
##
## clean_inep renames its first argument's columns by reference, so tests pass a
## data.table::copy().

make_inep_data <- function() {
  data.table::data.table(
    "Escola" = c("Escola Municipal João", "Colégio Estadual Dom Bosco", "Creche Sol"),
    "Codigo INEP" = c("11111111", "22222222", "33333333"),
    "UF" = c("AC", "AC", "AC"),
    "Município" = c("Rio Branco", "Rio Branco", "Rio Branco"),
    "Endereço" = c("Rua A 12345 Centro", "Av. Brasil 999", "Rua Sem Latitude"),
    "Latitude" = c(-9.9, -9.8, NA_real_),
    "Longitude" = c(-67.8, -67.7, -67.6)
  )
}

make_inep_codes <- function() {
  data.table::data.table(
    codigo_inep = c("11111111", "22222222", "33333333"),
    cod_localidade_ibge = c(1200401L, 1200401L, 1200401L)
  )
}

test_that("clean_inep drops schools with missing latitude", {
  out <- clean_inep(copy(make_inep_data()), copy(make_inep_codes()))
  expect_equal(nrow(out), 2L) # the NA-latitude row is dropped
  expect_false("33333333" %in% out$codigo_inep)
})

test_that("clean_inep joins the IBGE crosswalk and derives normalized columns", {
  out <- clean_inep(copy(make_inep_data()), copy(make_inep_codes()))
  expect_true(all(c("cod_localidade_ibge", "norm_school", "norm_addr") %in% names(out)))
  expect_equal(unique(out$cod_localidade_ibge), 1200401L)

  joao <- out[codigo_inep == "11111111"]
  expect_equal(joao$norm_school, "joao")
  # trailing 5-digit CEP block and everything after it are stripped from the address
  expect_equal(joao$norm_addr, "rua a")

  bosco <- out[codigo_inep == "22222222"]
  expect_equal(bosco$norm_school, "dom bosco")
  # a 3-digit house number is not a CEP, so it survives normalization
  expect_equal(bosco$norm_addr, "avenida brasil 999")
})
