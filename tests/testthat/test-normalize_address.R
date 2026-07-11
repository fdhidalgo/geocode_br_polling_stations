## Spec tests for normalize_address() (R/data_cleaning.R).
## Expected outputs are hand-authored from the documented normalization rules
## (lowercase -> transliterate to ASCII -> strip punctuation -> drop generic
## location descriptors -> expand av/r prefixes -> collapse "s n" -> squish),
## not snapshotted from current output.

test_that("normalize_address applies each rule on targeted inputs", {
  expect_equal(normalize_address("Av. São João"), "avenida sao joao") # av becomes avenida; accents stripped
  expect_equal(normalize_address("Av Brasil"), "avenida brasil") # av prefix without period
  expect_equal(normalize_address("R Onze"), "rua onze") # r becomes rua
  expect_equal(normalize_address("Rua das Flores"), "rua das flores") # "rua" is not re-expanded
  expect_equal(normalize_address("S N"), "sn") # collapse the standalone "s n" token
  expect_equal(normalize_address("Rua A, S N"), "rua a sn") # collapse "s n" mid-string
  expect_equal(normalize_address("José María"), "jose maria") # diacritics stripped
  expect_equal(normalize_address("PRAÇA DA SÉ"), "praca da se") # lowercased; cedilla flattened
})

test_that("normalize_address drops generic location descriptors", {
  expect_equal(normalize_address("Escola Zona Rural"), "escola")
  expect_equal(normalize_address("Povoado São Félix"), "sao felix")
  expect_equal(normalize_address("Localidade Boa Vista"), "boa vista")
})

test_that("normalize_address is vectorized and preserves order", {
  out <- normalize_address(c("Av Brasil", "R Onze", "S N"))
  expect_equal(out, c("avenida brasil", "rua onze", "sn"))
})

test_that("normalize_address matches the curated real-string fixture", {
  fx <- data.table::fread(
    testthat::test_path("fixtures", "normalize_strings.csv"),
    encoding = "UTF-8"
  )[fn == "normalize_address"]
  expect_equal(normalize_address(fx$input), fx$expected)
})
