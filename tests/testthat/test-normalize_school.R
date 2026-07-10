## Spec tests for normalize_school() (R/data_cleaning.R).
## normalize_school strips diacritics, lowercases, removes punctuation, and then
## removes a curated list of generic school-type terms (e.g. "escola municipal",
## "emef", "creche") so that only the distinctive part of the name remains.
## Expected outputs are hand-authored from that contract, not snapshotted.

test_that("normalize_school removes generic school-type terms", {
  expect_equal(normalize_school("Escola Municipal João"), "joao")
  expect_equal(normalize_school("EMEF Dom Pedro"), "dom pedro")
  expect_equal(normalize_school("Colégio Estadual Dom Bosco"), "dom bosco")
  expect_equal(normalize_school("CRECHE Tia Ana"), "tia ana")
  expect_equal(normalize_school("Grupo Escolar Castro Alves"), "castro alves")
})

test_that("normalize_school strips diacritics, case and punctuation", {
  expect_equal(normalize_school("COLÉGIO ESTADUAL Paula"), "paula")
  expect_equal(normalize_school("E.M.E.F. Pedro Álvares"), "pedro alvares")
})

test_that("normalize_school leaves a name with no generic terms intact", {
  expect_equal(normalize_school("Templo Central"), "templo central")
})

test_that("normalize_school is vectorized and preserves order", {
  out <- normalize_school(c("EMEF Dom Pedro", "CRECHE Tia Ana"))
  expect_equal(out, c("dom pedro", "tia ana"))
})

test_that("normalize_school matches the curated real-string fixture", {
  fx <- data.table::fread(
    testthat::test_path("fixtures", "normalize_strings.csv"),
    encoding = "UTF-8"
  )[fn == "normalize_school"]
  expect_equal(normalize_school(fx$input), fx$expected)
})
