# Test file for data cleaning functions

test_that("normalize_address handles Portuguese characters correctly", {
  # Test with Portuguese text containing special characters
  input <- "Av. São João, nº 123 - Zona Rural"
  expected <- "avenida sao joao nº 123"
  
  result <- normalize_address(input)
  expect_equal(result, expected)
})

test_that("normalize_address handles multiple address formats", {
  # Test street abbreviations
  expect_equal(normalize_address("R. Maria da Silva"), "rua maria da silva")
  expect_equal(normalize_address("Av. Brasil"), "avenida brasil")
  
  # Test s/n (sem número) normalization
  expect_equal(normalize_address("Rua A, s n"), "rua a sn")
  
  # Test telephone number removal
  expect_equal(normalize_address("Rua B Tel. 1234-5678"), "rua b")
  
  # Test punctuation removal
  expect_equal(normalize_address("Rua C, 123/456"), "rua c 123456")
})

test_that("normalize_address removes location keywords", {
  expect_equal(normalize_address("Povoado São José"), "sao jose")
  expect_equal(normalize_address("Localidade Rural"), "rural")
  expect_equal(normalize_address("Fazenda Zona Rural"), "fazenda")
})

test_that("normalize_school removes school type synonyms", {
  # Test various school type removals
  expect_equal(normalize_school("E.M.E.I. João Silva"), "joao silva")
  expect_equal(normalize_school("Escola Municipal Maria Santos"), "maria santos")
  expect_equal(normalize_school("CMEI Professor José"), "jose")
  expect_equal(normalize_school("Colégio Estadual São Paulo"), "sao paulo")
})

test_that("normalize_school handles punctuation and accents", {
  # Test accent removal
  expect_equal(normalize_school("Escola São José"), "sao jose")
  
  # Test punctuation removal
  expect_equal(normalize_school("E.E. Dr. Antonio Carlos"), "dr antonio carlos")
  
  # Test multiple school synonyms in one name
  expect_equal(normalize_school("Escola Municipal de Ensino Fundamental José Silva"), "de jose silva")
})