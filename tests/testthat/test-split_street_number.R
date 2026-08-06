## Spec tests for split_street_number() (R/data_cleaning.R).
## It splits a TSE address line into the street name and house number that geocodebr
## takes as separate fields. The hard part is that Brazilian street names routinely
## contain numbers ("BR 364", "AVENIDA 12 DE OUTUBRO", "RUA 15"), so a rule that simply
## pulls digits out destroys the name it was supposed to keep.

test_that("split_street_number reads the house number from trailing position", {
  r <- split_street_number("RUA SAO JOAO, 45")
  expect_equal(r$logradouro, "rua sao joao")
  expect_equal(r$numero, 45L)
})

test_that("split_street_number prefers an explicit N marker over position", {
  r <- split_street_number("AV. 12 DE OUTUBRO N 3221 - FONE 069-3541-7112")
  # The street's own "12" survives; the phone number does not become a house number.
  expect_equal(r$logradouro, "av 12 de outubro")
  expect_equal(r$numero, 3221L)

  expect_equal(split_street_number("RUA XV DE NOVEMBRO No 100")$numero, 100L)
})

test_that("split_street_number keeps numbers that belong to the street name", {
  # Highway identifier and milepost: both are the location, neither is a house number.
  r <- split_street_number("BR 364 - KM 114 S/N")
  expect_equal(r$logradouro, "br 364 km 114")
  expect_true(is.na(r$numero))

  r <- split_street_number("RODOVIA AC 401 KM-15 S/N VILA REDENCAO")
  expect_equal(r$logradouro, "rodovia ac 401 km 15 vila redencao")
  expect_true(is.na(r$numero))

  # A street named by a number, with no house number at all.
  r <- split_street_number("RUA 15")
  expect_equal(r$logradouro, "rua 15")
  expect_true(is.na(r$numero))
})

test_that("split_street_number treats sem-numero markers as no house number", {
  expect_equal(split_street_number("RUA ENEIDE BATISTA SN")$logradouro, "rua eneide batista")
  expect_true(is.na(split_street_number("RUA PROJETADA S/N")$numero))
  # Repeated markers are common in the TSE data.
  expect_equal(split_street_number("RUA GENY ASSIS S/N S/N")$logradouro, "rua geny assis")
})

test_that("split_street_number drops unit complements without losing the house number", {
  r <- split_street_number("RUA A, 123, CASA 2")
  expect_equal(r$logradouro, "rua a")
  expect_equal(r$numero, 123L)

  r <- split_street_number("AVENIDA BRASIL 1200 QUADRA 5")
  expect_equal(r$logradouro, "avenida brasil")
  expect_equal(r$numero, 1200L)
})

test_that("split_street_number does not read 'AP' as an apartment", {
  # In this data "AP" is a state highway prefix, a street name, or short for Aparecida --
  # never an apartment.
  expect_equal(
    split_street_number("RODOVIA AP 070, COMUNIDADE DE INAJA")$logradouro,
    "rodovia ap 070 comunidade de inaja"
  )
  expect_equal(split_street_number("RUA MARIA AP SOARES, SN")$logradouro, "rua maria ap soares")
})

test_that("split_street_number reports an address with no street as missing", {
  r <- split_street_number("S/N")
  expect_true(is.na(r$logradouro))
  expect_true(is.na(r$numero))
})

test_that("split_street_number is vectorized over its input", {
  r <- split_street_number(c("RUA SAO JOAO, 45", "RUA 15", NA_character_))
  expect_equal(r$logradouro, c("rua sao joao", "rua 15", NA_character_))
  expect_equal(r$numero, c(45L, NA_integer_, NA_integer_))
})
