## Spec tests for clean_text_for_geocodebr() (R/data_cleaning.R).
## Lowercases, transliterates to ASCII, replaces any non [a-z0-9 ] run with a
## single space, and trims. Punctuation and ordinal marks collapse to spaces.

test_that("clean_text_for_geocodebr lowercases, deaccents and strips punctuation", {
  expect_equal(clean_text_for_geocodebr("Rua São João, No 45"), "rua sao joao no 45")
  expect_equal(clean_text_for_geocodebr("PRAÇA da Sé"), "praca da se")
  expect_equal(clean_text_for_geocodebr("CENTRO 2"), "centro 2")
})

test_that("clean_text_for_geocodebr collapses repeated separators and trims", {
  expect_equal(clean_text_for_geocodebr("  a---b   c  "), "a b c")
})
