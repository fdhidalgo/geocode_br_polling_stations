## Spec tests for simplify_address_for_geocodebr() (R/data_cleaning.R).
## The function strips street-type prefixes (rua/avenida/...), house numbers,
## "sem numero" markers, and lot/block/house suffixes so geocodebr matches on the
## bare street name, then normalizes via clean_text_for_geocodebr.
##
## Prefix/number/suffix removal runs *before* the final lowercase, so it only
## fires on already-lowercased input. The pipeline feeds it normalized (lowercase)
## addresses, so these tests use lowercase inputs -- the real usage. The
## case-sensitivity of the pre-clean removals is tracked separately as a smell.

test_that("simplify_address_for_geocodebr strips prefixes and house numbers", {
  expect_equal(simplify_address_for_geocodebr("rua sao joao 45"), "sao joao")
  expect_equal(simplify_address_for_geocodebr("avenida brasil 1200 quadra 5"), "brasil")
  expect_equal(simplify_address_for_geocodebr("alameda dos anjos"), "dos anjos")
})

test_that("simplify_address_for_geocodebr strips sem-numero and lot/house suffixes", {
  expect_equal(simplify_address_for_geocodebr("praca da se sn"), "da se")
  expect_equal(simplify_address_for_geocodebr("rua a s n casa 2"), "a")
})

test_that("simplify_address_for_geocodebr can reduce to an empty string", {
  # travessa (prefix) + 7 (number) + "lote 3" (suffix) leaves nothing.
  expect_equal(simplify_address_for_geocodebr("travessa 7 lote 3"), "")
})
