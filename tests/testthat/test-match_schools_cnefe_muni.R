## Spec tests for match_schools_cnefe_muni() (R/string_matching.R).
## Matches polling-station names against CNEFE school descriptions, attaching the
## CNEFE coordinates and neighbourhood of the best name match. Empty CNEFE input
## returns NULL.

make_locais <- function() {
  data.table::data.table(
    local_id = c(1L, 2L),
    normalized_name = c("escola central", "qqq nomatch")
  )
}

make_schools_cnefe <- function() {
  data.table::data.table(
    norm_desc = c("escola central", "escola norte"),
    cnefe_long = c(-60, -61),
    cnefe_lat = c(-9, -8),
    norm_bairro = c("centro", "norte")
  )
}

test_that("match_schools_cnefe_muni attaches coordinates and bairro of the best name match", {
  out <- match_schools_cnefe_muni(make_locais(), make_schools_cnefe())
  expect_equal(nrow(out), 2L)
  r1 <- out[local_id == 1L]
  expect_equal(r1$match_schools_cnefe, "escola central")
  expect_equal(r1$mindist_schools_cnefe, 0)
  expect_equal(r1$match_long_schools_cnefe, -60)
  expect_equal(r1$match_lat_schools_cnefe, -9)
  expect_equal(r1$match_bairro_schools_cnefe, "centro")
})

test_that("match_schools_cnefe_muni leaves a non-matching station NA", {
  out <- match_schools_cnefe_muni(make_locais(), make_schools_cnefe())
  r2 <- out[local_id == 2L]
  expect_true(is.na(r2$match_schools_cnefe))
  expect_true(is.na(r2$match_long_schools_cnefe))
  expect_true(is.na(r2$match_bairro_schools_cnefe))
})

test_that("match_schools_cnefe_muni returns NULL when there are no CNEFE rows", {
  expect_null(match_schools_cnefe_muni(make_locais(), make_schools_cnefe()[0]))
})
