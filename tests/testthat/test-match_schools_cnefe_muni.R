## Spec tests for match_schools_cnefe_muni() (R/string_matching.R).
## Matches polling-station names against CNEFE school descriptions, attaching the
## CNEFE coordinates of the best name match, plus a similarity per address field.
## Empty CNEFE input returns NULL.

make_locais <- function() {
  data.table::data.table(
    local_id = c(1L, 2L),
    normalized_name = c("escola central", "qqq nomatch"),
    normalized_st = c("rua das flores", "rua das flores"),
    normalized_bairro = c("centro", "centro")
  )
}

make_schools_cnefe <- function() {
  data.table::data.table(
    norm_desc = c("escola central", "escola norte"),
    cnefe_long = c(-60, -61),
    cnefe_lat = c(-9, -8),
    norm_street = c("rua das flores", "avenida norte"),
    norm_bairro = c("centro", "norte")
  )
}

test_that("match_schools_cnefe_muni attaches coordinates of the best name match", {
  out <- match_schools_cnefe_muni(make_locais(), make_schools_cnefe())
  expect_equal(nrow(out), 2L)
  r1 <- out[local_id == 1L]
  expect_equal(r1$match_schools_cnefe, "escola central")
  expect_equal(r1$mindist_schools_cnefe, 0)
  expect_equal(r1$match_long_schools_cnefe, -60)
  expect_equal(r1$match_lat_schools_cnefe, -9)
})

test_that("match_schools_cnefe_muni scores name, street and bairro of the selected row", {
  out <- match_schools_cnefe_muni(make_locais(), make_schools_cnefe())
  r1 <- out[local_id == 1L]
  # Station 1 agrees with the selected school on all three fields.
  expect_equal(r1$sim_name_schools_cnefe, 0)
  expect_equal(r1$sim_street_schools_cnefe, 0)
  expect_equal(r1$sim_bairro_schools_cnefe, 0)
  # CNEFE school rows carry no whole address line to compare.
  expect_true(is.na(r1$sim_addr_schools_cnefe))
})

test_that("match_schools_cnefe_muni exposes a name match that disagrees on bairro", {
  locais <- make_locais()
  locais[local_id == 1L, normalized_bairro := "vila nova"]
  out <- match_schools_cnefe_muni(locais, make_schools_cnefe())
  r1 <- out[local_id == 1L]
  # The name still matches perfectly, so the whole-string distance sees nothing wrong;
  # only the decomposed bairro similarity records the disagreement.
  expect_equal(r1$mindist_schools_cnefe, 0)
  expect_equal(r1$sim_name_schools_cnefe, 0)
  expect_gt(r1$sim_bairro_schools_cnefe, 0)
})

test_that("match_schools_cnefe_muni leaves a non-matching station NA", {
  out <- match_schools_cnefe_muni(make_locais(), make_schools_cnefe())
  r2 <- out[local_id == 2L]
  expect_true(is.na(r2$match_schools_cnefe))
  expect_true(is.na(r2$match_long_schools_cnefe))
  # No selected reference row means no field to compare against.
  expect_true(is.na(r2$sim_name_schools_cnefe))
  expect_true(is.na(r2$sim_street_schools_cnefe))
  expect_true(is.na(r2$sim_bairro_schools_cnefe))
})

test_that("match_schools_cnefe_muni returns NULL when there are no CNEFE rows", {
  expect_null(match_schools_cnefe_muni(make_locais(), make_schools_cnefe()[0]))
})
