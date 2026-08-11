## Spec tests for match_inep_muni() (R/string_matching.R).
## For one municipality it matches polling-station names against INEP school
## names, and station addresses against INEP addresses, attaching the matched
## school's coordinates for each. Every station gets the nearest INEP row; how far
## away it is, is what mindist reports. Deciding a candidate is too far to use is
## the selection model's job, not this function's. Empty INEP input returns NULL.
##
## The fixture uses one name-match station, one address-only-match station, and
## one station that resembles nothing (the shape the spec prescribes).

make_locais <- function() {
  data.table::data.table(
    local_id = c(1L, 2L, 3L),
    normalized_name = c("escola central", "zzz nomatch", "qqq"),
    normalized_addr = c("rua a", "avenida brasil", "www")
  )
}

make_inep <- function() {
  data.table::data.table(
    norm_school = c("escola central", "outra escola"),
    norm_addr = c("rua x", "avenida brasil"),
    longitude = c(-60, -61),
    latitude = c(-9, -8)
  )
}

test_that("match_inep_muni links a station by school name and attaches coordinates", {
  out <- match_inep_muni(make_locais(), make_inep())
  expect_equal(nrow(out), 3L)
  expect_equal(out$local_id, 1:3)

  r1 <- out[local_id == 1L]
  expect_equal(r1$match_inep_name, "escola central")
  expect_equal(r1$mindist_inep_name, 0)
  expect_equal(r1$match_long_inep_name, -60)
  expect_equal(r1$match_lat_inep_name, -9)
})

test_that("match_inep_muni links a station by address when the name does not match", {
  out <- match_inep_muni(make_locais(), make_inep())
  r2 <- out[local_id == 2L]
  # The name still gets its nearest school, but at a distance that marks it unusable.
  expect_gt(r2$mindist_inep_name, 0.4)
  expect_equal(r2$match_inep_addr, "avenida brasil")
  expect_equal(r2$mindist_inep_addr, 0)
  expect_equal(r2$match_long_inep_addr, -61)
  expect_equal(r2$match_lat_inep_addr, -8)
})

test_that("match_inep_muni reports total dissimilarity as distance 1, not a dropped match", {
  out <- match_inep_muni(make_locais(), make_inep())
  r3 <- out[local_id == 3L]
  # "qqq" / "www" share no character with any INEP row, so Jaro-Winkler saturates at 1.
  # The row is still returned: an exact-token pre-filter would instead have dropped it,
  # and the model would never have seen how bad it was.
  expect_equal(r3$mindist_inep_name, 1)
  expect_equal(r3$mindist_inep_addr, 1)
  expect_false(is.na(r3$match_long_inep_name))
})

test_that("match_inep_muni leaves a station with no name to match fully NA", {
  locais <- make_locais()
  locais[local_id == 3L, normalized_name := NA_character_]
  r3 <- match_inep_muni(locais, make_inep())[local_id == 3L]
  expect_true(is.na(r3$match_inep_name))
  expect_true(is.infinite(r3$mindist_inep_name))
  expect_true(is.na(r3$match_long_inep_name))
})

test_that("match_inep_muni scores both INEP fields on each candidate", {
  out <- match_inep_muni(make_locais(), make_inep())
  r1 <- out[local_id == 1L]
  # Station 1 was selected on the school name, but the address of that same INEP row is
  # scored too: "rua a" vs "rua x" disagrees, which the name distance alone cannot show.
  expect_equal(r1$sim_name_inep_name, 0)
  expect_gt(r1$sim_addr_inep_name, 0)
  # INEP has no separate street or neighbourhood column.
  expect_true(is.na(r1$sim_street_inep_name))
  expect_true(is.na(r1$sim_bairro_inep_name))

  r2 <- out[local_id == 2L]
  # Selected on address; the school name of that row is scored as well.
  expect_equal(r2$sim_addr_inep_addr, 0)
  expect_gt(r2$sim_name_inep_addr, 0)
})

test_that("match_inep_muni returns NULL when there are no INEP rows", {
  expect_null(match_inep_muni(make_locais(), make_inep()[0]))
})
