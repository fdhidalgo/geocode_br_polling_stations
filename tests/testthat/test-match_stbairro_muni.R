## Spec tests for match_stbairro_muni() (R/string_matching.R).
## Matches polling-station streets against census street records and station
## neighbourhoods against census neighbourhood records, attaching the coordinates
## of each best match. Empty street input returns NULL.

make_locais <- function() {
  data.table::data.table(
    local_id = c(1L, 2L),
    normalized_st = c("rua das flores", "qqq nomatch"),
    normalized_bairro = c("centro velho", "zzz nomatch")
  )
}

make_st <- function() {
  data.table::data.table(
    norm_street = c("rua das flores", "rua nova"),
    long = c(-60, -61),
    lat = c(-9, -8)
  )
}

make_bairro <- function() {
  data.table::data.table(
    norm_bairro = c("centro velho", "jardim"),
    long = c(-62, -63),
    lat = c(-7, -6)
  )
}

test_that("match_stbairro_muni matches street and neighbourhood independently", {
  out <- match_stbairro_muni(make_locais(), make_st(), make_bairro())
  expect_equal(nrow(out), 2L)
  r1 <- out[local_id == 1L]
  expect_equal(r1$match_st, "rua das flores")
  expect_equal(r1$match_long_st, -60)
  expect_equal(r1$match_lat_st, -9)
  expect_equal(r1$match_bairro, "centro velho")
  expect_equal(r1$match_long_bairro, -62)
  expect_equal(r1$match_lat_bairro, -7)
})

test_that("match_stbairro_muni still returns the nearest row for a poor match", {
  out <- match_stbairro_muni(make_locais(), make_st(), make_bairro())
  r2 <- out[local_id == 2L]
  # Both aggregates report their nearest row and let mindist carry the bad news.
  expect_false(is.na(r2$match_st))
  expect_gt(r2$mindist_st, 0.3)
  expect_false(is.na(r2$match_bairro))
  expect_gt(r2$mindist_bairro, 0.4)
})

test_that("match_stbairro_muni leaves a station with no street to match NA", {
  locais <- make_locais()
  locais[local_id == 2L, normalized_st := NA_character_]
  r2 <- match_stbairro_muni(locais, make_st(), make_bairro())[local_id == 2L]
  expect_true(is.na(r2$match_st))
  expect_true(is.infinite(r2$mindist_st))
  expect_true(is.na(r2$match_long_st))
})

test_that("match_stbairro_muni scores the one field each aggregate knows", {
  out <- match_stbairro_muni(make_locais(), make_st(), make_bairro())
  r1 <- out[local_id == 1L]
  expect_equal(r1$sim_street_st, 0)
  expect_equal(r1$sim_bairro_bairro, 0)
})

test_that("match_stbairro_muni returns NULL when there are no street rows", {
  expect_null(match_stbairro_muni(make_locais(), make_st()[0], make_bairro()))
})
