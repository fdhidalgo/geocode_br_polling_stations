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

test_that("match_stbairro_muni leaves a non-matching station NA", {
  out <- match_stbairro_muni(make_locais(), make_st(), make_bairro())
  r2 <- out[local_id == 2L]
  expect_true(is.na(r2$match_st))
  expect_true(is.na(r2$match_long_st))
  expect_true(is.na(r2$match_bairro))
})

test_that("match_stbairro_muni scores only the one field each aggregate knows", {
  out <- match_stbairro_muni(make_locais(), make_st(), make_bairro())
  r1 <- out[local_id == 1L]
  expect_equal(r1$sim_street_st, 0)
  expect_equal(r1$sim_bairro_bairro, 0)
  # Street and neighbourhood aggregates are coordinate medians over a whole street or
  # neighbourhood, so they carry no name, no address line, and not the other's field.
  expect_true(is.na(r1$sim_name_st))
  expect_true(is.na(r1$sim_bairro_st))
  expect_true(is.na(r1$sim_addr_st))
  expect_true(is.na(r1$sim_name_bairro))
  expect_true(is.na(r1$sim_street_bairro))
  expect_true(is.na(r1$sim_addr_bairro))
})

test_that("match_stbairro_muni returns NULL when there are no street rows", {
  expect_null(match_stbairro_muni(make_locais(), make_st()[0], make_bairro()))
})
