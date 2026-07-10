## Spec tests for match_stbairro_agrocnefe_muni() (R/string_matching.R).
## Structurally identical to match_stbairro_cnefe_muni but against the Agro CNEFE
## street/neighbourhood records, emitting agrocnefe-prefixed columns. Empty street
## input returns NULL.

make_locais <- function() {
  data.table::data.table(
    local_id = c(1L, 2L),
    normalized_st = c("estrada do porto", "qqq nomatch"),
    normalized_bairro = c("vila rural", "zzz nomatch")
  )
}

make_st <- function() {
  data.table::data.table(
    norm_street = c("estrada do porto", "linha nova"),
    long = c(-50, -51),
    lat = c(-5, -4)
  )
}

make_bairro <- function() {
  data.table::data.table(
    norm_bairro = c("vila rural", "gleba"),
    long = c(-52, -53),
    lat = c(-3, -2)
  )
}

test_that("match_stbairro_agrocnefe_muni matches street and neighbourhood and attaches coords", {
  out <- match_stbairro_agrocnefe_muni(make_locais(), make_st(), make_bairro())
  expect_equal(nrow(out), 2L)
  r1 <- out[local_id == 1L]
  expect_equal(r1$match_agrocnefe_st, "estrada do porto")
  expect_equal(r1$match_long_agrocnefe_st, -50)
  expect_equal(r1$match_lat_agrocnefe_st, -5)
  expect_equal(r1$match_agrocnefe_bairro, "vila rural")
  expect_equal(r1$match_long_agrocnefe_bairro, -52)
  expect_equal(r1$match_lat_agrocnefe_bairro, -3)
})

test_that("match_stbairro_agrocnefe_muni leaves a non-matching station NA", {
  out <- match_stbairro_agrocnefe_muni(make_locais(), make_st(), make_bairro())
  r2 <- out[local_id == 2L]
  expect_true(is.na(r2$match_agrocnefe_st))
  expect_true(is.na(r2$match_agrocnefe_bairro))
})

test_that("match_stbairro_agrocnefe_muni returns NULL when there are no street rows", {
  expect_null(match_stbairro_agrocnefe_muni(make_locais(), make_st()[0], make_bairro()))
})
