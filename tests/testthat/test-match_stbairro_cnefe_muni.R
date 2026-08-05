## Spec tests for match_stbairro_cnefe_muni() (R/string_matching.R).
## Matches polling-station streets against CNEFE street records and station
## neighbourhoods against CNEFE neighbourhood records, attaching the coordinates
## of each best match. Empty street input returns NULL. `source` names the output
## columns and nothing else, so both censuses match identically.

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

test_that("match_stbairro_cnefe_muni matches street and neighbourhood independently", {
  out <- match_stbairro_cnefe_muni(make_locais(), make_st(), make_bairro(), "cnefe")
  expect_equal(nrow(out), 2L)
  r1 <- out[local_id == 1L]
  expect_equal(r1$match_cnefe_st, "rua das flores")
  expect_equal(r1$match_long_cnefe_st, -60)
  expect_equal(r1$match_lat_cnefe_st, -9)
  expect_equal(r1$match_cnefe_bairro, "centro velho")
  expect_equal(r1$match_long_cnefe_bairro, -62)
  expect_equal(r1$match_lat_cnefe_bairro, -7)
})

test_that("match_stbairro_cnefe_muni leaves a non-matching station NA", {
  out <- match_stbairro_cnefe_muni(make_locais(), make_st(), make_bairro(), "cnefe")
  r2 <- out[local_id == 2L]
  expect_true(is.na(r2$match_cnefe_st))
  expect_true(is.na(r2$match_long_cnefe_st))
  expect_true(is.na(r2$match_cnefe_bairro))
})

test_that("match_stbairro_cnefe_muni returns NULL when there are no street rows", {
  expect_null(match_stbairro_cnefe_muni(make_locais(), make_st()[0], make_bairro(), "cnefe"))
})

test_that("match_stbairro_cnefe_muni renames columns for the agro source without changing values", {
  cnefe <- match_stbairro_cnefe_muni(make_locais(), make_st(), make_bairro(), "cnefe")
  agro <- match_stbairro_cnefe_muni(make_locais(), make_st(), make_bairro(), "agrocnefe")

  expect_equal(
    names(agro),
    c(
      "local_id",
      "match_agrocnefe_st",
      "mindist_agrocnefe_st",
      "match_long_agrocnefe_st",
      "match_lat_agrocnefe_st",
      "match_agrocnefe_bairro",
      "mindist_agrocnefe_bairro",
      "match_long_agrocnefe_bairro",
      "match_lat_agrocnefe_bairro"
    )
  )
  expect_equal(unname(as.list(agro)), unname(as.list(cnefe)))
})
