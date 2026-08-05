## Spec tests for melt_match_candidates() (R/model.R).
## Turns one match table's per-candidate column groups into one row per candidate
## coordinate. The contract that matters is alignment: every group -- coordinates, the
## whole-string distance, and the four field similarities -- must be split by candidate
## in the same order, so a value never lands on the wrong candidate type.

make_two_candidate_matches <- function() {
  data.table::data.table(
    local_id = c(1L, 2L),
    match_long_st = c(-60, -61),
    match_lat_st = c(-9, -8),
    mindist_st = c(0.1, 0.2),
    sim_name_st = NA_real_,
    sim_street_st = c(0.11, 0.21),
    sim_bairro_st = NA_real_,
    sim_addr_st = NA_real_,
    match_long_bairro = c(-70, -71),
    match_lat_bairro = c(-19, -18),
    mindist_bairro = c(0.3, 0.4),
    sim_name_bairro = NA_real_,
    sim_street_bairro = NA_real_,
    sim_bairro_bairro = c(0.31, 0.41),
    sim_addr_bairro = NA_real_
  )
}

test_that("melt_match_candidates keeps every column group aligned to its candidate type", {
  long <- melt_match_candidates(make_two_candidate_matches(), c("street", "neighbourhood"))
  expect_equal(nrow(long), 4L)

  st <- long[type == "street"][order(local_id)]
  expect_equal(st$long, c(-60, -61))
  expect_equal(st$mindist, c(0.1, 0.2))
  expect_equal(st$sim_street, c(0.11, 0.21))
  expect_true(all(is.na(st$sim_bairro)))

  bairro <- long[type == "neighbourhood"][order(local_id)]
  expect_equal(bairro$long, c(-70, -71))
  expect_equal(bairro$mindist, c(0.3, 0.4))
  expect_equal(bairro$sim_bairro, c(0.31, 0.41))
  expect_true(all(is.na(bairro$sim_street)))
})

test_that("melt_match_candidates fails when a candidate type is unnamed", {
  # More column groups than names means the match table gained a candidate type without
  # the model being told about it -- a silent mislabel, so it must error.
  expect_error(melt_match_candidates(make_two_candidate_matches(), "street"))
})
