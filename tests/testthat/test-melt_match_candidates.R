## Spec tests for melt_match_candidates() (R/model.R).
## Turns one match table's per-candidate column groups into one row per candidate
## coordinate. The contract that matters is alignment: every group must be split by
## candidate in the same order, so a value never lands on the wrong candidate type.
## Both failure modes below are silent if the columns are matched by prefix instead of
## named, because melt() pads a short group with NA and assigns members by position.

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

TYPES <- c(st = "street", bairro = "neighbourhood")

test_that("melt_match_candidates keeps every column group aligned to its candidate type", {
  long <- melt_match_candidates(make_two_candidate_matches(), TYPES)
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

test_that("melt_match_candidates fails when a similarity column is missing", {
  # Without sim_bairro_st, the neighbourhood's 0.31 would otherwise shift onto the street
  # candidate: the group is padded from the left, not matched to its candidate.
  matches <- make_two_candidate_matches()
  matches[, sim_bairro_st := NULL]
  expect_error(melt_match_candidates(matches, TYPES))
})

test_that("melt_match_candidates fails when a candidate type is unnamed", {
  # A match table that gained a candidate the model was not told about would otherwise be
  # silently dropped rather than mislabelled.
  expect_error(melt_match_candidates(make_two_candidate_matches(), c(st = "street")))
})
