## Spec tests for the fused CNEFE aggregation helpers (R/utilities.R),
## introduced by the #67 reshape (spec 2026-07-partition-reference-data, D5/D6).
##
## aggregate_cnefe_coords() collapses cleaned CNEFE rows to per-municipality
## group medians, keeping only groups seen more than once. combine_cnefe_state_
## component() row-binds a named component across per-state results and asserts
## the D6 no-cross-state-duplicate invariant for keyed aggregates.

library(data.table)

test_that("aggregate_cnefe_coords takes group medians and keeps singletons", {
  addr <- data.table(
    id_munic_7 = c(1L, 1L, 1L, 1L),
    norm_street = c("rua a", "rua a", "rua a", "rua b"),
    cnefe_long = c(-60, -62, -64, -50),
    cnefe_lat = c(-8, -8, -8, -9)
  )
  out <- aggregate_cnefe_coords(addr, "norm_street")

  # Both groups are kept here (including the n == 1 "rua b"); the singleton drop
  # happens later in combine_cnefe_state_component(), after the duplicate check.
  expect_equal(nrow(out), 2L)
  expect_equal(out[norm_street == "rua a"]$n, 3L)
  expect_equal(out[norm_street == "rua a"]$long, median(c(-60, -62, -64)))
  expect_equal(out[norm_street == "rua b"]$n, 1L)
  expect_equal(names(out), c("id_munic_7", "norm_street", "long", "lat", "n"))
})

test_that("aggregate_cnefe_coords ignores NA coordinates in the median", {
  addr <- data.table(
    id_munic_7 = c(5L, 5L, 5L),
    norm_bairro = c("centro", "centro", "centro"),
    cnefe_long = c(-60, NA, -64),
    cnefe_lat = c(-8, -8, NA)
  )
  out <- aggregate_cnefe_coords(addr, "norm_bairro")

  expect_equal(out$long, median(c(-60, -64)))
  expect_equal(out$lat, -8)
  expect_equal(out$n, 3L)
})

test_that("per-state aggregate + combine equals national aggregate", {
  # Keys never span states, so aggregating each state then row-binding must
  # reproduce aggregating the concatenated national table (the old path).
  s1 <- data.table(
    id_munic_7 = c(1L, 1L, 2L, 2L, 2L),
    norm_street = c("rua a", "rua a", "rua c", "rua c", "rua d"),
    cnefe_long = c(-60, -61, -50, -52, -40),
    cnefe_lat = c(-8, -8, -9, -9, -7)
  )
  s2 <- data.table(
    id_munic_7 = c(3L, 3L, 3L),
    norm_street = c("rua e", "rua e", "rua f"),
    cnefe_long = c(-30, -32, -20),
    cnefe_lat = c(-6, -6, -5)
  )

  national <- rbindlist(list(s1, s2))
  old <- national[,
    .(long = median(cnefe_long, na.rm = TRUE), lat = median(cnefe_lat, na.rm = TRUE), n = .N),
    by = .(id_munic_7, norm_street)
  ][n > 1]

  state_results <- list(
    list(st = aggregate_cnefe_coords(s1, "norm_street")),
    list(st = aggregate_cnefe_coords(s2, "norm_street"))
  )
  new <- combine_cnefe_state_component(
    state_results,
    "st",
    unique_key = c("id_munic_7", "norm_street")
  )

  expect_identical(as.list(old), as.list(new))
})

test_that("combine_cnefe_state_component row-binds schools without a key check", {
  # Schools are legitimately many-per-municipality; no uniqueness invariant.
  state_results <- list(
    list(schools = data.table(id_munic_7 = c(1L, 1L), norm_desc = c("a", "b"))),
    list(schools = data.table(id_munic_7 = c(1L, 2L), norm_desc = c("a", "c")))
  )
  out <- combine_cnefe_state_component(state_results, "schools")
  expect_equal(nrow(out), 4L)
})

test_that("combine_cnefe_state_component stops on a cross-slice duplicate key", {
  # The same (id_munic_7, norm_street) in two slices is exactly what a
  # municipality spanning two state files would produce (D6).
  state_results <- list(
    list(st = data.table(id_munic_7 = 1L, norm_street = "x", long = 1, lat = 1, n = 3L)),
    list(st = data.table(id_munic_7 = 1L, norm_street = "x", long = 2, lat = 2, n = 5L))
  )
  expect_error(
    combine_cnefe_state_component(
      state_results,
      "st",
      unique_key = c("id_munic_7", "norm_street")
    ),
    "duplicated across"
  )
})

test_that("combine_cnefe_state_component catches a 1+1 cross-slice split", {
  # A key that appears exactly once in each of two slices (n == 1 per state) is
  # the case the per-state [n > 1] filter used to hide: both singletons would be
  # dropped before the invariant could see them. The check must fire before the
  # singleton drop. (Also confirms the surviving n > 1 groups are still thinned.)
  state_results <- list(
    list(
      st = data.table(
        id_munic_7 = c(1L, 9L),
        norm_street = c("x", "keep"),
        long = c(1, 1),
        lat = c(1, 1),
        n = c(1L, 2L)
      )
    ),
    list(
      st = data.table(
        id_munic_7 = 1L,
        norm_street = "x",
        long = 2,
        lat = 2,
        n = 1L
      )
    )
  )
  expect_error(
    combine_cnefe_state_component(
      state_results,
      "st",
      unique_key = c("id_munic_7", "norm_street")
    ),
    "duplicated across"
  )
})

test_that("combine_cnefe_state_component drops singletons after the check", {
  # No cross-slice duplicate: the check passes, then n == 1 groups are dropped
  # and only n > 1 groups survive (matching the national [n > 1] filter).
  state_results <- list(
    list(
      st = data.table(
        id_munic_7 = c(1L, 1L),
        norm_street = c("keep", "solo"),
        long = c(1, 2),
        lat = c(1, 2),
        n = c(4L, 1L)
      )
    )
  )
  out <- combine_cnefe_state_component(
    state_results,
    "st",
    unique_key = c("id_munic_7", "norm_street")
  )
  expect_equal(out$norm_street, "keep")
  expect_equal(out$n, 4L)
})
