## Spec tests for process_year_pairs() (R/panel_creation.R).
## Given a running panel keyed on local_id_<year_from>, it grafts on the matched
## local_id_<year_to> column from best_pairs, adding rows for any year_from id not
## already in the panel. The caller only reaches it with non-empty pairs.
##
## best_pairs is standardized by reference, so tests pass copies. Assertions are
## keyed on local_id (not row position) to stay robust to row ordering.

test_that("process_year_pairs joins the target-year id onto matching rows", {
  panel <- data.table::data.table(local_id_2018 = c("a", "b"))
  best_pairs <- data.table::data.table(
    x_local_id = c("a", "b"),
    y_local_id = c("a2", "b2")
  )
  out <- process_year_pairs(copy(panel), copy(best_pairs), 2018, 2020)

  expect_true(all(c("local_id_2018", "local_id_2020") %in% names(out)))
  expect_equal(out[local_id_2018 == "a", local_id_2020], "a2")
  expect_equal(out[local_id_2018 == "b", local_id_2020], "b2")
})

test_that("process_year_pairs adds rows for year_from ids missing from the panel", {
  panel <- data.table::data.table(local_id_2018 = "a")
  best_pairs <- data.table::data.table(
    x_local_id = c("a", "b"),
    y_local_id = c("a2", "b2")
  )
  out <- process_year_pairs(copy(panel), copy(best_pairs), 2018, 2020)

  expect_setequal(out$local_id_2018, c("a", "b"))
  expect_equal(out[local_id_2018 == "b", local_id_2020], "b2")
})

test_that("process_year_pairs refuses to process a transition twice", {
  panel <- data.table::data.table(local_id_2018 = "a", local_id_2020 = "a2")
  best_pairs <- data.table::data.table(x_local_id = "a", y_local_id = "a3")
  expect_error(
    process_year_pairs(copy(panel), copy(best_pairs), 2018, 2020),
    "already has column"
  )
})
