## Spec tests for make_panel_1block() (R/panel_creation.R).
## For one blocking group it runs the Fellegi-Sunter record linkage across the years
## the block holds and returns a long local_id -> panel_id table in which polling
## stations that are the same place across years share a panel_id. A block covering
## only one year has no pairs to link, and it returns NULL.
##
## The function prints progress via cat(); capture.output() keeps test output
## clean. Assertions are on the distinct local_id -> panel_id mapping, not on row
## count or order, so they do not pin the current duplicate-row behavior (tracked
## separately) or any particular ordering (the H6 concern).

make_block <- function() {
  data.table::data.table(
    local_id = c("2018_1", "2018_2", "2020_1", "2020_2"),
    ano = c(2018L, 2018L, 2020L, 2020L),
    sg_uf = "AC",
    cod_localidade_ibge = 1200013L,
    normalized_name = c("escola central", "hospital norte", "escola central", "hospital norte"),
    normalized_addr = c("rua a", "avenida b", "rua a", "avenida b")
  )
}

test_that("make_panel_1block links the same station across years under one panel_id", {
  invisible(capture.output(
    out <- make_panel_1block(copy(make_block()))
  ))

  expect_s3_class(out, "data.table")
  expect_named(out, c("local_id", "panel_id"), ignore.order = TRUE)

  mapping <- unique(out)
  expect_setequal(mapping$local_id, c("2018_1", "2018_2", "2020_1", "2020_2"))

  pid <- function(id) unique(mapping[local_id == id, panel_id])
  # Same place in both years -> shared panel_id; the two distinct places differ.
  expect_equal(pid("2018_1"), pid("2020_1"))
  expect_equal(pid("2018_2"), pid("2020_2"))
  expect_false(identical(pid("2018_1"), pid("2018_2")))
})

test_that("make_panel_1block returns NULL when the block covers only one year", {
  invisible(capture.output(
    out <- make_panel_1block(copy(make_block()[ano == 2018L]))
  ))
  expect_null(out)
})
