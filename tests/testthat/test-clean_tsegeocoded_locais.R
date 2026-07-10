## Fail-loud spec tests for clean_tsegeocoded_locais() (R/data_cleaning.R),
## cleanup phase 3, finding H1. The former code guarded the 2024 file with
## `if (length(tse_files) >= 4 && file.exists(...))`, silently dropping a
## ground-truth year when absent. The fixed contract asserts the expected file
## count up front (2018/2020/2022 today; 2024 is added by the release work), so a
## missing or unexpected file fails loud before any read.

test_that("clean_tsegeocoded_locais requires exactly the expected file count", {
  # The count check runs before any file is read, so dummy paths are fine.
  expect_error(
    clean_tsegeocoded_locais(c("a.csv", "b.csv"), muni_ids = NULL, locais = NULL),
    "Expected 3 TSE"
  )
  # Four files (a stray or prematurely-added 2024 file) also fails loud rather
  # than being silently combined.
  expect_error(
    clean_tsegeocoded_locais(rep("x.csv", 4), muni_ids = NULL, locais = NULL),
    "Expected 3 TSE"
  )
})
