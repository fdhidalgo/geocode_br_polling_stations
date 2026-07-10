## Fail-loud spec tests for clean_tsegeocoded_locais() (R/data_cleaning.R),
## cleanup phase 3, finding H1. The former code guarded the 2024 file with
## `if (length(tse_files) >= 4 && file.exists(...))`, silently dropping a
## ground-truth year when absent. The fixed contract asserts the expected file
## count up front (2018/2020/2022/2024 after the 2006-2024 re-release, #48), so a
## missing or unexpected file fails loud before any read.

test_that("clean_tsegeocoded_locais requires exactly the expected file count", {
  # The count check runs before any file is read, so dummy paths are fine.
  # Three files (the pre-2024 set, now missing the re-wired 2024 file) fails loud.
  expect_error(
    clean_tsegeocoded_locais(c("a.csv", "b.csv", "c.csv"), muni_ids = NULL, locais = NULL),
    "Expected 4 TSE"
  )
  # Five files (a stray extra file) also fails loud rather than being silently
  # combined.
  expect_error(
    clean_tsegeocoded_locais(rep("x.csv", 5), muni_ids = NULL, locais = NULL),
    "Expected 4 TSE"
  )
})
