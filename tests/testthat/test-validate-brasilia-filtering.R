# Load required libraries and source files
library(data.table)
source("../../R/filter_brasilia_municipal.R")
source("../../R/validate_brasilia_filtering.R")

test_that("validate_brasilia_filtering detects DF records in municipal years", {
  # Create test data with DF records in municipal years (should fail)
  test_dt <- data.table(
    sg_uf = c("DF", "DF", "SP"),
    ano = c(2008, 2014, 2008),
    local_id = 1:3
  )
  
  # Should fail validation
  expect_error(
    validate_brasilia_filtering(test_dt, stop_on_failure = TRUE),
    "Brasília filtering validation failed"
  )
  
  # Get results without stopping
  results <- validate_brasilia_filtering(test_dt, stop_on_failure = FALSE)
  
  expect_false(results$passed)
  expect_false(results$checks$no_df_municipal$passed)
  expect_equal(results$summary$df_records_total, 2)
})

test_that("validate_brasilia_filtering passes with correctly filtered data", {
  # Create test data
  test_dt <- data.table(
    sg_uf = c("DF", "DF", "SP", "RJ", "DF"),
    ano = c(2008, 2014, 2008, 2020, 2022),
    local_id = 1:5
  )
  
  # Filter it
  filtered_dt <- filter_brasilia_municipal_elections(test_dt)
  
  # Should pass validation
  results <- validate_brasilia_filtering(filtered_dt, stop_on_failure = FALSE)
  
  expect_true(results$passed)
  expect_true(results$checks$no_df_municipal$passed)
  expect_true(results$checks$df_federal_exists$passed)
  expect_true(results$checks$other_states_preserved$passed)
})

test_that("validate_brasilia_filtering handles data with no federal years", {
  # Create test data with only municipal years
  test_dt <- data.table(
    sg_uf = c("SP", "RJ", "MG"),
    ano = c(2008, 2012, 2016),
    local_id = 1:3
  )
  
  results <- validate_brasilia_filtering(test_dt, stop_on_failure = FALSE)
  
  expect_true(results$passed)
  expect_true(results$checks$no_df_municipal$passed)
  expect_true(is.na(results$checks$df_federal_exists$passed))
})

test_that("validate_brasilia_filtering handles alternative column names", {
  # Test with uf/ano_eleicao columns
  test_dt <- data.table(
    uf = c("DF", "SP"),
    ano_eleicao = c(2014, 2014),
    local_id = 1:2
  )
  
  results <- validate_brasilia_filtering(test_dt, stop_on_failure = FALSE)
  
  expect_true(results$passed)
  expect_equal(results$summary$df_records_total, 1)
})

test_that("validate_brasilia_filtering provides detailed failure information", {
  # Create test data with multiple DF records in different municipal years
  test_dt <- data.table(
    sg_uf = c("DF", "DF", "DF", "DF", "SP"),
    ano = c(2008, 2008, 2012, 2020, 2020),
    local_id = 1:5
  )
  
  results <- validate_brasilia_filtering(test_dt, stop_on_failure = FALSE)
  
  expect_false(results$passed)
  expect_false(results$checks$no_df_municipal$passed)
  
  # Check details
  details <- results$checks$no_df_municipal$details
  expect_equal(nrow(details), 3)  # 3 different municipal years
  expect_equal(details[get == 2008]$N, 2)  # 2 records in 2008
})

test_that("generate_brasilia_filtering_report creates accurate report", {
  # Create before data
  dt_before <- data.table(
    sg_uf = c("DF", "DF", "DF", "SP", "RJ"),
    ano = c(2008, 2014, 2020, 2020, 2020),
    local_id = 1:5
  )
  
  # Create after data (filtered)
  dt_after <- filter_brasilia_municipal_elections(dt_before)
  
  # Generate report
  report <- generate_brasilia_filtering_report(dt_before, dt_after)
  
  # Check summary
  expect_equal(report$summary$total_records_before, 5)
  expect_equal(report$summary$total_records_after, 3)
  expect_equal(report$summary$df_records_before, 3)
  expect_equal(report$summary$df_records_after, 1)
  expect_equal(report$summary$df_records_removed, 2)
  
  # Check details by year
  expect_equal(nrow(report$details_by_year), 3)  # 3 years with DF data
  
  # 2008 should be completely removed
  year_2008 <- report$details_by_year[year == 2008]
  expect_equal(year_2008$records_before, 1)
  expect_equal(year_2008$records_after, 0)
  expect_equal(year_2008$records_removed, 1)
})

test_that("generate_brasilia_filtering_report handles empty DF data", {
  # Create data with no DF records
  dt_before <- data.table(
    sg_uf = c("SP", "RJ", "MG"),
    ano = c(2008, 2012, 2016),
    local_id = 1:3
  )
  
  dt_after <- filter_brasilia_municipal_elections(dt_before)
  
  report <- generate_brasilia_filtering_report(dt_before, dt_after)
  
  expect_equal(report$summary$df_records_before, 0)
  expect_equal(report$summary$df_records_after, 0)
  expect_equal(report$summary$df_records_removed, 0)
})

test_that("generate_brasilia_filtering_report saves to file", {
  # Create test data
  dt_before <- data.table(
    sg_uf = c("DF", "DF"),
    ano = c(2008, 2014),
    local_id = 1:2
  )
  
  dt_after <- filter_brasilia_municipal_elections(dt_before)
  
  # Generate report with output file
  temp_file <- tempfile(fileext = ".rds")
  report <- generate_brasilia_filtering_report(dt_before, dt_after, temp_file)
  
  # Check file was created
  expect_true(file.exists(temp_file))
  
  # Load and verify
  loaded_report <- readRDS(temp_file)
  expect_equal(loaded_report$summary$df_records_removed, 1)
  
  # Clean up
  unlink(temp_file)
})

test_that("validation summary includes all relevant information", {
  test_dt <- data.table(
    sg_uf = c("DF", "SP", "RJ", "MG", "DF"),
    ano = c(2014, 2014, 2016, 2018, 2022),
    local_id = 1:5
  )
  
  results <- validate_brasilia_filtering(test_dt, stop_on_failure = FALSE)
  
  # Check summary contents
  expect_equal(results$summary$total_records, 5)
  expect_equal(results$summary$df_records_total, 2)
  expect_equal(results$summary$unique_states, 4)
  expect_equal(length(results$summary$years_in_data), 4)
  expect_true(all(c(2014, 2016, 2018, 2022) %in% results$summary$years_in_data))
})

test_that("validate_brasilia_filtering returns validation results invisibly", {
  # Use data that will definitely pass validation
  test_dt <- data.table(
    sg_uf = c("SP", "RJ", "DF"),
    ano = c(2014, 2016, 2022),  # DF only in federal year
    local_id = 1:3
  )
  
  # Capture output to suppress printing
  capture.output({
    results <- validate_brasilia_filtering(test_dt, stop_on_failure = FALSE)
  })
  
  # Should still return results
  expect_true(is.list(results))
  expect_true(results$passed)
})