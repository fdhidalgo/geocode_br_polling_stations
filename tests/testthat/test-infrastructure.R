# Basic infrastructure test to ensure testing framework is properly set up

test_that("testthat infrastructure is working", {
  # Basic arithmetic test
  expect_equal(1 + 1, 2)
  
  # Test that TRUE is TRUE
  expect_true(TRUE)
  
  # Test that FALSE is FALSE
  expect_false(FALSE)
})

test_that("required R source files exist", {
  # Check that key R files exist
  expect_true(file.exists("../../R/data_cleaning_fns.R"))
  expect_true(file.exists("../../R/string_matching_geocode_fns.R"))
  expect_true(file.exists("../../R/panel_id_fns.R"))
  expect_true(file.exists("../../R/functions_validate.R"))
})

test_that("test helper functions work correctly", {
  # Test the helper data creation functions
  test_data <- create_test_polling_data(n = 5)
  
  expect_s3_class(test_data, "data.table")
  expect_equal(nrow(test_data), 5)
  expect_true("local_id" %in% names(test_data))
  expect_true("normalized_name" %in% names(test_data))
  
  # Test INEP data creation
  inep_data <- create_test_inep_data(n = 3)
  expect_equal(nrow(inep_data), 3)
  expect_true("longitude" %in% names(inep_data))
  expect_true("latitude" %in% names(inep_data))
})