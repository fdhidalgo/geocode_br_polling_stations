# Unit tests for electoral data validation rules

test_that("geocoded polling stations validation rules work correctly", {
  # Source the validation rules
  source("../../R/validation_rules_electoral.R")
  
  # Create test data with valid coordinates
  valid_data <- data.table::data.table(
    local_id = 1:5,
    final_lat = c(-10.5, -20.3, -5.0, -30.0, 2.0),
    final_long = c(-65.0, -45.0, -70.0, -50.0, -40.0),
    ano = c(2010, 2012, 2014, 2016, 2018),
    nr_zona = c(1, 2, 3, 4, 5),
    nr_locvot = c(101, 102, 103, 104, 105),
    pred_lat = c(-10.5, -20.3, -5.0, -30.0, 2.0),
    pred_long = c(-65.0, -45.0, -70.0, -50.0, -40.0)
  )
  
  # Test valid data passes all rules
  result <- validate_geocoded_polling_stations(valid_data)
  expect_true(result$passed)
  expect_s3_class(result, "validation_result")
  
  # Test data with invalid latitude (outside Brazil)
  invalid_lat_data <- data.table::copy(valid_data)
  invalid_lat_data[1, final_lat := -40.0]  # Too far south
  
  result_invalid_lat <- validate_geocoded_polling_stations(invalid_lat_data)
  expect_false(result_invalid_lat$passed)
  
  # Test data with invalid longitude (outside Brazil)
  invalid_long_data <- data.table::copy(valid_data)
  invalid_long_data[1, final_long := -80.0]  # Too far west
  
  result_invalid_long <- validate_geocoded_polling_stations(invalid_long_data)
  expect_false(result_invalid_long$passed)
  
  # Test data with missing coordinates
  missing_coord_data <- data.table::copy(valid_data)
  missing_coord_data[1, final_lat := NA]
  
  result_missing <- validate_geocoded_polling_stations(missing_coord_data)
  expect_false(result_missing$passed)
  
  # Test duplicate station records
  duplicate_data <- data.table::copy(valid_data)
  duplicate_data <- rbind(duplicate_data, duplicate_data[1])  # Duplicate first row
  
  result_duplicate <- validate_geocoded_polling_stations(duplicate_data)
  expect_false(result_duplicate$passed)
  
  # Test year range validation
  invalid_year_data <- data.table::copy(valid_data)
  invalid_year_data[1, ano := 2005]  # Before valid range
  
  result_invalid_year <- validate_geocoded_polling_stations(invalid_year_data)
  expect_false(result_invalid_year$passed)
})

test_that("panel IDs validation rules work correctly", {
  # Source the validation rules
  source("../../R/validation_rules_electoral.R")
  
  # Create valid test data
  valid_panel_data <- data.table::data.table(
    panel_id = 1:5,
    local_id = 101:105,
    lat = c(-10.5, -20.3, -5.0, -30.0, 2.0),
    long = c(-65.0, -45.0, -70.0, -50.0, -40.0),
    pred_dist = c(0.1, 0.2, 0.3, 0.4, 0.5)
  )
  
  # Test valid data passes all rules
  result <- validate_panel_ids(valid_panel_data)
  expect_true(result$passed)
  expect_s3_class(result, "validation_result")
  
  # Test duplicate panel IDs
  duplicate_panel_data <- data.table::copy(valid_panel_data)
  duplicate_panel_data[2, panel_id := 1]  # Create duplicate
  
  result_duplicate <- validate_panel_ids(duplicate_panel_data)
  expect_false(result_duplicate$passed)
  
  # Test invalid coordinates
  invalid_coord_data <- data.table::copy(valid_panel_data)
  invalid_coord_data[1, lat := 10.0]  # Too far north for Brazil
  
  result_invalid <- validate_panel_ids(invalid_coord_data)
  expect_false(result_invalid$passed)
  
  # Test missing panel ID
  missing_panel_data <- data.table::copy(valid_panel_data)
  missing_panel_data[1, panel_id := NA]
  
  result_missing <- validate_panel_ids(missing_panel_data)
  expect_false(result_missing$passed)
  
  # Test negative pred_dist
  negative_dist_data <- data.table::copy(valid_panel_data)
  negative_dist_data[1, pred_dist := -0.5]
  
  result_negative <- validate_panel_ids(negative_dist_data)
  expect_false(result_negative$passed)
})

test_that("validation report printing works correctly", {
  # Source the validation rules
  source("../../R/validation_rules_electoral.R")
  
  # Create test data
  test_data <- data.table::data.table(
    local_id = 1:3,
    final_lat = c(-10.5, -20.3, -5.0),
    final_long = c(-65.0, -45.0, -70.0),
    ano = c(2010, 2012, 2014),
    nr_zona = c(1, 2, 3),
    nr_locvot = c(101, 102, 103)
  )
  
  # Validate and capture output
  result <- validate_geocoded_polling_stations(test_data)
  
  # Test that print function runs without error (it's expected to produce output)
  expect_no_error(print_electoral_validation_report(result))
  
  # Capture output to test content
  output <- capture.output(print_electoral_validation_report(result))
  
  # Check that output contains expected elements
  expect_true(any(grepl("VALIDATION REPORT", output)))
  expect_true(any(grepl("geocoded_polling_stations", output)))
  expect_true(any(grepl("Overall Status", output)))
})

test_that("coordinate bounds are correctly enforced", {
  # Source the validation rules
  source("../../R/validation_rules_electoral.R")
  
  # Test extreme valid coordinates (Brazil's bounds)
  extreme_coords <- data.table::data.table(
    local_id = 1:4,
    final_lat = c(-33.75, 5.27, -20.0, 0.0),  # South extreme, North extreme, middle
    final_long = c(-73.98, -34.79, -50.0, -60.0),  # West extreme, East extreme, middle
    ano = rep(2018, 4),
    nr_zona = 1:4,
    nr_locvot = 101:104
  )
  
  result <- validate_geocoded_polling_stations(extreme_coords)
  expect_true(result$passed)
  
  # Test just outside bounds
  outside_bounds <- data.table::data.table(
    local_id = 1:4,
    final_lat = c(-33.76, 5.28, -20.0, 0.0),  # Just outside south, just outside north
    final_long = c(-73.99, -34.78, -50.0, -60.0),  # Just outside west, just outside east
    ano = rep(2018, 4),
    nr_zona = 1:4,
    nr_locvot = 101:104
  )
  
  result_outside <- validate_geocoded_polling_stations(outside_bounds)
  expect_false(result_outside$passed)
})

test_that("unique station ID validation catches all duplicates", {
  # Source the validation rules
  source("../../R/validation_rules_electoral.R")
  
  # Create data with complex duplicate scenario
  complex_data <- data.table::data.table(
    local_id = c(1, 1, 1, 2, 2),
    final_lat = rep(-20.0, 5),
    final_long = rep(-50.0, 5),
    ano = c(2010, 2010, 2012, 2010, 2012),
    nr_zona = c(1, 1, 1, 1, 1),
    nr_locvot = c(101, 101, 101, 101, 101)
  )
  
  result <- validate_geocoded_polling_stations(complex_data)
  expect_false(result$passed)
  
  # Check that making them unique fixes the issue
  complex_data[2, nr_locvot := 102]  # Make second row unique
  result_fixed <- validate_geocoded_polling_stations(complex_data)
  expect_true(result_fixed$passed)
})