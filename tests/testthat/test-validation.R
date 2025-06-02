# Test file for validation functions

test_that("create_muni_ids_rules creates valid validator object", {
  skip_if_not_installed("validate")
  
  rules <- create_muni_ids_rules()
  
  # Check that rules is a validator object
  expect_s4_class(rules, "validator")
  
  # Check that it contains expected rules
  rule_names <- names(rules)
  expect_true("has_id_munic_7" %in% rule_names)
  expect_true("has_id_munic_6" %in% rule_names)
  expect_true("valid_state_codes" %in% rule_names)
  expect_true("unique_muni7_ids" %in% rule_names)
})

test_that("validate_muni_ids_data detects missing columns", {
  skip_if_not_installed("validate")
  
  # Create data missing required columns
  invalid_data <- data.frame(
    some_column = 1:5
  )
  
  result <- validate_muni_ids_data(invalid_data)
  
  # Check that validation detected missing columns
  expect_s3_class(result, "validation_result")
  expect_false(result$passed)
  
  # Check the actual confrontation result
  summary_result <- summary(result$result)
  
  # Check for specific failures
  expect_true(any(summary_result$name == "has_id_munic_7" & summary_result$fails > 0))
  expect_true(any(summary_result$name == "has_id_munic_6" & summary_result$fails > 0))
})

test_that("validate_muni_ids_data accepts valid data", {
  skip_if_not_installed("validate")
  
  # Create valid mock data
  valid_data <- data.frame(
    id_munic_7 = c(1234567, 2345678, 3456789),
    id_munic_6 = c(123456, 234567, 345678),
    id_estado = c(35, 41, 43),  # SP, PR, RS
    estado_abrev = c("SP", "PR", "RS"),
    municipio = c("São Paulo", "Curitiba", "Porto Alegre"),
    stringsAsFactors = FALSE
  )
  
  result <- validate_muni_ids_data(valid_data)
  
  # Check that validation passed
  expect_s3_class(result, "validation_result")
  expect_true(result$passed)
  
  # Check the actual confrontation result
  summary_result <- summary(result$result)
  
  # Check that all structure validations pass
  expect_true(all(summary_result$fails[summary_result$name == "has_id_munic_7"] == 0))
  expect_true(all(summary_result$fails[summary_result$name == "has_id_munic_6"] == 0))
  expect_true(all(summary_result$fails[summary_result$name == "valid_state_codes"] == 0))
  expect_true(all(summary_result$fails[summary_result$name == "unique_muni7_ids"] == 0))
})

test_that("validate_muni_ids_data detects invalid state codes", {
  skip_if_not_installed("validate")
  
  # Create data with invalid state codes
  invalid_data <- data.frame(
    id_munic_7 = c(1234567, 2345678),
    id_munic_6 = c(123456, 234567),
    id_estado = c(10, 99),  # Invalid codes (outside 11-53 range)
    estado_abrev = c("XX", "YY"),
    municipio = c("Test1", "Test2"),
    stringsAsFactors = FALSE
  )
  
  result <- validate_muni_ids_data(invalid_data)
  
  # Check that validation failed
  expect_s3_class(result, "validation_result")
  expect_false(result$passed)
  
  # Check the actual confrontation result
  summary_result <- summary(result$result)
  
  # Check that state code validation fails
  expect_true(any(summary_result$name == "valid_state_codes" & summary_result$fails > 0))
})