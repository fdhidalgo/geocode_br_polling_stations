# Improved Municipality Code Fix Tests
# Testing the LOGIC, not specific data values

library(testthat)
library(data.table)

# Source the data cleaning functions which now includes fix_municipality_codes_2024
project_root <- rprojroot::find_root(rprojroot::is_rstudio_project)
source(file.path(project_root, "R", "data_cleaning.R"))

# Source validation functions for testing
source(file.path(project_root, "R_backup_20250620_consolidation", "fix_municipality_codes_2024_validation_fns.R"))

test_that("conversion preserves data integrity", {
  # Test that the function doesn't lose or corrupt data
  
  test_data <- data.table(
    CD_MUNICIPIO = c(100, 200, 300, NA),
    SG_UF = c("AC", "MT", "RJ", "SP"),
    other_col1 = 1:4,
    other_col2 = letters[1:4]
  )
  
  result <- fix_municipality_codes_2024(test_data, verbose = FALSE)
  
  # Core integrity checks
  expect_equal(nrow(result), nrow(test_data))
  expect_true("CD_MUNICIPIO_TSE" %in% names(result))
  expect_equal(result$other_col1, test_data$other_col1)
  expect_equal(result$other_col2, test_data$other_col2)
  
  # Original values should be preserved somewhere
  expect_true(all(test_data$CD_MUNICIPIO %in% result$CD_MUNICIPIO_TSE | 
                  is.na(test_data$CD_MUNICIPIO)))
})

test_that("IBGE codes follow expected format rules", {
  # Test that converted codes follow IBGE standards
  
  test_data <- data.table(
    CD_MUNICIPIO = 1:10,
    SG_UF = rep(c("AC", "MT"), 5)
  )
  
  result <- fix_municipality_codes_2024(test_data, verbose = FALSE)
  
  # Check IBGE code format (7 digits, specific ranges per state)
  ibge_codes <- result$CD_MUNICIPIO[result$CD_MUNICIPIO != result$CD_MUNICIPIO_TSE]
  
  if (length(ibge_codes) > 0) {
    # All IBGE codes should be 7 digits
    expect_true(all(nchar(as.character(ibge_codes)) == 7))
    
    # State code validation (first 2 digits)
    # AC codes should start with 12, MT with 51, etc.
    ac_codes <- result[SG_UF == "AC" & CD_MUNICIPIO != CD_MUNICIPIO_TSE]$CD_MUNICIPIO
    if (length(ac_codes) > 0) {
      expect_true(all(substr(ac_codes, 1, 2) == "12"))
    }
    
    mt_codes <- result[SG_UF == "MT" & CD_MUNICIPIO != CD_MUNICIPIO_TSE]$CD_MUNICIPIO
    if (length(mt_codes) > 0) {
      expect_true(all(substr(mt_codes, 1, 2) == "51"))
    }
  }
})

test_that("validation metrics are mathematically consistent", {
  # Test that validation produces logically consistent metrics
  
  original <- data.table(CD_MUNICIPIO = 1:100, SG_UF = rep("AC", 100))
  
  # Create a fixed version where half are converted
  fixed <- copy(original)
  fixed$CD_MUNICIPIO_TSE <- original$CD_MUNICIPIO
  fixed$CD_MUNICIPIO[1:50] <- fixed$CD_MUNICIPIO[1:50] + 1000000
  
  validation <- validate_municipality_code_fixes(original, fixed)
  
  # Mathematical consistency checks
  expect_equal(validation$total_records, nrow(original))
  expect_lte(validation$records_converted, validation$total_records)
  expect_gte(validation$conversion_rate, 0)
  expect_lte(validation$conversion_rate, 100)
  expect_equal(validation$valid_ibge_codes + validation$invalid_codes, 
               validation$total_records)
})

test_that("function handles missing data gracefully", {
  # Test various missing data scenarios
  
  # Empty dataset
  empty <- data.table(CD_MUNICIPIO = integer(), SG_UF = character())
  expect_silent(result_empty <- fix_municipality_codes_2024(empty, verbose = FALSE))
  expect_equal(nrow(result_empty), 0)
  
  # All NAs
  all_na <- data.table(CD_MUNICIPIO = rep(NA_integer_, 5), SG_UF = LETTERS[1:5])
  result_na <- fix_municipality_codes_2024(all_na, verbose = FALSE)
  expect_equal(nrow(result_na), 5)
  expect_true(all(is.na(result_na$CD_MUNICIPIO)))
  
  # Mixed NAs
  mixed <- data.table(
    CD_MUNICIPIO = c(1, NA, 3, NA, 5),
    SG_UF = rep("AC", 5)
  )
  result_mixed <- fix_municipality_codes_2024(mixed, verbose = FALSE)
  expect_equal(sum(is.na(result_mixed$CD_MUNICIPIO_TSE)), 2)
})

test_that("conversion is deterministic", {
  # Same input should always produce same output
  
  test_data <- data.table(
    CD_MUNICIPIO = sample(1:1000, 20),
    SG_UF = sample(c("AC", "MT", "SP"), 20, replace = TRUE)
  )
  
  result1 <- fix_municipality_codes_2024(test_data, verbose = FALSE)
  result2 <- fix_municipality_codes_2024(test_data, verbose = FALSE)
  
  expect_identical(result1, result2)
})

test_that("state-level metrics sum correctly", {
  # Test that state-level validation adds up to totals
  
  test_data <- data.table(
    CD_MUNICIPIO = 1:12,
    CD_MUNICIPIO_TSE = 1:12,
    SG_UF = rep(c("AC", "MT", "RJ"), each = 4)
  )
  
  # Simulate some conversions
  test_data$CD_MUNICIPIO[c(1,2,5,6,9,10)] <- test_data$CD_MUNICIPIO[c(1,2,5,6,9,10)] + 1000000
  
  validation <- validate_municipality_code_fixes(
    test_data[, .(CD_MUNICIPIO = CD_MUNICIPIO_TSE, SG_UF)],
    test_data
  )
  
  # State totals should sum to overall totals
  expect_equal(sum(validation$by_state$total), validation$total_records)
  expect_equal(sum(validation$by_state$converted), validation$records_converted)
})

test_that("unmatched codes are handled appropriately", {
  # Test behavior for codes not in mapping
  
  # Use a code we know won't exist in mapping
  test_data <- data.table(
    CD_MUNICIPIO = c(999999, 888888),
    SG_UF = c("XX", "YY"),
    NM_MUNICIPIO = c("Fake City 1", "Fake City 2")
  )
  
  result <- fix_municipality_codes_2024(test_data, verbose = FALSE)
  
  # Unmatched codes should remain unchanged
  expect_equal(result$CD_MUNICIPIO, test_data$CD_MUNICIPIO)
  expect_equal(result$CD_MUNICIPIO_TSE, test_data$CD_MUNICIPIO)
})