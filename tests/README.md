# Testing Infrastructure

This directory contains the test suite for the geocode_br_polling_stations project using the `testthat` framework.

## Running Tests

To run all tests:

```r
# From R console in project root
source("tests/testthat.R")

# Or using devtools (if available)
devtools::test()

# Or using testthat directly
testthat::test_dir("tests/testthat")
```

## Test Structure

- `testthat.R` - Main test runner that sources all R functions and runs tests
- `testthat/` - Contains all test files
  - `helper-test-data.R` - Helper functions for creating test data
  - `test-infrastructure.R` - Basic tests to verify testing setup
  - `test-data-cleaning.R` - Tests for address/name normalization functions
  - `test-string-matching.R` - Tests for fuzzy string matching functions
  - `test-panel-id.R` - Tests for panel ID creation and record linkage
  - `test-validation.R` - Tests for data validation framework

## Writing New Tests

1. Create a new file in `tests/testthat/` named `test-<functionality>.R`
2. Use `test_that()` blocks to group related tests
3. Use helper functions from `helper-test-data.R` for consistent test data
4. Follow existing patterns for test organization

## Dependencies

The tests require these packages:
- `testthat` - Testing framework
- `stringr` - String manipulation (used by functions)
- `stringi` - String manipulation (used by functions)
- `data.table` - Data manipulation (used by functions)
- `validate` - Data validation (optional, some tests skip if not installed)
- `reclin` - Record linkage (optional, some tests skip if not installed)