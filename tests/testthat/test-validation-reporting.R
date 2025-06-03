# Unit tests for validation reporting functions

test_that("extract_failed_records works correctly", {
  # Source the functions
  source("../../R/validation_pipeline_stages.R")
  source("../../R/validation_reporting.R")
  
  # Create test data with known failures
  test_data <- data.table::data.table(
    id = 1:10,
    value = c(1:5, NA, NA, 8:10),
    category = c("A", "B", "A", "B", "A", "B", "A", "B", "A", "B")
  )
  
  # Create validation result with failures
  rules <- validator(
    no_missing_values = !is.na(value),
    positive_values = value > 0
  )
  
  val_result <- list(
    result = confront(test_data, rules),
    metadata = list(stage = "test", type = "test"),
    passed = FALSE,
    additional_checks = list()
  )
  class(val_result) <- "validation_result"
  
  # Test extraction
  failed_records <- extract_failed_records(val_result, test_data)
  
  expect_true(is.data.table(failed_records))
  expect_equal(nrow(failed_records), 2)  # Two records have NA values
  expect_true("failed_rule" %in% names(failed_records))
  expect_true("record_index" %in% names(failed_records))
  expect_equal(unique(failed_records$failed_rule), "no_missing_values")
  
  # Test with no failures
  good_data <- data.table::data.table(
    id = 1:5,
    value = 1:5
  )
  
  good_result <- list(
    result = confront(good_data, rules),
    metadata = list(stage = "test", type = "test"),
    passed = TRUE,
    additional_checks = list()
  )
  class(good_result) <- "validation_result"
  
  no_failures <- extract_failed_records(good_result, good_data)
  expect_equal(nrow(no_failures), 0)
})

test_that("summarize_validation_result works correctly", {
  source("../../R/validation_pipeline_stages.R")
  source("../../R/validation_reporting.R")
  
  # Create a validation result
  test_data <- data.table::data.table(
    id = 1:100,
    value = runif(100)
  )
  
  val_result <- validate_import_stage(
    data = test_data,
    stage_name = "test_import",
    expected_cols = c("id", "value"),
    min_rows = 50
  )
  
  # Test summarization
  summary_obj <- summarize_validation_result(val_result)
  
  expect_s3_class(summary_obj, "validation_summary")
  expect_equal(summary_obj$stage_name, "test_import")
  expect_equal(summary_obj$stage_type, "import")
  expect_true(summary_obj$overall_passed)
  expect_true(summary_obj$pass_rate > 0)
  expect_true(is.data.table(summary_obj$rule_details))
  expect_true("rule" %in% names(summary_obj$rule_details))
  
  # Check additional checks summary
  expect_false(is.null(summary_obj$additional_checks))
  expect_true("is_data_table" %in% summary_obj$additional_checks$check_name)
})

test_that("analyze_failure_patterns works correctly", {
  source("../../R/validation_reporting.R")
  
  # Create test failed records
  failed_records <- data.table::data.table(
    failed_rule = rep(c("rule1", "rule2", "rule1"), each = 3),
    record_index = 1:9,
    municipality = rep(c("A", "B", "C"), 3),
    year = rep(c(2020, 2021, 2022), 3)
  )
  
  # Test overall patterns
  patterns <- analyze_failure_patterns(failed_records)
  
  expect_true(is.list(patterns))
  expect_true("overall" %in% names(patterns))
  expect_true(is.data.table(patterns$overall))
  expect_equal(nrow(patterns$overall), 2)  # Two unique rules
  expect_equal(patterns$overall[failed_rule == "rule1"]$n_failures, 6)
  expect_equal(patterns$overall[failed_rule == "rule2"]$n_failures, 3)
  
  # Test grouped patterns
  patterns_grouped <- analyze_failure_patterns(failed_records, group_by = "municipality")
  
  expect_true(!is.null(patterns_grouped$grouped))
  expect_true(is.data.table(patterns_grouped$grouped))
  expect_true("municipality" %in% names(patterns_grouped$grouped))
})

test_that("generate_action_items works correctly", {
  source("../../R/validation_pipeline_stages.R")
  source("../../R/validation_reporting.R")
  
  # Create test validation summary
  test_data <- data.table::data.table(
    id = 1:100,
    value = c(runif(80), rep(NA, 20))  # 20% missing
  )
  
  val_result <- validate_import_stage(
    test_data,
    "test_stage",
    expected_cols = c("id", "value", "missing_col"),  # One missing column
    min_rows = 50
  )
  
  summary_obj <- summarize_validation_result(val_result)
  
  # Generate action items
  actions <- generate_action_items(summary_obj)
  
  expect_true(is.data.table(actions))
  expect_true(nrow(actions) > 0)  # Should have actions for missing column
  expect_true("priority" %in% names(actions))
  expect_true("action" %in% names(actions))
  expect_true("fix_command" %in% names(actions))
  
  # Check for missing column action
  missing_col_action <- actions[grepl("missing_col", rule)]
  expect_equal(nrow(missing_col_action), 1)
  expect_true(grepl("Add missing column", missing_col_action$action))
})


test_that("export_failed_records works correctly", {
  source("../../R/validation_reporting.R")
  
  # Create test failed records
  failed_records <- data.table::data.table(
    failed_rule = "rule1",
    id = 1:5,
    value = NA
  )
  
  # Test export
  temp_dir <- tempdir()
  export_dir <- file.path(temp_dir, "test_export")
  
  exported_file <- export_failed_records(failed_records, export_dir, "test_stage")
  
  expect_true(dir.exists(export_dir))
  expect_true(!is.null(exported_file))
  expect_true(file.exists(exported_file))
  
  # Check file contents
  loaded_data <- data.table::fread(exported_file)
  expect_equal(nrow(loaded_data), 5)
  expect_true("failed_rule" %in% names(loaded_data))
  
  # Test with empty data
  empty_records <- data.table::data.table()
  empty_export <- export_failed_records(empty_records, export_dir, "empty_stage")
  expect_null(empty_export)
  
  # Clean up
  unlink(export_dir, recursive = TRUE)
})

test_that("rule-specific action generation works correctly", {
  source("../../R/validation_reporting.R")
  
  # Test various rule patterns
  test_cases <- list(
    list(
      rule = "has_column_test_col",
      expected_action = "Add missing column: test_col"
    ),
    list(
      rule = "valid_lat",
      expected_action = "Fix coordinate values"
    ),
    list(
      rule = "no_na_id",
      expected_action = "Handle missing values in: id"
    ),
    list(
      rule = "unique_keys",
      expected_action = "Remove duplicate records"
    )
  )
  
  for (test in test_cases) {
    action <- generate_rule_specific_action(test$rule, "test_expression")
    expect_equal(action$action, test$expected_action)
    expect_true(nchar(action$details) > 0)
  }
})

