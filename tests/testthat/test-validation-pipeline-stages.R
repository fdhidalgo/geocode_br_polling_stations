# Unit tests for pipeline stage validation functions

test_that("validate_import_stage works correctly", {
  # Source the validation functions
  source("../../R/validation_pipeline_stages.R")
  
  # Test with valid data
  valid_data <- data.table::data.table(
    id = 1:100,
    name = paste0("Item_", 1:100),
    value = runif(100)
  )
  
  result <- validate_import_stage(
    data = valid_data,
    stage_name = "test_import",
    expected_cols = c("id", "name", "value"),
    min_rows = 50
  )
  
  expect_true(result$passed)
  expect_equal(result$metadata$stage, "test_import")
  expect_equal(result$metadata$type, "import")
  expect_equal(result$metadata$n_rows, 100)
  
  # Test with missing columns
  missing_col_data <- data.table::data.table(
    id = 1:100,
    name = paste0("Item_", 1:100)
  )
  
  result_missing <- validate_import_stage(
    data = missing_col_data,
    stage_name = "test_import",
    expected_cols = c("id", "name", "value"),
    min_rows = 50
  )
  
  expect_false(result_missing$passed)
  
  # Test with too few rows
  small_data <- data.table::data.table(
    id = 1:10,
    name = paste0("Item_", 1:10),
    value = runif(10)
  )
  
  result_small <- validate_import_stage(
    data = small_data,
    stage_name = "test_import",
    expected_cols = c("id", "name", "value"),
    min_rows = 50
  )
  
  expect_false(result_small$passed)
  
  # Test with non-data.table input
  df_data <- as.data.frame(valid_data)
  
  result_df <- validate_import_stage(
    data = df_data,
    stage_name = "test_import"
  )
  
  expect_false(result_df$passed)
})

test_that("validate_cleaning_stage works correctly", {
  source("../../R/validation_pipeline_stages.R")
  
  # Create original and cleaned data
  original_data <- data.table::data.table(
    id = 1:100,
    name = c(paste0("Item_", 1:90), rep(NA, 10)),
    address = paste0("Address ", 1:100)
  )
  
  cleaned_data <- data.table::data.table(
    id = 1:90,
    name = paste0("Item_", 1:90),
    address = paste0("Address ", 1:90),
    normalized_name = tolower(paste0("item_", 1:90)),
    normalized_address = tolower(paste0("address ", 1:90))
  )
  
  # Test valid cleaning
  result <- validate_cleaning_stage(
    cleaned_data = cleaned_data,
    original_data = original_data,
    stage_name = "test_cleaning",
    key_cols = c("id", "name")
  )
  
  expect_true(result$passed)
  expect_equal(result$metadata$rows_removed, 10)
  expect_equal(result$metadata$removal_rate, 10)
  expect_true(cleaned_data[, .N] == 90)  # Check cleaned data has correct rows
  
  # Test cleaning that removes all data (should fail)
  empty_cleaned <- data.table::data.table()
  
  result_empty <- validate_cleaning_stage(
    cleaned_data = empty_cleaned,
    original_data = original_data,
    stage_name = "test_cleaning"
  )
  
  expect_false(result_empty$passed)
  
  # Test cleaning with all NA column (should fail)
  bad_cleaned <- data.table::copy(cleaned_data)
  bad_cleaned[, bad_col := NA]
  
  result_bad <- validate_cleaning_stage(
    cleaned_data = bad_cleaned,
    stage_name = "test_cleaning"
  )
  
  expect_false(result_bad$passed)
})

test_that("validate_string_match_stage works correctly", {
  source("../../R/validation_pipeline_stages.R")
  
  # Create match result data
  match_data <- data.table::data.table(
    local_id = 1:100,
    matched_name = paste0("Match_", 1:100),
    lat = c(runif(80, -30, -5), rep(NA, 20)),
    long = c(runif(80, -70, -40), rep(NA, 20)),
    match_score = c(runif(80, 0.5, 1), rep(NA, 20))
  )
  
  # Test valid match data
  result <- validate_string_match_stage(
    match_data = match_data,
    stage_name = "test_match",
    id_col = "local_id",
    score_col = "match_score"
  )
  
  expect_true(result$passed)
  expect_equal(result$metadata$match_rate, 80)
  expect_equal(result$metadata$n_unique_ids, 100)
  
  # Test with missing ID column
  no_id_data <- data.table::copy(match_data)
  no_id_data[1:10, local_id := NA]
  
  result_no_id <- validate_string_match_stage(
    match_data = no_id_data,
    stage_name = "test_match",
    id_col = "local_id"
  )
  
  expect_false(result_no_id$passed)
  
  # Test with invalid scores
  bad_score_data <- data.table::copy(match_data)
  bad_score_data[1:10, match_score := 1.5]  # Score > 1
  
  result_bad_score <- validate_string_match_stage(
    match_data = bad_score_data,
    stage_name = "test_match",
    score_col = "match_score"
  )
  
  expect_false(result_bad_score$passed)
  
  # Test without coordinate columns
  no_coord_data <- match_data[, .(local_id, matched_name, match_score)]
  
  result_no_coord <- validate_string_match_stage(
    match_data = no_coord_data,
    stage_name = "test_match"
  )
  
  expect_false(result_no_coord$passed)
})

test_that("validate_merge_stage works correctly", {
  source("../../R/validation_pipeline_stages.R")
  
  # Create test data for merging
  left_data <- data.table::data.table(
    id = 1:100,
    name = paste0("Item_", 1:100),
    value_left = runif(100)
  )
  
  right_data <- data.table::data.table(
    id = c(1:80, 101:110),  # 80 matches, 10 non-matches
    category = sample(c("A", "B", "C"), 90, replace = TRUE),
    value_right = runif(90)
  )
  
  # Test left join
  merged_left <- merge(left_data, right_data, by = "id", all.x = TRUE)
  
  result_left <- validate_merge_stage(
    merged_data = merged_left,
    left_data = left_data,
    right_data = right_data,
    stage_name = "test_merge_left",
    merge_keys = "id",
    join_type = "left"
  )
  
  expect_true(result_left$passed)
  expect_equal(nrow(merged_left), 100)  # All left rows preserved
  expect_true(result_left$metadata$within_expected)
  
  # Test inner join
  merged_inner <- merge(left_data, right_data, by = "id")
  
  result_inner <- validate_merge_stage(
    merged_data = merged_inner,
    left_data = left_data,
    right_data = right_data,
    stage_name = "test_merge_inner",
    merge_keys = "id",
    join_type = "inner"
  )
  
  expect_true(result_inner$passed)
  expect_equal(nrow(merged_inner), 80)  # Only matching rows
  
  # Test merge with NA keys (should fail)
  bad_merged <- data.table::copy(merged_left)
  bad_merged[1:5, id := NA]
  
  result_bad <- validate_merge_stage(
    merged_data = bad_merged,
    left_data = left_data,
    right_data = right_data,
    stage_name = "test_merge_bad",
    merge_keys = "id",
    join_type = "left"
  )
  
  # Debug: check what's happening
  # print(result_bad$additional_checks)
  
  expect_false(result_bad$passed)
  
  # Test unexpected row count
  truncated_merge <- merged_left[1:50]  # Only half the expected rows
  
  result_truncated <- validate_merge_stage(
    merged_data = truncated_merge,
    left_data = left_data,
    right_data = right_data,
    stage_name = "test_merge_truncated",
    merge_keys = "id",
    join_type = "left"
  )
  
  expect_false(result_truncated$passed)
  expect_false(result_truncated$metadata$within_expected)
})

test_that("validate_prediction_stage works correctly", {
  source("../../R/validation_pipeline_stages.R")
  
  # Create prediction data
  pred_data <- data.table::data.table(
    id = 1:100,
    prediction = sample(c(0, 1), 100, replace = TRUE),
    probability = runif(100),
    feature1 = rnorm(100)
  )
  
  # Test valid predictions
  result <- validate_prediction_stage(
    predictions = pred_data,
    stage_name = "test_predictions",
    pred_col = "prediction",
    prob_col = "probability"
  )
  
  expect_true(result$passed)
  expect_equal(result$metadata$n_predictions, 100)
  expect_true(result$metadata$mean_probability >= 0 && result$metadata$mean_probability <= 1)
  
  # Test with missing predictions
  missing_pred_data <- data.table::copy(pred_data)
  missing_pred_data[, prediction := NA]
  
  result_missing <- validate_prediction_stage(
    predictions = missing_pred_data,
    stage_name = "test_predictions",
    pred_col = "prediction"
  )
  
  expect_false(result_missing$passed)
  
  # Test with invalid probabilities
  bad_prob_data <- data.table::copy(pred_data)
  bad_prob_data[1:10, probability := -0.1]  # Negative probabilities
  
  result_bad_prob <- validate_prediction_stage(
    predictions = bad_prob_data,
    stage_name = "test_predictions",
    pred_col = "prediction",
    prob_col = "probability"
  )
  
  expect_false(result_bad_prob$passed)
})

test_that("validate_output_stage works correctly", {
  source("../../R/validation_pipeline_stages.R")
  
  # Create output data
  output_data <- data.table::data.table(
    local_id = 1:100,
    final_lat = runif(100, -30, -5),
    final_long = runif(100, -70, -40),
    ano = rep(2022, 100),
    nr_zona = sample(1:10, 100, replace = TRUE),
    nr_locvot = sample(100:200, 100, replace = TRUE)
  )
  
  # Test valid output
  result <- validate_output_stage(
    output_data = output_data,
    stage_name = "test_output",
    required_cols = c("local_id", "final_lat", "final_long"),
    unique_keys = c("local_id", "ano", "nr_zona", "nr_locvot")
  )
  
  expect_true(result$passed)
  expect_false(result$metadata$has_duplicates)
  
  # Test with missing required column
  missing_col_data <- output_data[, -"final_lat"]
  
  result_missing <- validate_output_stage(
    output_data = missing_col_data,
    stage_name = "test_output",
    required_cols = c("local_id", "final_lat", "final_long")
  )
  
  expect_false(result_missing$passed)
  
  # Test with duplicate keys
  dup_data <- rbind(output_data, output_data[1:10])
  
  result_dup <- validate_output_stage(
    output_data = dup_data,
    stage_name = "test_output",
    unique_keys = c("local_id", "ano", "nr_zona", "nr_locvot")
  )
  
  expect_false(result_dup$passed)
  expect_true(result_dup$metadata$has_duplicates)
  
  # Test with all NA column
  all_na_data <- data.table::copy(output_data)
  all_na_data[, bad_col := NA]
  
  result_all_na <- validate_output_stage(
    output_data = all_na_data,
    stage_name = "test_output"
  )
  
  # Should still pass because not all columns are NA
  expect_true(result_all_na$passed)
})

test_that("create_pipeline_validation_report works correctly", {
  source("../../R/validation_pipeline_stages.R")
  
  # Create sample validation results
  val_results <- list(
    import = list(
      passed = TRUE,
      metadata = list(
        stage = "import_data",
        type = "import",
        timestamp = Sys.time(),
        n_rows = 1000
      ),
      result = list()  # Mock result
    ),
    merge = list(
      passed = FALSE,
      metadata = list(
        stage = "merge_data",
        type = "merge",
        timestamp = Sys.time(),
        n_rows = 900,
        join_type = "left",
        n_left = 1000,
        n_right = 500
      ),
      result = structure(
        list(),
        class = c("validation", "list")
      )
    )
  )
  
  # Add class to results
  class(val_results$import) <- "validation_result"
  class(val_results$merge) <- "validation_result"
  
  # Mock the summary method for the validation result
  val_results$merge$result <- list()
  
  # Test report generation (capture output)
  output <- capture.output(
    create_pipeline_validation_report(val_results)
  )
  
  # Check report contains expected elements
  expect_true(any(grepl("PIPELINE VALIDATION REPORT", output)))
  expect_true(any(grepl("Total stages validated: 2", output)))
  expect_true(any(grepl("Passed: 1", output)))
  expect_true(any(grepl("Failed: 1", output)))
  
  # Test saving to file
  temp_file <- tempfile(fileext = ".txt")
  
  capture.output(
    create_pipeline_validation_report(val_results, output_file = temp_file)
  )
  
  expect_true(file.exists(temp_file))
  
  # Clean up
  unlink(temp_file)
})

test_that("run_pipeline_validation works correctly", {
  source("../../R/validation_pipeline_stages.R")
  
  # Create test data
  test_data <- data.table::data.table(
    id = 1:50,
    value = runif(50)
  )
  
  # Define pipeline stages
  stages <- list(
    list(
      name = "import_stage",
      type = "import",
      data = test_data,
      expected_cols = c("id", "value"),
      min_rows = 10
    ),
    list(
      name = "output_stage",
      type = "output",
      data = test_data,
      required_cols = c("id", "value"),
      unique_keys = "id"
    )
  )
  
  # Run validation
  results <- run_pipeline_validation(stages)
  
  expect_length(results, 2)
  expect_true("import_stage" %in% names(results))
  expect_true("output_stage" %in% names(results))
  expect_true(results$import_stage$passed)
  expect_true(results$output_stage$passed)
})

test_that("validate_rbindlist_stage works correctly", {
  source("../../R/validation_pipeline_stages.R")
  
  # Create test data chunks
  chunk1 <- data.table::data.table(
    id = 1:50,
    value = runif(50),
    category = "A"
  )
  
  chunk2 <- data.table::data.table(
    id = 51:100,
    value = runif(50),
    category = "B"
  )
  
  chunk3 <- data.table::data.table(
    id = 101:150,
    value = runif(50),
    category = "C"
  )
  
  # Test successful merge
  result <- validate_rbindlist_stage(
    list_of_results = list(chunk1, chunk2, chunk3),
    stage_name = "test_rbindlist",
    expected_cols = c("id", "value", "category")
  )
  
  expect_true(result$passed)
  expect_equal(nrow(result$merged_data), 150)
  expect_equal(result$metadata$n_chunks, 3)
  expect_equal(result$metadata$row_difference, 0)
  expect_true(result$additional_checks$no_row_loss)
  
  # Test with missing columns (fill=TRUE should handle this)
  chunk_missing <- data.table::data.table(
    id = 151:200,
    value = runif(50)
    # Missing 'category' column
  )
  
  result_fill <- validate_rbindlist_stage(
    list_of_results = list(chunk1, chunk_missing),
    stage_name = "test_rbindlist_fill",
    fill = TRUE
  )
  
  expect_true(result_fill$passed)
  expect_equal(nrow(result_fill$merged_data), 100)
  
  # Test with empty list
  expect_error(
    validate_rbindlist_stage(
      list_of_results = list(),
      stage_name = "empty_list"
    ),
    "No results to merge"
  )
  
  # Test with pre-merged data
  pre_merged <- data.table::rbindlist(list(chunk1, chunk2))
  
  result_premerged <- validate_rbindlist_stage(
    merged_data = pre_merged,
    stage_name = "test_premerged",
    expected_cols = c("id", "value")
  )
  
  expect_true(result_premerged$passed)
  expect_equal(nrow(result_premerged$merged_data), 100)
  
  # Test duplicate columns detection
  dup_chunk1 <- data.table::data.table(
    id = 1:10,
    value = 1:10,
    value = 11:20  # Duplicate column name
  )
  
  # This will create a warning but should still work
  result_dup <- validate_rbindlist_stage(
    list_of_results = list(dup_chunk1),
    stage_name = "test_duplicate_cols"
  )
  
  # Check metadata captured the issue
  expect_false(result_dup$additional_checks$no_duplicate_columns)
})

test_that("validation functions handle edge cases correctly", {
  source("../../R/validation_pipeline_stages.R")
  
  # Test with empty data
  empty_dt <- data.table::data.table()
  
  result_empty <- validate_import_stage(
    data = empty_dt,
    stage_name = "empty_test",
    min_rows = 0
  )
  
  expect_false(result_empty$passed)  # Should fail because no rows
  
  # Test with single row
  single_row <- data.table::data.table(id = 1, value = 10)
  
  result_single <- validate_import_stage(
    data = single_row,
    stage_name = "single_row_test",
    min_rows = 1
  )
  
  expect_true(result_single$passed)
  
  # Test string matching with no matches
  no_match_data <- data.table::data.table(
    local_id = 1:10,
    matched_name = paste0("NoMatch_", 1:10)
    # No coordinate columns
  )
  
  result_no_matches <- validate_string_match_stage(
    match_data = no_match_data,
    stage_name = "no_matches"
  )
  
  expect_false(result_no_matches$passed)  # Should fail - no coordinates
})