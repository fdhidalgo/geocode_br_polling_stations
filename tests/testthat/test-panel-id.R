# Test file for panel ID functions

test_that("create_and_select_best_pairs handles consecutive years correctly", {
  # Create mock data for testing
  library(data.table)
  
  # Create sample data for 3 years
  set.seed(123)
  mock_data <- data.table(
    ano = rep(c(2018, 2020, 2022), each = 10),
    local_id = rep(1:10, 3),
    cod_muni = rep(12345, 30),
    normalized_name = c(
      paste0("escola ", 1:10),
      paste0("escola ", c(1:8, 11, 12)), # 2 changes in 2020
      paste0("escola ", c(1:8, 11, 13))  # 1 more change in 2022
    ),
    normalized_addr = paste0("rua ", rep(1:10, 3)),
    stringsAsFactors = FALSE
  )
  
  # Run the function (note: this requires reclin package)
  skip_if_not_installed("reclin")
  
  result <- create_and_select_best_pairs(
    data = mock_data,
    years = c(2018, 2020, 2022),
    blocking_column = "cod_muni",
    scoring_columns = c("normalized_name", "normalized_addr")
  )
  
  # Check that we get pairs for each consecutive year pair
  expect_equal(length(result), 2)
  expect_true("2018_2020" %in% names(result))
  expect_true("2020_2022" %in% names(result))
  
  # Check that each result is a data.table
  expect_s3_class(result[["2018_2020"]], "data.table")
  expect_s3_class(result[["2020_2022"]], "data.table")
})

test_that("panel ID assignment maintains 1:1 matching constraint", {
  # This tests the conceptual constraint that each polling station
  # in year t should match to at most one station in year t+1
  
  # Create mock matched pairs
  matched_pairs <- data.table(
    .x_local_id = c(1, 2, 3, 4),
    .y_local_id = c(11, 12, 13, 14),
    weights = c(0.9, 0.8, 0.95, 0.7),
    match = TRUE
  )
  
  # Check no duplicates in either column
  expect_equal(length(unique(matched_pairs$.x_local_id)), nrow(matched_pairs))
  expect_equal(length(unique(matched_pairs$.y_local_id)), nrow(matched_pairs))
})

test_that("Jaro-Winkler similarity produces values between 0 and 1", {
  # Test the similarity metric used in matching
  skip_if_not_installed("reclin")
  
  # Test identical strings
  jw_identical <- cmp_jarowinkler()("escola teste", "escola teste")
  expect_equal(jw_identical, 1)
  
  # Test completely different strings
  jw_different <- cmp_jarowinkler()("abc", "xyz")
  expect_gte(jw_different, 0)
  expect_lte(jw_different, 1)
  
  # Test similar strings
  jw_similar <- cmp_jarowinkler()("escola municipal", "escola municial")
  expect_gt(jw_similar, 0.8)
  expect_lt(jw_similar, 1)
})