# Test file for string matching functions

test_that("match_inep_muni returns NULL for empty inep data", {
  # Create mock data
  locais_muni <- data.frame(
    normalized_name = "escola teste",
    normalized_addr = "rua teste 123",
    stringsAsFactors = FALSE
  )
  
  inep_muni <- data.frame()
  
  result <- match_inep_muni(locais_muni, inep_muni)
  expect_null(result)
})

test_that("match_inep_muni finds best matches by name and address", {
  # Create mock data with known matches
  locais_muni <- data.frame(
    normalized_name = c("jose silva", "maria santos"),
    normalized_addr = c("rua a 123", "avenida b 456"),
    stringsAsFactors = FALSE
  )
  
  inep_muni <- data.frame(
    norm_school = c("jose silva", "maria santos", "pedro oliveira"),
    norm_addr = c("rua a 123", "avenida b 456", "praca c 789"),
    longitude = c(-45.1, -45.2, -45.3),
    latitude = c(-23.1, -23.2, -23.3),
    stringsAsFactors = FALSE
  )
  
  result <- match_inep_muni(locais_muni, inep_muni)
  
  # Check that result is not NULL
  expect_false(is.null(result))
  
  # Check that result has expected structure
  expect_true("mindist_name_inep" %in% names(result))
  expect_true("match_inep_name" %in% names(result))
  expect_true("match_long_inep_name" %in% names(result))
  expect_true("match_lat_inep_name" %in% names(result))
  
  # Check that exact matches have distance 0
  expect_equal(result$mindist_name_inep[1], 0)
  expect_equal(result$mindist_name_inep[2], 0)
})

test_that("string distance normalization works correctly", {
  # Test that normalized Levenshtein distance is between 0 and 1
  short_string <- "abc"
  long_string <- "abcdefghijklmnop"
  similar_string <- "abd"
  
  # Calculate normalized distance manually
  dist_raw <- stringdist::stringdist(short_string, long_string, method = "lv")
  dist_normalized <- dist_raw / max(nchar(short_string), nchar(long_string))
  
  expect_gte(dist_normalized, 0)
  expect_lte(dist_normalized, 1)
  
  # Test similar strings have low normalized distance
  dist_similar <- stringdist::stringdist(short_string, similar_string, method = "lv")
  dist_similar_norm <- dist_similar / max(nchar(short_string), nchar(similar_string))
  
  expect_lt(dist_similar_norm, 0.5)
})