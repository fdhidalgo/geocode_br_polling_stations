test_that("filter_election_appropriate removes special districts from municipal years", {
  # Create test data
  test_data <- data.table(
    local_id = 1:10,
    ano = c(2008, 2008, 2010, 2010, 2012, 2012, 2008, 2010, 2012, 2014),
    cod_localidade_ibge = c(
      5300108, 1234567, 5300108, 1234567, 5300108,  # First 5
      1234567, 2605459, 2605459, 2605459, 2605459   # Last 5
    ),
    nm_localidade = c(
      "BRASILIA", "CITY A", "BRASILIA", "CITY A", "BRASILIA",
      "CITY A", "FERNANDO DE NORONHA", "FERNANDO DE NORONHA", 
      "FERNANDO DE NORONHA", "FERNANDO DE NORONHA"
    ),
    sg_uf = c("DF", "AC", "DF", "AC", "DF", "AC", "PE", "PE", "PE", "PE")
  )
  
  # Test filtering mode
  filtered <- filter_election_appropriate(test_data, keep_all = FALSE)
  
  # Should remove Brasília from 2008 and 2012 (municipal years)
  expect_false(any(filtered$cod_localidade_ibge == 5300108 & filtered$ano %in% c(2008, 2012)))
  # Should keep Brasília in 2010 (federal/state year)
  expect_true(any(filtered$cod_localidade_ibge == 5300108 & filtered$ano == 2010))
  
  # Should remove Fernando de Noronha from 2008 and 2012
  expect_false(any(filtered$cod_localidade_ibge == 2605459 & filtered$ano %in% c(2008, 2012)))
  # Should keep Fernando de Noronha in 2010 and 2014
  expect_true(any(filtered$cod_localidade_ibge == 2605459 & filtered$ano %in% c(2010, 2014)))
  
  # Regular municipalities should be kept in all years
  expect_equal(nrow(filtered[cod_localidade_ibge == 1234567]), 3)
})

test_that("filter_election_appropriate adds appropriate flags when keep_all = TRUE", {
  # Create test data
  test_data <- data.table(
    local_id = 1:6,
    ano = c(2008, 2008, 2010, 2010, 2012, 2012),
    cod_localidade_ibge = c(5300108, 1234567, 5300108, 1234567, 5300108, 1234567),
    nm_localidade = c("BRASILIA", "CITY A", "BRASILIA", "CITY A", "BRASILIA", "CITY A"),
    sg_uf = c("DF", "AC", "DF", "AC", "DF", "AC")
  )
  
  # Test flagging mode
  flagged <- filter_election_appropriate(test_data, keep_all = TRUE)
  
  # Check that all rows are preserved
  expect_equal(nrow(flagged), nrow(test_data))
  
  # Check election_appropriate flag
  expect_false(flagged[local_id == 1]$election_appropriate)  # Brasília 2008
  expect_true(flagged[local_id == 2]$election_appropriate)   # City A 2008
  expect_true(flagged[local_id == 3]$election_appropriate)   # Brasília 2010
  expect_true(flagged[local_id == 4]$election_appropriate)   # City A 2010
  expect_false(flagged[local_id == 5]$election_appropriate)  # Brasília 2012
  expect_true(flagged[local_id == 6]$election_appropriate)   # City A 2012
  
  # Check election_type column
  expect_equal(flagged[ano == 2008]$election_type[1], "Municipal")
  expect_equal(flagged[ano == 2010]$election_type[1], "Federal/State")
  expect_equal(flagged[ano == 2012]$election_type[1], "Municipal")
  
  # Check jurisdiction_type column
  expect_equal(flagged[cod_localidade_ibge == 5300108]$jurisdiction_type[1], "Federal District")
  expect_equal(flagged[cod_localidade_ibge == 1234567]$jurisdiction_type[1], "Municipality")
})

test_that("validate_election_appropriateness detects issues correctly", {
  # Test data with issues
  test_data_with_issues <- data.table(
    local_id = 1:4,
    ano = c(2008, 2008, 2010, 2012),
    cod_localidade_ibge = c(5300108, 1234567, 5300108, 2605459),
    nm_localidade = c("BRASILIA", "CITY A", "BRASILIA", "FERNANDO DE NORONHA"),
    sg_uf = c("DF", "AC", "DF", "PE")
  )
  
  result <- validate_election_appropriateness(test_data_with_issues)
  
  expect_false(result$passed)
  expect_equal(result$n_issues, 2)  # Brasília 2008 and Fernando de Noronha 2012
  expect_true(grepl("special districts", result$message))
  expect_equal(nrow(result$summary), 2)
  
  # Test data without issues
  test_data_clean <- data.table(
    local_id = 1:3,
    ano = c(2010, 2014, 2008),
    cod_localidade_ibge = c(5300108, 2605459, 1234567),
    nm_localidade = c("BRASILIA", "FERNANDO DE NORONHA", "CITY A"),
    sg_uf = c("DF", "PE", "AC")
  )
  
  result_clean <- validate_election_appropriateness(test_data_clean)
  
  expect_true(result_clean$passed)
  expect_equal(result_clean$n_issues, 0)
  expect_equal(nrow(result_clean$summary), 0)
})

test_that("filter_election_appropriate handles edge cases", {
  # Empty data
  empty_data <- data.table()
  expect_equal(nrow(filter_election_appropriate(empty_data)), 0)
  
  # Data with no special districts
  regular_data <- data.table(
    local_id = 1:5,
    ano = rep(2008, 5),
    cod_localidade_ibge = rep(1234567, 5),
    nm_localidade = rep("REGULAR CITY", 5),
    sg_uf = rep("SP", 5)
  )
  filtered_regular <- filter_election_appropriate(regular_data)
  expect_equal(nrow(filtered_regular), nrow(regular_data))
  
  # Data with only special districts in appropriate years
  appropriate_only <- data.table(
    local_id = 1:4,
    ano = c(2006, 2010, 2014, 2018),
    cod_localidade_ibge = rep(5300108, 4),
    nm_localidade = rep("BRASILIA", 4),
    sg_uf = rep("DF", 4)
  )
  filtered_appropriate <- filter_election_appropriate(appropriate_only)
  expect_equal(nrow(filtered_appropriate), nrow(appropriate_only))
})