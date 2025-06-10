# Test Pipeline Integration for 2024 Municipality Code Fix
# Author: Pipeline Integration Testing
# Date: 2025-01-10

library(testthat)
library(data.table)

# Source required functions
project_root <- rprojroot::find_root(rprojroot::is_rstudio_project)

# Change to project root for relative paths to work
old_wd <- getwd()
setwd(project_root)

# Source with error handling
tryCatch({
  source("R/data_table_utils.R")
  source("R/data_cleaning_fns.R")
  source("R/fix_municipality_codes_2024.R")
}, error = function(e) {
  setwd(old_wd)
  stop(e)
})

# Restore working directory
setwd(old_wd)

test_that("import_locais correctly fixes 2024 municipality codes", {
  
  # Load muni_ids
  muni_ids <- fread(file.path(project_root, "data", "muni_identifiers.csv"), encoding = "UTF-8")
  
  # Create test data mixing years
  test_data <- data.table(
    ANO = c(2022, 2022, 2024, 2024),
    CD_LOCALIDADE_TSE = c(1570, 90670, 1570, 90670),  # Same codes, different years
    NR_ZONA = 1:4,
    NR_LOCVOT = 101:104,
    NR_CEP = rep("00000-000", 4),
    SG_UF = c("AC", "MT", "AC", "MT"),
    NM_LOCALIDADE = c("Assis Brasil", "Cuiabá", "Assis Brasil", "Cuiabá"),
    NM_LOCVOT = paste("Test Location", 1:4),
    DS_ENDERECO = paste("Street", 1:4),
    DS_BAIRRO = paste("District", 1:4)
  )
  
  # Create temporary file
  temp_file <- tempfile(fileext = ".csv")
  fwrite(test_data, temp_file)
  
  # Import with fix
  result <- import_locais(temp_file, muni_ids)
  
  # Tests
  expect_equal(nrow(result), 4)
  expect_true("cod_localidade_ibge" %in% names(result))
  
  # 2022 records should have proper IBGE codes from merge
  result_2022 <- result[ano == 2022]
  expect_equal(result_2022[sg_uf == "AC"]$cod_localidade_ibge, 1200054)
  expect_equal(result_2022[sg_uf == "MT"]$cod_localidade_ibge, 5103403)
  
  # 2024 records should ALSO have proper IBGE codes (after fix)
  result_2024 <- result[ano == 2024]
  expect_equal(result_2024[sg_uf == "AC"]$cod_localidade_ibge, 1200054)
  expect_equal(result_2024[sg_uf == "MT"]$cod_localidade_ibge, 5103403)
  
  # All MT codes should be in correct range
  mt_codes <- result[sg_uf == "MT"]$cod_localidade_ibge
  expect_true(all(mt_codes >= 5100000 & mt_codes <= 5199999))
  
  # Clean up
  unlink(temp_file)
})

test_that("2024 fix preserves data integrity in pipeline", {
  
  # Load muni_ids
  muni_ids <- fread(file.path(project_root, "data", "muni_identifiers.csv"), encoding = "UTF-8")
  
  # Create test data with only 2024 records
  test_data <- data.table(
    ANO = rep(2024, 5),
    CD_LOCALIDADE_TSE = c(90670, 91677, 89850, 99999, NA),  # Mix of valid and invalid
    NR_ZONA = 1:5,
    NR_LOCVOT = 201:205,
    NR_CEP = rep("78000-000", 5),
    SG_UF = rep("MT", 5),
    NM_LOCALIDADE = c("Cuiabá", "Várzea Grande", "Sinop", "Invalid", "Missing"),
    NM_LOCVOT = paste("Location", 1:5),
    DS_ENDERECO = paste("Address", 1:5),
    DS_BAIRRO = paste("Bairro", 1:5)
  )
  
  # Create temporary file
  temp_file <- tempfile(fileext = ".csv")
  fwrite(test_data, temp_file)
  
  # Import with fix
  result <- suppressWarnings(import_locais(temp_file, muni_ids))
  
  # Tests
  expect_equal(nrow(result), 5)  # All records preserved
  
  # Check that valid codes were converted
  expect_equal(result[nm_localidade == "Cuiabá"]$cod_localidade_ibge, 5103403)
  expect_equal(result[nm_localidade == "Várzea Grande"]$cod_localidade_ibge, 5108402)
  expect_equal(result[nm_localidade == "Sinop"]$cod_localidade_ibge, 5107909)
  
  # Invalid and NA codes should remain unchanged
  expect_true(is.na(result[nm_localidade == "Invalid"]$cod_localidade_ibge))
  expect_true(is.na(result[nm_localidade == "Missing"]$cod_localidade_ibge))
  
  # Original TSE codes should be preserved
  if ("cd_localidade_tse_original" %in% names(result)) {
    expect_equal(result$cd_localidade_tse_original, test_data$CD_LOCALIDADE_TSE)
  }
  
  # Clean up
  unlink(temp_file)
})

test_that("pipeline handles mixed year data correctly", {
  
  # Load muni_ids
  muni_ids <- fread(file.path(project_root, "data", "muni_identifiers.csv"), encoding = "UTF-8")
  
  # Create realistic mixed-year test data
  test_data <- data.table(
    ANO = c(2018, 2020, 2022, 2024, 2024),
    CD_LOCALIDADE_TSE = c(1200054, 5103403, 1570, 90670, 91677),  # Mix of IBGE and TSE
    NR_ZONA = 1:5,
    NR_LOCVOT = 301:305,
    NR_CEP = c("12000-000", "78000-000", "12000-000", "78000-000", "78100-000"),
    SG_UF = c("AC", "MT", "AC", "MT", "MT"),
    NM_LOCALIDADE = c("Assis Brasil", "Cuiabá", "Assis Brasil", "Cuiabá", "Várzea Grande"),
    NM_LOCVOT = paste("Station", 1:5),
    DS_ENDERECO = paste("Rua", 1:5),
    DS_BAIRRO = paste("Centro", 1:5)
  )
  
  # Create temporary file
  temp_file <- tempfile(fileext = ".csv")
  fwrite(test_data, temp_file)
  
  # Import with fix
  result <- import_locais(temp_file, muni_ids)
  
  # Tests
  expect_equal(nrow(result), 5)
  
  # All records should have valid IBGE codes
  valid_ibge <- result$cod_localidade_ibge >= 1000000 & 
                result$cod_localidade_ibge <= 9999999
  expect_true(all(valid_ibge[!is.na(result$cod_localidade_ibge)]))
  
  # 2024 MT records should be in correct range
  mt_2024 <- result[ano == 2024 & sg_uf == "MT"]
  expect_true(all(mt_2024$cod_localidade_ibge >= 5100000 & 
                  mt_2024$cod_localidade_ibge <= 5199999))
  
  # Clean up
  unlink(temp_file)
})