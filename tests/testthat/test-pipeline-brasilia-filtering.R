# Test Brasília filtering in pipeline context
library(data.table)
source("../../R/filter_brasilia_municipal.R")
source("../../R/validate_brasilia_filtering.R")

test_that("Brasília filtering integrates correctly with pipeline", {
  # Simulate pipeline data structure
  test_locais <- data.table(
    local_id = paste0("2008_DF_", 1:5),
    ano = 2008,
    nr_zona = 1:5,
    nr_locvot = 1001:1005,
    nm_locvot = paste("LOCAL", 1:5),
    nm_localidade = "BRASÍLIA",
    sg_uf = "DF",
    cod_localidade_ibge = 5300108,
    ds_endereco = paste("ENDEREÇO", 1:5)
  )
  
  # Add some non-DF data
  test_locais <- rbind(
    test_locais,
    data.table(
      local_id = paste0("2008_SP_", 1:3),
      ano = 2008,
      nr_zona = 1:3,
      nr_locvot = 2001:2003,
      nm_locvot = paste("LOCAL SP", 1:3),
      nm_localidade = "SÃO PAULO",
      sg_uf = "SP",
      cod_localidade_ibge = 3550308,
      ds_endereco = paste("ENDEREÇO SP", 1:3)
    )
  )
  
  # Add DF data from federal year
  test_locais <- rbind(
    test_locais,
    data.table(
      local_id = paste0("2022_DF_", 1:3),
      ano = 2022,
      nr_zona = 1:3,
      nr_locvot = 3001:3003,
      nm_locvot = paste("LOCAL", 1:3),
      nm_localidade = "BRASÍLIA",
      sg_uf = "DF",
      cod_localidade_ibge = 5300108,
      ds_endereco = paste("ENDEREÇO", 1:3)
    )
  )
  
  # Apply filtering
  filtered <- filter_brasilia_municipal_elections(test_locais)
  
  # Validate results
  expect_equal(nrow(filtered), 6) # 3 SP + 3 DF from 2022
  expect_equal(sum(filtered$sg_uf == "DF" & filtered$ano == 2008), 0)
  expect_equal(sum(filtered$sg_uf == "DF" & filtered$ano == 2022), 3)
  expect_equal(sum(filtered$sg_uf == "SP"), 3)
})

test_that("Pipeline validation accepts filtered data", {
  # Create data that would pass pipeline validation
  test_data <- data.table(
    local_id = paste0("2014_", c("SP", "RJ", "DF"), "_1"),
    ano = 2014,
    nr_zona = 1:3,
    nr_locvot = 1:3,
    nm_locvot = c("LOCAL SP", "LOCAL RJ", "LOCAL DF"),
    nm_localidade = c("SÃO PAULO", "RIO DE JANEIRO", "BRASÍLIA"),
    sg_uf = c("SP", "RJ", "DF"),
    cod_localidade_ibge = c(3550308, 3304557, 5300108),
    ds_endereco = paste("ENDEREÇO", 1:3)
  )
  
  # This should pass validation (DF in federal year)
  validation <- validate_brasilia_filtering(test_data, stop_on_failure = FALSE)
  expect_true(validation$passed)
})

test_that("Report generation works in pipeline context", {
  # Create before/after data
  before_data <- data.table(
    local_id = c("2008_DF_1", "2014_DF_1", "2008_SP_1"),
    ano = c(2008, 2014, 2008),
    nr_zona = 1:3,
    nr_locvot = 1:3,
    nm_locvot = paste("LOCAL", 1:3),
    nm_localidade = c("BRASÍLIA", "BRASÍLIA", "SÃO PAULO"),
    sg_uf = c("DF", "DF", "SP"),
    cod_localidade_ibge = c(5300108, 5300108, 3550308),
    ds_endereco = paste("ENDEREÇO", 1:3)
  )
  
  after_data <- filter_brasilia_municipal_elections(before_data)
  
  # Generate report
  temp_file <- tempfile(fileext = ".rds")
  report <- generate_brasilia_filtering_report(before_data, after_data, temp_file)
  
  # Check report contents
  expect_equal(report$summary$df_records_removed, 1)
  expect_equal(report$summary$total_records_after, 2)
  expect_true(file.exists(temp_file))
  
  # Cleanup
  unlink(temp_file)
})

test_that("Filtering preserves all required columns", {
  # Create data with all expected columns
  test_data <- data.table(
    local_id = "2008_DF_1",
    ano = 2008,
    nr_zona = 1,
    nr_locvot = 1001,
    nm_locvot = "ESCOLA TESTE",
    nm_localidade = "BRASÍLIA",
    sg_uf = "DF",
    cod_localidade_ibge = 5300108,
    ds_endereco = "RUA TESTE, 123",
    # Additional columns that might exist
    lat = -15.7801,
    long = -47.9292,
    extra_col = "test"
  )
  
  filtered <- filter_brasilia_municipal_elections(test_data)
  
  # All columns should be preserved (even though row is removed)
  expect_equal(names(test_data), names(filtered))
})