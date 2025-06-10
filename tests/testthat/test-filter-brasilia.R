# Load required libraries and source files
library(data.table)
source("../../R/filter_brasilia_municipal.R")

test_that("filter_brasilia_municipal_elections removes correct records", {
  # Create test data with DF records in various years
  test_dt <- data.table(
    sg_uf = c("DF", "DF", "DF", "DF", "SP", "RJ", "DF"),
    ano = c(2008, 2014, 2020, 2022, 2020, 2008, 2012),
    cod_localidade_ibge = c(5300108, 5300108, 5300108, 5300108, 3550308, 3304557, 5300108),
    local_id = 1:7,
    nm_localidade = c(rep("BRASÍLIA", 4), "SÃO PAULO", "RIO DE JANEIRO", "BRASÍLIA")
  )
  
  result <- filter_brasilia_municipal_elections(test_dt)
  
  # Should remove 2008, 2020, and 2012 DF records (3 records)
  expect_equal(nrow(result), 4)
  
  # Check that no DF records exist in municipal years
  expect_false(any(result$sg_uf == "DF" & result$ano %in% c(2008, 2012, 2020)))
  
  # Check that DF records in federal/state years are kept
  expect_true(any(result$sg_uf == "DF" & result$ano == 2014))
  expect_true(any(result$sg_uf == "DF" & result$ano == 2022))
  
  # Check that non-DF records are not affected
  expect_equal(sum(result$sg_uf %in% c("SP", "RJ")), 2)
})

test_that("filter_brasilia_municipal_elections handles different column names", {
  # Test with alternative column names (uf instead of sg_uf, ano_eleicao instead of ano)
  test_dt <- data.table(
    uf = c("DF", "DF", "SP"),
    ano_eleicao = c(2008, 2014, 2008),
    local_id = 1:3
  )
  
  result <- filter_brasilia_municipal_elections(test_dt)
  
  # Should remove the 2008 DF record
  expect_equal(nrow(result), 2)
  expect_false(any(result$uf == "DF" & result$ano_eleicao == 2008))
  expect_true(any(result$uf == "DF" & result$ano_eleicao == 2014))
})

test_that("filter_brasilia_municipal_elections preserves all columns", {
  test_dt <- data.table(
    sg_uf = c("DF", "SP"),
    ano = c(2008, 2008),
    local_id = 1:2,
    extra_col1 = c("A", "B"),
    extra_col2 = c(10, 20)
  )
  
  result <- filter_brasilia_municipal_elections(test_dt)
  
  # Check that all columns are preserved
  expect_equal(names(result), names(test_dt))
  
  # Check that SP record is unchanged
  sp_record <- result[sg_uf == "SP"]
  expect_equal(sp_record$extra_col1, "B")
  expect_equal(sp_record$extra_col2, 20)
})

test_that("filter_brasilia_municipal_elections handles empty data", {
  empty_dt <- data.table(sg_uf = character(), ano = numeric(), local_id = character())
  result <- filter_brasilia_municipal_elections(empty_dt)
  expect_equal(nrow(result), 0)
})

test_that("filter_brasilia_municipal_elections handles data without DF records", {
  test_dt <- data.table(
    sg_uf = c("SP", "RJ", "MG"),
    ano = c(2008, 2012, 2016),
    local_id = 1:3
  )
  
  result <- filter_brasilia_municipal_elections(test_dt)
  
  # Should return unchanged data
  expect_equal(nrow(result), 3)
  expect_equal(result, test_dt)
})

test_that("summarize_brasilia_records provides correct summary", {
  test_dt <- data.table(
    sg_uf = c("DF", "DF", "DF", "DF", "SP", "DF"),
    ano = c(2008, 2008, 2014, 2020, 2020, 2022),
    local_id = c("1", "2", "3", "4", "5", "6")
  )
  
  summary <- summarize_brasilia_records(test_dt)
  
  # Check structure
  expect_true("year" %in% names(summary))
  expect_true("n_records" %in% names(summary))
  expect_true("election_type" %in% names(summary))
  
  # Check counts
  expect_equal(summary[year == 2008]$n_records, 2)
  expect_equal(summary[year == 2008]$election_type, "Municipal")
  expect_equal(summary[year == 2014]$election_type, "Federal/State")
  expect_equal(summary[year == 2022]$election_type, "Federal/State")
  
  # Check total row
  total_row <- summary[year == "TOTAL"]
  expect_equal(total_row$n_records, 5)  # Total DF records
})

test_that("filter removes all municipal year records for DF", {
  # Comprehensive test with all years
  years <- 2006:2024
  test_dt <- data.table(
    sg_uf = rep("DF", length(years)),
    ano = years,
    local_id = seq_along(years)
  )
  
  result <- filter_brasilia_municipal_elections(test_dt)
  
  # Check that only federal/state years remain
  expect_equal(sort(unique(result$ano)), c(2006, 2007, 2009, 2010, 2011, 
                                           2013, 2014, 2015, 2017, 2018, 
                                           2019, 2021, 2022, 2023))
  
  # Verify municipal years are removed
  municipal_years <- c(2008, 2012, 2016, 2020, 2024)
  expect_false(any(result$ano %in% municipal_years))
})