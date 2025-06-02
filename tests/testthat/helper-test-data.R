# Helper functions and test data for testthat tests

# Load required libraries for tests
library(data.table)
library(stringr)
library(stringi)

# Create sample polling station data for testing
create_test_polling_data <- function(n = 10, year = 2022) {
  data.table(
    ano = year,
    local_id = seq_len(n),
    cod_muni = 12345,
    nome_local = paste0("Escola Municipal ", LETTERS[seq_len(n)]),
    endereco = paste0("Rua ", seq_len(n), ", nº ", seq_len(n) * 100),
    normalized_name = paste0("escola municipal ", tolower(LETTERS[seq_len(n)])),
    normalized_addr = paste0("rua ", seq_len(n), " n ", seq_len(n) * 100)
  )
}

# Create sample INEP school data for testing
create_test_inep_data <- function(n = 5) {
  data.frame(
    norm_school = paste0("escola municipal ", tolower(LETTERS[seq_len(n)])),
    norm_addr = paste0("rua ", seq_len(n), " n ", seq_len(n) * 100),
    longitude = -45 - runif(n),
    latitude = -23 - runif(n),
    stringsAsFactors = FALSE
  )
}

# Create sample CNEFE data for testing
create_test_cnefe_data <- function(n = 20) {
  data.table(
    cod_muni = 12345,
    nome_estab = c(
      paste0("Escola ", LETTERS[seq_len(n/2)]),
      paste0("Estabelecimento ", seq_len(n/2))
    ),
    logradouro = paste0("Rua ", seq_len(n)),
    numero = seq_len(n) * 10,
    longitude = -45 - runif(n),
    latitude = -23 - runif(n)
  )
}

# Helper to check if a package is installed
skip_if_package_not_installed <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    skip(paste("Package", pkg, "not installed"))
  }
}