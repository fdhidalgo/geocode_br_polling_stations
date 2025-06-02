# Example: Using Join Validation in Practice
#
# This file demonstrates how to use the join validation framework
# to prevent merge mistakes in the geocoding pipeline.

library(data.table)
source("R/validation_join_operations.R")
source("R/validation_pipeline_merges.R")

# Example 1: Basic join validation
example_basic_validation <- function() {
  # Create sample data
  locais <- data.table(
    local_id = c(1, 2, 3, 4, 5),
    nm_local = c("School A", "School B", "School C", "School D", "School E"),
    municipio = c("São Paulo", "Rio", "São Paulo", "Rio", "Brasília")
  )
  
  # Coordinates with a duplicate key (local_id = 2 appears twice)
  coords <- data.table(
    local_id = c(1, 2, 2, 3, 5, 6),  # Note: 2 is duplicated, 4 is missing, 6 is extra
    latitude = c(-23.5, -22.9, -22.8, -23.6, -15.8, -20.1),
    longitude = c(-46.6, -43.2, -43.1, -46.7, -47.9, -45.0)
  )
  
  cat("Example 1: Basic Join Validation\n")
  cat("================================\n\n")
  
  # This will detect the duplicate key issue
  validation <- validate_join_keys(locais, coords, keys = "local_id", join_type = "one-to-one")
  print(validation)
  
  # Attempting merge with validation
  cat("\nAttempting merge with validation...\n")
  tryCatch({
    result <- safe_merge(locais, coords, keys = "local_id", 
                        join_type = "one-to-one", 
                        stop_on_error = TRUE)
  }, error = function(e) {
    cat("Error caught:", e$message, "\n")
  })
}

# Example 2: Many-to-one join (common in CNEFE merges)
example_many_to_one <- function() {
  # CNEFE data (many records per municipality)
  cnefe <- data.table(
    id_munic_7 = c(3550308, 3550308, 3550308, 3304557, 3304557),
    endereco = c("Rua A", "Rua B", "Rua C", "Av X", "Av Y"),
    cep = c("01000-000", "01001-000", "01002-000", "20000-000", "20001-000")
  )
  
  # Municipality data (one record per municipality)
  muni_ids <- data.table(
    id_munic_7 = c(3550308, 3304557, 1234567),  # Note: 1234567 won't match
    municipio = c("São Paulo", "Rio de Janeiro", "Other City"),
    estado_abrev = c("SP", "RJ", "XX")
  )
  
  cat("\n\nExample 2: Many-to-One Join (CNEFE with Municipalities)\n")
  cat("=====================================================\n\n")
  
  # Validate before merge
  validation <- validate_join_keys(cnefe, muni_ids, keys = "id_munic_7", join_type = "many-to-one")
  print(validation)
  
  # Perform merge with validation
  result <- safe_merge(cnefe, muni_ids, keys = "id_munic_7", 
                      join_type = "many-to-one",
                      merge_type = "left")
  
  cat("\nMerged result:\n")
  print(result)
  
  cat("\nValidation report summary:\n")
  get_merge_validation_report(result)
}

# Example 3: Composite key validation
example_composite_keys <- function() {
  # Polling station data with composite keys
  locais_2020 <- data.table(
    ano = 2020,
    cod_localidade_ibge = c(3550308, 3550308, 3304557),
    nr_zona = c(1, 1, 10),
    nr_locvot = c(1001, 1002, 2001),
    nm_local = c("School A", "School B", "School X")
  )
  
  # Address data with missing values in keys
  addresses <- data.table(
    ano = c(2020, 2020, 2020, 2020),
    cod_localidade_ibge = c(3550308, 3550308, 3304557, NA),  # One NA
    nr_zona = c(1, 1, 10, 5),
    nr_locvot = c(1001, 1002, 2001, 3001),
    endereco = c("Rua A, 100", "Rua B, 200", "Av X, 500", "Unknown")
  )
  
  cat("\n\nExample 3: Composite Key Validation\n")
  cat("==================================\n\n")
  
  keys <- c("ano", "cod_localidade_ibge", "nr_zona", "nr_locvot")
  
  # This will detect the NA in composite keys
  validation <- validate_join_keys(locais_2020, addresses, keys = keys, 
                                  join_type = "one-to-one", 
                                  allow_missing = FALSE)
  print(validation)
}

# Example 4: Using pipeline-specific validation wrappers
example_pipeline_validation <- function() {
  # Simulate TSE geocoded data
  tse_geocoded <- data.table(
    cd_municipio = c(3550308, 3550308, 3304557, 9999999),  # 9999999 won't match
    local_id = 1:4,
    latitude = c(-23.5, -23.6, -22.9, -20.0),
    longitude = c(-46.6, -46.7, -43.2, -45.0)
  )
  
  # Municipality identifiers
  muni_ids <- data.table(
    id_TSE = c(3550308, 3304557),
    id_munic_7 = c(3550308, 3304557),
    municipio = c("São Paulo", "Rio de Janeiro"),
    estado_abrev = c("SP", "RJ")
  )
  
  cat("\n\nExample 4: Pipeline-Specific Validation\n")
  cat("======================================\n\n")
  
  # Use the pipeline-specific wrapper
  result <- validate_merge_tse_muni(tse_geocoded, muni_ids, stop_on_error = FALSE)
  
  cat("\nValidation automatically handled by wrapper\n")
}

# Run examples
if (interactive()) {
  example_basic_validation()
  example_many_to_one()
  example_composite_keys()
  example_pipeline_validation()
  
  cat("\n\n✅ All examples completed. Key takeaways:\n")
  cat("1. Always validate join keys before merging\n")
  cat("2. Specify correct join type (one-to-one, one-to-many, many-to-one)\n")
  cat("3. Check for missing values in composite keys\n")
  cat("4. Use pipeline-specific wrappers for common operations\n")
  cat("5. Review validation reports to catch issues early\n")
}