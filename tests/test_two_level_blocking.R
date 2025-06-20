# Test script for two-level blocking optimization
# This script tests that the new blocking approach produces identical results
# while reducing the number of comparisons

library(targets)
library(data.table)
library(reclin2)

# Source necessary functions
source("R/data_cleaning_fns.R")
source("R/panel_id_fns.R") 
source("R/panel_id_municipality_fns.R")
source("R/panel_id_blocking_fns.R")

cat("Loading test data...\n")

# Load a small subset of data for testing
tar_load(locais_filtered)
tar_load(pipeline_config)

# Select a test municipality with reasonable size
test_municipalities <- locais_filtered[, .N, by = cod_localidade_ibge][N > 100 & N < 500]
test_muni <- test_municipalities[1, cod_localidade_ibge]

cat("Testing with municipality:", test_muni, 
    "(", test_municipalities[1, N], "stations)\n\n")

# Extract test data
test_data <- locais_filtered[cod_localidade_ibge == test_muni]

# Test parameters
years <- c(2006, 2008, 2010, 2012, 2014, 2016, 2018, 2020, 2022)
blocking_column <- "cod_localidade_ibge"
scoring_columns <- c("normalized_name", "normalized_addr")

# Run test with word blocking DISABLED (baseline)
cat("Running baseline (no word blocking)...\n")
start_time <- Sys.time()
baseline_result <- make_panel_1block(
  block = copy(test_data),
  years = years,
  blocking_column = blocking_column,
  scoring_columns = scoring_columns,
  use_word_blocking = FALSE
)
baseline_time <- as.numeric(Sys.time() - start_time, units = "secs")

# Run test with word blocking ENABLED
cat("\nRunning with two-level blocking...\n")
start_time <- Sys.time()
blocked_result <- make_panel_1block(
  block = copy(test_data),
  years = years,
  blocking_column = blocking_column,
  scoring_columns = scoring_columns,
  use_word_blocking = TRUE
)
blocked_time <- as.numeric(Sys.time() - start_time, units = "secs")

# Compare results
cat("\n=== RESULTS COMPARISON ===\n")
cat("Baseline time:", round(baseline_time, 2), "seconds\n")
cat("Blocked time:", round(blocked_time, 2), "seconds\n")
cat("Speedup:", round(baseline_time / blocked_time, 1), "x\n\n")

# Check if results are identical
if (is.null(baseline_result) && is.null(blocked_result)) {
  cat("Both methods returned NULL (no results)\n")
} else if (is.null(baseline_result) || is.null(blocked_result)) {
  cat("ERROR: One method returned NULL while the other didn't!\n")
} else {
  # Sort both results for comparison
  setkey(baseline_result, local_id, panel_id)
  setkey(blocked_result, local_id, panel_id)
  
  if (identical(baseline_result, blocked_result)) {
    cat("SUCCESS: Results are identical!\n")
    cat("Number of panel IDs:", nrow(baseline_result), "\n")
  } else {
    cat("WARNING: Results differ!\n")
    cat("Baseline rows:", nrow(baseline_result), "\n")
    cat("Blocked rows:", nrow(blocked_result), "\n")
    
    # Find differences
    only_baseline <- baseline_result[!blocked_result, on = c("local_id", "panel_id")]
    only_blocked <- blocked_result[!baseline_result, on = c("local_id", "panel_id")]
    
    if (nrow(only_baseline) > 0) {
      cat("\nRows only in baseline:", nrow(only_baseline), "\n")
      print(head(only_baseline))
    }
    
    if (nrow(only_blocked) > 0) {
      cat("\nRows only in blocked:", nrow(only_blocked), "\n")
      print(head(only_blocked))
    }
  }
}

# Test word extraction function
cat("\n=== WORD EXTRACTION TEST ===\n")
test_texts <- c(
  "ESCOLA MUNICIPAL SANTOS DUMONT",
  "E M PROF MARIA DA SILVA",
  "RUA PRINCIPAL 123 CENTRO",
  "AVENIDA DOS ESTADOS UNIDOS 456"
)

for (text in test_texts) {
  words <- extract_significant_words(text)[[1]]
  cat(text, "->", paste(words, collapse = ", "), "\n")
}

cat("\nTest completed.\n")