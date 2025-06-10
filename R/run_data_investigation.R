#!/usr/bin/env Rscript
# Run Data Quality Investigation
# Purpose: Execute comprehensive investigation of data quality issues

library(targets)
library(data.table)

# Source investigation functions
source("R/data_quality_investigation.R")
source("R/sanity_check_validation.R")

cat("\n=== STARTING DATA QUALITY INVESTIGATION ===\n")
cat("Date:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

# Load the data from targets
cat("Loading data from targets pipeline...\n")
tar_load(c(geocoded_locais, panel_ids), store = "_targets")

# Basic data overview
cat("\n=== DATA OVERVIEW ===\n")
cat(sprintf("- Total records: %s\n", format(nrow(geocoded_locais), big.mark = ",")))
cat(sprintf("- Years covered: %s\n", paste(sort(unique(geocoded_locais$ano)), collapse = ", ")))
cat(sprintf("- States covered: %d\n", uniqueN(geocoded_locais$sg_uf)))
cat(sprintf("- Panel IDs assigned: %s\n", format(nrow(panel_ids), big.mark = ",")))

# 1. Validate sanity check calculations
cat("\n\n=== VALIDATING SANITY CHECK CALCULATIONS ===\n")
validation_results <- validate_sanity_checks(geocoded_locais, panel_ids)

# 2. Run comprehensive investigation
cat("\n\n=== RUNNING COMPREHENSIVE INVESTIGATION ===\n")
investigation_results <- investigate_data_quality(geocoded_locais, panel_ids, verbose = TRUE)

# 3. Generate corrected statistics
cat("\n\n=== CORRECTED SUMMARY STATISTICS ===\n")
corrected_stats <- generate_corrected_stats(geocoded_locais, panel_ids)
print(corrected_stats)

# 4. Deep dive into specific issues
cat("\n\n=== DEEP DIVE: MUNICIPALITY CODE ANALYSIS ===\n")

# Check for invalid municipality codes
invalid_codes <- geocoded_locais[, .(
  n_records = .N,
  years = paste(sort(unique(ano)), collapse = ","),
  example_name = nm_localidade[1]
), by = cod_localidade_ibge][
  nchar(as.character(cod_localidade_ibge)) != 7 | 
  !grepl("^[0-9]+$", as.character(cod_localidade_ibge))
]

if (nrow(invalid_codes) > 0) {
  cat("\nInvalid municipality codes found:\n")
  print(invalid_codes)
} else {
  cat("\nAll municipality codes appear valid (7 digits).\n")
}

# Check municipality code ranges by state
cat("\n\nMunicipality code ranges by state:\n")
code_ranges <- geocoded_locais[, .(
  min_code = min(cod_localidade_ibge),
  max_code = max(cod_localidade_ibge),
  n_unique = uniqueN(cod_localidade_ibge)
), by = sg_uf][order(sg_uf)]
print(code_ranges)

# 5. Analyze local_id structure
cat("\n\n=== DEEP DIVE: LOCAL_ID STRUCTURE ===\n")

# Sample local_ids
sample_ids <- unique(geocoded_locais$local_id)[1:20]
cat("\nSample local_ids:\n")
print(sample_ids)

# Analyze ID patterns
id_patterns <- data.table(
  contains_year = sum(grepl("20[0-2][0-9]", geocoded_locais$local_id)),
  contains_underscore = sum(grepl("_", geocoded_locais$local_id)),
  contains_dash = sum(grepl("-", geocoded_locais$local_id)),
  avg_length = mean(nchar(geocoded_locais$local_id)),
  min_length = min(nchar(geocoded_locais$local_id)),
  max_length = max(nchar(geocoded_locais$local_id))
)
cat("\nLocal ID patterns:\n")
print(id_patterns)

# Check if IDs are unique within years
id_uniqueness <- geocoded_locais[, .(
  n_total = .N,
  n_unique_ids = uniqueN(local_id),
  duplicates = .N - uniqueN(local_id)
), by = ano]
cat("\nLocal ID uniqueness by year:\n")
print(id_uniqueness)

# 6. Save investigation results
cat("\n\n=== SAVING RESULTS ===\n")

# Create output directory if it doesn't exist
if (!dir.exists("output/investigation")) {
  dir.create("output/investigation", recursive = TRUE)
}

# Save detailed results
saveRDS(list(
  validation = validation_results,
  investigation = investigation_results,
  corrected_stats = corrected_stats,
  invalid_codes = invalid_codes,
  code_ranges = code_ranges,
  id_patterns = id_patterns,
  timestamp = Sys.time()
), file = "output/investigation/data_quality_investigation_results.rds")

cat("\nDetailed results saved to: output/investigation/data_quality_investigation_results.rds\n")

# Generate summary report
if (requireNamespace("rmarkdown", quietly = TRUE)) {
  cat("\nGenerating HTML report...\n")
  generate_investigation_report(investigation_results, 
                               output_file = "output/investigation/data_quality_report.html")
}

cat("\n=== INVESTIGATION COMPLETE ===\n")
cat("\nKey findings summary:\n")
cat(sprintf("1. Municipality count: %d (expected ~5,570)\n", 
            uniqueN(geocoded_locais$cod_localidade_ibge)))
cat(sprintf("2. Stations in all years: %d\n", 
            investigation_results$station_persistence$stations_all_years))
cat(sprintf("3. Panel coverage: %.1f%%\n", 
            investigation_results$panel_coverage$overall_coverage))
cat(sprintf("4. Extreme changes (2022-2024): %d municipalities\n",
            if (!is.null(investigation_results$extreme_changes)) 
              nrow(investigation_results$extreme_changes$extreme_changes) else 0))

cat("\nReview the detailed results to determine which issues are legitimate.\n")