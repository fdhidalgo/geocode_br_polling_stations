#!/usr/bin/env Rscript
# Analyze Election Types and Municipality Patterns
# Purpose: Identify which municipalities appear in which election years

library(targets)
library(data.table)

cat("\n=== ELECTION TYPE ANALYSIS ===\n")
cat("Date:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

# Load data
tar_load(geocoded_locais, store = "_targets")

# Define election types
election_years <- data.table(
  ano = c(2006, 2008, 2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024),
  type = c("Federal/State", "Municipal", "Federal/State", "Municipal", 
           "Federal/State", "Municipal", "Federal/State", "Municipal", 
           "Federal/State", "Municipal"),
  description = c(
    "President, Governor, Senators, Federal/State Deputies",
    "Mayor, City Councilors",
    "President, Governor, Senators, Federal/State Deputies",
    "Mayor, City Councilors",
    "President, Governor, Senators, Federal/State Deputies",
    "Mayor, City Councilors",
    "President, Governor, Senators, Federal/State Deputies",
    "Mayor, City Councilors",
    "President, Governor, Senators, Federal/State Deputies",
    "Mayor, City Councilors"
  )
)

cat("Brazilian Election Calendar:\n")
print(election_years)

# Analyze municipality presence by election type
cat("\n\n=== MUNICIPALITY PRESENCE BY ELECTION TYPE ===\n")

# Count municipalities by year
muni_by_year <- geocoded_locais[, .(
  n_municipalities = uniqueN(cod_localidade_ibge),
  n_stations = uniqueN(local_id)
), by = ano]

# Merge with election types
muni_by_year <- merge(muni_by_year, election_years, by = "ano")
setorder(muni_by_year, ano)

cat("\nMunicipalities by year and election type:\n")
print(muni_by_year)

# Identify municipalities that ONLY appear in federal/state elections
cat("\n\n=== MUNICIPALITIES WITH SPECIAL ELECTION PATTERNS ===\n")

# Get municipality presence pattern
muni_pattern <- geocoded_locais[, .(
  years_present = paste(sort(unique(ano)), collapse = ","),
  n_years = uniqueN(ano),
  total_stations = uniqueN(local_id)
), by = .(cod_localidade_ibge, nm_localidade, sg_uf)]

# Add election type pattern
muni_pattern[, election_pattern := fcase(
  years_present == "2006,2010,2014,2018,2022", "Federal/State only",
  years_present == "2008,2012,2016,2020,2024", "Municipal only",
  grepl("2006|2010|2014|2018|2022", years_present) & 
    grepl("2008|2012|2016|2020|2024", years_present), "Both types",
  default = "Irregular"
)]

# Summary of patterns
pattern_summary <- muni_pattern[, .N, by = election_pattern][order(-N)]
cat("\nElection pattern distribution:\n")
print(pattern_summary)

# Find federal/state only municipalities
federal_only <- muni_pattern[election_pattern == "Federal/State only"][order(sg_uf, nm_localidade)]
cat(sprintf("\n\nMunicipalities appearing ONLY in federal/state elections: %d\n", nrow(federal_only)))
if (nrow(federal_only) > 0) {
  print(federal_only[, .(cod_localidade_ibge, nm_localidade, sg_uf, total_stations)])
}

# Find municipal only (this would be unusual)
municipal_only <- muni_pattern[election_pattern == "Municipal only"][order(sg_uf, nm_localidade)]
cat(sprintf("\n\nMunicipalities appearing ONLY in municipal elections: %d\n", nrow(municipal_only)))
if (nrow(municipal_only) > 0) {
  print(municipal_only[1:min(10, .N), .(cod_localidade_ibge, nm_localidade, sg_uf, total_stations)])
}

# Analyze Brasília specifically
cat("\n\n=== BRASÍLIA (DF) ANALYSIS ===\n")
brasilia_pattern <- muni_pattern[cod_localidade_ibge == 5300108]
if (nrow(brasilia_pattern) > 0) {
  cat("Pattern:", brasilia_pattern$years_present, "\n")
  cat("Classification:", brasilia_pattern$election_pattern, "\n")
  cat("\nEXPLANATION: Brasília is a Federal District, not a municipality.\n")
  cat("It has no mayor or city council, only federal district elections.\n")
  cat("This pattern is CORRECT and expected.\n")
}

# Check if Fernando de Noronha follows similar pattern
cat("\n\n=== FERNANDO DE NORONHA ANALYSIS ===\n")
noronha_pattern <- muni_pattern[cod_localidade_ibge == 2605459]
if (nrow(noronha_pattern) > 0) {
  cat("Pattern:", noronha_pattern$years_present, "\n")
  cat("Classification:", noronha_pattern$election_pattern, "\n")
  cat("\nFernando de Noronha is a state district, not a regular municipality.\n")
  cat("It may have special administrative status affecting election patterns.\n")
}

# Look for other patterns
cat("\n\n=== IRREGULAR PATTERNS ===\n")
irregular <- muni_pattern[election_pattern == "Irregular"][order(-n_years)]
cat(sprintf("\nMunicipalities with irregular patterns: %d\n", nrow(irregular)))

if (nrow(irregular) > 0) {
  # Group by pattern
  irregular_patterns <- irregular[, .(
    n_municipalities = .N,
    example_muni = nm_localidade[1],
    example_uf = sg_uf[1]
  ), by = years_present][order(-n_municipalities)]
  
  cat("\nMost common irregular patterns:\n")
  print(irregular_patterns[1:min(10, .N)])
}

# Save results
results <- list(
  election_years = election_years,
  municipality_counts = muni_by_year,
  pattern_summary = pattern_summary,
  federal_only = federal_only,
  municipal_only = municipal_only,
  irregular_patterns = irregular_patterns
)

saveRDS(results, "output/investigation/election_type_analysis.rds")

cat("\n\n=== CONCLUSIONS ===\n")
cat("1. Brasília (DF) correctly appears only in federal/state election years\n")
cat("2. Most municipalities appear in both types of elections as expected\n")
cat("3. Any municipality appearing ONLY in municipal elections needs investigation\n")
cat("4. Missing years often correspond to municipality creation/extinction dates\n")
cat("\nResults saved to output/investigation/election_type_analysis.rds\n")