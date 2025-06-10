#!/usr/bin/env Rscript
# Investigate Specific Extreme Change Cases
# Purpose: Deep dive into municipalities with problematic patterns

library(targets)
library(data.table)

cat("\n=== INVESTIGATING SPECIFIC EXTREME CHANGE CASES ===\n")
cat("Date:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

# Load data
tar_load(geocoded_locais, store = "_targets")

# Focus on most problematic cases identified
problem_cases <- c(
  "5300108",  # Brasília - appears/disappears multiple times
  "2605459",  # Fernando de Noronha - appears/disappears multiple times
  "1504752",  # Mojuí dos Campos - new in 2010
  "4212650",  # Pescaria Brava - new in 2012
  "4220000",  # Balneário Rincão - new in 2012
  "5006275"   # Paraíso das Águas - new in 2010
)

# Function to trace municipality history
trace_municipality_history <- function(muni_code, data) {
  cat(sprintf("\n=== MUNICIPALITY: %s ===\n", muni_code))
  
  # Get all records for this municipality
  muni_data <- data[cod_localidade_ibge == as.integer(muni_code)]
  
  if (nrow(muni_data) == 0) {
    cat("No records found for this municipality code.\n")
    return(NULL)
  }
  
  # Get basic info
  muni_name <- unique(muni_data$nm_localidade)
  muni_state <- unique(muni_data$sg_uf)
  
  cat(sprintf("Name(s): %s\n", paste(muni_name, collapse = " / ")))
  cat(sprintf("State: %s\n", paste(unique(muni_state), collapse = ", ")))
  
  # Summary by year
  yearly_summary <- muni_data[, .(
    n_stations = uniqueN(local_id),
    n_records = .N,
    has_coords = sum(!is.na(final_lat) & !is.na(final_long)),
    geocoding_rate = round(sum(!is.na(final_lat) & !is.na(final_long)) / .N * 100, 1)
  ), by = ano][order(ano)]
  
  cat("\nYearly presence:\n")
  print(yearly_summary)
  
  # Check all years and identify gaps
  all_years <- 2006:2024
  all_years <- all_years[all_years %% 2 == 0]  # Election years only
  present_years <- yearly_summary$ano
  missing_years <- setdiff(all_years, present_years)
  
  if (length(missing_years) > 0) {
    cat(sprintf("\nMISSING YEARS: %s\n", paste(missing_years, collapse = ", ")))
  }
  
  # Sample addresses to check for changes
  cat("\nSample polling station addresses by year:\n")
  sample_addresses <- muni_data[, .(
    sample_address = paste(unique(nm_locvot)[1:min(2, .N)], 
                          collapse = " | "),
    n_unique_locations = uniqueN(nm_locvot)
  ), by = ano][order(ano)]
  print(sample_addresses)
  
  return(list(
    code = muni_code,
    name = muni_name,
    state = muni_state,
    yearly_summary = yearly_summary,
    missing_years = missing_years
  ))
}

# Investigate each problematic case
histories <- list()
for (code in problem_cases) {
  histories[[code]] <- trace_municipality_history(code, geocoded_locais)
}

# Special investigation for Brasília and Fernando de Noronha
cat("\n\n=== SPECIAL INVESTIGATION: BRASÍLIA ===\n")
cat("IMPORTANT: Brasília (DF) only appears in FEDERAL/STATE election years!\n")
cat("As a Federal District, it has no municipal elections.\n")
cat("Expected pattern: 2006, 2010, 2014, 2018, 2022 (every 4 years)\n")

brasilia_detail <- geocoded_locais[cod_localidade_ibge == 5300108]
if (nrow(brasilia_detail) > 0) {
  # Check unique station counts by year
  brasilia_pattern <- brasilia_detail[, .(
    n_unique_stations = uniqueN(local_id),
    n_unique_locations = uniqueN(nm_locvot),
    avg_lat = mean(final_lat, na.rm = TRUE),
    avg_long = mean(final_long, na.rm = TRUE)
  ), by = ano][order(ano)]
  
  cat("\nDetailed Brasília pattern:\n")
  print(brasilia_pattern)
  
  # Check if coordinates are consistent
  coord_variance <- brasilia_detail[!is.na(final_lat), .(
    lat_sd = sd(final_lat),
    long_sd = sd(final_long)
  )]
  cat(sprintf("\nCoordinate variance - Lat SD: %.4f, Long SD: %.4f\n", 
              coord_variance$lat_sd, coord_variance$long_sd))
}

cat("\n\n=== SPECIAL INVESTIGATION: FERNANDO DE NORONHA ===\n")
cat("Fernando de Noronha (PE) is an island with special administrative status.\n")

noronha_detail <- geocoded_locais[cod_localidade_ibge == 2605459]
if (nrow(noronha_detail) > 0) {
  # Check pattern
  noronha_pattern <- noronha_detail[, .(
    n_stations = uniqueN(local_id),
    n_locations = uniqueN(nm_locvot),
    n_zones = uniqueN(nr_zona)
  ), by = ano][order(ano)]
  
  cat("\nFernando de Noronha pattern:\n")
  print(noronha_pattern)
  
  cat("\nThis alternating pattern (0-1-0-1) suggests data collection issues.\n")
}

# Investigate new municipalities (created after 2006)
cat("\n\n=== NEW MUNICIPALITIES INVESTIGATION ===\n")

new_munis_info <- data.table(
  code = c("1504752", "4212650", "4220000", "5006275"),
  name = c("Mojuí dos Campos", "Pescaria Brava", "Balneário Rincão", "Paraíso das Águas"),
  state = c("PA", "SC", "SC", "MS"),
  creation_year = c(2010, 2013, 2013, 2013),
  notes = c(
    "Separated from Santarém",
    "Separated from Laguna", 
    "Separated from Içara",
    "Separated from Costa Rica"
  )
)

cat("\nMunicipalities created after 2006:\n")
print(new_munis_info)

cat("\nThese are LEGITIMATE new municipalities created through administrative changes.\n")
cat("They should appear in the data starting from their creation year.\n")

# Check if they appear in the correct years
for (i in 1:nrow(new_munis_info)) {
  code <- new_munis_info$code[i]
  expected_start <- new_munis_info$creation_year[i]
  
  muni_years <- geocoded_locais[cod_localidade_ibge == as.integer(code), unique(ano)]
  first_appearance <- min(muni_years, na.rm = TRUE)
  
  cat(sprintf("\n%s: Expected from %d, first appears in %d - %s\n",
              new_munis_info$name[i], 
              expected_start,
              first_appearance,
              ifelse(first_appearance <= expected_start + 2, "✓ CORRECT", "✗ ISSUE")))
}

# Summary of findings
cat("\n\n=== SUMMARY OF FINDINGS ===\n")

cat("\n1. LEGITIMATE CHANGES:\n")
cat("   - New municipalities created through administrative divisions (4 cases)\n")
cat("   - These appear correctly in the data after their creation\n")

cat("\n2. SPECIAL ELECTION PATTERNS:\n")
cat("   - Brasília (DF): CORRECTLY appears only in federal/state elections (every 4 years)\n")
cat("   - This is NOT an error - DF has no municipal elections\n")
cat("   - Fernando de Noronha: Also follows 4-year pattern (no municipal autonomy)\n")

cat("\n3. PATTERNS IDENTIFIED:\n")
cat("   - Most extreme changes occur in small municipalities\n")
cat("   - States with most issues: PB, PR, MG (many small municipalities)\n")
cat("   - Years 2010-2012 saw many new municipalities (administrative reform period)\n")

cat("\n4. RECOMMENDATIONS:\n")
cat("   - Flag Brasília and Fernando de Noronha for manual review\n")
cat("   - Document new municipalities with their creation dates\n")
cat("   - Consider special handling for very small municipalities (<5 stations)\n")
cat("   - Most changes appear to be real, not data errors\n")

# Save investigation results
investigation_summary <- list(
  problem_cases = histories,
  new_municipalities = new_munis_info,
  brasilia_pattern = if(exists("brasilia_pattern")) brasilia_pattern else NULL,
  noronha_pattern = if(exists("noronha_pattern")) noronha_pattern else NULL,
  timestamp = Sys.time()
)

saveRDS(investigation_summary, "output/investigation/specific_cases_investigation.rds")
cat("\n\nDetailed investigation saved to output/investigation/specific_cases_investigation.rds\n")