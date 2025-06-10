# Sanity Check Validation Functions
# Purpose: Validate the sanity check report calculations and identify issues

library(data.table)

#' Validate sanity check calculations
#' 
#' @param geocoded_locais Geocoded polling station data
#' @param panel_ids Panel ID assignments
#' @return List of validation results
validate_sanity_checks <- function(geocoded_locais, panel_ids) {
  
  cat("\n=== SANITY CHECK VALIDATION ===\n\n")
  
  results <- list()
  
  # 1. Validate municipality count calculation
  cat("1. MUNICIPALITY COUNT VALIDATION\n")
  
  # Original calculation (potentially flawed)
  muni_summary_original <- geocoded_locais[, .(
    n_stations = uniqueN(local_id),
    n_years = uniqueN(ano)
  ), by = .(cod_localidade_ibge, nm_localidade, sg_uf)]
  
  # Correct calculation (by code only)
  muni_summary_correct <- geocoded_locais[, .(
    n_stations = uniqueN(local_id),
    n_years = uniqueN(ano),
    n_names = uniqueN(nm_localidade),
    names = paste(unique(nm_localidade), collapse = " | ")
  ), by = cod_localidade_ibge]
  
  cat(sprintf("  - Original method (grouping by code+name+state): %d rows\n", 
              nrow(muni_summary_original)))
  cat(sprintf("  - Correct method (grouping by code only): %d unique municipalities\n", 
              nrow(muni_summary_correct)))
  cat(sprintf("  - Municipalities with multiple names: %d\n",
              sum(muni_summary_correct$n_names > 1)))
  
  # Check for duplicate municipality names
  name_variations <- geocoded_locais[, .(
    n_variations = uniqueN(nm_localidade),
    variations = paste(unique(nm_localidade), collapse = " | ")
  ), by = cod_localidade_ibge][n_variations > 1][order(-n_variations)]
  
  if (nrow(name_variations) > 0) {
    cat("\n  Examples of name variations:\n")
    print(name_variations[1:min(5, .N)])
  }
  
  results$municipality_count <- list(
    original_count = nrow(muni_summary_original),
    correct_count = nrow(muni_summary_correct),
    name_variations = name_variations
  )
  
  # 2. Validate temporal consistency calculation
  cat("\n2. TEMPORAL CONSISTENCY VALIDATION\n")
  
  years_covered <- sort(unique(geocoded_locais$ano))
  
  # Municipalities present in all years
  munis_all_years_original <- muni_summary_original[n_years == length(years_covered)]
  munis_all_years_correct <- muni_summary_correct[n_years == length(years_covered)]
  
  cat(sprintf("  - Years in dataset: %s\n", paste(years_covered, collapse = ", ")))
  cat(sprintf("  - Original calculation: %d municipalities in all years\n", 
              nrow(munis_all_years_original)))
  cat(sprintf("  - Correct calculation: %d municipalities in all years\n", 
              nrow(munis_all_years_correct)))
  
  results$temporal_consistency <- list(
    original_all_years = nrow(munis_all_years_original),
    correct_all_years = nrow(munis_all_years_correct)
  )
  
  # 3. Validate extreme changes calculation
  cat("\n3. EXTREME CHANGES VALIDATION (2022-2024)\n")
  
  if (all(c(2022, 2024) %in% years_covered)) {
    # Check if the merge is done correctly
    muni_2022 <- geocoded_locais[ano == 2022, .(
      n_2022 = uniqueN(local_id)
    ), by = cod_localidade_ibge]
    
    muni_2024 <- geocoded_locais[ano == 2024, .(
      n_2024 = uniqueN(local_id)
    ), by = cod_localidade_ibge]
    
    # Correct merge
    muni_changes <- merge(muni_2022, muni_2024, 
                         by = "cod_localidade_ibge", 
                         all = TRUE)
    
    muni_changes[is.na(n_2022), n_2022 := 0]
    muni_changes[is.na(n_2024), n_2024 := 0]
    
    muni_changes[, pct_change := ifelse(n_2022 > 0, 
                                       (n_2024 - n_2022) / n_2022 * 100,
                                       ifelse(n_2024 > 0, Inf, 0))]
    
    extreme_changes <- muni_changes[abs(pct_change) >= 100 | is.infinite(pct_change)]
    
    cat(sprintf("  - Municipalities with 100%%+ change: %d\n", nrow(extreme_changes)))
    cat(sprintf("  - New municipalities in 2024: %d\n", 
                sum(muni_changes$n_2022 == 0 & muni_changes$n_2024 > 0)))
    cat(sprintf("  - Disappeared municipalities: %d\n",
                sum(muni_changes$n_2022 > 0 & muni_changes$n_2024 == 0)))
    
    results$extreme_changes <- list(
      n_extreme = nrow(extreme_changes),
      extreme_municipalities = extreme_changes
    )
  }
  
  # 4. Validate panel/station persistence
  cat("\n4. PANEL/STATION PERSISTENCE VALIDATION\n")
  
  # Check if local_id is unique per year or across years
  local_id_check <- geocoded_locais[, .(
    n_occurrences = .N,
    n_years = uniqueN(ano),
    years = paste(sort(unique(ano)), collapse = ",")
  ), by = local_id]
  
  # Check the structure of local_id
  sample_ids <- head(unique(geocoded_locais$local_id), 10)
  cat("  Sample local_ids:\n")
  print(sample_ids)
  
  # Check if local_ids contain year information
  id_has_year <- any(grepl("2020|2022|2024", geocoded_locais$local_id))
  cat(sprintf("\n  - local_id contains year: %s\n", id_has_year))
  
  # Stations appearing in multiple years
  multi_year_stations <- local_id_check[n_years > 1]
  cat(sprintf("  - Stations appearing in multiple years: %d\n", nrow(multi_year_stations)))
  cat(sprintf("  - Stations appearing in all %d years: %d\n", 
              length(years_covered), 
              sum(local_id_check$n_years == length(years_covered))))
  
  # Panel persistence
  if (nrow(panel_ids) > 0) {
    panel_temporal <- merge(
      geocoded_locais[, .(local_id, ano)],
      panel_ids[, .(local_id, panel_id)],
      by = "local_id"
    )
    
    panel_years <- panel_temporal[, .(
      n_years = uniqueN(ano),
      years = paste(sort(unique(ano)), collapse = ",")
    ), by = panel_id]
    
    cat(sprintf("\n  - Panels spanning all years: %d\n", 
                sum(panel_years$n_years == length(years_covered))))
    cat(sprintf("  - Panels spanning 2+ years: %d\n", 
                sum(panel_years$n_years >= 2)))
  }
  
  results$persistence <- list(
    local_id_has_year = id_has_year,
    multi_year_stations = nrow(multi_year_stations),
    all_year_stations = sum(local_id_check$n_years == length(years_covered))
  )
  
  # 5. Summary of issues
  cat("\n=== SANITY CHECK ISSUES IDENTIFIED ===\n")
  
  issues <- character()
  
  if (results$municipality_count$original_count > results$municipality_count$correct_count + 100) {
    issues <- c(issues, sprintf(
      "Municipality count inflated by %d due to grouping by name variations",
      results$municipality_count$original_count - results$municipality_count$correct_count
    ))
  }
  
  if (results$persistence$local_id_has_year) {
    issues <- c(issues, 
      "local_id appears to include year, preventing cross-year matching"
    )
  }
  
  if (results$persistence$all_year_stations == 0 && !results$persistence$local_id_has_year) {
    issues <- c(issues,
      "No stations found in all years despite year-independent IDs"
    )
  }
  
  if (length(issues) > 0) {
    cat("\n")
    for (i in seq_along(issues)) {
      cat(sprintf("%d. %s\n", i, issues[i]))
    }
  } else {
    cat("\nNo major issues found in sanity check calculations.\n")
  }
  
  results$issues <- issues
  
  return(results)
}

#' Generate corrected summary statistics
#' 
#' @param geocoded_locais Geocoded polling station data
#' @param panel_ids Panel ID assignments
#' @return Data.table with corrected statistics
generate_corrected_stats <- function(geocoded_locais, panel_ids) {
  
  years <- sort(unique(geocoded_locais$ano))
  
  # Correct municipality count (by code only)
  unique_munis <- uniqueN(geocoded_locais$cod_localidade_ibge)
  
  # Municipalities in all years
  muni_years <- geocoded_locais[, .(
    n_years = uniqueN(ano)
  ), by = cod_localidade_ibge]
  munis_all_years <- sum(muni_years$n_years == length(years))
  
  # Panel coverage
  total_stations <- uniqueN(geocoded_locais$local_id)
  stations_with_panel <- uniqueN(panel_ids$local_id)
  panel_coverage <- stations_with_panel / total_stations * 100
  
  # Stations by uniqueness
  station_years <- geocoded_locais[, .(
    n_years = uniqueN(ano)
  ), by = local_id]
  
  # Panel persistence
  if (nrow(panel_ids) > 0) {
    panel_temporal <- merge(
      geocoded_locais[, .(local_id, ano)],
      panel_ids[, .(local_id, panel_id)],
      by = "local_id"
    )
    
    panel_persistence <- panel_temporal[, .(
      n_years = uniqueN(ano)
    ), by = panel_id]
    
    panels_all_years <- sum(panel_persistence$n_years == length(years))
  } else {
    panels_all_years <- 0
  }
  
  stats <- data.table(
    Metric = c(
      "Unique Municipalities (Correct)",
      "Municipalities in All Years", 
      "Panel ID Coverage %",
      "Unique Stations Across Years",
      "Stations Appearing Once",
      "Stations in Multiple Years",
      "Stations in All Years",
      "Panels Spanning All Years"
    ),
    Value = c(
      unique_munis,
      munis_all_years,
      round(panel_coverage, 1),
      nrow(station_years),
      sum(station_years$n_years == 1),
      sum(station_years$n_years > 1),
      sum(station_years$n_years == length(years)),
      panels_all_years
    ),
    Expected = c(
      "~5,570",
      "~5,000",
      ">90%",
      "-",
      "-",
      ">100,000",
      ">50,000",
      ">50,000"
    )
  )
  
  return(stats)
}