# Data Quality Investigation Functions
# Author: Pipeline Investigation
# Date: 2025-01-06
# Purpose: Investigate and diagnose critical data quality issues

library(data.table)
library(stringr)

#' Run comprehensive data quality investigation
#' 
#' @param geocoded_locais data.table with geocoded polling station data
#' @param panel_ids data.table with panel ID assignments
#' @param verbose logical, print detailed output
#' @return list with investigation results
investigate_data_quality <- function(geocoded_locais, panel_ids, verbose = TRUE) {
  
  if (verbose) cat("\n=== DATA QUALITY INVESTIGATION ===\n\n")
  
  results <- list()
  
  # 1. Municipality Count Investigation
  if (verbose) cat("1. MUNICIPALITY COUNT ANALYSIS\n")
  results$municipality_counts <- investigate_municipality_counts(geocoded_locais, verbose)
  
  # 2. Temporal Consistency Investigation  
  if (verbose) cat("\n2. TEMPORAL CONSISTENCY ANALYSIS\n")
  results$temporal_consistency <- investigate_temporal_consistency(geocoded_locais, verbose)
  
  # 3. Extreme Changes Investigation
  if (verbose) cat("\n3. EXTREME CHANGES ANALYSIS (2022-2024)\n")
  results$extreme_changes <- investigate_extreme_changes(geocoded_locais, verbose)
  
  # 4. Panel ID Coverage Investigation
  if (verbose) cat("\n4. PANEL ID COVERAGE ANALYSIS\n")
  results$panel_coverage <- investigate_panel_coverage(geocoded_locais, panel_ids, verbose)
  
  # 5. Polling Station Persistence Analysis
  if (verbose) cat("\n5. POLLING STATION PERSISTENCE ANALYSIS\n")
  results$station_persistence <- investigate_station_persistence(geocoded_locais, panel_ids, verbose)
  
  # 6. Summary and Recommendations
  if (verbose) {
    cat("\n=== INVESTIGATION SUMMARY ===\n")
    print_investigation_summary(results)
  }
  
  return(results)
}

#' Investigate municipality count discrepancies
investigate_municipality_counts <- function(geocoded_locais, verbose = TRUE) {
  
  # Get unique municipalities in the data
  data_munis <- unique(geocoded_locais[, .(
    cod_municipio = cod_localidade_ibge,
    nm_municipio = nm_localidade,
    sg_uf = sg_uf
  )])
  
  # Count by year
  munis_by_year <- geocoded_locais[, .(
    n_municipios = uniqueN(cod_localidade_ibge)
  ), by = ano][order(ano)]
  
  # Load reference municipality list
  if (file.exists("data/muni_identifiers.csv")) {
    ref_munis <- fread("data/muni_identifiers.csv", encoding = "UTF-8")
    ref_count <- nrow(ref_munis[existe == 1])
    ref_codes <- ref_munis[existe == 1, id_munic_7]
  } else {
    ref_count <- 5570  # Known Brazilian municipality count
    ref_codes <- NULL
  }
  
  # Identify issues
  data_count <- nrow(data_munis)
  discrepancy <- data_count - ref_count
  
  # Check for duplicates with different names
  duplicate_codes <- data_munis[, .N, by = cod_municipio][N > 1]
  
  # Check for invalid codes
  if (!is.null(ref_codes)) {
    invalid_codes <- data_munis[!cod_municipio %in% ref_codes]
    missing_codes <- ref_codes[!ref_codes %in% data_munis$cod_municipio]
  } else {
    invalid_codes <- data_munis[nchar(as.character(cod_municipio)) != 7]
    missing_codes <- NULL
  }
  
  if (verbose) {
    cat(sprintf("  - Total unique municipalities in data: %d\n", data_count))
    cat(sprintf("  - Expected municipalities (IBGE): %d\n", ref_count))
    cat(sprintf("  - Discrepancy: %+d municipalities\n", discrepancy))
    cat(sprintf("  - Municipalities with duplicate entries: %d\n", nrow(duplicate_codes)))
    cat(sprintf("  - Invalid municipality codes: %d\n", nrow(invalid_codes)))
    if (!is.null(missing_codes)) {
      cat(sprintf("  - Missing municipalities: %d\n", length(missing_codes)))
    }
    
    cat("\n  Municipality count by year:\n")
    print(munis_by_year)
  }
  
  return(list(
    total_unique = data_count,
    expected = ref_count,
    discrepancy = discrepancy,
    by_year = munis_by_year,
    duplicate_codes = duplicate_codes,
    invalid_codes = invalid_codes,
    missing_codes = missing_codes
  ))
}

#' Investigate temporal consistency of municipalities
investigate_temporal_consistency <- function(geocoded_locais, verbose = TRUE) {
  
  years <- sort(unique(geocoded_locais$ano))
  
  # Get municipalities by year
  munis_by_year <- lapply(years, function(y) {
    unique(geocoded_locais[ano == y, cod_localidade_ibge])
  })
  names(munis_by_year) <- as.character(years)
  
  # Find municipalities present in all years
  munis_all_years <- Reduce(intersect, munis_by_year)
  
  # Find year-specific municipalities
  year_specific <- lapply(seq_along(years), function(i) {
    year <- years[i]
    only_this_year <- setdiff(munis_by_year[[i]], 
                             Reduce(union, munis_by_year[-i]))
    data.table(
      ano = year,
      n_exclusive = length(only_this_year),
      cod_municipio = only_this_year
    )
  })
  year_specific_dt <- rbindlist(year_specific)
  
  # Calculate consistency metrics
  consistency_matrix <- matrix(0, nrow = length(years), ncol = length(years))
  rownames(consistency_matrix) <- colnames(consistency_matrix) <- as.character(years)
  
  for (i in seq_along(years)) {
    for (j in seq_along(years)) {
      consistency_matrix[i, j] <- length(intersect(munis_by_year[[i]], 
                                                  munis_by_year[[j]]))
    }
  }
  
  if (verbose) {
    cat(sprintf("  - Years analyzed: %s\n", paste(years, collapse = ", ")))
    cat(sprintf("  - Municipalities in all %d years: %d\n", 
                length(years), length(munis_all_years)))
    cat(sprintf("  - Expected in all years: ~5,000\n"))
    cat(sprintf("  - Missing from full coverage: %d\n", 
                5000 - length(munis_all_years)))
    
    cat("\n  Year-specific municipalities:\n")
    print(year_specific_dt[, .(ano, n_exclusive)])
    
    cat("\n  Municipality overlap matrix:\n")
    print(consistency_matrix)
  }
  
  return(list(
    years = years,
    munis_all_years = munis_all_years,
    n_all_years = length(munis_all_years),
    year_specific = year_specific_dt,
    consistency_matrix = consistency_matrix
  ))
}

#' Investigate extreme changes between 2022 and 2024
investigate_extreme_changes <- function(geocoded_locais, verbose = TRUE) {
  
  # Check if both years exist
  if (!all(c(2022, 2024) %in% geocoded_locais$ano)) {
    if (verbose) cat("  - Cannot analyze 2022-2024 changes: missing data\n")
    return(NULL)
  }
  
  # Calculate polling stations by municipality and year
  stations_2022 <- geocoded_locais[ano == 2022, .(
    n_2022 = uniqueN(local_id)
  ), by = .(cod_municipio = cod_localidade_ibge, 
            nm_municipio = nm_localidade,
            sg_uf)]
  
  stations_2024 <- geocoded_locais[ano == 2024, .(
    n_2024 = uniqueN(local_id)
  ), by = .(cod_municipio = cod_localidade_ibge,
            nm_municipio = nm_localidade, 
            sg_uf)]
  
  # Merge and calculate changes
  changes <- merge(stations_2022, stations_2024, 
                   by = c("cod_municipio", "nm_municipio", "sg_uf"), 
                   all = TRUE)
  
  # Handle municipalities that appear/disappear
  changes[is.na(n_2022), n_2022 := 0]
  changes[is.na(n_2024), n_2024 := 0]
  
  # Calculate percentage change
  changes[, ':='(
    absolute_change = n_2024 - n_2022,
    pct_change = ifelse(n_2022 > 0, 
                       (n_2024 - n_2022) / n_2022 * 100,
                       ifelse(n_2024 > 0, Inf, 0))
  )]
  
  # Identify extreme changes
  extreme_changes <- changes[abs(pct_change) >= 100 | is.infinite(pct_change)]
  extreme_changes <- extreme_changes[order(-abs(pct_change))]
  
  # Categorize changes
  changes[, change_category := fcase(
    is.infinite(pct_change) & pct_change > 0, "New in 2024",
    pct_change == -100, "Disappeared in 2024",
    pct_change >= 100, "Doubled or more",
    pct_change <= -50, "Halved or worse",
    abs(pct_change) >= 30, "Large change",
    default = "Normal change"
  )]
  
  change_summary <- changes[, .N, by = change_category][order(-N)]
  
  if (verbose) {
    cat(sprintf("  - Municipalities with 100%%+ change: %d\n", 
                nrow(extreme_changes)))
    cat(sprintf("  - New municipalities in 2024: %d\n",
                nrow(changes[change_category == "New in 2024"])))
    cat(sprintf("  - Disappeared municipalities: %d\n",
                nrow(changes[change_category == "Disappeared in 2024"])))
    
    cat("\n  Change categories:\n")
    print(change_summary)
    
    if (nrow(extreme_changes) > 0) {
      cat("\n  Top 10 extreme changes:\n")
      print(extreme_changes[1:min(10, .N), .(
        nm_municipio, sg_uf, n_2022, n_2024, pct_change = round(pct_change, 1)
      )])
    }
  }
  
  return(list(
    all_changes = changes,
    extreme_changes = extreme_changes,
    change_summary = change_summary
  ))
}

#' Investigate panel ID coverage issues
investigate_panel_coverage <- function(geocoded_locais, panel_ids, verbose = TRUE) {
  
  # Overall coverage
  total_stations <- uniqueN(geocoded_locais$local_id)
  stations_with_panel <- uniqueN(panel_ids$local_id)
  coverage_pct <- stations_with_panel / total_stations * 100
  
  # Coverage by year
  coverage_by_year <- geocoded_locais[, .(
    total_stations = uniqueN(local_id)
  ), by = ano]
  
  panel_by_year <- merge(
    geocoded_locais[, .(local_id, ano)],
    panel_ids[, .(local_id, panel_id)],
    by = "local_id"
  )[, .(stations_with_panel = uniqueN(local_id)), by = ano]
  
  coverage_by_year <- merge(coverage_by_year, panel_by_year, by = "ano", all.x = TRUE)
  coverage_by_year[is.na(stations_with_panel), stations_with_panel := 0]
  coverage_by_year[, coverage_pct := stations_with_panel / total_stations * 100]
  
  # Coverage by state
  coverage_by_state <- geocoded_locais[, .(
    total_stations = uniqueN(local_id)
  ), by = sg_uf]
  
  panel_by_state <- merge(
    geocoded_locais[, .(local_id, sg_uf)] |> unique(),
    panel_ids[, .(local_id, panel_id)],
    by = "local_id"
  )[, .(stations_with_panel = uniqueN(local_id)), by = sg_uf]
  
  coverage_by_state <- merge(coverage_by_state, panel_by_state, by = "sg_uf", all.x = TRUE)
  coverage_by_state[is.na(stations_with_panel), stations_with_panel := 0]
  coverage_by_state[, coverage_pct := stations_with_panel / total_stations * 100]
  coverage_by_state <- coverage_by_state[order(coverage_pct)]
  
  # Identify patterns in missing panel IDs
  missing_panel_stations <- geocoded_locais[!local_id %in% panel_ids$local_id]
  missing_patterns <- missing_panel_stations[, .(
    n_missing = .N,
    years_present = paste(sort(unique(ano)), collapse = ",")
  ), by = .(sg_uf, cod_localidade_ibge)][order(-n_missing)]
  
  if (verbose) {
    cat(sprintf("  - Total unique stations: %d\n", total_stations))
    cat(sprintf("  - Stations with panel IDs: %d\n", stations_with_panel))
    cat(sprintf("  - Overall coverage: %.1f%%\n", coverage_pct))
    cat(sprintf("  - Target coverage: 90%%\n"))
    cat(sprintf("  - Coverage gap: %.1f%%\n", 90 - coverage_pct))
    
    cat("\n  Coverage by year:\n")
    print(coverage_by_year)
    
    cat("\n  States with lowest coverage:\n")
    print(coverage_by_state[coverage_pct < 90][1:min(10, .N)])
  }
  
  return(list(
    overall_coverage = coverage_pct,
    by_year = coverage_by_year,
    by_state = coverage_by_state,
    missing_patterns = missing_patterns
  ))
}

#' Investigate polling station persistence across years
investigate_station_persistence <- function(geocoded_locais, panel_ids, verbose = TRUE) {
  
  # Merge panel IDs with temporal data
  panel_temporal <- merge(
    geocoded_locais[, .(local_id, ano)],
    panel_ids[, .(local_id, panel_id)],
    by = "local_id",
    all.x = TRUE
  )
  
  # Count stations appearing in multiple years (with or without panel ID)
  station_year_count <- geocoded_locais[, .(
    n_years = uniqueN(ano),
    years = paste(sort(unique(ano)), collapse = ",")
  ), by = local_id]
  
  # For stations with panel IDs, check panel persistence
  panel_persistence <- panel_temporal[!is.na(panel_id), .(
    n_years = uniqueN(ano),
    years = paste(sort(unique(ano)), collapse = ",")
  ), by = panel_id]
  
  # Find stations/panels in all years
  all_years <- sort(unique(geocoded_locais$ano))
  n_years <- length(all_years)
  
  stations_all_years <- station_year_count[n_years == n_years]
  panels_all_years <- panel_persistence[n_years == n_years]
  
  # Distribution of persistence
  persistence_dist <- station_year_count[, .N, by = n_years][order(n_years)]
  panel_persistence_dist <- panel_persistence[, .N, by = n_years][order(n_years)]
  
  if (verbose) {
    cat(sprintf("  - Years in dataset: %s\n", paste(all_years, collapse = ", ")))
    cat(sprintf("  - Individual stations in all %d years: %d\n", 
                n_years, nrow(stations_all_years)))
    cat(sprintf("  - Panel IDs spanning all %d years: %d\n", 
                n_years, nrow(panels_all_years)))
    
    cat("\n  Station persistence distribution:\n")
    print(persistence_dist)
    
    cat("\n  Panel persistence distribution:\n")
    print(panel_persistence_dist)
  }
  
  return(list(
    stations_all_years = nrow(stations_all_years),
    panels_all_years = nrow(panels_all_years),
    persistence_distribution = persistence_dist,
    panel_persistence_distribution = panel_persistence_dist
  ))
}

#' Print investigation summary
print_investigation_summary <- function(results) {
  
  cat("\nCRITICAL ISSUES IDENTIFIED:\n")
  
  # 1. Municipality count issue
  if (results$municipality_counts$discrepancy > 100) {
    cat(sprintf("\n1. MUNICIPALITY COUNT: %+d excess municipalities (expected ~5,570, found %d)\n",
                results$municipality_counts$discrepancy,
                results$municipality_counts$total_unique))
    cat("   Likely causes: Duplicate entries, historical codes, data processing errors\n")
  }
  
  # 2. Temporal consistency issue  
  if (results$temporal_consistency$n_all_years < 4500) {
    cat(sprintf("\n2. TEMPORAL CONSISTENCY: Only %d municipalities present in all years (expected ~5,000)\n",
                results$temporal_consistency$n_all_years))
    cat("   Likely causes: Data collection gaps, municipality code changes, mergers/splits\n")
  }
  
  # 3. Extreme changes issue
  if (!is.null(results$extreme_changes) && nrow(results$extreme_changes$extreme_changes) > 10) {
    cat(sprintf("\n3. EXTREME CHANGES: %d municipalities with 100%%+ change from 2022-2024\n",
                nrow(results$extreme_changes$extreme_changes)))
    cat("   Likely causes: Data errors, municipality reorganization, geocoding mismatches\n")
  }
  
  # 4. Panel coverage issue
  if (results$panel_coverage$overall_coverage < 90) {
    cat(sprintf("\n4. PANEL COVERAGE: %.1f%% coverage (below 90%% target)\n",
                results$panel_coverage$overall_coverage))
    cat("   Likely causes: String matching failures, address changes, new stations\n")
  }
  
  # 5. Station persistence issue
  if (results$station_persistence$stations_all_years == 0) {
    cat("\n5. STATION PERSISTENCE: Zero individual stations appear in all years\n")
    cat("   Likely causes: ID generation method changes, lack of stable identifiers\n")
  }
  
  cat("\nRECOMMENDED ACTIONS:\n")
  cat("1. Review municipality code validation and deduplication logic\n")
  cat("2. Implement historical municipality code mapping\n")
  cat("3. Add data quality checks at import stage\n")
  cat("4. Improve panel ID matching algorithm\n")
  cat("5. Consider using stable geographic identifiers\n")
}

#' Generate data quality report
#' 
#' @param results Investigation results from investigate_data_quality()
#' @param output_file Path to save report
generate_investigation_report <- function(results, output_file = "output/data_quality_investigation_report.html") {
  
  # Create markdown content
  report_content <- c(
    "# Data Quality Investigation Report",
    paste0("Generated: ", Sys.Date()),
    "",
    "## Executive Summary",
    "",
    "This report documents the investigation of critical data quality issues in the polling station geocoding pipeline.",
    "",
    "### Key Findings:",
    ""
  )
  
  # Add findings based on results
  if (results$municipality_counts$discrepancy > 100) {
    report_content <- c(report_content,
      sprintf("- **Municipality Count Discrepancy**: Found %d municipalities (expected ~5,570)",
              results$municipality_counts$total_unique))
  }
  
  if (results$temporal_consistency$n_all_years < 4500) {
    report_content <- c(report_content,
      sprintf("- **Temporal Consistency Issue**: Only %d municipalities present in all years",
              results$temporal_consistency$n_all_years))
  }
  
  if (!is.null(results$extreme_changes)) {
    report_content <- c(report_content,
      sprintf("- **Extreme Changes**: %d municipalities with 100%%+ change from 2022-2024",
              nrow(results$extreme_changes$extreme_changes)))
  }
  
  if (results$panel_coverage$overall_coverage < 90) {
    report_content <- c(report_content,
      sprintf("- **Panel Coverage Gap**: %.1f%% coverage (target: 90%%)",
              results$panel_coverage$overall_coverage))
  }
  
  if (results$station_persistence$stations_all_years == 0) {
    report_content <- c(report_content,
      "- **Station Persistence**: No individual stations found in all years")
  }
  
  # Add detailed sections
  report_content <- c(report_content,
    "",
    "## Detailed Analysis",
    "",
    "### 1. Municipality Count Analysis",
    "",
    "```",
    capture.output(print(results$municipality_counts$by_year)),
    "```",
    "",
    "### 2. Temporal Consistency",
    "",
    "Overlap matrix showing municipality counts shared between years:",
    "```",
    capture.output(print(results$temporal_consistency$consistency_matrix)),
    "```"
  )
  
  # Save report
  writeLines(report_content, con = gsub("\\.html$", ".md", output_file))
  
  if (requireNamespace("rmarkdown", quietly = TRUE)) {
    rmarkdown::render(gsub("\\.html$", ".md", output_file),
                     output_format = "html_document",
                     output_file = output_file,
                     quiet = TRUE)
    cat(sprintf("\nReport saved to: %s\n", output_file))
  }
}