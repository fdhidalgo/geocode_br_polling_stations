# Data Quality Monitoring Functions V2
# Fixed version that accepts data as parameters instead of using tar_read
# Complements the existing polling_station_sanity_check.qmd report
# Author: Data Quality Monitoring
# Date: 2025-01-10

library(data.table)
library(yaml)
library(quarto)

#' Create data quality monitoring report
#' 
#' This function serves as the main entry point for targets pipeline.
#' It takes the export file paths as inputs to establish proper dependencies.
#' 
#' @param geocoded_export Path to exported geocoded polling stations file
#' @param panelid_export Path to exported panel IDs file
#' @param geocoded_locais data.table with geocoded polling stations
#' @param panel_ids data.table with panel identifiers
#' @param config_file path to configuration YAML file
#' @return list with monitoring results
#' @export
create_data_quality_monitor <- function(geocoded_export,
                                      panelid_export,
                                      geocoded_locais,
                                      panel_ids,
                                      config_file = "config/data_quality_config.yaml") {
  
  # Verify export files exist (this ensures they were created)
  if (!file.exists(geocoded_export)) {
    stop("Geocoded export file not found: ", geocoded_export)
  }
  if (!file.exists(panelid_export)) {
    stop("Panel ID export file not found: ", panelid_export)
  }
  
  message(sprintf("Export files verified: %s, %s", 
                  basename(geocoded_export), 
                  basename(panelid_export)))
  
  # Run the actual monitoring
  run_data_quality_monitoring_v2(
    geocoded_locais = geocoded_locais,
    panel_ids = panel_ids,
    generate_alerts = TRUE,
    config_file = config_file
  )
}

#' Run comprehensive data quality monitoring
#' 
#' Wraps the existing Quarto report and adds additional checks.
#' NOTE: This function receives data directly from the targets pipeline,
#' not from the exported CSV files. The export targets (geocoded_export, 
#' panelid_export) should be run before this to ensure CSV files are updated.
#' 
#' @param geocoded_locais data.table with geocoded polling stations
#' @param panel_ids data.table with panel identifiers
#' @param generate_alerts logical, whether to print alerts
#' @param config_file path to configuration YAML file
#' @return list with monitoring results
#' @export
run_data_quality_monitoring_v2 <- function(geocoded_locais,
                                          panel_ids = NULL,
                                          generate_alerts = TRUE,
                                          config_file = "config/data_quality_config.yaml") {
  
  # Load configuration
  config <- yaml::read_yaml(config_file)
  results <- list()
  
  message("Running data quality monitoring suite...")
  
  # 1. Run existing Quarto sanity check report
  message("Executing polling station sanity check report...")
  quarto_result <- run_quarto_sanity_check()
  results$quarto_report <- quarto_result
  
  # 2. Extract key metrics from the provided data
  message("Extracting key metrics...")
  results$metrics <- extract_key_metrics_v2(geocoded_locais, panel_ids)
  
  # 3. Run additional monitoring checks
  message("Running historical comparison...")
  results$historical_comparison <- check_historical_trends_v2(geocoded_locais, config)
  
  message("Checking for duplicates...")
  results$duplicate_detection <- check_duplicates_v2(geocoded_locais, config)
  
  # 4. Generate pass/fail assessment
  results$overall_assessment <- assess_data_quality(results, config)
  
  # 5. Generate alerts if requested
  if (generate_alerts) {
    alerts <- generate_quality_alerts(results, config)
    results$alerts <- alerts
    
    if (length(alerts) > 0) {
      cat("\n🚨 DATA QUALITY ALERTS:\n")
      for (alert in alerts) {
        cat(paste0("  - ", alert, "\n"))
      }
    } else {
      cat("\n✅ All data quality checks passed!\n")
    }
  }
  
  # Save results for tracking
  output_dir <- "output/monitoring"
  if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
  saveRDS(results, file.path(output_dir, paste0("quality_monitoring_", Sys.Date(), ".rds")))
  
  # Print summary of what was analyzed
  cat("\n📊 Data Quality Monitoring Summary:\n")
  cat(paste0("  - Total records analyzed: ", format(nrow(geocoded_locais), big.mark = ","), "\n"))
  cat(paste0("  - States covered: ", length(unique(geocoded_locais$sg_uf)), "\n"))
  cat(paste0("  - Municipalities: ", results$metrics$total_municipalities, "\n"))
  cat(paste0("  - Years: ", paste(range(results$metrics$years_covered), collapse = "-"), "\n"))
  cat(paste0("  - Overall status: ", results$overall_assessment$overall_status, "\n\n"))
  
  return(results)
}

#' Extract key metrics from the provided data
extract_key_metrics_v2 <- function(geocoded_locais, panel_ids = NULL) {
  tryCatch({
    # Calculate key metrics
    metrics <- list(
      total_stations = uniqueN(geocoded_locais$local_id),
      total_records = nrow(geocoded_locais),
      total_municipalities = uniqueN(geocoded_locais$cod_localidade_ibge),
      years_covered = sort(unique(geocoded_locais$ano)),
      states_covered = sort(unique(geocoded_locais$sg_uf)),
      geocoding_coverage = geocoded_locais[, sum(!is.na(final_lat) & !is.na(final_long)) / .N * 100]
    )
    
    # Add panel coverage if available
    if (!is.null(panel_ids) && nrow(panel_ids) > 0) {
      metrics$panel_coverage <- uniqueN(panel_ids$local_id) / metrics$total_stations * 100
    }
    
    # Add year-specific metrics
    metrics$by_year <- geocoded_locais[, .(
      n_stations = uniqueN(local_id),
      n_municipalities = uniqueN(cod_localidade_ibge),
      geocoding_rate = sum(!is.na(final_lat) & !is.na(final_long)) / .N * 100
    ), by = ano][order(ano)]
    
    return(metrics)
    
  }, error = function(e) {
    warning("Could not extract metrics: ", e$message)
    return(list(error = as.character(e)))
  })
}

#' Check historical trends using provided data
check_historical_trends_v2 <- function(geocoded_locais, config) {
  tryCatch({
    years <- sort(unique(geocoded_locais$ano))
    
    if (length(years) < config$historical_comparison$min_years_required) {
      return(list(
        status = "insufficient_data", 
        message = sprintf("Need at least %d years for comparison, found %d", 
                         config$historical_comparison$min_years_required, 
                         length(years))
      ))
    }
    
    trends <- list()
    
    # Calculate year-over-year changes for all consecutive year pairs
    for (i in 2:length(years)) {
      prev_year <- years[i-1]
      curr_year <- years[i]
      
      # Municipality-level analysis
      prev_counts <- geocoded_locais[ano == prev_year, .(
        stations = uniqueN(local_id)
      ), by = cod_localidade_ibge]
      
      curr_counts <- geocoded_locais[ano == curr_year, .(
        stations = uniqueN(local_id)
      ), by = cod_localidade_ibge]
      
      # Merge and calculate changes
      comparison <- merge(prev_counts, curr_counts, 
                         by = "cod_localidade_ibge",
                         all = TRUE, 
                         suffixes = c("_prev", "_curr"))
      
      comparison[is.na(stations_prev), stations_prev := 0]
      comparison[is.na(stations_curr), stations_curr := 0]
      comparison[, change_pct := ifelse(stations_prev == 0, 
                                       ifelse(stations_curr > 0, Inf, 0),
                                       (stations_curr - stations_prev) / stations_prev * 100)]
      
      # Summary statistics
      trends[[paste0(prev_year, "_to_", curr_year)]] <- list(
        total_municipalities_prev = nrow(prev_counts),
        total_municipalities_curr = nrow(curr_counts),
        total_stations_prev = sum(prev_counts$stations),
        total_stations_curr = sum(curr_counts$stations),
        municipalities_disappeared = sum(comparison$stations_curr == 0 & comparison$stations_prev > 0),
        municipalities_appeared = sum(comparison$stations_prev == 0 & comparison$stations_curr > 0),
        extreme_increases = sum(comparison$change_pct > config$extreme_change_threshold, na.rm = TRUE),
        extreme_decreases = sum(comparison$change_pct < -config$extreme_change_threshold, na.rm = TRUE),
        # Keep the actual extreme cases for investigation
        extreme_cases = comparison[abs(change_pct) > config$extreme_change_threshold & 
                                  !is.infinite(change_pct)][
                                  order(-abs(change_pct))][1:min(10, .N)]
      )
    }
    
    # Add overall trend summary
    trends$summary <- list(
      years_analyzed = years,
      total_year_pairs = length(years) - 1,
      periods_with_extreme_changes = sum(sapply(trends, function(x) {
        if (is.list(x) && !is.null(x$extreme_increases)) {
          x$extreme_increases + x$extreme_decreases > 0
        } else FALSE
      }))
    )
    
    return(trends)
    
  }, error = function(e) {
    return(list(status = "error", message = as.character(e)))
  })
}

#' Check for duplicate polling stations using provided data
check_duplicates_v2 <- function(geocoded_locais, config) {
  tryCatch({
    duplicates <- list()
    
    # Check for exact coordinate duplicates (same lat/long)
    coord_data <- geocoded_locais[!is.na(final_lat) & !is.na(final_long)]
    
    if (nrow(coord_data) > 0) {
      # Round coordinates to 6 decimal places (about 0.1m precision)
      coord_data[, coord_key := paste(round(final_lat, 6), round(final_long, 6), sep = "_")]
      
      # 1. CROSS-YEAR DUPLICATES (expected behavior - same location used across years)
      cross_year_dups <- coord_data[, .(
        count = .N,
        n_years = uniqueN(ano),
        years = paste(sort(unique(ano)), collapse = ","),
        n_stations = uniqueN(local_id),
        states = paste(sort(unique(sg_uf)), collapse = ","),
        municipalities = uniqueN(cod_localidade_ibge)
      ), by = coord_key][n_years > 1][order(-count)]
      
      if (nrow(cross_year_dups) > 0) {
        duplicates$cross_year_duplicates <- list(
          total_locations_reused = nrow(cross_year_dups),
          total_stations_involved = sum(cross_year_dups$count),
          avg_years_per_location = mean(cross_year_dups$n_years),
          severity = "LOW",  # This is expected behavior
          message = "Locations reused across multiple elections (expected)",
          top_examples = cross_year_dups[1:min(5, .N)]
        )
      }
      
      # 2. SAME-YEAR DUPLICATES (multiple stations at same coordinates in same year)
      same_year_dups <- coord_data[, .(
        n_stations = uniqueN(local_id),
        station_numbers = paste(sort(unique(nr_locvot)), collapse = ","),
        sg_uf = first(sg_uf),
        municipio = first(nm_localidade)
      ), by = .(coord_key, ano)][n_stations > 1][order(-n_stations)]
      
      if (nrow(same_year_dups) > 0) {
        # Analyze addresses for same-year duplicates
        # Check if stations at same coordinates have same or different addresses
        address_analysis <- coord_data[coord_key %in% same_year_dups$coord_key][, .(
          n_stations = uniqueN(local_id),
          n_unique_addresses = uniqueN(ds_endereco),
          same_address = uniqueN(ds_endereco) == 1,
          example_addresses = paste(head(unique(ds_endereco), 3), collapse = " | ")
        ), by = .(coord_key, ano)]
        
        # Separate concerning vs non-concerning duplicates
        same_address_dups <- address_analysis[same_address == TRUE]
        diff_address_dups <- address_analysis[same_address == FALSE]
        
        # Summarize by year
        same_year_summary <- same_year_dups[, .(
          duplicate_locations = .N,
          total_stations = sum(n_stations),
          max_stations_per_location = max(n_stations)
        ), by = ano][order(ano)]
        
        duplicates$same_year_duplicates <- list(
          total_cases = nrow(same_year_dups),
          total_stations_affected = sum(same_year_dups$n_stations),
          # Same address duplicates - not concerning
          same_address_cases = nrow(same_address_dups),
          same_address_stations = sum(same_address_dups$n_stations),
          same_address_pct = nrow(same_address_dups) / nrow(address_analysis) * 100,
          # Different address duplicates - concerning
          diff_address_cases = nrow(diff_address_dups),
          diff_address_stations = sum(diff_address_dups$n_stations),
          diff_address_pct = nrow(diff_address_dups) / nrow(address_analysis) * 100,
          severity = ifelse(nrow(diff_address_dups) / nrow(address_analysis) > 0.3, "MEDIUM", "LOW"),
          message = sprintf("Found %d locations where multiple stations share coordinates but have DIFFERENT addresses (%.1f%% of duplicates) - these may indicate geocoding issues",
                           nrow(diff_address_dups), nrow(diff_address_dups) / nrow(address_analysis) * 100),
          explanation = sprintf("%.1f%% of coordinate duplicates share the SAME address (expected for large venues), while %.1f%% have DIFFERENT addresses (potential geocoding errors)",
                               nrow(same_address_dups) / nrow(address_analysis) * 100,
                               nrow(diff_address_dups) / nrow(address_analysis) * 100),
          by_year = same_year_summary,
          concerning_examples = diff_address_dups[order(-n_stations)][1:min(10, .N)]
        )
      }
      
      # 3. OVERALL COORDINATE DUPLICATES (for backward compatibility)
      coord_dup_summary <- coord_data[, .(
        count = .N,
        years = paste(sort(unique(ano)), collapse = ","),
        states = paste(sort(unique(sg_uf)), collapse = ","),
        municipalities = uniqueN(cod_localidade_ibge)
      ), by = coord_key][count > 1][order(-count)]
      
      if (nrow(coord_dup_summary) > 0) {
        duplicates$coordinate_duplicates <- list(
          total_duplicate_groups = nrow(coord_dup_summary),
          total_duplicate_stations = sum(coord_dup_summary$count),
          severity = ifelse(nrow(coord_dup_summary) > config$duplicate_thresholds$coordinates, 
                           "HIGH", "MEDIUM"),
          top_duplicates = coord_dup_summary[1:min(5, .N)]
        )
      }
    }
    
    # Check for duplicate local_id (should be unique)
    id_duplicates <- geocoded_locais[, .(count = .N), by = local_id][count > 1]
    
    if (nrow(id_duplicates) > 0) {
      duplicates$id_duplicates <- list(
        total_duplicate_ids = nrow(id_duplicates),
        total_affected_records = sum(id_duplicates$count),
        severity = "HIGH"  # ID duplicates are always high severity
      )
    }
    
    # Check for potential duplicates within same municipality/year
    # (same polling station number in same municipality)
    if ("nr_locvot" %in% names(geocoded_locais)) {
      within_muni_dups <- geocoded_locais[, .(
        count = .N,
        stations = paste(unique(local_id), collapse = ",")
      ), by = .(ano, cod_localidade_ibge, nr_locvot)][count > 1]
      
      if (nrow(within_muni_dups) > 0) {
        duplicates$within_municipality <- list(
          total_cases = nrow(within_muni_dups),
          total_affected_stations = sum(within_muni_dups$count),
          severity = "MEDIUM"
        )
      }
    }
    
    # Summary
    duplicates$summary <- list(
      has_coordinate_duplicates = !is.null(duplicates$coordinate_duplicates),
      has_cross_year_duplicates = !is.null(duplicates$cross_year_duplicates),
      has_same_year_duplicates = !is.null(duplicates$same_year_duplicates),
      has_id_duplicates = !is.null(duplicates$id_duplicates),
      has_within_muni_duplicates = !is.null(duplicates$within_municipality),
      total_issues = sum(c(
        !is.null(duplicates$id_duplicates),
        !is.null(duplicates$same_year_duplicates),
        !is.null(duplicates$within_municipality)
      ))  # Only count actual issues, not expected cross-year duplicates
    )
    
    return(duplicates)
    
  }, error = function(e) {
    return(list(status = "error", message = as.character(e)))
  })
}

# Copy over the unchanged functions from the original file
run_quarto_sanity_check <- function() {
  # Quarto requires filename only, not path for output_file
  output_filename <- paste0("sanity_check_", Sys.Date(), ".html")
  output_path <- file.path("output", output_filename)
  
  # Ensure output directory exists
  if (!dir.exists("output")) dir.create("output", recursive = TRUE)
  
  tryCatch({
    # Change to project root for quarto execution
    old_wd <- getwd()
    on.exit(setwd(old_wd))
    
    # Check if we're in a targets pipeline
    if (exists("tar_read", mode = "function")) {
      # We're in targets context
      quarto::quarto_render(
        "reports/polling_station_sanity_check.qmd",
        output_file = output_filename,
        execute_dir = "."
      )
    } else {
      # Manual run - need to ensure data is available
      message("Note: Running outside targets pipeline. Ensure data is available.")
      
      # Check if data files exist
      if (!file.exists("output/geocoded_polling_stations.csv.gz")) {
        stop("Cannot find geocoded polling stations data. Run pipeline first.")
      }
      
      quarto::quarto_render(
        "reports/polling_station_sanity_check.qmd",
        output_file = output_filename,
        execute_dir = ".",
        quiet = FALSE
      )
    }
    
    # Move to output directory if needed
    if (file.exists(output_filename) && !file.exists(output_path)) {
      file.rename(output_filename, output_path)
    }
    
    return(list(
      success = TRUE,
      path = output_path,
      generated = Sys.time()
    ))
  }, error = function(e) {
    return(list(
      success = FALSE,
      error = as.character(e),
      generated = Sys.time()
    ))
  })
}

# Keep the other unchanged functions
assess_data_quality <- function(results, config) {
  assessment <- list(
    overall_status = "PASS",
    failed_checks = character(0),
    warnings = character(0),
    details = list()
  )
  
  # Check Quarto report generation
  if (!results$quarto_report$success) {
    assessment$warnings <- c(assessment$warnings, "quarto_report_failed")
    assessment$details$quarto_error <- results$quarto_report$error
  }
  
  # Check municipality counts
  if (!is.null(results$metrics$total_municipalities)) {
    muni_diff <- abs(results$metrics$total_municipalities - config$expected_municipality_count)
    if (muni_diff > config$muni_count_tolerance) {
      assessment$failed_checks <- c(assessment$failed_checks, "municipality_count")
      assessment$overall_status <- "FAIL"
      assessment$details$municipality_count <- list(
        actual = results$metrics$total_municipalities,
        expected = config$expected_municipality_count,
        difference = muni_diff
      )
    }
  }
  
  # Check panel coverage
  if (!is.null(results$metrics$panel_coverage)) {
    if (results$metrics$panel_coverage < config$alert_thresholds$panel_coverage) {
      assessment$warnings <- c(assessment$warnings, "low_panel_coverage")
      assessment$details$panel_coverage <- results$metrics$panel_coverage
    }
  }
  
  # Check geocoding coverage
  if (!is.null(results$metrics$geocoding_coverage)) {
    if (results$metrics$geocoding_coverage < config$alert_thresholds$geocoding_coverage) {
      assessment$warnings <- c(assessment$warnings, "low_geocoding_coverage")
      assessment$details$geocoding_coverage <- results$metrics$geocoding_coverage
    }
  }
  
  # Check for excessive duplicates
  if (!is.null(results$duplicate_detection$id_duplicates)) {
    assessment$failed_checks <- c(assessment$failed_checks, "duplicate_ids")
    assessment$overall_status <- "FAIL"
  }
  
  # Check same-year coordinate duplicates - distinguish between same/different addresses
  if (!is.null(results$duplicate_detection$same_year_duplicates)) {
    dup_info <- results$duplicate_detection$same_year_duplicates
    
    # Only warn if there are many different-address duplicates
    if (dup_info$diff_address_pct > 30) {
      assessment$warnings <- c(assessment$warnings, "coordinate_duplicates_diff_addresses")
      assessment$details$coordinate_duplicates_concerning <- list(
        diff_address_cases = dup_info$diff_address_cases,
        diff_address_pct = dup_info$diff_address_pct,
        message = sprintf("%d locations (%.1f%%) have multiple stations at same coordinates with DIFFERENT addresses - potential geocoding issues",
                         dup_info$diff_address_cases, dup_info$diff_address_pct)
      )
    }
    
    # Always note the non-concerning same-address duplicates
    assessment$details$coordinate_duplicates_expected <- list(
      same_address_cases = dup_info$same_address_cases,
      same_address_pct = dup_info$same_address_pct,
      message = sprintf("%d locations (%.1f%%) have multiple stations at same coordinates with SAME address - expected for large venues",
                       dup_info$same_address_cases, dup_info$same_address_pct)
    )
  }
  
  # Note cross-year duplicates (informational only)
  if (!is.null(results$duplicate_detection$cross_year_duplicates)) {
    assessment$details$location_reuse <- list(
      locations_reused = results$duplicate_detection$cross_year_duplicates$total_locations_reused,
      avg_years = results$duplicate_detection$cross_year_duplicates$avg_years_per_location,
      message = "Normal pattern - locations reused across elections"
    )
  }
  
  # Check historical trends for extreme changes
  if (!is.null(results$historical_comparison) && 
      is.list(results$historical_comparison) &&
      (is.null(results$historical_comparison$status) || 
       results$historical_comparison$status != "insufficient_data")) {
    
    extreme_periods <- 0
    for (period in names(results$historical_comparison)) {
      if (period != "summary" && is.list(results$historical_comparison[[period]])) {
        trend <- results$historical_comparison[[period]]
        if ((trend$extreme_increases + trend$extreme_decreases) > 
            config$alert_thresholds$extreme_changes) {
          extreme_periods <- extreme_periods + 1
          assessment$warnings <- c(assessment$warnings, 
                                 paste0("extreme_changes_", period))
        }
      }
    }
    
    if (extreme_periods > 0) {
      assessment$details$extreme_change_periods <- extreme_periods
    }
  }
  
  # Update overall status if there are warnings but no failures
  if (assessment$overall_status == "PASS" && length(assessment$warnings) > 0) {
    assessment$overall_status <- "PASS_WITH_WARNINGS"
  }
  
  return(assessment)
}

generate_quality_alerts <- function(results, config) {
  alerts <- character(0)
  
  # Municipality count alerts
  if (!is.null(results$metrics$total_municipalities)) {
    muni_diff <- results$metrics$total_municipalities - config$expected_municipality_count
    if (abs(muni_diff) > config$muni_count_tolerance) {
      alerts <- c(alerts, sprintf(
        "Municipality count discrepancy: %d found vs %d expected (difference: %+d)",
        results$metrics$total_municipalities, 
        config$expected_municipality_count,
        muni_diff
      ))
    }
  }
  
  # Panel coverage alerts
  if (!is.null(results$metrics$panel_coverage)) {
    if (results$metrics$panel_coverage < config$alert_thresholds$panel_coverage) {
      alerts <- c(alerts, sprintf(
        "Low panel ID coverage: %.1f%% (threshold: %.0f%%)",
        results$metrics$panel_coverage,
        config$alert_thresholds$panel_coverage
      ))
    }
  }
  
  # Geocoding coverage alerts
  if (!is.null(results$metrics$geocoding_coverage)) {
    if (results$metrics$geocoding_coverage < config$alert_thresholds$geocoding_coverage) {
      alerts <- c(alerts, sprintf(
        "Low geocoding coverage: %.1f%% (threshold: %.0f%%)",
        results$metrics$geocoding_coverage,
        config$alert_thresholds$geocoding_coverage
      ))
    }
  }
  
  # Duplicate alerts
  if (!is.null(results$duplicate_detection$id_duplicates)) {
    alerts <- c(alerts, sprintf(
      "Found %d duplicate polling station IDs affecting %d records",
      results$duplicate_detection$id_duplicates$total_duplicate_ids,
      results$duplicate_detection$id_duplicates$total_affected_records
    ))
  }
  
  # Same-year coordinate duplicates - only alert on concerning ones
  if (!is.null(results$duplicate_detection$same_year_duplicates)) {
    dup_info <- results$duplicate_detection$same_year_duplicates
    
    # Alert on different-address duplicates if significant
    if (dup_info$diff_address_pct > 30) {
      alerts <- c(alerts, sprintf(
        "Found %d locations (%.1f%% of coordinate duplicates) where stations share coordinates but have DIFFERENT addresses - potential geocoding issues",
        dup_info$diff_address_cases,
        dup_info$diff_address_pct
      ))
    }
    
    # Info message about same-address duplicates (not an alert)
    message(sprintf(
      "INFO: %d locations (%.1f%%) have multiple stations at same coordinates with SAME address - this is expected behavior for large venues",
      dup_info$same_address_cases,
      dup_info$same_address_pct
    ))
  }
  
  # Cross-year duplicates (informational only - this is expected)
  if (!is.null(results$duplicate_detection$cross_year_duplicates)) {
    # This is expected behavior, so we might want to make it informational
    message(sprintf(
      "INFO: %d locations reused across multiple elections (avg %.1f years per location) - this is expected behavior",
      results$duplicate_detection$cross_year_duplicates$total_locations_reused,
      results$duplicate_detection$cross_year_duplicates$avg_years_per_location
    ))
  }
  
  # Historical trend alerts
  if (!is.null(results$historical_comparison) && 
      is.list(results$historical_comparison) &&
      (is.null(results$historical_comparison$status) || 
       results$historical_comparison$status != "insufficient_data")) {
    
    for (period in names(results$historical_comparison)) {
      if (period != "summary" && is.list(results$historical_comparison[[period]])) {
        trend <- results$historical_comparison[[period]]
        total_extreme <- trend$extreme_increases + trend$extreme_decreases
        
        if (total_extreme > config$alert_thresholds$extreme_changes) {
          alerts <- c(alerts, sprintf(
            "Period %s: %d municipalities with extreme changes (>±%d%%) - %d increases, %d decreases",
            period, total_extreme, config$extreme_change_threshold,
            trend$extreme_increases, trend$extreme_decreases
          ))
        }
      }
    }
  }
  
  return(alerts)
}