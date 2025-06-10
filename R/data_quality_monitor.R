# Data Quality Monitoring Functions
# Complements the existing polling_station_sanity_check.qmd report
# Author: Data Quality Monitoring
# Date: 2025-01-10

library(data.table)
library(yaml)
library(quarto)

#' Run comprehensive data quality monitoring
#' 
#' Wraps the existing Quarto report and adds additional checks
#' 
#' @param generate_alerts logical, whether to print alerts
#' @param config_file path to configuration YAML file
#' @return list with monitoring results
#' @export
run_data_quality_monitoring <- function(generate_alerts = TRUE,
                                      config_file = "config/data_quality_config.yaml") {
  
  # Load configuration
  config <- yaml::read_yaml(config_file)
  results <- list()
  
  message("Running data quality monitoring suite...")
  
  # 1. Run existing Quarto sanity check report
  message("Executing polling station sanity check report...")
  quarto_result <- run_quarto_sanity_check()
  results$quarto_report <- quarto_result
  
  # 2. Extract key metrics from the data directly
  message("Extracting key metrics...")
  results$metrics <- extract_key_metrics()
  
  # 3. Run additional monitoring checks
  message("Running historical comparison...")
  results$historical_comparison <- check_historical_trends(config)
  
  message("Checking for duplicates...")
  results$duplicate_detection <- check_duplicates(config)
  
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
  
  return(results)
}

#' Run the Quarto sanity check report
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

#' Extract key metrics from the data
extract_key_metrics <- function() {
  # Try to load data from targets or fallback to direct file reading
  tryCatch({
    # Try targets first
    if (exists("tar_read", mode = "function")) {
      geocoded_locais <- tar_read(geocoded_locais)
      panel_ids <- tar_read(panel_ids)
    } else {
      # Fallback to reading from files
      if (file.exists("output/geocoded_polling_stations.csv.gz")) {
        geocoded_locais <- fread("output/geocoded_polling_stations.csv.gz")
      } else {
        stop("Cannot find geocoded polling stations data")
      }
      
      if (file.exists("output/panel_ids.csv.gz")) {
        panel_ids <- fread("output/panel_ids.csv.gz")
      } else {
        panel_ids <- NULL
      }
    }
    
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
    if (!is.null(panel_ids)) {
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

#' Check historical trends across all available years
check_historical_trends <- function(config) {
  tryCatch({
    # Load geocoded data
    if (exists("tar_read", mode = "function")) {
      geocoded_locais <- tar_read(geocoded_locais)
    } else if (file.exists("output/geocoded_polling_stations.csv.gz")) {
      geocoded_locais <- fread("output/geocoded_polling_stations.csv.gz")
    } else {
      return(list(status = "no_data", message = "Cannot load geocoded data"))
    }
    
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

#' Check for duplicate polling stations
check_duplicates <- function(config) {
  tryCatch({
    # Load geocoded data
    if (exists("tar_read", mode = "function")) {
      geocoded_locais <- tar_read(geocoded_locais)
    } else if (file.exists("output/geocoded_polling_stations.csv.gz")) {
      geocoded_locais <- fread("output/geocoded_polling_stations.csv.gz")
    } else {
      return(list(status = "no_data", message = "Cannot load geocoded data"))
    }
    
    duplicates <- list()
    
    # Check for exact coordinate duplicates (same lat/long)
    coord_data <- geocoded_locais[!is.na(final_lat) & !is.na(final_long)]
    
    if (nrow(coord_data) > 0) {
      # Round coordinates to 6 decimal places (about 0.1m precision)
      coord_data[, coord_key := paste(round(final_lat, 6), round(final_long, 6), sep = "_")]
      
      # Find duplicates
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
      has_id_duplicates = !is.null(duplicates$id_duplicates),
      has_within_muni_duplicates = !is.null(duplicates$within_municipality),
      total_issues = length(duplicates) - 1  # Exclude summary itself
    )
    
    return(duplicates)
    
  }, error = function(e) {
    return(list(status = "error", message = as.character(e)))
  })
}

#' Assess overall data quality
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
  
  # Check historical trends for extreme changes
  if (!is.null(results$historical_comparison) && 
      results$historical_comparison$status != "insufficient_data") {
    
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

#' Generate quality alerts
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
  
  if (!is.null(results$duplicate_detection$coordinate_duplicates)) {
    alerts <- c(alerts, sprintf(
      "Found %d locations with duplicate coordinates affecting %d stations",
      results$duplicate_detection$coordinate_duplicates$total_duplicate_groups,
      results$duplicate_detection$coordinate_duplicates$total_duplicate_stations
    ))
  }
  
  # Historical trend alerts
  if (!is.null(results$historical_comparison) && 
      results$historical_comparison$status != "insufficient_data") {
    
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

#' Quick quality check - convenience function
#' 
#' @return invisible results list
#' @export
quick_quality_check <- function() {
  results <- run_data_quality_monitoring(generate_alerts = FALSE)
  
  # Print summary
  cat("\n📊 DATA QUALITY SUMMARY\n")
  cat("=======================\n")
  
  # Overall status
  status_symbol <- switch(results$overall_assessment$overall_status,
                         "PASS" = "✅",
                         "PASS_WITH_WARNINGS" = "⚠️",
                         "FAIL" = "❌",
                         "❓")
  
  cat(sprintf("Overall Status: %s %s\n", 
              status_symbol,
              results$overall_assessment$overall_status))
  
  # Key metrics
  if (!is.null(results$metrics)) {
    cat(sprintf("\nKey Metrics:\n"))
    cat(sprintf("  - Total Municipalities: %s (expected: ~5,570)\n", 
                format(results$metrics$total_municipalities, big.mark = ",")))
    cat(sprintf("  - Total Polling Stations: %s\n", 
                format(results$metrics$total_stations, big.mark = ",")))
    cat(sprintf("  - Years Covered: %s\n", 
                paste(results$metrics$years_covered, collapse = ", ")))
    
    if (!is.null(results$metrics$panel_coverage)) {
      cat(sprintf("  - Panel Coverage: %.1f%%\n", results$metrics$panel_coverage))
    }
    
    cat(sprintf("  - Geocoding Coverage: %.1f%%\n", results$metrics$geocoding_coverage))
  }
  
  # Specific checks
  cat("\nQuality Checks:\n")
  cat(sprintf("  - Quarto Report: %s\n",
              ifelse(results$quarto_report$success, "✅ Generated", "❌ Failed")))
  cat(sprintf("  - Municipality Counts: %s\n",
              ifelse("municipality_count" %in% results$overall_assessment$failed_checks,
                     "❌ FAIL", "✅ PASS")))
  cat(sprintf("  - Duplicate Detection: %s\n",
              ifelse("duplicate_ids" %in% results$overall_assessment$failed_checks,
                     "❌ FAIL", "✅ PASS")))
  cat(sprintf("  - Historical Trends: %s\n",
              ifelse(any(grepl("extreme_changes", results$overall_assessment$warnings)),
                     "⚠️ WARNINGS", "✅ PASS")))
  
  # Active alerts
  if (length(results$alerts) > 0) {
    cat("\n🚨 Active Alerts:\n")
    for (alert in results$alerts) {
      cat(paste0("  - ", alert, "\n"))
    }
  }
  
  cat("\n")
  
  return(invisible(results))
}