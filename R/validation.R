## Data-quality validation and release gates for the geocoding pipeline.

library(validate)
library(data.table)
library(knitr)

# Confronts merged data with row-count, duplicate, and merge-key rules.
validate_merge_stage <- function(
  merged_data,
  left_data,
  right_data = NULL,
  stage_name,
  merge_keys,
  join_type = "left"
) {
  base_rules <- list(
    has_rows = quote(nrow(.) > 0),
    no_complete_duplicates = quote(nrow(.) == nrow(unique(.)))
  )

  if (!is.null(merge_keys)) {
    for (key in merge_keys) {
      base_rules[[paste0("has_key_", key)]] <- parse(
        text = sprintf(
          "'%s' %%in%% names(.)",
          key
        )
      )[[1]]
    }
  }

  rules <- do.call(validator, base_rules)

  result <- confront(merged_data, rules)

  metadata <- list(
    stage = stage_name,
    type = "merge",
    timestamp = Sys.time(),
    n_rows = nrow(merged_data),
    n_cols = ncol(merged_data)
  )

  structure(
    list(
      result = result,
      metadata = metadata,
      stage = stage_name,
      passed = all(result)
    ),
    class = "validation_result"
  )
}

# Confronts model predictions with column-presence, type, and probability-range rules.
validate_prediction_stage <- function(predictions, stage_name, pred_col = "prediction", prob_col = NULL) {
  base_rules <- list(
    has_rows = quote(nrow(.) > 0),
    has_pred_col = parse(text = sprintf("'%s' %%in%% names(.)", pred_col))[[1]]
  )

  if (pred_col %in% names(predictions)) {
    base_rules$predictions_numeric <- parse(
      text = sprintf(
        "is.numeric(.[[%s]])",
        deparse(pred_col)
      )
    )[[1]]
    base_rules$predictions_valid <- parse(
      text = sprintf(
        "all(!is.na(.[[%s]]))",
        deparse(pred_col)
      )
    )[[1]]
  }

  if (!is.null(prob_col)) {
    base_rules$has_prob_col <- parse(
      text = sprintf(
        "'%s' %%in%% names(.)",
        prob_col
      )
    )[[1]]
    if (prob_col %in% names(predictions)) {
      base_rules$probs_valid <- parse(
        text = sprintf(
          "all(.[[%s]] >= 0 & .[[%s]] <= 1, na.rm = TRUE)",
          deparse(prob_col),
          deparse(prob_col)
        )
      )[[1]]
    }
  }

  rules <- do.call(validator, base_rules)

  result <- confront(predictions, rules)

  if (pred_col %in% names(predictions)) {
    n_predictions <- sum(!is.na(predictions[[pred_col]]))
    prediction_rate <- n_predictions / nrow(predictions)
  } else if ("long" %in% names(predictions) && "lat" %in% names(predictions)) {
    n_predictions <- sum(!is.na(predictions$long) | !is.na(predictions$lat))
    prediction_rate <- n_predictions / nrow(predictions)
  } else {
    n_predictions <- 0
    prediction_rate <- 0
  }

  metadata <- list(
    stage = stage_name,
    type = "prediction",
    timestamp = Sys.time(),
    n_rows = nrow(predictions),
    n_predictions = n_predictions,
    prediction_rate = prediction_rate,
    pred_col = pred_col,
    prob_col = prob_col
  )

  structure(
    list(
      result = result,
      metadata = metadata,
      stage = stage_name,
      passed = all(result)
    ),
    class = "validation_result"
  )
}

# Confronts final output with required-column, coordinate, and key-uniqueness rules.
validate_output_stage <- function(output_data, stage_name, required_cols = NULL, unique_keys = NULL) {
  if (is.null(required_cols)) {
    required_cols <- c("local_id", "ano", "nr_zona", "nr_locvot")
  }

  base_rules <- list(
    has_rows = quote(nrow(.) > 0),
    has_coordinates = quote(any(c("final_long", "final_lat", "pred_long", "pred_lat") %in% names(.)))
  )

  for (col in required_cols) {
    rule_name <- paste0("has_", col)
    base_rules[[rule_name]] <- substitute(col %in% names(.), list(col = col))
  }

  rules <- do.call(validator, base_rules)

  result <- confront(output_data, rules)

  coord_cols <- intersect(c("final_long", "final_lat", "pred_long", "pred_lat"), names(output_data))
  n_geocoded <- 0

  if ("final_long" %in% names(output_data) && "final_lat" %in% names(output_data)) {
    n_geocoded <- sum(!is.na(output_data$final_long) & !is.na(output_data$final_lat))
  } else if ("pred_long" %in% names(output_data) && "pred_lat" %in% names(output_data)) {
    n_geocoded <- sum(!is.na(output_data$pred_long) & !is.na(output_data$pred_lat))
  }

  geocoding_rate <- n_geocoded / nrow(output_data)

  metadata <- list(
    stage = stage_name,
    type = "output",
    timestamp = Sys.time(),
    n_rows = nrow(output_data),
    n_cols = ncol(output_data),
    n_geocoded = n_geocoded,
    geocoding_rate = geocoding_rate,
    coordinate_columns = coord_cols
  )

  if (!is.null(unique_keys) && all(unique_keys %in% names(output_data))) {
    n_unique <- nrow(unique(output_data[, ..unique_keys]))
    metadata$n_unique_keys <- n_unique
    metadata$duplicate_keys <- nrow(output_data) - n_unique

    if (metadata$duplicate_keys > 0) {
      warning(paste("Found", metadata$duplicate_keys, "duplicate key combinations"))
    }
  }

  summary_df <- summary(result)
  passed <- all(summary_df$fails == 0, na.rm = TRUE)

  if (!is.null(unique_keys) && metadata$duplicate_keys > 0) {
    passed <- FALSE
  }

  structure(
    list(
      result = result,
      metadata = metadata,
      stage = stage_name,
      passed = passed
    ),
    class = "validation_result"
  )
}

# Validates a merge and warns with a caller-supplied message on failure.
validate_merge_simple <- function(
  merged_data,
  left_data,
  stage_name,
  merge_keys,
  join_type = "left_many",
  warning_message = NULL
) {
  result <- validate_merge_stage(
    merged_data = merged_data,
    left_data = left_data,
    right_data = NULL, # Multiple sources merged
    stage_name = stage_name,
    merge_keys = merge_keys,
    join_type = join_type
  )

  if (!result$passed && !is.null(warning_message)) {
    warning(warning_message)
  }

  return(result)
}

# Validates model predictions and stops the pipeline on failure.
validate_predictions_simple <- function(
  predictions,
  stage_name = "model_predictions",
  pred_col = "pred_dist",
  stop_on_failure = TRUE
) {
  result <- validate_prediction_stage(
    predictions = predictions,
    stage_name = stage_name,
    pred_col = pred_col,
    prob_col = NULL # No probability column
  )

  if (!result$passed && stop_on_failure) {
    stop("Model predictions validation failed")
  }

  return(result)
}

# Validates the final geocoded output and stops before export on failure.
validate_final_output <- function(
  output_data,
  stage_name = "geocoded_locais",
  required_cols,
  unique_keys,
  stop_on_failure = TRUE
) {
  result <- validate_output_stage(
    output_data = output_data,
    stage_name = stage_name,
    required_cols = required_cols,
    unique_keys = unique_keys
  )

  if (!result$passed && stop_on_failure) {
    stop("Final output validation failed - do not export!")
  }

  message(sprintf(
    "Geocoding complete: %d polling stations geocoded",
    nrow(output_data)
  ))

  return(result)
}

# Checks municipality, INEP, and polling-station row counts against expected ranges.
validate_inputs_consolidated <- function(muni_ids, inep_codes, locais_filtered, pipeline_config) {
  # Define expected sizes based on mode, for the datasets that dev mode filters.
  expected_sizes <- if (pipeline_config$dev_mode) {
    list(
      muni_ids = list(min = 30, max = 100, name = "municipalities"),
      locais = list(min = 1000, max = 20000, name = "polling stations")
    )
  } else {
    list(
      muni_ids = list(min = 5000, max = 6000, name = "municipalities"),
      locais = list(min = 100000, max = 1000000, name = "polling stations")
    )
  }
  # inep_codes is never filtered by dev mode, so its expected range is always national.
  expected_sizes$inep_codes <- list(min = 100000, max = 300000, name = "INEP schools")

  checks <- list()
  messages <- list()
  all_passed <- TRUE

  muni_count <- nrow(muni_ids)
  checks$muni_ids_size <- muni_count >= expected_sizes$muni_ids$min &&
    muni_count <= expected_sizes$muni_ids$max
  messages$muni_ids <- sprintf(
    "%s: %d (expected %d-%d)",
    expected_sizes$muni_ids$name,
    muni_count,
    expected_sizes$muni_ids$min,
    expected_sizes$muni_ids$max
  )
  all_passed <- all_passed && checks$muni_ids_size

  inep_count <- nrow(inep_codes)
  checks$inep_codes_size <- inep_count >= expected_sizes$inep_codes$min &&
    inep_count <= expected_sizes$inep_codes$max
  messages$inep_codes <- sprintf(
    "%s: %d (expected %d-%d)",
    expected_sizes$inep_codes$name,
    inep_count,
    expected_sizes$inep_codes$min,
    expected_sizes$inep_codes$max
  )
  all_passed <- all_passed && checks$inep_codes_size

  locais_count <- nrow(locais_filtered)
  checks$locais_size <- locais_count >= expected_sizes$locais$min &&
    locais_count <= expected_sizes$locais$max
  messages$locais <- sprintf(
    "%s: %d (expected %d-%d)",
    expected_sizes$locais$name,
    locais_count,
    expected_sizes$locais$min,
    expected_sizes$locais$max
  )
  all_passed <- all_passed && checks$locais_size

  metadata <- list(
    stage = "input_validation",
    type = "consolidated",
    timestamp = Sys.time(),
    mode = ifelse(pipeline_config$dev_mode, "DEVELOPMENT", "PRODUCTION"),
    counts = list(
      municipalities = muni_count,
      inep_schools = inep_count,
      polling_stations = locais_count
    ),
    messages = messages
  )

  cat("\n=== INPUT DATA VALIDATION ===\n")
  cat("Mode:", metadata$mode, "\n")
  for (msg in messages) {
    cat("-", msg, ifelse(grepl("expected", msg) && !all_passed, "❌", "✓"), "\n")
  }
  cat("=============================\n\n")

  validation_output <- list(
    result = NULL, # No detailed rules, just size checks
    metadata = metadata,
    passed = all_passed,
    checks = checks
  )

  class(validation_output) <- "validation_result"

  # Fail loud: a warning here would let the pipeline continue on inputs that failed validation.
  if (!all_passed) {
    failed <- names(checks)[!unlist(checks)]
    stop(sprintf(
      "Input data validation failed for: %s.\n%s",
      paste(failed, collapse = ", "),
      paste(unlist(messages), collapse = "\n")
    ))
  }

  return(validation_output)
}

# Timestamped file names (rds, summary, details) for one validation report.
get_report_output_files <- function(base_name = "validation_report") {
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")

  list(
    rds = paste0(base_name, "_", timestamp, ".rds"),
    summary = paste0(base_name, "_", timestamp, "_summary.txt"),
    details = paste0(base_name, "_", timestamp, "_details.csv")
  )
}

# Writes validation results to an rds plus a text summary; returns the two paths.
create_validation_report_target <- function(validation_results, output_dir = "output/validation_reports") {
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

  files <- get_report_output_files()

  saveRDS(validation_results, file.path(output_dir, files$rds))

  summary_text <- capture.output({
    cat("Validation Report Summary\n")
    cat("========================\n")
    cat("Generated:", format(Sys.time()), "\n\n")

    for (stage in names(validation_results)) {
      result <- validation_results[[stage]]
      cat(sprintf("Stage: %s - %s\n", stage, ifelse(result$passed, "PASSED", "FAILED")))
    }
  })

  writeLines(summary_text, file.path(output_dir, files$summary))

  list(
    rds = file.path(output_dir, files$rds),
    summary = file.path(output_dir, files$summary)
  )
}

# create_validation_report_target() plus a per-stage metrics CSV.
create_validation_report <- function(validation_results, output_dir = "output/validation_reports") {
  report <- create_validation_report_target(validation_results, output_dir)

  if (length(validation_results) > 0) {
    metrics <- lapply(validation_results, function(x) {
      if (!is.null(x$metadata)) {
        x$metadata
      } else {
        list(stage = "unknown", passed = x$passed)
      }
    })

    metrics_df <- do.call(
      rbind,
      lapply(names(metrics), function(name) {
        data.frame(
          stage = name,
          type = metrics[[name]]$type %||% NA,
          passed = validation_results[[name]]$passed,
          n_rows = metrics[[name]]$n_rows %||% NA,
          stringsAsFactors = FALSE
        )
      })
    )

    csv_file <- file.path(output_dir, gsub("_summary.txt", "_metrics.csv", basename(report$summary)))
    write.csv(metrics_df, csv_file, row.names = FALSE)

    report$metrics <- csv_file
  }

  return(report)
}

# Writes the four critical validation results to a report and prints a console summary.
generate_validation_report_simplified <- function(
  validate_inputs,
  validate_model_data,
  validate_predictions,
  validate_geocoded_output,
  locais_filtered,
  model_data,
  model_predictions,
  geocoded_locais,
  pipeline_config
) {
  validation_results <- list(
    inputs = validate_inputs,
    model_data_merge = validate_model_data,
    model_predictions = validate_predictions,
    geocoded_output = validate_geocoded_output
  )

  data_sources <- list(
    inputs = NULL, # Input validation doesn't need data export
    model_data_merge = model_data,
    model_predictions = model_predictions,
    geocoded_output = geocoded_locais
  )

  summary_stats <- create_validation_report(
    validation_results,
    output_dir = "output/validation_reports"
  )

  saveRDS(summary_stats, "output/validation_summary.rds")

  cat("\n========== VALIDATION REPORT SUMMARY ==========\n")
  cat("Report generated:", summary_stats$rds, "\n")
  cat("Critical validations checked:", length(validation_results), "\n")
  cat("Passed:", sum(sapply(validation_results, function(x) x$passed)), "\n")
  cat("Failed:", sum(sapply(validation_results, function(x) !x$passed)), "\n")
  cat(
    "Mode:",
    ifelse(pipeline_config$dev_mode, "DEVELOPMENT", "PRODUCTION"),
    "\n"
  )

  cat("\nValidation Results:\n")
  cat("- Input data sizes:", ifelse(validation_results$inputs$passed, "✅", "❌"), "\n")
  cat("- Model data merge:", ifelse(validation_results$model_data_merge$passed, "✅", "❌"), "\n")
  cat("- Model predictions:", ifelse(validation_results$model_predictions$passed, "✅", "❌"), "\n")
  cat("- Final output:", ifelse(validation_results$geocoded_output$passed, "✅", "❌"), "\n")

  cat(
    "\nOverall status:",
    ifelse(sum(sapply(validation_results, function(x) !x$passed)) == 0, "✅ SUCCESS", "❌ FAILURES DETECTED"),
    "\n"
  )
  cat("===============================================\n\n")

  return(summary_stats)
}

# Computes coverage and duplicate-coordinate quality metrics; stops on CRITICAL status.
create_data_quality_monitor <- function(
  geocoded_export,
  panelid_export,
  geocoded_locais,
  panel_ids,
  expected_municipality_count = 5570,
  muni_count_tolerance = 50,
  extreme_change_threshold = 30,
  duplicate_coord_threshold = 10,
  near_duplicate_threshold = 50,
  near_duplicate_distance = 100,
  alert_muni_discrepancy = 100,
  alert_extreme_changes = 50,
  alert_panel_coverage = 90,
  alert_geocoding_coverage = 95,
  min_years_required = 2
) {
  cat("Running data quality monitoring...\n")

  alerts <- list()

  results <- list(
    timestamp = Sys.time(),
    geocoded_export_path = geocoded_export,
    panelid_export_path = panelid_export,
    metrics = list(
      n_geocoded = nrow(geocoded_locais),
      n_panel_ids = nrow(panel_ids),
      n_unique_stations = length(unique(geocoded_locais$local_id)),
      n_unique_panels = length(unique(panel_ids$panel_id))
    ),
    thresholds = list(
      expected_municipality_count = expected_municipality_count,
      muni_count_tolerance = muni_count_tolerance,
      extreme_change_threshold = extreme_change_threshold,
      duplicate_coord_threshold = duplicate_coord_threshold,
      near_duplicate_threshold = near_duplicate_threshold,
      near_duplicate_distance = near_duplicate_distance,
      alert_muni_discrepancy = alert_muni_discrepancy,
      alert_extreme_changes = alert_extreme_changes,
      alert_panel_coverage = alert_panel_coverage,
      alert_geocoding_coverage = alert_geocoding_coverage
    ),
    status = "OK",
    message = "Data quality monitoring completed",
    alerts = alerts
  )

  if (!file.exists(geocoded_export)) {
    results$status <- "WARNING"
    results$message <- paste("Geocoded export file not found:", geocoded_export)
    alerts <- append(alerts, "Geocoded export file not found")
  }

  if (!file.exists(panelid_export)) {
    results$status <- "WARNING"
    results$message <- paste(results$message, "\nPanel ID export file not found:", panelid_export)
    alerts <- append(alerts, "Panel ID export file not found")
  }

  if ("cd_localidade_tse" %in% names(geocoded_locais)) {
    n_municipalities <- length(unique(geocoded_locais$cd_localidade_tse))
    results$metrics$n_municipalities <- n_municipalities

    muni_diff <- abs(n_municipalities - expected_municipality_count)
    if (muni_diff > muni_count_tolerance) {
      alerts <- append(
        alerts,
        sprintf(
          "Municipality count (%d) differs from expected (%d) by %d",
          n_municipalities,
          expected_municipality_count,
          muni_diff
        )
      )
      if (muni_diff > alert_muni_discrepancy) {
        results$status <- "CRITICAL"
      } else if (results$status == "OK") {
        results$status <- "WARNING"
      }
    }
  }

  if (all(c("final_long", "final_lat") %in% names(geocoded_locais))) {
    geocoding_rate <- mean(!is.na(geocoded_locais$final_long) & !is.na(geocoded_locais$final_lat)) * 100
    results$metrics$geocoding_coverage <- geocoding_rate

    if (geocoding_rate < alert_geocoding_coverage) {
      alerts <- append(
        alerts,
        sprintf("Geocoding coverage (%.1f%%) below threshold (%.1f%%)", geocoding_rate, alert_geocoding_coverage)
      )
      if (results$status == "OK") {
        results$status <- "WARNING"
      }
    }
  }

  if ("local_id" %in% names(panel_ids) && "local_id" %in% names(geocoded_locais)) {
    n_with_panels <- sum(geocoded_locais$local_id %in% panel_ids$local_id)
    panel_coverage <- (n_with_panels / nrow(geocoded_locais)) * 100
    results$metrics$panel_coverage <- panel_coverage

    if (panel_coverage < alert_panel_coverage) {
      alerts <- append(
        alerts,
        sprintf("Panel coverage (%.1f%%) below threshold (%.1f%%)", panel_coverage, alert_panel_coverage)
      )
      if (results$status == "OK") {
        results$status <- "WARNING"
      }
    }
  }

  if (all(c("final_long", "final_lat") %in% names(geocoded_locais))) {
    coords_dt <- geocoded_locais[!is.na(final_long) & !is.na(final_lat), .(n = .N), by = .(final_long, final_lat)]
    duplicate_groups <- nrow(coords_dt[n > 1])
    results$metrics$duplicate_coord_groups <- duplicate_groups

    if (duplicate_groups > duplicate_coord_threshold) {
      alerts <- append(
        alerts,
        sprintf("Found %d duplicate coordinate groups (threshold: %d)", duplicate_groups, duplicate_coord_threshold)
      )
      if (results$status == "OK") {
        results$status <- "WARNING"
      }
    }
  }

  results$alerts <- alerts

  cat("Data quality monitoring completed.\n")
  cat("  Status:", results$status, "\n")
  cat("  Geocoded locations:", results$metrics$n_geocoded, "\n")
  cat("  Panel IDs:", results$metrics$n_panel_ids, "\n")
  if (!is.null(results$metrics$n_municipalities)) {
    cat("  Municipalities:", results$metrics$n_municipalities, "\n")
  }
  if (!is.null(results$metrics$geocoding_coverage)) {
    cat("  Geocoding coverage:", sprintf("%.1f%%", results$metrics$geocoding_coverage), "\n")
  }
  if (length(alerts) > 0) {
    cat("  Alerts:", length(alerts), "\n")
  }

  # A CRITICAL status must stop the build, not merely be recorded in the result.
  if (identical(results$status, "CRITICAL")) {
    stop(sprintf(
      "Data quality monitoring reported CRITICAL status. Alerts:\n%s",
      paste(unlist(alerts), collapse = "\n")
    ))
  }

  return(results)
}

# Release gates: fail-loud structural tripwires on the production rebuild.

# The exact geocoded_locais schema written to geocoded_polling_stations.csv.gz, in the
# order finalize_coords() produces it. The gate asserts set equality, so a dropped or
# added column trips it.
RELEASE_EXPORT_COLS <- c(
  "cd_localidade_tse",
  "ano",
  "nr_zona",
  "nr_locvot",
  "nr_cep",
  "sg_uf",
  "nm_localidade",
  "nm_locvot",
  "ds_endereco",
  "ds_bairro",
  "cod_localidade_ibge",
  "local_id",
  "pred_long",
  "pred_lat",
  "pred_dist",
  "tse_lat",
  "tse_long",
  "final_long",
  "final_lat"
)

# All election years the pipeline geocodes.
RELEASE_EXPECTED_YEARS <- seq(2006L, 2024L, by = 2L)

# Sane-scale band for the 2024 partition (2024 address count: 93,337).
RELEASE_N_2024_MIN <- 85000L
RELEASE_N_2024_MAX <- 100000L

# Plausible national band for any single year; counts sit around 90-96k per year.
RELEASE_N_YEAR_MIN <- 50000L
RELEASE_N_YEAR_MAX <- 120000L

# Landed 2024 TSE-coverage hard gate, in percent.
RELEASE_TSE_COVERAGE_GATE <- 92

# Election years for which TSE publishes field-collected coordinates. Coverage
# begins with the 2018 vintage; earlier years carry no TSE ground truth.
RELEASE_TSE_VINTAGES <- c(2018L, 2020L, 2022L, 2024L)

# Gate 7 tolerance in percentage points: observed legitimate join loss is <= 1.6 pt, while
# the merge bug this gate catches dropped ~36 pt.
RELEASE_TSE_JOIN_SLACK <- 5L

# Runs the release gates over the geocoded export; stops the build on any failure.
validate_release_gates <- function(
  geocoded_locais,
  tse_coverage,
  tse_raw_availability,
  export_paths,
  dev_mode,
  panel_gate = NULL
) {
  # panel_gate is a dependency-only argument: taking the panel_release_gates target makes
  # this check depend on it, so building release_gates also runs that gate.
  # Read-only, so skip a deep copy of the ~945k-row national table.
  dt <- if (is.data.table(geocoded_locais)) {
    geocoded_locais
  } else {
    as.data.table(geocoded_locais)
  }
  failures <- character(0)
  add_fail <- function(...) failures <<- c(failures, sprintf(...))

  # Gate 1: all election years present, with a non-empty 2024 partition.
  present_years <- sort(unique(dt$ano))
  missing_years <- setdiff(RELEASE_EXPECTED_YEARS, present_years)
  if (length(missing_years) > 0) {
    add_fail("Gate 1 (all years): missing election years: %s", paste(missing_years, collapse = ", "))
  }
  n_2024 <- dt[ano == 2024L, .N]
  if (n_2024 == 0L) {
    add_fail("Gate 1 (all years): 2024 partition is empty")
  }

  # Gate 2: exported schema exactly unchanged - no column removed or added.
  missing_cols <- setdiff(RELEASE_EXPORT_COLS, names(dt))
  extra_cols <- setdiff(names(dt), RELEASE_EXPORT_COLS)
  if (length(missing_cols) > 0) {
    add_fail("Gate 2 (schema): missing columns: %s", paste(missing_cols, collapse = ", "))
  }
  if (length(extra_cols) > 0) {
    add_fail("Gate 2 (schema): unexpected extra columns: %s", paste(extra_cols, collapse = ", "))
  }

  # Gate 3: coordinates not all-NA in any year.
  coord_by_year <- dt[,
    .(
      n = .N,
      n_coord = sum(!is.na(final_lat) & !is.na(final_long))
    ),
    by = ano
  ][order(ano)]
  zero_coord_years <- coord_by_year[n_coord == 0L, ano]
  if (length(zero_coord_years) > 0) {
    add_fail("Gate 3 (coords): years with zero non-NA coordinates: %s", paste(zero_coord_years, collapse = ", "))
  }

  # Gate 4: output files exist on disk.
  for (p in export_paths) {
    if (!file.exists(p)) {
      add_fail("Gate 4 (files): output file missing: %s", p)
    }
  }

  # Gate 5: sane per-year row counts, production only (dev mode processes only AC/RR).
  if (!isTRUE(dev_mode)) {
    if (n_2024 < RELEASE_N_2024_MIN || n_2024 > RELEASE_N_2024_MAX) {
      add_fail(
        "Gate 5 (counts): 2024 station count %d outside sane range [%d, %d]",
        n_2024,
        RELEASE_N_2024_MIN,
        RELEASE_N_2024_MAX
      )
    }
    off <- coord_by_year[
      ano != 2024L & (n < RELEASE_N_YEAR_MIN | n > RELEASE_N_YEAR_MAX)
    ]
    if (nrow(off) > 0) {
      add_fail(
        "Gate 5 (counts): years outside plausible national range [%d, %d]: %s",
        RELEASE_N_YEAR_MIN,
        RELEASE_N_YEAR_MAX,
        paste(sprintf("%d=%d", off$ano, off$n), collapse = ", ")
      )
    }
  }

  # Gates 6 & 7: landed TSE coverage, aggregated from per-year x state to per-year.
  cov <- as.data.table(tse_coverage)
  cov_year <- cov[,
    .(
      n_total = sum(n_total),
      n_covered = sum(n_covered)
    ),
    by = ano
  ]
  cov_year[, coverage_pct := 100 * n_covered / n_total]
  setorder(cov_year, ano)

  # Gate 6: landed 2024 TSE coverage >= 92% (hard gate).
  cov_2024 <- cov_year[ano == 2024L, coverage_pct]
  if (length(cov_2024) == 0) {
    add_fail("Gate 6 (2024 coverage): no 2024 rows in tse_coverage")
  } else if (cov_2024 < RELEASE_TSE_COVERAGE_GATE) {
    add_fail("Gate 6 (2024 coverage): landed 2024 TSE coverage %.2f%% < %d%% gate", cov_2024, RELEASE_TSE_COVERAGE_GATE)
  }

  # Gate 7: landed coverage must track each vintage's own raw TSE availability (~51% in 2018 to ~94% in 2024).
  raw <- as.data.table(tse_raw_availability)
  cov_vs_raw <- merge(
    cov_year[, .(ano, coverage_pct)],
    raw[, .(ano, raw_avail_pct)],
    by = "ano"
  )
  missing_vintages <- setdiff(RELEASE_TSE_VINTAGES, cov_vs_raw$ano)
  if (length(missing_vintages) > 0) {
    add_fail(
      "Gate 7 (coverage regression): TSE vintages absent from coverage/availability: %s",
      paste(missing_vintages, collapse = ", ")
    )
  }
  cov_vs_raw[, shortfall := raw_avail_pct - coverage_pct]
  regressed <- cov_vs_raw[shortfall > RELEASE_TSE_JOIN_SLACK]
  if (nrow(regressed) > 0) {
    add_fail(
      "Gate 7 (coverage regression): landed TSE coverage falls >%d pts below raw availability: %s",
      RELEASE_TSE_JOIN_SLACK,
      paste(
        sprintf("%d=%.1f%% landed vs %.1f%% raw", regressed$ano, regressed$coverage_pct, regressed$raw_avail_pct),
        collapse = ", "
      )
    )
  }

  # Gate 8: TSE-covered stations ship ground-truth coordinates, so pred_dist must be 0.
  # Column-guarded so a missing column stays Gate 2's failure rather than a raw error.
  if (all(c("tse_long", "tse_lat", "pred_dist") %in% names(dt))) {
    n_bad_preddist <- dt[
      !is.na(tse_long) & !is.na(tse_lat) & (is.na(pred_dist) | pred_dist != 0),
      .N
    ]
    if (n_bad_preddist > 0) {
      add_fail(
        "Gate 8 (pred_dist): %d TSE-covered rows have non-zero pred_dist (expected 0 for ground-truth coordinates)",
        n_bad_preddist
      )
    }
  }

  # Gate 9: the published header comes from to_geocoded_export_schema(); check it matches the 0.141 schema.
  published_cols <- tryCatch(
    names(to_geocoded_export_schema(head(dt, 1L))),
    error = function(e) character(0)
  )
  if (!identical(published_cols, GEOCODED_EXPORT_SCHEMA)) {
    add_fail(
      "Gate 9 (published schema): export mapper output does not match the 0.141 schema.\n    expected: %s\n    got: %s",
      paste(GEOCODED_EXPORT_SCHEMA, collapse = ", "),
      paste(published_cols, collapse = ", ")
    )
  }

  summary <- list(
    passed = length(failures) == 0,
    failures = failures,
    dev_mode = isTRUE(dev_mode),
    years_present = present_years,
    n_2024 = n_2024,
    coverage_by_year = cov_year[],
    coverage_vs_raw = cov_vs_raw[],
    coord_by_year = coord_by_year[]
  )

  if (length(failures) > 0) {
    stop(sprintf(
      "Release gates FAILED (release spec Validation gates) - do not ship:\n  - %s",
      paste(failures, collapse = "\n  - ")
    ))
  }
  message(sprintf(
    "Release gates PASSED: %d years present (%s); 2024 n=%d; 2024 landed TSE coverage=%.2f%%",
    length(present_years),
    paste(present_years, collapse = ","),
    n_2024,
    cov_2024
  ))
  summary
}

# Max share of panel rows allowed to ship without a coordinate. A panel lacks one only
# when every one of its station-years failed to geocode, so the real rate sits well under 1%.
RELEASE_PANEL_COORD_NA_MAX_PCT <- 1

# Release gate for panel_ids.csv.gz: stops if pred_dist is missing or too many panels lack a coordinate.
validate_panel_release <- function(panel_ids) {
  dt <- if (is.data.table(panel_ids)) panel_ids else as.data.table(panel_ids)
  failures <- character(0)
  add_fail <- function(...) failures <<- c(failures, sprintf(...))

  # Gate P1: the accuracy-filter column must reach the output.
  if (!("pred_dist" %in% names(dt))) {
    add_fail("Gate P1 (schema): panel_ids is missing the pred_dist column")
  }

  # Gate P2: coordinates present for essentially every panel.
  has_coords <- all(c("long", "lat") %in% names(dt))
  coord_na_pct <- if (has_coords) 100 * mean(is.na(dt$long) | is.na(dt$lat)) else NA_real_
  if (!has_coords) {
    add_fail("Gate P2 (coords): panel_ids is missing long/lat columns")
  } else if (coord_na_pct > RELEASE_PANEL_COORD_NA_MAX_PCT) {
    add_fail(
      "Gate P2 (coords): %.2f%% of panel rows lack a coordinate (> %g%% max) - the panel step is likely ignoring model coordinates",
      coord_na_pct,
      RELEASE_PANEL_COORD_NA_MAX_PCT
    )
  }

  summary <- list(
    passed = length(failures) == 0,
    failures = failures,
    n_rows = nrow(dt),
    coord_na_pct = coord_na_pct
  )

  if (length(failures) > 0) {
    stop(sprintf(
      "Panel release gates FAILED (do not ship):\n  - %s",
      paste(failures, collapse = "\n  - ")
    ))
  }
  message(sprintf(
    "Panel release gates PASSED: %d rows; %.2f%% without a coordinate",
    summary$n_rows,
    coord_na_pct
  ))
  summary
}
