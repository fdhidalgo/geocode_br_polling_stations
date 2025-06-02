# Validation Integration Guide for Geocoding Pipeline
#
# This file shows exactly where and how to integrate join validations
# into the existing targets pipeline to prevent merge mistakes.

# STEP 1: Update _targets.R to load validation functions
# Add these lines after line 43 in _targets.R:
# source("./R/validation_join_operations.R")
# source("./R/validation_pipeline_merges.R")

# STEP 2: Add validation targets after critical merges

# Example validation targets to add to _targets.R:

validate_targets <- list(
  # After target 'cnefe22' (line 159) - Validates CNEFE merge with municipalities
  tar_target(
    name = validate_cnefe22,
    command = {
      # Check the merge performed in clean_cnefe22()
      validation_report <- attr(cnefe22, "validation_report")
      if (is.null(validation_report)) {
        warning(
          "No validation report found for cnefe22. Consider updating clean_cnefe22() to use safe_merge()"
        )
      } else {
        get_merge_validation_report(cnefe22)
      }
      cnefe22 # Pass through the data
    }
  ),

  # After target 'tsegeocoded_locais' (line 309) - Critical merge validation
  tar_target(
    name = validate_tse_merge,
    command = {
      # This merge is critical - polling stations with coordinates
      validation_report <- attr(tsegeocoded_locais, "validation_report")
      if (is.null(validation_report)) {
        stop(
          "CRITICAL: No validation report for TSE merge. Update clean_tsegeocoded_locais() to use safe_merge()"
        )
      }

      # Additional custom validation
      na_coords <- tsegeocoded_locais[is.na(latitude) | is.na(longitude), .N]
      total_rows <- nrow(tsegeocoded_locais)
      coverage <- (total_rows - na_coords) / total_rows * 100

      if (coverage < 80) {
        warning(sprintf(
          "Low coordinate coverage in TSE data: %.1f%%",
          coverage
        ))
      }

      tsegeocoded_locais
    }
  ),

  # After target 'model_data' (line 483) - Validates all string matching merges
  tar_target(
    name = validate_model_data,
    command = {
      # Check that all matches were properly joined
      required_cols <- c(
        "inep_match",
        "schools_cnefe_match",
        "tse_match",
        "cnefe22_match",
        "area_muni",
        "pib_pc"
      )

      missing_cols <- setdiff(required_cols, names(model_data))
      if (length(missing_cols) > 0) {
        stop(sprintf(
          "Model data missing required columns: %s",
          paste(missing_cols, collapse = ", ")
        ))
      }

      # Check join completeness
      na_summary <- model_data[,
        lapply(.SD, function(x) sum(is.na(x))),
        .SDcols = required_cols
      ]
      print("NA summary for model data columns:")
      print(na_summary)

      model_data
    }
  ),

  # After target 'geocoded_locais' (line 500) - Final validation
  tar_target(
    name = validate_final_geocoding,
    command = {
      # This is the final output - must be thoroughly validated

      # 1. Check coordinate coverage
      total_locais <- nrow(geocoded_locais)
      geocoded_count <- geocoded_locais[
        !is.na(latitude) & !is.na(longitude),
        .N
      ]
      coverage_pct <- geocoded_count / total_locais * 100

      message(sprintf(
        "Final geocoding coverage: %.1f%% (%d of %d locations)",
        coverage_pct,
        geocoded_count,
        total_locais
      ))

      if (coverage_pct < 70) {
        stop("Geocoding coverage below 70% threshold")
      }

      # 2. Check for duplicates
      dup_check <- geocoded_locais[, .(n = .N), by = local_id][n > 1]
      if (nrow(dup_check) > 0) {
        stop(sprintf(
          "Found %d duplicate local_ids in final output",
          nrow(dup_check)
        ))
      }

      # 3. Validate coordinates are within Brazil
      invalid_coords <- geocoded_locais[
        !is.na(latitude) &
          !is.na(longitude) &
          (latitude < -33.75 |
            latitude > 5.27 |
            longitude < -73.99 |
            longitude > -34.79),
        .N
      ]

      if (invalid_coords > 0) {
        warning(sprintf(
          "%d locations have coordinates outside Brazil",
          invalid_coords
        ))
      }

      # 4. Check panel ID consistency
      if ("panel_id" %in% names(geocoded_locais)) {
        panel_years <- geocoded_locais[
          !is.na(panel_id),
          .(years = uniqueN(ano)),
          by = panel_id
        ]
        single_year_panels <- panel_years[years == 1, .N]
        if (single_year_panels > 0) {
          warning(sprintf(
            "%d panel IDs appear in only one year",
            single_year_panels
          ))
        }
      }

      geocoded_locais
    }
  ),

  # After target 'panel_ids' (line 366) - Panel ID validation
  tar_target(
    name = validate_panel_ids,
    command = {
      # Check panel ID uniqueness within years
      panel_year_dups <- panel_ids[, .(n = .N), by = .(ano, panel_id)][n > 1]
      if (nrow(panel_year_dups) > 0) {
        stop(sprintf(
          "Panel ID validation failed: %d duplicate panel_id-year combinations",
          nrow(panel_year_dups)
        ))
      }

      # Check that panel IDs track locations properly across time
      panel_summary <- panel_ids[,
        .(
          n_years = uniqueN(ano),
          n_locations = uniqueN(local_id),
          years = paste(sort(unique(ano)), collapse = ",")
        ),
        by = panel_id
      ]

      # Panels linking too many locations might indicate problems
      suspicious_panels <- panel_summary[n_locations > n_years]
      if (nrow(suspicious_panels) > 0) {
        warning(sprintf(
          "Found %d panel IDs linking more locations than years",
          nrow(suspicious_panels)
        ))
        print(head(suspicious_panels))
      }

      panel_ids
    }
  )
)

# STEP 3: Update existing functions to use safe_merge()

# Example for clean_cnefe22() function:
clean_cnefe22_with_validation <- function(cnefe22_file, muni_ids) {
  # Load and process CNEFE data
  cnefe22 <- fread(cnefe22_file) %>%
    normalize_cnefe22_data()

  # Original merge:
  # cnefe22 <- merge(cnefe22, muni_ids, by = "id_munic_7", all.x = TRUE)

  # Replace with validated merge:
  cnefe22 <- safe_merge(
    dt1 = cnefe22,
    dt2 = muni_ids,
    keys = "id_munic_7",
    join_type = "many-to-one", # Many CNEFE records per municipality
    merge_type = "left",
    validate_before = TRUE,
    validate_after = TRUE,
    stop_on_error = TRUE # Critical merge - stop on errors
  )

  # Continue with rest of processing...
  return(cnefe22)
}

# Example for clean_tsegeocoded_locais() - MOST CRITICAL:
clean_tsegeocoded_locais_with_validation <- function(
  tse_files,
  muni_ids,
  locais
) {
  # Process TSE files...
  locs <- process_tse_files(tse_files)

  # Critical merge 1: Add municipality info
  locs <- safe_merge(
    dt1 = locs,
    dt2 = muni_ids[, .(cd_municipio = id_TSE, id_munic_7, municipio)],
    keys = "cd_municipio",
    join_type = "many-to-one",
    merge_type = "left",
    stop_on_error = TRUE
  )

  # Critical merge 2: Match with locais data (composite keys!)
  locs <- safe_merge(
    dt1 = locais,
    dt2 = locs,
    keys = c("ano", "cod_localidade_ibge", "nr_zona", "nr_locvot"),
    join_type = "one-to-one", # Should be unique match
    merge_type = "left",
    validate_before = TRUE,
    validate_after = TRUE,
    stop_on_error = TRUE
  )

  # Validate merge completeness
  match_rate <- locs[!is.na(latitude), .N] / nrow(locs)
  message(sprintf("TSE geocoding match rate: %.1f%%", match_rate * 100))

  if (match_rate < 0.5) {
    warning("Low TSE geocoding match rate - check composite keys")
  }

  return(locs)
}

# STEP 4: Add validation checkpoints in _targets.R

# Insert validation dependencies in critical targets:
# Example - modify the geocoded_locais target to depend on validations:
tar_target(
  name = geocoded_locais,
  command = finalize_coords(
    locais,
    model_predictions,
    tsegeocoded_locais
  ),
  # Add validation dependencies to ensure they run first
  # (This is implicit in targets, but makes it clear)
  format = "fst_dt"
)

# STEP 5: Create validation summary report

tar_target(
  name = validation_summary,
  command = {
    # Collect all validation results
    validations <- list(
      cnefe22 = validate_cnefe22,
      tse_merge = validate_tse_merge,
      model_data = validate_model_data,
      panel_ids = validate_panel_ids,
      final_geocoding = validate_final_geocoding
    )

    # Create summary report
    report <- data.table(
      validation = names(validations),
      timestamp = Sys.time(),
      status = "passed" # Update based on actual results
    )

    # Save report
    fwrite(report, "output/validation_summary.csv")

    message("All validations completed. See output/validation_summary.csv")
    report
  }
)

# USAGE NOTES:
# 1. Add validation functions to _targets.R as shown above
# 2. Update existing merge operations to use safe_merge()
# 3. Run pipeline: targets::tar_make()
# 4. Check validation reports: targets::tar_read(validation_summary)
# 5. For detailed merge reports: get_merge_validation_report(data_object)

# PRIORITY ORDER FOR IMPLEMENTATION:
# 1. tsegeocoded_locais - Most critical, coordinates assignment
# 2. geocoded_locais - Final output validation
# 3. panel_ids - Temporal tracking validation
# 4. model_data - Training data completeness
# 5. cnefe merges - Data enrichment validation
