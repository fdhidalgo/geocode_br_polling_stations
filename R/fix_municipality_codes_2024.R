# Fix Municipality Codes in 2024 Data
# Author: Data Quality Fix
# Date: 2025-01-10
# Purpose: Convert TSE codes to IBGE codes in 2024 polling station data

library(data.table)
library(stringr)

#' Convert TSE municipality codes to IBGE codes
#' 
#' @param dt_2024 data.table with 2024 polling station data
#' @param verbose logical, print progress messages
#' @return data.table with fixed municipality codes
fix_municipality_codes_2024 <- function(dt_2024, verbose = TRUE) {
  
  if (verbose) cat("Fixing municipality codes in 2024 data...\n")
  
  # Load municipality identifier mapping
  # Handle both regular execution and test execution contexts
  if (file.exists("data/muni_identifiers.csv")) {
    muni_map <- fread("data/muni_identifiers.csv", encoding = "UTF-8")
  } else {
    # Try from project root (for tests)
    project_root <- rprojroot::find_root(rprojroot::is_rstudio_project)
    muni_map <- fread(file.path(project_root, "data", "muni_identifiers.csv"), encoding = "UTF-8")
  }
  
  # Create TSE to IBGE mapping
  tse_to_ibge <- muni_map[existe == 1, .(
    id_TSE = as.integer(id_TSE),
    id_munic_7 = as.integer(id_munic_7),
    municipio_nome = municipio,
    estado_abrev
  )]
  
  if (verbose) {
    cat(sprintf("  - Loaded mapping for %d municipalities\n", nrow(tse_to_ibge)))
  }
  
  # Check current situation
  # Determine which column we're working with
  muni_col <- if ("CD_MUNICIPIO" %in% names(dt_2024)) "CD_MUNICIPIO" else "cd_localidade_tse"
  original_codes <- unique(dt_2024[[muni_col]])
  if (verbose) {
    cat(sprintf("  - Original unique municipality codes: %d\n", length(original_codes)))
    cat(sprintf("  - Code range: %d - %d\n", min(original_codes), max(original_codes)))
  }
  
  # Create a copy to preserve original
  dt_fixed <- copy(dt_2024)
  
  # Add row number to preserve order
  dt_fixed[, .row_order := .I]
  
  # Determine which column names we're working with
  # Handle both original TSE format (CD_MUNICIPIO) and cleaned pipeline format (cd_localidade_tse)
  muni_col <- if ("CD_MUNICIPIO" %in% names(dt_fixed)) "CD_MUNICIPIO" else "cd_localidade_tse"
  uf_col <- if ("SG_UF" %in% names(dt_fixed)) "SG_UF" else "sg_uf"
  
  # Add IBGE code column by merging with mapping
  dt_fixed <- merge(
    dt_fixed,
    tse_to_ibge,
    by.x = c(muni_col, uf_col),
    by.y = c("id_TSE", "estado_abrev"),
    all.x = TRUE
  )
  
  # Restore original order
  setorder(dt_fixed, .row_order)
  dt_fixed[, .row_order := NULL]
  
  # Check merge results
  unmatched <- dt_fixed[is.na(id_munic_7)]
  if (nrow(unmatched) > 0 && verbose) {
    cat(sprintf("  - WARNING: %d records could not be matched to IBGE codes\n", nrow(unmatched)))
    # Handle different possible municipality name columns
    nm_col <- if ("NM_MUNICIPIO" %in% names(unmatched)) "NM_MUNICIPIO" else "nm_localidade"
    unmatched_summary <- unmatched[, .(count = .N), by = c(muni_col, uf_col, nm_col)][order(-count)]
    cat("  - Top unmatched municipalities:\n")
    print(head(unmatched_summary, 10))
  }
  
  # Replace municipality code with IBGE code
  if (muni_col == "CD_MUNICIPIO") {
    dt_fixed[, CD_MUNICIPIO_TSE := CD_MUNICIPIO]  # Keep original for reference
    dt_fixed[!is.na(id_munic_7), CD_MUNICIPIO := id_munic_7]
  } else {
    # For pipeline data, return the IBGE code in the expected column
    dt_fixed[!is.na(id_munic_7), cod_localidade_ibge := id_munic_7]
    # Keep track of original TSE code
    dt_fixed[, cd_localidade_tse_original := get(muni_col)]
  }
  
  dt_fixed[, id_munic_7 := NULL]  # Remove temporary column
  dt_fixed[, municipio_nome := NULL]  # Remove temporary column
  
  # Validate the fix
  final_col <- if (muni_col == "CD_MUNICIPIO") "CD_MUNICIPIO" else "cod_localidade_ibge"
  if (final_col %in% names(dt_fixed)) {
    fixed_codes <- unique(dt_fixed[[final_col]])
    ibge_codes <- fixed_codes[fixed_codes >= 1000000 & fixed_codes <= 9999999]
    
    if (verbose) {
      cat("\nAfter fix:\n")
      cat(sprintf("  - Unique municipality codes: %d\n", length(fixed_codes)))
      cat(sprintf("  - Valid IBGE codes (7 digits): %d\n", length(ibge_codes)))
      
      # Count conversions
      if (muni_col == "CD_MUNICIPIO") {
        converted <- sum(dt_fixed$CD_MUNICIPIO != dt_fixed$CD_MUNICIPIO_TSE, na.rm = TRUE)
      } else {
        converted <- sum(!is.na(dt_fixed$cod_localidade_ibge))
      }
      cat(sprintf("  - Successfully converted: %d records\n", converted))
      
      # Check MT specifically
      mt_fixed <- dt_fixed[get(uf_col) == "MT"]
      if (final_col %in% names(mt_fixed)) {
        mt_codes_fixed <- unique(mt_fixed[[final_col]])
        mt_in_range <- sum(mt_codes_fixed >= 5100000 & mt_codes_fixed <= 5199999, na.rm = TRUE)
        cat(sprintf("\n  - Montana (MT) codes in correct range (51xxxxx): %d/%d\n", 
                    mt_in_range, length(mt_codes_fixed)))
      }
    }
  }
  
  return(dt_fixed)
}

#' Validate municipality code fixes
#' 
#' @param dt_original Original data
#' @param dt_fixed Fixed data
#' @return list with validation results
validate_municipality_code_fixes <- function(dt_original, dt_fixed) {
  
  validation <- list()
  
  # Check record count preserved
  validation$records_preserved <- nrow(dt_original) == nrow(dt_fixed)
  
  # Check code conversion success rate
  validation$total_records <- nrow(dt_fixed)
  validation$records_with_tse_code <- sum(!is.na(dt_fixed$CD_MUNICIPIO_TSE))
  validation$records_converted <- sum(dt_fixed$CD_MUNICIPIO != dt_fixed$CD_MUNICIPIO_TSE, na.rm = TRUE)
  validation$conversion_rate <- validation$records_converted / validation$records_with_tse_code * 100
  
  # Check IBGE code validity
  ibge_codes <- dt_fixed$CD_MUNICIPIO
  validation$valid_ibge_codes <- sum(ibge_codes >= 1000000 & ibge_codes <= 9999999, na.rm = TRUE)
  validation$invalid_codes <- sum(ibge_codes < 1000000 | ibge_codes > 9999999, na.rm = TRUE)
  
  # State-specific validation
  state_validation <- dt_fixed[, .(
    total = .N,
    converted = sum(CD_MUNICIPIO != CD_MUNICIPIO_TSE, na.rm = TRUE),
    valid_ibge = sum(CD_MUNICIPIO >= 1000000 & CD_MUNICIPIO <= 9999999, na.rm = TRUE)
  ), by = SG_UF]
  state_validation[, conversion_rate := converted / total * 100]
  validation$by_state <- state_validation
  
  return(validation)
}

#' Generate data quality fix report
#' 
#' @param dt_original Original data
#' @param dt_fixed Fixed data
#' @param validation Validation results
#' @param output_dir Output directory for report
generate_data_quality_fix_report <- function(dt_original, dt_fixed, validation, 
                                            output_dir = "output/data_quality") {
  
  # Create output directory if needed
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }
  
  # Generate report content
  report <- c(
    "# Data Quality Fix Report: 2024 Municipality Codes",
    paste0("Generated: ", Sys.Date()),
    "",
    "## Summary",
    "",
    sprintf("- Total records processed: %s", format(validation$total_records, big.mark = ",")),
    sprintf("- Records successfully converted: %s (%.1f%%)", 
            format(validation$records_converted, big.mark = ","),
            validation$conversion_rate),
    sprintf("- Valid IBGE codes after fix: %s", format(validation$valid_ibge_codes, big.mark = ",")),
    sprintf("- Invalid/unmatched codes: %s", format(validation$invalid_codes, big.mark = ",")),
    "",
    "## Issue Identified",
    "",
    "The 2024 polling station data uses TSE (Electoral Court) municipality codes instead of standard IBGE codes.",
    "This caused:",
    "- Municipality codes outside expected ranges (e.g., MT codes not in 51xxxxx range)",
    "- Inability to match with other datasets using IBGE codes",
    "",
    "## Fix Applied",
    "",
    "Converted TSE codes to IBGE codes using official municipality identifier mapping.",
    "",
    "## State-by-State Results",
    "",
    "```"
  )
  
  # Add state validation table
  report <- c(report,
    capture.output(print(validation$by_state[order(-conversion_rate)])),
    "```",
    "",
    "## Recommendations",
    "",
    "1. Update data import pipeline to handle TSE codes in future years",
    "2. Add validation check for municipality code format",
    "3. Document this code system change in data dictionary"
  )
  
  # Save report
  report_file <- file.path(output_dir, "municipality_code_fix_report.md")
  writeLines(report, report_file)
  
  # Save detailed results
  fwrite(validation$by_state, file.path(output_dir, "municipality_code_fix_by_state.csv"))
  
  cat(sprintf("\nReport saved to: %s\n", report_file))
}

# Example usage in pipeline
if (FALSE) {
  # Load 2024 data
  dt_2024 <- fread("data/eleitorado_local_votacao_2024.csv.gz")
  
  # Apply fixes
  dt_fixed <- fix_municipality_codes_2024(dt_2024)
  
  # Validate
  validation <- validate_municipality_code_fixes(dt_2024, dt_fixed)
  
  # Generate report
  generate_data_quality_fix_report(dt_2024, dt_fixed, validation)
  
  # Save fixed data
  fwrite(dt_fixed, "data/processed/eleitorado_local_votacao_2024_fixed.csv.gz")
}