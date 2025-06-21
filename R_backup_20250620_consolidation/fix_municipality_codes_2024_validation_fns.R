# Archive: Validation and Reporting Functions for 2024 Municipality Code Fix
# Date archived: 2025-06-21
# These functions were used for one-time analysis and validation of the TSE to IBGE code conversion
# They are not part of the main pipeline but preserved here for potential future use

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