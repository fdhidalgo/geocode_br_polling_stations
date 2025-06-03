# Functions for partitioning large data files by state

library(data.table)

#' Partition CNEFE data by state
#'
#' This function reads a full CNEFE file and splits it into state-specific
#' compressed files for more efficient processing
#'
#' @param cnefe_path Path to the full CNEFE file (.csv.gz or .gz)
#' @param year Year of the CNEFE data (2010 or 2022)
#' @param output_dir Base directory for output files
#' @param sep Field separator (default ";", use "," for CNEFE 2022)
#' @return Invisible NULL, creates files as side effect
partition_cnefe_by_state <- function(cnefe_path, 
                                     year, 
                                     output_dir = "data",
                                     sep = NULL) {
  
  # Auto-detect separator based on year if not provided
  if (is.null(sep)) {
    sep <- if (year == 2010) ";" else ","
  }
  
  # Create year-specific directory
  year_dir <- file.path(output_dir, paste0("cnefe_", year))
  dir.create(year_dir, recursive = TRUE, showWarnings = FALSE)
  
  message(sprintf("Reading CNEFE %d data from %s...", year, cnefe_path))
  
  # Read full CNEFE file
  cnefe_dt <- fread(cnefe_path, 
                    sep = sep,
                    encoding = "UTF-8",
                    showProgress = TRUE)
  
  # Standardize column names to lowercase
  setnames(cnefe_dt, names(cnefe_dt), tolower(names(cnefe_dt)))
  
  # Get unique states
  states <- unique(cnefe_dt$cod_uf)
  states <- states[!is.na(states)]
  
  message(sprintf("Found %d states in CNEFE %d data", length(states), year))
  
  # Create mapping of state codes to abbreviations
  state_map <- data.table(
    cod_uf = c(11, 12, 13, 14, 15, 16, 17, 21, 22, 23, 24, 25, 26, 27, 28, 29,
               31, 32, 33, 35, 41, 42, 43, 50, 51, 52, 53),
    uf = c("RO", "AC", "AM", "RR", "PA", "AP", "TO", "MA", "PI", "CE", "RN", "PB",
           "PE", "AL", "SE", "BA", "MG", "ES", "RJ", "SP", "PR", "SC", "RS", "MS",
           "MT", "GO", "DF")
  )
  
  # Split by state and save as compressed files
  for (state_code in states) {
    # Get state abbreviation
    state_abbr <- state_map[cod_uf == state_code, uf]
    
    if (length(state_abbr) == 0) {
      warning(sprintf("Unknown state code: %d, skipping", state_code))
      next
    }
    
    # Filter data for this state
    state_dt <- cnefe_dt[cod_uf == state_code]
    
    # Create output filename
    output_path <- file.path(year_dir, 
                             paste0("cnefe_", year, "_", state_abbr, ".csv.gz"))
    
    # Write compressed file
    fwrite(state_dt, 
           output_path, 
           encoding = "UTF-8", 
           compress = "gzip",
           sep = ",")  # Always use comma for output files
    
    message(sprintf("Saved %s records for state %s (%s)", 
                    format(nrow(state_dt), big.mark = ","), 
                    state_abbr, 
                    output_path))
  }
  
  message(sprintf("Partitioning complete. Files saved in %s", year_dir))
  invisible(NULL)
}

#' Get file size statistics for partitioned CNEFE files
#'
#' @param year Year of CNEFE data
#' @param data_dir Base data directory
#' @return Data table with file statistics
get_partition_stats <- function(year, data_dir = "data") {
  year_dir <- file.path(data_dir, paste0("cnefe_", year))
  
  if (!dir.exists(year_dir)) {
    stop(sprintf("Directory %s does not exist", year_dir))
  }
  
  files <- list.files(year_dir, pattern = "\\.csv\\.gz$", full.names = TRUE)
  
  if (length(files) == 0) {
    stop(sprintf("No .csv.gz files found in %s", year_dir))
  }
  
  stats <- data.table(
    file = basename(files),
    state = gsub(".*_([A-Z]{2})\\.csv\\.gz$", "\\1", basename(files)),
    size_mb = file.size(files) / (1024^2)
  )
  
  stats[order(size_mb)]
}

#' Verify partition integrity
#'
#' Compares row counts between original and partitioned files
#'
#' @param original_path Path to original CNEFE file
#' @param year Year of CNEFE data
#' @param data_dir Base data directory
#' @param sep Field separator for original file
#' @return Logical indicating if verification passed
verify_partition_integrity <- function(original_path, 
                                       year, 
                                       data_dir = "data",
                                       sep = NULL) {
  
  # Auto-detect separator based on year if not provided
  if (is.null(sep)) {
    sep <- if (year == 2010) ";" else ","
  }
  
  message("Counting rows in original file...")
  original_count <- fread(original_path, 
                          sep = sep,
                          select = 1L,  # Only read first column for counting
                          showProgress = TRUE)[, .N]
  
  message("Counting rows in partitioned files...")
  year_dir <- file.path(data_dir, paste0("cnefe_", year))
  files <- list.files(year_dir, pattern = "\\.csv\\.gz$", full.names = TRUE)
  
  partition_counts <- sapply(files, function(f) {
    fread(f, select = 1L)[, .N]
  })
  
  partition_total <- sum(partition_counts)
  
  message(sprintf("Original file: %s rows", format(original_count, big.mark = ",")))
  message(sprintf("Partitioned files: %s rows", format(partition_total, big.mark = ",")))
  
  if (original_count == partition_total) {
    message("✓ Verification passed: row counts match")
    return(TRUE)
  } else {
    warning(sprintf("✗ Verification failed: %d row difference", 
                    abs(original_count - partition_total)))
    return(FALSE)
  }
}