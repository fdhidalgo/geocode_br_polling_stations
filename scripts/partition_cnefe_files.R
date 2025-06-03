#!/usr/bin/env Rscript

# Script to partition CNEFE files by state
# This needs to be run once before using the state filtering in targets pipeline

library(data.table)
source("R/data_partitioning.R")

# Check if partitioned files already exist
cnefe_2010_dir <- "data/cnefe_2010"
cnefe_2022_dir <- "data/cnefe_2022"

# Partition CNEFE 2010
if (!dir.exists(cnefe_2010_dir) || length(list.files(cnefe_2010_dir, pattern = "\\.csv\\.gz$")) == 0) {
  message("\n========================================")
  message("Partitioning CNEFE 2010 data by state...")
  message("========================================")
  
  if (file.exists("data/CNEFE_combined.gz")) {
    partition_cnefe_by_state(
      cnefe_path = "data/CNEFE_combined.gz",
      year = 2010,
      output_dir = "data",
      sep = ";"
    )
    
    # Verify integrity
    message("\nVerifying CNEFE 2010 partition integrity...")
    verify_partition_integrity(
      original_path = "data/CNEFE_combined.gz",
      year = 2010,
      data_dir = "data",
      sep = ";"
    )
    
    # Show statistics
    message("\nCNEFE 2010 file statistics:")
    print(get_partition_stats(2010, "data"))
  } else {
    warning("CNEFE 2010 file not found: data/CNEFE_combined.gz")
  }
} else {
  message("CNEFE 2010 files already partitioned. Skipping...")
  message("\nExisting CNEFE 2010 file statistics:")
  print(get_partition_stats(2010, "data"))
}

# Partition CNEFE 2022
if (!dir.exists(cnefe_2022_dir) || length(list.files(cnefe_2022_dir, pattern = "\\.csv\\.gz$")) == 0) {
  message("\n========================================")
  message("Partitioning CNEFE 2022 data by state...")
  message("========================================")
  
  if (file.exists("data/cnefe22.csv.gz")) {
    partition_cnefe_by_state(
      cnefe_path = "data/cnefe22.csv.gz",
      year = 2022,
      output_dir = "data",
      sep = ","
    )
    
    # Verify integrity
    message("\nVerifying CNEFE 2022 partition integrity...")
    verify_partition_integrity(
      original_path = "data/cnefe22.csv.gz",
      year = 2022,
      data_dir = "data",
      sep = ","
    )
    
    # Show statistics
    message("\nCNEFE 2022 file statistics:")
    print(get_partition_stats(2022, "data"))
  } else {
    warning("CNEFE 2022 file not found: data/cnefe22.csv.gz")
  }
} else {
  message("CNEFE 2022 files already partitioned. Skipping...")
  message("\nExisting CNEFE 2022 file statistics:")
  print(get_partition_stats(2022, "data"))
}

message("\n========================================")
message("Partitioning complete!")
message("========================================")
message("\nTo use state filtering in the pipeline:")
message("1. Edit _targets.R")
message("2. Set DEV_MODE <- TRUE for development (AC, RR only)")
message("3. Set DEV_MODE <- FALSE for production (all states)")
message("4. Run: targets::tar_make()")

# Optional: Remove original files to save space
message("\nOriginal CNEFE files can be deleted to save space:")
message("- data/CNEFE_combined.gz (", 
        round(file.size("data/CNEFE_combined.gz") / (1024^3), 1), " GB)")
message("- data/cnefe22.csv.gz (", 
        round(file.size("data/cnefe22.csv.gz") / (1024^3), 1), " GB)")
message("\nTo delete: unlink(c('data/CNEFE_combined.gz', 'data/cnefe22.csv.gz'))")