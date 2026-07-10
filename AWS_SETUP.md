# AWS S3 Multi-Computer Setup Guide

This guide helps you set up the geocode_br_polling_stations project on a new computer when using AWS S3 storage for targets.

## Overview

The project uses a hybrid storage strategy:
- **Input file targets**: Local storage (`repository = "local"`) for tracking data files
- **Data targets**: S3 storage for computed objects (models, processed data)  
- **Output data targets**: S3 storage, return file paths as strings
- Only production mode uses S3 (DEV_MODE remains fully local)
- Versioning enabled for milestone tracking

## Complete Setup on New Computer

### 1. Install System Dependencies

```bash
# Install AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# Install R (if not installed)
# Follow instructions for your OS from https://cran.r-project.org/
```

### 2. Configure AWS Credentials

```bash
aws configure
# Enter your AWS Access Key ID
# Enter your AWS Secret Access Key  
# Enter default region (e.g., us-east-1)
# Enter default output format: json

# Test access to the bucket
aws s3 ls s3://geocode-br-polling-stations/
```

### 3. Clone and Setup Project

```bash
git clone <your-repo-url>
cd geocode_br_polling_stations

# Restore R environment (this may take 10-15 minutes)
R -e "renv::restore()"

# Install AWS integration package
R -e "install.packages('paws.storage')"
```

### 3.5. Handle Input Data Files

**Important**: The project uses a hybrid storage strategy where input data files (with `format = "file"`) must exist locally on each computer, while computed targets are stored in S3.

**Required input files**:
- `data/muni_identifiers.csv`
- `data/inep_codes.csv`
- `data/census_tracts2010_shp.rds`
- Additional CNEFE and polling station data files


### 4. Download Pipeline State from S3

```bash
# Download latest metadata from S3
R -e "targets::tar_meta_download()"

# Check what targets are available
R -e "targets::tar_visnetwork()"
R -e "targets::tar_progress()"
```

### 5. Verify Setup

```bash
# Test reading a target from S3 (this should work without local files)
R -e "
if ('geocoded_locais' %in% targets::tar_manifest()$name) {
  data <- targets::tar_read(geocoded_locais)
  cat('✓ Successfully read target from S3. Rows:', nrow(data), '\n')
} else {
  cat('ℹ Target not yet available. Run pipeline first.\n')
}
"
```

## Troubleshooting

- **AWS credentials**: Run `aws sts get-caller-identity` to verify
- **Bucket access**: Check bucket permissions if `tar_meta_download()` fails
- **Package issues**: Run `renv::status()` and `renv::restore()` if packages are missing
- **Large downloads**: Initial `tar_meta_download()` may be slow depending on pipeline size
- **File target errors**: Input files are stored locally - ensure all required data files exist in `data/` directory before running pipeline
- **Mixed storage**: Don't worry if you see both local and S3 targets - this is by design!

## Regular Workflow

```bash
# Production mode (uses S3 automatically)
R -e "targets::tar_make()"

# Development mode (local storage, AC/RR subset, never touches S3)
# Selected by the TAR_PROJECT env var — no file edits needed
TAR_PROJECT=dev R -e "targets::tar_make()"

# Read targets from S3 (production data)
R -e "data <- targets::tar_read(geocoded_locais)"

# Download specific workspace for debugging
R -e "targets::tar_workspace_download('target_name'); targets::tar_workspace('target_name')"
```

## Version Management

```bash
# Before major changes, commit metadata for versioning
git add _targets/meta/meta
git commit -m "Milestone: pre-analysis snapshot"

# Roll back to previous version
git checkout previous-commit -- _targets/meta/meta
R -e "targets::tar_meta_upload()"  # Sync rolled-back metadata to S3
```