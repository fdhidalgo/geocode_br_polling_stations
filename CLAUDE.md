# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This R project geocodes Brazilian polling stations (2006-2022) using administrative datasets and fuzzy string matching. It creates a comprehensive dataset of polling station coordinates and panel identifiers to track stations across time.

**Memory requirement**: 50GB+ RAM due to large datasets

## Key Commands

### Development Mode (IMPORTANT)
**Always use development mode when testing pipeline changes** - the full pipeline takes hours. Set `DEV_MODE = TRUE` in `_targets.R` to work with a small subset of data (AC and RR states only):

```bash
# Check if dev mode is enabled
grep "DEV_MODE" _targets.R

# Run pipeline in dev mode (fast - minutes instead of hours)
R -e "targets::tar_make()"

# Run specific targets
R -e "targets::tar_make(names = 'target_name')"

# Check pipeline status
R -e "targets::tar_visnetwork()"
```

### Full Pipeline Commands
```bash
# Install dependencies (use renv for reproducibility)
R -e "renv::restore()"

# Clean and rebuild
R -e "targets::tar_destroy()"
R -e "targets::tar_make()"
```

## Architecture

### Data Pipeline (`_targets.R`)
The project uses `targets` package for pipeline management with these stages:
1. **Data Import**: Municipal data, CNEFE census data, polling station addresses
2. **Data Cleaning**: Normalize addresses using `R/data_cleaning_fns.R`
3. **String Matching**: Fuzzy match polling stations to known coordinates using `R/string_matching_geocode_fns.R`
4. **Model Training**: Train boosted trees (lightgbm) to select best matches
5. **Panel Creation**: Create temporal identifiers using `R/panel_id_fns.R`
6. **Validation**: Validate outputs using `R/functions_validate.R`

### Key Functions
- **String Matching**: `match_inep_muni()`, `match_schools_cnefe_muni()` - fuzzy matching with Levenshtein distance
- **Panel IDs**: Fellegi-Sunter record linkage with Jaro-Winkler similarity
- **Data Cleaning**: `normalize_address()`, `normalize_names()`, `clean_cnefe_*()`
- **Parallel Processing**: Uses `future` package, configured in `_targets.R`

## Data Sources & Outputs

**External downloads required**:
- CNEFE Census data (2010, 2017, 2022)
- TSE geocoded data (ground truth)
- Census tract shapefiles (via geobr)

**Outputs**:
- `output/geocoded_polling_stations.csv.gz`: Final geocoded coordinates
- `output/panel_ids.csv.gz`: Panel identifiers linking stations across time

**AWS S3 Storage Architecture**:
- Bucket: `geocode-br-polling-stations`
- **Hybrid storage strategy**:
  - **Input file targets**: Local storage (`repository = "local"`) for tracking data files
  - **Data targets**: S3 storage for computed objects (models, processed data)  
  - **Output data targets**: S3 storage, return file paths as strings
- Only production mode uses S3 (DEV_MODE remains fully local)
- Versioning enabled for milestone tracking
- **Benefits**: Eliminates file target + S3 compatibility issues, faster input file access

### Multi-Computer Setup Guide

**Complete setup on new computer**:

1. **Install system dependencies**:
```bash
# Install AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# Install R (if not installed)
# Follow instructions for your OS from https://cran.r-project.org/
```

2. **Configure AWS credentials**:
```bash
aws configure
# Enter your AWS Access Key ID
# Enter your AWS Secret Access Key  
# Enter default region (e.g., us-east-1)
# Enter default output format: json

# Test access to the bucket
aws s3 ls s3://geocode-br-polling-stations/
```

3. **Clone and setup project**:
```bash
git clone <your-repo-url>
cd geocode_br_polling_stations

# Restore R environment (this may take 10-15 minutes)
R -e "renv::restore()"

# Install AWS integration package
R -e "install.packages('paws.storage')"
```

4. **Download pipeline state from S3**:
```bash
# Download latest metadata from S3
R -e "targets::tar_meta_download()"

# Check what targets are available
R -e "targets::tar_visnetwork()"
R -e "targets::tar_progress()"
```

5. **Verify setup**:
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

**Troubleshooting**:
- **AWS credentials**: Run `aws sts get-caller-identity` to verify
- **Bucket access**: Check bucket permissions if `tar_meta_download()` fails
- **Package issues**: Run `renv::status()` and `renv::restore()` if packages are missing
- **Large downloads**: Initial `tar_meta_download()` may be slow depending on pipeline size
- **File target errors**: Input files are stored locally - ensure data files exist before running pipeline
- **Mixed storage**: Don't worry if you see both local and S3 targets - this is by design!

**Regular workflow**:
```bash
# Production mode (uses S3 automatically)
R -e "targets::tar_make()"

# Development mode (local storage)
# Change dev_mode_flag to TRUE in _targets.R first
R -e "targets::tar_make()"

# Read targets from S3 (production data)
R -e "data <- targets::tar_read(geocoded_locais)"

# Download specific workspace for debugging
R -e "targets::tar_workspace_download('target_name'); targets::tar_workspace('target_name')"
```

**Version management**:
```bash
# Before major changes, commit metadata for versioning
git add _targets/meta/meta
git commit -m "Milestone: pre-analysis snapshot"

# Roll back to previous version
git checkout previous-commit -- _targets/meta/meta
R -e "targets::tar_meta_upload()"  # Sync rolled-back metadata to S3
```

## Development Guidelines

### Core Stack
- **Data**: `data.table` for all operations
- **Pipeline**: `targets` package
- **Validation**: `validate` package
- **Testing**: `testthat`
- **Parallelization**: `future` package

### Code Standards
- Use snake_case naming
- Document with Roxygen2
- Use relative paths
- Prefer pure functions without side effects
- **_targets.R readability**: When creating new targets, almost always create a helper function rather than long blocks of code. Only use inline code if the command is 3-4 lines or less. This keeps _targets.R readable and maintainable.

### Claude Code Requirements
- **IMPORTANT**: Always explain major function changes and get user approval before proceeding
- Run validation after changes: `R -e "targets::tar_make()"`, unless pipeline will take too long. In that case, ask user to run the pipeline and report results.

### Git Commit Guidelines
- **Format**: Imperative mood, <50 chars, reference issues (e.g., "Fix duplicate rows. Closes #3")
- There are two types of commits:
  - **Perfect commits**: Implementation + tests + docs + issue reference. Use these after a major change.
  - **Minor commits**: While working on a feature, commit often. Use these for smaller changes.
- Use GitHub issues for context and decision documentation

### Refactoring Guidelines
**CRITICAL**: Never do partial refactoring - it breaks pipelines. Key steps:
1. Inventory functions before starting: `grep "^[a-zA-Z_].*<- function" R/*.R`
2. Test incrementally in DEV_MODE after each change
3. Maintain exact function signatures and column names (R is case-sensitive)
4. Run full verification: `R -e "targets::tar_destroy(); targets::tar_make()"`

### Validation Best Practices
- Validate after: data import, transformations, merges
- Critical for merges: Check join keys, row counts, NA patterns
- Add validation as targets in pipeline

### Pipeline Debugging Workflow
When encountering pipeline errors:
1. Check error messages carefully for missing packages or functions
2. **Check git history early** if behavior has unexpectedly changed: `git log -p -- <file>`
3. Verify all required packages are in `tar_option_set` in `target_helpers.R`
4. Test individual components outside the pipeline first
5. Use `tar_invalidate()` to force re-run of cached targets

### Testing Pipeline Components
Before running the full pipeline after changes:
1. Test individual functions with small data subsets
2. Use `tar_make(names = "specific_target")` to test single targets
3. Create minimal test scripts to verify functionality outside targets
4. Check intermediate results with `tar_load()` and inspect data structure

### Avoid Escaping Issues


#### ✅ **RECOMMENDED: Use Write Tool for Complex Scripts**
```bash
# BEST: Create R scripts with Write tool (avoids ALL escape issues)
Write /tmp/analysis.R
Rscript /tmp/analysis.R
```

#### ✅ **RECOMMENDED: Direct Commands for Simple Operations**
```bash
# GOOD: Simple operations with -e flag
R -e "library(targets); tar_load('data'); cat('Records:', nrow(data))"
```

#### ✅ **RECOMMENDED: Alternative Syntax to Avoid Special Characters**
```r
# AVOID: result <- data[!is.na(column)]     # ! causes bash issues
# USE:   result <- data[is.na(column) == FALSE]
# OR:    result <- data[complete.cases(column)]
# OR:    result <- subset(data, is.na(column) == FALSE)
```

#### ✅ **RECOMMENDED: Incremental Testing Approach**
```bash
# Instead of one 30-line diagnostic script, use 3 focused scripts:
Write /tmp/step1_load.R      # Test data loading only
Write /tmp/step2_process.R   # Test processing only
Write /tmp/step3_analyze.R   # Test analysis only
```

#### ❌ **AVOID: Bash Heredocs with Special Characters**
```bash
# PROBLEMATIC: Even quoted heredocs process some escapes
cat > /tmp/script.R <<'EOF'
data[!is.na(x)]  # This ! can still cause issues
EOF
```

#### **Why These Practices Matter**
- **Escape Character Issues**: `!`, `$`, `\` in bash heredocs cause failures
- **Debug Difficulty**: Large failing scripts are hard to troubleshoot
- **Reliability**: Write tool and simple -e commands eliminate bash interaction issues
