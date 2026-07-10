# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This R project geocodes Brazilian polling stations (2006-2022) using administrative datasets and fuzzy string matching. It creates a comprehensive dataset of polling station coordinates and panel identifiers to track stations across time.

**Memory requirement**: 50GB+ RAM due to large datasets

## Key Commands

### Development Mode (IMPORTANT)
**Always use development mode when testing pipeline changes** - the full pipeline takes hours. Dev mode restricts processing to a small subset of data (AC and RR states, the two smallest, defined in `get_pipeline_config()` in `R/config.R`).

To enable it, set **both** flags in `_targets.R` to `TRUE`:
- the `dev_mode_flag` target's `command` (drives `pipeline_config` and all data filtering)
- `dev_mode_flag_value` near the top of the file (gates AWS S3 — dev mode stays fully local)

These are intentionally separate: `dev_mode_flag_value` runs before the pipeline is built, so it can't read the target. Keep the two in sync.

```bash
# Check if dev mode is enabled
grep "dev_mode_flag" _targets.R

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
2. **Data Cleaning**: Normalize addresses using `R/data_cleaning.R`
3. **String Matching**: Fuzzy match polling stations to known coordinates using `R/string_matching.R`
4. **Model Training**: Train boosted trees (lightgbm via `bonsai`/`parsnip`) to select best matches — `R/model.R`
5. **Panel Creation**: Create temporal identifiers using `R/panel_creation.R`
6. **Validation**: Validate outputs using `R/validation.R`

Other source files: `R/config.R` (pipeline config + crew controllers), `R/utilities.R` (helpers, `%||%`), `R/string_match_diagnostics.R` (match-quality reporting). Inventory functions with `grep "<- function" R/*.R`.

### Key Functions
- **String Matching**: `match_inep_muni()`, `match_schools_cnefe_muni()`, `match_stbairro_cnefe_muni()`, `match_geocodebr_muni()` - fuzzy matching with Levenshtein/string distance (`R/string_matching.R`)
- **Panel IDs**: Fellegi-Sunter record linkage (`reclin2`) with Jaro-Winkler similarity (`R/panel_creation.R`)
- **Data Cleaning**: `normalize_address()`, `normalize_school()`, `clean_inep()`, `clean_agro_cnefe()`, `clean_tsegeocoded_locais()` (`R/data_cleaning.R`)
- **Parallel Processing**: Uses `crew` (mirai-backed) local controllers, not `future`. Two controllers are defined in `get_crew_controllers()` in `R/config.R`: `standard` (up to 28 workers) and `memory_limited` (up to 8 workers) for memory-heavy CNEFE/matching targets. Assign a target to one via `resources = tar_resources(crew = tar_resources_crew(controller = "memory_limited"))`.

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

### Multi-Computer Setup

For setting up this project on a new computer with AWS S3 integration, see [AWS_SETUP.md](AWS_SETUP.md).

## Development Guidelines

### Core Stack
- **Data**: `data.table` for all operations
- **Pipeline**: `targets` (+ `tarchetypes`)
- **Modeling**: `tidymodels` stack (`parsnip`, `recipes`, `workflows`, `tune`, `finetune`, `rsample`, `yardstick`) with `bonsai` for lightgbm
- **Record linkage**: `reclin2`; **spatial**: `sf`, `geosphere`, `geobr`; **string distance**: `stringdist`, `stringr`
- **Validation**: `validate` package (see `R/validation.R`)
- **Parallelization**: `crew` (mirai-backed local controllers)
- **Dependencies**: pinned with `renv` (`renv.lock`); `.Rprofile` prefers binary installs / `pak`

Note: there is no active `testthat` suite — the `test_*.R` files under `backup/` are historical scratch scripts, not a maintained test directory.

### Code Standards
- Use snake_case naming
- Document with Roxygen2
- Use relative paths
- Prefer pure functions without side effects
- **_targets.R readability**: When creating new targets, almost always create a helper function rather than long blocks of code. Only use inline code if the command is 3-4 lines or less. This keeps _targets.R readable and maintainable.

### Claude Code Requirements
- **IMPORTANT**: Always explain major function changes and get user approval before proceeding
- Run validation after changes: `R -e "targets::tar_make()"`, unless pipeline will take too long. In that case, ask user to run the pipeline and report results.


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
3. Verify all required packages are in the `packages` vector of `configure_targets_options()` in `R/config.R`
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
