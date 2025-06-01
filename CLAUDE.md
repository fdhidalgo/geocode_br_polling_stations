# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This R project geocodes Brazilian polling stations (2006-2022) using administrative datasets and fuzzy string matching. It creates a comprehensive dataset of polling station coordinates and panel identifiers to track stations across time.

## Key Commands

### Development Commands
```bash
# Install dependencies (use renv for reproducibility)
R -e "renv::restore()"

# Run the full pipeline
R -e "targets::tar_make()"

# Run specific targets
R -e "targets::tar_make(names = 'target_name')"

# Check pipeline status
R -e "targets::tar_visnetwork()"

# Clean and rebuild
R -e "targets::tar_destroy()"
R -e "targets::tar_make()"
```

### Memory Management
The pipeline requires 50GB+ RAM. If encountering memory issues:
- Adjust `future::plan()` workers in `_targets.R`
- The pipeline uses 2GB limit for future globals: `options(future.globals.maxSize = 2147483648)`

## Architecture

### Data Pipeline (`_targets.R`)
The project uses `targets` package for pipeline management with these stages:
1. **Data Import**: Municipal data, CNEFE census data, polling station addresses
2. **Data Cleaning**: Normalize addresses using functions in `R/data_cleaning_fns.R`
3. **String Matching**: Fuzzy match polling stations to known coordinates using `R/string_matching_geocode_fns.R`
4. **Model Training**: Train boosted trees (lightgbm via bonsai) to select best matches
5. **Panel Creation**: Create temporal identifiers using `R/panel_id_fns.R`
6. **Validation**: Validate outputs using `R/functions_validate.R`

### Key Functions

**`R/string_matching_geocode_fns.R`**: Core matching logic
- `match_inep_muni()`: Match to INEP school catalog
- `match_schools_cnefe_muni()`: Match to schools in census data
- Uses normalized Levenshtein distance for fuzzy matching

**`R/panel_id_fns.R`**: Panel identifier creation
- Implements Fellegi-Sunter record linkage framework
- Uses Jaro-Winkler similarity and EM algorithm
- Ensures 1:1 matching constraints across years

**`R/data_cleaning_fns.R`**: Data normalization
- `normalize_address()`, `normalize_names()`: String standardization
- `clean_cnefe_*()`: Process different census years
- `create_tract_centroids()`: Generate geographic centroids

### Parallel Processing
- Uses `future` and `future.apply` for parallelization
- Configured in `_targets.R` with adaptive worker allocation
- Municipality-level parallelization for string matching

## Data Sources

Large datasets (marked with *) must be downloaded separately:
- CNEFE Census data (2010*, 2017*, 2022*)
- TSE geocoded data* (ground truth for training)
- Census tract shapefiles* (via geobr package)

Included in `data/`:
- INEP school catalog
- Polling station addresses
- Municipal identifiers

## Output Files

- `output/geocoded_polling_stations.csv.gz`: Final geocoded coordinates
- `output/panel_ids.csv.gz`: Panel identifiers linking stations across time

## Development Guidelines

### Core Technologies
- **Data Manipulation**: `data.table` (primary) - use for all data operations
- **Pipeline**: `targets` package for reproducible workflows
- **Validation**: `validate` package for data quality checks
- **Testing**: `testthat` for unit tests (when implemented)
- **Parallel Processing**: Currently `future`/`future.apply`, planned migration to `mirai` via targets dynamic branching

### Code Style and Standards
- **Functions**: Use snake_case naming, document with Roxygen2
- **data.table**: Use `:=` for in-place operations, chain operations logically
- **Paths**: Use relative paths or `here::here()`, never hardcode
- **Pure Functions**: Prefer functions without side effects

### Git Commit Guidelines - The Perfect Commit

Each commit should strive to include:
1. **Implementation**: A single, focused change
2. **Tests**: Demonstrating the implementation works (when testing framework is implemented)
3. **Documentation**: Updated docs/comments reflecting the change
4. **Issue link**: Reference to GitHub issue for context (e.g., "Addresses #7")

**Commit Message Format:**
- **Subject**: Imperative mood, <50 chars (e.g., "Add function to normalize addresses")
- **Body** (optional): Explain the *what* and *why*, not the *how*
- **Issue reference**: Always include when possible

**Examples of good commits:**
- "Add validation for CNEFE merge operations. Addresses #7"
- "Fix duplicate rows in geocoded output. Closes #3"
- "Refactor string matching to use consistent method. See #8"

**When perfect commits aren't needed:**
- Simple typo fixes
- Minor documentation updates
- Bug fixes where behavior is already documented correctly

**For exploratory work:**
- Use feature branches for experimental code
- Write informal "WIP" commits
- Squash-merge into a single perfect commit when ready

### GitHub Issues as Development Documentation

Use GitHub issues to capture context and decision-making:
- **Background**: Why is this change needed?
- **Research**: Links to relevant docs, StackOverflow, discussions
- **Code snippets**: Failed attempts, design explorations
- **Decisions**: What options were considered? Why this approach?
- **Before/After**: Screenshots or data samples showing the change

Even for solo work, issues provide valuable context that commit messages can't capture effectively.

### Testing Requirements (Future Implementation)
- Unit tests should cover typical behavior, edge cases, and error conditions
- Tests go in `tests/testthat/` matching R file names
- Run via `devtools::test()` or `testthat::test_dir("tests/testthat")`
- Start with simple test infrastructure (even `assert 1 + 1 == 2`) to enable easy additions

### Validation Best Practices
- **Validate after key operations**: data import, transformations, merges
- **Critical for merges**: Check join keys uniqueness, row counts, NA patterns
- **Integration**: Add validation as targets in pipeline
- **Handle failures**: Stop pipeline for critical issues, warn for minor ones

## Important Notes

- The project uses `renv` for package management - always run `renv::restore()` first
- String matching is computationally intensive and benefits from parallel processing
- The machine learning model (boosted trees) predicts distance to true location for selecting best match
- Memory requirement: 50GB+ RAM due to large datasets

## Known Issues and Active Development

### Data Quality (Issue #3)
- The dataset may contain duplicate rows - verify uniqueness before analysis
- TSE geocoded data may be outdated for some states (Issue #2)

### Planned Improvements (from open issues)
- **Dynamic branching** (Issue #12): Current `future_lapply` calls in `_targets.R` should be migrated to targets dynamic branching
- **String matching optimization** (Issue #8): Multiple string matching implementations need consolidation
- **Validation framework** (Issue #7): Pipeline lacks systematic validation checkpoints between transformations
- **Code style** (Issue #6): Mixed naming conventions and styles throughout codebase
- **Testing** (Issue #10): No automated test suite currently exists
- **Documentation** (Issue #11): Methodological decisions need comprehensive documentation

### Code Quality Considerations
- When working on string matching, be aware of Portuguese text peculiarities (abbreviations, accents)
- Panel creation code in `R/panel_id_fns.R` works well but could benefit from modularization
- Use data.table consistently throughout the codebase
- Consider consolidating to either data.table OR tidyverse (not both) for maintainability 