# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This R project geocodes Brazilian polling stations (2006-2022) using administrative datasets and fuzzy string matching. It creates a comprehensive dataset of polling station coordinates and panel identifiers to track stations across time.

**Memory requirement**: 50GB+ RAM due to large datasets

## Key Commands

### Development Mode (IMPORTANT)
**Always use development mode when testing pipeline changes** - the full pipeline takes hours. Set `DEV_MODE = TRUE` in `_targets.R` to work with a small subset of data:

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

### Claude Code Requirements
- **IMPORTANT**: Always explain major function changes and get user approval before proceeding
- Use DEV_MODE for testing pipeline changes
- Run validation after changes: `R -e "targets::tar_make()"`

### Git Commit Guidelines
- **Format**: Imperative mood, <50 chars, reference issues (e.g., "Fix duplicate rows. Closes #3")
- **Perfect commits**: Implementation + tests + docs + issue reference
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

## Task Management with Task Master MCP

This project uses Task Master AI for development task management. Tasks are stored in `.taskmaster/tasks/`.

**IMPORTANT**: Always use `projectRoot = "/home/dhidalgo/projects/geocode_br_polling_stations"` for all task functions.

### Essential Task Commands
```r
# Core workflow
mcp__taskmaster-ai__get_tasks(projectRoot = "...")              # View all tasks
mcp__taskmaster-ai__next_task(projectRoot = "...")              # Get next task to work on
mcp__taskmaster-ai__get_task(id = "1", projectRoot = "...")     # View specific task
mcp__taskmaster-ai__set_task_status(id = "1", status = "done", projectRoot = "...")

# Task management
mcp__taskmaster-ai__expand_task(id = "1", projectRoot = "...", research = TRUE)  # Break down complex tasks
mcp__taskmaster-ai__update_task(id = "5", prompt = "changes", projectRoot = "...")
mcp__taskmaster-ai__add_task(prompt = "description", dependencies = "1,2", projectRoot = "...")

# Analysis
mcp__taskmaster-ai__analyze_project_complexity(projectRoot = "...", research = TRUE)
mcp__taskmaster-ai__complexity_report(projectRoot = "...")
```

### Current Project Tasks
The project has 10 major tasks in `.taskmaster/tasks/tasks.json`. A PRD already exists in `.taskmaster/docs/prd.txt`.

### Task Workflow
**IMPORTANT**: Always ask user to review tasks/subtasks before executing. Never proceed without explicit approval.
**CRITICAL**: Never mark a task as "done" without explicit permission from the user. Always ask for confirmation before using `set_task_status` with status="done".

1. Use `mcp__taskmaster-ai__next_task()` to find ready tasks
2. Expand complex tasks with `mcp__taskmaster-ai__expand_task()`
3. Update status with `mcp__taskmaster-ai__set_task_status()`
4. Test changes in DEV_MODE before marking complete
5. Ask user for permission before marking any task as "done"

### Key Task Master Patterns
```r
# When requirements change
mcp__taskmaster-ai__update(from = "5", prompt = "explanation", research = TRUE, projectRoot = "...")

# Add bugs as tasks
mcp__taskmaster-ai__add_task(prompt = "Fix bug: ...", priority = "high", projectRoot = "...")

# Break down complex work
mcp__taskmaster-ai__expand_task(id = "7", research = TRUE, projectRoot = "...")
```

## Known Issues
- **Data Quality**: Dataset may contain duplicates (Issue #3)
- **Dynamic branching**: Migrate from `future_lapply` to targets branching (Issue #12)
- **String matching**: Multiple implementations need consolidation (Issue #8)
- **Portuguese text**: Be aware of abbreviations and accents in string matching 