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

## Task Management with Task Master CLI

This project uses Task Master AI CLI for development task management. Tasks are stored in `.taskmaster/tasks/`.

**IMPORTANT**: All task-master commands should be run from the project root directory: `/home/dhidalgo/projects/geocode_br_polling_stations`

### Essential Task Commands
```bash
# Core workflow
task-master list                                    # View all tasks
task-master next                                    # Get next task to work on
task-master show <id>                               # View specific task
task-master set-status --id=<id> --status=done     # Set task status

# Task management
task-master expand --id=<id> --research             # Break down complex tasks
task-master update-task --id=<id> --prompt="changes"
task-master add-task --prompt="description" --dependencies=1,2

# Analysis
task-master analyze-complexity --research
task-master complexity-report
```

### Current Project Tasks
The project lists major tasks in `.taskmaster/tasks/tasks.json`. A PRD already exists in `.taskmaster/docs/prd.txt`.

### Task Workflow
**IMPORTANT**: Always ask user to review tasks/subtasks before executing. Never proceed without explicit approval.
**CRITICAL**: Never mark a task as "done" without explicit permission from the user. Always ask for confirmation before using `task-master set-status --id=<id> --status=done`.
**COMMIT REQUIREMENT**: When about to mark a main task (not subtask) as "done", always offer to clean up the project directory and move unneeded files to the backup folder. 

1. Use `task-master next` to find ready tasks
2. Expand complex tasks with `task-master expand --id=<id>`
3. Update status with `task-master set-status --id=<id> --status=<status>`
4. Test changes in DEV_MODE before marking complete
5. Ask user for permission before marking any task as "done"
6. When marking a main task as "done", offer to create a commit

### Key Task Master Patterns
```bash
# When requirements change
task-master update --from=<id> --prompt="explanation" --research

# Add bugs as tasks
task-master add-task --prompt="Fix bug: ..." --priority=high

# Break down complex work
task-master expand --id=<id> --research
```


## Workflow Guidelines
- Anytime you are asked to make a substantial change, please make a plan and get approval before proceeding.