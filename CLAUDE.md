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

## Task Management with Task Master MCP

This project uses Task Master AI via MCP (Model Context Protocol) for development task management. Tasks are stored in `.taskmaster/tasks/`.

**IMPORTANT**: The project root directory is: `/home/dhidalgo/projects/geocode_br_polling_stations`

### Essential Task Master MCP Tools

Use the `mcp__taskmaster-ai__` prefixed tools for task management:

```
# Core workflow
mcp__taskmaster-ai__get_tasks(
  projectRoot="/home/dhidalgo/projects/geocode_br_polling_stations",
  status="pending",              # Optional: filter by status
  withSubtasks=true             # Optional: include subtasks
)

mcp__taskmaster-ai__next_task(
  projectRoot="/home/dhidalgo/projects/geocode_br_polling_stations"
)

mcp__taskmaster-ai__get_task(
  id="36",                      # Required: task ID
  projectRoot="/home/dhidalgo/projects/geocode_br_polling_stations"
)

mcp__taskmaster-ai__set_task_status(
  id="36",                      # Required: task ID (can be comma-separated)
  status="done",                # Required: pending/done/in-progress/review/deferred/cancelled
  projectRoot="/home/dhidalgo/projects/geocode_br_polling_stations"
)

# Task management
mcp__taskmaster-ai__expand_task(
  id="36",                      # Required: task ID to expand
  projectRoot="/home/dhidalgo/projects/geocode_br_polling_stations",
  research=true,                # Optional: use research for expansion
  num="5"                       # Optional: number of subtasks
)

mcp__taskmaster-ai__update_task(
  id="36",                      # Required: task ID
  prompt="Update to use new API", # Required: changes to apply
  projectRoot="/home/dhidalgo/projects/geocode_br_polling_stations",
  research=false                # Optional: use research
)

mcp__taskmaster-ai__add_task(
  prompt="Implement caching layer", # Required: task description
  projectRoot="/home/dhidalgo/projects/geocode_br_polling_stations",
  dependencies="1,2",           # Optional: comma-separated dependency IDs
  priority="high"               # Optional: high/medium/low
)

# Analysis
mcp__taskmaster-ai__analyze_project_complexity(
  projectRoot="/home/dhidalgo/projects/geocode_br_polling_stations",
  ids="1,3,5",                  # Optional: specific task IDs to analyze
  threshold=5                   # Optional: complexity threshold (1-10)
)

mcp__taskmaster-ai__complexity_report(
  projectRoot="/home/dhidalgo/projects/geocode_br_polling_stations"
)
```

### Current Project Tasks
The project lists major tasks in `.taskmaster/tasks/tasks.json`. A PRD already exists in `.taskmaster/docs/prd.txt`.

### Task Workflow
**IMPORTANT**: Always ask user to review tasks/subtasks before executing. Never proceed without explicit approval.
**CRITICAL**: Never mark a task as "done" without explicit permission from the user. Always ask for confirmation before setting task status to done.
**COMMIT REQUIREMENT**: When about to mark a main task (not subtask) as "done", always offer to clean up the project directory and move unneeded files to the backup folder. 

1. Use `mcp__taskmaster-ai__next_task(projectRoot="/home/dhidalgo/projects/geocode_br_polling_stations")` to find ready tasks
2. Expand complex tasks with `mcp__taskmaster-ai__expand_task(id="<task_id>", projectRoot="...", research=true)`
3. Update status with `mcp__taskmaster-ai__set_task_status(id="<task_id>", status="<status>", projectRoot="...")`
4. Test changes in DEV_MODE before marking complete
5. Ask user for permission before marking any task as "done"
6. When marking a main task as "done", offer to create a commit

### Key Task Master Patterns
```
# When requirements change (update multiple upcoming tasks)
mcp__taskmaster-ai__update(
  from="10",                    # Required: starting task ID
  prompt="API endpoint changed to v2, update all integration tasks",
  projectRoot="/home/dhidalgo/projects/geocode_br_polling_stations",
  research=true                 # Optional: use research for updates
)

# Add bugs as tasks
mcp__taskmaster-ai__add_task(
  prompt="Fix memory leak in panel ID processing for large municipalities",
  projectRoot="/home/dhidalgo/projects/geocode_br_polling_stations",
  priority="high",
  dependencies="36"             # Optional: depends on optimization task
)

# Break down complex work
mcp__taskmaster-ai__expand_task(
  id="36",
  projectRoot="/home/dhidalgo/projects/geocode_br_polling_stations",
  research=true,
  num="5",                      # Generate 5 subtasks
  prompt="Focus on memory optimization aspects"  # Optional: additional context
)
```


## Workflow Guidelines
- Anytime you are asked to make a substantial change, please make a plan and get approval before proceeding.