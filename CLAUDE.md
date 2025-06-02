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

### Claude Code Development Preferences
- **IMPORTANT**: Anytime you create or change a function in a major way, stop and explain to the user the changes or the rationale for the function and then ask the user whether we should proceed
- Always explain the purpose and implementation approach before making significant code changes
- Wait for explicit user approval before implementing major functionality

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

### Refactoring Guidelines - Lessons from data.table Migration

**CRITICAL**: Refactoring must be systematic and comprehensive. Partial refactoring breaks pipelines.

#### Pre-Refactoring Checklist
1. **Function Inventory**: List all functions before starting
   ```bash
   grep "^[a-zA-Z_].*<- function" R/*.R | cut -d: -f2 | cut -d' ' -f1 | sort > functions_before.txt
   ```

2. **Dependency Analysis**: Check where functions are called
   ```bash
   # Find function calls in pipeline
   grep -E "command = [a-zA-Z_]+\\(" _targets.R
   # Find function calls in other R files
   grep -rn "function_name(" R/
   ```

3. **Test Coverage**: Write tests for existing behavior BEFORE refactoring
   ```r
   # Capture current behavior
   test_that("function produces expected output", {
     result <- original_function(test_input)
     saveRDS(result, "tests/testthat/fixtures/expected_output.rds")
   })
   ```

#### During Refactoring
1. **Incremental Changes**: Refactor one function at a time and test
   ```bash
   # After each function change
   R -e "targets::tar_make()"
   ```

2. **Maintain Signatures**: Keep function names and parameters identical
   - If renaming is necessary, update ALL calling code
   - If changing parameters, ensure backward compatibility

3. **Case Sensitivity**: R is case-sensitive - maintain exact column names
   - `id_TSE` ≠ `id_tse`
   - Use `grep -i` to find case variations

4. **Complete Migration**: If refactoring a file, migrate ALL functions
   ```r
   # Don't leave functions behind in original files
   # Either refactor completely or not at all
   ```

#### Post-Refactoring Verification
1. **Function Comparison**:
   ```bash
   grep "^[a-zA-Z_].*<- function" R/*.R | cut -d: -f2 | cut -d' ' -f1 | sort > functions_after.txt
   diff functions_before.txt functions_after.txt
   ```

2. **Pipeline Testing**:
   ```bash
   # Clean rebuild to catch all issues
   R -e "targets::tar_destroy()"
   R -e "targets::tar_make()"
   ```

3. **Documentation**: Update a refactoring log
   ```markdown
   ## Refactoring Log
   - make_tract_centroids → create_tract_centroids (line 125 in _targets.R)
   - Parameter changes: locais_path → locais_file
   - Implementation: now uses data.table throughout
   ```

#### Common Pitfalls to Avoid
1. **Partial Function Migration**: Missing functions break pipelines
2. **Silent Renames**: Changing names without updating callers
3. **Parameter Mismatches**: Different argument names/order
4. **Helper Function Dependencies**: Ensure all dependencies are included
5. **Column Name Cases**: Exact case matching is critical for joins

#### Refactoring Workflow
```bash
# 1. Create feature branch
git checkout -b refactor/descriptive-name

# 2. Document current state
R -e "targets::tar_make()" # Ensure it works
git add -A && git commit -m "Baseline before refactoring"

# 3. Refactor incrementally
# - One function at a time
# - Test after each change
# - Commit working states

# 4. Final verification
R -e "targets::tar_destroy(); targets::tar_make()"

# 5. Document changes
echo "## Changes made:" >> refactoring_notes.md
```

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

## Task Management with task-master CLI

This project uses the `task-master` CLI tool to manage development tasks. Tasks are stored in `.taskmaster/tasks/` directory.

### Key Task Management Commands

```bash
# View all tasks and their status
task-master list

# See the next recommended task to work on
task-master next

# View details of a specific task
task-master show <id>

# Mark a task as complete
task-master set-status --id=<id> --status=done

# Expand a complex task into subtasks
task-master expand --id=<id> --research

# Update future tasks when implementation changes
task-master update --from=<id> --prompt="<explanation>"

# Analyze task complexity before starting work
task-master analyze-complexity --research
task-master complexity-report

# Add new tasks
task-master add-task --prompt="<description>" --dependencies=<ids>
```

### Current Project Tasks

The project has 10 major tasks tracked in `.taskmaster/tasks/tasks.json`:
1. **Setup Development Environment** - Testing infrastructure with testthat
2. **Code Standardization** - Consistent data.table usage
3. **Validation Framework** - Systematic data quality checks
4. **Dynamic Branching Migration** - Convert to targets branching
5. **String Matching Consolidation** - Unified fuzzy matching
6. **Data Integration Pipeline** - Multi-source ETL
7. **ML Coordinate Selection** - LightGBM models
8. **Panel Identifier System** - Record linkage across time
9. **geocodebr Integration** - Analyze IPEA methodology
10. **R Package Creation** - Package for data distribution

### Task Workflow Integration

**IMPORTANT**: Always ask the user to review tasks and subtasks before executing them. Never proceed with task implementation without explicit approval.

1. **Starting Work**: Run `task-master next` to identify ready tasks
2. **Task Selection**: Check dependencies are complete before starting
3. **Complex Tasks**: Use `task-master expand --id=<id>` to break down
4. **Review Required**: Present generated tasks/subtasks to user for approval
5. **Progress Tracking**: Update status with `task-master set-status`
6. **Implementation Changes**: Use `task-master update` to modify future tasks

### R-Specific Task Implementation

When working on tasks in this R project:

1. **Before Starting a Task**:
   ```bash
   # Check task details and dependencies
   task-master show <id>
   
   # Ensure R environment is ready
   R -e "renv::status()"
   ```

2. **During Implementation**:
   - Follow the task's detailed implementation notes
   - Create functions in appropriate R/ files
   - Use data.table consistently as specified in tasks
   - Add testthat tests when implementing new functions

3. **Testing Your Changes**:
   ```bash
   # Run specific pipeline targets
   R -e "targets::tar_make(names = 'your_target')"
   
   # Run tests (when implemented)
   R -e "devtools::test()"
   
   # Check code style
   R -e "lintr::lint_dir('R/')"
   ```

4. **Completing Tasks**:
   ```bash
   # Verify implementation meets test strategy
   # Mark task as done
   task-master set-status --id=<id> --status=done
   
   # Generate updated task files
   task-master generate
   ```

### Task-Specific R Commands

Based on the current tasks:

- **Task 1 (Testing)**: `R -e "usethis::use_testthat()"`
- **Task 2 (Style)**: `R -e "lintr::lint_dir('R/', linters = lintr::linters_with_defaults())"`
- **Task 3 (Validation)**: `R -e "library(validate); ?validator"`
- **Task 4 (Targets)**: `R -e "targets::tar_visnetwork()"`
- **Task 5-8 (Implementation)**: Use task details for specific R code
- **Task 10 (Package)**: `R -e "usethis::create_package('.')"`

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