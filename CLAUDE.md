# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This R project geocodes Brazilian polling stations (2006-2022) using administrative datasets and fuzzy string matching. It creates a comprehensive dataset of polling station coordinates and panel identifiers to track stations across time.

**Memory requirement**: 50GB+ RAM due to large datasets

## Key Commands

### Development Mode (IMPORTANT)
**Always use development mode when testing pipeline changes** - the full pipeline takes hours. Dev mode restricts processing to a small subset of data (AC and RR states, the two smallest, defined in `get_pipeline_config()` in `R/config.R`).

Dev mode is a single switch: the `TAR_PROJECT` environment variable selects a
`targets` project profile (defined in `_targets.yaml`). Setting `TAR_PROJECT=dev`
selects the `dev` profile, which uses its own data store (`_targets_dev/`), processes
only the AC/RR subset, and stays fully local (never touches S3). The default (`main`)
profile is production. In `_targets.R`, `DEV_MODE <- identical(Sys.getenv("TAR_PROJECT"), "dev")`
derives a single constant that drives both the S3 gate and the `dev_mode_flag` target,
so the two can no longer disagree. There is nothing to edit — you just set the env var.

```bash
# Run pipeline in dev mode (fast - minutes instead of hours; store: _targets_dev/)
TAR_PROJECT=dev R -e "targets::tar_make()"

# Run a specific target in dev mode
TAR_PROJECT=dev R -e "targets::tar_make(names = 'target_name')"

# Check pipeline status in dev mode
TAR_PROJECT=dev R -e "targets::tar_visnetwork()"

# Production runs use the default profile (store: _targets/, S3-backed) — omit TAR_PROJECT
R -e "targets::tar_make()"
```

### Full Pipeline Commands
```bash
# Install dependencies (use renv for reproducibility)
R -e "renv::restore()"

# Clean and rebuild
R -e "targets::tar_destroy()"
R -e "targets::tar_make()"
```

### Testing

The test layer has two tiers plus a commit-time formatting gate (see
[docs/specs/2026-07-testing-spec.md](docs/specs/2026-07-testing-spec.md)).

```bash
# Fast unit tests over pure functions (seconds, no data/network/store).
# tests/testthat/setup.R loads functions via tar_source("R"), the same loader
# _targets.R uses, so tests exercise the definitions tar_make() runs.
Rscript tests/testthat.R

# Slow dev-mode (AC/RR) end-to-end check: builds the pipeline fresh in dev mode
# and asserts structural properties of the two final outputs (minutes, needs the
# CNEFE/TSE inputs and real memory). NOTE: it writes the shared output/*.csv.gz
# paths, overwriting any production outputs with AC/RR data.
Rscript tests/integration/dev_pipeline_check.R
```

Tests are hand-authored **spec** tests (asserting intended input→output contracts,
not snapshots of current output) so they never pin behavior the code-cleanup work
is about to change. Coverage is the first tranche of ~13 pure functions (string
transforms, matching core, panel identity); the rest are deferred to later
tranches. Behavior-changing fail-loud tests land alongside their fixes in the
cleanup work, not here.

#### Formatting gate (air)

Formatting is handled by [air](https://posit-dev.github.io/air/), an idempotent
R formatter configured in `air.toml` (`line-width = 120`, `indent-width = 2`).
The whole repo was formatted once, so there is no legacy backlog — a committed
git hook just keeps it formatted: at commit time it runs `air format --check`
against the staged `.R` files and blocks the commit if any would be reformatted.

```bash
# Activate the hook once per clone:
git config core.hooksPath .githooks

# The hook needs the air binary (it skips with a message if absent):
# https://posit-dev.github.io/air/cli.html#installation

# Format everything (or configure format-on-save in your editor):
air format .

# Check without writing (what the hook runs, over the whole repo):
air format --check .
```

Formatting is never a `testthat` failure — the correctness suite stays
style-agnostic; formatting is gated separately at commit time. air formats
layout only; it does not enforce semantic rules (naming, banned functions).

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

Census tract and municipality boundaries are read from pre-saved `.rds` files in
`data/` (`census_tracts2010_shp.rds`, `muni_shp.rds`), not fetched at pipeline
run time.

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

### Worktrees

A worktree checks out tracked files only, so it starts with all the code and none of the
pipeline — no stores, no CNEFE/TSE downloads, no R library. Without seeding, every target
rebuilds, including ones the branch never touched.

[scripts/seed-worktree.sh](scripts/seed-worktree.sh) fills that in from the main checkout.
It takes a few seconds, reports what it did and what it skipped, and does nothing when run
in the main checkout.

```bash
scripts/seed-worktree.sh
```

A `SessionStart` hook runs it automatically **in Claude Code sessions**. A worktree created
from a terminal stays unseeded until a session opens in it, so run the command above by
hand there. The hook also only reaches a worktree once the script is on the branch it was
cut from — which means `master`.

**Each worktree owns its stores.** A branch that changes pipeline code rebuilds its own
`_targets_dev/` rather than invalidating anyone else's.

**Run dev mode in worktrees** (`TAR_PROJECT=dev`). Production is seeded only so
`tar_outdated()` tells the truth: every checkout's `_targets/` points at the same S3
prefix, so a production `tar_destroy()` from a worktree deletes objects the main checkout
is still using.

**The seed lists are maintained by hand**, at the top of the script, and the rules for
editing them are documented there. The one worth knowing from outside: a directory the
pipeline starts writing into later has to join `SNAPSHOT_DIRS`, or every target writing
there rebuilds in every worktree, forever.

Inputs are hardlinked rather than copied, which is what keeps a worktree at ~0.8GB instead
of ~6.7GB on this filesystem — so nothing in `TOPUP_DIRS` may ever be written by the
pipeline, or the build would write through the link into the main checkout.

## Paper code

Research code for a paper and a public dataset — not a software product. **Machinery
test**: *machinery whose weight exceeds the analysis it serves gets cut.*

Most likely violations here:
- **No roxygen on internal functions** — there is no API. One line per simple function; a
  file over ~10% comment lines is ornament (`panel_creation.R`, `utilities.R` ≈27%).
- **Assert invariants once, where each table is built.** Matching, model, and panel code
  trusts its inputs. Never guard, fall back, or fill in — let it error.
- **`_targets.R` is a manifest** — a multi-line `command` is a function not yet written.
- **No speculative generality** — no parameter ever called with one value, no registry
  with one consumer; literal values at call sites unless used 3+ times.

Carve-outs, because this ships a public dataset: released column schemas are a contract
(renaming is versioned, not cleanup), and the pipeline is too slow to be the test harness
— pure, non-obvious, hand-verifiable functions (fuzzy matching, linkage) earn unit tests.

At review time apply `~/.claude/skills/paper-code/references/standards.md` (and
`targets-pipeline.md` beside it) in place of generic simplification angles.

## Development Guidelines

### Core Stack
- **Data**: `data.table` for all operations
- **Pipeline**: `targets` (+ `tarchetypes`)
- **Modeling**: `tidymodels` stack (`parsnip`, `recipes`, `workflows`, `tune`, `finetune`, `rsample`, `yardstick`) with `bonsai` for lightgbm
- **Record linkage**: `reclin2`; **spatial**: `sf`, `geosphere` (boundaries are pre-saved `.rds`, not fetched via `geobr`); **string distance**: `stringdist`, `stringr`
- **Assertions**: base `stop()`/`stopifnot()` at cleaning boundaries (see Paper code)
- **Validation**: `validate` package for the stage-validation targets (see `R/validation.R`)
- **Parallelization**: `crew` (mirai-backed local controllers)
- **Dependencies**: pinned with `renv` (`renv.lock`); `.Rprofile` prefers binary installs / `pak`

Note: the maintained test suite lives in `tests/` (see the Testing section above and
[docs/specs/2026-07-testing-spec.md](docs/specs/2026-07-testing-spec.md)) — fast
`testthat` unit tests over the few functions that earn one, plus a dev-mode
integration check.

### Code Standards
- Use snake_case naming
- One line saying what a function does; no roxygen (see Paper code)
- Use relative paths
- Prefer pure functions without side effects
- **_targets.R readability**: Every function body lives in `R/`. A target command longer than 3-4 lines is a helper function that hasn't been written yet.

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
- Assert at the end of the cleaning step that builds each table, once: key uniqueness, join cardinality, expected row counts, value ranges
- Critical for merges: check join keys, row counts, NA patterns — at the merge, not again downstream
- Estimation, matching, and figure code re-validates nothing. An invariant worth checking there is worth checking at the cleaning boundary instead
- A computed zero means an observed zero; missing stays `NA` and named

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

## Agent skills

### Issue tracker

Issues and PRDs live as GitHub issues, managed with the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical triage labels, used as-is (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` + `docs/adr/` at the repo root, created lazily. See `docs/agents/domain.md`.
