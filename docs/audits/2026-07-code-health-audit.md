# Code-health audit: current state vs modern R/targets practice

**Date:** 2026-07-10
**Wayfinder ticket:** [#19](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/18) (map: [#18](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/18))
**Feeds:** the testing spec (#20) and the code-cleanup spec (#21).

## Method

Three parallel audits over `_targets.R` (1,122 lines) and `R/*.R` (≈4,860 lines across 8 files):
a targets-pipeline review (structure, branching, storage, crew, invalidation), an R code-quality
review (fail-silent patterns, dependencies, documentation, code smells), and a mechanical sweep
(dead code, function purity, every tryCatch/guard/suppression). Findings were merged, deduplicated,
and re-ranked; the highest-impact claims (missing lockfile entries, the model data-leakage bug,
geobr non-use) were verified directly against the source. Scope per the ticket: **targeted cleanup
only** — package restructure is out of scope, and this document recommends none.

Severity meaning: **Critical** = can corrupt outputs, ship wrong results, or break reproducibility
today. **High** = silently degrades data quality or violates the fail-loud policy in ways that
matter. **Medium** = maintainability, performance, or latent-bug risk. **Low** = polish.

---

## Critical

### C1. Dual dev-mode flags can write dev data into the production S3 store
`_targets.R:81` (`dev_mode_flag_value`, gates S3) and `_targets.R:113-116` (`dev_mode_flag` target,
gates data filtering) are synced only by a comment. If the target says dev but the variable says
production, AC/RR-subset objects are written into the production S3 bucket under the production
prefix, silently corrupting the shared object cache for every machine that syncs it.
**Fix:** one top-level constant (e.g. from an env var) spliced into the target with
`tar_target(dev_mode_flag, !!DEV_MODE)`; ideally also separate dev/production stores via
`tar_config_set()` project profiles (`TAR_PROJECT`), which additionally stops dev↔prod toggles from
mass-invalidating the shared store.

### C2. The pipeline's largest inputs are invisible to invalidation
- CNEFE 2010/2022 state files are read inside `process_cnefe_state()` (`R/utilities.R:332-349`,
  used at `_targets.R:273-310, 324-342`) from paths built at runtime, with **no** `format = "file"`
  target tracking them. Re-downloaded or corrected CNEFE data never invalidates anything.
- The state lists themselves come from `list.files()` executed *inside* the `cnefe10_states` /
  `cnefe22_states` targets (`R/config.R:62-68`): adding a state file does not change the command,
  so new states are silently skipped until an unrelated rerun.
- `agro_cnefe_files` (`_targets.R:399-402`) returns paths as a plain data target — same problem
  for the 2017 agro-CNEFE file.

**Fix:** track all of these with `tar_files_input()` (or per-file `format = "file",
repository = "local"` targets) and branch over the tracked file target with `pattern = map()`.

### C3. `renv::restore()` produces a broken environment: lightgbm missing from renv.lock *(verified)*
The model engine is lightgbm via bonsai (`R/model.R:389`), but `renv.lock` has no lightgbm entry —
a fresh machine restores cleanly and then fails at model training. `qs2` is also absent, which
targets ≥ 1.8 uses to implement `format = "qs"` (targets 1.11.3 is pinned).
**Fix:** install lightgbm (and confirm qs2), then `renv::snapshot()`; audit `renv::status()`
generally (see also H8 on lockfile drift).

### C4. Hyperparameter tuning leaks the test set *(verified)*
`R/model.R:362-366`: `group_vfold_cv(model_data, ...)` builds tuning resamples from the **full**
dataset, while the recipe/final fit use the `training_set` half from `group_initial_split()`.
Tuning therefore sees the held-out test municipalities, and `last_fit()` metrics are optimistic —
the reported match-selection model performance is not trustworthy as an out-of-sample estimate.
**Fix:** pass `training_set` to `group_vfold_cv()`. (Any published accuracy numbers derived from
`last_fit()` should be re-estimated after the fix.)

### C5. The geocodebr matching source can silently vanish, wholesale or per-municipality
A cluster of fail-silent patterns in `match_geocodebr_muni()` (`R/string_matching.R:448-559`):
- `_targets.R`'s fail-loud policy is inverted by an outer `tryCatch` (`:453`) that converts **any**
  error into `warning() + return(NULL)` — the comment says "prevent pipeline crashes" — so a whole
  municipality disappears from geocodebr coverage on any failure.
- An inner `tryCatch` (`:493-534`) turns geocoding errors into an empty result, indistinguishable
  from "no matches", masking systematic failures.
- If geocodebr isn't installed, `requireNamespace()` (`:455-458`) warns and returns NULL — the
  entire matching source is silently disabled for the run.
- `geocoded_result[, local_id := dt_geocode$local_id]` (`:536`) reattaches IDs **positionally**,
  assuming `geocodebr::geocode()` returns exactly one row per input in order; any drop/reorder
  assigns coordinates to the wrong polling stations with no error.

**Fix:** delete both tryCatch wrappers (or use targets' `error = "continue"` + workspaces if
partial failure is genuinely acceptable), `stop()` on missing package, pass `local_id` through the
geocode input or join back on address fields, and assert `nrow(geocoded_result) == nrow(dt_geocode)`.

---

## High

The fail-silent family (H1–H6) all violate the project's fail-loud-and-early rule; they are the
priority intake for the cleanup spec.

### H1. The known 2024-file guard, and its siblings in data cleaning
- `R/data_cleaning.R:233-245`: `if (length(tse_files) >= 4 && file.exists(tse_files[4]))` silently
  drops the 2024 TSE geocoded file when absent — a three-year ground-truth dataset with no error.
  Note `tse_files` (`_targets.R:553-563`) currently lists only 2018/2020/2022, so the guarded
  branch never fires: if 2024 TSE ground truth exists, it is silently missing today.
- `R/data_cleaning.R:152-163` (`clean_cnefe22`): empty municipality table → columns filled with NA
  instead of an error.
- `R/data_cleaning.R:248-251`: the keep-common-columns step intersects only `loc22`/`loc18` names,
  ignoring loc20/loc24 — a schema change in those years is papered over by `rbindlist(fill = TRUE)`.

**Fix:** assert expected file counts and non-empty inputs; `Reduce(intersect, ...)` over all years.

### H2. Dev-mode filters fall back to *unfiltered* data
`filter_by_dev_mode()`, `filter_data_by_state()`, `filter_data_by_municipalities()`
(`R/utilities.R:27-34, 45-48, 74-77`) return the **full dataset** with only a `warning()` when the
expected column is missing — in dev mode that silently runs the multi-hour full pipeline, and
warnings inside crew workers are easy to miss. `filter_data_by_municipalities()` (`:64-78`) also
probes four alternative ID columns and filters on whichever it finds first (possibly the wrong ID
system); `apply_brasilia_filters()` (`:147-170`) stacks three layers of column-guessing fallback.
**Fix:** explicit column arguments; `stop()` on absence.

### H3. Per-municipality errors are swallowed in panel creation
`process_panel_ids_municipality_batch()` (`R/panel_creation.R:378-389`) wraps `make_panel_1block()`
in a tryCatch that `cat()`s the error and returns NULL; NULL results are then filtered out
(`:225-227, :402`), so any municipality that errors is silently excluded from published panel IDs.
**Fix:** remove the handler, or collect failures and `stop()` at batch end naming the failed
municipalities.

### H4. Validation warns where it should stop
- `validate_inputs_consolidated()` (`R/validation.R:405-408`) only `warning()`s on failed checks.
- `create_data_quality_monitor()` (`R/validation.R:699-771`) accumulates "WARNING"/"CRITICAL"
  status strings and missing-export alerts (`:699-709`) but never raises a condition — a CRITICAL
  data-quality state finishes green.

**Fix:** `stop()` on failure/CRITICAL, mirroring `validate_predictions_simple(stop_on_failure = TRUE)`.

### H5. Output files are untracked side effects
`geocoded_export`, `panelid_export`, `section_panel_export` (`_targets.R:1039-1060`) and
`string_match_diagnostics_report` (`:913-923`) write files and return path strings stored as qs
objects on S3. Deleting or hand-editing `output/geocoded_polling_stations.csv.gz` leaves targets
"up to date"; a second machine restores only the path string, not the file.
**Fix:** `format = "file", repository = "local"` on export targets, matching the input-file policy.

### H6. Published `local_id` depends on input row order
`R/data_cleaning.R:419` assigns `local_id := .I`, so the primary station identifier — which flows
into published panel IDs — changes wholesale if the input file is ever re-sorted upstream.
**Fix:** derive `local_id` from a deterministic key (year + municipality + zone + station number),
or at minimum assert the ordering invariant.

### H7. Performance/memory design of the heavy stages
- CNEFE 2010 state files are read and fully cleaned **twice** — `cnefe10_cleaned_by_state` and
  `schools_cnefe10_by_state` (`_targets.R:279-310`) both call `process_cnefe_state()` on the same
  file, though `clean_cnefe10()` already produces both outputs in one pass.
- The national combine targets `cnefe10`, `cnefe22`, `schools_cnefe10` (`_targets.R:312-322,
  344-354, 357-367`) `rbindlist()` all states on the default 28-worker controller — the biggest
  single-worker memory spike sits exactly where no `memory_limited` resource is assigned.
- Batched match targets (`_targets.R:593-621, 721-892`) ship the entire `locais_filtered` plus
  full national reference tables to every branch and subset inside the worker, multiplying
  transfer cost and per-worker memory. `tar_group_size()`/`iteration = "group"` would send each
  branch only its rows.
- `data.table::setDTthreads(1)` (`_targets.R:46`) only affects the main process; crew workers are
  fresh sessions defaulting to multi-threaded data.table, so 28 workers oversubscribe the box
  (worker ceilings already sum to 36 on 32 cores; `R/config.R:178, 191`).

### H8. Environment drift and a bootstrap trap
- `renv.lock` was last snapshotted 2025-08-02, pins R 4.5.1; the machine runs R 4.5.3 — the
  recorded environment no longer matches the one producing results.
- `.Rprofile:10` calls `rspm::enable()` unguarded: on a machine where the library isn't restored
  yet this errors during profile evaluation and halts every `Rscript` call, **including the
  `renv::restore()` needed to fix it** (observed directly during this audit).
  **Fix:** wrap in `if (requireNamespace("rspm", quietly = TRUE))`.

### H9. `pipeline_config` embeds the machine's core count
`get_pipeline_config()` (`R/config.R:32, 39`) stores `detectCores() - 1` in the config object, so
rerunning `pipeline_config` on a machine with a different core count changes its hash and cascades
invalidation through essentially the whole pipeline — a real hazard given the documented
multi-computer S3 setup. The fields (`max_workers`, `n_cores`, plus unused `batch_size`,
`cache_dir`, `log_level`) are not what sizes the controllers anyway (`get_crew_controllers()`
hardcodes 28/8). **Fix:** drop machine-dependent and unused fields from the stored config.

---

## Medium

### Duplication and dead weight
- **Dead functions (zero call sites repo-wide):** `chunk_string_match` (`R/string_matching.R:44`),
  `get_expected_municipality_range` (`R/config.R:147`), `monitor_memory` (`R/data_cleaning.R:747`),
  `process_string_match_batch` (`R/utilities.R:193`), `process_stbairro_match_batch`
  (`R/utilities.R:265`), `render_sanity_check_report` + its only-reachable-through-it helper
  `ensure_quarto_path` (`R/validation.R:431, 414`), and `read_cnefe_chunked`
  (`R/data_cleaning.R:671` — unreached; also harbors a latent off-by-one at `:691-708` that would
  duplicate one row per chunk if ever exercised). The two dead batch generics also dispatch on
  `deparse(substitute())` — fragile NSE that would break if revived. **Fix:** delete all of them.
- **Phantom source:** `R/panel_creation.R:436-438` — `source("R/panel_id_blocking_fns.R")` points
  at a file that does not exist, shielded by an `exists()` guard that is always false (the function
  is defined in the same file). Delete guard and call.
- **Commented-out target:** `_targets.R:1101-1121`, the ~20-line `geocode_writeup` render target.
- **Duplicated constants/logic:** the 55-item `school_syns` vector verbatim in
  `R/data_cleaning.R:508-564` and `R/model.R:153-209` (with internal duplicates); the 27-state
  vector twice in `R/config.R:41-43, 72-75`; the especie label table (with the "estabeleciemento"
  typo, which ships into output labels) in both `clean_cnefe22` and `clean_cnefe10`
  (`R/data_cleaning.R:170-175, 864-869`); `match_stbairro_cnefe_muni` vs
  `match_stbairro_agrocnefe_muni` ~95% identical (`R/string_matching.R:309-444`); five
  near-identical `process_*_batch` loops in `R/utilities.R:393-631`; six near-identical ~11-line
  inline median-coordinate aggregation blocks in `_targets.R:369-480`.
- **Backward-compat shims with no external callers:** dual signatures in
  `apply_dev_mode_filters()` (`R/utilities.R:87-114`), `make_model_data(geocodebr_match = NULL)`
  (`R/model.R:18`), `validate_prediction_stage()` (`R/validation.R:113-117`). Migrate callers,
  delete shims.
- **`backup/`:** 33 subdirectories of historical scratch; nothing in `_targets.R` or `R/`
  references it. Inert — archive or delete at will.

### `_targets.R` structure
- Nine multi-line inline command blocks beyond the six aggregation blocks above
  (`_targets.R:119-144, 192-253, 575-584, 593-621, 913-923, 1052-1060, 1062-1093`) exceed the
  project's own ≤3-4-line inline rule; extract to named helpers.
- Dev-mode filtering exists in three implementations (inline `if` blocks at `:192-253`,
  `filter_by_dev_mode()`, `apply_dev_mode_filters()`); standardize on one.
- `data_quality_monitoring` (`_targets.R:1062-1093`) has `cue = tar_cue(mode = "always")` **and**
  writes an untracked `output/latest_quality_results.rds` — reruns (and re-uploads to S3) every
  `tar_make` while its file output is invisible to the graph.
- `tar_render()` drives a **Quarto** document (`_targets.R:1096-1100`); use
  `tarchetypes::tar_quarto()` (likely removes the `QUARTO_PATH` workaround at `:31-39`), and add
  `repository = "local"` — the report currently inherits `repository = "aws"`, contradicting the
  project's own file-target policy.
- Manual `controller_group$start()` / top-level `on.exit(terminate())` (`_targets.R:57-73`) is
  stale practice; current targets + crew manage controller lifecycle.
- crew: `seconds_wall = 3600/7200` (`R/config.R:181, 193`) may kill long full-Brazil branches
  mid-run; verify against `tar_meta(fields = seconds)`.

### Smaller correctness/robustness items
- `convert_coord()` (`R/data_cleaning.R:636-668`): every parse failure silently becomes NA with no
  accounting; also called via `sapply()` once per row over tens of millions of CNEFE 2010 rows
  (`:891-893`) — convert unique values and join back, and report the NA rate.
- TSE reads (`R/data_cleaning.R:211-239`): warning-handler re-reads each file under
  `suppressWarnings()` — masks encoding/parse diagnostics and doubles I/O.
- Panel weight threshold read from `getOption("geocode_br.panel_weight_threshold", 0)`
  (`R/panel_creation.R:538-539`) — an invisible, untracked pipeline parameter whose default (0)
  contradicts the adjacent comment (~0.5). Make it an explicit argument wired through config.
- `create_section_panel_mapping()` (`R/panel_creation.R:761-774, 812, 836`): empty inputs → empty
  result; low join rates reported via `cat("Warning: ...")`, which is not even an R condition.
- `calculate_string_match_diagnostics()` (`R/string_match_diagnostics.R:21-43`): guesses coordinate
  columns from fallback lists, returns an `error` string row instead of failing.
- `get_expected_municipality_count()` (`R/config.R:139-141`): unknown state → NA with warning.
- Latent `case_when()` without dplyr attached (`R/validation.R:56-62`) — currently in a dead
  branch of `validate_merge_simple()`; delete branch or use base R.
- `get_adaptive_chunk_size()` default `available_memory_gb = 4` (`R/string_matching.R:172`) on a
  50GB+ machine — over-chunks unless callers pass reality.
- Median imputation of missing municipality sizes in load balancing (`R/utilities.R:684`) masks
  key mismatches.
- Hot R-level loops: `prefilter_by_common_words()` O(n×m) double loop
  (`R/string_matching.R:17-42`) and per-pair loop in `create_two_level_blocked_pairs()`
  (`R/panel_creation.R:698-722`) — vectorize via word-index joins.

### Dependencies and docs
- renv.lock carries a large never-used tail (shiny/bs4Dash/DT/reactable/waiter, arrow, duckdb,
  ellmer, mcptools, gt, sp, enderecobr…) — an all-installed-packages snapshot obscuring the true
  dependency set. `renv::clean()` + dependency-scanned snapshot.
- CLAUDE.md lists geobr in the stack, but geobr appears nowhere in code or lockfile *(verified)* —
  tracts/munis come from pre-saved `.rds` files (`_targets.R:184, 209`). Update docs to describe
  the `.rds` provenance.
- Roxygen coverage ≈ half: `data_cleaning.R`, `string_matching.R`, `config.R`, `model.R` largely
  undocumented; existing headers carry meaningless `@export` tags and stale references
  (`R/utilities.R:642` names a nonexistent function; `:650` example uses an undefined object).
- Logging split between `cat()` (panel_creation, parts of validation) and `message()` (rest);
  `cat()` can't be captured as a condition and interleaves badly across crew workers.

---

## Low

- Redundant per-target `format`/`storage`/`retrieval`/`deployment` settings repeating the global
  defaults across dozens of targets — strip to true overrides only.
- `secc_loc_map_file` is a pipeline *input* living under `./output/` (`_targets.R:649-654`).
- Unused `panel` context in `get_states_for_processing()` (`R/config.R:70-76`).
- `library(bonsai)` inside `train_model()` (`R/model.R:319`); commented-out `metrics` argument in
  `tune_race_anova()` (`:407`) means tuning uses default metrics while the defined `metric_set`
  applies only at `last_fit`; `sample(1:nrow(...))` idiom and argument shadowing (`:345`);
  log-offset `0.0001` hardcoded independently at `:377` and `:432`.
- Unused `muni_ids` parameter in `match_geocodebr_muni()` plus wasted per-batch filtering feeding it.
- Custom `%||%` (`R/utilities.R:16`) shadows base R ≥ 4.4's operator.
- Non-word-bounded `str_remove(num_endereco_char, "SN")` (`R/data_cleaning.R:107`).
- `batch_id := integer()` zero-length-RHS idiom (`R/panel_creation.R:276`); row-wise
  `apply(.SD, 1, min)` for panel_id (`:170`).
- `x %in% y == TRUE` / `grepl(...) == TRUE` comparisons; emoji in worker console output;
  `options(pkgType = "binary")` meaningless on Linux.
- A `.lintr` exists (snake_case, data.table rules) but nothing runs it — no CI, no pre-commit
  hook. (CI is deferred fog on the map; the testing spec should weigh this.)

---

## Testability inventory (input to the testing spec, #20)

No function in the audited set uses RNG, `<<-`, or `Sys.setenv`; impurity is almost entirely file
I/O at the edges. The matching and panel layers are pure — the best possible shape for the agreed
testing strategy (testthat over pure functions with tiny fixtures).

**Pure — prime unit-test targets:**

| Layer | Functions (file:line) |
|---|---|
| Normalization / cleaning | `normalize_address` (data_cleaning.R:485), `normalize_school` (:506), `clean_inep` (:581), `convert_coord` (:636), `standardize_column_names` (:17, mutates by reference), `finalize_coords` (:424), `make_tract_centroids` (:463), `calc_muni_area` (:620), `get_cnefe22_schools` (:629), `clean_text_for_geocodebr` (:949), `simplify_address_for_geocodebr` (:962) |
| String matching | `prefilter_by_common_words` (string_matching.R:17), `match_strings_memory_efficient` (:68), `get_adaptive_chunk_size` (:172), `match_inep_muni` (:193), `match_schools_cnefe_muni` (:261), `match_stbairro_cnefe_muni` (:309), `match_stbairro_agrocnefe_muni` (:377) |
| Panel creation | `process_year_pairs` (panel_creation.R:24), `make_panel_ids` (:74), `create_panel_dataset` (:116), `make_panel_1block` (:196), `combine_state_panel_ids` (:223), `create_panel_municipality_batches` (:259), `process_panel_ids_municipality_batch` (:322), `extract_significant_words` (:615), `create_two_level_blocked_pairs` (:657), `create_section_panel_mapping` (:756) |
| Utilities | `filter_by_dev_mode` (utilities.R:18), `filter_data_by_state` (:37), `filter_data_by_municipalities` (:57), `apply_dev_mode_filters` (:87), `apply_brasilia_filters` (:128), `process_inep_batch` (:393), `process_schools_cnefe_batch` (:425), `process_cnefe_stbairro_batch` (:490), `process_agrocnefe_stbairro_batch` (:566), `create_municipality_batch_assignments` (:652) |

**Impure — integration-test or dev-mode-pipeline territory:** the `clean_cnefe*` /
`import_locais` / `clean_tsegeocoded_locais` / `clean_agro_cnefe` readers (fread),
`process_cnefe_state`, `match_geocodebr_muni` + `process_geocodebr_batch` (external geocodebr DB),
`create_and_select_best_pairs_optimized` (reads a global option — see Medium; pure once that's an
argument), and the four `export_*` writers.

Two caveats for fixture design: several "pure" functions `cat()`/`message()` progress output
(wrap in `suppressMessages()` or capture in tests), and `standardize_column_names` mutates its
data.table argument by reference (test with a copy).

---

## Implications

**For the testing spec (#20):** the pure-function inventory above is the menu. Highest-value first
targets: `normalize_address`/`normalize_school`/`clean_inep` (deterministic string transforms, easy
fixtures), `convert_coord` (has a known silent-NA failure mode to pin down), the four
`match_*_muni` functions (core algorithm), and `make_panel_ids`/`process_year_pairs` (panel
identity logic, including the H6 row-order concern). The fail-silent fixes in the cleanup spec
will *change observable behavior* (errors where there was silence) — tests written before cleanup
should assert the intended fail-loud behavior, not the current silent one.

**For the cleanup spec (#21):** suggested sequencing — (1) reproducibility blockers C3/H8
(lockfile, .Rprofile trap) since everything else is verified by rerunning the pipeline; (2) the
correctness bugs C4 (data leakage) and C5 (geocodebr ID reattachment); (3) the fail-silent family
C5/H1–H4 plus the Medium silent-degradation items, in one sweep with tests landing alongside;
(4) invalidation integrity C1/C2/H5/H9 (needs a planned full rebuild — coordinate with the 2024
release); (5) dead-code deletion and deduplication (mechanical, low risk); (6) performance items
H7 and the hot loops (optional, benefit-driven).
