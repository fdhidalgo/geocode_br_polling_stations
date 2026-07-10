# Code-cleanup spec: scope, sequencing, and phase plan

**Date:** 2026-07-10
**Wayfinder ticket:** [#21](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/21) (map: [#18](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/18))
**Inputs:** the ranked findings in [docs/audits/2026-07-code-health-audit.md](../audits/2026-07-code-health-audit.md) (ticket #19) and the conventions in [docs/specs/2026-07-testing-spec.md](2026-07-testing-spec.md) (ticket #20).
**Finding IDs (C1–C5, H1–H9) refer to the audit document** — it stays the ledger of what each
problem is; this spec decides what gets fixed, how, and in what order.

## Purpose (plain language)

A recent audit of this pipeline found bugs that can silently produce wrong or incomplete
published data, gaps that make the project hard to reproduce on a fresh machine, and a
family of places where errors are swallowed instead of raised. This spec turns the audit's
findings into an ordered, execution-ready cleanup plan: five phases, each one GitHub issue,
each independently startable when its preconditions are met. It also says out loud what we
are *not* fixing now, so deferred work is a decision rather than an accident.

## Scope

**In scope:** all 5 Critical findings, all 9 High findings, plus a curated subset of Medium:
the silent-degradation items, dead-code deletion, the phantom `source()`, and
duplicated-constants dedup (including the "estabeleciemento" typo that ships into output labels).

**Explicitly deferred** (recorded here so it never needs re-litigating):

- Performance work: H7 (double CNEFE cleaning, national combine memory, branch shipping,
  worker thread oversubscription) and the hot R-level loops (`prefilter_by_common_words`,
  `create_two_level_blocked_pairs`). Benefit-driven; revisit only if runtimes hurt.
- Structural dedup: merging `match_stbairro_cnefe_muni`/`match_stbairro_agrocnefe_muni`,
  the five `process_*_batch` loops, the six inline median-coordinate aggregation blocks,
  and the `_targets.R` inline-command extractions. Higher-risk refactoring that wants the
  test suite matured first.
- `data_quality_monitoring`'s `cue = "always"` + untracked `.rds` output, and the
  `tar_render()`→`tar_quarto()` migration (Medium `_targets.R` structure items).
- Roxygen coverage and `cat()`/`message()` logging harmonization.
- renv never-used-package tail cleanup (`renv::clean()` + dependency-scanned snapshot).
- All Low-tier polish.
- Lint-debt paydown: the staged-ratchet hook from the testing spec pays it down naturally;
  phase 4 records the count only.

## Cross-cutting conventions

- **Fail loud and early.** Every fix converts silent degradation into an R error. No new
  existence guards, no fallback branches, no `warning()`-and-continue.
- **Collect-and-stop for per-municipality batch work** (decided for C5 and H3, applies to
  any batch loop touched): a municipality that errors is recorded (id + condition message)
  and the batch continues; at batch end, if any failures accumulated, `stop()` with the full
  named list. The target still errors — no NULL is ever filtered into published output — but
  one run surfaces every failing municipality. Structural preconditions (missing package,
  malformed inputs) still `stop()` immediately at function entry.
- **Behavior-changing fixes land with their tests in the same commit** (testing-spec §5).
  Assert the intended fail-loud contract with `expect_error(f(bad_input), "…fragment…")`
  plus the correct happy path. Never assert current silent behavior. This is why phase 3
  is blocked on the #20 harness existing.
- Phases 1, 2, and 4 need no test harness and are unblocked immediately.

---

## Phase 1 — Reproducibility and dev/prod isolation (C3, H8, C1)

*Unblocked now. Everything else is verified by rerunning the pipeline, so a restorable
environment and a dev mode that cannot touch production state come first.*

1. **C3 — lockfile completeness:** install lightgbm, confirm qs2, `renv::snapshot()`; run
   `renv::status()` and reconcile remaining drift (H8: lock pins R 4.5.1, machine runs
   4.5.3 — re-snapshot under the current R). Acceptance: on a clean library,
   `renv::restore()` then a dev-mode `tar_make()` completes through model training.
2. **H8 — bootstrap trap:** wrap `.Rprofile`'s `rspm::enable()` in
   `if (requireNamespace("rspm", quietly = TRUE))` so a fresh clone can run
   `renv::restore()` at all.
3. **C1 — one dev-mode knob, two stores:** define the dev/prod switch once and derive
   everything from it.
   - `_targets.yaml` with two `targets` project profiles: `main` (production, default,
     store `_targets`) and `dev` (store `_targets_dev`). Selection via the `TAR_PROJECT`
     env var — the standard `targets` mechanism.
   - In `_targets.R`, `DEV_MODE <- identical(Sys.getenv("TAR_PROJECT"), "dev")` replaces
     **both** `dev_mode_flag_value` and the hand-synced `dev_mode_flag` target command; the
     target becomes `tar_target(dev_mode_flag, !!DEV_MODE)` (spliced constant). The S3
     gate and the data filtering can no longer disagree, and dev runs write to their own
     store — dev↔prod toggles stop mass-invalidating the shared production store.
   - Update CLAUDE.md's dev-mode instructions (`TAR_PROJECT=dev R -e "targets::tar_make()"`)
     and AWS_SETUP.md. This is the C1 fix the testing spec's integration runner waits on;
     once landed, `tests/integration/dev_pipeline_check.R` stops being a stopgap.
   - Acceptance: with `TAR_PROJECT=dev`, `tar_make()` writes only under `_targets_dev/`
     and never uploads to S3; without it, the production store is untouched by dev runs.

## Phase 2 — Verified correctness bugs (C4, C5's ID reattachment)

*Unblocked now, parallel to phase 1 and to #20's harness build. Small, self-evident fixes
for verified bugs; waiting on test infrastructure to fix a data-leakage bug inverts priorities.*

1. **C4 — tuning leaks the test set:** `group_vfold_cv(training_set, ...)` instead of the
   full `model_data` (`R/model.R:362-366`). Published accuracy numbers are known-optimistic
   until re-estimated; per decision, the honest re-estimate comes from the 2024 release's
   full run (phase 5 rider), when the methodology document's numbers are refreshed in
   lockstep. No dedicated retraining run.
2. **C5 (partial) — positional ID reattachment:** in `match_geocodebr_muni()`, stop
   assigning `local_id` by position (`R/string_matching.R:536`). Carry `local_id` through
   the geocode input (or join back on the address fields) and assert
   `nrow(geocoded_result) == nrow(dt_geocode)`. The surrounding tryCatch restructuring is
   phase 3; this phase only makes the reattachment correct.

## Phase 3 — The fail-silent sweep (C5 wrappers, H1–H4, Medium silent-degradation)

*Blocked on the #20 test harness (`tests/testthat/` + conventions) existing. Every item
lands with its fail-loud tests in the same commit; `master` stays green throughout.*

1. **C5 (rest) — geocodebr error handling:** delete the outer tryCatch that converts any
   error to `warning() + NULL` and the inner one that turns geocoding errors into empty
   results; `stop()` at entry if geocodebr is not installed. Per-municipality errors follow
   the collect-and-stop convention.
2. **H1 — data-cleaning guards:** replace the 2024-file existence guard with an assertion
   on the expected TSE file count (and add the 2024 file to `tse_files` only via the 2024
   release work, #22/#23 — this spec does not validate 2024 data); `stop()` on empty
   municipality table in `clean_cnefe22`; `Reduce(intersect, ...)` over **all** year tables
   in the keep-common-columns step, with an assertion on the surviving column set.
3. **H2 — dev-mode filters:** explicit column arguments; `stop()` when the column is
   absent; remove the four-way ID-column probing in `filter_data_by_municipalities()` and
   the stacked fallbacks in `apply_brasilia_filters()`. (These tests double as insurance
   for the integration check's exactly-`{AC,RR}` tripwire.)
4. **H3 — panel batch errors:** replace the swallow-and-NULL handler in
   `process_panel_ids_municipality_batch()` with collect-and-stop; remove the NULL-filtering
   downstream.
5. **H4 — validators that warn:** `validate_inputs_consolidated()` stops on failed checks;
   `create_data_quality_monitor()` raises an error on CRITICAL status instead of finishing
   green.
6. **Medium silent-degradation items, same conventions:**
   - `convert_coord()`: count parse failures and raise the NA rate as a condition; assert
     the result is not entirely NA. (The per-row `sapply` performance item stays deferred.)
   - Panel weight threshold: promote `getOption("geocode_br.panel_weight_threshold", 0)`
     to an explicit function argument wired through the pipeline config. **Keep the
     effective value 0** — the comment says ~0.5, and which is intended is an evaluation
     question (ticket #25's territory), not a cleanup call. Flag the discrepancy there.
   - `create_section_panel_mapping()`: `stop()` on empty inputs; `cat("Warning: ...")`
     becomes a real R condition with the join-rate in the message.
   - `calculate_string_match_diagnostics()`: explicit coordinate-column arguments; fail
     instead of returning an `error` string row.
   - `get_expected_municipality_count()`: unknown state → `stop()`.
   - Load balancing (`R/utilities.R:684`): `stop()` on municipality-size key mismatches
     instead of median-imputing them.
   - TSE reads (`R/data_cleaning.R:211-239`): drop the `suppressWarnings()` re-read
     handler; let encoding/parse warnings surface once.

## Phase 4 — Dead code and duplicated constants

*Unblocked now; mechanical and low-risk. No tests required.*

1. **Delete the dead functions** (zero call sites, per audit): `chunk_string_match`,
   `get_expected_municipality_range`, `monitor_memory`, `process_string_match_batch`,
   `process_stbairro_match_batch`, `render_sanity_check_report` + `ensure_quarto_path`,
   `read_cnefe_chunked` (with its latent off-by-one). Also the dead `case_when()` branch in
   `validate_merge_simple()`, the phantom `source("R/panel_id_blocking_fns.R")` + its
   always-false `exists()` guard, and the ~20-line commented-out `geocode_writeup` target.
2. **Backward-compat shims with no external callers:** migrate callers and delete the dual
   signatures in `apply_dev_mode_filters()`, `make_model_data(geocodebr_match = NULL)`,
   `validate_prediction_stage()`.
3. **Deduplicate constants:** single definitions for the 55-item `school_syns` vector
   (currently in both `data_cleaning.R` and `model.R`, with internal duplicates), the
   27-state vector (twice in `config.R`), and the especie label table (both `clean_cnefe22`
   and `clean_cnefe10`) — fixing the "estabeleciemento" typo, an output-visible label
   change (allowed; note it in the 2024 release notes).
4. **`backup/`:** delete (33 directories of historical scratch, nothing references it;
   git history preserves it).
5. **Riders:** run `lintr::lint_dir("R")` once and record the violation count in the
   phase-4 issue (informational only — testing-spec §7); fix CLAUDE.md's stack description
   (geobr is not used; tracts/munis come from pre-saved `.rds` files).

## Phase 5 — Invalidation integrity, riding the 2024 release run (C2, H5, H9, H6)

*Lands last. These changes invalidate most of the pipeline; per decision, the 2024
release's full production run is their verification rebuild — one expensive run serves
both. Sequence with the 2024 release spec (#23); do not land mid-roadmap.*

1. **C2 — track the real inputs:** `tar_files_input()` (or per-file `format = "file",
   repository = "local"` targets) for the CNEFE 2010/2022 state files and the 2017
   agro-CNEFE file; branch over the tracked targets with `pattern = map()` so a
   re-downloaded or added state file invalidates exactly its branches.
2. **H5 — exports become real file targets:** `format = "file", repository = "local"` on
   `geocoded_export`, `panelid_export`, `section_panel_export`,
   `string_match_diagnostics_report`, matching the input-file policy.
3. **H9 — machine-independent config:** drop `detectCores()`-derived and unused fields
   (`max_workers`, `n_cores`, `batch_size`, `cache_dir`, `log_level`) from the stored
   `pipeline_config` so a different machine's rerun doesn't cascade invalidation.
4. **H6 — deterministic `local_id`:** derive from year + municipality + zone + station
   number instead of `.I`.
   - **Pre-adoption check (do first):** verify the candidate key is unique across the full
     2006–2024 input. If duplicates surface, fall back to keeping `.I` plus a hard
     assertion of the ordering invariant, and return the key question to the map.
   - The creation code gets a permanent uniqueness `stop()` assertion either way.
   - This changes every published `local_id` (and downstream panel IDs) once — a breaking
     change announced in the 2024 release notes (the release spec owns the announcement).
5. **Phase riders:** the C4 honest accuracy re-estimate and the methodology-document
   number refresh come from this same run (see phase 2).

---

## Sequencing summary

| Phase | Findings | Precondition |
|---|---|---|
| 1. Reproducibility + isolation | C3, H8, C1 | none — start now |
| 2. Correctness bugs | C4, C5 (ID reattach) | none — start now |
| 3. Fail-silent sweep | C5 (wrappers), H1–H4, Medium silent-degradation | #20 test harness landed |
| 4. Dead code + dedup | Medium dead code, shims, dup constants | none — start anytime |
| 5. Invalidation integrity | C2, H5, H9, H6 | 2024 release run scheduled (#23) |

Phases 1, 2, 4 are mutually independent. Phase 3 assumes phase 2's `match_geocodebr_muni`
reattachment fix is in (both touch the same function; 2 is minimal, 3 restructures around it).
Phase 5 is deliberately last and coupled to the release timeline.
