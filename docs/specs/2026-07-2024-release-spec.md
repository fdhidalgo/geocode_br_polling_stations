# Spec: validated 2024 polling-station geocoding release (v0.15)

**Wayfinder ticket:** [#23 — Decide the 2024 validation & release spec](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/23)
**Builds on:** [#22 — 2024 integration-state audit](docs/research/2026-07-2024-integration-audit.md)
**Blocks on:** [#25 — Decide the evaluation spec](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/25) (the release ships with #25's refreshed evaluation)
**Rides:** [#36 — Cleanup phase 5](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/36) (its verification rebuild *is* this release's rebuild)
**Date:** 2026-07-10

## Purpose (plain language)

The pipeline already pulls in 2024 election data, but the official 2024 coordinates
from the electoral authority (TSE) were silently dropped from the code, no 2024 results
have ever been published from committed code, and the one full run that did include 2024
is not reproducible. This spec says exactly what has to be true to publish a trustworthy
2024 dataset — and, because the same rebuild regenerates every earlier year too, it
scopes the work as a **full 2006–2024 re-release (version 0.15)** rather than a 2024 bolt-on.
It is planning only: it defines the release's gates, dependencies, and deliverables so the
work can be executed after the wayfinder map is done.

## The nine decisions

1. **Scope — full 2006–2024 re-release.** The pipeline builds all years together (the
   combined `polling_stations_2006_2024.csv.gz` input; cross-year panel linkage), so any
   clean rebuild regenerates every year. The cleanup fixes (C4 leak, H6 `local_id`, C5
   reattach) also move earlier years' numbers. Publishing only 2024 while keeping the old
   2006–2022 files would ship a dataset whose early years disagree with the code that
   exists. Therefore the release re-publishes all years; "2024 support" is the headline
   feature of v0.15, not a partial update. **This release's rebuild is the same production
   run as cleanup phase 5 (#36).**

2. **2024 TSE coverage — hard gate at ≥ 92% landed.** The raw 2024 TSE file has official
   coordinates for **93.6%** of stations, but the stale 2 Aug run landed only ~57% — a
   ~36-point merge-loss from the identity join (`ano · cod_localidade_ibge · nr_zona ·
   nr_locvot` plus the TSE↔IBGE municipality crosswalk). The release must diagnose and fix
   that join so **landed 2024 TSE coverage is ≥ 92%** (within ~2 points of available; the
   slack absorbs genuinely unmatchable records). Outputting a model guess for a station
   that has a published official coordinate is the least defensible failure mode and is a
   plumbing bug, not a modeling limit. **Regression tripwire:** compute landed TSE-coverage
   % for every year in the rebuild and flag any year whose coverage drops relative to
   expectation (the merge-gap fix and deterministic `local_id` must not silently reduce
   earlier-year coverage).

3. **Evaluation — the release blocks on #25.** The release ships with the redesigned,
   leakage-controlled evaluation from the evaluation spec (#25), not an interim number.
   #25 is sequenced before the release rebuild. (Rationale: a full public re-release should
   not carry a knowingly circular accuracy figure — TSE coordinates are currently both the
   training target and the yardstick — even caveated.)

4. **Accuracy — no performance gate.** There is **no** pass/fail bar on accuracy. The
   rebuild uses the improved, leakage-controlled metrics from #25, reports the honest
   numbers, and ships regardless of whether accuracy improved (fixing the C4 test-set leak
   may legitimately make the *reported* number look lower, because the old published figure
   was inflated by the leak). The release's **hard gates are correctness/plumbing only:**
   the structural tripwires (§ Validation gates) plus the ≥ 92% 2024 coverage gate.

5. **Training — include 2024 labels.** Once 2024 TSE truth is re-wired into `tse_files`,
   2024's ~87k TSE-labeled stations automatically join the training set (`make_model_data`
   merges all TSE-truth rows and labels each with distance-to-TSE). Keep this default; do
   **not** hold 2024 out as an unseen year. More labels help the model, especially the ~7%
   of 2024 stations with no official coordinate (the only 2024 stations the model actually
   determines). Out-of-sample honesty comes from #25's cross-validation folds (fold
   isolation guarantees a station is never evaluated on a model trained on it), not from a
   whole-year holdout.

6. **Versioning & channel — v0.15 on GitHub Releases.** Continue the existing numeric
   scheme (latest is 0.141); this is the largest change since 2022→2024, so bump to
   **0.15** (not 1.0 — avoid implying a stability guarantee). Channel unchanged: a GitHub
   Release with `geocoded_polling_stations.csv.gz` and `panel_ids.csv.gz` attached, as the
   README already directs users. A citable DOI / Zenodo / `CITATION.cff` is **out of scope**
   for this release — filed as a separate issue.

7. **Schema — columns stay stable; value changes documented.** No column is added or
   removed. Provenance is already derivable (`tse_lat`/`tse_long` non-NA, or `pred_dist ==
   0`), so no `coord_source` column is added. Two **value** changes must be documented, not
   schema-versioned: `local_id` becomes deterministic (H6), so its values differ from prior
   releases and are **not comparable across versions** (external joins keyed on `local_id`
   break); and coordinates and `pred_dist` shift because of the C4 leak fix and the
   merge-gap fix. Replacing `pred_dist` with a calibrated-uncertainty measure stays in the
   methodology thread (#29) — out of scope here.

8. **Documentation deliverables.**
   - **Methodology doc** (`doc/geocoding_procedure.qmd`) — lockstep constraint (mandatory).
     Currently evaluates 2018 only and never mentions 2024. Update to cover 2024, and author
     its accuracy section from #25's refreshed, leakage-controlled evaluation.
   - **README** — change "2006 to 2022" → "2006–2024"; refresh the station/row counts; add
     a note that `local_id` values are **not comparable across releases** (merge on the
     natural key or `panel_id` instead).
   - **Release notes (0.15)** — document: 2024 added; all years re-published; `local_id`
     values changed (breaking external joins on it); coordinates and `pred_dist` moved due
     to the test-set-leak fix and the merge-gap fix; the honest accuracy story from #25.

9. **No old-vs-new diff artifact.** The rebuild ships without a quantitative comparison
   against 0.141; earlier-year changes are described qualitatively in the release notes.

## Execution prerequisites (hard dependencies)

Carried from the #22 audit; the release cannot proceed without these.

- **Re-wire 2024 TSE truth, fail-loud.** Add `./data/eleitorado_local_votacao_2024.csv.gz`
  back to the `tse_files` target (dropped inadvertently in commit `4768e54`). Replace the
  fail-silent guard in `clean_tsegeocoded_locais()` (`R/data_cleaning.R:232`,
  `file.exists` / `length(tse_files) >= 4`) with fail-loud behaviour that asserts every
  expected TSE file is present. **Coordinate with cleanup phase 3 (#34), the fail-silent
  sweep.**
- **Dev/prod store split (C1)** — cleanup phase 1 (#32). Required so the production rebuild
  writes to the production store, not a dev-contaminated one.
- **Deterministic `local_id` (H6)** — cleanup phase 5 (#36). `local_id := .I` is
  run-order-dependent; a deterministic key (with the pre-adoption uniqueness check) is
  required before a reproducible release.
- **C4 test-set-leak fix** — cleanup phase 2 (#33). Required for honest evaluation numbers.
- **Clean full rebuild from committed code**, production mode. Do **not** ship the stale
  2 Aug 2025 artifact (built from uncommitted working-tree code; unreproducible). This run
  is cleanup phase 5's verification rebuild and the source of all published numbers.

## Validation gates (hard — the release ships only if all pass)

Structural tripwires on the production rebuild, extending the testing spec's dev-mode
checks (#20) to the full run:

1. **All years present**, 2006–2024, with a non-empty 2024 partition (~93k stations).
2. **Schema intact** — the documented column set is present and unchanged.
3. **Coordinates not all-NA** — every year has real `final_lat`/`final_long` coverage.
4. **Output files exist** — `geocoded_polling_stations.csv.gz` and `panel_ids.csv.gz`.
5. **Sane per-year row counts** — no year collapses or explodes vs its known scale
   (2024 station count ≈ 93,339, matching the address count 93,337).
6. **Landed 2024 TSE coverage ≥ 92%** (decision 2).
7. **Per-year TSE-coverage regression tripwire** — no TSE-bearing vintage's landed
   coverage falls more than `RELEASE_TSE_JOIN_SLACK` (5 pt) below that vintage's own
   *raw* TSE availability (decision 2). Raw availability ramps from ~51% (2018) to ~94%
   (2024) as TSE progressively geocoded stations, so the gate compares each year against
   its own availability ceiling (`compute_tse_raw_availability()`), not a flat floor a
   sparse pre-2024 vintage could never clear.

Accuracy is reported, not gated (decision 4).

## Dependency summary (execution order)

```
#32 phase1 (C1 dev/prod split) ─┐
#33 phase2 (C4 leak fix) ───────┤
#34 phase3 (fail-loud tse_files)┤
#25 evaluation spec ────────────┴─► #36 phase5 rebuild = the 2024 release run
                                       └─► validate gates ─► docs ─► publish v0.15
```

## Out of scope (this release)

- Citable DOI / Zenodo / `CITATION.cff` — separate issue.
- Replacing `pred_dist` with calibrated uncertainty — methodology thread (#29).
- Any schema (column) change.
- A quantitative old-vs-0.141 diff artifact.
