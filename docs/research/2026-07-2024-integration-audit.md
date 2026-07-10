# Audit: 2024 election-data integration state

**Wayfinder ticket:** [#22 — Audit the 2024 integration state](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/22)
**Feeds:** [#23 — Decide the 2024 validation & release spec](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/23)
**Date:** 2026-07-10

## Purpose (plain language)

The pipeline already pulls in 2024 election data, but nobody has checked whether it
actually works end to end, and no 2024 results have been published. This document
establishes exactly what is wired up, what is broken or half-wired, what the official
2024 data from the electoral authority (TSE) contains, and what it would take to
validate and release a 2024 geocoded dataset. It is the fact base for writing the
release spec (#23); it does not itself decide the release.

## Bottom line

2024 is **partially integrated**. Three separate things have to be true for 2024 to be
"in the pipeline," and only two of them are:

1. **2024 polling-station addresses flow in and get geocoded** — ✅ wired.
2. **2024 stations get panel identifiers linking them across years** — ✅ wired.
3. **2024 official TSE coordinates are loaded as ground truth** — ❌ **not wired in the
   committed code.** The 2024 TSE file exists on disk (and is tracked in git) but is
   omitted from the `tse_files` target.

Because of (3), the committed pipeline cannot: use 2024 official coordinates in the
final output, train or evaluate the model against 2024 ground truth, or report honest
2024 accuracy. A full run *did* complete locally on 2 Aug 2025 and produced 2024
output — but that run used working-tree code that was **never committed**, so it is not
reproducible from `master` today.

## Evidence

### Layer 1 — 2024 addresses are geocoded (wired)

- `_targets.R:512` — `locais_file = "./data/polling_stations_2006_2024.csv.gz"`. This
  combined file includes 2024 and feeds `locais → locais_filtered`, which drives all
  string-matching and model-prediction targets.
- On-disk output `output/geocoded_polling_stations.csv.gz` contains **93,337 rows for
  `ano == 2024`** (out of 944,689 total across 2006–2024).

### Layer 2 — 2024 is in panel-ID creation (wired)

- `_targets.R:607` — panel processing year vector includes 2024:
  `c(2006, 2008, 2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024)`.
- `R/panel_creation.R:363` — the Distrito Federal special case
  `c(2006, 2008, 2010, 2012, 2014, 2018, 2022, 2024)` also includes 2024. (Its omission
  of 2016 and 2020 is **correct**, not a bug: Brasília holds no municipal elections, so
  those municipal years don't exist for DF.)
- `output/panel_ids.csv.gz` exists with `panel_id, long, lat, local_id`.

### Layer 3 — 2024 TSE ground truth is NOT wired (the core gap)

- `_targets.R:553-563` — the `tse_files` target lists **only three files**:
  2018, 2020, 2022. The 2024 file is absent.
- `R/data_cleaning.R:208-245` — `clean_tsegeocoded_locais()` is *written* to accept a
  4th (2024) file behind a guard: `if (length(tse_files) >= 4 && file.exists(tse_files[4]))`.
  With only three files passed, that branch is dead code and 2024 truth is never read.
- `R/data_cleaning.R:232` also carries a **fail-silent guard** (`file.exists` →
  silently fall back to three years). Even if 2024 were re-added to `tse_files`, a
  missing or misnamed file would be swallowed rather than erroring. This is one of the
  fail-silent patterns the code-health audit (#19) and cleanup spec (#21, phase 3)
  target.

**How this happened (commit archaeology):**

- `a4ac469` (2025-06-05, "Integrate 2024 Brazilian election data into pipeline")
  **added** 2024 as the 4th entry of `tse_files` and set the panel year vectors.
- `4768e54` (2025-06-07, "Implement memory-efficient CNEFE processing…") — a commit
  whose message is entirely about CNEFE memory handling — **removed** the 2024 line from
  `tse_files` again. The removal is not mentioned in the commit message and appears to
  be an unintended revert/merge artifact during that refactor. The panel year vectors
  were later restored (they include 2024 today) but `tse_files` was not.

Net effect: the *address* and *panel* integration survived; the *TSE-truth* integration
was silently dropped and never restored on `master`.

### Has the full pipeline ever completed with 2024?

**Yes, once, locally — but not from committed code.**

- `output/geocoded_polling_stations.csv.gz` (mtime 2025-08-02 03:59) and
  `_targets/meta/meta` (mtime 2025-08-02 07:25) show a full **production** run (all
  states, 944k rows) completed on 2 Aug 2025. `dev_mode_flag = FALSE` and
  `dev_mode_flag_value = FALSE` today, i.e. production/S3 mode.
- That output contains 2024 rows with non-empty `tse_lat`. Because `local_id` is a
  **unique per-station-year row index** (`R/data_cleaning.R:419`, `local_id := .I`), and
  the final TSE merge (`finalize_coords`, `R/data_cleaning.R:449-454`) joins on
  `local_id` alone, a 2024 row can only carry a TSE coordinate if a TSE record with
  `aa_eleicao == 2024` was loaded and matched to that row. **The presence of any 2024
  TSE coordinate in the output therefore proves the 2024 TSE file was loaded during that
  run** — i.e. `tse_files` contained four entries at run time.
- Yet `_targets/meta/meta` records the `tse_files` command as the **three-file** version.
  The consistent explanation: after the 2 Aug full run (built with an uncommitted
  four-file working tree), a later partial `tar_make` rebuilt the `tse_files` target with
  the committed three-file code, overwriting that target's metadata without regenerating
  the output CSV. No `git stash` exists and no commit re-adds 2024, so **the code that
  produced the good 2024 output was never committed and is not recoverable from git.**

Conclusion for the release: the on-disk 2024 output **cannot be trusted or reproduced**.
A clean rebuild from committed code (after re-wiring 2024) is a prerequisite.

### What the TSE published for 2024

Direct inspection of `data/eleitorado_local_votacao_2024.csv.gz` (45 MB, `;`-delimited,
Latin-1, section-level — 599,216 section rows):

- Columns include `NR_LATITUDE` / `NR_LONGITUDE` (official coordinates), plus
  `DS_ENDERECO`, `NM_BAIRRO`, `NR_CEP`, `NM_LOCAL_VOTACAO`, etc.
- **93,339 distinct polling stations** (by UF · município · zona · local).
- **87,352 of them (93.6%) carry a valid official coordinate.** This is materially
  higher than the ~90% seen for 2022 and far higher than pre-2018 years — TSE's own
  geocoding coverage has kept improving.

This matters: for 2024, official TSE coordinates alone could cover ~94% of stations
directly, leaving only ~6% to the model. The station count (93,339) also matches the
address count in the pipeline (93,337), so the two 2024 sources are consistent in scale.

### The coverage gap in the (stale) output

In the 2 Aug output, only ~57% of 2024 rows carried a TSE coordinate (~53k of 93k;
counts approximate — see caveat), versus **93.6% available** in the raw TSE file. That
~36-point gap means the merge in `clean_tsegeocoded_locais()` (matching TSE records to
`locais` on `ano · cod_localidade_ibge · nr_zona · nr_locvot`, then dropping `-1`/NA
coordinates) lost a large share of matchable stations even when 2024 *was* loaded.
Station-identity mismatches between the combined `polling_stations_2006_2024` file and
the TSE file, and the municipality TSE↔IBGE crosswalk, are the likely culprits. The
release must investigate and close this gap, not just re-add the file.

> **Caveat on output-derived counts.** R is currently unusable in this repo (broken
> `renv`/`rspm` restore — a separate critical audit finding), so the output was parsed
> with shell tools. Some rows contain embedded commas/quotes in address fields (visible
> as garbage `local_id` values such as `N°141`), which breaks naive column splitting.
> Per-year TSE-coverage counts from the output are therefore **approximate**; the raw
> 2024-TSE-file coverage (93.6%) is clean. Exact numbers should come from the release
> rebuild.

### Downstream consequences of the missing TSE truth

- **Model training** (`R/model.R:290-312`): the training target is the haversine
  distance between each candidate match and the TSE coordinate. Rows without TSE truth
  contribute no labels. With 2024 truth absent, the model is trained purely on
  2018/2020/2022 labels and 2024 stations are pure prediction targets — their accuracy
  is never measured against 2024 truth.
- **Final coordinates** (`finalize_coords`): 2024 stations fall back to model
  predictions instead of the (94%-available) official 2024 coordinates — lower accuracy
  and lower coverage than achievable.
- **Methodology doc** (`doc/geocoding_procedure.qmd`): evaluates on **2018 only** and
  describes CNEFE 2010/2017/2022; it never mentions 2024. Per the map's lockstep
  constraint, any 2024 release requires updating this document. Its accuracy numbers are
  also entangled with the model test-set leak (C4 in the audit), so honest 2024 numbers
  depend on that fix too.

## What a validated 2024 release requires (end to end)

A checklist to hand to the release spec (#23). Ordered roughly by dependency.

1. **Re-wire 2024 TSE truth.** Add `./data/eleitorado_local_votacao_2024.csv.gz` back to
   the `tse_files` target. Replace the fail-silent `file.exists`/`length >= 4` guard
   with fail-loud behaviour (assert all expected files present) — coordinate with
   cleanup spec #21 phase 3.
2. **Verify station-identity matching for 2024.** Diagnose why the TSE→`locais` merge
   loses ~36 points of coverage; fix the join (crosswalk, key formatting) so landed 2024
   TSE coverage approaches the ~94% available.
3. **Depend on the dev/prod store split (C1) and deterministic `local_id` (H6).** The
   cleanup spec already routes 2024's verification rebuild through phase 5 (blocked on
   #23). `local_id := .I` is run-order-dependent; a deterministic key is needed before a
   reproducible release (H6 includes a pre-adoption uniqueness check).
4. **Clean full rebuild from committed code** in production mode; do not ship the stale
   2 Aug artifact. This run is the verification rebuild for cleanup phase 5 and the
   source of honest accuracy numbers.
5. **Evaluate 2024** against 2024 TSE truth (after the C4 test-set-leak fix), so the
   release can state a real 2024 error rate.
6. **Update the methodology doc** (`doc/geocoding_procedure.qmd`) to cover 2024 and
   refreshed accuracy figures (lockstep constraint).
7. **Decide output-schema and release-notes changes** — README / release notes refresh
   is currently fog on the map; sharpen it here.

## Open questions for the release spec (#23)

- Should 2024 stations prefer **official 2024 TSE coordinates** directly (≈94% coverage)
  over model predictions, and is the current "TSE overrides prediction" logic in
  `finalize_coords` sufficient, or does the identity-matching gap need to be closed
  first?
- What is the acceptance bar for the release — a target 2024 coverage %, a max error
  rate vs TSE truth, or parity with the 2022 release?
- Does the release cover **only 2024**, or is it a full 2006–2024 re-release (the
  cleanup fixes change earlier years' numbers too)?
- Should the model be **retrained with 2024 labels included**, or kept as-is with 2024
  purely predicted where TSE truth is absent?
