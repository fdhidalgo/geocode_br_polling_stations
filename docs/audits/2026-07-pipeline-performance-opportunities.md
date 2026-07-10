# Pipeline performance opportunities (2026-07)

**Purpose (plain language):** The full production pipeline is slow (hours) and
memory-hungry (50 GB+). This document is a scouting report: it lists the changes
that would actually move the needle on run time and peak memory, ranked, so we can
turn the worthwhile ones into work. It does **not** change any code — it is the
survey that feeds the tickets.

The findings came from four focused reads of the hot paths (CNEFE ingestion, string
matching, panel creation, model training + `targets`/`crew` infrastructure). The
load-bearing claims were verified directly against the source.

## Data scale (context for weighting)

- CNEFE 2022 input: **3.8 GB compressed** on disk (~tens of GB in RAM when combined).
- CNEFE 2010 input: **~1 GB compressed**.
- Production: ~5,570 municipalities; string-match `batch_size = 15` → **~370 branches**;
  27 states.

---

## The dominant theme

One architectural anti-pattern is responsible for most of the cost and surfaced
**independently in three stages**: **the pipeline ships whole national reference
tables into every parallel branch, which then filters down to a handful of
municipalities.** Fixing it is the same shape each time — *partition the reference
data once, branch over the slice* — and it is both the largest speed win and the
largest memory win available.

---

## Tier 1 — the real needle-movers (→ Track B, grilling first)

These are architectural reshapes touching ~10+ targets across the matching and panel
stages. They share one design and one hard invariant: **the output must stay
bit-identical.** That invariant, and how we prove it (a dev-mode AC/RR equivalence
check), is why these get grilled and spec'd before implementation rather than patched
directly.

### T1. Stop broadcasting national reference tables to every dynamic branch
Each `*_match_batch` / `panel_ids_by_batch` branch has `retrieval = "worker"` and
receives a **national** table (CNEFE streets/neighborhoods, school lists, or the full
`locais_filtered`), then filters to its ~15 municipalities internally via an unkeyed
linear scan (`R/utilities.R:229-351`; panel: `R/panel_creation.R:336`). Across ~5
CNEFE-family match targets × ~370 branches that is **1,800+ whole-table
fetch-and-deserialize events** (S3 downloads in production) to use a sliver of each,
with up to 8–28 copies resident concurrently.
**Fix:** pre-split each reference table by `batch_id` into a list, branch over the
slice (`pattern = map(batch_ids, ref_by_batch)`); `setkey(id_munic_7)`. Contained to
~10 targets + helpers; dev-mode testable.
**Win:** likely the single biggest production speedup + large peak-memory cut.
**Risk:** medium (branching refactor; keep muni→batch alignment exact).

### T2. Stop materializing the CNEFE national tables whole
`cnefe10` / `cnefe22` (`_targets.R:326-368`) rbind all 27 states into one
tens-of-GB object, consumed **only** by the street/neighborhood median aggregations
and the school extract — three separate deserializations of `cnefe22`, with no
`resources` override, so they can run concurrently on the 28-worker `standard`
controller. Every aggregation is `median(...) by (id_munic_7, ...)`, and a
municipality never spans two states.
**Fix:** push the aggregation into per-state dynamic branches, `rbindlist` the small
results, and **delete the combined `cnefe10`/`cnefe22` targets** (nothing else reads
them).
**Win:** biggest memory reduction — removes the peak that most likely sets the 50 GB
requirement — plus removes 5 giant deserializations.
**Risk:** medium (restructures 5 targets, deletes 2). Rests on the
municipality-never-spans-state invariant, which holds by construction of `id_munic_7`.

### T3. Stop reading + cleaning CNEFE 2010 twice
`cnefe10_cleaned_by_state` (`extract_schools = FALSE`) and `schools_cnefe10_by_state`
(`extract_schools = TRUE`) each `fread` and fully clean all 27 2010 state files; the
schools target is just a filtered subset. CNEFE 2022 already does this right
(`get_cnefe22_schools(cnefe22)`, no re-read).
**Fix:** derive 2010 schools from the cleaned output via a `get_cnefe10_schools()`
helper. Subsumed naturally by T2's per-state restructuring.
**Win:** roughly halves 2010 ingest wall-clock.
**Risk:** low-medium (preserve the `norm_desc != ""` filter).

---

## Tier 2 — cheap, low-risk wins (→ Track A, ticket-ready)

### T4. Delete the dead `norm_address` column computed on ~80M rows
**Verified dead:** `norm_address` is written at `R/data_cleaning.R:224` (2022) and
`:869` (2010) and read nowhere. Producing it runs the most expensive normalization
pass (`stringi` Latin-ASCII + ~10 regex) over every CNEFE row. The raw `address`
field it derives from is also unread by any matcher. In `clean_cnefe10`, removing
`address` additionally makes `num_endereco_char` and `dsc_modificador_nosn`
(`:777-778`) dead (`street` does not use them).
**Win:** removes ~1/3 of per-row CNEFE cleaning CPU + one long character column of
memory/serialization. **Risk:** low (dead-code removal; verify no reader first).

### T6. Vectorize the 2010 DMS→decimal coordinate conversion
`convert_coord()` / `convert_coords_checked()` (`R/data_cleaning.R:679-739`, used at
`:849-851`) parse coordinates one string at a time with a `tryCatch` per value, over
tens of millions of rows. (2022 already has numeric lat/long and skips this.)
**Fix:** vectorize with `data.table::tstrsplit` + vectorized arithmetic and sign flip
for S/W/O, preserving the NA edge cases (<4 tokens → NA, unparseable → NA) and the
all-NA-stop / NA-rate message in `convert_coords_checked`.
**Win:** large constant-factor speedup on the 2010 coordinate step; compounds with T3.
**Risk:** low-medium (needs a test comparing old vs. new on a sample).

---

## Tier 3 — noted, smaller or already tracked

- **T5 (already tracked as issue #45):** the interpreted exact-token prefilter
  (`prefilter_by_common_words`, `R/string_matching.R:17-42`) and the panel
  word-blocking per-pair loop (`R/panel_creation.R:701-721`) should become vectorized
  token-blocking joins. Highest correctness risk (changes candidate selection). See
  issue #45.
- **Model tuning parallelism:** `tune_race_anova` runs `allow_par = FALSE`
  (`R/model.R:371`) with ~50 candidates one fit at a time, relying on LightGBM's
  within-fit threads (`num_threads = 0`), which saturate well below 28 cores. Could be
  2–5× but nested parallelism under `crew` is fiddly. Also `grid_n = 50` → ~20–25.
- **Cheap cleanups:** redundant forced `gc()` in hot loops (panel + CNEFE clean),
  logging-only rescans (`R/utilities.R:338-340`), `data_quality_monitoring`'s
  `tar_cue(mode = "always")` re-scanning the full national output each build, and
  qs2 high-compression on read-heavy giant tables (consider `parquet`/`fst`).

---

## Tickets spawned from this document

- **Track A (ready-for-agent):** T4 (dead `norm_address`) → **#60**; T6 (vectorize
  DMS) → **#61**.
- **Track B (grilling, ready-for-human):** T1 + T2 + T3 as one "partition instead of
  broadcast" reshape → **#62** (grill → spec → tickets).
- **Already open:** T5 (interpreted prefilter/word-blocking loops) → **#45**.
