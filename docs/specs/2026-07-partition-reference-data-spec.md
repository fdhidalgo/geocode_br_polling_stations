# Partition reference data instead of broadcasting national tables

**Status:** accepted (grilled 2026-07-10, issue #62)
**Source audit:** `docs/audits/2026-07-pipeline-performance-opportunities.md` (findings T1, T2, T3)

## Purpose (plain language)

The pipeline currently ships whole national reference tables (street/neighborhood
coordinate aggregates, school lists) into every parallel worker task, and each task
then throws away all but ~15 municipalities' worth. Separately, it glues all 27
states' cleaned census (CNEFE) data into single tens-of-gigabytes objects only to
reduce them to small per-municipality summary tables. This spec reshapes both
patterns: summarize the census data per state without ever building the national
object, and hand each matching task only the slice of reference data it needs. The
primary goal is cutting peak memory (the reason the pipeline demands a 50 GB+
machine); the secondary goal is wall-clock time, which improves mainly because
slimmer tasks can run on the wide 28-worker pool instead of the memory-restricted
8-worker pool.

## What the grilling changed about the original plan

The issue (#62) framed this as "the single biggest production speedup." Production
timing metadata (`_targets/meta`) falsified that framing:

| Target | Total CPU | Branches | Max single branch |
|---|---|---|---|
| `panel_ids_by_batch` | 182,111 s | 221 | 7,778 s |
| `cnefe22_stbairro_match_batch` | 108,488 s | 372 | **25,600 s (7.1 h)** |
| `inep_string_match_batch` | 102,299 s | 372 | 9,059 s |
| `cnefe10_stbairro_match_batch` | 79,970 s | 372 | **22,025 s (6.1 h)** |
| `agrocnefe_stbairro_match_batch` | 17,039 s | 372 | 901 s |
| `schools_cnefe22_match_batch` | 14,121 s | 372 | 4,331 s |
| `schools_cnefe10_match_batch` | 13,974 s | 372 | 3,891 s |
| `geocodebr_match_batch` | 7,102 s | 372 | 29 s |
| CNEFE cleaning (2010 + 2022 + dup schools pass) | ~34,500 s | 27–39 | ~2,200 s |
| `cnefe10` / `cnefe22` combines | 539 s | 2 | — |

Conclusions drawn from this evidence:

- **Wall-clock is floored by one municipality.** The slowest matching branch (the
  São Paulo batch) runs ~7 hours of sequential per-municipality CPU that no
  batching or data-shipping change can split. That ceiling belongs to issue #45
  (vectorize the matching inner loops), not to this reshape.
- **The broadcast's deserialization cost is minutes, not hours** (compare
  `geocodebr_match_batch`, same 372-branch shape with tiny dependencies: 29 s max
  branch). The real speed lever hidden in T1 is that national-table-holding
  branches force the heavy match targets onto the 8-worker `memory_limited`
  controller; slice-holding branches can run 28-wide (~3.8 h → ~1.1 h for the
  biggest target).
- **The panel stage was mis-diagnosed.** It broadcasts only `locais_filtered`
  (tiny), already runs on the `standard` controller, and its 50 CPU-hours are
  `reclin2` pair scoring. It is cut from scope.

## Decisions

**D1 — Objective ranking: peak memory > wall-clock > S3 traffic.** The ~7 h
mega-branch wall-clock ceiling is an explicit non-goal, owned by #45.

**D2 — Controller promotion is part of the partition ticket's definition of
done.** After slicing, the five CNEFE-family match targets default to the
`standard` (28-worker) controller, gated by: (a) measured per-branch peak memory in
dev mode, (b) computed production slice sizes for the fattest batch (derivable from
the existing store without a production run). If the arithmetic says 28-wide is
unsafe for the `stbairro` targets, they stay on `memory_limited` — a recorded,
data-driven decision, not a silent skip.

**D3 — Scope: the five CNEFE-family match targets only** (`schools_cnefe10`,
`schools_cnefe22`, `cnefe10_stbairro`, `cnefe22_stbairro`, `agrocnefe_stbairro`
match batches). The panel stage is out. `geocodebr_match_batch` is untouched.
`inep_string_match_batch` joins the slicing mechanism only if a one-off measurement
inside the partition ticket shows `inep_data`'s production object is non-trivially
large. The dead `batch_type` column (`create_panel_municipality_batches`, computed
and wired to nothing) is deleted in a separate cleanup ticket.

**D4 — Slicing mechanism: grouped stems + main-process retrieval (Option A),
proven by a spike first.** For each reference table, one new target joins
`batch_id` (from `municipality_batch_assignments`, the single source of truth for
municipality→batch alignment) and groups by it (`iteration = "group"`). Match
targets use `pattern = map(ref_grouped)` with `retrieval = "main"`: targets ≥1.8
omits the whole stem from the branch subpipeline and ships only the group slice to
the worker (verified in targets NEWS; repo pins 1.12.0). The grouped stems need
persistent memory on the main process while their consumers run.
**Fallback (Option B), specified so it is not a redesign:** a splitter target with
`pattern = map(batch_ids)`, `deployment = "main"`, persistent memory, storing each
slice as its own branch object; match targets map branch-over-branch and workers
fetch only their slice from the store. The spike (ticket 1) picks A or B by
measurement.

**D5 — Fuse aggregation into per-state cleaning; never persist full cleaned
CNEFE.** The full cleaned CNEFE tables have no consumers other than the three
aggregate families (verified: `cnefe10` feeds only `cnefe10_st`/`cnefe10_bairro`;
`cnefe22` feeds only `cnefe22_st`/`cnefe22_bairro`/`schools_cnefe22`). The
per-state cleaning target therefore returns `list(st, bairro, schools)` computed
in-memory; the combined `cnefe10`/`cnefe22` targets and the tens-of-GB per-state
serialized intermediates are deleted. The duplicate 2010 schools read/clean pass
(`schools_cnefe10_by_state`, ~12,000 s) is deleted; schools come from the same
in-memory cleaned table (`clean_cnefe10(extract_schools = TRUE)` already does
this). Accepted trade-off: changes to aggregation code re-run cleaning (~3 h CPU),
and cleaned rows are no longer inspectable via `tar_read` — forensics fall back to
running `process_cnefe_state()` interactively for one state.

**D6 — Invariant assertion at the combine step.** After `rbindlist`-ing per-state
aggregates, `stop()` if any `(id_munic_7, norm_street)` (resp. `norm_bairro`) key
is duplicated. A duplicate is exactly what a municipality spanning two states — or
a state-file mis-assignment — would produce.

**D7 — Equivalence is a detector, not a hard gate.** A committed script,
`tests/integration/equivalence_check.R`, with `snapshot` and `compare` modes,
compares dev-mode (AC/RR) builds at `identical()` strictness after normalizing
data.table internals (keys/indices dropped). Compared targets: the six reference
aggregates, all seven match outputs, `model_data`, `geocoded_locais`. (`panel_ids`
skipped — untouched inputs and code; `trained_model` skipped — if `model_data` is
identical, its unchanged target name/seed makes it deterministic; no RNG exists
anywhere in matching/cleaning/panel code, verified by grep.) **Acceptance is
"every diff explained and accepted," not "no diffs."** Deliberate improvements are
allowed as their own commits, called out in the report. First accepted diff: 2010
schools adopt the `norm_desc != ""` filter, harmonizing with 2022 (currently 2010
keeps empty-description rows; 2022 filters them).
**Production backstop:** dev mode exercises neither S3 nor controller promotion,
so the first production run after merge is diffed against the shipped
`geocoded_polling_stations.csv.gz` / `panel_ids.csv.gz` and the report reviewed.

## Child tickets

1. **Spike: prove the slicing mechanism** (no merged code). Build
   `schools_cnefe22_match_batch` under Option A in dev mode; measure per-branch
   load behavior and main-process memory; check Option B only if A fails. Verdict
   posted on the ticket. `ready-for-agent`; no dependencies.
2. **Fused per-state CNEFE aggregation (T2+T3).** Implements D5, D6; authors the
   equivalence harness (D7); lands the `norm_desc` harmonization as its own
   commit. `ready-for-agent`; independent of the spike; **ships the primary memory
   win on its own.**
3. **Partition the five match targets + controller promotion (T1).** Implements
   D2–D4 using the spike's mechanism verdict; includes the `inep_data` size
   measurement. Blocked by ticket 1; sequenced after ticket 2 (both edit the same
   `_targets.R` region).
4. **Cleanup: delete dead `batch_type`.** Trivial; anytime.

## Consequences

- Ticket 2 restructures the CNEFE targets, so the next production run rebuilds
  everything downstream of CNEFE (several hours). That run doubles as the
  production backstop diff (D7) — the cost is paid once and buys the verification.
- Post-reshape wall-clock expectation, stated honestly: the pipeline remains
  floored at roughly the ~7 h São Paulo `stbairro` branch plus downstream stages
  until #45 lands. This reshape's wins are the 50 GB peak, the S3 round-trips of
  giant intermediates, ~12,000 s of duplicated 2010 cleaning, and ~2–3 h of match
  wall-clock from controller promotion.

## Out of scope

- Vectorizing the per-municipality matching inner loops (issue #45) — the
  wall-clock ceiling.
- Panel-stage partitioning (mis-diagnosed by the audit; see evidence above).
- Model-tuning parallelism, batch-size knobs, storage-format changes (`parquet`
  /`fst`) — noted in the audit's Tier 3, unticketed.
