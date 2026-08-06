# Frozen evaluation baseline — v0.15

The methodology roadmap makes the v0.15 evaluation the **frozen baseline** every upgrade
is measured against ([roadmap](../../specs/2026-07-methodology-roadmap.md), policy 3).
Until this archive existed, those numbers lived only in the production `_targets/` store —
mutable, S3-backed, and overwritten by the next `tar_make()` — and in an untracked
`reports/evaluation_report.html`. These CSVs are the committed copy.

## Files

| File | Source target | Rows |
|---|---|---|
| `accuracy_tables.csv` | `accuracy_tables` | 50 |
| `calibration_rank_filter.csv` | `calibration_check$rank_filter` | 6 |
| `calibration_reliability.csv` | `calibration_check$reliability` | 10 |

ENCE (expected normalized calibration error, `calibration_check$ence`): **24.12**.

Column meanings are in [the evaluation spec](../../specs/2026-07-evaluation-spec.md); the
targets that compute them are in `R/evaluation.R`.

`tse_coverage` is deliberately not archived: it describes the ground-truth density of the
input data, which no methodology upgrade changes, so a future run reproduces it.

## Headline numbers

Station-grouped out-of-fold, over the 257,339 TSE-covered station-years.

| | match rate | median | p90 | within 500 m |
|---|---|---|---|---|
| overall | 100.0% | 46 m | 6.69 km | 74.6% |
| urban | 100.0% | 38 m | 1.66 km | 82.4% |
| rural | 100.0% | 211 m | 17.37 km | 57.2% |

The weakest non-suppressed strata — where an upgrade has the most room and the
no-stratum-regresses half of the adoption gate binds hardest — are the neighborhood-median
match sources (`bairro_cnefe_2017` at 26.9% within 500 m, `bairro_cnefe_2010` at 34.8%)
and `rural:Sul` at 35.7%.

## Provenance

Extracted from the production store with `targets::tar_read(<target>, store = "_targets")`.

- Release: [v0.15](https://github.com/fdhidalgo/geocode_br_polling_stations/releases/tag/v0.15), tagged at `e3c3b10` on 2026-07-12.
- `accuracy_tables`, `calibration_check`, `oof_selected_matches` were built 2026-07-11 21:01.
- `oof_predictions` carries a later build stamp (2026-07-12 09:25, after `trained_model` at
  09:23) than the metrics derived from it. `targets` skips downstream when a rebuild
  reproduces the same value, so the archived numbers are the 2026-07-11 ones either way —
  noted here so the timestamps don't read as a broken chain.
- The store has since had upstream targets rebuilt (`locais_filtered`, 2026-08-05), so
  these downstream metrics are already flagged outdated there and the next production run
  will overwrite them. That is what this archive is for.
