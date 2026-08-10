# Methodology upgrade roadmap

**Wayfinder ticket:** [#30](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/30)
**Status:** decided 2026-07-10
**Inputs:** the geocodebr tooling survey ([#26](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/26), [doc](../research/2026-07-geocodebr-tooling-survey.md)), the string/record-matching survey ([#27](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/27), [doc](../research/2026-07-string-matching-methods-survey.md)), the LLM-assisted-matching assessment ([#28](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/28), [doc](../research/2026-07-llm-assisted-matching.md)), the match-selection-model assessment ([#29](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/29), [doc](../research/2026-07-match-selection-model.md)), and the evaluation spec ([#25](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/25), [doc](2026-07-evaluation-spec.md)).

## Purpose in plain language

This project geocodes Brazilian polling stations by matching messy address
strings against several reference datasets and letting a trained model pick the
best candidate per station. Four research passes surveyed what modern tooling
and methods could improve that machinery. This document is the decision: which
upgrades happen, in what order, and under what evidence each is adopted. It
turns "we could try X" into a short, sequenced list of issues with explicit
adoption gates, so upgrades are driven by measured accuracy on the refreshed
evaluation rather than by enthusiasm.

## Standing policies

These four decisions govern every item below.

1. **LLM policy — Reading B (pragmatic).** A one-time *offline* LLM step whose
   human-reviewed output is committed to the repo and consumed
   deterministically is compatible with "free/open tools in production": the
   committed artifact is just data. Any model, including paid frontier models,
   may be used offline. **No LLM ever enters the runtime build path.** If an
   LLM step were ever proposed for production, the reproducibility ledger in
   the [#28 assessment](../research/2026-07-llm-assisted-matching.md) applies
   (deterministic/batch-invariant inference, pinned weights/engine/seed) — but
   the offline-committed-artifact pattern is always preferred.
2. **Standard adoption gate.** An upgrade is adopted only if, measured on the
   [evaluation harness](2026-07-evaluation-spec.md) (station-grouped
   out-of-fold predictions, headline = median distance + %-within-500 m,
   always joint with match rate, stratified urban/rural × region × vintage):
   - it improves at least one headline metric at an equal-or-better match
     rate, **and**
   - no urban/rural × region stratum gets meaningfully worse.

   No fixed numeric threshold is pre-committed; the measured delta is weighed
   against the upgrade's complexity cost at review time, but the measurement
   protocol itself is fixed here and is not re-litigated per issue.
3. **Release interlock.** The 2024 release (v0.15,
   [release spec](2026-07-2024-release-spec.md)) ships the **current**
   methodology; its leakage-controlled evaluation is the **frozen baseline**
   all upgrades are measured against. Methodology upgrades land after the
   v0.15 rebuild and roll up into a future v0.16 release. The release timeline
   never waits on methodology work, and upgrade deltas are never confounded
   with the cleanup fixes riding v0.15.
4. **Confidence column (schema change).** The released dataset gains a
   calibrated per-station uncertainty column: a 90% upper-bound distance
   quantile, `conf_dist_km`, landed by wave 2g. The miscalibrated `pred_dist`
   was removed rather than relabeled — the two measure different things and
   were never interchangeable. Achieved coverage is a **reported diagnostic**
   in the evaluation report, not a blocking gate; the original gate was dropped
   deliberately, so a reader judges the number rather than the pipeline
   refusing to ship it. v0.15 shipped without the column.

## Wave 1 — unconditional upgrades

Ticketed now, no adoption gate (each is measurement infrastructure or
unconditionally right), blocked on the v0.15 rebuild
([#36](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/36)) per
policy 3. Their accuracy effects are still *measured* on the harness — the
absence of a gate means they land regardless, not that they go unmeasured.

- **(a) geocodebr modernization.** Upgrade the pin from 0.2.1 toward current
  0.6.x (re-auditing `match_geocodebr_muni()` against the breaking changes,
  notably the v0.5.0 `data.table`→`data.frame` output); feed `numero`,
  `bairro`/`localidade`, and `cep` instead of stripping them; replace the
  synthetic `precisao`-derived distance in `R/model.R` with real
  `desvio_metros` and `contagem_cnefe` features; reconcile nested parallelism
  (geocodebr's core usage vs. the `crew` controllers) deliberately; pin the
  CNEFE data release used. One issue — one code region.
- **(b) Field-decomposed similarity features.** Compute per-field similarity
  (name / street / bairro / number as separate `stringdist` columns) feeding
  the selection model, instead of whole-string similarity only. Cheapest win;
  shared top feature gap of the #27 and #29 docs.
- **(c) Heuristic-baseline comparison.** Score a trivial deterministic
  selection rule on the same harness so "is the model worth it" has a number.
  Rides the evaluation harness; measurement-only.
- **(d) geocodebr-vs-TSE ground-truth comparison.** The decisive experiment
  the #26 survey found missing: compare geocodebr coordinates vs. the custom
  pipeline's vs. TSE ground truth on the covered set. Cheap (data already in
  the pipeline); its result operates the wave-3 gate on retiring the bespoke
  2022 CNEFE tables.

## Wave 2 — gated bets

Ticketed now, each carrying the standard adoption gate (policy 2) plus any
extra gate noted; blocked on their wave-1 inputs and the v0.15 baseline.

- **(e) Local embedding similarity feature.** A free/open local embedding
  model (Serafim-PT / BGE-M3 / LaBSE via `reticulate` or `text`), precomputed
  and cached as a `targets` object, as an arbitrator feature for the
  name/street component. **Extra gate:** must beat wave 1b (field
  decomposition) *alone* — embeddings have to earn the Python dependency.
  Blocks on (b).
- **(f) Cross-source consensus features.** Do independent candidate sources
  agree on a location — the #29 doc's highest expected gain per unit effort.
  Blocks on (a) and (b), so the consensus is computed over the enriched
  candidate set.
- **(g) Calibrated distance quantile.** *Landed.* Replaced the exported
  `pred_dist` (a biased-low, uncalibrated back-transformed geometric mean)
  with `conf_dist_km`, a calibrated upper-bound quantile: LightGBM quantile
  objective, wrapped in one-sided conformalized quantile regression calibrated
  on municipality-grouped held-out folds. Achieved coverage, cut by region /
  urban-rural / vintage and reported beside median bound width, is a diagnostic
  in the evaluation report rather than a gate (policy 4). The change also fixed
  a latent bug: the outcome log-transform was a recipe step marked `skip`, so
  every hyperparameter had been selected on a metric comparing log-scale
  predictions to kilometre-scale truth.
- **(h) Similarity-based blocking.** Replace the exact-token
  `prefilter_by_common_words` with similarity-based blocking
  (`blocking::pair_ann` or `zoomerjoin`), closing a known recall leak and
  removing an `O(n·m)` loop (also flagged by the code-health audit). Gate is
  the standard one — in particular, match rate must not regress.

## Wave 3 — conditional ledger (not ticketed)

Recorded here so none is re-litigated; each graduates to an issue only when
its trigger fires.

| Item | Trigger / gate |
|---|---|
| Retire the bespoke 2022 CNEFE street/neighborhood tables in favor of geocodebr's 2022 surface | Wave 1d shows geocodebr parity-or-better on 2022-referenced matches. The 2010/2017 multi-vintage references, INEP matching, the arbitrator, panel linkage, and station-specific normalization stay regardless — geocodebr does none of these. |
| `libpostal` field parsing | Wave 1b/2e are demonstrably bottlenecked by normalization quality (budget: ~2 GB model + C binding). |
| Ranking / calibrated-classification reframe of the selection model | Wave 1c shows the model earns its keep **and** the reframe beats distance regression on the harness. |
| Offline LLM dictionary-expansion pass (synonyms for `normalize_school()` etc.) | Per policy 1 (Reading B): LLM proposes, human reviews, vetted list committed, pipeline reads only the committed file. Worth doing once residual normalization error is characterized on the harness. |
| Offline LLM hard-case adjudication / label generation | Only after wave 2f (consensus features) is exhausted — every published LLM win is vs. rules, never vs. a tuned domain model with consensus features; measure that delta first. |
| `fastLink` (panel linkage, missing-data model) | Panel-linkage errors are shown to trace to missing fields. Keep `reclin2` otherwise. |
| `Splink` (panel linkage at scale) | Throughput ever binds. It does not today. |
| `fuzzylink` | **Evaluation-only, permanently** (paid API + data egress) — an upper-bound probe, never production. |
| Deep neural entity matching (Ditto / HierGAT) | Fog: only if name-matching is shown to dominate residual error and labels + GPU budget exist. |
| LLM candidate generation / blocking, or asking an LLM for coordinates | **Ruled out** — worst cost/latency and documented geographic hallucination biased against Brazil. |

## Sequencing summary

```
v0.15 release rebuild (#36) ── frozen baseline
        │
        ├─ wave 1: (a) geocodebr modernization   (b) field-decomposed features
        │          (c) heuristic baseline        (d) geocodebr-vs-TSE comparison
        │
        ├─ wave 2: (e) embeddings   ← (b)
        │          (f) consensus    ← (a), (b)
        │          (g) calibrated quantile + confidence column
        │          (h) similarity blocking
        │
        └─ wave 3: conditional ledger above; graduates only on its triggers
                   → adopted upgrades roll up into v0.16
```

The methodology document (`doc/geocoding_procedure`) is updated in lockstep
with every adopted change, per the standing map constraint.
