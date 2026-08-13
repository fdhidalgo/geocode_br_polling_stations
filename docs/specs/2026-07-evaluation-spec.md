# Evaluation spec: honest held-out accuracy + `conf_dist_km` calibration

**Ticket:** [#25 — Decide the evaluation spec](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/25)
**Feeds:** [#30 — methodology upgrade roadmap](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/30), [#36 — 2024 release run / cleanup phase 5](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/36)
**Builds on:** [#24 — evaluation survey](docs/research/2026-07-evaluation-survey.md)
**Date:** 2026-07-10
**Status:** execution-ready spec (planning output of the wayfinder map #18)

## Purpose in plain language

This pipeline produces coordinates for Brazilian polling stations, but it has no
honest, repeatable way to state *how accurate those coordinates are*. This spec defines
that evaluation. It says what to measure (positional accuracy as a distribution, not a
single number), how to measure it without cheating (a held-out split so the model is
scored on stations it never trained on), how to check that the pipeline's own
confidence score is trustworthy, and where the numbers live (rebuilt with the pipeline,
so the public methodology document stays in sync). It deliberately does *not* solve the
one problem no cheap method can solve — that the stations the model actually determines
are the ones with no reference coordinate to check against — and instead reports that
honestly as an extrapolation, while laying the groundwork (a validated Google reference)
for closing it later.

The decision this spec records: adopt the survey's **Design A** (honest held-out TSE
evaluation) plus the **calibration half of Design B**. Drop geocodebr as an evaluation
benchmark. Defer the manual gold set (Design C) and the independent Google-on-uncovered
run to future efforts; this round validates Google against TSE first.

---

## 1. The central constraint (why the design is shaped this way)

Three facts about the current pipeline (established in the survey) fix everything below:

- **Where TSE has a coordinate, the output *is* the TSE coordinate**
  (`merge_geocoded_locais()`, `R/data_cleaning.R:457`:
  `final_long := ifelse(is.na(tse_long), pred_long, tse_long)`). The model-selected
  coordinate reaches the output **only for TSE-uncovered stations**.
- **TSE coordinates are the model's training target** (`make_model_data()` /
  `train_model()`, `R/model.R`): the models fit `log(haversine-to-TSE)` on match features,
  and the best match per station is the one with the lowest expected error
  (`select_best_candidate()`).
- **TSE coordinates are field-collected (GEL system), not centrally geocoded** — so they
  are genuinely independent of any CNEFE-based geocoder, which is what qualifies them as
  ground truth. Their weaknesses here are (a) they are the training target, so evaluation
  needs a strict held-out split, and (b) coverage varies by state and year.

> **Amendment 2026-08-11 ([#143](https://github.com/fdhidalgo/geocode_br_polling_stations/pull/143)):**
> when this spec was written, one quantile model did both jobs — the pipeline fit the 90th
> percentile of `log(haversine-to-TSE)` and selected each station's match on the smallest
> bound. The pipeline now fits two models on the same features and split: an L2 model whose
> expected error does the selecting, and the quantile model, which still produces the
> published `conf_dist_km` after the conformal correction. Selecting on the bound favored
> candidates whose error was predictable over candidates whose error was small, costing
> ~4 points of within-500 m accuracy. Nothing in this spec's *design* changes — the
> protocol, metrics, strata, and calibration checks all still apply — but three passages
> described the old rule and have been updated in place: this bullet, §3's per-station
> selection requirement, and §7's opening sentence. The bound is still what §7 validates;
> it is simply no longer what ranks candidates.

**Consequence.** The stations whose output the model actually determines (TSE-uncovered)
are exactly the ones with no reference. Any accuracy number is measured on the
TSE-*covered* subset and *extrapolated* to the uncovered subset. This spec makes the
covered-subset measurement honest and reproducible, and reports the extrapolation
explicitly rather than hiding it.

---

## 2. What the spec delivers

Two evaluation surfaces, hybrid-sited:

- **Pipeline targets** (rebuilt every run; feed the methodology doc in lockstep):
  1. TSE coverage-by-year × state.
  2. Station-grouped k-fold out-of-fold (OOF) predictions over the covered set.
  3. Stratified accuracy tables (median / percentiles / %-within-threshold, joint with
     match rate), with small-cell suppression.
  4. The `conf_dist_km` calibration check (coverage + sharpness + rank-and-filter).
- **A thin Quarto report** that renders the targets above for human reading and adds the
  one-time **frozen-Google reference-validation** (Google-vs-TSE agreement on a covered
  sample).

Everything is free/open except the one-time Google API call, whose *output* is committed
as a frozen artifact so the report reading it stays reproducible.

---

## 3. The honest held-out protocol

**Mechanism: station-grouped k-fold cross-validation, evaluated on out-of-fold
predictions over the entire covered set.** Not a single 20% holdout — the covered subset
is already a limited, biased slice of all stations, and OOF uses all of it, tightens
per-stratum estimates, and directly feeds the calibration check.

Requirements (all mandatory):

- **Station-grouped folds.** Every candidate row for a given station must be in the same
  fold; the group key is the station identity used elsewhere in the pipeline. Splitting a
  station's candidates across folds leaks the TSE target.
- **Split upstream of tuning.** The fold assignment is created once, before hyperparameter
  tuning, so tuning cannot peek at the evaluation stations. This consumes the C4
  test-set-leak fix already scoped in cleanup phase 2
  ([#21](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/21) /
  [#33](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/33)) — this spec
  does not re-implement it; it depends on it.
- **Per-station selected match from OOF scores.** For each covered station, rank its
  candidates by their OOF expected error, select the best, and score that pick's haversine
  distance to the TSE coordinate. This is the number that enters the accuracy tables. Both
  models are refit per fold, so the bound reported alongside each pick is out-of-fold too.
- **k** is an execution detail (5 or 10); pick for stable per-stratum cells given TSE
  coverage density (§6).

Production remains a single full-data fit (predicting for all stations including
uncovered); OOF is the *evaluation* substrate, not the production path.

---

## 4. Metrics and stratification

### 4a. Metrics

Positional accuracy is a right-skewed distribution, so lead with the distribution, not
the mean/RMSE.

- **Headline numbers:** median haversine error **and** %-within-500 m.
- **Full table per stratum:** percentiles **50 / 90 / 95 / 99**; share within **100 m /
  500 m / 1 km**; and **match rate** (share of stations geocoded) — accuracy and match
  rate **always reported jointly**, never accuracy alone (they trade off).
- Error is computed against the model-selected coordinate (`pred_long/lat`), **not** the
  TSE-substituted `final_*` (matching the existing methodology-doc computation).

### 4b. Stratification axes

Every accuracy table is cut by:

- **urban/rural** (the dominant axis; rural is multiples worse),
- **region**,
- **vintage (election year)**,
- **match source** (which matcher won: INEP schools `match_inep_muni()`, CNEFE schools
  `match_schools_cnefe_muni()`, CNEFE street/neighborhood `match_stbairro_cnefe_muni()`,
  geocodebr `match_geocodebr_muni()`).

Deferred (future fog): a **spatial-autocorrelation diagnostic** (Moran's I on residuals +
a residual map) to catch localized failure pockets that stratified medians hide.

### 4c. Vintages

Evaluation covers **2018 / 2020 / 2022 / 2024** only — TSE coordinates begin with the
2018 vintage. The pipeline geocodes 2006–2024, but pre-2018 station-years have no TSE
reference and are **unmeasurable**; they fold into the extrapolation caveat (§5), never
into a silent headline. Anchor each station-year to its same/nearest-vintage TSE
reference (2018/2022 general, 2020/2024 municipal — the active-station sets differ).

---

## 5. The uncovered subset (reported honestly, not solved)

The TSE-uncovered stations — the ones the model determines — are reported this round as
**explicitly extrapolated / unmeasured**. The accuracy tables carry an unambiguous
caveat that headline numbers are measured on the covered subset and assume carry-over.

Two paths to actually *measure* the uncovered subset are **deferred**, each as future
fog, not part of this spec's execution:

- **Google on the uncovered subset** — gated on the covered-only Google validation (§7)
  showing Google is trustworthy enough relative to field-GPS TSE.
- **Design C manual gold set** — a stratified, dual-rater, adjudicated audit (Street View
  urban, satellite rural); the only design that quantifies TSE's own noise floor and
  directly scores uncovered stations. High labor; a future effort.

**geocodebr is not used** to bound the uncovered subset. It is CNEFE-based and so is this
pipeline, so their errors are correlated: agreement confirms CNEFE-consistency, not
correctness, and (critically) the two agree precisely on the hard stations where both are
confidently wrong — so a triage signal calibrated on covered stations would transfer
*false confidence* to the uncovered ones. Its disagreement signal is also largely
redundant with the pipeline's own candidate ranking. geocodebr's methodology role
(features, deeper adoption) stays in
[#26](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/26) /
[#30](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/30); it has no role
in this evaluation.

---

## 6. TSE coverage density

TSE coordinate coverage (non-missing, non-`-1` share per distinct station) is unpublished
and varies by state and year; it determines how much real ground truth each stratum has.

- **Compute coverage by year × state as a first-class pipeline target** — a cheap count
  over files already ingested — and surface it alongside every accuracy table so numbers
  are read against their ground-truth density.
- **Small-cell suppression:** report a stratum's accuracy only above a minimum held-out N;
  below the floor, flag/suppress rather than publish a noisy median. The floor is an
  execution parameter (document the chosen value).

---

## 7. `conf_dist_km` calibration check

Validates the distance bound the pipeline publishes for each station's selected match.
Runs on the OOF predictions from §3. The conformal correction is derived inside each fold
from municipalities held out of that fold's fit, so coverage measured here is a test of the
published number rather than a restatement of its construction.

- **Coverage and sharpness (headline artifact):** the share of covered stations whose
  realized error falls inside their bound, against the nominal 90%, cut by urban/rural,
  region, and vintage. Median bound width is reported in every cell: coverage alone can
  always be bought with width, so the two are only meaningful together. Conformal
  guarantees coverage *marginally* and promises nothing per stratum, which is exactly why
  the cuts are reported — a rural cell below nominal is the expected failure mode.
- **Conditional coverage across the bound's range:** bin by the bound itself, report
  coverage per decile. An adaptive bound has to hold at both ends, not only on average.
- **Rank-and-filter demonstration:** sort covered stations by their bound; show that
  dropping the widest-bound tail *monotonically* lowers realized median error and raises
  %-within-500 m. Coverage says the bound is honest; this says it is also informative.

Coverage is a **reported diagnostic, not a release gate** — the column ships with whatever
coverage it achieves, and the report is where a reader judges it. (Issue
[#44](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/44) originally scoped
it as a blocking gate; that was dropped deliberately.)

All of this is measured on TSE-covered stations, which skew urban and easier to locate,
while the column ships for uncovered stations too. There the guarantee is an extrapolation,
not a measurement; closing that gap needs the gold set of §Design C.

---

## 7a. Trivial-heuristic baseline (added by [#40](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/40))

The harness above says how accurate the pipeline is. It does not say whether the *tuned
LightGBM selector* is what makes it accurate. This section adds the comparison that
answers that — wave 1c of the methodology roadmap, and the gate on the wave-3
ranking/classification reframe (§6b of the match-selection assessment). Measurement only;
no production behavior changes.

**The rule.** Per covered station, take the highest-precedence candidate available,
breaking ties *within* a rank on the smallest string distance:

| rank | candidate types | what it is |
|---|---|---|
| 1 | `schools_inep_name` | INEP school registry, matched on school name |
| 2 | `schools_cnefe_name_2022`, `schools_cnefe_name_2010` | CNEFE school establishment, matched on name |
| 3 | `schools_inep_addr` | INEP school registry, matched on address line |
| 4 | `geocodebr` | address geocoded by `geocodebr` |
| 5 | `st_cnefe_2022`, `st_cnefe_2010`, `st_agrocnefe_2017` | median coordinate of the matched street |
| 6 | `bairro_cnefe_2022`, `bairro_cnefe_2010`, `bairro_agrocnefe_2017` | centroid of the matched neighborhood |

Precedence runs most-specific-first: a reference that locates the building, then one that
locates the address, then two aggregates that only stand in for it. Census vintages of the
same reference share a rank — they are the same kind of reference differing only in year,
so `mindist` decides between them rather than an invented vintage preference.

**Why the tie-break stays inside a rank.** `mindist` is not comparable across ranks — it
is Jaro-Winkler computed over different fields (name / street / neighborhood / address
line), and absent for `geocodebr`. A 0.2 name distance and a 0.2 street distance are not
the same evidence. Within a rank it *is* like-for-like (same matcher, same field), so the
"smallest `mindist` wins" variant from the assessment is not implemented.

The rank table is the baseline's entire definition, so it is exhaustive over the candidate
types the modeling table emits and the selector **errors** on an unranked type: a new
candidate source has to be placed deliberately, not default silently to the bottom. It
lives inside `select_baseline_candidates()` in `R/evaluation.R`, its only consumer.

**Protocol.** The baseline is scored on the *same* covered candidate rows and the *same*
station universe as the model's out-of-fold picks. It trains on nothing, so it has no
fold structure and nothing to hold out. Because both selectors rank the same candidate
rows, a station geocodes under one exactly when it geocodes under the other: **match
rates are identical by construction** and the comparison is pure accuracy. The harness
asserts this rather than assuming it.

**Reported.** Median error and %-within-500 m, baseline vs model with signed deltas, for
every stratum except the match-source cut — each selector partitions stations by
whichever source *it* picked, so that cut holds different stations under the two and a
per-level delta would not be like-for-like. The source mix itself is reported side by
side instead.

---

## 7b. geocodebr vs the pipeline against TSE ground truth (added by [#41](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/41))

`geocodebr` is one candidate source among several, and how good its coordinates actually
are has never been measured — the [tooling survey](../research/2026-07-geocodebr-tooling-survey.md)
(§1.7, §4c) found no published benchmark and no in-repo comparison. This section adds one:
wave 1d of the methodology roadmap. Measurement only; no production behavior changes.

**What it answers.** geocodebr distributes its own aggregation of the 2022 CNEFE, which is
the same source the project's bespoke 2022 street and neighborhood reference tables are
built from. If geocodebr's surface is at parity on the stations those tables currently win,
the tables can be retired (roadmap wave 3). If it is not, they stay. Nothing else is on the
table: the 2010/2017 multi-vintage references, INEP matching, the arbitrator, panel linkage,
and station-specific normalization are outside geocodebr's scope regardless.

**The third selector.** `select_geocodebr_candidates()` takes geocodebr's own coordinate for
every covered station it resolved, read out of the modeling table rather than recomputed, so
a geocodebr hit the pipeline could not score (a coordinate with no `desvio_metros`) is absent
here exactly as it is absent from the model's candidates. Its `match_source` is the precision
tier the cascade landed on (`numero` / `numero_aproximado` / `logradouro` / `cep` /
`localidade` / `municipio`) — for a single-source selector that is what "which source produced
this coordinate" means, so the standard §4b source cut becomes the precision-tier ladder for
free. It joins the same covered universe as the other two selectors and gets the same §4a
metric ladder.

**The head-to-head.** Unlike the §7a baseline, geocodebr and the model do *not* geocode the
same stations: geocodebr resolves some the pipeline cannot and misses others it can. So
`compare_geocodebr_to_model()` computes each cell's metrics on the stations where **both**
produced a coordinate — a median gap is then a real accuracy difference, not a coverage
difference wearing one — and reports each side's coverage over the whole cell alongside,
because parity on the intersection means nothing if geocodebr resolves far fewer stations.
Stations geocodebr never resolved form their own tier level (`sem_resultado`) rather than
dropping out of the cut.

**Two universes, five cuts.** Every cell is reported over *all covered stations* and over the
`cnefe22_winner` subset — the stations whose out-of-fold winning match is `st_cnefe_2022` or
`bairro_cnefe_2022`. (CNEFE-2022 *schools* are matched on establishment name against a
different table and are not part of the proposed substitution.) Each universe is cut overall,
by urban/rural, by region, by urban/rural × region, and by geocodebr precision tier.

**Delta orientation, the rule for both comparison tables.** A delta is always *the subject of
the section's question* minus *what it is compared against*, so a negative median delta and a
positive within-500 m delta always favour the subject. §7a asks whether the model earns its
keep, so its delta is model minus heuristic; §7b asks whether geocodebr could replace the
tables, so its delta is geocodebr minus model.

**Reading the gate.** Parity-or-better on the `cnefe22_winner` universe, at comparable
coverage, is what retires the tables. Read coverage through the tier cut, not the match
rate: the cascade bottoms out at the municipal centroid, so geocodebr returns *something*
for nearly every station and its match rate is ~100% by construction — a municipal centroid
is a coordinate, not a located station. If geocodebr only reaches parity at
`numero`/`logradouro` precision, the substitution is partial at best, since the bespoke
tables are what currently carry the stations that fall to coarser rungs.

---

## 7c. Panel coordinate quality (added by [#142](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/142))

Everything above scores a station-year against its own ground truth. A panel is a different
object: it links one polling station across elections and publishes a **single** coordinate
that all of its station-years inherit. So it makes its own selection — which member year's
coordinate to hand to the rest — and that choice needs its own measurement, because a panel
coordinate is wrong at a member year whenever the year it came from was wrong.

**What changed.** `make_panel_ids()` ranked members on the published `conf_dist_km`. That is
the same inversion §1's amendment describes at the candidate level: the bound is a calibrated
upper bound, so ranking on it prefers a coordinate whose error is *predictable* over one whose
error is *small*. Members are now ranked on expected error (`final_logmean`, the selection
model's prediction for the coordinate that shipped, threaded through `geocoded_locais` as an
internal column). Ties still break toward the most recent year, and a TSE-covered year still
sorts first: its error is zero, so its `final_logmean` is `log(0)` — `-Inf`, the same way its
`conf_dist_km` is 0. The infinity is deliberate. A finite floor would make ground-truth
precedence depend on no LightGBM prediction ever falling below it, which nothing enforces.

**The measurement.** `compute_panel_coord_accuracy()` runs both rules over the same panels and
scores each rule's panel coordinate against **every covered member's** TSE coordinate, on the
§4a ladder, cut overall and by the §4b axes. Delta orientation follows §7b's rule: the subject
is the shipped rule, so the delta is expected-error minus bound.

**Why it withholds the TSE substitution.** It scores out-of-fold *model* coordinates only. In
production, a panel holding any covered year ships that year's ground truth under either rule,
so the two rules can only diverge on panels with **no** covered year — precisely the panels
with no truth to measure. Withholding the substitution makes the covered panels stand in for
the uncovered ones; it is the same extrapolation §1 makes for stations, one level up.

**Reading it.** The table reports the share of station-years whose shipped coordinate actually
differs between the rules. Every station-year where the rules agree dilutes the metric deltas
toward zero, so the deltas are only interpretable against that share.

---

## 8. The Google reference-validation (covered-only this round)

Purpose: quantify **Google's own error budget** relative to field-GPS TSE *before*
spending trust on Google for the uncovered subset (quantify the reference's error before
using it as one).

- **Sample:** covered stations only, stratified by urban/rural × region; on the order of a
  few thousand points (Google Geocoding API is ~$5/1,000, so cost is not binding; sample
  for rate limits and a tractable frozen artifact, not for cost).
- **Frozen artifact:** commit the Google results (as `data/google_geocoded.csv` was),
  so the report reading them is reproducible even though the API call is not free to rerun.
  The stale 2020 snapshot's unreproducibility was a named weakness; do not repeat it.
- **What it reports:** the Google-vs-TSE agreement distribution (same metric ladder as
  §4a) — i.e., how close Google lands to field-GPS TSE, by stratum. This is a
  *reference-validation*, **not** a release gate and **not** an accuracy number for the
  pipeline.
- **Sets up:** a future Google-on-uncovered run (fog, §5), unlocked only if this
  validation shows Google's error budget is well below the errors we're trying to measure.

---

## 9. Siting, lockstep, and gating

- **Pipeline targets:** coverage (§6), OOF predictions (§3), accuracy tables (§4),
  calibration check (§7), heuristic baseline and its comparison table (§7a), geocodebr's
  selector and its head-to-head table (§7b) live in
  `_targets.R`, rebuilt every run. Follow the readability
  rule — helper functions in `R/`, not long inline blocks. Assign memory-heavy targets to
  the `memory_limited` crew controller as needed.
- **Methodology doc in lockstep:** `doc/geocoding_procedure.qmd` ("Estimating Geocoding
  Error") reads the pipeline-produced numbers; it is updated in the same change as any
  evaluation change (a standing project constraint).
- **Thin Quarto report:** renders the targets + the frozen-Google validation (§8) for
  human reading; the exploratory, non-lockstep surface.
- **Release gating:** this spec **blocks the 2024 release run
  ([#36](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/36))** so the
  release ships with leakage-controlled numbers, and **feeds the methodology roadmap
  ([#30](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/30))**. Per the
  release spec, the release gates are **correctness-only**; the honest accuracy numbers
  are *reported*, not gated on a threshold. The Google validation (§8) is independent of
  the release and gates nothing.

---

## 10. Dependencies and sequencing

- **Depends on:** C4 test-set-leak fix (station-grouped split upstream of tuning) —
  cleanup phase 2, [#33](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/33)
  under [#21](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/21).
- **Blocks:** [#36](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/36)
  (2024 release run), [#30](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/30)
  (methodology roadmap) — both edges already wired.
- **Interlocks with:** [#29](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/29)
  (match-selection refresh) — the calibration harness (§7) measures #29's calibrated
  quantile, landed as `conf_dist_km` in
  [#44](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/44).

## 11. Deferred / future fog (handed to the map)

- Google-on-uncovered run (gated on §8).
- Design C manual gold set.
- Spatial-autocorrelation diagnostic (§4b).
- A shipped per-station confidence field in the released dataset — a
  [#30](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/30) methodology
  decision, deliberately kept out of this evaluation spec.
