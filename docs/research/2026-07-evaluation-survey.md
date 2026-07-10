# Geocoding-evaluation survey and proposed evaluation designs

**Ticket:** [#24 — Survey geocoding-evaluation practice and available ground truth](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/24)
**Feeds:** [#25 — Decide the evaluation spec](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/25)
**Date:** 2026-07-10
**Status:** research findings (planning input, not an execution spec)

## Purpose in plain language

This project already produces coordinates for Brazilian polling stations, but we do
not currently have an honest, repeatable way to say *how accurate those coordinates
are*. This document surveys how geocoders are evaluated in practice, inventories what
we can realistically use as a reference ("ground truth") to check ourselves against,
and proposes a small number of concrete evaluation designs — with their costs and
blind spots — so the next ticket can pick one.

---

## 1. What the pipeline does today (the starting point)

Three facts about the current code shape every option below.

**1a. TSE coordinates are the only ground truth in the repo, and they are noisy.**
`clean_tsegeocoded_locais()` (`R/data_cleaning.R:208`) reads the TSE-published
latitude/longitude for polling stations from the 2018, 2020, 2022, and 2024 election
files, drops placeholder `-1` coordinates and out-of-country rows, and keeps the most
recent vintage per station. These coordinates are self-reported by the electoral
authorities and are exactly the thing this project was built to improve on — so they
are a *reference*, not clean truth.

**1b. TSE coordinates are used as the model's training target.**
`make_model_data()` (`R/model.R`) merges candidate matches against the TSE point and
computes the haversine `dist` between them; `train_model()` regresses `log(dist)` on
match features (string distances, population, area, rurality, …). `get_predictions()`
turns that into `pred_dist` — the model's *predicted distance-from-truth* for each
candidate — and the best match per station is the one with the smallest `pred_dist`
(`R/model.R:432`).

**1c. Where TSE has a coordinate, the output *is* the TSE coordinate.**
`merge_geocoded_locais()` (`R/data_cleaning.R:457`) sets
`final_long := ifelse(is.na(tse_long), pred_long, tse_long)` (and likewise for lat).
So the model-selected coordinate only reaches the output for stations that TSE never
geocoded — **precisely the stations with no ground truth.**

**The central tension.** The stations whose output the model actually determines are
the ones we cannot directly score, because they have no reference coordinate. Any
evaluation is therefore an *extrapolation*: we measure accuracy on the TSE-covered
subset and argue it carries over to the uncovered subset. Every design below is really
a different way of making that extrapolation credible.

### What evaluation already exists

- **Methodology doc** (`doc/geocoding_procedure.qmd`, "Estimating Geocoding Error",
  line 299+): on the 2018 stations that have TSE coordinates, it computes the haversine
  error of the *model-selected* coordinate (`pred_long/lat`, correctly **not** the
  TSE-substituted `final_*`) against the TSE point, reports the 25th/50th/75th
  percentile error in km, splits urban vs. rural via census tracts, and compares
  against a one-time October-2020 Google Maps API snapshot (`data/google_geocoded.csv`).
- **Training metrics**: `train_model()` runs `tune::last_fit()` and reports test-set
  RMSE/MAE/R² — but on the *surrogate* task (predicting distance-from-TSE), not on final
  geocoding accuracy.
- **Sanity-check report** (`reports/polling_station_sanity_check.qmd`,
  `render_sanity_check_report()`): reports **geocoding rate** (completeness) only — no
  positional accuracy.

### Known weaknesses of the current evaluation

1. **Optimistic / leaky.** The reported error is measured against the same TSE labels
   the model was trained on. There is no enforced held-out split for the *reported*
   numbers, and the code-health audit already flagged that tuning leaks the test set
   (finding C4, [#19](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/19)).
2. **Stale, unreproducible benchmark.** The Google comparison is a 2020 snapshot; it
   cannot be regenerated without paying, and it only covers 2018.
3. **One vintage.** Only 2018 is evaluated; 2020/2022/2024 are unmeasured.
4. **No calibration check.** `pred_dist` is a *predicted* error in meters, but nothing
   checks whether predicted error matches realized error — even though the whole
   match-selection step trusts that ranking.
5. **Completeness and accuracy reported separately**, never in one frame, so a design
   that geocodes fewer-but-better vs. more-but-worse cannot be compared honestly.

---

## 2. Current practice for evaluating geocoders

The strongest evaluation literature is in health geography / spatial epidemiology and
GIScience, where geocoder positional accuracy has been studied rigorously; it transfers
directly to polling-station geocoding. Sources are listed in §Sources.

### 2a. Positional accuracy is a distribution, not a number

Positional accuracy is the great-circle distance between the geocoded point and a
reference coordinate, computed per record and summarized as a *distribution*. Because
the error distribution is heavily right-skewed (a long tail of gross errors), standard
practice leads with the **median and high percentiles (90/95/99th)** plus the **share
within fixed thresholds** (100 m / 250 m / 500 m / 1 km), i.e. the full cumulative
distribution; mean and RMSE are reported but are dominated by the tail (Bonner et al.
2003; Zandbergen 2006). Thresholds should be tied to the **smallest spatial unit the
data will be assigned to** — street-level error already misplaces ~5% of records into
the wrong census tract (Zandbergen 2006).

The single biggest driver of error is **match level**: rooftop/parcel-point ≪
street/range-interpolated ≪ ZIP/place centroid. Whitsel et al. (2006) measured
interpolated matches at ~270 m mean error vs. centroid matches at 4,200–5,500 m — an
order of magnitude worse. A headline median hides this, so accuracy must be reported
stratified by match level / match source.

### 2b. Report match rate and accuracy jointly — they trade off

Match rate (completeness) and positional accuracy are orthogonal axes that move in
*opposite* directions: a geocoder inflates match rate by falling back to coarse
centroids, which raises completeness while degrading accuracy. Whitsel et al. (2006)
found vendors matching 98% of addresses did so at ~1,809 m mean error while a 30%-match
vendor achieved ~228 m. Reporting either number alone is therefore systematically
misleading; report accuracy conditional on match tier and match rate conditional on
accuracy tier. Ratcliffe's (2004) ~85% minimum match rate for reliable point-pattern
analysis is a widely cited floor, but later work shows it is context-dependent (Kim et
al. 2020) — treat 85% as a floor to beat, not a target.

### 2c. Stratification

Standard stratifiers, all with empirical support: **urban/rural** (the dominant axis),
**match level/address type**, **region**, and **geocoder/reference vintage**. The
urban→rural penalty is large and monotonic: Cayo & Talbot (2003) measured urban median
error 38 m (95th pct 152 m) vs. rural median 201 m (95th pct **2,872 m**), with match
rate falling 94%→62%; Fisher et al. (2021) found a >7× rural/non-rural gap (173 m vs.
23 m). Error is also **spatially clustered**, not random (Fisher et al. report
significant Moran's I on residuals), so a good global median can hide whole bad regions
— worth a spatial-autocorrelation diagnostic, not just a global average. For Brazil
specifically, Cortes et al. (2021) reached 85.7% completeness in Rio only after heavy
address standardization, tiered rooftop 83.8% / interpolated 15.1% / centroid 1.1% — so
an **address-structure/completeness** stratum matters here too.

### 2d. Calibration of predicted error — a genuine gap, with a clear method

This is the least-served question in the geocoding literature and the most relevant to
this project, whose match selection *trusts* a predicted-distance ranking (`pred_dist`).
A model that emits per-record predicted error is a **regression-uncertainty calibration**
problem, and the ML literature gives well-defined tools (Levi et al. 2022; Kuleshov et
al. 2018): bin records by predicted error, plot **predicted vs. realized error**
(reliability diagram), summarize the gap with **Expected Normalized Calibration Error
(ENCE)**, and check **prediction-interval coverage**. The practical, presentation-ready
version is a **rank-and-filter demonstration**: sort by predicted error, and show that
dropping the worst-predicted tail monotonically lowers realized error and raises
%-within-threshold — evidence the score carries information even if not calibrated in
absolute meters. Notably, the best-known geocoding confidence index (Davis & Fonseca's
GCI, 2007) was *never validated against measured error*, and geocodebr's `desvio_metros`
is an internal-dispersion estimate, not a validated error — so a calibration check here
would contribute something the field largely lacks. The empirically grounded fallback
stratifier, if calibration proves weak, is the ordinal match level.

### 2e. Ground-truth strategies and their caveats

Ordered by independence/accuracy: GPS field collection (~8–15 m; gold standard, sample
only) > surveyed authoritative points > parcel/building centroids > **manual audit via
satellite/street imagery** (analyst places the point by eye; the pragmatic large-sample
option, endorsed as a ground-truth substitute — Cayo & Talbot used 1 m orthoimagery).
Because any reference is itself imperfect, good studies (i) **quantify the reference's
own error budget** and keep it well below the errors being measured, (ii) **triangulate
across independent sources** and report inter-source agreement, and (iii) draw a
**stratified, multi-rater, adjudicated sample** for the expensive-truth layer. A
critical caveat: if the reference and the geocoder draw on **overlapping source data**,
agreement is inflated — prefer a reference independent of the geocoder's inputs.

### 2f. Benchmarking against a second geocoder

With no gold standard, running inputs through a second geocoder and analyzing
**agreement/disagreement** is cheap and scales to the full dataset; it is genuinely
useful for **triage** (high agreement → likely good; disagreement → audit) and for
exposing systematic regional divergence. But **agreement ≠ accuracy**: two geocoders
sharing reference data can agree and both be wrong (correlated error masquerading as
validation), disagreement can't say which is right, and agreement is only defined on the
easy-address intersection. The recommended hybrid is second-geocoder triage plus a
ground-truth-resolved sample of disagreements, which calibrates what an inter-geocoder
distance implies about real error.

---

## 3. Available ground truth and benchmarks (Brazil, 2018–2024)

### 3a. TSE-published coordinates — field-collected, independent, but coverage is unmeasured

TSE publishes `nr_latitude`/`nr_longitude` inside the **"Eleitorado por local de
votação"** datasets (`eleitorado_local_votacao_YYYY`), starting with the **2018**
vintage — exactly the files this pipeline ingests. Crucially, these coordinates are
**field-collected by TRE/electoral-zone staff via dedicated georeferencing apps (the GEL
system), on a partly voluntary basis — not produced by a central geocoding engine**
(TRE-CE 2017; TRE-PA 2018; TRE-BA 2020). That makes them **genuinely independent of any
CNEFE-based geocoder**, which is what qualifies them as ground truth here. Their
weakness in this project is *not* source overlap — it is that they are the model's
training target (so evaluation needs a strict held-out split) and that decentralized,
voluntary collection makes **coverage and quality vary by state and year**.

Quality handling is already partly encoded: the pipeline converts the **`-1` sentinel**
to `NA` and drops out-of-country (`ZZ`) rows. TSE publishes no formal data dictionary
for the sentinel — it is known only from ingestion behavior. **Coverage share by year is
not published anywhere** and must be computed from the raw files (non-missing, non-`-1`
share per distinct station); expect it lowest in 2018 and improving by 2022/2024. Note
2020/2024 were municipal and 2018/2022 general elections, so the active-station set
differs across years — anchor each station-year to a same/nearest-vintage reference.

### 3b. CNEFE 2022 — a strong reference, but not independent of *this* pipeline

CNEFE (IBGE's national address register) exists in 2010, 2017 (agro), and 2022 vintages.
The **2022 CNEFE is georeferenced for the first time: ~111.1M coordinates captured, 109.9M
validated ≈ 98.9%**, at roughly building-entrance precision (IBGE 2024). That is an
excellent address-level reference — **but only for a geocoder that does not itself
consume CNEFE.** This pipeline does (via `match_geocodebr_muni()` and CNEFE matching), so
grading it against CNEFE is grading against its own inputs — correlated error, inflated
agreement. CNEFE 2022 is also contemporaneous with the 2022 election only, so it won't
reference 2018/2020-only stations.

### 3c. geocodebr — a *correlated* cross-check, not an independent oracle

IPEA's `geocodebr` geocodes against CNEFE and returns a precision category (`numero` →
`municipio`) and a **`desvio_metros`** uncertainty radius (the 95%-dispersion of matching
CNEFE points — an internal estimate, *not* validated positional error; no external
gold-standard accuracy figure for geocodebr was found). Because geocodebr is CNEFE-based
and **so is this pipeline, their errors are correlated** — agreement mainly confirms
CNEFE-consistency, not correctness. Its real evaluation value is therefore **triage and
flagging** (its precision category + `desvio_metros` cheaply flag low-confidence
stations), not independent validation. (Ticket #26 owns the fuller geocodebr/tooling
survey; here it appears only in its benchmark role.)

### 3d. Commercial geocoders — independent, paid, eval-only

The recurring quartet in the Brazilian literature is **Google, ArcGIS, OSM/Nominatim,
and CNEFE-based methods**; a 2025 Federal District dengue study ranked Google strongest
and ArcGIS weakest, with all struggling without clean CEP (Geospatial Health 2025 — treat
its absolute error magnitudes cautiously; the robust signal is the ordering and the
difficulty). Broader literature puts commercial urban error at ~50–300 m with universal
rural degradation. **Google is the literature's strongest independent option for Brazil**
and, unlike geocodebr, does *not* share this pipeline's CNEFE inputs — so a single
eval-only Google run is the most credible independent triangulation, at a cost. HERE /
Placekey / Azure did not appear in Brazilian evaluation studies.

### 3e. Manual audit — feasible urban, satellite-only rural

Street-View-based audits with inter-rater protocols are well established (2+ raters
inspect imagery, report kappa/agreement). The binding constraint in Brazil is rural
coverage: the SALURBAL study found Google Street View available at only **45.1% of
near-road points across 371 Latin American cities**, skewed to higher-SES areas, and
**rural Street View is limited to main roads**. So an audit must use **Street View for
urban and high-resolution satellite/aerial imagery (full national coverage) for rural**
— satellite confirms a building footprint but not a street sign. Published audits use low
hundreds of dual-rated points per stratum; a national polling-station audit should
stratify by urban/rural × state.

---

## 4. Proposed evaluation designs

Three designs, in increasing order of cost and rigor. They are **cumulative** —
each is the previous one plus more — so the decision in #25 is really "how far up
this ladder do we go now."

### Design A — Honest held-out TSE evaluation (the disciplined refresh)

**Idea.** Keep the existing "distance-to-TSE" evaluation, but make it *honest and
reproducible*: a station-grouped train/test split created once, upstream of tuning, so
reported accuracy is genuinely out-of-sample; run it for every vintage (2018–2024), not
just 2018; and report accuracy and completeness in one table, stratified by urban/rural
and region.

**Metrics.** Per stratum: median / 75th / 90th percentile haversine error; share within
100 m / 500 m / 1 km; match rate (share of stations geocoded); and the two reported
*jointly* so the completeness–accuracy tradeoff is visible.

**Ground truth.** The held-out TSE subset only. No new data, no cost.

**Fixes.** Weaknesses 1, 3, 5 above (leak, single vintage, split reporting).
**Leaves open.** The reference is still self-reported TSE (noise floor unknown), and the
uncovered-station extrapolation is still an assumption.

**Cost.** Low — mostly reorganizing existing code and enforcing the split. Depends on
the C4 test-set-leak fix already scoped in the cleanup spec
([#21](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/21)).

### Design B — Add `pred_dist` calibration + geocoder triage (correlated ≠ independent)

**Idea.** Design A, plus (i) a **calibration check** of `pred_dist`: bin candidates by
predicted error, plot predicted vs. realized error (reliability diagram / ENCE), and —
the presentation-ready core — a **rank-and-filter demonstration** that dropping the
worst-predicted tail monotonically improves realized accuracy, proving the ranking that
drives match selection carries information; and (ii) a **free, reproducible
second-geocoder pass with geocodebr** for **triage** across the full dataset, including
the uncovered subset — flagging low-confidence stations (via disagreement,
`desvio_metros`, and precision category) that Design A cannot touch.

**Critical caveat from the research.** geocodebr is CNEFE-based and *so is this
pipeline*, so their errors are **correlated** — geocodebr agreement confirms
CNEFE-consistency, not correctness, and must be labeled as triage, not validation.
CNEFE 2022 is disqualified as a reference for the same reason. The genuinely independent
benchmark is a paid geocoder (**Google**, strongest on Brazil, does not share our CNEFE
inputs): an **optional one-time eval-only Google run** over a sample gives the real
independent triangulation, budget permitting.

**Ground truth / benchmark.** Held-out TSE (as A, independent field GPS) + geocodebr
(correlated triage) ± one Google sample (independent).

**Fixes.** Weakness 4 (calibration) fully; gives an *indirect, correlation-aware* read on
the uncovered subset. **Leaves open.** Agreement is not accuracy — geocodebr agreement is
especially weak evidence here because of shared CNEFE inputs.

**Cost.** Medium — a calibration report target + a geocodebr benchmark target (no manual
labor); the optional Google sample adds a small paid, eval-only cost.

### Design C — Add a manual audit gold set (anchor the noise floor)

**Idea.** Design B, plus a **small, stratified, dual-rater, adjudicated gold set**: draw
a few hundred stations per stratum (urban/rural × region × TSE-covered/uncovered),
locate each by hand — **Street View for urban, high-resolution satellite for rural** (the
research shows rural Street View is essentially unavailable) — with two independent raters
and adjudication of disagreements. This is the only design that (i) measures the TSE
reference's *own* error, turning it from assumed-truth into a quantified noise floor, and
(ii) directly scores the uncovered-station subset the model actually determines.

**Ground truth.** A purpose-built audited gold set + everything in B.

**Fixes.** The central tension — it is the only design that measures accuracy where the
model actually does its work.
**Leaves open.** Sample size limits stratum-level precision; imagery coverage is weakest
in exactly the rural areas we most doubt, and satellite confirms a footprint but not a
street sign.

**Cost.** High — dual-rater adjudication time; needs a sampling design and an
adjudication protocol. Best treated as a one-time gold set that later runs re-use.

### Recommendation (for #25 to confirm)

Adopt **Design A now** as the reproducible accuracy backbone (cheap, unblocks an honest
methodology-doc refresh, rides the C4 test-set-leak fix already planned in
[#21](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/21)), and **commit
to Design B's calibration half** in the same spec — the `pred_dist` calibration /
rank-and-filter check is a small addition and directly validates the match-selection step
the pipeline already depends on. Treat Design B's **geocodebr pass as triage only**, never
as validation, given the correlated-CNEFE-inputs finding; make the **independent Google
sample and the Design C gold set explicit decisions for #25** — both cost real
money/labor, and both are the *only* ways to (a) quantify the TSE noise floor and (b)
score the uncovered subset the model actually determines. This keeps the evaluation spec
execution-ready without blocking it on a labor-intensive audit, while flagging that
without C (or at least the Google sample) the headline accuracy remains an
extrapolation from TSE-covered to TSE-uncovered stations.

---

## 5. Open questions handed to #25 (the evaluation-spec decision)

- **Headline metric & thresholds.** Median vs. %-within-threshold as the headline;
  which thresholds (100 m / 500 m / 1 km), tied to the smallest spatial unit stations are
  assigned to.
- **The uncovered-subset problem.** How to report accuracy honestly for TSE-uncovered
  stations where no direct measure exists — explicit "extrapolated/unmeasured" caveat,
  a correlation-aware geocodebr agreement bound, or resolve it with the Google sample /
  Design C gold set.
- **Independent benchmark.** Whether one eval-only Google run is worth it, given
  geocodebr alone is a *correlated* (not independent) benchmark.
- **Manual audit (Design C).** Whether to build the gold set now; if so its per-stratum
  size, urban/rural × region strata, and dual-rater/adjudication protocol.
- **Measure TSE coverage first.** Coverage share by year (2018/2020/2022/2024) is
  unpublished — compute it from the files as an input to the spec, since it determines how
  much real ground truth exists per year. (Overlaps the 2024 audit in
  [#22](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/22).)
- **Where it lives.** Whether evaluation becomes a pipeline target (rebuilt every run,
  feeding the methodology-doc refresh) or a separate report.

## Sources

**Geocoder evaluation methodology (§2)**
- Bonner et al. 2003, *Epidemiology* 14(4):408–412 — https://pubmed.ncbi.nlm.nih.gov/12843763/
- Cayo & Talbot 2003, *Int. J. Health Geographics* 2:10 — https://pmc.ncbi.nlm.nih.gov/articles/PMC324564/
- Whitsel et al. 2006, *Epidemiologic Perspectives & Innovations* 3:8 — https://pmc.ncbi.nlm.nih.gov/articles/PMC1557664/
- Zandbergen 2006, *Int. J. Health Geographics* 5:23 — https://pmc.ncbi.nlm.nih.gov/articles/PMC1523259/
- Zandbergen et al. 2015, *Annals of GIS* — doi:10.1080/19475683.2015.1085437
- Fisher et al. 2021, *IJERPH* 18(4):1637 — https://pmc.ncbi.nlm.nih.gov/articles/PMC7915413/
- Ratcliffe 2004, *IJGIS* 18(1) — doi:10.1080/13658810310001596076; Kim et al. 2020, *IJGIS* 34(7) — doi:10.1080/13658816.2019.1703994
- Davis & Fonseca 2007 (GCI), *GeoInformatica* 11(1):103–129
- Levi et al. 2022, *Sensors* 22(15):5540 — https://pmc.ncbi.nlm.nih.gov/articles/PMC9330317/; Kuleshov et al., ICML 2018
- Goldberg et al., manual geocode improvement — https://pmc.ncbi.nlm.nih.gov/articles/PMC2612650/

**Brazilian ground truth & benchmarks (§3)**
- Cortes, Silveira & Junger 2021, *Cadernos de Saúde Pública* 37(7) — https://www.scielo.br/j/csp/a/PzqnKn6Zs5CjHC9RkR6pdMq/?lang=en
- TSE Portal de Dados Abertos, Eleitorado — https://dadosabertos.tse.jus.br/dataset/eleitorado-2022 (files `eleitorado_local_votacao_YYYY`)
- TSE georeferencing program: TRE-CE 2017, TRE-PA 2018, TRE-BA 2020 (GEL system) — see agency news pages cited in the research transcript
- IBGE, 2022 CNEFE georeferencing (~98.9% validated) — https://agenciadenoticias.ibge.gov.br/agencia-noticias/2012-agencia-de-noticias/noticias/39065-ibge-divulga-pela-primeira-vez-as-coordenadas-geograficas-dos-enderecos-do-pais
- geocodebr (IPEA/ipeaGIT) — https://cran.r-project.org/web/packages/geocodebr/refman/geocodebr.html ; https://github.com/ipeaGIT/geocodebr
- Geospatial Health 2025 (Federal District dengue, Google/CNEFE/OSM/ArcGIS) — https://www.geospatialhealth.net/gh/article/view/1403
- Fry et al. 2020, Google Street View availability in Latin American cities, *J. Urban Health* — https://pmc.ncbi.nlm.nih.gov/articles/PMC7392983/

**Coverage gaps flagged by the research** (measure directly, do not cite): (a) TSE
coordinate coverage % by year; (b) any externally validated positional-error figure for
geocodebr; (c) a formal TSE data dictionary for the `-1` sentinel (known only from this
repo's ingestion code).
