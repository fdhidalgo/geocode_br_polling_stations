# Match-selection model: refresh assessment

**Ticket:** [#29 — Assess match-selection model refresh options](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/29)
**Feeds:** [#30 — Decide the methodology upgrade roadmap](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/30)
**Sibling (coordinate, don't duplicate):** [#24 evaluation survey](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/24) → [`docs/research/2026-07-evaluation-survey.md`](2026-07-evaluation-survey.md)
**Date:** 2026-07-10
**Status:** research findings (planning input, not an execution spec)

## Purpose in plain language

After the fuzzy string matching runs, each polling station has several *candidate*
coordinates — one from each administrative dataset it could be matched against. Something
has to pick the best candidate. That "something" is a trained machine-learning model (a
LightGBM boosted-tree regression). This document asks a narrow question: **is that model
still the right tool, and if we refreshed it, what would we change?** It covers the data
the model learns from, the features it uses, whether the `pred_dist` number it emits is
honest, and whether a simpler or better design exists. It does *not* re-decide how we
*measure* accuracy — that is [#24](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/24)'s
job, and this document leans on it wherever measurement comes up.

**Scope boundary.** This is only the *selection* model — the layer that ranks candidates
and picks one. How candidates are *generated* (string distance, record linkage, geocodebr,
LLMs) is [#26](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/26)/[#27](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/27)/[#28](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/28).

---

## 1. What the model does today (the starting point)

Read from `R/model.R` and its wiring in `_targets.R:930–985`.

**1a. Framing — regression on distance-to-truth.** `make_model_data()` stacks every
candidate match for every station into long format, merges the TSE-published coordinate
as ground truth, and computes the haversine `dist` (in **km**, `r = 6378.137`) between
candidate and TSE point (`R/model.R:298`). `train_model()` fits a LightGBM regression of
`log(dist)` on candidate features. `get_predictions()` scores every candidate, back-
transforms to `pred_dist = exp(pred_logdist) − .0001` (`R/model.R:432`), and for each
station `finalize_coords()` keeps the single candidate with the smallest `pred_dist`
(`R/data_cleaning.R:426–430`).

**1b. The model only matters for stations *without* a TSE coordinate.**
`finalize_coords()` uses the TSE coordinate directly whenever one exists and falls back to
the model-selected candidate only when it does not (`R/data_cleaning.R:457–458`). So the
model is trained on TSE-covered stations but does its *real work* on the TSE-*uncovered*
ones — a train/apply population mismatch that matters for calibration (§4).

**1c. `pred_dist` is exported and users filter on it.** `pred_dist` rides through to
`output/geocoded_polling_stations.csv.gz` (`R/utilities.R:717`). It plays two roles that
demand different things: as a *ranking key* it only needs to order candidates correctly
(ordinal); as a *published quality column* a user thresholds on ("keep < 1 km") it needs
to be an *honest predicted error in km* (cardinal/calibrated). Today one number serves both
and is validated only for existence, not calibration (`validate_predictions_simple()`,
`R/validation.R:269`).

**1d. Predictor set entering `dist ~ .`** (all columns except the id-roled
`cod_localidade_ibge` / `local_id` and the outcome `dist`):

| Feature | Notes |
|---|---|
| `type` | match source (`st_cnefe_2010`, `geocodebr`, `schools_inep_name`, …); **raw character**, no `step_dummy` / factor encoding (`R/model.R:369`) |
| `mindist` | string-match distance for the candidate; for geocodebr it is a **synthetic** `(3 − precision_score) * 0.1`, not a string distance (`R/model.R:137`) |
| `long`, `lat` | the **candidate's own coordinates** — the model can learn raw geographic priors |
| `precision_score` | geocodebr precision (3/2/1); **NA for every non-geocodebr row** |
| `logpop`, `pct_rural`, `area` | municipality covariates (median-imputed in the recipe) |
| `centro`, `zona_rural`, `school` | binary address flags |
| `length_norm_name`, `length_norm_addr` | string lengths |

**1e. Tuning.** `finetune::tune_race_anova()` (racing) over a size-`grid_n` space
(50 in production) tuning `trees`, `min_n`, `mtry`, `learn_rate`, `loss_reduction`,
`num_leaves`; select best RMSE; `last_fit()` on a 50/50 municipality-grouped split
(`group_initial_split(group = cod_localidade_ibge)`). The stack is modern:
tidymodels + `bonsai` + `finetune` racing.

**Bottom line:** a defensible, modern boosted-tree setup, but framed as *regression on a
back-transformed distance* and serving one uncalibrated number for two different jobs.

---

## 2. Training-data vintage (sub-question 1)

**Labels are TSE self-geocodes, 2018–2024.** `clean_tsegeocoded_locais()`
(`R/data_cleaning.R:208`) reads the TSE-published coordinates for the 2018, 2020, 2022,
and (when present) 2024 elections, drops `−1` placeholders and out-of-country rows. These
are the training *labels*. Two consequences:

- **The ground truth is noisy.** TSE coordinates are self-reported by the electoral
  authority and are the very thing this project exists to improve on. The model is
  therefore trained to *imitate TSE*, and its ceiling is TSE's own accuracy. #24 flags this
  same noise floor and proposes (Design C) a manual gold set to quantify it. **Do not treat
  a lower `pred_dist` as unambiguously better** until that noise floor is known.
- **2024 is wired but unvalidated.** The 2024 election file is already read (`R/data_cleaning.R:233`)
  but the 2024 release has never been validated ([#22](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/22)/[#23](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/23)).
  Adding a validated 2024 vintage grows the label set by a full election cycle and is the
  single cheapest way to expand training data. **This is the main "newer vintage" lever**,
  and it is owned by the 2024 thread — the model refresh should *consume* it, not re-litigate it.

**Bottom line:** the vintage lever is real but small and already in flight (2024). The
deeper issue is not quantity but that the labels *are* the noisy reference — which is why
every refresh recommendation below is conditioned on #24's honest evaluation, not on
`pred_dist` improving.

---

## 3. The baseline is not trustworthy yet (prerequisite)

Per the ticket's own heads-up and code-health finding **C4**: the cross-validation folds
are built from the **full** `model_data`, not the `training_set`
(`R/model.R:362–366` — `group_vfold_cv(model_data, …)` where it should be `training_set`).
Hyperparameters are therefore selected with the test half in view, so the `last_fit()`
metrics are optimistic. **No refresh option can be judged until this is fixed and an honest
held-out baseline exists.** That fix is scoped in the cleanup spec
([#21](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/21), phase 2) and the
honest re-evaluation is #24's Design A. This document's recommendations are all framed as
*"adopt X only if the honest baseline shows Y"* precisely because Y is not yet measurable.

**Bottom line:** fix C4 and re-establish the baseline first; it is the gate on everything else.

---

## 4. Is `pred_dist` honest? (sub-question 3)

Three distinct problems, in rough order of severity:

**4a. Back-transformation (retransformation) bias.** The model fits `log(dist)` and reports
`exp(prediction)`. By Jensen's inequality, `exp(E[log dist]) ≤ E[dist]` — the back-transform
of the mean-of-logs is the conditional *geometric* mean, systematically *below* the
arithmetic mean, with the gap growing in residual variance (Manning 1998). The textbook fix
is Duan's smearing estimator (1983), with group-specific factors under heteroscedasticity
(Manning 1998), *or* fitting on the original km scale with a positive right-skewed objective
(Gamma/Tweedie, both native in LightGBM). This means `pred_dist` is not just noisy but
**biased low as a distance estimate** — exactly the wrong direction for a column users trust
to be conservative. *(This bias does not affect ranking, so §1c's two roles diverge here: the
ranking is unharmed, the published number is biased.)* **Note the shortcut:** if the export
switches to a *quantile* of distance (4b), the bias largely dissolves — quantiles are
equivariant under the monotone `exp`, so `exp(τ-quantile of log-dist) = τ-quantile of dist`,
whereas the mean is not. One change fixes both 4a and 4b.

**4b. It is a point prediction, not a calibrated error/interval.** A user thresholding on
`pred_dist < 1 km` is implicitly reading it as "this point is within ~1 km." But it is a
conditional-mean point estimate with no coverage guarantee. Honest thresholding needs a
*calibrated* quantity: predict a **quantile** of the error (LightGBM supports a quantile
objective; Koenker & Bassett 1978) and/or wrap it in **conformal prediction** for a
distribution-free coverage guarantee — specifically **Conformalized Quantile Regression**
(Romano et al. 2019), which stays short and adapts to the heteroscedastic error this problem
has, with Angelopoulos & Bates (2021) the readable entry point. Verify with regression
calibration diagnostics: reliability curves (Kuleshov et al. 2018) and PIT/sharpness
(Gneiting et al. 2007). #24's Design B already scopes the calibration *check*; this document
adds the *fix* — switch the exported quantity from a mean point-estimate to a calibrated
quantile/interval.

**4c. Train/apply mismatch (§1b).** Calibration is learned on TSE-*covered* stations but
applied to TSE-*uncovered* ones, which skew rural/harder. Even a perfectly calibrated
`pred_dist` on the training population can be miscalibrated where the model actually
operates. The only real remedy is #24's Design C gold set on the uncovered subset; short of
that, the honest move is to **document `pred_dist` as an in-domain estimate** and avoid
implying uncovered-station coverage guarantees.

**Bottom line:** `pred_dist` is currently biased low (4a) and uncalibrated (4b), and its
calibration is unmeasured exactly where the model does its work (4c). A refresh should
**stop exporting a back-transformed conditional mean** and export a calibrated quantile
(quantile objective + conformal wrap), with calibration checked per #24 Design B.

---

## 5. Feature set vs. modern practice (sub-question 2)

The current features are thin on the dimensions the record-linkage / entity-resolution
literature says matter most for *choosing among candidates*:

- **One string-similarity number per candidate.** Only `mindist` describes match quality,
  and for geocodebr it isn't even a string distance (§1d). The standard empirical comparison
  (Cohen, Ravikumar & Fienberg 2003) found *hybrid* metrics best — Soft-TF-IDF and
  Jaro-Winkler beat pure edit-distance — and record-linkage practice (Christen 2012) builds a
  *comparison vector* of several measures, decomposed by field (street vs. neighborhood vs.
  municipality). The candidate-generation tickets (#27) will surface these; the selection
  model should then *consume* several field-decomposed similarity features, not one.
- **No cross-source agreement / consensus feature — the biggest gap.** The model scores each
  candidate in isolation. It never sees the single most informative geocoding signal:
  *do multiple independent sources place this station at nearly the same point?* Consensus
  among independent geocodes is a well-established accuracy signal, and it is free to compute
  here (the candidates already exist per station). A "distance to the nearest other-source
  candidate," "number of sources within N metres," or dispersion-of-the-candidate-cloud
  feature is likely the highest-value, lowest-cost addition. Consensus scoring is used in
  production geocoding-fusion systems, but its predictive strength is **under-quantified in
  the peer-reviewed literature** (the strong evidence is applied/industry), so engineer it as
  a soft *feature* and validate against TSE ground truth — never as a hard gate, since a
  single-source location can still be correct.
- **Encoding hygiene.** `type` enters as raw character with no `step_dummy`/factor step
  (`R/model.R:369`); `precision_score` is structurally NA for all non-geocodebr rows. These
  are handled implicitly (or silently dropped) rather than deliberately — a refresh should
  encode `type` as a factor/dummy and make the geocodebr precision signal a first-class,
  non-NA feature. This overlaps code-health territory ([#19](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/19)); flag, don't fix here.

**Bottom line:** the highest-value feature work is a **cross-source consensus feature** and
**multiple string-similarity measures**; both are cheap and both depend on the candidate-
generation decisions in #26/#27, so they sequence *after* those.

---

## 6. Simpler or better alternatives, and the stack (sub-question 4)

**6a. Establish a heuristic baseline the model must beat.** Before any ML refresh, measure a
trivial selector — e.g. pick the smallest `mindist`, or a fixed source-priority order — on
the honest held-out split. If a tuned LightGBM barely beats "pick the best string match,"
the model's complexity is not paying rent. This is a near-free experiment once §3 is done
and is the cleanest way to answer "is this the right tool at all."

**6b. Reframe from regression to ranking (or calibrated classification).** The task is
*"which candidate is correct,"* which is a **learning-to-rank** or **binary
classification** problem, not distance regression. In learning-to-rank terms, regression-on-
distance is the *pointwise* family — the weakest — because it scores each candidate in
isolation and never learns from the contrast among a station's own competing candidates.
Options:
  - *Learning-to-rank* (LightGBM ships `lambdarank`; Burges 2010, LambdaMART): group-aware,
    directly optimizes putting the correct candidate on top — the metric we actually care
    about — instead of the incidental distance value.
  - *Binary "is this the correct candidate"* (correct = within τ km of truth) with a
    **calibrated** probability — the record-linkage default since Fellegi & Sunter (1969),
    formalized in Christen (2012). It doubles as an honest confidence column and sidesteps
    §4a entirely (no log/exp round-trip). The geocoding-ML studies that exist (Goldberg et al.
    2010 on candidate selection; recent RF/GBDT address-matching work) are classifier-based,
    not distance regression.

  Regression-on-distance is defensible but optimizes the wrong thing; a ranking or
  classification reframe aligns the objective with the job and fixes the `pred_dist` honesty
  problem at the root. The clean design decouples the two jobs: **rank/classify to select,
  and separately emit a calibrated quantile to export.**

**6c. Keep the stack.** tidymodels + `bonsai`/LightGBM + `finetune` racing is current and
appropriate; no reason to change engines. The gains are in **objective/framing** (6a/6b) and
**features** (§5), not the toolchain. Quantile objective and `lambdarank` are both available
*within* LightGBM, so the reframes are engine-compatible.

**Bottom line:** don't rewrite the stack; **(1)** benchmark against a trivial heuristic,
**(2)** reframe the objective to ranking or calibrated classification, **(3)** export a
calibrated confidence instead of a back-transformed mean. Each is a bounded change inside
the existing tooling.

---

## 7. Recommendations handed to #30 (the methodology-roadmap decision)

Sequenced, each gated on measurement rather than asserted:

1. **Gate everything on the honest baseline.** Fix C4 (#21 phase 2) and stand up #24 Design A
   before scoring any refresh. *(prerequisite, already scoped)*
2. **Add a heuristic-baseline comparison** (§6a) to the evaluation — near-free, answers
   "is the model worth it." *(rides #24's harness)*
3. **Fix `pred_dist` honesty** (§4): switch the exported quantity to a calibrated quantile
   (LightGBM quantile objective + conformalized wrap), or adopt the classification reframe
   whose probability *is* the confidence. Adopt **only if** #24 Design B's calibration check
   confirms the current number is miscalibrated (it almost certainly is, per §4a).
4. **Add a cross-source consensus feature** (§5) — highest expected accuracy gain per unit
   effort; sequence after the #26/#27 candidate-generation decisions land.
5. **Consider the ranking/classification reframe** (§6b) as the larger, optional upgrade —
   adopt only if (2) shows the model earns its keep *and* the reframe beats regression on the
   honest split.
6. **Consume, don't re-decide, the 2024 vintage** (§2) from the #22/#23 thread.

Ordering rationale: 1→2 are prerequisites and cheap; 3 is a self-contained honesty fix; 4
depends on candidate-generation decisions; 5 is the big optional bet. Nothing here changes
the toolchain, and everything is conditioned on the honest baseline the map already plans.

---

## Sources

**Record linkage / entity resolution / ranking (§5, §6)**
- Fellegi, I.P. & Sunter, A.B. (1969). "A Theory for Record Linkage." *JASA* 64(328):1183–1210.
- Christen, P. (2012). *Data Matching.* Springer. <https://www.springer.com/gp/book/9783642311635>
- Cohen, W.W., Ravikumar, P. & Fienberg, S.E. (2003). "A Comparison of String Distance Metrics for Name-Matching Tasks." *IJCAI-03 Workshop.* <https://www.cs.cmu.edu/~wcohen/postscript/ijcai-ws-2003.pdf>
- Burges, C.J.C. (2010). "From RankNet to LambdaRank to LambdaMART: An Overview." MSR-TR-2010-82. <https://www.microsoft.com/en-us/research/publication/from-ranknet-to-lambdarank-to-lambdamart-an-overview/>
- Barlaug, N. & Gulla, J.A. (2021). "Neural Networks for Entity Matching: A Survey." *ACM TKDD* 15(3). <https://arxiv.org/abs/2010.11075>

**Geocoding-specific ML (§1, §5, §6)**
- Goldberg, D.W. et al. (2010). "Improving Geocode Accuracy with Candidate Selection Criteria." *Transactions in GIS* 14(s1):149–176.
- "Improving a Street-Based Geocoding Algorithm Using Machine Learning Techniques." (2020) *Applied Sciences* 10(16):5628. <https://www.mdpi.com/2076-3417/10/16/5628>
- "Explainable address matching … ensemble classification." (2025) *GeoInformatica.* <https://link.springer.com/article/10.1007/s10707-025-00562-y>
- "Toward building next-generation geocoding systems: a systematic review." (2025) <https://arxiv.org/pdf/2503.18888>
- EarthDaily, "Geocoding Consensus Algorithm." <https://earthdaily.com/blog/geocoding-consensus-algorithm-a-foundation-for-accurate-risk-assessment>

**Calibration / quantiles / conformal (§4)**
- Koenker, R. & Bassett, G. (1978). "Regression Quantiles." *Econometrica* 46(1):33–50.
- Romano, Y., Patterson, E. & Candès, E.J. (2019). "Conformalized Quantile Regression." *NeurIPS 32.* <https://arxiv.org/abs/1905.03222>
- Angelopoulos, A.N. & Bates, S. (2021). "A Gentle Introduction to Conformal Prediction." <https://arxiv.org/abs/2107.07511>
- Kuleshov, V., Fenner, N. & Ermon, S. (2018). "Accurate Uncertainties for Deep Learning Using Calibrated Regression." *ICML.* <https://proceedings.mlr.press/v80/kuleshov18a.html>
- Gneiting, T., Balabdaoui, F. & Raftery, A.E. (2007). "Probabilistic Forecasts, Calibration and Sharpness." *JRSS-B* 69(2):243–268.
- LightGBM objectives (quantile / gamma / tweedie). <https://lightgbm.readthedocs.io/en/latest/Parameters.html>

**Retransformation bias (§4)**
- Duan, N. (1983). "Smearing Estimate: A Nonparametric Retransformation Method." *JASA* 78(383):605–610.
- Manning, W.G. (1998). "The logged dependent variable, heteroscedasticity, and the retransformation problem." *J. Health Economics* 17(3):283–295.
