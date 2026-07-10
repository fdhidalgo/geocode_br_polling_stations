# Modern string / record-matching methods for address linkage: a survey

**Ticket:** [#27 — Survey modern string/record-matching methods for address linkage](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/27)
**Feeds:** [#30 — Decide the methodology upgrade roadmap](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/30)
**Siblings (coordinate, don't duplicate):**
[#26 geocodebr tooling survey](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/26) → [`2026-07-geocodebr-tooling-survey.md`](2026-07-geocodebr-tooling-survey.md) ·
[#29 match-selection model](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/29) → [`2026-07-match-selection-model.md`](2026-07-match-selection-model.md) ·
[#24 evaluation survey](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/24) → [`2026-07-evaluation-survey.md`](2026-07-evaluation-survey.md)
**Date:** 2026-07-10
**Status:** research findings (planning input, not an execution spec)

## Purpose in plain language

The pipeline turns a polling station's messy text address ("EMEF Pref. João da Silva,
Rua das Flores s/n, Centro") into a set of *candidate* coordinates by fuzzily matching that
text against several reference address lists (the CNEFE census, INEP schools, geocodebr).
The matching is done with classic string-distance math — mostly Jaro-Winkler — written by
hand into `R/string_matching.R`. A separate step in `R/panel_creation.R` links the same
station across election years using the `reclin2` record-linkage package.

This document asks: **since that matching code was written, what better tools exist for
deciding whether two address strings refer to the same place, and are any of them worth
adopting here?** It covers four things the ticket named — (1) embedding / vector-similarity
approaches for address text, (2) current record-linkage packages versus the existing
`reclin2` Fellegi-Sunter setup, (3) blocking strategies (how you avoid comparing every
station against every reference row), and (4) what is actually practical inside a `targets`
+ `data.table` pipeline on a 50 GB-RAM machine using only free / open tools. The output is a
ranked list of candidate upgrades, each scored by expected accuracy gain against integration
cost.

**Scope boundary.** This is the *candidate-generation* layer — how candidate coordinates are
produced and how two strings are compared. It is **not** the *selection* layer that picks
the winning candidate (that is [#29](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/29)),
and it is **not** geocodebr adoption (that is [#26](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/26)).
The clean seam between this document and #29: **this layer produces candidates and
similarity signals; #29's arbitrator consumes them.** Every recommendation here is framed as
"add a candidate or a feature the arbitrator can weigh," never "replace the arbitrator."

---

## 1. What the matching does today (the starting point)

Read from `R/string_matching.R` and `R/panel_creation.R`.

**1a. Geocoding match — per-municipality, exact-token pre-filter, Jaro-Winkler, single
best.** Every `match_*_muni()` function (`match_inep_muni`, `match_schools_cnefe_muni`,
`match_stbairro_cnefe_muni`, `match_stbairro_agrocnefe_muni`) runs **within a single
municipality**. Inside a municipality it:

1. **Blocks** via `prefilter_by_common_words()` (`R/string_matching.R:17`): keeps only
   reference rows that share **≥ 1 exact lowercased whole word** with the query. This is a
   nested `for` loop over query × target words — `O(n·m)` — with a dense
   `n_query × n_target` logical matrix.
2. **Compares** with `stringdist::stringdistmatrix(method = "jw")` (Jaro-Winkler), optionally
   divided by the longer string's length (`chunk_string_match`, `R/string_matching.R:44`).
3. **Selects** the single nearest reference row per query (`which.min`,
   `R/string_matching.R:151`) and emits one `mindist_*` distance plus the matched
   coordinates.

So each source contributes **one candidate and one similarity number** per station. The
municipality partition *is* the blocking key; everything downstream is within-municipality.

**1b. Panel linkage — `reclin2` Fellegi-Sunter.** `R/panel_creation.R` links stations across
years: `pair_blocking(..., municipality)` → `compare_pairs(default_comparator =
cmp_jarowinkler(0.9))` → `problink_em()` (EM estimation of Fellegi-Sunter *m*/*u* weights) →
`select_n_to_m(threshold, n = 1, m = 1)` (1:1 assignment). This is a textbook, defensible
Fellegi-Sunter setup, also blocked on municipality.

**1c. Two facts that shape the whole survey.**

- **Blocking is already solved by geography.** Because matching is partitioned by
  municipality, the candidate set per query is small (tens to a few thousand rows), and the
  quadratic-scaling problem that the entire modern blocking literature exists to solve
  ([Papadakis et al. 2020](https://dl.acm.org/doi/abs/10.1145/3377455)) is **largely absent
  here**. This is the single most important framing fact in this document: *do not adopt a
  scalable blocking method expecting an accuracy win it cannot deliver.*
- **The comparison is purely lexical.** Jaro-Winkler measures character overlap. It cannot
  see that "EMEF João Paulo" and "Escola Municipal de Ensino Fundamental João Paulo" are the
  same school, or that "Cel." = "Coronel". Brazilian school and address names are
  abbreviation-dense, so this is exactly where a lexical metric is weakest — and where the
  real accuracy lever lives.

**Bottom line:** the matching is a hand-rolled, municipality-blocked, single-best Jaro-Winkler
matcher. Its weak point is *comparison semantics* (abbreviations, word order, synonyms), not
*scale*. Read every option below through that lens.

---

## 2. Embedding / vector-similarity for address text (sub-question 1)

**2a. The idea and why it fits here.** A pretrained text-embedding model maps a string to a
dense vector such that semantically similar strings land near each other, so
`cosine("EMEF João Paulo", "Esc. Mun. João Paulo")` is high even though their edit distance
is large. This is precisely the failure mode of §1c. The motivating example from the R
package that popularized this for record linkage — [`fuzzylink`](https://github.com/joeornstein/fuzzylink)
(Ornstein) — is that "Patricia" is lexically closer to "Patrick" than to "Trish," yet
embeddings get "Patricia"≈"Trish" right. Brazilian polling-station names (heavy on
`EMEF`/`EEEF`/`E.E.`/`Cel.`/`Pref.` abbreviations) are a strong fit.

**2b. The free/open catch with `fuzzylink`.** `fuzzylink` is the obvious R-native embedding
linker, but as shipped it **calls a paid API** (OpenAI `text-embedding-3-large` by default;
Mistral/Anthropic/OpenRouter optional), which means **every string leaves the machine** and
the run costs money. That violates this project's production constraint (free/open tools,
data stays local). Two consequences:

- `fuzzylink` **as-is is inadmissible in the production pipeline**, but is admissible as an
  **evaluation-only benchmark** (the map's Notes explicitly allow paid services in
  evaluation comparisons) — e.g. an upper-bound "what would a strong embedding model buy us"
  probe on a sample.
- The *technique* is fully reproducible with **local, free, open models**, which is the
  admissible path (§2c).

**2c. Local free/open embedding models that fit.** All of these are Apache/MIT-licensed,
run on CPU or a single GPU, and embed short strings by the thousand per second:

| Model | Coverage | Note |
|---|---|---|
| [LaBSE](https://arxiv.org/abs/2007.01852) | 109 languages | language-agnostic; long the default for cross-lingual name matching |
| [multilingual-e5](https://arxiv.org/abs/2402.05672) / [BGE-M3](https://arxiv.org/abs/2402.03216) | 100+ languages | current strong general multilingual retrieval encoders |
| [Serafim PT\*](https://arxiv.org/abs/2407.19527) | Portuguese-specific | PT-trained encoders beat generic multilingual ones by 10–20 pts on PT semantic-similarity tasks — most relevant here |

Access from R without a paid service via `reticulate` + `sentence-transformers`, or the R
[`text`](https://r-text.org/) package (both wrap Hugging Face models locally). Embeddings for
the **reference** lists (CNEFE etc.) are computed **once per unique string** and cached as a
`targets` object; at match time you embed only the (far smaller) station strings and take
cosine similarity to the candidate rows already surfaced within the municipality.

**2d. What embeddings do and do not fix.** They shine on the **name / semantic** component
(school names, POI names, abbreviations). They are **weak on the parts of an address that are
symbolic, not semantic** — house numbers, CEP digits, "s/n" — where a wrong digit is a wrong
location but a near-identical vector. So the right design is **hybrid**: embedding similarity
for the name/street-name component, kept *alongside* (not replacing) a lexical/numeric
comparison of the structured fields. This mirrors the long-standing empirical finding that
*hybrid* similarity beats any single metric
([Cohen, Ravikumar & Fienberg 2003](https://www.cs.cmu.edu/~wcohen/postscript/ijcai-ws-2003.pdf)).

**2e. Deep neural entity matching (Ditto, DeepMatcher, HierGAT) — not now.** The
transformer-based entity-matching line
([Li et al. Ditto 2020](https://arxiv.org/abs/2004.00584); surveyed in
[Zeakis et al. 2023](https://www.vldb.org/pvldb/vol16/p2225-skoutas.pdf) and
[the 2025 heterogeneity survey](https://arxiv.org/abs/2508.08076)) is more accurate on hard
benchmarks but needs **labeled matched/non-matched training pairs**, a GPU, and real MLOps.
The pipeline already has a trained arbitrator (#29) that can *consume* a cheap embedding
feature; a bespoke deep EM model is a large integration cost for gain that is unmeasured on
this data. **Park it in fog**, revisit only if the honest evaluation (#24/#25) shows the
name-matching layer is the dominant error source. Zero-shot small-LM matchers
([AnyMatch 2024](https://arxiv.org/pdf/2409.04073)) are worth watching as a lower-cost future
entrant.

**Bottom line:** the highest-value idea in this survey is a **local, free/open embedding
similarity signal** for the name/street component, fed as one more feature/candidate into the
#29 arbitrator — the `fuzzylink` idea, done locally to satisfy the constraint. `fuzzylink`
itself is an evaluation-only benchmark; deep neural EM stays in fog.

---

## 3. Record-linkage packages vs. the current `reclin2` setup (sub-question 2)

The panel-linkage task (§1b) already uses a modern Fellegi-Sunter package. The question is
whether to change it and whether any of these help the *geocoding* match.

| Package | Lang | Model | Scale | Fit here |
|---|---|---|---|---|
| **`reclin2`** (van der Laan) — *current* | R | Fellegi-Sunter + EM | moderate (all-pairs within block) | Works; blocked on municipality; no reason to rip out |
| **`fastLink`** ([Enamorado, Fifield & Imai 2019](https://doi.org/10.1017/S0003055418000783)) | R | Fellegi-Sunter + EM, principled missing-data handling | moderate | The political-science standard; strongest where fields are missing (frequent here: `s/n`, blank bairro). A credible drop-in alternative to `reclin2`, not an upgrade in kind |
| **`Splink`** ([MoJ](https://github.com/moj-analytical-services/splink)) | Python | Fellegi-Sunter, term-frequency adjustments, DuckDB/Spark backends | **very high** (~1M records/min on a laptop; 100M+ on Spark) | The scale successor. But Python — integration cost in an R/`targets` pipeline via `reticulate`. Scale is **not** this project's bottleneck (§1c), so its main draw (throughput) buys little; its term-frequency weighting is a genuine modeling nicety |
| **`fuzzylink`** (Ornstein) | R | embedding similarity + calibrated match probability | small–moderate | Embedding linker; **paid API / data egress** (§2b) → evaluation-only |
| **`RecordLinkage`** (Sariyar & Borg) | R | classic F-S / ML | small | Older; superseded by `reclin2`/`fastLink` for this use |

**3a. Reading of the table.** `reclin2` is fine for what it does; the Fellegi-Sunter *family*
(reclin2 / fastLink / Splink) is all the same statistical model with different engineering.
Switching engines is a **lateral move, not an accuracy upgrade** — unless a specific pain
appears: if missing fields are hurting the panel linkage, `fastLink`'s missing-data model is
the targeted fix; if throughput ever becomes the constraint, `Splink` is the answer. Neither
is urgent.

**3b. The transferable idea for the *geocoding* match.** The geocoding matcher (§1a) is
**not** Fellegi-Sunter — it is single-best Jaro-Winkler with **no principled combination of
multiple field similarities**. The record-linkage literature's core contribution is exactly
that: build a **comparison vector** of several similarities (name, street, bairro, number),
each with its own agreement weight ([Christen 2012](https://www.springer.com/gp/book/9783642311635);
[Fellegi & Sunter 1969](https://doi.org/10.1080/01621459.1969.10501049)). Here that principle
is already realized in a *better* place than a hand-tuned F-S weight vector — the **#29
lightgbm arbitrator**, which can learn the field-similarity combination from TSE ground
truth. So the recommendation is **not** "adopt F-S for geocoding" but "**feed the arbitrator
field-decomposed similarities**" — the same idea #29 §5 flagged as its top feature gap.

**Bottom line:** keep `reclin2` for the panel; hold `fastLink` (missing data) and `Splink`
(scale) as targeted answers to problems this project does not yet have. The portable win is
**field-decomposed similarity features into the #29 arbitrator**, not a new linkage engine.

---

## 4. Blocking strategies (sub-question 3)

**4a. Blocking is mostly already done.** Because matching is partitioned by municipality
(§1c), the expensive part of entity resolution — avoiding the `O(n²)` all-pairs comparison —
is handled by a domain key that is **exactly right** (a station and its true reference row
are in the same municipality by construction). The modern blocking toolkit is therefore
**low expected accuracy gain** here; it is mostly a *performance and cleanliness* question,
not an accuracy one.

**4b. The one real accuracy leak: the exact-token pre-filter.**
`prefilter_by_common_words()` requires **≥ 1 shared exact whole word**. A candidate whose
every shared word is misspelled or abbreviated ("E.M." vs "Escola Municipal") shares **zero**
exact tokens and is **silently dropped before Jaro-Winkler ever sees it**. This is a genuine
recall hole, and it is *also* an `O(n·m)` nested-loop code smell (§1a) — [code-health
finding territory, #19](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/19).
Two admissible, free/open, R-native replacements that widen recall *and* delete the quadratic
loop:

- **[`blocking`](https://cran.r-project.org/web/packages/blocking/index.html)** (Beręsewicz &
  Strojny; [paper 2025](https://arxiv.org/abs/2504.04266)) — approximate-nearest-neighbour
  blocking over character-shingle or embedding vectors, backends `rnndescent` / `RcppHNSW` /
  `RcppAnnoy`, and it **plugs straight into `reclin2` via `blocking::pair_ann()`**. This is
  the cleanest modern-R blocking option and integrates with the existing linkage code.
- **[`zoomerjoin`](https://github.com/beniaminogreen/zoomerjoin)** (Green,
  [JOSS 2023](https://joss.theoj.org/papers/10.21105/joss.05693)) — MinHash-LSH fuzzy joins
  (Rust-backed), near-linear time; Jaccard for strings, Euclidean for vectors. Good if the
  pre-filter is ever run *across* municipalities or on a national scale.

Both let the pre-filter be **token-similarity** (shingled Jaccard / ANN) instead of
**exact-token overlap**, recovering abbreviation/typo candidates the current filter loses.
Caveat from the literature: MinHash-LSH is **parameter-sensitive** with skewed bucket sizes
([Papadakis et al. 2020](https://dl.acm.org/doi/abs/10.1145/3377455)); ANN methods (HNSW /
NN-descent, i.e. the `blocking` package) are more robust to that skew, which is why `blocking`
is the safer default.

**4c. Embedding blocking is the natural companion to §2.** If embeddings are adopted for
comparison (§2c), the *same* vectors give ANN blocking for free (embed once, use for both
blocking and scoring). That composition — embed reference strings once, ANN-block within
municipality, score by cosine — is the coherent end-state, but it is an optimization of an
already-cheap step; sequence it *after* the comparison-quality wins, not before.

**Bottom line:** blocking is not the bottleneck. The one worthwhile blocking change is
replacing the exact-token pre-filter with **similarity-based blocking (`blocking`/`pair_ann`,
or `zoomerjoin`)**, which closes a real recall leak and removes a quadratic loop — a modest
accuracy gain bundled with a code-health win, not a headline.

---

## 5. Practicality inside `targets` + `data.table` on 50 GB RAM, free/open (sub-question 4)

- **Local embeddings (§2c):** the only new heavy dependency. Feasible: embed each **unique**
  reference string once (CNEFE is large but has far fewer *unique* normalized strings than
  rows), cache as a `targets` object keyed by string hash, batch on CPU (or one GPU if
  available). The existing `crew` `memory_limited` controller already exists for exactly this
  class of memory-heavy target. Dependency reality: `reticulate` + a Python env, or the R
  `text` package — a real but bounded addition, and fully offline after model download.
- **`libpostal`** ([openvenues](https://github.com/openvenues/libpostal)) — worth a dedicated
  note. It is a free/open (MIT) statistical address parser/normalizer trained on OpenStreetMap,
  supports **Portuguese/Brazilian** normalization, and would let the pipeline **parse addresses
  into typed fields** (road / house_number / suburb / city), which is the raw material for the
  field-decomposed comparison vector of §3b and a more principled replacement for parts of the
  hand-rolled `normalize_address()`. Cost: it is a **C library with a data model download
  (~2 GB)** and needs an R binding (`Rpostal`/`poster`, or `reticulate`+`postal`), so it is a
  build/ops dependency, not a `install.packages()` away. Medium integration cost, medium gain
  (cleaner fields help both matching and geocodebr feeding, cf. #26).
- **`blocking` / `zoomerjoin` / `fastLink`:** all pure-R / CRAN with compiled backends,
  drop-in, negligible ops cost.
- **`Splink` / `fuzzylink` / deep EM:** Python and/or paid and/or GPU — highest ops cost,
  lowest fit under the constraint (Splink admissible but unmotivated; fuzzylink eval-only;
  deep EM fog).

**Bottom line:** the admissible, free/open, `targets`-friendly additions are **local
embeddings (precompute-and-cache), `blocking`/`zoomerjoin`, and optionally `libpostal`**.
Everything requiring a paid API or a GPU-trained bespoke model is either evaluation-only or
fog.

---

## 6. Findings ranked by accuracy gain vs. integration cost

Ordered by expected accuracy-gain-per-unit-cost. All gains are **hypotheses to be measured on
#24/#25's honest held-out evaluation**, not asserted — the same gating discipline as #29.

| # | Upgrade | Expected accuracy gain | Integration cost | Free/open? | Verdict |
|---|---|---|---|---|---|
| 1 | **Field-decomposed similarity features** into the #29 arbitrator (name/street/bairro/number, each its own similarity) | Medium–High | Low (compute more `stringdist` columns; no new dep) | ✓ | **Do first** — cheapest, shared with #29 §5 |
| 2 | **Local embedding similarity feature** (LaBSE/BGE-M3/Serafim, cosine) as an arbitrator feature for the name/street component | **High** (abbreviation-heavy names) | Medium (`reticulate`/`text`, precompute+cache) | ✓ | **Highest-value bet**; the `fuzzylink` idea done locally |
| 3 | **Similarity-based blocking** (`blocking::pair_ann` or `zoomerjoin`) replacing the exact-token pre-filter | Low–Medium (recovers dropped candidates) + code-health | Low–Medium | ✓ | Do alongside #1–2; closes a real recall leak + kills a quadratic loop |
| 4 | **`libpostal` field parsing** feeding #1 and geocodebr (#26) | Medium | Medium (C lib + 2 GB model + binding) | ✓ | Adopt if #1's field decomposition is bottlenecked by messy normalization |
| 5 | **`fastLink`** for the panel linkage (missing-data model) | Low (unless missingness is shown to hurt) | Low (R, drop-in) | ✓ | Targeted fix; adopt only if panel-linkage errors trace to missing fields |
| 6 | **`Splink`** for panel linkage at scale | ~None (scale isn't the bottleneck) | Medium (Python) | ✓ | Hold; revisit only if throughput ever binds |
| 7 | **`fuzzylink`** (paid embedding linker) | High but inadmissible in prod | — | ✗ (paid, egress) | **Evaluation-only** upper-bound probe |
| 8 | **Deep neural EM** (Ditto / HierGAT / zero-shot LMs) | Unknown, possibly high | High (labels + GPU + MLOps) | ✓ (compute-heavy) | **Fog**; revisit only if name-matching is shown to dominate error |

**Sequencing logic.** #1 and #3 are cheap and can land together; #2 is the marquee bet and
should be measured against #1 alone to prove embeddings earn their dependency; #4 is a
supporting investment that pays off across matching *and* geocodebr feeding; #5–#8 are
contingency/fog. Nothing here changes the *arbitrator* — this layer only enriches the
candidates and signals it consumes, so the whole survey composes cleanly with #29.

---

## 7. Recommendations handed to #30 (the methodology-roadmap decision)

1. **Frame the whole layer as "candidates + signals feeding the #29 arbitrator."** No new
   selection engine; the wins are richer inputs. *(framing)*
2. **Add field-decomposed similarity features** (§3b, finding #1) — cheapest win, already
   #29's top feature gap; do it in the same work.
3. **Prototype a local embedding similarity feature** (§2c, finding #2) — the highest-value
   bet. Gate adoption on it beating finding #1 alone on #24/#25's honest split. Use a local
   free/open model (Serafim PT / BGE-M3 / LaBSE) via `reticulate`/`text`, precomputed and
   cached as a `targets` object.
4. **Replace the exact-token pre-filter with similarity-based blocking** (§4b, finding #3) —
   `blocking::pair_ann` (integrates with `reclin2`) or `zoomerjoin`; closes a recall leak and
   removes an `O(n·m)` loop. Coordinate with the code-health thread
   ([#19](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/19)).
5. **Consider `libpostal`** (§5, finding #4) for principled field parsing if #2/#3's field
   decomposition is limited by normalization quality; note the ~2 GB model + C-binding ops
   cost. Interacts with geocodebr feeding (#26).
6. **Keep `reclin2` for the panel**; hold `fastLink` (missing data) and `Splink` (scale) as
   named answers to problems not yet observed (§3). Do not switch engines speculatively.
7. **`fuzzylink` is evaluation-only** (§2b) and **deep neural EM is fog** (§2e) — record both
   so they are not re-litigated.
8. **Gate every adoption on the honest evaluation** (#24/#25), exactly as #29 does — none of
   these gains is trustworthy until measured against de-leaked TSE ground truth.

Ordering rationale: 2→4 are cheap and near-term; 3 is the measured marquee bet; 5 is a
supporting investment; 6–7 are contingency/fog. The toolchain (`targets`, `data.table`,
`crew`, tidymodels/lightgbm) is unchanged throughout; every admissible option is free/open and
runs locally.

---

## Sources

**R / Python record-linkage & blocking packages (§3, §4)**
- van der Laan, D. `reclin2`. <https://cran.r-project.org/package=reclin2>
- Enamorado, T., Fifield, B. & Imai, K. (2019). "Using a Probabilistic Model to Assist Merging of Large-Scale Administrative Records." *APSR* 113(2):353–371. <https://doi.org/10.1017/S0003055418000783> · `fastLink`
- Linacre, R. et al. (MoJ). *Splink.* <https://github.com/moj-analytical-services/splink>
- Green, B. (2023). "Zoomerjoin: Superlatively-Fast Fuzzy Joins." *JOSS* 8(89):5693. <https://joss.theoj.org/papers/10.21105/joss.05693>
- Beręsewicz, M. & Strojny, T. (2025). "BlockingPy: approximate nearest neighbours for blocking of records for entity resolution." <https://arxiv.org/abs/2504.04266> · R pkg `blocking`: <https://cran.r-project.org/package=blocking>
- Ornstein, J. `fuzzylink`: "Probabilistic Record Linkage Using Pretrained Text Embeddings." <https://github.com/joeornstein/fuzzylink> · <https://joeornstein.github.io/publications/fuzzylink.pdf>

**Embeddings & neural entity matching (§2)**
- Feng, F. et al. (2020). "Language-agnostic BERT Sentence Embedding" (LaBSE). <https://arxiv.org/abs/2007.01852>
- Wang, L. et al. (2024). "Multilingual E5 Text Embeddings." <https://arxiv.org/abs/2402.05672>
- Chen, J. et al. (2024). "BGE-M3." <https://arxiv.org/abs/2402.03216>
- Gomes, L. et al. (2024). "Open Sentence Embeddings for Portuguese with the Serafim PT\* encoders family." <https://arxiv.org/abs/2407.19527>
- Li, Y. et al. (2020). "Deep Entity Matching with Pre-Trained Language Models" (Ditto). <https://arxiv.org/abs/2004.00584>
- Zeakis, A. et al. (2023). "Pre-trained Embeddings for Entity Resolution: An Experimental Analysis." *PVLDB* 16(9):2225–2238. <https://www.vldb.org/pvldb/vol16/p2225-skoutas.pdf>
- "Heterogeneity in Entity Matching: A Survey and Experimental Analysis." (2025). <https://arxiv.org/abs/2508.08076>
- "AnyMatch — Efficient Zero-Shot Entity Matching with a Small Language Model." (2024). <https://arxiv.org/pdf/2409.04073>

**Blocking, filtering & string metrics (§1, §4)**
- Papadakis, G., Skoutas, D., Thanos, E. & Palpanas, T. (2020). "Blocking and Filtering Techniques for Entity Resolution: A Survey." *ACM Computing Surveys* 53(2):1–42. <https://dl.acm.org/doi/abs/10.1145/3377455>
- Steorts, R. et al. (2014). "A Comparison of Blocking Methods for Record Linkage." <https://arxiv.org/abs/1407.3191>
- Cohen, W., Ravikumar, P. & Fienberg, S. (2003). "A Comparison of String Distance Metrics for Name-Matching Tasks." *IJCAI-03 WS.* <https://www.cs.cmu.edu/~wcohen/postscript/ijcai-ws-2003.pdf>

**Foundations & address normalization (§2, §3, §5)**
- Fellegi, I. & Sunter, A. (1969). "A Theory for Record Linkage." *JASA* 64(328):1183–1210. <https://doi.org/10.1080/01621459.1969.10501049>
- Christen, P. (2012). *Data Matching.* Springer. <https://www.springer.com/gp/book/9783642311635>
- openvenues. *libpostal* — international street-address parser/normalizer (statistical NLP on OpenStreetMap). <https://github.com/openvenues/libpostal>
