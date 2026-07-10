# geocodebr and modern open Brazilian geocoding tooling — survey and recommendation

**Date:** 2026-07-10
**Ticket:** wayfinder research task feeding "Decide the methodology upgrade roadmap"
**Author:** research agent (Claude)
**Status:** research note — no code changed

## Purpose (plain language)

This project pins coordinates onto Brazilian polling stations. To do that it
built a large amount of its own machinery that cleans IBGE's national address
registry (CNEFE) and fuzzy-matches polling-station addresses against it. But the
project *also* calls IPEA's `geocodebr` package, which does essentially the same
thing against the same underlying registry. This note surveys what `geocodebr`
has become (it has moved from the pinned v0.2.1 to v0.6.3), what other open
Brazilian geocoding resources exist, and gives a decisive opinion on whether the
pipeline should lean on `geocodebr` more heavily and retire some of the
home-grown code.

Throughout, claims are cited inline. I mark **[verified]** where a primary source
states it directly and **[inferred]** where I am reasoning from primary evidence
but did not find a verbatim statement.

---

## 1. geocodebr: current state and method

### 1.1 Version gap

- The project pins **geocodebr 0.2.1** (`renv.lock` line 918–919, `"Version": "0.2.1"`).
- The current CRAN release is **0.6.3, published 2026-05-24** ([CRAN package page](https://cran.r-project.org/web/packages/geocodebr/index.html)).
- So the pipeline is roughly a year and five minor releases behind, and several of
  those releases were **breaking** (see §1.5). This matters: some of the defensive
  code in `match_geocodebr_muni()` (dropping fields, `n_cores = 1`, wrapping every
  call in `tryCatch`) was written against 0.2.x behavior and may be obsolete or
  even counter-productive on 0.6.x.

### 1.2 What `geocode()` does — a deterministic precision cascade (plus optional probabilistic tier)

`geocode()` matches each input address against CNEFE-derived reference tables and
returns the best coordinate it can, labeling the **precision** achieved. The
precision cascade, most-to-least specific, is **[verified]**
([geocode reference](https://ipea.github.io/geocodebr/reference/geocode.html)):

1. `numero` — exact street + house number
2. `numero_aproximado` — interpolated / nearest house number on the street
3. `logradouro` — street-level (used for "S/N", sem número, entries)
4. `cep` — postal-code centroid
5. `localidade` — neighborhood/locality centroid
6. `municipio` — municipality centroid

The `tipo_resultado` field encodes the match with a four-character code whose
**first character is `d` (deterministic) or `p` (probabilistic)**
([geocode reference](https://ipea.github.io/geocodebr/reference/geocode.html)).
So `geocode()` is **primarily deterministic** (exact/normalized field matching
against CNEFE) with an **optional probabilistic fallback tier** that was added in
v0.2.0 **[verified]** ([NEWS](https://ipea.github.io/geocodebr/news/index.html)).
Probabilistic codes exist at the `numero`, `numero_aproximado`, and `logradouro`
tiers (`pn**`, `pa**`, `pl**`); `cep`, `localidade`, and `municipio` tiers are
deterministic-only (`dc01/dc02`, `db01`, `dm01`)
([geocode reference](https://ipea.github.io/geocodebr/reference/geocode.html)).
This is fundamentally different from the project's own approach, which is
*string-distance* matching (Jaro-Winkler / Levenshtein) and returns a continuous
distance score.

### 1.3 Field mapping via `definir_campos()`

`campos_endereco = definir_campos(...)` maps the caller's data-frame columns to
the address roles `estado`, `municipio`, `logradouro`, `numero`, `cep`,
`localidade`, `bairro` etc. Whatever fields you supply drive how deep the cascade
can go: supply a number column and you can reach `numero` precision; supply only
street and you top out at `logradouro`
([geocode reference](https://ipea.github.io/geocodebr/reference/geocode.html)).
**The current pipeline deliberately supplies only `estado`/`municipio`/`logradouro`**
(`R/string_matching.R:504–515`), so it *structurally cannot* obtain
`numero`-level results today, even though it strips the number out of the address
in `simplify_address_for_geocodebr()` (`R/data_cleaning.R:962–979`).

### 1.4 Tie resolution (`resolver_empates`) **[verified]**

When one input address matches multiple CNEFE coordinates,
`resolver_empates = TRUE` (now the default) resolves them
([geocode reference](https://ipea.github.io/geocodebr/reference/geocode.html)):

- **Strategy 1** — if the candidate coordinates are >1 km apart, *or* the street
  is an ambiguous placeholder (e.g. "RUA A"): return the point with the **highest
  CNEFE establishment count** (`contagem_cnefe`).
- **Strategy 2** — if candidates are <1 km apart and the street is non-ambiguous:
  return the **CNEFE-count-weighted average** of the coordinates.

Unmatched addresses return `NA` coordinates/precision. With
`resolver_empates = FALSE`, all tied candidates are returned (one row each).

### 1.5 Output fields and what changed since 0.2.1

Current output columns **[verified]**
([geocode reference](https://ipea.github.io/geocodebr/reference/geocode.html)):

| Field | Meaning |
|---|---|
| `lat`, `lon` | coordinates in **SIRGAS 2000, EPSG:4674** |
| `precisao` | the six-level cascade label (§1.2) |
| `tipo_resultado` | detailed `d/p` + tier code |
| `desvio_metros` | **95% confidence radius in meters** (uncertainty estimate) — *new in v0.3.0* |
| `contagem_cnefe` | number of CNEFE establishments backing the matched point |
| `endereco_encontrado` | reference address matched (only with `resultado_completo = TRUE`) |
| `cod_setor` | census tract code (only with `resultado_completo = TRUE`) — *new in v0.6.0* |

Major changes 0.2.1 → 0.6.3 **[verified]**
([NEWS](https://ipea.github.io/geocodebr/news/index.html),
[releases](https://github.com/ipeaGIT/geocodebr/releases)):

- **v0.2.0**: probabilistic matching, `busca_por_cep()`, `geocode_reverso()` added.
- **v0.3.0**: `desvio_metros` uncertainty column; `h3_res` H3-hexagon output;
  new CNEFE data release with improved coordinate aggregation; Parquet
  compression (~60% smaller); versioned cache subfolders with auto-cleanup.
- **v0.4.0**: single-letter/digit-only street names excluded from probabilistic
  matching; **Rcpp dependency removed** (moved to DuckDB).
- **v0.5.0** (breaking): **output changed from `data.table` to `data.frame`**;
  `resolver_empates` now defaults `TRUE`; `n_cores` defaults `NULL` (= all
  physical cores); new `padronizar_enderecos` argument (default `TRUE`).
- **v0.6.0**: `cod_setor` output; `enderecobr` bumped to ≥0.5.0 (rewritten in
  Rust); "CNEFE padronizado v0.4.0".
- **v0.6.2**: cache restricted to current data release; `geocode()` errors on
  non-alphanumeric column names.
- **v0.6.3**: **partial-field address tables now accepted** (only `municipio`
  and `estado` are strictly required).

The v0.5.0 `data.table` → `data.frame` change is directly relevant: the pipeline
does `geocoded_result[, local_id := ...]` (`R/string_matching.R:536`), which
assumes a `data.table`. Upgrading past 0.5.0 would require re-checking that code.
The v0.6.2 "errors on non-alphanumeric column names" change and v0.6.3
partial-field acceptance also affect how the current defensive wrapper behaves.

### 1.6 `geocode_reverso()` (reverse geocoding) **[verified]**

Added in v0.2.0; takes coordinates and returns the nearest CNEFE address(es)
within a `dist_max`. Memory use was cut substantially in v0.6.x
([NEWS](https://ipea.github.io/geocodebr/news/index.html)). Relevant if the
pipeline ever wants to *validate* an existing coordinate by checking what address
CNEFE thinks is there, or to backfill an address for a station known only by
coordinate.

### 1.7 Published benchmarks — largely NOT found **[gap]**

I could **not** locate a formal IPEA "Texto para Discussão" or a published
accuracy/coverage benchmark table for `geocodebr`. The README, CRAN page, and
pkgdown site describe the six precision categories and the `desvio_metros`
uncertainty estimate but give **no headline match-rate or accuracy figure**
([CRAN README](https://cran.r-project.org/web/packages/geocodebr/readme/README.html),
[pkgdown site](https://ipea.github.io/geocodebr/reference/geocode.html)). The one
concrete *performance* (not accuracy) number I found is from the IPEA-affiliated
Urban Demographics blog: **the entire CadÚnico register — 43M+ addresses —
geocoded in ~65 minutes** (≈11k addresses/minute)
([Urban Demographics](https://www.urbandemographics.org/post/geocoding-brazilian-data-with-geocodebr/)).
**Treat "how accurate is geocodebr vs. our approach" as an open, unmeasured
question** — see §4 risks.

---

## 2. CNEFE vintage and how geocodebr distributes reference data

- geocodebr's reference tables are built from **CNEFE from the 2022 Demographic
  Census** **[inferred, strongly]**. I could not extract a verbatim "CNEFE 2022"
  sentence from the pkgdown site (several article URLs 404'd), but the evidence
  converges: (a) IBGE's 2022 CNEFE is the **first-ever 100% geographically
  referenced** version, with coordinates for **106M+ addresses**, released
  Feb–Jun 2024
  ([IBGE 2022 CNEFE announcement](https://www.ibge.gov.br/novo-portal-destaques/40076-ibge-divulgara-em-14-de-junho-o-cadastro-nacional-de-enderecos-para-fins-estatisticos-cnefe-atualizado-no-censo-demografico-2022.html));
  (b) geocodebr's whole design depends on per-address coordinates and CNEFE
  establishment counts, which only the 2022 vintage supplies at national scale;
  (c) the NEWS tracks a single evolving "CNEFE padronizado" release rather than
  multiple selectable vintages
  ([NEWS](https://ipea.github.io/geocodebr/news/index.html)).
- **Distribution/caching** **[verified]**: reference data is **downloaded on
  first use and cached locally** (Parquet, versioned cache subfolders,
  auto-cleanup of stale releases), controlled by the `cache` argument
  ([geocode reference](https://ipea.github.io/geocodebr/reference/geocode.html),
  [NEWS v0.3.0/v0.6.2](https://ipea.github.io/geocodebr/news/index.html)).
- **Single vintage, not multi-vintage** **[inferred]**: geocodebr exposes **only
  the latest bundled CNEFE release**; there is no documented parameter to select
  CNEFE 2010 or the 2017 "agro" CNEFE. This is the crux of the overlap tension
  below (§4): the project's own tables cover **2010, 2017, and 2022 separately**
  precisely because it matches stations across time, whereas geocodebr gives you
  one modern (2022) reference surface only.

### Contrast with the project's hand-rolled CNEFE machinery

The pipeline cleans and aggregates CNEFE **2010**, the **2017 agro** CNEFE, and
CNEFE **2022** independently, building school / street / neighborhood reference
tables for each (`R/data_cleaning.R`, `R/string_matching.R`; targets
`schools_cnefe10_by_state`, `schools_cnefe22`, agro-CNEFE cleaners). geocodebr's
bundled 2022 data heavily overlaps the project's **2022** tables but does **not**
replace the **2010/2017** tables — those have no geocodebr equivalent.

---

## 3. Other open Brazilian geocoding resources (brief relevance scan)

- **INEP school catalog (Catálogo de Escolas / Censo Escolar)** — schools are a
  large share of polling stations, and INEP publishes **school-level lat/lon in
  SIRGAS 2000 / EPSG:4674**
  ([INEP catalog docs](https://ecossistemagis.com/coordenadas-do-catalogo-de-escolas-do-inep-no-qgis/)),
  also downloadable via `geobr::read_schools()`
  ([geobr read_schools](https://ipeagit.github.io/geobr/reference/read_schools.html)).
  **The project already uses this** (`inep_catalogo_das_escolas.csv.gz`,
  `clean_inep()` at `R/data_cleaning.R:581`, `match_inep_muni()` at
  `R/string_matching.R:193`, matching on both school **name** and **address**).
  This is a genuinely distinct and valuable source that geocodebr does *not*
  subsume — keep it. Caveat: INEP does not have coordinates for every school
  ([search summary](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/inep-data/catalogo-de-escolas/)).
- **IBGE CNEFE 2022 direct coordinates** — available as raw CSV via IBGE FTP
  ([IBGE FTP CNEFE 2022](https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/Censo_Demografico_2022/Arquivos_CNEFE/CSV/)).
  This is what both the project's 2022 tables and geocodebr are built on; there is
  no reason to consume it twice.
- **OpenStreetMap (osmdata / Nominatim)** — usable for Brazilian streets, but
  address/house-number coverage in Brazil is patchy and uneven, Nominatim has
  rate limits/usage-policy constraints, and it introduces a non-reproducible
  external dependency. Low priority for a reproducible national polling-station
  pipeline; at most a last-resort fallback for addresses CNEFE misses. (Assessed
  from general OSM/Nominatim usage constraints — **[inferred]**, not a
  polling-station-specific benchmark.)
- **geobr** (IPEA) — provides **boundaries and geolocated reference layers**
  (municipalities, census tracts, schools) but is **not an address geocoder**
  ([geobr](https://ipeagit.github.io/geobr/)). The project already depends on it
  for shapefiles. Complementary, not a substitute for geocodebr.
- **galileo / other packages** — I did **not** find a maintained, primary-source
  R geocoder for Brazil competitive with geocodebr; geocodebr is described by IPEA
  as "the first fully free and open-source geocoder built entirely on official
  Brazilian address data"
  ([Urban Demographics](https://www.urbandemographics.org/post/geocoding-brazilian-data-with-geocodebr/)).
  **[gap]** — treat the absence as "did not find," not "does not exist."

---

## 4. Assessment and recommendation

### Headline: **Hybrid — adopt geocodebr more deeply for the modern (2022) reference surface, upgrade the version, and enrich the fields; but keep the project-specific machinery that geocodebr cannot replace.**

Do **not** rip out the custom CNEFE machinery wholesale, and do **not** keep
geocodebr frozen at 0.2.1 with fields deliberately starved. The right move is in
between.

### 4a. What deeper adoption would concretely look like

1. **Upgrade the pin** from 0.2.1 toward a recent 0.6.x, and re-audit
   `match_geocodebr_muni()` against the breaking changes (§1.5): the
   `data.table`→`data.frame` output (v0.5.0) breaks the in-place
   `geocoded_result[, local_id := ...]` at `R/string_matching.R:536`; the
   `resolver_empates`/`n_cores` default changes mean some explicit args are now
   redundant; v0.6.3 lets you pass partial fields without the old DB errors that
   motivated the field-stripping.
2. **Feed geocodebr richer fields.** Today the pipeline passes only
   `estado/municipio/logradouro` and strips the house number
   (`R/data_cleaning.R:962–979`), capping it at `logradouro` precision. Passing
   **`numero`, `bairro`/`localidade`, and `cep`** via `definir_campos()` would let
   the cascade reach `numero` / `cep` / `localidade` precision. This is the single
   biggest latent gain and is currently left on the table by design.
3. **Use `contagem_cnefe` and `desvio_metros` as real model features.** The
   lightgbm selector currently fakes a distance from `precisao` alone
   (`R/model.R:127–137`, `mindist := (3 - precision_score) * 0.1`). `desvio_metros`
   is a genuine per-match uncertainty radius and `contagem_cnefe` is a
   density/confidence signal — both are far better model inputs than a synthetic
   constant, and both are already returned (`R/string_matching.R:547`) but
   underused.
4. **Consider `geocode_reverso()`** as a validation/backfill step (§1.6) — not
   core, but cheap insurance.
5. **Lean on geocodebr's 2022 tables to shrink the project's own 2022 CNEFE
   cleaning.** The project's bespoke 2022 school/street/neighborhood tables are
   the piece with the most overlap; geocodebr already does that aggregation
   (including the tricky establishment-count-weighted tie resolution) against the
   same source.

### 4b. What it would obsolete vs. what must stay

**Could be obsoleted / thinned (geocodebr overlaps it):**
- The bespoke **CNEFE 2022** street and neighborhood reference-table construction
  and its Jaro-Winkler/Levenshtein matching — geocodebr's deterministic +
  probabilistic cascade against the same 2022 CNEFE is a direct substitute, and it
  handles number-level and tie resolution that the custom code does not.
- The manual `precisao → synthetic distance` fudge in `R/model.R` — replace with
  real `desvio_metros` / `contagem_cnefe`.

**Must stay (genuinely project-specific; geocodebr does NOT do these):**
- **Multi-vintage temporal reference data (2010 + 2017 agro).** geocodebr ships
  only the latest (2022) surface. Matching stations back to 2006 needs
  period-appropriate references, which only the project's own 2010/2017 tables
  provide. This is the strongest reason not to delete machinery (a).
- **INEP school matching by name and address** (`match_inep_muni`) — a distinct,
  high-value source for the large school share of polling stations; not subsumed.
- **The lightgbm match-selection model** (`R/model.R`) that arbitrates among all
  candidate sources per station — geocodebr is one input to it, not a replacement.
- **Panel-ID record linkage across years** (Fellegi-Sunter / reclin2,
  `R/panel_creation.R`) — entirely outside geocodebr's scope.
- **Polling-station-specific address normalization** — TSE addresses have quirks
  (school-name-as-address, "ZONA RURAL", "S/N", building-name noise) that the
  project's `normalize_address()`/`normalize_school()` handle; geocodebr's
  `enderecobr` standardizer is generic and may not cover these. Keep the
  project's front-end cleaning; feed its output into richer geocodebr fields.

### 4c. Concrete risks and unknowns

- **Accuracy is unmeasured [gap].** No published geocodebr accuracy benchmark was
  found (§1.7), and there is no in-repo comparison of geocodebr coordinates vs.
  the custom-match coordinates vs. the TSE ground truth the project already holds.
  **Before deleting any custom machinery, run that comparison** using the existing
  ground-truth data — this is the decisive experiment and it is cheap given the
  data is already in the pipeline.
- **Number-level matching may not help on messy inputs.** Polling-station
  addresses are frequently "S/N" or carry a school name rather than a numbered
  street address. Feeding `numero` helps only where a real number exists; the
  expected win is concentrated in urban stations. Quantify before committing.
- **Version-pinning / reproducibility.** geocodebr's reference data is a *moving*
  bundled release (v0.6.2 even prunes old cache). A silent upstream data refresh
  can change coordinates between runs. For a reproducible research pipeline, pin
  both the **package version** and, if possible, record the **CNEFE data release
  hash** used, and store outputs (the project's S3 versioning helps here).
- **Performance at national scale.** ~11k addresses/min is ample for ~1 station
  count per municipality-year, but geocodebr now defaults to all physical cores
  and DuckDB; the pipeline pins `n_cores = 1` and runs per-municipality under
  `crew`. Nested parallelism must be reconciled (let geocodebr use cores, or keep
  it single-core under the crew controller — but decide deliberately, not by
  leftover 0.2.x defensiveness).
- **Breaking-change surface.** Jumping 0.2.1 → 0.6.x crosses the
  `data.table`→`data.frame` and default-argument changes (§1.5); budget for a
  focused re-test of `match_geocodebr_muni()` and the `R/model.R` long-format
  step, in dev mode (AC/RR) first.

### Bottom line

Keep `geocodebr` as one source among several, but stop under-using it: **upgrade
the version, pass it number/bairro/cep fields, and use its real uncertainty and
CNEFE-count outputs in the selection model.** Let it carry more of the **2022**
CNEFE load and retire the overlapping bespoke 2022 tables — *after* a ground-truth
accuracy comparison confirms parity or improvement. Retain the multi-vintage
(2010/2017) references, INEP school matching, the lightgbm arbitrator, the
panel-ID linkage, and the polling-station-specific normalization: none of these is
something geocodebr does.

---

## Sources

- geocodebr CRAN page (v0.6.3, 2026-05-24): https://cran.r-project.org/web/packages/geocodebr/index.html
- geocodebr CRAN README: https://cran.r-project.org/web/packages/geocodebr/readme/README.html
- geocodebr `geocode()` reference (precision cascade, tipo_resultado codes, tie resolution, output fields): https://ipea.github.io/geocodebr/reference/geocode.html
- geocodebr NEWS / changelog: https://ipea.github.io/geocodebr/news/index.html
- geocodebr GitHub releases: https://github.com/ipeaGIT/geocodebr/releases
- geocodebr GitHub repo / README: https://github.com/ipeaGIT/geocodebr
- Urban Demographics blog (CadÚnico 43M in ~65 min; "first fully free open-source geocoder"): https://www.urbandemographics.org/post/geocoding-brazilian-data-with-geocodebr/
- IBGE CNEFE 2022 (100% georeferenced, 106M+ addresses, released 2024): https://www.ibge.gov.br/novo-portal-destaques/40076-ibge-divulgara-em-14-de-junho-o-cadastro-nacional-de-enderecos-para-fins-estatisticos-cnefe-atualizado-no-censo-demografico-2022.html
- IBGE CNEFE 2022 FTP (raw CSV): https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/Censo_Demografico_2022/Arquivos_CNEFE/CSV/
- INEP school catalog coordinates (SIRGAS 2000 / EPSG:4674): https://ecossistemagis.com/coordenadas-do-catalogo-de-escolas-do-inep-no-qgis/
- geobr `read_schools()` (INEP school geolocation via R): https://ipeagit.github.io/geobr/reference/read_schools.html
- geobr package (boundaries, not a geocoder): https://ipeagit.github.io/geobr/

### In-repo references cited
- `renv.lock:918` — geocodebr pinned at 0.2.1
- `R/string_matching.R:448–556` — `match_geocodebr_muni()` (fields passed, defensive wrapping, output columns)
- `R/data_cleaning.R:949–979` — `clean_text_for_geocodebr()`, `simplify_address_for_geocodebr()` (strips house number)
- `R/data_cleaning.R:581` / `R/string_matching.R:193` — INEP cleaning and school matching
- `R/model.R:117–140` — geocodebr long-format + synthetic-distance-from-precisao logic
- `_targets.R:285–491` — CNEFE 2010/2022 school extraction, INEP import targets
