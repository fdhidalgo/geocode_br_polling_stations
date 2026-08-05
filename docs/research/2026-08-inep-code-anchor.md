# Can INEP school codes anchor polling-station linkage ground truth?

**Verdict: secondary corroborating signal.** An INEP code can be recovered for
under a third of polling-station-years, so it cannot be the primary source of
auto-labels. What it does recover is unusually clean, and it is informative in
both directions — it finds real links the current engine misses *and* real false
merges the current engine makes. Use it as a high-precision partial label and as
a targeted probe for gap bridging, not as the ground-truth set.

Measured against production `panel_ids.csv.gz` and `geocoded_polling_stations.csv.gz`
(2006–2024, all states: 944,687 station-years, 108,171 panels) and
`data/inep_catalogo_das_escolas.csv.gz` (226,251 schools).

## The anchor does not exist yet

Worth stating first, because the question presupposes otherwise: **no INEP code
reaches any polling station today.** `match_inep_muni()` returns matched name and
address strings plus the matched school's coordinates, and discards
`codigo_inep`. Neither released output carries an INEP field. The code is
recoverable — `best_index` already points at the matched catalog row — but
building the anchor is a change, not a lookup.

Two properties of the source constrain everything below:

- **The catalog is a single undated snapshot.** There is no year column. It can
  assign a *current* code to a historical station-year name; it cannot show
  whether a school kept its code over 2006–2024. That question is not answerable
  from anything in this repo.
- **`clean_inep()` drops 87,455 schools (38.7%) for having no coordinates.** An
  identity anchor does not need coordinates. The filter is right for geocoding
  and wrong for linkage, and it costs 6.4 points of coverage (below).

## (a) What a code identifies

Verified directly against the catalog:

| Property | Finding |
|---|---|
| Format | 8 numeric digits, no exceptions in 226,251 rows |
| Uniqueness | 226,251 distinct codes for 226,251 schools — the code is the catalog's key |
| Internal structure | First two digits equal the IBGE UF code of the school's municipality in **100%** of rows (zero exceptions) |

The UF prefix is the load-bearing structural fact: the code is **state-scoped**.
A school relocating across a state line cannot keep its code, so cross-state
continuity is unrepresentable by construction. Within-state relocation is not
constrained this way.

From INEP's own guidance, the code identifies a **physical establishment**, not
an administrative unit or a network: each location of a multi-site institution is
registered separately precisely so that INEP issues different codes, while
distance-learning poles get no code of their own and report through their parent
location. Codes are requested through the state or municipal education
secretariat and issued by INEP after homologation in the school register
(Educacenso/CENSE).

## (b) Temporal stability

Documented intent: the code is permanent for the life of the school, with new
codes issued on merger (*fusão*), split (*desmembramento*), or structural change;
schools carry a *situação de funcionamento* of active, *paralisada*, or *extinta*,
and public schools are extinguished by an act of the education secretariat.

**Confidence here is lower than for (a),** and the report should not pretend
otherwise. INEP's authoritative manual (*Caderno de Conceitos e Orientações*)
could not be retrieved: `download.inep.gov.br` serves an incomplete TLS chain and
every fetch fails certificate verification. The permanence and reuse claims
therefore rest on secondary sources that agree with each other but are not
primary. **The decisive test is a separate download** — whether `CO_ENTIDADE`
persists across Censo Escolar year files — and it should be run before any design
leans on code permanence.

What our own data does show is the practically relevant form of the question. The
snapshot degrades monotonically as you go back in time:

| Year | 2006 | 2008 | 2010 | 2012 | 2014 | 2016 | 2018 | 2020 | 2022 | 2024 |
|---|---|---|---|---|---|---|---|---|---|---|
| Unambiguous code | 24.9% | 26.0% | 27.2% | 28.0% | 29.4% | 30.8% | 31.8% | 32.8% | 33.3% | 34.0% |

A 9-point spread across the panel, worst in the oldest years — which is exactly
where the linkage problem is hardest. Whatever mix of school openings, closures,
and name drift produces this, the anchor is weakest where it is most needed.

## (c) Coverage in our data

Exact match on normalized name, blocked within municipality (the pipeline's own
blocking key), over all 944,687 station-years. Three station-years carry an empty
IBGE municipality code and are dropped.

| Reference set and normalization | Unambiguous | Ambiguous | No match |
|---|---|---|---|
| Full catalog, pipeline normalization | **29.8%** | 1.2% | 69.0% |
| Full catalog, keeps "escola municipal" etc. | 9.0% | 0.1% | 91.0% |
| Coordinate-having only (what `clean_inep()` keeps) | 23.4% | 0.5% | 76.0% |
| Coordinate-having only, less lossy names | 6.8% | 0.0% | 93.2% |

Two things to read off this table:

**Stripping the generic school terms is what makes matching work at all.**
Keeping "escola municipal" cuts coverage from 29.8% to 9.0%. TSE and INEP write
the same school's type differently — "E.M.E.I.E.F." against "ESCOLA MUNICIPAL" —
so any normalization that preserves the type guarantees a mismatch. The existing
`normalize_school()` is the right instinct.

**Ambiguity is not the problem.** Only 1.2% of station-years match a name shared
by two or more codes in the municipality, so the collision risk from stripping
distinguishing words ("municipal" vs. "estadual") is real but small. Missing
matches outnumber ambiguous ones 57 to 1.

### Coverage failures are largely ours, not INEP's

Three states collapse, and the reason is visible in the raw names:

| Lowest | | Highest | |
|---|---|---|---|
| PR | 0.5% | PE | 48.7% |
| DF | 2.5% | GO | 46.7% |
| SC | 9.2% | MG | 45.2% |

In PR and SC the catalog writes the school type as an **appended abbreviation** —
`AGOSTINHO STEFANELLO E E EF`, `ALVINO MENDONCA C M E I VER`, `NEIM CRISTO
REDENTOR`, `CENTRO EDUC NS MONTE SERRAT` — while TSE writes a spelled-out prefix
(`GRUPO ESCOLAR DOM BOSCO`). In DF the catalog abbreviates (`CEE 02 DE BRASILIA`)
where TSE spells out (`CENTRO DE ENSINO FUNDAMENTAL 02 DE BRASILIA`). Those
suffix forms are mostly absent from `school_synonyms`, so they survive
normalization and defeat the match.

This is a normalization defect, not a property of INEP codes. Coverage by
municipality size is flat by comparison (36.9% for the smallest municipalities
down to 28.7% for the 201–1000 band), and among unambiguous matches 65.5% of
schools are urban, 29.4% rural — so the shortfall is not concentrated in rural
areas either.

### Fuzzy matching does not rescue recall

Near-exact fuzzy matching on the names exact matching missed, as normalized
Levenshtein distance, cumulative on top of 29.8%:

| Threshold | ≤0.05 | ≤0.10 | ≤0.15 | ≤0.20 | ≤0.30 |
|---|---|---|---|---|---|
| Cumulative coverage | 31.1% | 33.2% | 36.3% | 40.8% | 48.8% |

Even a loose 0.30 threshold — well past where a name match is trustworthy as an
*identity* claim — stops just short of half. Requiring the best match to be a
single code costs almost nothing (48.3% at 0.30), confirming ambiguity is not the
binding constraint. **The realistic ceiling for a name-derived INEP anchor is
33–41%.** Fixing the state-specific normalization gaps above is the only lever
likely to move it materially.

## (d) Failure modes

Restricting to the 281,625 station-years with an unambiguous code (29.8%); 7,530
of them (2.7%) sit in no current panel.

**One code, one year, two places — rare.** Of 279,854 (code, year) pairs, 1,642
(0.6%) cover more than one distinct polling place. Most are benign: 62.6% of
those are within 100 m, i.e. one building registered twice. 22.5% are more than
5 km apart and are genuinely wrong matches. That puts clearly-wrong code
assignments at roughly 0.13% of the anchor set — a high-precision signal.

**The anchor agrees with the current engine 97.3% of the time.** Among 35,864
panels holding two or more coded station-years, all codes agree in 34,881.

**Where it disagrees, the sampled cases are engine errors, not anchor errors.**
In all four inspected disagreements the TSE name itself changes to a different
school mid-panel — `EEEFM ESTUDO E TRABALHO` → `E.E.E.F.M. MARIANA`, `ANTONIO
FERREIRA DA SILVA` → `ERMELINDO MONTEIRO BRASIL`, `JADER MACHADO` → `HUMBERTO DE
CAMPOS`, `FRANCISCO SEVERIANO DA SILVEIRA` → `ANTONIO FENELON DE OLIVEIRA`. These
are two different schools welded into one panel, and the code correctly splits
them. Four examples do not license a claim about all 983; the rate of
engine-error versus anchor-error in that set needs the labeling pass. But the
direction is consistent and the evidence sits in TSE's own name field.

**It finds the gap-crossing links the current architecture cannot represent.**
1,156 codes (3.1% of 37,899 codes appearing in panels) span more than one panel —
candidate merges. 81% of those panel pairs are within 1 km. **742 are
gap-separated** (two panels, no shared year, spanning at least four years), and
678 of those are also within 1 km. This lands in the same range as the 848
gap-crossing exact-name successors in the motivating diagnosis, found by an
independent route.

All five inspected gap-separated candidates are unmistakably one school:

| Code | Panels | Evidence |
|---|---|---|
| 11027380 | 47 (2006–16), 757883 (2022–24) | same address `RUA DOS PIONEIROS, 2033`, ~150 m apart |
| 11001828 | 289 (2006–08), 293 (2016–24) | same avenue, identical coordinates |
| 12013021 | 811 (2006–08), 379265 (2014–24) | identical coordinates; street renamed `RUA SERTANEJO` → `ESTRADA DO SAO FRANCISCO` |
| 12013498 | 813 (2006–12), 876 (2016–24) | identical coordinates, same street, house number drift 124/115/125 |
| 12012165 | 872 (2006–18), 475025 (2022–24) | identical name, identical address, identical coordinates, gap only at 2020 |

The last is the sharpest: name, address, and coordinates all match exactly across
the gap, and the current engine still broke the chain — because adjacent-year
chaining has no edge to offer when 2020 is missing.

**It can attach orphans.** Of the 7,530 coded station-years in no panel, 3,871
(51.4%) carry a code that already appears inside an existing panel, so they can
be attached directly.

## What this means for the evaluation design

1. **Not a primary auto-label source.** 30% coverage — 33–41% with fuzzy — and
   worst in the oldest years. A ground-truth set built only from INEP codes would
   be badly unrepresentative of the cases linkage actually gets wrong.
2. **Yes as a high-precision partial label.** ~0.13% clearly-wrong assignments
   and 97.3% agreement with existing panels. Good enough to seed labels without
   human review on the unambiguous exact matches, reserving spot-checks for the
   disagreements.
3. **Yes as a targeted probe for gap bridging** — the specific failure this map
   exists to fix. 742 gap-separated candidates with 678 co-located, plus 3,871
   attachable orphans, is a ready-made positive-class sample that owes nothing to
   the current engine's adjacent-year assumption.
4. **It labels both error directions.** Cross-panel sharing finds missed links;
   within-panel disagreement finds false merges. An evaluation using only the
   first would miss half of what the anchor can measure.
5. **Two cheap fixes come before any of this.** Keep coordinate-less schools when
   matching for identity (+6.4 points), and extend `school_synonyms` to the
   suffix-abbreviation forms that sink PR, SC, and DF. Both raise the anchor's
   reach without touching the linkage engine.
6. **Confirm code permanence before relying on it.** Check `CO_ENTIDADE`
   persistence across Censo Escolar year files. Under the state-scoped prefix,
   cross-state continuity is impossible by construction; treat cross-state moves
   as out of the anchor's reach rather than as evidence of a break.

## Reproducing

Scripts are throwaway and not committed; the analysis reads production artifacts
read-only and writes nothing to the pipeline. Sequence: load and normalize the
catalog and station-years with the pipeline's own `normalize_school()`; exact-join
on (municipality, normalized name); stratify; scan unmatched names with
`stringdist` per municipality; join the unambiguous assignments to `panel_ids` for
the failure modes. Note that `stringdist` must thread internally —
`parallel::mclapply` deadlocks against `data.table`'s OpenMP pool.

## Sources

- [Censo Escolar FAQ — Inep](https://www.gov.br/inep/pt-br/acesso-a-informacao/perguntas-frequentes/censo-escolar) — *situação de funcionamento* values; school register procedures.
- [Microdados do Censo Escolar — Inep](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar) — microdata and data dictionary; the source for the deferred `CO_ENTIDADE` persistence test.
- [Censo Escolar — Base dos Dados](https://basedosdados.org/dataset/dae21af4-4b6a-42f4-b94a-4c2061ea9de5) — curated 2007–2024 mirror.
- [Caderno de Conceitos e Orientações do Censo Escolar 2024](https://download.inep.gov.br/pesquisas_estatisticas_indicadores_educacionais/censo_escolar/orientacoes/matricula_inicial/caderno_de_conceitos_e_orientacoes_censo_escolar_2024.pdf) — INEP's authoritative manual; **could not be retrieved**, incomplete TLS chain on the host.
- Structural facts in (a) are verified against `data/inep_catalogo_das_escolas.csv.gz` and `data/inep_codes.csv` directly, not taken from documentation.
