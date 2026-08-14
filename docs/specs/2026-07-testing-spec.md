# Testing spec: unit tests + dev-mode integration check

**Date:** 2026-07-10
**Wayfinder ticket:** [#20](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/20) (map: [#18](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/18))
**Inputs:** the testability inventory in [docs/audits/2026-07-code-health-audit.md](../audits/2026-07-code-health-audit.md) (ticket [#19](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/19)).
**Feeds:** the code-cleanup spec ([#21](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/21)), which owns the behavior-changing tests this spec hands off.

This is an **execution-ready spec**, not the implementation. It fixes every decision needed to
build the test suite; someone (agent or human) can now write the files without further design calls.

The strategy has exactly **two layers** — fast unit tests over pure functions, and a slow dev-mode
(AC/RR) end-to-end pipeline check — plus a **lint gate** at commit time. CI is out of scope (fog).

---

## 1. Harness and invocation

The project is a `targets` project, **not** an R package, and stays that way (no package restructure —
map decision). Tests reuse the pipeline's own function loader so they provably exercise the same
definitions `tar_make()` runs.

**Layout:**

```
tests/
  testthat.R                     # runner: testthat::test_dir("tests/testthat")
  testthat/
    setup.R                      # targets::tar_source("R"); testthat::local_edition(3)
    fixtures/                    # committed fixture files (see §2)
    test-normalize_address.R
    test-normalize_school.R
    ...                          # one test-<function>.R per covered function
  integration/
    dev_pipeline_check.R         # slow AC/RR end-to-end check (see §4)
```

**Decisions:**

- **No `DESCRIPTION`, no `devtools`/`usethis` test tooling.** A lone `DESCRIPTION` introduces a second
  dependency ledger (`Imports:`) that drifts against `renv.lock` (already an audit finding, H8) and
  makes the existing meaningless `@export` tags load-bearing. The single source of truth for "what
  code exists and how it loads" stays `tar_source("R")` in one place.
- **Loader:** `tests/testthat/setup.R` calls `targets::tar_source("R")` — the exact call `_targets.R`
  uses (`_targets.R:27`). Adding/renaming a function or splitting a file is picked up automatically by
  both the pipeline and the tests; nothing to keep in sync.
- **testthat edition:** call `testthat::local_edition(3)` in `setup.R` (no `DESCRIPTION` to declare
  `Config/testthat/edition: 3`).
- **Invocation:** `Rscript tests/testthat.R`. Document it in CLAUDE.md next to the `tar_make` commands.
- **Runtime:** seconds. No data files, no network, no `_targets/` store access. This is the suite that
  can later become a cheap CI job (see §6).

---

## 2. Fixture strategy

**Philosophy: spec tests, not characterization/snapshot tests.** A test asserts the *intended*
input→output contract of a function, hand-authored from the specification. It does **not** assert
"equals whatever the function produces today," because that would pin current behavior — including the
bugs the cleanup spec is about to fix (the audit warns cleanup will *intentionally* change behavior to
fail loud). Snapshotting current output would lock in the wrong thing.

**Concrete rules:**

- **Default: hand-constructed minimal inputs, inline in the test file.**
  - Scalar/vector string functions (`normalize_address`, `normalize_school`, `convert_coord`,
    `clean_text_for_geocodebr`, `simplify_address_for_geocodebr`): a vector of literals, each case
    targeting one rule, with the expected output written out. Deliberately include the audit's
    known-tricky inputs: `"Av. São João"` → `"avenida sao joao"`, `"S N"` → `"sn"`, a `zona rural`
    case, accented characters, and (for `convert_coord`) malformed DMS strings that must return
    `NA_real_`.
  - Data.table-in/data.table-out functions (`match_*_muni`, panel functions, `clean_inep`): build the
    smallest input with `data.table(...)` inline — e.g. a 3-row `locais_muni` + 3-row `inep_muni` with
    one obvious name-match, one address-match, one non-match. These functions are deterministic
    (trigram Jaccard via `stringdist`), so hand-built cases are stable.
- **Single exception — one curated real-string CSV.** For `normalize_address` / `normalize_school`,
  where real-world CNEFE messiness (diacritics, abbreviations, inconsistent spacing) is the point,
  commit **one** small CSV at `tests/testthat/fixtures/` — a couple dozen real, public, hand-picked
  example strings (not a random sample) with their expected normalized form. Reference it via
  `testthat::test_path("fixtures", "…")`.
- **No binary fixtures** (`.rds`/`.qs`): they are opaque in review and diffs. Inline or CSV only.
- **No anonymization step.** All source datasets (TSE polling stations, IBGE CNEFE, INEP schools) are
  public administrative data, and we are not bulk-sampling them anyway.

**Two mechanical caveats from the audit's inventory** (bake into the test conventions):

- Several "pure" functions `cat()`/`message()` progress output — wrap calls in `suppressMessages()`
  (and `capture.output()` if they use `cat()`) so test output stays clean.
- `standardize_column_names` mutates its `data.table` argument **by reference** — pass it a
  `data.table::copy()` in tests.

---

## 3. First tranche of coverage (~13 functions)

The pure-function inventory is ~40 functions (audit §"Testability inventory"). The spec commits to the
following **first tranche**, chosen for the best value-to-fixture-effort ratio and for guarding known
failure modes. Everything not listed is **explicitly deferred** to later tranches.

**Group 1 — string transforms** (trivial fixtures, immediate value):
`normalize_address` (`R/data_cleaning.R:485`), `normalize_school` (`:506`), `clean_inep` (`:581`),
`convert_coord` (`:636`), `clean_text_for_geocodebr` (`:949`), `simplify_address_for_geocodebr` (`:962`).
`convert_coord` earns inclusion by pinning the DMS-parse contract, including its `NA_real_`-on-malformed
return.

**Group 2 — matching core** (the algorithm that determines geocoding quality):
the four `match_*_muni` functions — `match_inep_muni` (`R/string_matching.R:193`),
`match_schools_cnefe_muni` (`:261`), `match_stbairro_cnefe_muni` (`:309`),
`match_stbairro_agrocnefe_muni` (`:377`) — plus the shared engine, since renamed to `match_strings`
(the chunking apparatus and its `prefilter_by_common_words` / `get_adaptive_chunk_size` helpers were
cut). Testing the engine + `match_inep_muni` covers most of the risk; the two `stbairro` variants are
~95% identical.

**Group 3 — panel identity** (the H6 row-order concern lives here):
`process_year_pairs` (`R/panel_creation.R:24`), `make_panel_ids` (`:74`), `make_panel_1block` (`:196`).

**Explicitly deferred to later tranches:** the `process_*_batch` utility loops, `create_two_level_blocked_pairs`,
`create_section_panel_mapping`, and the fail-silent utilities/validators — see §5 for why the last group
is not simply "later" but owned by a different ticket.

**Test file convention:** one `test-<function>.R` per covered function, mirroring the function name.

---

## 4. Dev-mode (AC/RR) end-to-end integration check

Dev mode restricts the pipeline to Acre and Roraima (the two smallest states) — minutes, not hours.
The pipeline already validates itself at stages via live targets (`validate_inputs`,
`validate_model_data`, `validate_predictions`, `validate_geocoded_output`); this check leans on those
for stage-by-stage validation and adds a thin top-line structural layer over the two final outputs.

**Invocation (decisions):**

- **Separate runner:** `tests/integration/dev_pipeline_check.R`, its own `Rscript` command, *not* part
  of `Rscript tests/testthat.R`. The unit suite must stay fast; a `tar_make()`-driven check would
  destroy that. Document the command next to the dev-mode commands in CLAUDE.md.
- **Builds fresh.** The check runs `tar_make()` in dev mode and then asserts — a true end-to-end test,
  not an assertion over a pre-existing artifact.
- **Depends on C1 (dev-store isolation).** Running `tar_make()` fresh should target an **isolated dev
  store** so it cannot touch production state. That isolation is the audit's C1 fix (`TAR_PROJECT`
  profiles), owned by the cleanup spec #21. Until C1 lands, this runner is a **stopgap** that runs
  against the developer's existing dev store; the spec states this dependency openly rather than hiding
  it. The check should not be wired into any automation until C1 is done.

**Assertions (plain testthat `expect_*`, no `validate`-package DSL).** Seven structural checks over the
two final outputs (`geocoded_polling_stations.csv.gz`, `panel_ids.csv.gz`), several deliberately chosen
as tripwires for headline audit findings:

1. Both output files exist and are non-empty. *(tripwire: H5 untracked side effects.)*
2. Required columns present — reuse `validate_final_output`'s `required_cols`
   (`_targets.R:1003`).
3. Uniqueness holds on `c("local_id", "ano", "nr_zona", "nr_locvot")` (geocoded, per `_targets.R:1013`)
   and on the panel key (panel_ids).
4. Coordinate sanity: lat/long within Brazil's bounding box (≈ lat `[-34, 6]`, long `[-74, -34]`), and
   the coordinate columns are **not entirely NA**. *(tripwire: C5 geocodebr positional-reattach /
   silent-vanish.)*
5. **Exactly `{AC, RR}` present — no other state.** *(tripwire: H2 — if a dev-mode filter silently fell
   back to the full dataset, all 27 states would appear. Single most valuable assertion in the check.)*
6. Expected years present, **including 2024**. *(tripwire: the un-validated 2024 integration; ties to
   [#22](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/22)/[#23](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/23).)*
7. Row count in a loose sane range for AC+RR — non-trivially nonzero, not absurdly large. **Not** an
   exact count (brittle).

---

## 5. Division of labor with the cleanup spec (#21)

The fail-silent functions the cleanup will change (contracts change from "return degraded data +
`warning()`" to `stop()`) are **not** tested here. Their intended fail-loud behavior is tested in **#21,
in the same commit as each fix**, so `master`'s suite is always green and never certifies a known bug.

- **This spec (#20) owns:** unit tests for the stable-behavior first tranche (§3), the fixture and
  fail-loud assertion **conventions**, the harness, the dev-mode check, and the lint gate.
- **#21 owns:** the behavior-changing tests, landing alongside their fixes — the fail-silent family
  (`filter_by_dev_mode` and siblings H2, the warn-not-stop validators H4, the `match_geocodebr_muni`
  cluster C5, etc.).

**Fail-loud assertion pattern to hand #21** (the convention #20 defines and #21 applies): for a function
whose fixed contract is to error on bad input, assert with
`expect_error(f(bad_input), "…message fragment…")`, and assert the happy path returns the correct
non-degraded result. Never assert the current silent/degraded output.

---

## 6. CI — out of scope (fog), with one de-risking note

No GitHub Actions is built. Recorded finding (the map asked the testing spec to weigh CI's cheap path):
**all three checks are single `Rscript` commands** —

- `Rscript tests/testthat.R`
- `Rscript tests/integration/dev_pipeline_check.R`
- `air format --check .`

— so a future CI job, *if* it graduates from fog, is a thin wrapper needing no restructuring. Note the
split: the **unit runner** is CI-cheap (seconds, no data) and is the natural first CI candidate; the
**integration runner** needs the full CNEFE/TSE inputs and 50GB+ RAM and is **not** a natural CI job.

---

## 7. Formatting gating

> **Amendment 2026-07-11 (#58):** the original ratchet below used `lintr` + a staged-files ratchet to
> pay down a formatting backlog gradually. That was replaced by [air](https://posit-dev.github.io/air/),
> a whole-repo formatter: the repo was formatted once (`air.toml`, `line-width = 120`), which eliminates
> the backlog entirely, so there is no longer a ratchet to maintain. `lintr` and `.lintr` were removed;
> air formats layout only and does not carry over lintr's semantic checks (naming, banned functions).
> The rest of this section is retained for history and describes the superseded lintr ratchet.

Formatting **gates at commit time** via a committed git hook. The whole repo is kept air-formatted, so
the gate is simply "stay formatted" — there is no backlog to ratchet down.

**Decisions:**

- **Mechanism:** a plain shell hook committed at `.githooks/pre-commit` (no new R-package dependency —
  air is a standalone binary — version-controlled, shared). Activated per-clone with
  `git config core.hooksPath .githooks`; documented in CLAUDE.md / AWS_SETUP.md.
- **Scope — staged `.R` files:** the hook runs `air format --check` against the staged `.R` files and
  blocks the commit if any would be reformatted. Because the repo was formatted up front, this stays
  green as long as edits are formatted (editor format-on-save, or `air format .`).
- **Correctness suite stays style-agnostic:** a formatting difference is never a `testthat` failure.
  Formatting is gated at commit time, at the right altitude.

_Superseded (lintr ratchet):_ the hook formerly ran `lintr::lint()` against only the staged `.R` files
using a `.lintr` config, blocking the commit if any staged file linted dirty, so the legacy backlog was
paid down gradually. A one-time `lintr::lint_dir("R")` baseline (632 violations, 2026-07) sized that
paydown. Both are retired by the air migration.

---

## Summary of decisions

| # | Decision |
|---|----------|
| 1 | Plain `tests/testthat/`; `setup.R` uses `tar_source("R")`; runner `Rscript tests/testthat.R`; no `DESCRIPTION`. |
| 2 | Hand-constructed **spec** tests (not snapshots); one curated real-string CSV for `normalize_*`; no binary fixtures; no anonymization. |
| 3 | First tranche ≈13 functions (Groups 1–3); rest explicitly deferred. |
| 4 | Separate dev-mode runner `tests/integration/dev_pipeline_check.R`; builds fresh; depends on C1 (stopgap until then). |
| 5 | Seven structural assertions as H2/C5/H5/2024 tripwires; plain testthat; no `validate`-DSL. |
| 6 | #20 = stable-behavior tests + conventions; #21 = behavior-changing tests alongside fixes; `master` always green. |
| 7 | CI out of scope (fog); "single-command" cheap-path note recorded. |
| 8 | Formatting gates via committed `.githooks/pre-commit` running `air format --check` on staged `.R` files; suite stays style-agnostic. _(Amended 2026-07-11, #58: air replaced the lintr ratchet — see §7.)_ |
