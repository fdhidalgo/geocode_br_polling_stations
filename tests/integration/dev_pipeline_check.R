#!/usr/bin/env Rscript
## Dev-mode (AC/RR) end-to-end integration check.
##
## Builds the pipeline fresh in development mode (the two smallest states, Acre
## and Roraima) and asserts seven structural properties over the two final output
## files. Several assertions are deliberate tripwires for headline findings in the
## 2026-07 code-health audit (see docs/specs/2026-07-testing-spec.md §4).
##
## This is NOT part of the fast unit suite (Rscript tests/testthat.R). It runs
## tar_make(), so it needs the CNEFE/TSE inputs and real memory, and takes minutes.
##
## Run with:  Rscript tests/integration/dev_pipeline_check.R
##
## Isolation: it sets TAR_PROJECT=dev, so all targets read/write the isolated dev
## store (_targets_dev/). The export step still writes the shared output/*.csv.gz
## paths, so this OVERWRITES any production output/ files with AC/RR data -- an
## accepted limitation of the dev/prod split (the output paths are not yet
## profile-isolated). Regenerate production outputs with a full `tar_make()`.
##
## Exit code is non-zero if any check fails, so it can later become a CI job.

Sys.setenv(TAR_PROJECT = "dev")
suppressPackageStartupMessages({
  library(testthat)
  library(data.table)
  library(targets)
})

# --- Build the two final outputs fresh (pulls all upstream targets) ------------
message("Building dev-mode pipeline outputs (this takes minutes)...")
tar_make(names = tidyselect::all_of(c("geocoded_export", "panelid_export")))

geocoded_path <- tar_read(geocoded_export)
panel_path <- tar_read(panelid_export)

# --- Small runner: run every check, report each, exit non-zero on any failure --
run_check <- function(desc, code) {
  ok <- tryCatch(
    {
      test_that(desc, code)
      TRUE
    },
    error = function(e) {
      cat(sprintf("✗ FAIL: %s\n       %s\n", desc, conditionMessage(e)))
      FALSE
    }
  )
  if (ok) {
    cat(sprintf("✓ PASS: %s\n", desc))
  }
  ok
}

# Brazil bounding box (loose), used for the coordinate sanity tripwire.
br_lat <- c(-34, 6)
br_long <- c(-74, -34)

# Columns required on the *published* geocoded file. The recommended coordinate
# ships as long/lat (0.141-compatible names); the internal table calls it
# final_long/final_lat and is remapped at export by to_geocoded_export_schema().
geocoded_required_cols <- c(
  "local_id",
  "lat",
  "long",
  "ano",
  "nr_zona",
  "nr_locvot",
  "nm_locvot",
  "nm_localidade"
)
panel_required_cols <- c("local_id", "panel_id", "long", "lat")

results <- c(
  # 1. Both output files exist and are non-empty. (tripwire: H5 side effects.)
  run_check("1. output files exist and are non-empty", {
    expect_true(file.exists(geocoded_path))
    expect_true(file.exists(panel_path))
    expect_gt(file.info(geocoded_path)$size, 0)
    expect_gt(file.info(panel_path)$size, 0)
  })
)

# Read the actual output files (true end-to-end: assert on what was written).
geocoded <- fread(geocoded_path)
panel <- fread(panel_path)

results <- c(
  results,
  # 2. Required columns present on both outputs.
  run_check("2. required columns present", {
    expect_true(all(geocoded_required_cols %in% names(geocoded)))
    expect_true(all(panel_required_cols %in% names(panel)))
  }),

  # 3. Uniqueness invariants.
  run_check("3. uniqueness holds on the geocoded key and the panel mapping", {
    expect_equal(
      uniqueN(geocoded[, .(local_id, ano, nr_zona, nr_locvot)]),
      nrow(geocoded)
    )
    # Each polling station maps to exactly one panel_id. (Duplicate *rows* are a
    # separate tracked bug; this asserts the mapping is well-defined, which is
    # the invariant that matters.)
    per_station_ids <- panel[, uniqueN(panel_id), by = local_id]
    expect_true(all(per_station_ids$V1 == 1L))
  }),

  # 4. Coordinate sanity + not-all-NA. (tripwire: C5 geocodebr silent-vanish.)
  run_check("4. coordinates are within Brazil and not entirely NA", {
    expect_false(all(is.na(geocoded$lat)))
    expect_false(all(is.na(geocoded$long)))
    lat <- geocoded$lat[!is.na(geocoded$lat)]
    long <- geocoded$long[!is.na(geocoded$long)]
    expect_true(all(lat >= br_lat[1] & lat <= br_lat[2]))
    expect_true(all(long >= br_long[1] & long <= br_long[2]))
    expect_false(all(is.na(panel$lat)))
    expect_false(all(is.na(panel$long)))
  }),

  # 5. Exactly {AC, RR} present. (tripwire: H2 dev-filter fallback -> all states.)
  run_check("5. exactly the AC and RR states are present", {
    expect_setequal(unique(geocoded$sg_uf), c("AC", "RR"))
  }),

  # 6. Expected years present, including the un-validated 2024 integration.
  run_check("6. expected election years present, including 2024", {
    years <- sort(unique(geocoded$ano))
    expect_true(2024 %in% years)
    expect_gte(length(years), 2)
    expect_true(all(years %in% 2006:2024))
  }),

  # 7. Row count in a loose sane range for AC+RR (non-trivial, not absurd).
  run_check("7. row count is in a sane range for AC+RR", {
    expect_gt(nrow(geocoded), 2000)
    expect_lt(nrow(geocoded), 500000)
    expect_gt(nrow(panel), 0)
  })
)

n_fail <- sum(!results)
cat(sprintf("\n%d/%d integration checks passed.\n", sum(results), length(results)))
if (n_fail > 0) {
  stop(sprintf("Dev-mode integration check FAILED: %d of %d checks failed.", n_fail, length(results)))
}
cat("Dev-mode integration check PASSED.\n")
