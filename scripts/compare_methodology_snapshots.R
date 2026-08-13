## Compare two methodology snapshots written by scripts/snapshot_methodology_baseline.R.
##
##   Rscript scripts/compare_methodology_snapshots.R <baseline_label> <candidate_label>
##
## Reads output/methodology_snapshot_<label>.rds for each. Prints the accuracy deltas the
## evaluation spec gates on, the trivial precedence selector beside them, and the shift in
## which source gets selected.
##
## Deltas are candidate minus baseline: within_500m up is better, median/p90/p95 down is
## better. The national within-500m figure is not the gate -- urban stations are two thirds
## of the universe, so it can sit flat while rural moves a point. Read the rural row and the
## p90/p95 tail. Selected-source churn is expected whenever the matcher changes, because
## mindist is a trained feature and the model re-learns on the new scale; churn on its own
## is not evidence either way.

suppressMessages(library(data.table))
options(width = 150)

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 2L) {
  stop("Usage: Rscript scripts/compare_methodology_snapshots.R <baseline_label> <candidate_label>")
}

read_snapshot <- function(label) {
  path <- file.path("output", sprintf("methodology_snapshot_%s.rds", label))
  if (!file.exists(path)) {
    stop("no snapshot at ", path)
  }
  readRDS(path)
}

base <- read_snapshot(args[1])
cand <- read_snapshot(args[2])

# Every delta below is a difference between two runs over the same stations. A dev-mode
# snapshot against a production one merges cleanly on stratum:level and prints deltas that
# are differences between populations, not methodologies.
universe <- function(snap) as.data.table(snap$accuracy_tables)[stratum == "overall", n_total]
stopifnot(
  "the two snapshots cover different station universes" = universe(base) == universe(cand)
)

cat(sprintf(
  "baseline  %s (built_from %s, taken %s)\ncandidate %s (built_from %s, taken %s)\n",
  base$label,
  base$built_from,
  format(base$taken_at),
  cand$label,
  cand$built_from,
  format(cand$taken_at)
))

METRICS <- c("match_rate", "median_km", "p90", "p95", "within_500m")

# Accuracy deltas on the strata the spec gates on. The match_source cut is excluded: its
# levels hold different stations under each run, so a per-level delta is not like-for-like.
delta_table <- function(a, b, metrics) {
  a <- as.data.table(a)[stratum != "match_source"]
  b <- as.data.table(b)[stratum != "match_source"]
  m <- merge(
    a[, c("stratum", "level", "n_total", metrics), with = FALSE],
    b[, c("stratum", "level", metrics), with = FALSE],
    by = c("stratum", "level"),
    suffixes = c("_base", "_cand"),
    all = TRUE
  )
  for (v in metrics) {
    m[, (paste0("d_", v)) := get(paste0(v, "_cand")) - get(paste0(v, "_base"))]
  }
  m[, c("stratum", "level", "n_total", paste0("d_", metrics)), with = FALSE]
}

acc <- delta_table(base$accuracy_tables, cand$accuracy_tables, METRICS)

cat("\n== accuracy delta (candidate - baseline) ==\n")
cat("within_500m up is better; median_km / p90 / p95 down is better.\n\n")
print(acc[stratum %in% c("overall", "urban_rural", "region", "urban_rural:region")], digits = 3)

# The trivial precedence selector, which trains on nothing. Read it in absolute terms:
# it breaks ties within a rank on mindist, so a change to the matcher moves it too, and
# it is model-free but not matcher-free. The model-minus-trivial gap is therefore not a
# control -- a matcher that improves raw ranking lifts the trivial selector most, and the
# gap narrows while both selectors get better. What the trivial column does isolate is
# match quality with no learned compensation on top.
cat("\n== trivial precedence selector, within_500m ==\n")
cat("d_trivial is the matcher's effect with nothing learned on top; d_model is beside it.\n\n")
bc_cols <- function(snap) {
  as.data.table(snap$baseline_comparison)[, .(
    stratum,
    level,
    trivial = within_500m_baseline,
    model = within_500m_model
  )]
}
bc <- merge(bc_cols(base), bc_cols(cand), by = c("stratum", "level"), suffixes = c("_base", "_cand"), all = TRUE)
bc[, `:=`(d_trivial = trivial_cand - trivial_base, d_model = model_cand - model_base)]
print(
  bc[
    stratum %in% c("overall", "urban_rural", "region"),
    .(stratum, level, trivial_base, trivial_cand, d_trivial, d_model)
  ],
  digits = 3
)

# Which source the model picks, and how accurate each pick is. Expect movement here.
cat("\n== selected match source ==\n")
source_share <- function(snap) {
  d <- as.data.table(snap$oof_selected_matches)[geocoded == TRUE]
  d[, .(share = 100 * .N / nrow(d), within_500m = 100 * mean(error_km <= 0.5)), by = match_source]
}
ms <- merge(
  source_share(base)[, .(match_source, share_base = share, w500_base = within_500m)],
  source_share(cand)[, .(match_source, share_cand = share, w500_cand = within_500m)],
  by = "match_source",
  all = TRUE
)
# A source absent from one run was picked zero times there -- an observed zero, so the
# share it gained or lost is reportable. Its accuracy stays NA: no picks, nothing to score.
ms[is.na(share_base), share_base := 0]
ms[is.na(share_cand), share_cand := 0]
ms[, d_share := share_cand - share_base]
print(ms[order(-share_cand)], digits = 3)
