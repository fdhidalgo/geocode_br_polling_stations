## Freeze the current production evaluation outputs before a methodology change overwrites
## the shared S3 store. The pipeline's own `baseline_accuracy_tables` is the trivial-heuristic
## baseline (evaluation spec 7a), not the previous methodology's numbers, so without this
## there is nothing to compare an upgrade against.
##
## Run from the main checkout, on the branch whose numbers you want to keep, before building
## the candidate branch:  Rscript scripts/snapshot_methodology_baseline.R [label]

suppressMessages({
  library(targets)
  library(data.table)
})

if (nzchar(Sys.getenv("TAR_PROJECT"))) {
  stop("TAR_PROJECT is set; unset it so this reads the production store.")
}

label <- commandArgs(trailingOnly = TRUE)[1]
if (is.na(label)) {
  label <- "baseline"
}

commit <- system("git rev-parse --short HEAD", intern = TRUE)
branch <- system("git rev-parse --abbrev-ref HEAD", intern = TRUE)

snapshot <- list(
  label = label,
  commit = commit,
  branch = branch,
  taken_at = Sys.time(),
  accuracy_tables = tar_read(accuracy_tables),
  oof_selected_matches = tar_read(oof_selected_matches),
  baseline_comparison = tar_read(baseline_comparison),
  geocodebr_vs_model = tar_read(geocodebr_vs_model)
)

path <- file.path("output", sprintf("methodology_snapshot_%s.rds", label))
saveRDS(snapshot, path)

cat(sprintf("wrote %s (%.1f MB)\n  %s @ %s\n", path, file.size(path) / 1e6, branch, commit))
at <- as.data.table(snapshot$accuracy_tables)
print(at[stratum == "overall", .(level, n_total, match_rate, median_km, within_500m)])
