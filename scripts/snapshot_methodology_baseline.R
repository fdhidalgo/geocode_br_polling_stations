## Freeze the current production evaluation outputs before a methodology change overwrites
## the shared S3 store. The pipeline's own `baseline_accuracy_tables` is the trivial-heuristic
## baseline (evaluation spec 7a), not the previous methodology's numbers, so without this
## there is nothing to compare an upgrade against.
##
##   Rscript scripts/snapshot_methodology_baseline.R <label> [built_from]
##
## `built_from` is the commit or branch that produced the store's contents. It is not
## necessarily HEAD: a checkout can be switched after a build, leaving the store holding
## results from code that is no longer checked out. Nothing in the targets metadata records
## which commit built a target, so this cannot be detected -- it has to be stated. When
## omitted it defaults to HEAD and the script says so.

suppressMessages({
  library(targets)
  library(data.table)
})

if (nzchar(Sys.getenv("TAR_PROJECT"))) {
  stop("TAR_PROJECT is set; unset it so this reads the production store.")
}

args <- commandArgs(trailingOnly = TRUE)
label <- args[1]
if (is.na(label)) {
  stop("Give the snapshot a label: Rscript scripts/snapshot_methodology_baseline.R <label> [built_from]")
}

head_commit <- system("git rev-parse --short HEAD", intern = TRUE)
head_branch <- system("git rev-parse --abbrev-ref HEAD", intern = TRUE)

built_from <- args[2]
if (is.na(built_from)) {
  built_from <- head_commit
  cat(sprintf("built_from not given; assuming the store was built from HEAD (%s %s).\n", head_branch, head_commit))
  cat("If the checkout was switched after the build, re-run with the building commit as the second argument.\n")
}

snapshot <- list(
  label = label,
  built_from = built_from,
  head_at_snapshot = head_commit,
  taken_at = Sys.time(),
  accuracy_tables = tar_read(accuracy_tables),
  oof_selected_matches = tar_read(oof_selected_matches),
  baseline_comparison = tar_read(baseline_comparison),
  geocodebr_vs_model = tar_read(geocodebr_vs_model)
)

path <- file.path("output", sprintf("methodology_snapshot_%s.rds", label))
saveRDS(snapshot, path)

cat(sprintf("wrote %s (%.1f MB)\n  built_from %s, HEAD %s\n", path, file.size(path) / 1e6, built_from, head_commit))
at <- as.data.table(snapshot$accuracy_tables)
print(at[stratum == "overall", .(level, n_total, match_rate, median_km, within_500m)])
