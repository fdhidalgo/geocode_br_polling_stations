#!/usr/bin/env Rscript
## Dev-mode (AC/RR) equivalence harness (spec 2026-07-partition-reference-data,
## decision D7).
##
## Purpose (plain language): the CNEFE reshape (issue #67) changes *how* the
## reference aggregates are built but is meant to leave *what* they contain
## unchanged. This script is the detector for that. It snapshots a set of target
## values built in dev mode, and later compares a fresh build against that
## snapshot, reporting per-target PASS/FAIL at identical() strictness.
##
## It is a DETECTOR, not a hard gate: acceptance is "every diff explained and
## accepted," not "no diffs." The one expected diff in issue #67 is the
## deliberate `norm_desc != ""` harmonization of 2010 schools, which shifts
## `schools_cnefe10`'s row count (and possibly a few downstream match rows).
##
## Usage:
##   # On master (baseline):
##   git checkout master
##   Rscript tests/integration/equivalence_check.R snapshot
##
##   # On the reshape branch:
##   git checkout <branch>
##   Rscript tests/integration/equivalence_check.R compare
##
## Each mode runs tar_make() in dev mode first, so the compared targets are
## rebuilt from the checked-out code before they are read. Both modes use the
## isolated dev store (_targets_dev/); the snapshot itself is written to a
## gitignored path (see snapshot_path) so it survives the branch switch.
##
## Exit code is non-zero if any target FAILs the comparison, so a run can gate
## CI once every accepted diff is encoded as an exception.

Sys.setenv(TAR_PROJECT = "dev")
suppressPackageStartupMessages({
  library(data.table)
  library(targets)
})

# --- Configuration -------------------------------------------------------------

# Snapshot file. Lives outside the targets store and outside git tracking so it
# persists across `tar_make()` rebuilds and `git checkout`.
snapshot_path <- "tests/integration/.equivalence_snapshot.rds"

# The targets compared (spec D7): six reference aggregates, seven match outputs,
# plus model_data and geocoded_locais. panel_ids and trained_model are excluded
# for the reasons given in D7 (untouched inputs; determinism given identical
# model_data and no RNG in the matching/cleaning/panel code).
compare_targets <- c(
  # six reference aggregates
  "cnefe10_st", "cnefe10_bairro",
  "cnefe22_st", "cnefe22_bairro",
  "schools_cnefe10", "schools_cnefe22",
  # seven match outputs
  "inep_string_match",
  "schools_cnefe10_match", "schools_cnefe22_match",
  "cnefe10_stbairro_match", "cnefe22_stbairro_match",
  "agrocnefe_stbairro_match",
  "geocodebr_match",
  # modeling inputs / outputs
  "model_data", "geocoded_locais"
)

# --- Helpers -------------------------------------------------------------------

# Strip data.table internals (key, secondary indices) that are irrelevant to
# value equality, and reduce to the underlying named list of columns. Comparing
# as.list() with identical() gives element-wise strict comparison of column
# names, order, types, and values while ignoring the `.internal.selfref`
# external pointer and sort/index attributes that identical() would otherwise
# trip on.
as_canonical <- function(x) {
  if (is.data.frame(x)) {
    x <- data.table::as.data.table(x)
    x <- data.table::copy(x)
    data.table::setattr(x, "sorted", NULL)
    data.table::setattr(x, "index", NULL)
    return(as.list(x))
  }
  x
}

# Locate the first element at which two atomic vectors differ (NA-aware), for a
# human-readable failure detail. Returns NULL if they are element-wise equal.
first_diff_detail <- function(a, b) {
  if (length(a) != length(b)) {
    return(sprintf(
      "row count differs (snapshot %d vs current %d)",
      length(a), length(b)
    ))
  }
  if (is.list(a) || is.list(b)) {
    # list-columns: fall back to a coarse index scan
    neq <- !mapply(identical, a, b)
  } else {
    neq <- (a != b)
    neq[is.na(a) & is.na(b)] <- FALSE
    neq[xor(is.na(a), is.na(b))] <- TRUE
    neq[is.na(neq)] <- FALSE
  }
  idx <- which(neq)
  if (!length(idx)) {
    return(NULL)
  }
  i <- idx[1]
  sprintf(
    "first differs at row %d (snapshot=%s, current=%s)",
    i, format(a[[i]]), format(b[[i]])
  )
}

# Compare one target's snapshot value against its current value. Returns a list
# with pass (logical) and detail (character, "" when passing).
compare_one <- function(name, snap, cur) {
  # "unavailable": one side is missing (target failed to build, or is absent from
  # the snapshot). This is a "could not verify," reported distinctly from a real
  # value diff so a broken-upstream target does not masquerade as a detected diff.
  if (is.null(snap)) {
    return(list(status = "unavailable", detail = "absent from snapshot"))
  }
  if (is.null(cur)) {
    return(list(status = "unavailable", detail = "absent from current build"))
  }

  snap_c <- as_canonical(snap)
  cur_c <- as_canonical(cur)

  # Non-tabular fallback: compare the canonical objects directly.
  if (!is.list(snap_c) || is.null(names(snap_c))) {
    if (identical(snap_c, cur_c)) {
      return(list(status = "pass", detail = ""))
    }
    return(list(status = "diff", detail = "values differ (non-tabular)"))
  }

  if (!identical(names(snap_c), names(cur_c))) {
    return(list(status = "diff", detail = sprintf(
      "column set/order differs: snapshot=[%s] current=[%s]",
      paste(names(snap_c), collapse = ", "),
      paste(names(cur_c), collapse = ", ")
    )))
  }

  for (col in names(snap_c)) {
    if (!identical(snap_c[[col]], cur_c[[col]])) {
      detail <- first_diff_detail(snap_c[[col]], cur_c[[col]])
      if (is.null(detail)) {
        # identical() FALSE but element-wise equal -> a type/attribute-level
        # difference (e.g. integer vs double storage). Surface it plainly.
        detail <- sprintf(
          "types differ (snapshot=%s, current=%s)",
          paste(class(snap_c[[col]]), collapse = "/"),
          paste(class(cur_c[[col]]), collapse = "/")
        )
      }
      return(list(
        status = "diff",
        detail = sprintf("column '%s': %s", col, detail)
      ))
    }
  }

  list(status = "pass", detail = "")
}

# --- Build the compared targets from the checked-out code ----------------------

mode <- commandArgs(trailingOnly = TRUE)[1]
if (is.na(mode) || !mode %in% c("snapshot", "compare")) {
  stop("Usage: equivalence_check.R [snapshot|compare]")
}

message("Building dev-mode compared targets (this takes minutes)...")
# Build each target independently and tolerate failures: if one target (or its
# upstream) is broken for a reason unrelated to this change, the others are still
# built and compared, and the broken one is reported as "BUILD FAILED" rather
# than aborting the whole run. callr_function = NULL runs the pipeline in this
# process so `compare_targets` (a local variable) is in scope for the tidyselect
# `names` expression; the default callr subprocess would not see it.
read_all <- function(names) {
  vals <- lapply(names, function(nm) {
    ok <- tryCatch({
      tar_make(names = tidyselect::all_of(nm), callr_function = NULL)
      TRUE
    }, error = function(e) {
      message(sprintf("BUILD FAILED for %s: %s", nm, conditionMessage(e)))
      FALSE
    })
    if (!ok) {
      return(NULL)
    }
    tryCatch(tar_read_raw(nm), error = function(e) NULL)
  })
  names(vals) <- names
  vals
}

current <- read_all(compare_targets)

# --- Snapshot mode -------------------------------------------------------------

if (mode == "snapshot") {
  saveRDS(current, snapshot_path)
  cat(sprintf(
    "Snapshot written to %s (%d targets).\n",
    snapshot_path, length(current)
  ))
  quit(status = 0)
}

# --- Compare mode --------------------------------------------------------------

if (!file.exists(snapshot_path)) {
  stop(sprintf(
    "No snapshot at %s. Run `equivalence_check.R snapshot` on the baseline first.",
    snapshot_path
  ))
}
snapshot <- readRDS(snapshot_path)

cat("\n=== Equivalence comparison (identical() strictness) ===\n")
statuses <- vapply(compare_targets, function(nm) {
  res <- compare_one(nm, snapshot[[nm]], current[[nm]])
  label <- switch(res$status,
    pass = "PASS ",
    diff = "DIFF ",
    unavailable = "UNAVL"
  )
  if (nzchar(res$detail)) {
    cat(sprintf("%s %s -- %s\n", label, nm, res$detail))
  } else {
    cat(sprintf("%s %s\n", label, nm))
  }
  res$status
}, character(1))

n_pass <- sum(statuses == "pass")
n_diff <- sum(statuses == "diff")
n_unavail <- sum(statuses == "unavailable")
cat(sprintf(
  "\n%d identical, %d differ, %d unavailable (of %d).\n",
  n_pass, n_diff, n_unavail, length(statuses)
))
if (n_unavail > 0) {
  cat(sprintf(
    paste0(
      "%d target(s) could not be built/compared (see UNAVL above) -- likely a ",
      "broken upstream unrelated to this change. Rerun in an environment where ",
      "they build to complete the gate.\n"
    ),
    n_unavail
  ))
}
if (n_diff > 0) {
  cat(sprintf(
    paste0(
      "%d target(s) differ. This is expected iff every diff is an accepted, ",
      "documented behavior change (e.g. the 2010 `norm_desc` harmonization). ",
      "Review each DIFF above before accepting.\n"
    ),
    n_diff
  ))
}
if (n_diff > 0 || n_unavail > 0) {
  quit(status = 1)
}
cat("All compared targets are identical to the snapshot.\n")
