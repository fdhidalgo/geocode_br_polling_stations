# Test setup: load the pipeline's functions exactly as tar_make() does.
#
# The suite deliberately reuses the pipeline's own loader (targets::tar_source),
# so tests exercise the same function definitions the pipeline runs; adding,
# renaming, or moving a function is picked up automatically with nothing to keep
# in sync. See docs/specs/2026-07-testing-spec.md (§1).
#
# testthat runs each file with the working directory set to tests/testthat, so we
# resolve the project root (the directory holding _targets.R) before sourcing R/.
find_project_root <- function(start = getwd()) {
  path <- normalizePath(start, mustWork = FALSE)
  repeat {
    if (file.exists(file.path(path, "_targets.R"))) {
      return(path)
    }
    parent <- dirname(path)
    if (identical(parent, path)) {
      stop("Could not locate project root (no _targets.R above ", start, ")")
    }
    path <- parent
  }
}

testthat::local_edition(3)
targets::tar_source(file.path(find_project_root(), "R"))
