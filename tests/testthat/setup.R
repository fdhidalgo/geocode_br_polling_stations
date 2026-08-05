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

# The fixtures carry accented Brazilian text (column names like "Município",
# addresses like "Avenida São João"), and the cleaning functions under test are
# the ones that de-accent it. In a non-UTF-8 locale R converts that text to the
# native encoding as it parses each test file — accented characters become
# literal "<U+00ED>" escapes — so the fixtures never reach the functions intact.
# Establish the encoding the fixtures assume before any test file is parsed.
if (!l10n_info()[["UTF-8"]]) {
  for (locale in c("C.UTF-8", "en_US.UTF-8", "C.utf8")) {
    suppressWarnings(Sys.setlocale("LC_CTYPE", locale))
    if (l10n_info()[["UTF-8"]]) break
  }
  if (!l10n_info()[["UTF-8"]]) {
    stop("The test suite needs a UTF-8 locale; none of C.UTF-8, en_US.UTF-8, C.utf8 are installed.")
  }
}

testthat::local_edition(3)
targets::tar_source(file.path(find_project_root(), "R"))
