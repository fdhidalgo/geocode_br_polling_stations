# Unit test runner for the geocoding pipeline.
#
# This is a `targets` project, not an R package, so there is no DESCRIPTION and no
# devtools/usethis test tooling. tests/testthat/setup.R loads the pipeline's
# functions via targets::tar_source("R") — the same loader _targets.R uses — so
# the tests provably exercise the definitions tar_make() runs.
#
# Fast, hand-authored spec tests over pure functions only: no data files, no
# network, no _targets/ store. Runtime is seconds. The slow AC/RR end-to-end
# check lives separately in tests/integration/dev_pipeline_check.R.
#
# Run with: Rscript tests/testthat.R
library(testthat)

testthat::test_dir("tests/testthat")
