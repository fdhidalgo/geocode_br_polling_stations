# Minimal test runner for the evaluation-harness pure helpers.
# The project has no full package test suite; these tests cover the deterministic
# helpers in R/evaluation.R that are cheap to check with synthetic data.
# Run with: Rscript -e "testthat::test_dir('tests/testthat')"
library(testthat)
library(data.table)

# Source only the units under test (avoid loading the whole pipeline).
source(file.path("R", "evaluation.R"))

test_dir("tests/testthat")
