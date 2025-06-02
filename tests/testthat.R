# This file is part of the standard testthat setup.
# Do not edit by hand.

library(testthat)
library(stringr)
library(stringi)
library(data.table)

# Set working directory to project root if needed
if (basename(getwd()) == "tests") {
  setwd("..")
}

# Source all R files from the R directory
R_files <- list.files("R", pattern = "\\.R$", full.names = TRUE)
for (file in R_files) {
  source(file)
}

# Run tests
test_dir("tests/testthat")