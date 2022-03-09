## Load your packages, e.g. library(drake).
source("./packages.R")

## Load your R files
lapply(list.files("./R", full.names = TRUE, pattern = "fns|plan"), source)

## Setup parallel processing
all_cores <- parallel::detectCores(logical = FALSE) - 1
library(doFuture)
library(parallel)
registerDoFuture()
cl <- makeCluster(all_cores)
future::plan(cluster, workers = cl)

## Do not use s2 spherical geometry package
sf::sf_use_s2(FALSE)

## Synchronize package library
renv::restore()

drake_config(the_plan,
        lock_envir = FALSE, memory_strategy = "lookahead",
        garbage_collection = TRUE
)