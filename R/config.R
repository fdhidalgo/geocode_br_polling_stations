## Pipeline configuration: dev/production settings, crew controllers, and the
## enumeration of tracked input files.

library(data.table)
library(crew)

# The two smallest states by population (Acre and Roraima), processed in dev mode for
# fast testing, with their municipality counts per IBGE's 2022 figures.
DEV_STATE_MUNICIPALITY_COUNTS <- c(AC = 22, RR = 15)
DEV_STATES <- names(DEV_STATE_MUNICIPALITY_COUNTS)

# Pipeline configuration for the current mode. This object is the
# `pipeline_config` target, so every field it carries is hashed and cascades
# invalidation to the whole pipeline; it holds only machine-independent,
# behavior-affecting settings. Worker counts live in get_crew_controllers().
get_pipeline_config <- function(dev_mode) {
  if (dev_mode) {
    message("Running in DEVELOPMENT MODE, processing states: ", paste(DEV_STATES, collapse = ", "))
  } else {
    message("Running in PRODUCTION MODE, processing all Brazilian states")
  }

  list(
    dev_mode = dev_mode,
    # Process the dev subset in dev mode, all states (NULL) in production.
    dev_states = if (dev_mode) DEV_STATES else NULL
  )
}

# The file-enumeration helpers below run when _targets.R is sourced, so
# tar_files_input() has its file vector before any target runs. They take
# dev_mode directly rather than the pipeline_config target for that reason.

# Extract the state abbreviation from a CNEFE filename, e.g.
# "cnefe_2010_AC.csv.gz" -> "AC". The cnefe_<year>_<STATE>.csv.gz convention is
# encoded only here, so dev filtering and per-branch state derivation cannot drift.
cnefe_state_from_file <- function(file, year) {
  sub(paste0("^cnefe_", year, "_(.+)\\.csv\\.gz$"), "\\1", basename(file))
}

# Per-state CNEFE .csv.gz paths for the given year (2010 or 2022), restricted to
# the dev subset in dev mode. One tracked branch per returned file.
get_cnefe_state_files <- function(year, dev_mode = FALSE, dev_states = DEV_STATES) {
  dir <- file.path("data", paste0("cnefe_", year))
  pattern <- paste0("^cnefe_", year, "_.*\\.csv\\.gz$")
  files <- list.files(dir, pattern = pattern, full.names = TRUE)
  if (dev_mode) {
    states <- cnefe_state_from_file(files, year)
    # Silently dropping a requested dev state would run a partial dev pipeline
    # that looks complete.
    missing <- setdiff(dev_states, states)
    if (length(missing) > 0L) {
      stop(sprintf(
        "Missing CNEFE %d file(s) for dev state(s): %s (under %s/)",
        year,
        paste(missing, collapse = ", "),
        dir
      ))
    }
    files <- files[states %in% dev_states]
  }
  if (length(files) == 0L) {
    stop(sprintf("No CNEFE %d state files found under %s/", year, dir))
  }
  sort(files)
}

# 2017 agro-CNEFE state file paths, restricted to the dev subset in dev mode.
# Agro filenames encode the IBGE UF code and name ("12_ACRE.csv.gz") rather than
# the state abbreviation, so the dev filter needs an explicit abbreviation ->
# filename map.
get_agro_cnefe_files <- function(dev_mode = FALSE, dev_states = DEV_STATES) {
  files <- list.files("data/agro_censo", pattern = "\\.csv\\.gz$", full.names = TRUE)
  if (dev_mode) {
    state_file_map <- c("AC" = "12_ACRE.csv.gz", "RR" = "14_RORAIMA.csv.gz")
    missing <- setdiff(dev_states, names(state_file_map))
    if (length(missing) > 0L) {
      stop(
        "get_agro_cnefe_files(): no agro filename mapping for dev state(s): ",
        paste(missing, collapse = ", ")
      )
    }
    wanted <- state_file_map[dev_states]
    # Silently dropping a mapped dev file would run a partial dev pipeline that
    # looks complete.
    absent <- setdiff(wanted, basename(files))
    if (length(absent) > 0L) {
      stop(
        "Missing agro-CNEFE file(s) for dev run under data/agro_censo/: ",
        paste(absent, collapse = ", ")
      )
    }
    files <- files[basename(files) %in% wanted]
  }
  if (length(files) == 0L) {
    stop("No agro-CNEFE files found under data/agro_censo/")
  }
  sort(files)
}

# Municipalities this run should produce: the dev states' summed IBGE counts in dev mode,
# the national total in production. Keeps the data-quality monitor from failing on a
# legitimately dev-filtered run.
expected_municipality_count <- function(dev_mode) {
  if (dev_mode) sum(DEV_STATE_MUNICIPALITY_COUNTS) else 5570
}

# Retain a strong reference to every processx handle the launcher creates, so
# crew's local launcher cannot SIGKILL a live worker when a pruned launch-handle
# row is garbage-collected in the main process.
#
# The wrapper must be installed with assign() into the launcher environment:
# `controller$launcher$launch_worker <- ...` fails because R's compound
# assignment writes the launcher back through the controller's read-only
# `launcher` active binding.
keep_crew_launch_handles <- function(controller) {
  launcher <- controller$launcher
  handles <- list()
  orig_launch_worker <- launcher$launch_worker
  wrapper <- function(call) {
    handle <- orig_launch_worker(call)
    handles[[length(handles) + 1L]] <<- handle
    handle
  }
  unlockBinding("launch_worker", launcher)
  assign("launch_worker", wrapper, envir = launcher)
  lockBinding("launch_worker", launcher)
  invisible(controller)
}

# The only sanctioned way to build a local crew controller here: constructing the
# controller and installing the handle keeper are inseparable, so no controller
# can silently skip the protection.
crew_controller_local_kept <- function(...) {
  keep_crew_launch_handles(crew::crew_controller_local(...))
}

# Build the crew controller group used for parallel processing. The same
# controllers serve dev and production; crew only spawns workers as needed.
get_crew_controllers <- function() {
  # Capture each worker's stdout/stderr to per-worker log files, so a worker that
  # dies mid-task leaves its actual dying error on disk instead of only the opaque
  # "worker crashed N consecutive times" message. Created eagerly: crew does not
  # mkdir it.
  crew_log_dir <- "crew_logs"
  dir.create(crew_log_dir, showWarnings = FALSE, recursive = TRUE)

  # Standard controller for most tasks - sized for a 32-core machine.
  controller_standard <- crew_controller_local_kept(
    name = "standard",
    workers = 28, # Max workers - crew only spawns as needed
    seconds_idle = Inf, # no idle churn
    seconds_wall = Inf, # no wall-time churn
    seconds_timeout = 300,
    reset_globals = TRUE,
    reset_packages = FALSE,
    garbage_collection = TRUE,
    options_local = crew::crew_options_local(log_directory = crew_log_dir)
  )

  # Memory-limited controller for CNEFE operations: fewer workers, more memory
  # per worker.
  controller_memory <- crew_controller_local_kept(
    name = "memory_limited",
    workers = 8, # Max workers for memory-intensive tasks
    seconds_idle = Inf, # no idle churn
    seconds_wall = Inf, # no wall-time churn
    seconds_timeout = 600, # 10 minutes timeout
    reset_globals = TRUE,
    reset_packages = FALSE,
    garbage_collection = TRUE,
    options_local = crew::crew_options_local(log_directory = crew_log_dir)
  )

  controller_group <- crew::crew_controller_group(
    controller_standard,
    controller_memory
  )

  return(controller_group)
}

# Set the global targets options: packages loaded on workers, storage format, and
# the crew controller group.
configure_targets_options <- function(controller_group) {
  tar_option_set(
    packages = c(
      "data.table",
      # R.utils is required by data.table::fread() to read the project's .csv.gz
      # inputs; fread loads it by string, so it must be named explicitly here (both
      # so workers can read gzipped data and so renv discovers/locks it).
      "R.utils",
      "stringr",
      "stringdist",
      "validate",
      "sf",
      "reclin2",
      "bonsai",
      # lightgbm is the bonsai engine dispatched to during model training; it is
      # referenced only as the string "lightgbm" in set_engine(), so it must be
      # named explicitly here (both to load it on workers and so renv discovers it).
      "lightgbm",
      # qs2 backs format = "qs"; with retrieval = "worker" the workers deserialize
      # qs2-formatted targets, so it is a genuine worker dependency (and, being a
      # targets Suggests invoked by string config, is otherwise undiscoverable).
      "qs2",
      "geosphere",
      "rsample",
      "recipes",
      "parsnip",
      "workflows",
      "yardstick",
      "finetune",
      # lme4 is required by finetune::tune_race_anova(): the racing procedure fits a
      # mixed-effects ANOVA model (via lme4) to eliminate hyperparameter candidates.
      # finetune loads it by string, so it must be named explicitly for renv.
      "lme4",
      "tune"
    ),
    format = "qs",
    controller = controller_group,
    storage = "worker",
    retrieval = "worker",
    memory = "transient",
    garbage_collection = TRUE,
    resources = tar_resources(
      crew = tar_resources_crew(controller = "standard") # Default to standard controller
    )
  )
}
