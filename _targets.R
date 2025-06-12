## Targets pipeline configuration for geocoding Brazilian polling stations
#
# This script defines a comprehensive targets pipeline for geocoding Brazilian polling stations
# using multiple data sources and advanced string matching techniques. The pipeline includes:
# - Data import and cleaning from various sources (CNEFE, INEP, TSE)
# - Validation stages for data quality
# - String matching and geolocation prediction
# - Geocoding of polling stations
# - Validation and export of results
#
# The pipeline supports both development (subset of states) and production (full Brazil) modes.
# Parallel processing is managed using the crew package for efficient computation.
# Load packages required to define the pipeline:
library(targets)
library(tarchetypes)
library(conflicted)
# library(future.apply) # No longer needed with dynamic branching
library(data.table)
library(crew)

# Set global options
# options(future.globals.maxSize = 2 * 1024^3) # No longer needed with crew/mirai

# Fix for quarto not being found in crew workers
# Set QUARTO_PATH environment variable if not already set
if (Sys.getenv("QUARTO_PATH") == "") {
  quarto_bin <- Sys.which("quarto")
  if (nzchar(quarto_bin)) {
    Sys.setenv(QUARTO_PATH = as.character(quarto_bin))
    message("Setting QUARTO_PATH to: ", quarto_bin)
  }
}

# Define the null coalescing operator
`%||%` <- function(x, y) if (is.null(x)) y else x

## Setup parallel processing with crew/mirai

# Limit data.table threads to prevent memory contention
data.table::setDTthreads(1)

# Set memory limits to prevent allocation errors
options(
  # Increase R memory limit (in MB)
  memory.limit = 100000, # 100GB limit
  # Garbage collection frequency
  gcinfo = FALSE
)


# Enable crew logging for debugging (optional)
if (!dir.exists("crew_logs")) {
  dir.create("crew_logs")
}
options(
  crew.log_directory = "crew_logs",
  crew.log_rotate = TRUE
)

# Create two controllers: one memory-limited for CNEFE operations, one standard
# Memory-limited controller for CNEFE cleaning and street/neighborhood matching
controller_memory <- crew::crew_controller_local(
  name = "memory_limited",
  workers = 3, # Limit to 3 workers to prevent memory exhaustion
  seconds_idle = 300, # Keep workers alive for batched work
  seconds_wall = 36000, # 10 hours for longer CNEFE processing
  seconds_timeout = 32400, # 9 hours timeout (was timing out at 8h 25m)
  tasks_max = 1, # Process one task then restart worker to clear memory
  tasks_timers = 1, # Start idle timer after first task
  seconds_interval = 0.1, # Faster polling for task dispatch
  reset_globals = TRUE, # Reset globals to prevent memory accumulation
  reset_packages = TRUE, # Reset packages to clear cached data
  reset_options = TRUE, # Reset options for clean state
  garbage_collection = TRUE, # Force GC for memory management
)

# Standard controller for all other operations
controller_standard <- crew::crew_controller_local(
  name = "standard",
  workers = 28, # Use most cores for parallel processing
  seconds_idle = 60, # Shorter idle for faster worker recycling
  seconds_wall = 14400, # 4 hours wall time (increased from 1 hour)
  seconds_timeout = 3600, # 1 hour timeout (increased from 5 minutes)
  tasks_max = 50, # Moderate limit to improve task distribution
  tasks_timers = 1, # Start idle timer after first task
  seconds_interval = 0.05, # Very fast polling for quick dispatch
  reset_globals = FALSE, # Keep globals for performance
  reset_packages = FALSE,
  reset_options = FALSE,
  garbage_collection = FALSE # Skip GC for speed
)

# Create controller group
controller_group <- crew::crew_controller_group(
  controller_memory,
  controller_standard
)

# Only start controllers when tar_make() is running
# This prevents orphaned workers when sourcing _targets.R interactively
if (targets::tar_active()) {
  controller_group$start()

  # Register cleanup on exit
  on.exit(
    {
      if (!is.null(controller_group)) {
        controller_group$terminate()
      }
    },
    add = TRUE
  )
}

# Set target options:
tar_option_set(
  packages = c(
    "conflicted",
    "targets",
    "data.table",
    "stringr",
    "bonsai",
    # "future", # Replaced by crew/mirai
    "reclin2",
    # "future.apply", # No longer needed with dynamic branching
    "validate",
    "geosphere",
    "sf",
    "geocodebr"
  ),
  format = "qs", # default storage format - better compression
  memory = "transient", # Free memory after each target
  garbage_collection = TRUE, # Force gc() after each target
  storage = "main", # Default to main storage (override for large objects)
  retrieval = "main", # Default to main retrieval (override for large objects)
  controller = controller_group, # Use controller group
  resources = tar_resources(
    crew = tar_resources_crew(controller = "standard") # Default to standard controller
  )
)

library(progressr)

# Development mode flag - set to TRUE for faster iteration with subset of states
DEV_MODE <- FALSE # Process only AC, RR states when TRUE

# Load the R scripts with your custom functions:
lapply(list.files("./R", full.names = TRUE, pattern = "fns"), source)
# Load quarto path fix for crew workers
source("./R/quarto_path_fix.R")
# Load geocodebr matching functions
source("./R/geocodebr_matching.R")
# Load parallel processing functions
# source("./R/parallel_processing_fns.R") # No longer needed with unified controller
source("./R/parallel_integration_fns.R")
# Load memory-efficient CNEFE processing
source("./R/memory_efficient_cnefe.R")
# Load validation functions
source("./R/functions_validate.R")
# Load comprehensive validation framework
source("./R/validation_pipeline_stages.R")
# Load Brasília filtering functions
source("./R/filter_brasilia_municipal.R")
source("./R/validate_brasilia_filtering.R")
source("./R/expected_municipality_counts.R")
source("./R/update_validation_for_brasilia.R")
source("./R/validation_reporting.R")
source("./R/validation_report_renderer.R")
# Load column mapping functions
source("./R/column_mapping.R")
# Load state filtering functions
source("./R/state_filtering.R")

## Do not use s2 spherical geometry package
# sf::sf_use_s2(FALSE)

# Get pipeline configuration based on development mode
pipeline_config <- get_pipeline_config(DEV_MODE)

# Print configuration info
if (pipeline_config$dev_mode) {
  message("Running in DEVELOPMENT MODE")
  message(
    "Processing states: ",
    paste(pipeline_config$dev_states, collapse = ", ")
  )
} else {
  message("Running in PRODUCTION MODE")
  message("Processing all Brazilian states")
}

# Replace the target list below with your own:
list(
  ## import identifiers
  tar_target(
    name = muni_ids_file,
    command = "./data/muni_identifiers.csv",
    format = "file"
  ),
  tar_target(
    name = muni_ids_all,
    command = fread(muni_ids_file)
  ),
  tar_target(
    name = muni_ids,
    command = {
      if (pipeline_config$dev_mode) {
        muni_ids_all[estado_abrev %in% pipeline_config$dev_states]
      } else {
        muni_ids_all
      }
    }
  ),
  tar_target(
    name = validate_muni_ids,
    command = {
      result <- validate_import_stage(
        data = muni_ids,
        stage_name = "muni_ids",
        expected_cols = c(
          "id_munic_7",
          "id_munic_6",
          "id_TSE",
          "estado_abrev",
          "municipio"
        ),
        min_rows = ifelse(pipeline_config$dev_mode, 30, 5000) # AC+RR have ~37 municipalities, Brazil has ~5,570
      )
      if (!result$passed) {
        warning(
          "Municipality identifiers validation failed - check data quality"
        )
      }
      result
    }
  ),
  tar_target(
    name = inep_codes_file,
    command = "./data/inep_codes.csv",
    format = "file"
  ),
  tar_target(
    name = inep_codes,
    command = fread(inep_codes_file)
  ),
  tar_target(
    name = validate_inep_codes,
    command = {
      result <- validate_import_stage(
        data = inep_codes,
        stage_name = "inep_codes",
        expected_cols = c("codigo_inep", "id_munic_7"),
        min_rows = ifelse(pipeline_config$dev_mode, 1000, 100000) # Expect many schools
      )
      if (!result$passed) {
        warning("INEP codes validation failed - check data quality")
      }
      result
    }
  ),
  ## import shape files
  tar_target(
    name = tract_shp_file,
    command = "./data/census_tracts2010_shp.rds",
    format = "file"
  ),
  tar_target(
    name = tract_shp_all,
    command = sf::st_make_valid(readRDS(tract_shp_file))
  ),
  tar_target(
    name = tract_shp,
    command = {
      if (pipeline_config$dev_mode) {
        # Filter census tracts to dev states using sf-aware subsetting
        dev_state_codes <- substr(as.character(muni_ids$id_munic_7), 1, 2)
        # Use sf's subsetting to preserve geometry
        tract_filtered <- tract_shp_all[
          substr(tract_shp_all$code_tract, 1, 2) %in% unique(dev_state_codes),
        ]
        # Ensure it's still a valid sf object
        sf::st_as_sf(tract_filtered)
      } else {
        tract_shp_all
      }
    }
  ),
  tar_target(
    name = muni_shp_file,
    command = "./data/muni_shp.rds",
    format = "file"
  ),
  tar_target(
    name = muni_shp_all,
    command = sf::st_make_valid(readRDS(muni_shp_file))
  ),
  tar_target(
    name = muni_shp,
    command = {
      if (pipeline_config$dev_mode) {
        # Filter municipality shapes to dev states using sf-aware subsetting
        dev_muni_codes <- muni_ids$id_munic_7
        # Use sf's subsetting to preserve geometry
        muni_filtered <- muni_shp_all[
          muni_shp_all$code_muni %in% dev_muni_codes,
        ]
        # Ensure it's still a valid sf object
        sf::st_as_sf(muni_filtered)
      } else {
        muni_shp_all
      }
    }
  ),
  ## import municipal demographic data
  tar_target(
    name = muni_demo_file,
    command = "./data/atlas_brasil_census_data.csv.gz",
    format = "file"
  ),
  tar_target(
    name = muni_demo_all,
    command = fread(muni_demo_file)
  ),
  tar_target(
    name = muni_demo,
    command = {
      if (pipeline_config$dev_mode) {
        # Filter demographic data to dev municipalities
        dev_muni_codes <- muni_ids$id_munic_7
        muni_demo_all[Codmun7 %in% dev_muni_codes]
      } else {
        muni_demo_all
      }
    }
  ),
  #  calculate geographic features of municipalities
  tar_target(
    name = tract_centroids,
    command = make_tract_centroids(tract_shp)
  ),
  tar_target(
    name = muni_area,
    command = calc_muni_area(muni_shp)
  ),

  ## import and clean CNEFE data
  # Get list of states to process for CNEFE10
  tar_target(
    name = cnefe10_states,
    command = {
      if (pipeline_config$dev_mode) {
        pipeline_config$dev_states
      } else {
        # Get all states from the cnefe_2010 directory
        state_files <- list.files(
          "data/cnefe_2010",
          pattern = "cnefe_2010_.*\\.csv\\.gz$"
        )
        gsub("cnefe_2010_(.+)\\.csv\\.gz", "\\1", state_files)
      }
    }
  ),
  # Process CNEFE10 state by state to avoid memory issues
  tar_target(
    name = cnefe10_cleaned_by_state,
    command = {
      # Read state file
      state_file <- file.path(
        "data/cnefe_2010",
        paste0("cnefe_2010_", cnefe10_states, ".csv.gz")
      )

      # Read state data with appropriate separator
      state_data <- fread(
        state_file,
        sep = ",", # Partitioned files use comma
        encoding = "UTF-8",
        verbose = FALSE,
        showProgress = FALSE
      )

      # Get municipality IDs for this state
      state_muni_ids <- muni_ids[estado_abrev == cnefe10_states]

      # Get tract centroids for this state
      state_codes <- unique(substr(
        as.character(state_muni_ids$id_munic_7),
        1,
        2
      ))
      state_tract_centroids <- tract_centroids[
        substr(setor_code, 1, 2) %in% state_codes
      ]

      # Clean the state data and extract schools
      result <- clean_cnefe10(
        cnefe_file = state_data,
        muni_ids = state_muni_ids,
        tract_centroids = state_tract_centroids,
        extract_schools = TRUE
      )

      # Force garbage collection after processing each state
      gc(verbose = FALSE)

      # Return the cleaned data (schools are now in result$schools)
      result$data
    },
    pattern = map(cnefe10_states),
    format = "qs",
    iteration = "list", # Keep branches as list for rbindlist
    resources = tar_resources(
      crew = tar_resources_crew(controller = "memory_limited")
    )
  ),
  # Extract schools by state for CNEFE10
  tar_target(
    name = schools_cnefe10_by_state,
    command = {
      # Read state file
      state_file <- file.path(
        "data/cnefe_2010",
        paste0("cnefe_2010_", cnefe10_states, ".csv.gz")
      )

      # Read state data with appropriate separator
      state_data <- fread(
        state_file,
        sep = ",", # Partitioned files use comma
        encoding = "UTF-8",
        verbose = FALSE,
        showProgress = FALSE
      )

      # Get municipality IDs for this state
      state_muni_ids <- muni_ids[estado_abrev == cnefe10_states]

      # Get tract centroids for this state
      state_codes <- unique(substr(
        as.character(state_muni_ids$id_munic_7),
        1,
        2
      ))
      state_tract_centroids <- tract_centroids[
        substr(setor_code, 1, 2) %in% state_codes
      ]

      # Clean the state data and extract schools
      result <- clean_cnefe10(
        cnefe_file = state_data,
        muni_ids = state_muni_ids,
        tract_centroids = state_tract_centroids,
        extract_schools = TRUE
      )

      # Force garbage collection after processing each state
      gc(verbose = FALSE)

      # Return only the schools
      result$schools
    },
    pattern = map(cnefe10_states),
    format = "qs",
    iteration = "list", # Keep branches as list for rbindlist
    resources = tar_resources(
      crew = tar_resources_crew(controller = "memory_limited")
    )
  ),
  # Combine all cleaned state data for CNEFE10
  tar_target(
    name = cnefe10,
    command = {
      # Combine all state data.tables
      combined <- rbindlist(
        cnefe10_cleaned_by_state,
        use.names = TRUE,
        fill = TRUE
      )

      # Force garbage collection
      gc(verbose = FALSE)

      # Return combined dataset
      combined
    },
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  # Get list of states to process based on mode
  tar_target(
    name = cnefe22_states,
    command = {
      if (pipeline_config$dev_mode) {
        pipeline_config$dev_states
      } else {
        # Get all states from the cnefe_2022 directory
        state_files <- list.files(
          "data/cnefe_2022",
          pattern = "cnefe_2022_.*\\.csv\\.gz$"
        )
        gsub("cnefe_2022_(.+)\\.csv\\.gz", "\\1", state_files)
      }
    }
  ),
  tar_target(
    name = validate_cnefe10_clean,
    command = {
      # Memory-efficient validation: sample the data instead of loading all
      # Get total row count without loading full dataset
      n_rows <- nrow(cnefe10)

      # Sample a subset for validation (max 100k rows)
      sample_size <- min(100000, n_rows)
      sample_indices <- sample.int(n_rows, sample_size)

      # Load only the sample
      cnefe10_sample <- cnefe10[sample_indices, ]

      # Force garbage collection after sampling
      gc(verbose = FALSE)

      # Validate the sample
      result <- validate_import_stage(
        data = cnefe10_sample,
        stage_name = "cnefe10_cleaned",
        expected_cols = c(
          "id_munic_7",
          "cnefe_lat",
          "cnefe_long",
          "norm_address",
          "norm_street",
          "norm_bairro"
        ),
        min_rows = ifelse(pipeline_config$dev_mode, 10000, 100000) # Adjusted for sample
      )

      # Add actual row count to metadata
      result$metadata$total_rows <- n_rows
      result$metadata$sample_size <- sample_size

      if (!result$passed) {
        stop("CNEFE 2010 cleaning validation failed - pipeline halted")
      }
      result
    },
    resources = tar_resources(
      crew = tar_resources_crew(controller = "memory_limited")
    )
  ),
  # Process CNEFE22 state by state to avoid memory issues
  tar_target(
    name = cnefe22_cleaned_by_state,
    command = {
      # Read state file
      state_file <- file.path(
        "data/cnefe_2022",
        paste0("cnefe_2022_", cnefe22_states, ".csv.gz")
      )

      # Get municipality IDs for this state
      state_muni_ids <- muni_ids[estado_abrev == cnefe22_states]

      # Clean the state data
      result <- clean_cnefe22(
        cnefe22_file = state_file,
        muni_ids = state_muni_ids
      )

      # Force garbage collection after processing each state
      gc(verbose = FALSE)

      result
    },
    pattern = map(cnefe22_states),
    format = "qs",
    iteration = "list", # Keep branches as list for rbindlist
    resources = tar_resources(
      crew = tar_resources_crew(controller = "memory_limited")
    )
  ),
  # Combine all cleaned state data
  tar_target(
    name = cnefe22,
    command = {
      # Combine all state data.tables
      combined <- rbindlist(
        cnefe22_cleaned_by_state,
        use.names = TRUE,
        fill = TRUE
      )

      # Return combined dataset
      combined
    },
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  tar_target(
    name = validate_cnefe22_clean,
    command = {
      # Memory-efficient validation: sample the data instead of loading all
      # Get total row count without loading full dataset
      n_rows <- nrow(cnefe22)

      # Sample a subset for validation (max 100k rows)
      sample_size <- min(100000, n_rows)
      sample_indices <- sample.int(n_rows, sample_size)

      # Load only the sample
      cnefe22_sample <- cnefe22[sample_indices, ]

      # Force garbage collection after sampling
      gc(verbose = FALSE)

      # Validate the sample
      result <- validate_import_stage(
        data = cnefe22_sample,
        stage_name = "cnefe22_cleaned",
        expected_cols = c(
          "id_munic_7",
          "cnefe_lat",
          "cnefe_long",
          "norm_address",
          "norm_street",
          "norm_bairro"
        ),
        min_rows = ifelse(pipeline_config$dev_mode, 10000, 100000) # Adjusted for sample
      )

      # Add actual row count to metadata
      result$metadata$total_rows <- n_rows
      result$metadata$sample_size <- sample_size

      if (!result$passed) {
        stop("CNEFE 2022 cleaning validation failed - pipeline halted")
      }
      result
    },
    resources = tar_resources(
      crew = tar_resources_crew(controller = "memory_limited")
    )
  ),

  ## Combine state-level school extracts for 2010 CNEFE
  tar_target(
    name = schools_cnefe10,
    command = {
      # Combine all state school extracts
      schools <- rbindlist(
        schools_cnefe10_by_state,
        use.names = TRUE,
        fill = TRUE
      )

      # Force garbage collection
      gc(verbose = FALSE)

      # Return combined schools dataset
      schools
    },
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  ## Create a dataset of streets in 2010 CNEFE
  tar_target(
    name = cnefe10_st,
    command = cnefe10[,
      .(
        long = median(cnefe_long, na.rm = TRUE),
        lat = median(cnefe_lat, na.rm = TRUE),
        n = .N
      ),
      by = .(id_munic_7, norm_street)
    ][n > 1],
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  ## Create a dataset of neighborhoods in 2010 CNEFE
  tar_target(
    name = cnefe10_bairro,
    command = cnefe10[,
      .(
        long = median(cnefe_long, na.rm = TRUE),
        lat = median(cnefe_lat, na.rm = TRUE),
        n = .N
      ),
      by = .(id_munic_7, norm_bairro)
    ][n > 1],
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  ## Import and clean 2017 CNEFE
  tar_target(
    name = agro_cnefe_files,
    command = {
      if (pipeline_config$dev_mode) {
        # Map state abbreviations to agro censo file names
        state_file_map <- c(
          "AC" = "12_ACRE.csv.gz",
          "RR" = "14_RORAIMA.csv.gz",
          "AP" = "16_AMAPA.csv.gz",
          "RO" = "11_RONDONIA.csv.gz"
        )
        dev_files <- state_file_map[pipeline_config$dev_states]
        file.path("data/agro_censo", dev_files[!is.na(dev_files)])
      } else {
        dir("data/agro_censo/", full.names = TRUE)
      }
    }
  ),
  tar_target(
    name = agro_cnefe,
    command = clean_agro_cnefe(
      agro_cnefe_files = agro_cnefe_files,
      muni_ids = muni_ids
    ),
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  ## Create a dataset of streets in 2017 CNEFE
  tar_target(
    name = agrocnefe_st,
    command = agro_cnefe[,
      .(
        long = median(longitude, na.rm = TRUE),
        lat = median(latitude, na.rm = TRUE),
        n = .N
      ),
      by = .(id_munic_7, norm_street)
    ][n > 1],
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  ## Create a dataset of neighborhoods in 2017 CNEFE
  tar_target(
    name = agrocnefe_bairro,
    command = agro_cnefe[,
      .(
        long = median(longitude, na.rm = TRUE),
        lat = median(latitude, na.rm = TRUE),
        n = .N
      ),
      by = .(id_munic_7, norm_bairro)
    ][n > 1],
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  tar_target(
    ## Extract schools frome 2022 CNEFE
    name = schools_cnefe22,
    command = get_cnefe22_schools(cnefe22),
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  ## Create a dataset of streets in 2022 CNEFE
  tar_target(
    name = cnefe22_st,
    command = cnefe22[,
      .(
        long = median(cnefe_long, na.rm = TRUE),
        lat = median(cnefe_lat, na.rm = TRUE),
        n = .N
      ),
      by = .(id_munic_7, norm_street)
    ][n > 1],
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  ## Create a dataset of neighborhoods in 2022 CNEFE
  tar_target(
    name = cnefe22_bairro,
    command = cnefe22[,
      .(
        long = median(cnefe_long, na.rm = TRUE),
        lat = median(cnefe_lat, na.rm = TRUE),
        n = .N
      ),
      by = .(id_munic_7, norm_bairro)
    ][n > 1],
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  ## Import and clean INEP data
  tar_target(
    name = inep_file,
    command = "./data/inep_catalogo_das_escolas.csv.gz",
    format = "file"
  ),
  tar_target(
    name = inep_data_all,
    command = clean_inep(
      inep_data = fread(inep_file),
      inep_codes = inep_codes
    )
  ),
  tar_target(
    name = inep_data,
    command = {
      if (pipeline_config$dev_mode) {
        # Get municipality codes for dev states
        dev_muni_codes <- get_muni_codes_for_states(
          muni_ids,
          pipeline_config$dev_states
        )
        inep_data_all[id_munic_7 %in% dev_muni_codes]
      } else {
        inep_data_all
      }
    }
  ),
  tar_target(
    name = validate_inep_clean,
    command = {
      result <- validate_cleaning_stage(
        cleaned_data = inep_data,
        original_data = inep_codes,
        stage_name = "inep_cleaned",
        key_cols = c("codigo_inep", "id_munic_7", "latitude", "longitude")
      )
      if (!result$passed) {
        warning("INEP data cleaning validation failed")
      }
      result
    }
  ),
  ## Import Locais de Votação Data
  tar_target(
    name = locais_file,
    command = "./data/polling_stations_2006_2024.csv.gz",
    format = "file"
  ),
  tar_target(
    name = locais_all,
    command = import_locais(
      locais_file = locais_file,
      muni_ids = muni_ids
    ),
    storage = "worker",
    retrieval = "worker"
  ),
  tar_target(
    name = locais,
    command = {
      if (pipeline_config$dev_mode) {
        filter_data_by_state(locais_all, pipeline_config$dev_states, "sg_uf")
      } else {
        locais_all
      }
    }
  ),
  # Filter Brasília from municipal election years
  tar_target(
    name = locais_filtered,
    command = {
      # Apply Brasília filtering
      dt_filtered <- filter_brasilia_municipal_elections(locais)
      
      # Generate filtering report
      report <- generate_brasilia_filtering_report(
        dt_before = locais,
        dt_after = dt_filtered,
        output_file = "output/brasilia_filtering_report.rds"
      )
      
      # Validate the filtering
      validation <- validate_brasilia_filtering(dt_filtered, stop_on_failure = FALSE)
      if (!validation$passed) {
        warning("Brasília filtering validation failed - check the filtering logic")
      }
      
      # Return filtered data
      dt_filtered
    }
  ),
  tar_target(
    name = validate_locais,
    command = {
      result <- validate_import_stage(
        data = locais_filtered,
        stage_name = "locais",
        expected_cols = c(
          "local_id",
          "ano",
          "nr_zona",
          "nr_locvot",
          "nm_locvot",
          "nm_localidade",
          "sg_uf",
          "cod_localidade_ibge",
          "ds_endereco"
        ),
        min_rows = ifelse(pipeline_config$dev_mode, 1000, 100000)
      )
      if (!result$passed) {
        warning("Polling stations validation failed - check data quality")
      }
      result
    }
  ),
  tar_target(
    ## Import geocoded polling stations from TSE for ground truth
    name = tse_files,
    command = c(
      "./data/eleitorado_local_votacao_2018.csv.gz",
      "./data/eleitorado_local_votacao_2020.csv.gz",
      "./data/eleitorado_local_votacao_2022.csv.gz"
    ),
    format = "file"
  ),
  tar_target(
    name = tsegeocoded_locais,
    command = clean_tsegeocoded_locais(
      tse_files = tse_files,
      muni_ids = muni_ids,
      locais = locais_filtered
    )
  ),
  ## Create panel ids to track polling stations across time
  ## Define all states for panel ID processing
  tar_target(
    panel_states_all,
    command = {
      if (pipeline_config$dev_mode) {
        # In dev mode, only process dev states
        pipeline_config$dev_states
      } else {
        # In production, process all states
        c(
          "AC",
          "AL",
          "AM",
          "AP",
          "BA",
          "CE",
          "DF",
          "ES",
          "GO",
          "MA",
          "MG",
          "MS",
          "MT",
          "PA",
          "PB",
          "PE",
          "PI",
          "PR",
          "RJ",
          "RN",
          "RO",
          "RR",
          "RS",
          "SC",
          "SE",
          "SP",
          "TO"
        )
      }
    }
  ),

  ## Process panel IDs by state using dynamic branching
  tar_target(
    name = panel_ids_by_state,
    command = {
      # Handle DF differently due to different years
      if (panel_states_all == "DF") {
        process_panel_ids_single_state(
          locais_full = locais_filtered,
          state_code = panel_states_all,
          years = c(2006, 2008, 2010, 2012, 2014, 2018, 2022, 2024),
          blocking_column = "cod_localidade_ibge",
          scoring_columns = c("normalized_name", "normalized_addr")
        )
      } else {
        process_panel_ids_single_state(
          locais_full = locais_filtered,
          state_code = panel_states_all,
          years = c(2006, 2008, 2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024),
          blocking_column = "cod_localidade_ibge",
          scoring_columns = c("normalized_name", "normalized_addr")
        )
      }
    },
    pattern = map(panel_states_all),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "worker",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "standard")
    )
  ),

  ## Combine panel IDs from all states
  tar_target(
    name = panel_ids_combined,
    command = combine_state_panel_ids(panel_ids_by_state),
    deployment = "main",
    storage = "worker",
    retrieval = "worker"
  ),

  ## Final panel IDs with coordinates
  tar_target(
    name = panel_ids,
    command = {
      # The combined panel IDs are already properly formatted
      # Just need to add coordinates using the existing function
      # Pass empty data.table for df_panels since all states are now combined
      make_panel_ids(data.table(), panel_ids_combined, tsegeocoded_locais)
    }
  ),

  # String Matching
  # Create target for municipalities to iterate over
  tar_target(
    name = municipalities_for_matching,
    command = unique(locais_filtered$cod_localidade_ibge)
  ),

  # Create municipality batch assignments for flattened parallel processing
  tar_target(
    name = municipality_batch_assignments,
    command = create_municipality_batch_assignments(
      municipalities_for_matching,
      batch_size = ifelse(pipeline_config$dev_mode, 10, 50)
    )
  ),

  # Extract unique batch IDs for dynamic branching
  tar_target(
    name = batch_ids,
    command = unique(municipality_batch_assignments$batch_id)
  ),

  # INEP string matching - process municipalities in batches
  tar_target(
    name = inep_string_match_batch,
    command = {
      # Get municipalities for this batch
      batch_munis <- municipality_batch_assignments[
        batch_id == batch_ids
      ]$muni_code

      # Process all municipalities in this batch
      batch_results <- lapply(batch_munis, function(muni_code) {
        match_inep_muni(
          locais_muni = locais_filtered[cod_localidade_ibge == muni_code],
          inep_muni = inep_data[id_munic_7 == muni_code]
        )
      })

      # Remove NULL results and combine
      batch_results <- batch_results[!sapply(batch_results, is.null)]
      if (length(batch_results) > 0) {
        rbindlist(batch_results, use.names = TRUE, fill = TRUE)
      } else {
        data.table()
      }
    },
    pattern = map(batch_ids),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "worker",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "standard")
    )
  ),
  tar_target(
    name = inep_string_match,
    command = rbindlist(inep_string_match_batch, use.names = TRUE, fill = TRUE),
    deployment = "main"
  ),
  tar_target(
    name = validate_inep_match,
    command = {
      result <- validate_string_match_stage(
        match_data = inep_string_match,
        stage_name = "inep_string_match",
        id_col = "local_id",
        score_col = "dist"
      )

      message(sprintf(
        "INEP string matching: %.1f%% match rate (%d matched out of %d)",
        result$metadata$match_rate,
        sum(!is.na(inep_string_match$lat), na.rm = TRUE),
        nrow(inep_string_match)
      ))

      if (!result$passed) {
        warning("INEP string match validation has issues")
      }
      result
    }
  ),
  # Schools CNEFE 2010 matching with batched dynamic branching
  tar_target(
    name = schools_cnefe10_match_batch,
    command = {
      # Get municipalities for this batch
      batch_munis <- municipality_batch_assignments[
        batch_id == batch_ids
      ]$muni_code

      # Process all municipalities in this batch
      batch_results <- lapply(batch_munis, function(muni_code) {
        match_schools_cnefe_muni(
          locais_muni = locais_filtered[cod_localidade_ibge == muni_code],
          schools_cnefe_muni = schools_cnefe10[id_munic_7 == muni_code]
        )
      })

      # Remove NULL results and combine
      batch_results <- batch_results[!sapply(batch_results, is.null)]
      if (length(batch_results) > 0) {
        rbindlist(batch_results, use.names = TRUE, fill = TRUE)
      } else {
        data.table()
      }
    },
    pattern = map(batch_ids),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "worker",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "standard")
    )
  ),
  tar_target(
    name = schools_cnefe10_match,
    command = rbindlist(schools_cnefe10_match_batch),
    storage = "worker",
    retrieval = "worker"
  ),
  # Schools CNEFE 2022 matching - process municipalities in batches
  tar_target(
    name = schools_cnefe22_match_batch,
    command = {
      # Get municipalities for this batch
      batch_munis <- municipality_batch_assignments[
        batch_id == batch_ids
      ]$muni_code

      # Process all municipalities in this batch
      batch_results <- lapply(batch_munis, function(muni_code) {
        match_schools_cnefe_muni(
          locais_muni = locais_filtered[cod_localidade_ibge == muni_code],
          schools_cnefe_muni = schools_cnefe22[id_munic_7 == muni_code]
        )
      })

      # Remove NULL results and combine
      batch_results <- batch_results[!sapply(batch_results, is.null)]
      if (length(batch_results) > 0) {
        rbindlist(batch_results, use.names = TRUE, fill = TRUE)
      } else {
        data.table()
      }
    },
    pattern = map(batch_ids),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "worker",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "standard")
    )
  ),
  tar_target(
    name = schools_cnefe22_match,
    command = rbindlist(
      schools_cnefe22_match_batch,
      use.names = TRUE,
      fill = TRUE
    ),
    deployment = "main"
  ),
  # CNEFE 2010 street/neighborhood matching with batched dynamic branching
  tar_target(
    name = cnefe10_stbairro_match_batch,
    command = {
      # Get municipalities for this batch
      batch_munis <- municipality_batch_assignments[
        batch_id == batch_ids
      ]$muni_code

      # Process all municipalities in this batch
      batch_results <- lapply(batch_munis, function(muni_code) {
        match_stbairro_cnefe_muni(
          locais_muni = locais_filtered[cod_localidade_ibge == muni_code],
          cnefe_st_muni = cnefe10_st[id_munic_7 == muni_code],
          cnefe_bairro_muni = cnefe10_bairro[id_munic_7 == muni_code]
        )
      })

      # Remove NULL results and combine
      batch_results <- batch_results[!sapply(batch_results, is.null)]
      if (length(batch_results) > 0) {
        rbindlist(batch_results, use.names = TRUE, fill = TRUE)
      } else {
        data.table()
      }
    },
    pattern = map(batch_ids),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "worker",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "standard")
    )
  ),
  tar_target(
    name = cnefe10_stbairro_match,
    command = rbindlist(cnefe10_stbairro_match_batch),
    storage = "worker",
    retrieval = "worker"
  ),
  # CNEFE 2022 street/neighborhood matching with batched dynamic branching
  tar_target(
    name = cnefe22_stbairro_match_batch,
    command = {
      # Get municipalities for this batch
      batch_munis <- municipality_batch_assignments[
        batch_id == batch_ids
      ]$muni_code

      # Process all municipalities in this batch
      batch_results <- lapply(batch_munis, function(muni_code) {
        match_stbairro_cnefe_muni(
          locais_muni = locais_filtered[cod_localidade_ibge == muni_code],
          cnefe_st_muni = cnefe22_st[id_munic_7 == muni_code],
          cnefe_bairro_muni = cnefe22_bairro[id_munic_7 == muni_code]
        )
      })

      # Remove NULL results and combine
      batch_results <- batch_results[!sapply(batch_results, is.null)]
      if (length(batch_results) > 0) {
        rbindlist(batch_results, use.names = TRUE, fill = TRUE)
      } else {
        data.table()
      }
    },
    pattern = map(batch_ids),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "worker"
  ),
  tar_target(
    name = cnefe22_stbairro_match,
    command = rbindlist(cnefe22_stbairro_match_batch),
    storage = "worker",
    retrieval = "worker"
  ),
  # Agro CNEFE street/neighborhood matching with batched dynamic branching
  tar_target(
    name = agrocnefe_stbairro_match_batch,
    command = {
      # Get municipalities for this batch
      batch_munis <- municipality_batch_assignments[
        batch_id == batch_ids
      ]$muni_code

      # Process all municipalities in this batch
      batch_results <- lapply(batch_munis, function(muni_code) {
        match_stbairro_agrocnefe_muni(
          locais_muni = locais_filtered[cod_localidade_ibge == muni_code],
          agrocnefe_st_muni = agrocnefe_st[id_munic_7 == muni_code],
          agrocnefe_bairro_muni = agrocnefe_bairro[id_munic_7 == muni_code]
        )
      })

      # Remove NULL results and combine
      batch_results <- batch_results[!sapply(batch_results, is.null)]
      if (length(batch_results) > 0) {
        rbindlist(batch_results, use.names = TRUE, fill = TRUE)
      } else {
        data.table()
      }
    },
    pattern = map(batch_ids),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "worker",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "standard")
    )
  ),
  tar_target(
    name = agrocnefe_stbairro_match,
    command = rbindlist(agrocnefe_stbairro_match_batch),
    storage = "worker",
    retrieval = "worker"
  ),
  # geocodebr matching with dynamic branching by batch
  tar_target(
    name = geocodebr_match_batch,
    command = {
      # Get municipalities for this batch
      batch_munis <- municipality_batch_assignments[
        batch_id == batch_ids
      ]$muni_code

      # Process all municipalities in this batch
      batch_results <- lapply(batch_munis, function(muni_code) {
        match_geocodebr_muni(
          locais_muni = locais_filtered[cod_localidade_ibge == muni_code],
          muni_ids = muni_ids[id_munic_7 == muni_code]
        )
      })

      # Remove NULL results and combine
      batch_results <- batch_results[!sapply(batch_results, is.null)]
      if (length(batch_results) > 0) {
        rbindlist(batch_results, use.names = TRUE, fill = TRUE)
      } else {
        data.table()
      }
    },
    pattern = map(batch_ids),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "worker",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "standard")
    )
  ),
  tar_target(
    name = geocodebr_match,
    command = rbindlist(geocodebr_match_batch, fill = TRUE, use.names = TRUE),
    storage = "worker",
    retrieval = "worker"
  ),
  tar_target(
    name = validate_geocodebr_match,
    command = {
      result <- validate_string_match_stage(
        match_data = geocodebr_match,
        stage_name = "geocodebr_match",
        id_col = "local_id",
        score_col = "mindist_geocodebr"
      )

      # Report precision breakdown
      if (nrow(geocodebr_match) > 0) {
        precision_summary <- geocodebr_match[, .N, by = precisao_geocodebr]
        message("geocodebr precision breakdown:")
        print(precision_summary)

        # Calculate street-level precision percentage
        total_matches <- nrow(geocodebr_match)
        street_level <- precision_summary[precisao_geocodebr == "logradouro", N]
        if (length(street_level) > 0) {
          pct_street <- 100 * street_level / total_matches
          message(sprintf("Street-level precision: %.1f%%", pct_street))
        }
      }

      result
    }
  ),
  ## Combine string matching data for modeling
  tar_target(
    name = model_data,
    command = make_model_data(
      cnefe10_stbairro_match = cnefe10_stbairro_match,
      cnefe22_stbairro_match = cnefe22_stbairro_match,
      schools_cnefe10_match = schools_cnefe10_match,
      schools_cnefe22_match = schools_cnefe22_match,
      agrocnefe_stbairro_match = agrocnefe_stbairro_match,
      inep_string_match = inep_string_match,
      geocodebr_match = geocodebr_match,
      muni_demo = muni_demo,
      muni_area = muni_area,
      locais = locais_filtered,
      tsegeocoded_locais = tsegeocoded_locais
    ),
    storage = "worker",
    retrieval = "worker"
  ),
  tar_target(
    name = validate_model_data,
    command = {
      result <- validate_merge_stage(
        merged_data = model_data,
        left_data = locais_filtered,
        right_data = NULL, # Multiple sources merged
        stage_name = "model_data_merge",
        merge_keys = "local_id",
        join_type = "left_many" # One-to-many join expected for fuzzy matching
      )

      if (!result$passed) {
        warning("Model data merge validation failed")
      }
      result
    }
  ),
  ## Train model and make predictions
  tar_target(
    name = trained_model,
    command = train_model(model_data, grid_n = 50),
  ),
  tar_target(
    name = model_predictions,
    command = get_predictions(trained_model, model_data),
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  tar_target(
    name = validate_predictions,
    command = {
      result <- validate_prediction_stage(
        predictions = model_predictions,
        stage_name = "model_predictions",
        pred_col = "pred_dist", # Distance prediction column
        prob_col = NULL # No probability column
      )
      if (!result$passed) {
        stop("Model predictions validation failed")
      }
      result
    }
  ),

  # # Use string matches to geocode
  tar_target(
    name = geocoded_locais,
    command = finalize_coords(locais, model_predictions, tsegeocoded_locais),
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  tar_target(
    name = validate_geocoded_output,
    command = {
      result <- validate_output_stage(
        output_data = geocoded_locais,
        stage_name = "geocoded_locais",
        required_cols = c(
          "local_id",
          "final_lat",
          "final_long",
          "ano",
          "nr_zona",
          "nr_locvot",
          "nm_locvot",
          "nm_localidade"
        ),
        unique_keys = c("local_id", "ano", "nr_zona", "nr_locvot")
      )

      # Final quality check
      if (!result$passed) {
        stop("Final output validation failed - do not export!")
      }

      message(sprintf(
        "Geocoding complete: %d polling stations geocoded",
        nrow(geocoded_locais)
      ))

      result
    }
  ),

  ## Generate comprehensive validation report as pipeline output
  tar_target(
    name = validation_report,
    command = {
      # Collect all validation results
      validation_results <- list(
        muni_ids = validate_muni_ids,
        inep_codes = validate_inep_codes,
        cnefe10_cleaned = validate_cnefe10_clean,
        cnefe22_cleaned = validate_cnefe22_clean,
        inep_cleaned = validate_inep_clean,
        locais = validate_locais,
        inep_string_match = validate_inep_match,
        model_data_merge = validate_model_data,
        model_predictions = validate_predictions,
        geocoded_output = validate_geocoded_output
      )

      # Store original data references for failed record export
      # ONLY for smaller datasets to avoid memory exhaustion
      for (name in names(validation_results)) {
        # Skip CNEFE datasets which are too large to load into memory
        if (name %in% c("cnefe10_cleaned", "cnefe22_cleaned")) {
          validation_results[[name]]$metadata$data <- NULL
          validation_results[[name]]$metadata$skip_export <- TRUE
          next
        }

        validation_results[[name]]$metadata$data <- switch(
          name,
          muni_ids = muni_ids,
          inep_codes = inep_codes,
          inep_cleaned = inep_data,
          locais = locais_filtered,
          inep_string_match = inep_string_match,
          model_data_merge = model_data,
          model_predictions = model_predictions,
          geocoded_output = geocoded_locais
        )
      }

      # Add Brasília filtering report path
      validation_results$brasilia_filtering <- list(
        passed = TRUE,
        metadata = list(
          type = "preprocessing",
          stage = "brasilia_filtering",
          n_rows = nrow(locais_filtered),
          report_path = "output/brasilia_filtering_report.rds",
          note = "Brasília records filtered from municipal election years"
        )
      )
      
      # Generate text validation report
      report_path <- generate_text_validation_report(
        validation_results = validation_results,
        output_dir = "output/validation_reports",
        export_failures = TRUE
      )

      # Save validation summary for future reference
      summary_stats <- list(
        report_path = report_path,
        timestamp = Sys.time(),
        pipeline_version = "1.0.0",
        total_validations = length(validation_results),
        passed = sum(sapply(validation_results, function(x) x$passed)),
        failed = sum(sapply(validation_results, function(x) !x$passed)),
        dev_mode = pipeline_config$dev_mode,
        stage_summary = sapply(validation_results, function(x) {
          list(
            passed = x$passed,
            type = x$metadata$type,
            n_rows = x$metadata$n_rows %||% NA
          )
        })
      )

      # Save summary
      saveRDS(summary_stats, "output/validation_summary.rds")

      # Print summary to console
      cat("\n========== VALIDATION REPORT SUMMARY ==========\n")
      cat("Report generated:", report_path, "\n")
      cat("Total stages validated:", summary_stats$total_validations, "\n")
      cat("Passed:", summary_stats$passed, "\n")
      cat("Failed:", summary_stats$failed, "\n")
      cat(
        "Mode:",
        ifelse(summary_stats$dev_mode, "DEVELOPMENT", "PRODUCTION"),
        "\n"
      )
      cat(
        "Overall status:",
        ifelse(summary_stats$failed == 0, "✅ SUCCESS", "❌ FAILURES DETECTED"),
        "\n"
      )
      cat("===============================================\n\n")

      summary_stats
    }
  ),

  ## Export data (only after validation passes)
  tar_target(
    name = geocoded_export,
    command = {
      # Ensure validation report has been generated
      validation_report
      export_geocoded_locais(geocoded_locais)
    },
    format = "file"
  ),
  tar_target(
    name = panelid_export,
    command = {
      # Ensure validation report has been generated
      validation_report
      export_panel_ids(panel_ids)
    },
    format = "file"
  ),
  ## Data Quality Monitoring
  tar_target(
    name = data_quality_monitoring,
    command = {
      # Load monitoring functions
      source("R/data_quality_monitor_v2.R")
      
      # Run comprehensive monitoring with data passed as parameters
      results <- run_data_quality_monitoring_v2(
        geocoded_locais = geocoded_locais,
        panel_ids = panel_ids,
        generate_alerts = TRUE,
        config_file = "config/data_quality_config.yaml"
      )
      
      # Save latest results for tracking
      saveRDS(results, "output/latest_quality_results.rds")
      
      # Return results for downstream use
      results
    },
    # Always run monitoring to catch issues early
    cue = tar_cue(mode = "always")
  ),
  ## Sanity Check Report
  tar_target(
    name = sanity_check_report,
    command = {
      # Ensure quarto can be found
      if (Sys.getenv("QUARTO_PATH") == "") {
        quarto_bin <- Sys.which("quarto")
        if (!nzchar(quarto_bin)) {
          quarto_bin <- "/usr/local/bin/quarto"
        }
        Sys.setenv(QUARTO_PATH = quarto_bin)
      }
      
      # Render the report
      quarto::quarto_render(
        input = "reports/polling_station_sanity_check.qmd",
        execute_dir = getwd()
      )
      
      # Return the output files
      output_files <- "reports/polling_station_sanity_check.html"
      support_dir <- "reports/polling_station_sanity_check_files"
      if (dir.exists(support_dir)) {
        output_files <- c(output_files, support_dir)
      }
      output_files
    },
    format = "file",
    deployment = "main"  # Run on main to ensure data access
  ),
  ## Methodology and Evaluation
  # Only render in production mode
  if (!pipeline_config$dev_mode) {
    tar_render(
      name = geocode_writeup,
      path = "./doc/geocoding_procedure.Rmd",
      cue = tar_cue(mode = "thorough")
    )
  } else {
    # Skip in dev mode - return dummy target
    tar_target(
      name = geocode_writeup,
      command = {
        message("Skipping geocode_writeup in DEV_MODE")
        "./doc/geocoding_procedure.html" # Return expected output path
      }
    )
  }
)
