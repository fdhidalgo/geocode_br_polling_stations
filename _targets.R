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

# ===== CONFIGURATION =====
# Development mode flag - set to TRUE for faster iteration with subset of states
DEV_MODE <- FALSE # Process only AC, RR states when TRUE

# ===== SETUP =====
# Load packages required to define the pipeline:
library(targets)
library(tarchetypes)
library(data.table)
library(crew)

# ===== GLOBAL FUNCTION LOADING =====
# Load ALL custom functions - makes functions available to all workers
tar_source(files = "R")

# Set global options

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

# Set garbage collection options
options(
  # Garbage collection frequency
  gcinfo = FALSE
)

# Create controller group using configuration function
controller_group <- get_crew_controllers(dev_mode = DEV_MODE)

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

# Set target options using configuration function
configure_targets_options(controller_group)


# Get pipeline configuration based on development mode
pipeline_config <- get_pipeline_config(DEV_MODE)

# Enable memory-efficient string matching if configured
if (pipeline_config$use_memory_efficient) {
  options(geocode_br.use_memory_efficient = TRUE)
  message("Memory-efficient string matching is ENABLED")
}

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

# ===== TARGETS PIPELINE =====
list(
  # ========================================
  # DATA IMPORT TARGETS
  # ========================================

  ## Municipality and code identifiers
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
    command = filter_by_dev_mode(muni_ids_all, pipeline_config, state_col = "estado_abrev")
  ),
  tar_target(
    name = validate_muni_ids,
    command = validate_simple(
      data = muni_ids,
      stage_name = "muni_ids",
      expected_cols = c("id_munic_7", "id_munic_6", "id_TSE", "estado_abrev", "municipio"),
      min_rows_dev = 30, # AC+RR have ~37 municipalities
      min_rows_prod = 5000, # Brazil has ~5,570
      pipeline_config = pipeline_config,
      warning_message = "Municipality identifiers validation failed - check data quality"
    )
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
    command = validate_simple(
      data = inep_codes,
      stage_name = "inep_codes",
      expected_cols = c("codigo_inep", "id_munic_7"),
      min_rows_dev = 1000, # Expect many schools
      min_rows_prod = 100000,
      pipeline_config = pipeline_config,
      warning_message = "INEP codes validation failed - check data quality"
    )
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
    command = if (pipeline_config$dev_mode) {
      dev_state_codes <- substr(as.character(muni_ids$id_munic_7), 1, 2)
      tract_filtered <- tract_shp_all[
        substr(tract_shp_all$code_tract, 1, 2) %in% unique(dev_state_codes),
      ]
      sf::st_as_sf(tract_filtered)
    } else {
      tract_shp_all
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
    command = if (pipeline_config$dev_mode) {
      dev_muni_codes <- muni_ids$id_munic_7
      muni_filtered <- muni_shp_all[muni_shp_all$code_muni %in% dev_muni_codes, ]
      sf::st_as_sf(muni_filtered)
    } else {
      muni_shp_all
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
    command = if (pipeline_config$dev_mode) {
      muni_demo_all[Codmun7 %in% muni_ids$id_munic_7]
    } else {
      muni_demo_all
    }
  ),

  # ========================================
  # GEOGRAPHIC FEATURES
  # ========================================

  tar_target(
    name = tract_centroids,
    command = make_tract_centroids(tract_shp)
  ),
  tar_target(
    name = muni_area,
    command = calc_muni_area(muni_shp)
  ),

  # ========================================
  # CNEFE DATA PROCESSING
  # ========================================

  ## CNEFE 2010 Processing
  tar_target(
    name = cnefe10_states,
    command = get_states_for_processing("cnefe10", pipeline_config)
  ),
  # Process CNEFE10 state by state to avoid memory issues
  tar_target(
    name = cnefe10_cleaned_by_state,
    command = process_cnefe_state(
      state = cnefe10_states,
      year = 2010,
      muni_ids = muni_ids,
      tract_centroids = tract_centroids,
      extract_schools = FALSE
    ),
    pattern = map(cnefe10_states),
    format = "qs",
    iteration = "list",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "memory_limited")
    )
  ),
  # Extract schools by state for CNEFE10
  tar_target(
    name = schools_cnefe10_by_state,
    command = process_cnefe_state(
      state = cnefe10_states,
      year = 2010,
      muni_ids = muni_ids,
      tract_centroids = tract_centroids,
      extract_schools = TRUE
    ),
    pattern = map(cnefe10_states),
    format = "qs",
    iteration = "list",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "memory_limited")
    )
  ),
  # Combine all cleaned state data for CNEFE10
  tar_target(
    name = cnefe10,
    command = rbindlist(
      cnefe10_cleaned_by_state,
      use.names = TRUE,
      fill = TRUE
    ),
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  # Get list of states to process based on mode
  tar_target(
    name = cnefe22_states,
    command = get_states_for_processing("cnefe22", pipeline_config)
  ),
  # Process CNEFE22 state by state to avoid memory issues
  tar_target(
    name = cnefe22_cleaned_by_state,
    command = process_cnefe_state(
      state = cnefe22_states,
      year = 2022,
      muni_ids = muni_ids
    ),
    pattern = map(cnefe22_states),
    format = "qs",
    iteration = "list",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "memory_limited")
    )
  ),
  # Combine all cleaned state data
  tar_target(
    name = cnefe22,
    command = rbindlist(
      cnefe22_cleaned_by_state,
      use.names = TRUE,
      fill = TRUE
    ),
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),

  ## Combine state-level school extracts for 2010 CNEFE
  tar_target(
    name = schools_cnefe10,
    command = rbindlist(
      schools_cnefe10_by_state,
      use.names = TRUE,
      fill = TRUE
    ),
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
    command = get_agro_cnefe_files(pipeline_config)
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
  # INEP data filtered by development mode
  tar_target(
    name = inep_data,
    command = apply_dev_mode_filters(inep_data_all, pipeline_config, muni_ids, "municipality", muni_col = "id_munic_7")
  ),
  tar_target(
    name = validate_inep_clean,
    command = validate_cleaning(
      cleaned_data = inep_data,
      original_data = inep_codes,
      stage_name = "inep_cleaned",
      key_cols = c("codigo_inep", "id_munic_7", "latitude", "longitude"),
      warning_message = "INEP data cleaning validation failed"
    )
  ),

  # ========================================
  # POLLING STATION DATA
  # ========================================

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
  # Locais data filtered by development mode
  tar_target(
    name = locais,
    command = apply_dev_mode_filters(locais_all, pipeline_config, filter_type = "state", state_col = "sg_uf")
  ),
  # Filter Brasília from municipal election years - using helper function
  tar_target(
    name = locais_filtered,
    command = apply_brasilia_filters(locais)
  ),
  tar_target(
    name = validate_locais,
    command = validate_simple(
      data = locais_filtered,
      stage_name = "locais",
      expected_cols = c(
        "local_id", "ano", "nr_zona", "nr_locvot", "nm_locvot",
        "nm_localidade", "sg_uf", "cod_localidade_ibge", "ds_endereco"
      ),
      min_rows_dev = 1000,
      min_rows_prod = 100000,
      pipeline_config = pipeline_config,
      warning_message = "Polling stations validation failed - check data quality"
    )
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
  ## Create municipality batches for panel ID processing
  tar_target(
    name = panel_municipality_batches,
    command = create_panel_municipality_batches(
      locais_data = locais_filtered,
      target_batch_size = ifelse(pipeline_config$dev_mode, 2000, 5000)
    )
  ),

  ## Extract unique batch IDs for dynamic branching
  tar_target(
    name = panel_batch_ids,
    command = unique(panel_municipality_batches$batch_id)
  ),

  ## Monitor panel processing setup
  tar_target(
    name = panel_batch_summary,
    command = {
      monitor_panel_progress(panel_municipality_batches)
      panel_municipality_batches
    }
  ),

  ## Process panel IDs by municipality batch using dynamic branching
  tar_target(
    name = panel_ids_by_batch,
    command = {
      # Get municipalities for this batch
      batch_municipalities <- panel_municipality_batches[batch_id == panel_batch_ids]

      # Process panel IDs for this batch
      process_panel_ids_municipality_batch(
        locais_full = locais_filtered,
        municipality_batch = batch_municipalities,
        years = c(2006, 2008, 2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024),
        blocking_column = "cod_localidade_ibge",
        scoring_columns = c("normalized_name", "normalized_addr")
      )
    },
    pattern = map(panel_batch_ids),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "worker",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "standard")
    )
  ),

  ## Combine panel IDs from all batches
  tar_target(
    name = panel_ids_combined,
    command = combine_state_panel_ids(panel_ids_by_batch),
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

  # ========================================
  # STRING MATCHING TARGETS
  # ========================================
  ## Setup for parallel string matching
  tar_target(
    name = municipalities_for_matching,
    command = unique(locais_filtered$cod_localidade_ibge)
  ),

  # Calculate municipality sizes for smart batching
  tar_target(
    name = municipality_sizes,
    command = {
      # Count polling stations per municipality
      locais_filtered[, .(size = .N), by = .(muni_code = cod_localidade_ibge)]
    }
  ),

  # Create municipality batch assignments for flattened parallel processing
  tar_target(
    name = municipality_batch_assignments,
    command = create_municipality_batch_assignments(
      municipalities_for_matching,
      batch_size = ifelse(pipeline_config$dev_mode, 5, 15),
      muni_sizes = municipality_sizes
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
    command = process_inep_batch(
      batch_ids = batch_ids,
      municipality_batch_assignments = municipality_batch_assignments,
      locais_filtered = locais_filtered,
      inep_data = inep_data
    ),
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
    command = validate_string_match_with_stats(
      match_data = inep_string_match,
      stage_name = "inep_string_match",
      id_col = "local_id",
      score_col = "dist",
      lat_col = "lat",
      custom_message = "INEP string matching: %.1f%% match rate (%d matched out of %d)",
      warning_message = "INEP string match validation has issues"
    )
  ),
  # Schools CNEFE 2010 matching with batched dynamic branching
  tar_target(
    name = schools_cnefe10_match_batch,
    command = process_schools_cnefe_batch(
      batch_ids = batch_ids,
      municipality_batch_assignments = municipality_batch_assignments,
      locais_filtered = locais_filtered,
      schools_cnefe = schools_cnefe10
    ),
    pattern = map(batch_ids),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "worker",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "memory_limited")
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
    command = process_schools_cnefe_batch(
      batch_ids = batch_ids,
      municipality_batch_assignments = municipality_batch_assignments,
      locais_filtered = locais_filtered,
      schools_cnefe = schools_cnefe22
    ),
    pattern = map(batch_ids),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "worker",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "memory_limited")
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
    command = process_cnefe_stbairro_batch(
      batch_ids = batch_ids,
      municipality_batch_assignments = municipality_batch_assignments,
      locais_filtered = locais_filtered,
      cnefe_st = cnefe10_st,
      cnefe_bairro = cnefe10_bairro
    ),
    pattern = map(batch_ids),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "worker",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "memory_limited")
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
    command = process_cnefe_stbairro_batch(
      batch_ids = batch_ids,
      municipality_batch_assignments = municipality_batch_assignments,
      locais_filtered = locais_filtered,
      cnefe_st = cnefe22_st,
      cnefe_bairro = cnefe22_bairro
    ),
    pattern = map(batch_ids),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "worker",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "memory_limited")
    )
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
    command = process_agrocnefe_stbairro_batch(
      batch_ids = batch_ids,
      municipality_batch_assignments = municipality_batch_assignments,
      locais_filtered = locais_filtered,
      agrocnefe_st = agrocnefe_st,
      agrocnefe_bairro = agrocnefe_bairro
    ),
    pattern = map(batch_ids),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "worker",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "memory_limited")
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
    command = process_geocodebr_batch(
      batch_ids = batch_ids,
      municipality_batch_assignments = municipality_batch_assignments,
      locais_filtered = locais_filtered,
      muni_ids = muni_ids
    ),
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
    command = validate_geocodebr_precision(
      match_data = geocodebr_match,
      stage_name = "geocodebr_match",
      id_col = "local_id",
      score_col = "mindist_geocodebr",
      precision_col = "precisao_geocodebr"
    )
  ),

  # ========================================
  # MODEL TRAINING AND PREDICTION
  # ========================================

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
    command = validate_merge_simple(
      merged_data = model_data,
      left_data = locais_filtered,
      stage_name = "model_data_merge",
      merge_keys = "local_id",
      join_type = "left_many", # One-to-many join expected for fuzzy matching
      warning_message = "Model data merge validation failed"
    )
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
    command = validate_predictions_simple(
      predictions = model_predictions,
      stage_name = "model_predictions",
      pred_col = "pred_dist",
      stop_on_failure = TRUE
    )
  ),

  # ========================================
  # FINAL GEOCODING
  # ========================================

  tar_target(
    name = geocoded_locais,
    command = finalize_coords(locais, model_predictions, tsegeocoded_locais),
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  tar_target(
    name = validate_geocoded_output,
    command = validate_final_output(
      output_data = geocoded_locais,
      stage_name = "geocoded_locais",
      required_cols = c(
        "local_id", "final_lat", "final_long", "ano",
        "nr_zona", "nr_locvot", "nm_locvot", "nm_localidade"
      ),
      unique_keys = c("local_id", "ano", "nr_zona", "nr_locvot"),
      stop_on_failure = TRUE
    )
  ),

  # ========================================
  # VALIDATION AND REPORTING
  # ========================================
  ## Generate comprehensive validation report
  tar_target(
    name = validation_report,
    command = generate_validation_report_complete(
      validate_muni_ids = validate_muni_ids,
      validate_inep_codes = validate_inep_codes,
      validate_inep_clean = validate_inep_clean,
      validate_locais = validate_locais,
      validate_inep_match = validate_inep_match,
      validate_model_data = validate_model_data,
      validate_predictions = validate_predictions,
      validate_geocoded_output = validate_geocoded_output,
      muni_ids = muni_ids,
      inep_codes = inep_codes,
      inep_data = inep_data,
      locais_filtered = locais_filtered,
      inep_string_match = inep_string_match,
      model_data = model_data,
      model_predictions = model_predictions,
      geocoded_locais = geocoded_locais,
      pipeline_config = pipeline_config
    )
  ),

  # ========================================
  # DATA EXPORT
  # ========================================

  tar_target(
    name = geocoded_export,
    command = export_geocoded_with_validation(geocoded_locais, validation_report),
    format = "file"
  ),
  tar_target(
    name = panelid_export,
    command = export_panel_ids_with_validation(panel_ids, validation_report),
    format = "file"
  ),
  ## Data Quality Monitoring
  tar_target(
    name = data_quality_monitoring,
    command = {
      # Load monitoring functions
      source("R/data_quality_monitor_v2.R")

      # Create monitoring report with export files as dependencies
      results <- create_data_quality_monitor(
        geocoded_export = geocoded_export,
        panelid_export = panelid_export,
        geocoded_locais = geocoded_locais,
        panel_ids = panel_ids,
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
  # Sanity check report - using helper function
  create_validation_report_target(
    target_name = "sanity_check_report",
    input_file = "reports/polling_station_sanity_check.qmd",
    dependencies = c("geocoded_locais", "panel_ids"),
    report_type = "sanity_check"
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
      command = "./doc/geocoding_procedure.html"
    )
  }
)
