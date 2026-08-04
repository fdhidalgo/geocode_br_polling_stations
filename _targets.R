## Geocoding pipeline for Brazilian polling stations: import and clean CNEFE/INEP/TSE
## sources, fuzzy-match addresses, score matches with a boosted tree, build panel ids,
## and export the geocoded and panel datasets. Runs in dev (AC/RR) or production mode.

# --- Configuration ---
# TAR_PROJECT=dev selects the `dev` profile in _targets.yaml (AC/RR subset, local store).
# DEV_MODE is the single derived constant, so the S3 gate and data filtering can't disagree.
DEV_MODE <- identical(Sys.getenv("TAR_PROJECT"), "dev")

# --- Setup ---
# Load packages required to define the pipeline:
library(targets)
library(tarchetypes)
library(data.table)
library(crew)

# --- Global function loading ---
# Load ALL custom functions - makes functions available to all workers
tar_source(files = "R")

# Quarto is not on the PATH inside crew workers, so pin its location here.
if (Sys.getenv("QUARTO_PATH") == "") {
  quarto_bin <- Sys.which("quarto")
  if (nzchar(quarto_bin)) {
    Sys.setenv(QUARTO_PATH = as.character(quarto_bin))
    message("Setting QUARTO_PATH to: ", quarto_bin)
  }
}

## Setup parallel processing with crew/mirai

# Limit data.table threads to prevent memory contention
data.table::setDTthreads(1)

options(
  gcinfo = FALSE
)

controller_group <- get_crew_controllers()

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

# Production stores targets on S3; dev runs stay fully local.
if (!DEV_MODE) {
  # targets loads paws.storage by string when repository = "aws", so naming it here
  # keeps it visible to renv and fails loud on a machine that lacks it.
  if (!requireNamespace("paws.storage", quietly = TRUE)) {
    stop(
      "Production mode requires the 'paws.storage' package for S3 storage. ",
      "Install it (renv::install(\"paws.storage\")) or run in dev mode ",
      "(TAR_PROJECT=dev)."
    )
  }
  # Preserve the existing crew resources while adding the AWS ones.
  existing_resources <- tar_option_get("resources")
  tar_option_set(
    repository = "aws",
    repository_meta = "aws",
    resources = tar_resources(
      aws = tar_resources_aws(
        bucket = "geocode-br-polling-stations",
        prefix = "production"
      ),
      crew = existing_resources$crew # Preserve crew configuration
    )
  )
  message("AWS S3 storage configured for production mode")
} else {
  message("Using local storage (development mode)")
}

# --- Targets pipeline ---
list(
  # --- Configuration targets ---

  # Development mode flag - controls whether to process all states or just AC/RR.
  tar_target(
    name = dev_mode_flag,
    command = !!DEV_MODE
  ),

  # Pipeline configuration based on development mode
  tar_target(
    name = pipeline_config,
    command = {
      config <- get_pipeline_config(dev_mode_flag)
      # Add word blocking setting to config
      config$use_word_blocking <- TRUE # Set to TRUE to enable two-level blocking

      # Log configuration
      if (config$dev_mode) {
        message("Running in DEVELOPMENT MODE")
        message(
          "Processing states: ",
          paste(config$dev_states, collapse = ", ")
        )
      } else {
        message("Running in PRODUCTION MODE")
        message("Processing all Brazilian states")
      }
      message(
        "Two-level blocking for panel IDs is ",
        ifelse(config$use_word_blocking, "ENABLED", "DISABLED")
      )

      config
    }
  ),

  # --- Data import targets ---

  ## Municipality and code identifiers
  tar_target(
    name = muni_ids_file,
    command = "./data/muni_identifiers.csv",
    format = "file",
    repository = "local"
  ),
  tar_target(
    name = muni_ids_all,
    command = fread(muni_ids_file),
    format = "qs"
  ),
  tar_target(
    name = muni_ids,
    command = filter_by_dev_mode(
      muni_ids_all,
      pipeline_config$dev_states,
      id_column = "estado_abrev"
    ),
    format = "qs"
  ),
  tar_target(
    name = inep_codes_file,
    command = "./data/inep_codes.csv",
    format = "file",
    repository = "local"
  ),
  tar_target(
    name = inep_codes,
    command = fread(inep_codes_file)
  ),
  ## import shape files
  tar_target(
    name = tract_shp_file,
    command = "./data/census_tracts2010_shp.rds",
    format = "file",
    repository = "local"
  ),
  tar_target(
    name = tract_shp_all,
    command = sf::st_make_valid(readRDS(tract_shp_file))
  ),
  tar_target(
    name = tract_shp,
    command = {
      # Explicitly depend on pipeline_config
      if (pipeline_config$dev_mode) {
        dev_state_codes <- substr(as.character(muni_ids$id_munic_7), 1, 2)
        tract_filtered <- tract_shp_all[
          substr(tract_shp_all$code_tract, 1, 2) %in% unique(dev_state_codes),
        ]
        sf::st_as_sf(tract_filtered)
      } else {
        tract_shp_all
      }
    }
  ),
  tar_target(
    name = muni_shp_file,
    command = "./data/muni_shp.rds",
    format = "file",
    repository = "local"
  ),
  tar_target(
    name = muni_shp_all,
    command = sf::st_make_valid(readRDS(muni_shp_file))
  ),
  tar_target(
    name = muni_shp,
    command = {
      # Explicitly depend on pipeline_config
      if (pipeline_config$dev_mode) {
        dev_muni_codes <- muni_ids$id_munic_7
        muni_filtered <- muni_shp_all[
          muni_shp_all$code_muni %in% dev_muni_codes,
        ]
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
    format = "file",
    repository = "local"
  ),
  tar_target(
    name = muni_demo_all,
    command = fread(muni_demo_file)
  ),
  tar_target(
    name = muni_demo,
    command = {
      # Explicitly depend on pipeline_config
      if (pipeline_config$dev_mode) {
        muni_demo_all[Codmun7 %in% muni_ids$id_munic_7]
      } else {
        muni_demo_all
      }
    }
  ),

  # --- Geographic features ---

  tar_target(
    name = tract_centroids,
    command = make_tract_centroids(tract_shp)
  ),
  tar_target(
    name = muni_area,
    command = calc_muni_area(muni_shp)
  ),

  # --- CNEFE data processing ---

  ## CNEFE 2010: one tracked file per state, so a re-downloaded file rebuilds only
  ## its branch. Per-state cleaning stays in memory; only the small aggregates persist.
  tarchetypes::tar_files_input(
    cnefe10_files,
    get_cnefe_state_files(2010, DEV_MODE),
    format = "file",
    repository = "local"
  ),
  # Clean each state and aggregate in-memory (streets, neighborhoods, schools)
  tar_target(
    name = cnefe10_by_state,
    command = process_cnefe_state(
      state_file = cnefe10_files,
      year = 2010,
      muni_ids = muni_ids,
      tract_centroids = tract_centroids
    ),
    pattern = map(cnefe10_files),
    format = "qs",
    iteration = "list",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "memory_limited")
    )
  ),
  ## CNEFE 2022: one tracked file per state.
  tarchetypes::tar_files_input(
    cnefe22_files,
    get_cnefe_state_files(2022, DEV_MODE),
    format = "file",
    repository = "local"
  ),
  # Clean each state and aggregate in-memory (streets, neighborhoods, schools)
  tar_target(
    name = cnefe22_by_state,
    command = process_cnefe_state(
      state_file = cnefe22_files,
      year = 2022,
      muni_ids = muni_ids
    ),
    pattern = map(cnefe22_files),
    format = "qs",
    iteration = "list",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "memory_limited")
    )
  ),

  ## Combine per-state school extracts for 2010 CNEFE
  tar_target(
    name = schools_cnefe10,
    command = combine_cnefe_state_component(cnefe10_by_state, "schools"),
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  ## Create a dataset of streets in 2010 CNEFE (key uniqueness asserted at combine)
  tar_target(
    name = cnefe10_st,
    command = combine_cnefe_state_component(
      cnefe10_by_state,
      "st",
      unique_key = c("id_munic_7", "norm_street")
    ),
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  ## Create a dataset of neighborhoods in 2010 CNEFE (key uniqueness asserted at combine)
  tar_target(
    name = cnefe10_bairro,
    command = combine_cnefe_state_component(
      cnefe10_by_state,
      "bairro",
      unique_key = c("id_munic_7", "norm_bairro")
    ),
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  ## Import and clean 2017 agro CNEFE. clean_agro_cnefe is not mapped, so it receives
  ## the full vector of branch paths and rebuilds when any state file changes.
  tarchetypes::tar_files_input(
    agro_cnefe_files,
    get_agro_cnefe_files(DEV_MODE),
    format = "file",
    repository = "local"
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
  ## Combine per-state school extracts for 2022 CNEFE
  tar_target(
    name = schools_cnefe22,
    command = combine_cnefe_state_component(cnefe22_by_state, "schools"),
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  ## Create a dataset of streets in 2022 CNEFE (key uniqueness asserted at combine)
  tar_target(
    name = cnefe22_st,
    command = combine_cnefe_state_component(
      cnefe22_by_state,
      "st",
      unique_key = c("id_munic_7", "norm_street")
    ),
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  ## Create a dataset of neighborhoods in 2022 CNEFE (key uniqueness asserted at combine)
  tar_target(
    name = cnefe22_bairro,
    command = combine_cnefe_state_component(
      cnefe22_by_state,
      "bairro",
      unique_key = c("id_munic_7", "norm_bairro")
    ),
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  ## Import and clean INEP data
  tar_target(
    name = inep_file,
    command = "./data/inep_catalogo_das_escolas.csv.gz",
    format = "file",
    repository = "local"
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
    command = apply_dev_mode_filters(
      inep_data_all,
      pipeline_config,
      state_col = "uf"
    )
  ),

  # --- Polling station data ---

  tar_target(
    name = locais_file,
    command = "./data/polling_stations_2006_2024.csv.gz",
    format = "file",
    repository = "local"
  ),
  tar_target(
    name = locais_all,
    command = import_locais(
      locais_file = locais_file,
      # National import, so it needs the unfiltered crosswalk: dev-filtered muni_ids
      # would leave other states with cod_localidade_ibge = NA, colliding on local_id.
      muni_ids = muni_ids_all
    ),
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),
  # Locais data filtered by development mode
  tar_target(
    name = locais,
    command = apply_dev_mode_filters(
      locais_all,
      pipeline_config,
      state_col = "sg_uf"
    ),
    format = "qs"
  ),
  # Filter Brasília from municipal election years - using helper function
  tar_target(
    name = locais_filtered,
    command = apply_brasilia_filters(locais),
    format = "qs"
  ),
  # Consolidated input validation - checks dataset sizes
  tar_target(
    name = validate_inputs,
    command = validate_inputs_consolidated(
      muni_ids = muni_ids,
      inep_codes = inep_codes,
      locais_filtered = locais_filtered,
      pipeline_config = pipeline_config
    )
  ),
  tar_target(
    ## Import geocoded polling stations from TSE for ground truth
    name = tse_files,
    command = c(
      "./data/eleitorado_local_votacao_2018.csv.gz",
      "./data/eleitorado_local_votacao_2020.csv.gz",
      "./data/eleitorado_local_votacao_2022.csv.gz",
      "./data/eleitorado_local_votacao_2024.csv.gz"
    ),
    format = "file",
    repository = "local" # Force local storage for file targets to avoid S3 issues
  ),
  tar_target(
    name = tsegeocoded_locais,
    command = clean_tsegeocoded_locais(
      tse_files = tse_files,
      muni_ids = muni_ids,
      locais = locais_filtered
    ),
    format = "qs"
  ),
  ## Create panel ids to track polling stations across time
  ## Create municipality batches for panel ID processing
  tar_target(
    name = panel_municipality_batches,
    command = {
      # Explicitly depend on pipeline_config
      create_panel_municipality_batches(
        locais_data = locais_filtered,
        target_batch_size = ifelse(pipeline_config$dev_mode, 2000, 5000)
      )
    }
  ),

  ## Extract unique batch IDs for dynamic branching
  tar_target(
    name = panel_batch_ids,
    command = unique(panel_municipality_batches$batch_id)
  ),

  ## Process panel IDs by municipality batch using dynamic branching
  tar_target(
    name = panel_ids_by_batch,
    command = {
      # Get municipalities for this batch
      # In dynamic branching, panel_batch_ids represents the current batch ID value
      current_batch_id <- panel_batch_ids
      batch_municipalities <- panel_municipality_batches[
        batch_id == current_batch_id
      ]

      # Process panel IDs for this batch
      process_panel_ids_municipality_batch(
        locais_full = locais_filtered,
        municipality_batch = batch_municipalities,
        years = c(2006, 2008, 2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024),
        blocking_column = "cod_localidade_ibge",
        scoring_columns = c("normalized_name", "normalized_addr"),
        use_word_blocking = pipeline_config$use_word_blocking,
        panel_weight_threshold = pipeline_config$panel_weight_threshold
      )
    },
    pattern = map(panel_batch_ids),
    iteration = "list",
    # Mega-city batches (Sao Paulo, Rio) run 25+ minutes single-threaded here, which
    # crew 1.3.1 mistook for a dead worker until keep_crew_launch_handles() fixed it.
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
      # The combined panel IDs are already properly formatted; make_panel_ids()
      # attaches each panel's best coordinate. Pass the full geocoded output
      # (geocoded_locais) - not the TSE-only table - so panels whose years
      # predate TSE ground truth still get the model's coordinate, and pred_dist
      # is available to pick the most accurate one. Empty df_panels because all
      # states are already combined.
      make_panel_ids(data.table(), panel_ids_combined, geocoded_locais)
    },
    format = "qs"
  ),

  # --- Section-location mapping ---

  ## Import section-to-location mapping
  tar_target(
    name = secc_loc_map_file,
    command = "./data/secc_loc_map/secc_loc_map_2006_24.csv.gz",
    format = "file",
    repository = "local"
  ),
  tar_target(
    name = secc_loc_map_all,
    command = fread(secc_loc_map_file),
    format = "qs"
  ),
  # Filter section mapping by development mode
  tar_target(
    name = secc_loc_map,
    command = apply_dev_mode_filters(
      secc_loc_map_all,
      pipeline_config,
      state_col = "sg_uf"
    ),
    format = "qs"
  ),

  ## Create section-to-panel mapping for easy user joins
  tar_target(
    name = section_panel_mapping,
    command = create_section_panel_mapping(
      secc_loc_map = secc_loc_map,
      geocoded_locais = geocoded_locais,
      panel_ids = panel_ids
    ),
    format = "qs",
    storage = "worker",
    retrieval = "worker"
  ),

  # --- String matching targets ---
  # Size-balanced municipality batches, the unit every match target branches over.
  tar_target(
    name = municipality_batch_assignments,
    command = build_municipality_batches(locais_filtered, pipeline_config$dev_mode)
  ),

  # Extract unique batch IDs for dynamic branching
  tar_target(
    name = batch_ids,
    command = unique(municipality_batch_assignments$batch_id)
  ),

  # --- Reference slices ---
  # Each reference table is split into per-batch groups so a match branch retrieves
  # only its own slice (megabytes) instead of the whole national table (hundreds of
  # megabytes; inep_data alone is ~61 MB in production). The stbairro stems union the
  # street and neighborhood aggregates so the pair can never fall out of alignment.
  tar_target(
    name = inep_grouped,
    command = make_ref_batch_groups(inep_data, municipality_batch_assignments),
    iteration = "group",
    deployment = "main",
    memory = "persistent",
    format = "qs"
  ),
  tar_target(
    name = schools_cnefe10_grouped,
    command = make_ref_batch_groups(schools_cnefe10, municipality_batch_assignments),
    iteration = "group",
    deployment = "main",
    memory = "persistent",
    format = "qs"
  ),
  tar_target(
    name = schools_cnefe22_grouped,
    command = make_ref_batch_groups(schools_cnefe22, municipality_batch_assignments),
    iteration = "group",
    deployment = "main",
    memory = "persistent",
    format = "qs"
  ),
  tar_target(
    name = cnefe10_stbairro_grouped,
    command = make_stbairro_batch_groups(
      cnefe10_st,
      cnefe10_bairro,
      municipality_batch_assignments
    ),
    iteration = "group",
    deployment = "main",
    memory = "persistent",
    format = "qs"
  ),
  tar_target(
    name = cnefe22_stbairro_grouped,
    command = make_stbairro_batch_groups(
      cnefe22_st,
      cnefe22_bairro,
      municipality_batch_assignments
    ),
    iteration = "group",
    deployment = "main",
    memory = "persistent",
    format = "qs"
  ),
  tar_target(
    name = agrocnefe_stbairro_grouped,
    command = make_stbairro_batch_groups(
      agrocnefe_st,
      agrocnefe_bairro,
      municipality_batch_assignments
    ),
    iteration = "group",
    deployment = "main",
    memory = "persistent",
    format = "qs"
  ),

  # INEP string matching - process municipalities in batches
  tar_target(
    name = inep_string_match_batch,
    command = process_inep_batch(
      municipality_batch_assignments = municipality_batch_assignments,
      locais_filtered = locais_filtered,
      inep_data = inep_grouped
    ),
    pattern = map(inep_grouped),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "main",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "standard")
    )
  ),
  tar_target(
    name = inep_string_match,
    command = combine_match_batches(inep_string_match_batch, "inep_string_match"),
    deployment = "main"
  ),
  # Schools CNEFE 2010 matching with batched dynamic branching
  tar_target(
    name = schools_cnefe10_match_batch,
    command = process_schools_cnefe_batch(
      municipality_batch_assignments = municipality_batch_assignments,
      locais_filtered = locais_filtered,
      schools_cnefe = schools_cnefe10_grouped
    ),
    pattern = map(schools_cnefe10_grouped),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "main",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "standard")
    )
  ),
  tar_target(
    name = schools_cnefe10_match,
    command = combine_match_batches(schools_cnefe10_match_batch, "schools_cnefe10_match"),
    storage = "worker",
    retrieval = "worker"
  ),
  # Schools CNEFE 2022 matching - process municipalities in batches
  tar_target(
    name = schools_cnefe22_match_batch,
    command = process_schools_cnefe_batch(
      municipality_batch_assignments = municipality_batch_assignments,
      locais_filtered = locais_filtered,
      schools_cnefe = schools_cnefe22_grouped
    ),
    pattern = map(schools_cnefe22_grouped),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "main",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "standard")
    )
  ),
  tar_target(
    name = schools_cnefe22_match,
    command = combine_match_batches(schools_cnefe22_match_batch, "schools_cnefe22_match"),
    deployment = "main"
  ),
  # CNEFE 2010 street/neighborhood matching with batched dynamic branching.
  # The street/neighborhood matchers stay on `memory_limited` (8 workers): their peak
  # is the per-municipality distance matrix, min(10000, n_locais) x n_ref_streets x 8
  # bytes, which reaches multiple GB for the largest cities. 28 workers would exceed
  # the 50 GB machine.
  tar_target(
    name = cnefe10_stbairro_match_batch,
    command = process_cnefe_stbairro_batch(
      municipality_batch_assignments = municipality_batch_assignments,
      locais_filtered = locais_filtered,
      cnefe_stbairro = cnefe10_stbairro_grouped
    ),
    pattern = map(cnefe10_stbairro_grouped),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "main",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "memory_limited")
    )
  ),
  tar_target(
    name = cnefe10_stbairro_match,
    command = combine_match_batches(cnefe10_stbairro_match_batch, "cnefe10_stbairro_match"),
    storage = "worker",
    retrieval = "worker"
  ),
  # CNEFE 2022 street/neighborhood matching with batched dynamic branching.
  tar_target(
    name = cnefe22_stbairro_match_batch,
    command = process_cnefe_stbairro_batch(
      municipality_batch_assignments = municipality_batch_assignments,
      locais_filtered = locais_filtered,
      cnefe_stbairro = cnefe22_stbairro_grouped
    ),
    pattern = map(cnefe22_stbairro_grouped),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "main",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "memory_limited")
    )
  ),
  tar_target(
    name = cnefe22_stbairro_match,
    command = combine_match_batches(cnefe22_stbairro_match_batch, "cnefe22_stbairro_match"),
    storage = "worker",
    retrieval = "worker"
  ),
  # Agro CNEFE street/neighborhood matching with batched dynamic branching.
  # A former municipality-code mismatch made this match table come out empty; the
  # fix produces matches in dev but has not yet been re-run in production.
  tar_target(
    name = agrocnefe_stbairro_match_batch,
    command = process_agrocnefe_stbairro_batch(
      municipality_batch_assignments = municipality_batch_assignments,
      locais_filtered = locais_filtered,
      agrocnefe_stbairro = agrocnefe_stbairro_grouped
    ),
    pattern = map(agrocnefe_stbairro_grouped),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "main",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "memory_limited")
    )
  ),
  tar_target(
    name = agrocnefe_stbairro_match,
    # Plain rbindlist: tolerated empty until re-verified on a full production run.
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
    command = combine_match_batches(geocodebr_match_batch, "geocodebr_match"),
    storage = "worker",
    retrieval = "worker"
  ),

  # --- String match diagnostics ---

  # Calculate NA coordinate percentages and match quality metrics
  tar_target(
    name = string_match_diagnostics,
    command = aggregate_string_match_diagnostics(
      inep_string_match = inep_string_match,
      cnefe10_stbairro_match = cnefe10_stbairro_match,
      cnefe22_stbairro_match = cnefe22_stbairro_match,
      schools_cnefe10_match = schools_cnefe10_match,
      schools_cnefe22_match = schools_cnefe22_match,
      agrocnefe_stbairro_match = agrocnefe_stbairro_match
    ),
    deployment = "main"
  ),

  # Save diagnostics report
  tar_target(
    name = string_match_diagnostics_report,
    command = {
      report_text <- format_string_match_diagnostics(string_match_diagnostics)
      cat(report_text)
      dir.create("output", showWarnings = FALSE)
      writeLines(report_text, "output/string_match_diagnostics.txt")
      "output/string_match_diagnostics.txt"
    },
    format = "file",
    repository = "local"
  ),

  # --- Model training and prediction ---

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
    command = train_model(
      model_data,
      grid_n = ifelse(pipeline_config$dev_mode, 5, 50),
      dev_mode = pipeline_config$dev_mode
    ),
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

  # --- Evaluation harness ---
  # Leakage-controlled out-of-fold accuracy over the TSE-covered set, plus TSE
  # coverage density and the pred_dist calibration check. See R/evaluation.R.

  ## TSE coverage by year x state - ground-truth density.
  tar_target(
    name = tse_coverage,
    command = compute_tse_coverage(locais_filtered, tsegeocoded_locais)
  ),

  ## Raw per-year TSE coordinate availability - the ceiling landed coverage is read
  ## against by the near-lossless join tripwire in the release gates.
  tar_target(
    name = tse_raw_availability,
    command = compute_tse_raw_availability(tse_files, locais_filtered)
  ),

  ## Station-grouped fold assignment, created once upstream of any refit so a fold
  ## never leaks its TSE target.
  tar_target(
    name = eval_fold_assignment,
    command = assign_eval_folds(model_data)
  ),

  ## Out-of-fold pred_dist for every covered candidate. Memory-heavy (k LightGBM
  ## refits over most of the covered data) -> memory_limited controller.
  tar_target(
    name = oof_predictions,
    command = compute_oof_predictions(
      model_data,
      trained_model,
      eval_fold_assignment
    ),
    format = "qs",
    storage = "worker",
    retrieval = "worker",
    resources = tar_resources(
      crew = tar_resources_crew(controller = "memory_limited")
    )
  ),

  ## Per-station selected match from OOF scores, joined onto the covered-station
  ## universe with stratification axes.
  tar_target(
    name = oof_selected_matches,
    command = select_oof_matches(
      oof_predictions,
      locais_filtered,
      tsegeocoded_locais,
      tract_shp
    ),
    format = "qs"
  ),

  ## Stratified accuracy tables, joint with match rate, small-cell suppressed.
  tar_target(
    name = accuracy_tables,
    command = compute_accuracy_tables(oof_selected_matches)
  ),

  ## pred_dist calibration: rank-and-filter plus reliability/ENCE.
  tar_target(
    name = calibration_check,
    command = compute_calibration(oof_selected_matches)
  ),

  ## Thin Quarto report rendering the evaluation targets for human reading.
  tar_render(
    name = evaluation_report,
    path = "reports/evaluation_report.qmd",
    output_dir = "reports"
  ),

  # --- Final geocoding ---

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
        "local_id",
        "final_lat",
        "final_long",
        "ano",
        "nr_zona",
        "nr_locvot",
        "nm_locvot",
        "nm_localidade"
      ),
      unique_keys = c("local_id", "ano", "nr_zona", "nr_locvot"),
      stop_on_failure = TRUE
    )
  ),

  # --- Data export ---

  # Each export writes its file and returns the path, so format = "file" rebuilds it
  # when the output is deleted; repository = "local" keeps the outputs off S3.
  tar_target(
    name = geocoded_export,
    command = export_geocoded_locais(
      geocoded_locais,
      gates = list(validate_inputs, validate_model_data, validate_predictions, validate_geocoded_output)
    ),
    format = "file",
    repository = "local"
  ),
  tar_target(
    name = panelid_export,
    command = export_panel_ids(
      panel_ids,
      gates = list(validate_inputs, validate_model_data, validate_predictions, validate_geocoded_output)
    ),
    format = "file",
    repository = "local"
  ),
  tar_target(
    name = section_panel_export,
    command = {
      dir.create("output", showWarnings = FALSE)
      fwrite(section_panel_mapping, "output/section_panel_mapping.csv.gz")
      "output/section_panel_mapping.csv.gz"
    },
    format = "file",
    repository = "local"
  ),
  ## Fail-loud structural tripwires on a production rebuild before it can be shipped.
  ## Depends on the export paths so the output-files gate checks the written files.
  tar_target(
    name = release_gates,
    command = validate_release_gates(
      geocoded_locais = geocoded_locais,
      tse_coverage = tse_coverage,
      tse_raw_availability = tse_raw_availability,
      export_paths = c(geocoded_export, panelid_export),
      dev_mode = pipeline_config$dev_mode,
      panel_gate = panel_release_gates
    ),
    cue = tar_cue(mode = "always")
  ),
  ## Panel-output release gate: guards panel_ids.csv.gz. A dependency of release_gates
  ## so the canonical release check can't be built without also running this one.
  tar_target(
    name = panel_release_gates,
    command = validate_panel_release(panel_ids),
    cue = tar_cue(mode = "always")
  ),
  ## Data Quality Monitoring
  tar_target(
    name = data_quality_monitoring,
    command = create_data_quality_monitor(
      geocoded_locais = geocoded_locais,
      panel_ids = panel_ids,
      # Expected municipality count is derived from the states this run processes, so a
      # dev-filtered (AC/RR) output does not trip the municipality-count check.
      expected_municipality_count = get_expected_municipality_count_for_config(pipeline_config)
    ),
    # Always run monitoring to catch issues early
    cue = tar_cue(mode = "always")
  ),
  ## Sanity Check Report
  # Sanity check report - generate if quarto file exists
  tar_render(
    name = sanity_check_report,
    path = "reports/polling_station_sanity_check.qmd",
    output_dir = "reports"
  )
)
