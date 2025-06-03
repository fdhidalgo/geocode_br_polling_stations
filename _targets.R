# Load packages required to define the pipeline:
library(targets)
library(tarchetypes)
library(conflicted)
library(future.apply)
library(data.table)

# Set global options for future
options(future.globals.maxSize = 2 * 1024^3) # 2GB limit, was in process_with_progress

# Set target options:
tar_option_set(
  packages = c(
    "conflicted",
    "targets",
    "data.table",
    "stringr",
    "bonsai",
    "future",
    "reclin2",
    "future.apply",
    "validate",
    "geosphere",
    "sf"
  ),
  format = "qs", # default storage format,
  memory = "transient",
  garbage_collection = TRUE
)

## Setup parallel processing

data.table::setDTthreads(future::availableCores(
  omit = floor(future::availableCores() / 2)
))
future::plan(
  future::multisession,
  workers = floor(future::availableCores() / 2)
)
library(progressr)

# Development mode flag - set to TRUE for faster iteration with subset of states
DEV_MODE <- TRUE  # Process only AC, RR, AP, RO states when TRUE

# Load the R scripts with your custom functions:
lapply(list.files("./R", full.names = TRUE, pattern = "fns"), source)
# Load validation functions
source("./R/functions_validate.R")
# Load state filtering functions
source("./R/state_filtering.R")

## Do not use s2 spherical geometry package
# sf::sf_use_s2(FALSE)

# Get pipeline configuration based on development mode
pipeline_config <- get_pipeline_config(DEV_MODE)

# Print configuration info
if (pipeline_config$dev_mode) {
  message("Running in DEVELOPMENT MODE")
  message("Processing states: ", paste(pipeline_config$dev_states, collapse = ", "))
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
      result <- validate_muni_ids_data(muni_ids)
      if (!result$passed) {
        stop(
          "Municipal ID validation failed. Run tar_read(validate_muni_ids) to see details."
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
      result <- validate_inep_codes_data(inep_codes)
      if (!result$passed) {
        stop(
          "INEP codes validation failed. Run tar_read(validate_inep_codes) to see details."
        )
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
        tract_filtered <- tract_shp_all[substr(tract_shp_all$code_tract, 1, 2) %in% unique(dev_state_codes), ]
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
        muni_filtered <- muni_shp_all[muni_shp_all$code_muni %in% dev_muni_codes, ]
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
  tar_target(
    name = cnefe10_raw,
    command = {
      if (pipeline_config$dev_mode) {
        # Read only dev states from partitioned files
        read_state_cnefe(2010, pipeline_config$dev_states, base_dir = "data")
      } else {
        # In production, read the full file
        fread("./data/CNEFE_combined.gz")
      }
    }
  ),
  tar_target(
    name = cnefe22_raw,
    command = {
      if (pipeline_config$dev_mode) {
        # Read only dev states from partitioned files
        read_state_cnefe(2022, pipeline_config$dev_states, base_dir = "data")
      } else {
        # In production, read the full file
        fread("./data/cnefe22.csv.gz")
      }
    }
  ),
  tar_target(
    name = cnefe10,
    command = clean_cnefe10(
      cnefe_file = cnefe10_raw,
      muni_ids = muni_ids,
      tract_centroids = tract_centroids
    ),
    format = "fst_dt"
  ),
  tar_target(
    name = cnefe22,
    command = clean_cnefe22(
      cnefe22_file = cnefe22_raw,
      muni_ids = muni_ids
    ),
    format = "fst_dt"
  ),

  ## Subset on schools in 2010 CNEFE
  tar_target(
    name = schools_cnefe10,
    command = cnefe10[especie_lab == "estabelecimento de ensino"][,
      norm_desc := normalize_school(desc)
    ],
    format = "fst_dt"
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
    format = "fst_dt"
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
    format = "fst_dt"
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
    format = "fst_dt"
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
    format = "fst_dt"
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
    format = "fst_dt"
  ),
  tar_target(
    ## Extract schools frome 2022 CNEFE
    name = schools_cnefe22,
    command = get_cnefe22_schools(cnefe22),
    format = "fst_dt"
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
    format = "fst_dt"
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
    format = "fst_dt"
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
        dev_muni_codes <- get_muni_codes_for_states(muni_ids, pipeline_config$dev_states)
        inep_data_all[id_munic_7 %in% dev_muni_codes]
      } else {
        inep_data_all
      }
    }
  ),
  ## Import Locais de Votação Data
  tar_target(
    name = locais_file,
    command = "./data/polling_stations_2006_2022.csv.gz",
    format = "file"
  ),
  tar_target(
    name = locais_all,
    command = import_locais(
      locais_file = locais_file,
      muni_ids = muni_ids
    )
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
      locais = locais
    )
  ),
  ## Create panel ids to track polling stations across time
  ## Create panel ids for Brasília
  tar_target(
    name = panel_ids_df,
    command = {
      # Only process DF if it exists in the data (production mode)
      df_data <- locais[sg_uf == "DF"]
      if (nrow(df_data) > 0) {
        make_panel_1block(
          df_data,
          years = c(2006, 2008, 2010, 2012, 2014, 2018, 2022),
          blocking_column = "cod_localidade_ibge",
          scoring_columns = c("normalized_name", "normalized_addr")
        )
      } else {
        # Return empty data.table with expected structure
        data.table()
      }
    },
    format = "fst_dt"
  ),
  tar_target(
    panel_state,
    command = {
      if (pipeline_config$dev_mode) {
        # In dev mode, only process dev states (excluding DF which is handled separately)
        setdiff(pipeline_config$dev_states, "DF")
      } else {
        # In production, process all states except DF
        c(
          "AC", "AL", "AM", "AP", "BA", "CE", "ES", "GO",
          "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI",
          "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE",
          "SP", "TO"
        )
      }
    }
  ),
  ## Iterate over states (except Brasília) to create panel ids
  tar_target(
    name = panel_ids_states,
    command = make_panel_1block(
      block = locais[sg_uf == panel_state],
      years = c(2006, 2008, 2010, 2012, 2014, 2016, 2018, 2020, 2022),
      blocking_column = "cod_localidade_ibge",
      scoring_columns = c("normalized_name", "normalized_addr")
    ),
    pattern = map(panel_state)
  ),
  tar_target(
    name = panel_ids,
    command = make_panel_ids(panel_ids_df, panel_ids_states, geocoded_locais)
  ),

  # String Matching
  tar_target(
    name = inep_string_match,
    command = {
      future.apply::future_lapply(
        X = unique(locais$cod_localidade_ibge),
        FUN = function(.x) {
          match_inep_muni(
            locais_muni = locais[cod_localidade_ibge == .x],
            inep_muni = inep_data[id_munic_7 == .x]
          )
        },
        future.seed = TRUE
      ) |>
        data.table::rbindlist()
    }
  ),
  tar_target(
    name = schools_cnefe10_match,
    command = {
      future.apply::future_lapply(
        X = unique(locais$cod_localidade_ibge),
        FUN = function(.x) {
          match_schools_cnefe_muni(
            locais_muni = locais[cod_localidade_ibge == .x],
            schools_cnefe_muni = schools_cnefe10[id_munic_7 == .x]
          )
        },
        future.seed = TRUE
      ) |>
        data.table::rbindlist()
    }
  ),
  tar_target(
    name = schools_cnefe22_match,
    command = {
      future.apply::future_lapply(
        X = unique(locais$cod_localidade_ibge),
        FUN = function(.x) {
          match_schools_cnefe_muni(
            locais_muni = locais[cod_localidade_ibge == .x],
            schools_cnefe_muni = schools_cnefe22[id_munic_7 == .x]
          )
        },
        future.seed = TRUE
      ) |>
        data.table::rbindlist()
    }
  ),
  tar_target(
    name = cnefe10_stbairro_match,
    command = {
      future.apply::future_lapply(
        X = unique(locais$cod_localidade_ibge),
        FUN = function(.x) {
          match_stbairro_cnefe_muni(
            locais_muni = locais[cod_localidade_ibge == .x],
            cnefe_st_muni = cnefe10_st[id_munic_7 == .x],
            cnefe_bairro_muni = cnefe10_bairro[id_munic_7 == .x]
          )
        },
        future.seed = TRUE
      ) |>
        data.table::rbindlist()
    }
  ),
  tar_target(
    name = cnefe22_stbairro_match,
    command = {
      future.apply::future_lapply(
        X = unique(locais$cod_localidade_ibge),
        FUN = function(.x) {
          match_stbairro_cnefe_muni(
            locais_muni = locais[cod_localidade_ibge == .x],
            cnefe_st_muni = cnefe22_st[id_munic_7 == .x],
            cnefe_bairro_muni = cnefe22_bairro[id_munic_7 == .x]
          )
        },
        future.seed = TRUE
      ) |>
        data.table::rbindlist()
    }
  ),
  tar_target(
    name = agrocnefe_stbairro_match,
    command = {
      future.apply::future_lapply(
        X = unique(locais$cod_localidade_ibge),
        FUN = function(.x) {
          match_stbairro_agrocnefe_muni(
            locais_muni = locais[cod_localidade_ibge == .x],
            agrocnefe_st_muni = agrocnefe_st[id_munic_7 == .x],
            agrocnefe_bairro_muni = agrocnefe_bairro[id_munic_7 == .x]
          )
        },
        future.seed = TRUE
      ) |>
        data.table::rbindlist()
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
      muni_demo = muni_demo,
      muni_area = muni_area,
      locais = locais,
      tsegeocoded_locais = tsegeocoded_locais
    ),
  ),
  ## Train model and make predictions
  tar_target(
    name = trained_model,
    command = train_model(model_data, grid_n = 50),
  ),
  tar_target(
    name = model_predictions,
    command = get_predictions(trained_model, model_data),
    format = "fst_dt"
  ),

  # # Use string matches to geocode
  tar_target(
    name = geocoded_locais,
    command = finalize_coords(locais, model_predictions, tsegeocoded_locais),
    format = "fst_dt"
  ),
  ## Export data
  tar_target(
    name = geocoded_export,
    command = export_geocoded_locais(geocoded_locais),
    format = "file"
  ),
  tar_target(
    name = panelid_export,
    command = export_panel_ids(panel_ids),
    format = "file"
  ),
  ## Methodology and Evaluation
  tar_render(
    name = geocode_writeup,
    path = "./doc/geocoding_procedure.Rmd",
    cue = tar_cue(mode = ifelse(pipeline_config$dev_mode, "never", "thorough"))
  )
)
