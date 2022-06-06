# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline # nolint

# Load packages required to define the pipeline:
library(targets)
library(tarchetypes) # Load other packages as needed. # nolint
library(conflicted)

# Set target options:
tar_option_set(
  packages = c(
    "conflicted", "targets", "data.table", "dplyr", "purrr",
    "sf", "stringr", "tidyr", "recipes", "parsnip",
    "workflows", "tune", "rsample"
  ), # packages that your targets need to run
  format = "rds" # default storage format
  # Set other options as needed.
)

## Setup parallel processing
all_cores <- parallel::detectCores(logical = FALSE) - 1
data.table::setDTthreads(all_cores)
library(doFuture)
library(parallel)
registerDoFuture()
cl <- makeCluster(all_cores)
future::plan(cluster, workers = cl)

conflict_prefer("filter", "dplyr")
conflict_prefer("workflow", "workflows")
conflict_prefer("first", "dplyr")

# Load the R scripts with your custom functions:
lapply(list.files("./R", full.names = TRUE, pattern = "fns"), source)

## Do not use s2 spherical geometry package
sf::sf_use_s2(FALSE)

# Replace the target list below with your own:
list(
  ## import identifiers
  tar_target(
    name = muni_ids_file,
    command = "./data/muni_identifiers.csv",
    format = "file"
  ),
  tar_target(
    name = muni_ids,
    command = fread(muni_ids_file)
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
  ## import shape files
  tar_target(
    name = tract_shp_file,
    command = "./data/census_tracts2010_shp.rds",
    format = "file"
  ),
  tar_target(
    name = tract_shp,
    command = readRDS(tract_shp_file)
  ),
  tar_target(
    name = muni_shp_file,
    command = "./data/muni_shp.rds",
    format = "file"
  ),
  tar_target(
    name = muni_shp,
    command = readRDS(muni_shp_file)
  ),
  ## import municipal demographic data
  tar_target(
    name = muni_demo_file,
    command = "./data/atlas_brasil_census_data.xlsx",
    format = "file"
  ),
  tar_target(
    name = muni_demo,
    command = import_muni_demo(muni_demo_file)
  ),
  ## calculate geographic features of municipalities
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
    name = cnefe_file,
    command = "./data/CNEFE_combined.gz",
    format = "file"
  ),
  tar_target(
    name = cnefe,
    command = clean_cnefe(
      cnefe_file = cnefe_file,
      muni_ids = muni_ids, tract_centroids = tract_centroids
    ),
    format = "fst_dt"
  ),
  ## Subset on schools in CNEFE
  tar_target(
    name = schools_cnefe,
    command = dplyr::filter(cnefe, especie_lab == "estabelecimento de ensino") %>%
      mutate(norm_desc = normalize_school(desc)) %>%
      as.data.table(),
    format = "fst_dt"
  ),
  ## Create a dataset of streets in CNEFE
  tar_target(
    name = cnefe_st,
    command = cnefe[, .(long = median(cnefe_long, na.rm = TRUE), lat = median(cnefe_lat, na.rm = TRUE), n = .N),
      by = .(id_munic_7, norm_street)
    ][n > 1],
    format = "fst_dt"
  ),
  ## Create a dataset of neighborhoods in CNEFE
  tar_target(
    name = cnefe_bairro,
    command = cnefe[, .(long = median(cnefe_long, na.rm = TRUE), lat = median(cnefe_lat, na.rm = TRUE), n = .N),
      by = .(id_munic_7, norm_bairro)
    ][n > 1],
    format = "fst_dt"
  ),
  ## Import and clean 2017 CNEFE
  tar_target(
    name = agro_cnefe_files,
    command = (dir("./data/agro_censo/",
      full.names = TRUE
    )),
    format = "file"
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
    command = agro_cnefe[, .(
      long = median(longitude, na.rm = TRUE),
      lat = median(latitude, na.rm = TRUE), n = .N
    ),
    by = .(id_munic_7, norm_street)
    ][n > 1],
    format = "fst_dt"
  ),
  ## Create a dataset of neighborhoods in 2017 CNEFE
  tar_target(
    name = agrocnefe_bairro,
    command = agro_cnefe[, .(
      long = median(longitude, na.rm = TRUE),
      lat = median(latitude, na.rm = TRUE), n = .N
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
    name = inep_data,
    command = clean_inep(
      inep_data = fread(inep_file),
      inep_codes = inep_codes
    )
  ),
  ## Import Locais de Votação Data
  tar_target(
    name = locais_file,
    command = "./data/polling_stations_2006_2018.csv.gz",
    format = "file"
  ),
  tar_target(
    name = locais,
    command = import_locais(
      locais_file = locais_file,
      muni_ids = muni_ids
    )
  ),
  ## Create panel ids to track polling stations across time
  tar_target(
    name = panel_ids,
    command = create_panel_ids_munis(locais, prop_match_cutoff = .3)
  ),

  ## Import geocoded polling stations from TSE for ground truth
  tar_target(
    name = tse_file,
    command = "./data/eleitorado_local_votacao_2018.csv.gz",
    format = "file"
  ),
  tar_target(
    name = tsegeocoded_locais18,
    command = clean_locais18(fread(tse_file, encoding = "Latin-1"),
      muni_ids = muni_ids, locais = locais
    ),
  ),
  ## String Matching
  tar_target(
    name = inep_string_match,
    command = rbindlist(map(
      unique(locais$cod_localidade_ibge),
      ~ match_inep_muni(
        locais_muni = locais[cod_localidade_ibge == .x],
        inep_muni = inep_data[id_munic_7 == .x]
      )
    ))
  ),
  tar_target(
    name = schools_cnefe_match,
    command = rbindlist(map(
      unique(locais$cod_localidade_ibge),
      ~ match_schools_cnefe_muni(
        locais_muni = locais[cod_localidade_ibge == .x],
        schools_cnefe_muni = schools_cnefe[id_munic_7 == .x]
      )
    ))
  ),
  tar_target(
    name = cnefe_stbairro_match,
    command = rbindlist(map(
      unique(locais$cod_localidade_ibge),
      ~ match_stbairro_cnefe_muni(
        locais_muni = locais[cod_localidade_ibge == .x],
        cnefe_st_muni = cnefe_st[id_munic_7 == .x],
        cnefe_bairro_muni = cnefe_bairro[id_munic_7 == .x]
      )
    ))
  ),
  tar_target(
    name = agrocnefe_stbairro_match,
    command = rbindlist(map(
      unique(locais$cod_localidade_ibge),
      ~ match_stbairro_agrocnefe_muni(
        locais_muni = locais[cod_localidade_ibge == .x],
        agrocnefe_st_muni = agrocnefe_st[id_munic_7 == .x],
        agrocnefe_bairro_muni = agrocnefe_bairro[id_munic_7 == .x]
      )
    ))
  ),

  # Choose best match
  tar_target(
    name = string_match,
    command = predict_distance(
      cnefe_stbairro_match = cnefe_stbairro_match,
      schools_cnefe_match = schools_cnefe_match,
      inep_string_match = inep_string_match,
      agrocnefe_stbairro_match = agrocnefe_stbairro_match,
      locais = locais, tsegeocoded_locais18 = tsegeocoded_locais18,
      muni_demo = muni_demo, muni_area = muni_area
    )
  ),

  # Use string matches to geocode and add panel ids
  tar_target(
    name = geocoded_locais,
    command = finalize_coords(locais, string_match, tsegeocoded_locais18)
  ),
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

  ##Methodology and Evaluation
  tar_render(
    name = geocode_writeup,
    path = "./doc/geocoding_procedure.Rmd"
  )
)
