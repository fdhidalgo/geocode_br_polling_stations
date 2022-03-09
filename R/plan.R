the_plan <-
  drake_plan(
    # import identifiers -------------------------------------------------------------
    muni_ids = fread(file_in("./data/muni_identifiers.csv")),
    inep_codes = fread(file_in("data/inep_codes.csv")),

    # import shape files -------------------------------------------------------------
    tract_shp = readRDS(file_in("./data/census_tracts2010_shp.rds")),

    # import municipal demographics -------------------------------------------
    muni_demo = import_muni_demo(file_in("./data/atlas_brasil_census_data.xlsx")),

    # calculate geographic features of municipalities ---------------------------------------------
    tract_centroids = make_tract_centroids(tract_shp),
    muni_area = calc_muni_area(readRDS(file_in("./data/muni_shp.rds"))),

    # Import and CLean CNEFE data --------------------------------------------------------------
    cnefe = target(clean_cnefe(
      cnefe_file = "./data/CNEFE_combined.gz",
      muni_ids = muni_ids, tract_centroids = tract_centroids
    ),
    format = "fst_dt"
    ),
    ### Subset on schools in CNEFE
    schools_cnefe = target(dplyr::filter(cnefe, especie_lab == "estabelecimento de ensino") %>%
      mutate(norm_desc = normalize_school(desc)) %>%
      as.data.table(),
    format = "fst_dt"
    ),
    ### Create dataset of streets in CNEFE
    cnefe_st = target(cnefe[, .(long = median(cnefe_long, na.rm = TRUE), lat = median(cnefe_lat, na.rm = TRUE), n = .N),
      by = .(id_munic_7, norm_street)
    ][n > 1], format = "fst_dt"),
    ### Create dataset of neighborhoods in CNEFE
    cnefe_bairro = target(cnefe[, .(long = median(cnefe_long, na.rm = TRUE), lat = median(cnefe_lat, na.rm = TRUE), n = .N),
      by = .(id_munic_7, norm_bairro)
    ][n > 1], format = "fst_dt"),

    ## Import and clean 2017 CNEFE
    agro_cnefe = target(clean_agro_cnefe(agro_cnefe_files = (dir("./data/agro_censo/",
      full.names = TRUE
    )), muni_ids = muni_ids), format = "fst_dt"),
    agrocnefe_st = target(agro_cnefe[, .(
      long = median(longitude, na.rm = TRUE),
      lat = median(latitude, na.rm = TRUE), n = .N
    ),
    by = .(id_munic_7, norm_street)
    ][n > 1], format = "fst_dt"),
    agrocnefe_bairro = target(agro_cnefe[, .(
      long = median(longitude, na.rm = TRUE),
      lat = median(latitude, na.rm = TRUE), n = .N
    ),
    by = .(id_munic_7, norm_bairro)
    ][n > 1], format = "fst_dt"),

    # Import and Clean INEP Data ---------------------------------------------------------------
    inep_data = clean_inep(
      inep_data = fread(file_in("./data/inep_catalogo_das_escolas.csv.gz")),
      inep_codes = inep_codes
    ),

    # Import Locais de Votacao Data -------------------------------------------
    locais = import_locais(file_in("./data/polling_stations_2006_2018.csv.gz"), muni_ids = muni_ids),

    # Create panel ids to track polling stations across time ------------------
    panel_ids = create_panel_ids_munis(locais, prop_match_cutoff = .3),

    # Import geocoded polling stations from TSE for ground truth --------------
    tsegeocoded_locais18 = clean_locais18(
      locais18 = fread(file_in("./data/eleitorado_local_votacao_2018.csv.gz"),
        encoding = "Latin-1"
      ),
      muni_ids = muni_ids, locais = locais
    ),

    # String Matching ---------------------------------------------------------
    inep_string_match = rbindlist(map(
      unique(locais$cod_localidade_ibge),
      ~ match_inep_muni(
        locais_muni = locais[cod_localidade_ibge == .x],
        inep_muni = inep_data[id_munic_7 == .x]
      )
    )),
    schools_cnefe_match = rbindlist(map(
      unique(locais$cod_localidade_ibge),
      ~ match_schools_cnefe_muni(
        locais_muni = locais[cod_localidade_ibge == .x],
        schools_cnefe_muni = schools_cnefe[id_munic_7 == .x]
      )
    )),
    cnefe_stbairro_match = rbindlist(map(
      unique(locais$cod_localidade_ibge),
      ~ match_stbairro_cnefe_muni(
        locais_muni = locais[cod_localidade_ibge == .x],
        cnefe_st_muni = cnefe_st[id_munic_7 == .x],
        cnefe_bairro_muni = cnefe_bairro[id_munic_7 == .x]
      )
    )),
    agrocnefe_stbairro_match = rbindlist(map(
      unique(locais$cod_localidade_ibge),
      ~ match_stbairro_agrocnefe_muni(
        locais_muni = locais[cod_localidade_ibge == .x],
        agrocnefe_st_muni = agrocnefe_st[id_munic_7 == .x],
        agrocnefe_bairro_muni = agrocnefe_bairro[id_munic_7 == .x]
      )
    )),
    # Choose best match
    string_match = predict_distance(
      cnefe_stbairro_match = cnefe_stbairro_match,
      schools_cnefe_match = schools_cnefe_match,
      inep_string_match = inep_string_match,
      agrocnefe_stbairro_match = agrocnefe_stbairro_match,
      locais = locais, tsegeocoded_locais18 = tsegeocoded_locais18,
      muni_demo = muni_demo, muni_area = muni_area
    ),

    # Use string matches to geocode and add panel ids -------------------------------------------
    geocoded_locais = finalize_coords(locais, string_match, tsegeocoded_locais18),
    geocode_export = readr::write_csv(geocoded_locais, file_out("./output/geocoded_polling_stations.csv.gz")),
    panelid_export = readr::write_csv(panel_ids, file_out("./output/panel_ids.csv.gz")),

    # Documentation and Writeup -----------------------------------------------
    geocode_writeup = target(
      command = {
        rmarkdown::render(knitr_in("doc/geocoding_procedure.Rmd"))
        file_out("doc/geocoding_procedure.html")
      }
    )
  )