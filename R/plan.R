the_plan <-
  drake_plan(

    # import identifiers -------------------------------------------------------------

    muni_ids = fread(file_in("./data/muni_identifiers.csv")),
    inep_codes =  fread(file_in("data/inep_codes.csv")),
    mesomicro_ids = fread(file_in("./data/meso_micro_region_ids.csv")),

    # import shape files -------------------------------------------------------------
    tract_shp = readRDS(file_in("./data/census_tracts2010_shp.rds")),
    muni_shp = make_muni_shp(muni_shp = readRDS(file_in("./data/muni_shp.rds")),
                             semiarid05_shp = readRDS(file_in("./data/semiarid05_shp.rds"))),


  # import municipal demographics -------------------------------------------
  muni_demo = readxl::read_xlsx("./data/atlas_brasil_census_data.xlsx", sheet = 2) %>%
    select(id_munic_7 = Codmun7, ANO, POP, pesoRUR, RDPC) %>%
    janitor::clean_names() %>%
    filter(ano == 2010) %>% select(-ano),


    # calculate centroids -----------------------------------------------------
    tract_centroids = make_tract_centroids(tract_shp),
    muni_centroids = make_muni_centroids(readRDS(file_in("./data/muni_shp.rds"))),

    # Import and CLean CNEFE data --------------------------------------------------------------
    cnefe = target(clean_cnefe(cnefe = fread(file_in("./data/CNEFE_Universo_07-03-2019.gz"),
                                             drop = c("SITUACAO_SETOR", "NOM_COMP_ELEM1",
                                                      "VAL_COMP_ELEM1", "NOM_COMP_ELEM2",
                                                      "VAL_COMP_ELEM2", "NOM_COMP_ELEM3",
                                                      "VAL_COMP_ELEM3", "NOM_COMP_ELEM4",
                                                      "VAL_COMP_ELEM4", "NOM_COMP_ELEM5",
                                                      "VAL_COMP_ELEM5", "INDICADOR_ENDERECO",
                                                      "NUM_QUADRA", "NUM_FACE", "CEP_FACE",
                                                      "COD_UNICO_ENDERECO")),
                               muni_ids = muni_ids, tract_centroids = tract_centroids),
                   format = "fst_dt"),
    ### Subset on schools in CNEFE
    schools_cnefe = target(dplyr::filter(cnefe, especie_lab == "estabelecimento de ensino") %>%
                             mutate(norm_desc = normalize_school(desc)) %>%
                             as.data.table(),
                           format = "fst_dt"),
    ### Create dataset of streets in CNEFE
    cnefe_st = target(cnefe[, .(long = median(cnefe_long, na.rm = TRUE), lat = median(cnefe_lat, na.rm = TRUE), n = .N),
                            by = .(id_munic_7, norm_street)][n > 1], format = "fst_dt"),
    ###Create dataset of neighborhoods in CNEFE
    cnefe_bairro = target(cnefe[, .(long = median(cnefe_long, na.rm = TRUE), lat = mean(cnefe_lat, na.rm = TRUE), n = .N),
                                by = .(id_munic_7, norm_bairro)][n > 1], format = "fst_dt"),

    ## Import and clean 2017 CNEFE
    agro_cnefe = target(clean_agro_cnefe(agro_cnefe =  janitor::clean_names(
      rbindlist(lapply(dir("./data/agro_censo/", full.names = TRUE), fread,
                       drop = c("SITUACAO", "NOM_COMP_ELEM1", "COD_DISTRITO",
                                "VAL_COMP_ELEM1", "NOM_COMP_ELEM2",
                                "VAL_COMP_ELEM2", "NOM_COMP_ELEM3",
                                "VAL_COMP_ELEM3", "NOM_COMP_ELEM4",
                                "VAL_COMP_ELEM4", "NOM_COMP_ELEM5",
                                "VAL_COMP_ELEM5",  "ALTITUDE", "COD_ESPECIE",
                                "CEP")))),
      muni_ids = muni_ids), format = "fst_dt"),
    agrocnefe_st = target(agro_cnefe[, .(long = mean(longitude, na.rm = TRUE), lat = mean(latitude, na.rm = TRUE), n = .N),
                                     by = .(id_munic_7, norm_street)][n > 1], format = "fst_dt"),

    agrocnefe_bairro = target(agro_cnefe[, .(long = mean(longitude, na.rm = TRUE), lat = mean(latitude, na.rm = TRUE), n = .N),
                                         by = .(id_munic_7, norm_bairro)][n > 1], format = "fst_dt"),

    # Import and Clean INEP Data ---------------------------------------------------------------
    inep_data = clean_inep(inep_data = fread(file_in("./data/inep_catalogo_das_escolas.csv")),
                           inep_codes = inep_codes),

    # Import Locais de Votacao Data -------------------------------------------
    locais = fread("./data/secoes/painel_lv.csv") %>%
      janitor::clean_names() %>%
      mutate(normalized_name = normalize_school(nm_locvot),
             normalized_addr = paste(normalize_address(ds_endereco), normalize_address(ds_bairro)),
             normalized_st = normalize_address(ds_endereco),
             normalized_bairro = normalize_address(ds_bairro)) %>%
      left_join(select(muni_ids, cod_localidade_ibge = id_munic_7, cd_localidade_tse = id_TSE)) %>%
      filter(!is.na(nm_locvot)) %>%
      mutate(local_id = 1:n()),

    tsegeocoded_locais18 = clean_locais18(locais18 = fread(file_in("./data/secoes/local-votacao-08-08-2018.csv")),
                                          muni_ids = muni_ids) %>%
      select(cod_localidade_ibge, nr_zona = zona, nr_locvot = num_local,
             tse_lat = latitude_local, tse_long = longitude_local)  %>%
      filter(!is.na(tse_lat)) %>% mutate(ano = 2018) %>%
      left_join(select(locais, local_id, ano, cod_localidade_ibge, nr_zona, nr_locvot)),

    # String Matching ---------------------------------------------------------

    inep_string_match = rbindlist(map(unique(locais$cod_localidade_ibge),
                                      ~ match_inep_muni(locais_muni = locais[cod_localidade_ibge == .x],
                                                        inep_muni = inep_data[id_munic_7 == .x]))),
    schools_cnefe_match = rbindlist(map(unique(locais$cod_localidade_ibge),
                                        ~ match_schools_cnefe_muni(locais_muni = locais[cod_localidade_ibge == .x],
                                                                   schools_cnefe_muni = schools_cnefe[id_munic_7 == .x]))),
    cnefe_stbairro_match = rbindlist(map(unique(locais$cod_localidade_ibge),
                                         ~ match_stbairro_cnefe_muni(locais_muni = locais[cod_localidade_ibge == .x],
                                                                     cnefe_st_muni = cnefe_st[id_munic_7 == .x],
                                                                     cnefe_bairro_muni = cnefe_bairro[id_munic_7 == .x]))),
    agrocnefe_stbairro_match = rbindlist(map(unique(locais$cod_localidade_ibge),
                                             ~ match_stbairro_agrocnefe_muni(locais_muni = locais[cod_localidade_ibge == .x],
                                                                             agrocnefe_st_muni = agrocnefe_st[id_munic_7 == .x],
                                                                             agrocnefe_bairro_muni = agrocnefe_bairro[id_munic_7 == .x]))),
    # Choose best match
    best_string_match = get_best_string_match(cnefe_stbairro_match = cnefe_stbairro_match,
                                              schools_cnefe_match = schools_cnefe_match,
                                              inep_string_match = inep_string_match,
                                              agrocnefe_stbairro_match = agrocnefe_stbairro_match,
                                              locais = locais, tsegeocoded_locais18 = tsegeocoded_locais18,
                                              muni_demo = muni_demo),

    # Import Google Geocoded Data --------------------------------------------------------------
    google_geocoded_df = fread(file_in("./data/google_geocoded.csv")),

    # Use string matches to geocode -------------------------------------------
    geocoded_locais = left_join(locais, best_string_match) %>%
      left_join(tsegeocoded_locais18), #%>%
  #    mutate(long = ifelse(is.na(tse_long), long, tse_long),
  #           lat = ifelse(is.na(tse_lat), lat, tse_lat)),


    # Documentation and Writeup -----------------------------------------------

    geocode_writeup = target(
      command = {
        rmarkdown::render(knitr_in("doc/geocode_polling_stations/geocode_polling_stations.Rmd"))
        file_out("doc/geocode_polling_stations/geocode_polling_stations.html")
      }
    )
  )

