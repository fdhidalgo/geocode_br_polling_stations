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

    # calculate centroids -----------------------------------------------------
    tract_centroids = make_tract_centroids(tract_shp),
    muni_centroids = make_muni_centroids(readRDS(file_in("./data/muni_shp.rds"))),

    # Import Google Geocoded Data --------------------------------------------------------------
    google_geocoded_df = fread(file_in("./data/google_geocoded.csv")),


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

    locais12 = fread(file_in("./data/secoes/VOTOS_PREFEITO_LOCAL_VOTACAO_2012.csv")) %>%
      janitor::clean_names() %>%
      select(uf, cod_mun_tse, cod_mun_tse, cod_mun_ibge, num_zona, num_local_votacao,
             nome_municipio, nome_local_votacao, descricao_endereco, descricao_bairro) %>%
      distinct() %>%
      mutate(normalized_name = normalize_school(nome_local_votacao),
             normalized_addr = paste(normalize_address(descricao_endereco), normalize_address(descricao_bairro)),
             normalized_st = normalize_address(descricao_endereco),
             normalized_bairro = normalize_address(descricao_bairro)),

    locais14 = fread(file_in("./data/secoes/VOTOS_PRESIDENTE_LOCAL_VOTACAO_2014.csv")) %>%
      janitor::clean_names() %>%
      select(uf, cod_mun_tse, cod_mun_tse, cod_mun_ibge, num_zona, num_local_votacao,
             nome_municipio, nome_local_votacao, descricao_endereco, descricao_bairro) %>%
      distinct() %>%
      mutate(normalized_name = normalize_school(nome_local_votacao),
             normalized_addr = paste(normalize_address(descricao_endereco), normalize_address(descricao_bairro)),
             normalized_st = normalize_address(descricao_endereco),
             normalized_bairro = normalize_address(descricao_bairro)),

    locais16 = fread(file_in("./data/secoes/locais_voto_2016.csv")) %>%
      janitor::clean_names() %>%
      select(sg_uf, cd_localidade_tse, nm_localidade, nr_zona, nr_locvot,
             nm_locvot, ds_endereco, ds_bairro) %>%
      distinct() %>%
      mutate(normalized_name = normalize_school(nm_locvot),
             normalized_addr = paste(normalize_address(ds_endereco), normalize_address(ds_bairro)),
             normalized_st = normalize_address(ds_endereco),
             normalized_bairro = normalize_address(ds_bairro)),

    locais18 = clean_locais18(locais18 = fread(file_in("./data/secoes/local-votacao-08-08-2018.csv")),
                              muni_ids = muni_ids),

    locais = combine_locais(muni_ids = muni_ids, locais12 = locais12, locais14 = locais14, locais16 = locais16,
                            locais18 = locais18),


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
    #choose best match
    best_string_match = get_best_string_match(cnefe_stbairro_match = cnefe_stbairro_match,
                                              schools_cnefe_match = schools_cnefe_match,
                                              inep_string_match = inep_string_match,
                                              agrocnefe_stbairro_match = agrocnefe_stbairro_match),


    # Use string matches to geocode -------------------------------------------

    geocoded_locais = left_join(locais, best_string_match) %>%
      mutate(long = ifelse(is.na(longitude_local), long, longitude_local),
             lat = ifelse(is.na(latitude_local), lat, latitude_local)),


# Documentation and Writeup -----------------------------------------------

        geocode_writeup = target(
          command = {
            rmarkdown::render(knitr_in("doc/geocode_polling_stations/geocode_polling_stations.Rmd"))
            file_out("doc/geocode_polling_stations/geocode_polling_stations.html")
          }
    )
  )

