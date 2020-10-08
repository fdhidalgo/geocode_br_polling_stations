
##normalize addresses
normalize_address <- function(x){
  stringi::stri_trans_general(x, "Latin-ASCII") %>%
    str_to_lower() %>%
    str_remove_all("[[:punct:]]") %>%
    str_remove("\\bzona rural\\b") %>%
    str_remove("\\bpovoado\\b") %>%
    str_remove("\\blocalidade\\b") %>%
    str_remove_all("\\.|\\/") %>%
    str_replace_all("^av\\b", "avenida") %>%
    str_replace_all("^r\\b", "rua") %>%
    str_replace_all("\\bs n\\b", "sn")%>%
    str_squish()

}


##normalize school names
normalize_school <- function(x){
  school_syns <- c("e m e i", "esc inf", "esc mun", "unidade escolar", "centro educacional", "escola municipal",
                   "colegio estadual", "cmei", "emeif", "grupo escolar", "escola estadual", "erem", "colegio municipal",
                   "centro de ensino infantil", "escola mul", "e m", "grupo municipal", "e e", "creche", "escola",
                   "colegio", "em", "de referencia", "centro comunitario", "grupo", "de referencia em ensino medio",
                   "intermediaria", "ginasio municipal", "ginasio", "emef", "centro de educacao infantil", "esc", "ee",
                   "e f", "cei", "emei", "ensino fundamental", "ensino medio", "eeief", "eef", "e f", "ens fun",
                   "eem", "eeem", "est ens med", "est ens fund", "ens fund", "mul", "professora", "professor",
                   "eepg", "eemg", "prof", "ensino fundamental")
  stringi::stri_trans_general(x, "Latin-ASCII") %>%
    str_to_lower() %>%
    str_remove_all("\\.") %>%
    str_remove_all("[[:punct:]]") %>%
    str_squish() %>%
    str_remove(paste0("\\b", school_syns, "\\b", collapse = "|")) %>% ##Remove School synonyms
    str_squish()
}

make_tract_centroids <- function(tracts) {
  tracts$centroid  <- sf::st_transform(tracts, 4674) %>%
    sf::st_centroid() %>%
    sf::st_geometry()
  tracts$tract_centroid_long <-  sf::st_coordinates(tracts$centroid)[,1]
  tracts$tract_centroid_lat <-  sf::st_coordinates(tracts$centroid)[,2]
  tracts <- sf::st_drop_geometry(tracts)
  tracts <- data.table(tracts)[, .(code_tract, zone, tract_centroid_lat, tract_centroid_long)]
  setnames(tracts, "code_tract", "setor_code")
  tracts
}

make_muni_centroids <- function(munis){
  munis$centroid  <- sf::st_transform(munis, 4674) %>%
    sf::st_centroid() %>%
    sf::st_geometry()
  munis$muni_centroid_long <-  sf::st_coordinates(munis$centroid)[,1]
  munis$muni_centroid_lat <-  sf::st_coordinates(munis$centroid)[,2]
  munis <- sf::st_drop_geometry(munis)
  munis <- data.table(munis)[, .(code_muni, muni_centroid_lat, muni_centroid_long)]
  setnames(munis, "code_muni", "id_munic_7")
  munis
}

clean_cnefe <- function(cnefe, muni_ids, tract_centroids){

  setnames(cnefe, names(cnefe),
           janitor::make_clean_names(names(cnefe))) #make nicer names


  ##Pad administrative codes to proper length
  cnefe[, cod_municipio := str_pad(cod_municipio,
                                   width = 5, side = "left", pad = "0")]
  cnefe[, cod_distrito := str_pad(cod_distrito,
                                  width = 2, side = "left", pad = "0")]
  cnefe[, cod_setor := str_pad(cod_setor,
                               width = 6, side = "left", pad = "0")]
  cnefe[, setor_code := paste0(cod_uf, cod_municipio, cod_distrito, cod_setor)]
  cnefe[, c("cod_distrito", "cod_setor") := NULL]


  ## Create address variable
  cnefe[, num_endereco_char := fifelse(num_endereco == 0,
                                       dsc_modificador, as.character(num_endereco)) ]
  cnefe[, dsc_modificador_nosn := fifelse(dsc_modificador != "SN", dsc_modificador, "")]
  cnefe[,  address := str_squish(paste(nom_tipo_seglogr,
                                       nom_titulo_seglogr,
                                       nom_seglogr,
                                       num_endereco_char,
                                       dsc_modificador_nosn))]
  cnefe[,  street := str_squish(paste(nom_tipo_seglogr,
                                      nom_titulo_seglogr,
                                      nom_seglogr))]
  cnefe[, c("nom_tipo_seglogr", "nom_titulo_seglogr",
            "num_endereco_char", "num_endereco", "nom_seglogr",
            "dsc_modificador_nosn", "dsc_modificador") := NULL]

  ##ADD NAs where data is missing
  cnefe[val_longitude == "", val_longitude := NA]
  cnefe[val_latitude == "", val_latitude := NA]
  cnefe[dsc_estabelecimento == "", dsc_estabelecimento := NA]

  ##Remove extraneous white space from dsc_estabelecimento
  cnefe[, dsc_estabelecimento := str_squish(dsc_estabelecimento)]

  ##Add municipality code and identifiers
  cnefe[, id_munic_7 := as.numeric(paste0(cod_uf, cod_municipio))]
  cnefe <- muni_ids[, .(id_munic_7, id_TSE, municipio, estado_abrev)][cnefe, on = "id_munic_7"]

  gc()
  ##Merge in especie labels
  especie_labs <- data.table(especie = 1:7,
                             especie_lab = c("domicílio particular", "domicílio coletivo",
                                             "estabeleciemento agropecuário",
                                             "estabelecimento de ensino",
                                             "estabelecimento de saúde",
                                             "estabeleciemento de outras finalidades",
                                             "edificação em construção"))
  cnefe <- especie_labs[cnefe, on = "especie"]


  ##Make smaller CNEFE dataset
  addr <- cnefe[, .(id_munic_7, id_TSE, municipio, setor_code, especie_lab, street,
                    address, dsc_localidade, dsc_estabelecimento, val_longitude, val_latitude)]
  setnames(addr, c("dsc_localidade", "dsc_estabelecimento", "val_longitude", "val_latitude"),
           c("bairro", "desc", "cnefe_long", "cnefe_lat"))
  rm(cnefe)
  gc()

  # Convert degree longitude and latitude to decimal minutes
  addr$cnefe_long <-  biogeo::dms2dd(as.numeric(substr(addr$cnefe_long, 1, 2)),
                                     as.numeric(substr(addr$cnefe_long, 4, 5)),
                                     as.numeric(substr(addr$cnefe_long, 7, 13)), "W")
  addr$cnefe_lat <-  biogeo::dms2dd(as.numeric(substr(addr$cnefe_lat, 1, 2)),
                                    as.numeric(substr(addr$cnefe_lat, 4, 5)),
                                    as.numeric(substr(addr$cnefe_lat, 7, 13)), "S")

  ##Imputation of longitude and latitude for CNEFE
  ##Merge in centroids
  addr <- tract_centroids[addr, on = "setor_code"]
  #Note that some setor codes in the CNEFE data appear to bad

  ##If longitude and latitude is missing, but tract centroid is available, use tract centroid
  addr$cnefe_impute_tract_centroid <- as.numeric(is.na(addr$cnefe_lat)  & (is.na(addr$tract_centroid_lat) == FALSE))

  ## If longitude and latitude is missing and tract centroid is missing, drop
  addr <- addr[(is.na(addr$cnefe_lat)  & (is.na(addr$tract_centroid_lat) == TRUE)) == FALSE]

  addr[cnefe_impute_tract_centroid == 1, cnefe_long := tract_centroid_long]
  addr[cnefe_impute_tract_centroid == 1, cnefe_lat := tract_centroid_lat]

  #Normalize name
  addr[, norm_address := normalize_address(address)]
  addr[, norm_bairro := normalize_address(bairro)]
  addr[, norm_street := normalize_address(street)]

  addr

}

clean_inep <- function(inep_data, inep_codes){
  inep_data <- inep_data %>%
    janitor::clean_names()
  inep_data <- inep_data[!is.na(latitude), .(escola, codigo_inep, uf,
                                             municipio, endereco, latitude, longitude)]
  inep_data <- inep_codes[inep_data, on = "codigo_inep"]

  #normalize name and address
  inep_data[, norm_school := normalize_school(escola)]
  inep_data[, norm_addr := normalize_address(endereco)]
  #remove cep and municipality
  inep_data[, norm_addr := str_remove(norm_addr, " ([0-9]{5}).*")]
  inep_data

}

clean_locais18 <- function(locais18, muni_ids){

  locais18 <- janitor::clean_names(locais18)


  locais18 <- unique(locais18[, .(sgl_uf, cod_localidade_ibge, zona, num_local, local_votacao,
                                  endereco, bairro_local_vot, latitude_local, longitude_local)])
  locais18[, local_id := 1:nrow(locais18)]
  #Normaize name and address
  locais18[, normalized_name := normalize_school(local_votacao)]
  locais18[, normalized_addr := paste(normalize_address(endereco), normalize_address(bairro_local_vot))]
  locais18[, normalized_st := normalize_address(endereco)]
  locais18[, normalized_bairro := normalize_address(bairro_local_vot)]

  #Merge in municipality name
  locais18 <- left_join(locais18, select(muni_ids, id_munic_7, municipio),
                        by = c("cod_localidade_ibge" = "id_munic_7"))

  locais18 <- locais18[sgl_uf != "ZZ"]
  locais18[, latitude_local := as.numeric(str_replace(latitude_local, ",", "."))]
  locais18[, longitude_local := as.numeric(str_replace(longitude_local, ",", "."))]


  #  locais18[, latitude_local := NULL]
  #  locais18[, longitude_local := NULL]
  locais18
}

clean_agro_cnefe <- function(agro_cnefe, muni_ids){

  ##Pad administrative codes to proper length
  agro_cnefe[, cod_municipio := str_pad(cod_municipio,
                                        width = 5, side = "left", pad = "0")]

  ## Create address variable
  agro_cnefe[, num_endereco_chr := fifelse(is.na(num_endereco), "",
                                           as.character(num_endereco))]

  agro_cnefe[,  address := str_squish(paste(nom_tipo_seglogr,
                                            nom_titulo_seglogr,
                                            nom_seglogr,
                                            num_endereco_chr,
                                            dsc_modificador))]
  agro_cnefe[,  street := str_squish(paste(nom_tipo_seglogr,
                                           nom_titulo_seglogr,
                                           nom_seglogr))]
  agro_cnefe[, c("nom_tipo_seglogr", "nom_titulo_seglogr",
                 "num_endereco_chr", "num_endereco", "nom_seglogr",
                 "dsc_modificador") := NULL]

  ##Remove extraneous white space from dsc_localidade
  agro_cnefe[, dsc_localidade := str_squish(dsc_localidade)]

  ##Add municipality code and identifiers
  agro_cnefe[, id_munic_7 := as.numeric(paste0(cod_uf, cod_municipio))]
  agro_cnefe <- muni_ids[, .(id_munic_7, id_TSE, municipio, estado_abrev)][agro_cnefe, on = "id_munic_7"]

  agro_cnefe[, c("cod_uf", "cod_subdistrito") := NULL]

  agro_cnefe[,.(n = .N), by = dsc_localidade][order(-n)]

  #Normalize name
  agro_cnefe[, norm_address := normalize_address(address)]
  agro_cnefe[, norm_bairro := normalize_address(dsc_localidade)]
  agro_cnefe[, norm_street := normalize_address(street)]

  agro_cnefe

}


make_muni_shp <- function(muni_shp, semiarid05_shp){

  muni_shp$semiarid05 <- ifelse(muni_shp$code_muni %in% semiarid05_shp$code_muni,
                                TRUE, FALSE)
  #create a dummy for neihboring semi arid zone
  muni_shp$semiarid05_nbr <- ifelse(sapply(st_intersects(muni_shp, semiarid05_shp),
                                           length) > 0 & muni_shp$semiarid05 == FALSE,
                                    TRUE, FALSE)
  #create a dummy for being on the border of the semi-arid zone
  semiarid05_shp$semiarid05_brdr <- ifelse(sapply(st_intersects(semiarid05_shp,
                                                                filter(muni_shp, semiarid05 == FALSE)),
                                                  length) > 0, TRUE,FALSE)

  muni_shp$semiarid05_brdr <- ifelse(muni_shp$code_muni %in%
                                       semiarid05_shp$code_muni[semiarid05_shp$semiarid05_brdr == TRUE],
                                     TRUE, FALSE)

  #create factor variable for semi-arid status
  muni_shp$sample_status <- case_when(
    muni_shp$semiarid05_brdr == TRUE ~ "Semi-Arid Border",
    muni_shp$semiarid05_nbr == TRUE ~ "Semi-Arid Neighbor",
    muni_shp$semiarid05 == TRUE & muni_shp$semiarid05_brdr == FALSE ~ "Semi-Arid",
    TRUE ~ "Not Semi-Arid"
  )

  #Remove Fernando de Noronha
  muni_shp <- filter(muni_shp, code_muni != 2605459)

  muni_shp
}


add_dist_to_saz <- function(geocoded_locais, muni_shp, tract_shp){

  geocoded_locais$longitude <- geocoded_locais$long
  geocoded_locais$latitude <- geocoded_locais$lat

  geocoded_locais <- st_as_sf(geocoded_locais[!is.na(long)], coords = c("long", "lat"),
                              crs = 4674)

  nd_locais <- filter(geocoded_locais,
                      sgl_uf %in% c("AL", "BA", "CE", "MG", "PB", "PE", "PI", "RN", "SE", "MG")) %>%
    mutate(semiarid05 = ifelse(cod_localidade_ibge %in%
                                 muni_shp$code_muni[muni_shp$semiarid05 == TRUE], TRUE, FALSE))

  tract_locais <- st_join(nd_locais, tract_shp,
                          join = st_within, left = TRUE) %>%
    st_drop_geometry() %>%
    select(local_id, code_tract, zone)

  nd_locais <- left_join(nd_locais, tract_locais)

  #Calculate Distance to SAZ Border
  dist_in_border <- apply(st_distance(nd_locais,
                                      filter(muni_shp, sample_status %in% c("Semi-Arid Neighbor"))), 1, min) / 1000
  dist_nb_border <- apply(st_distance(nd_locais,
                                      filter(muni_shp, sample_status %in% c("Semi-Arid Border"))), 1, min) / 1000
  nd_locais$dist_saz_brdr <- ifelse(nd_locais$semiarid05 == FALSE,
                                    dist_nb_border, dist_in_border)

  nd_locais <- st_drop_geometry(nd_locais)

  select(nd_locais, year, sgl_uf, cod_localidade_ibge, zona, num_local, local_id,
         municipio, semiarid05, dist_saz_brdr, code_tract, rural_urban_tract = zone, longitude, latitude)
}

clean_2018_elec <- function(dir){
  results18 <- lapply(fs::dir_ls(dir), fread,
                      select = c("SG_ UF", "NR_TURNO", "CD_MUNICIPIO", "NM_MUNICIPIO",
                                 "NR_ZONA", "NR_SECAO", "NR_LOCAL_VOTACAO",
                                 "DS_CARGO_PERGUNTA", "SG_PARTIDO", "QT_APTOS",
                                 "QT_COMPARECIMENTO", "QT_ABSTENCOES", "DS_TIPO_URNA",
                                 "DS_TIPO_VOTAVEL", "NR_VOTAVEL", "NM_VOTAVEL", "QT_VOTOS"),
                      encoding = "Latin-1") %>%
    rbindlist()

  setnames(results18, old = names(results18), new = janitor::make_clean_names(names(results18)))

  results18_loc <- results18[, .(qt_votos = sum(qt_votos),
                                 qt_aptos = sum(qt_aptos),
                                 qt_comparecimento = sum(qt_comparecimento),
                                 qt_abstencoes = sum(qt_abstencoes)),
                             by = .(sg_uf, cd_municipio, nr_turno, nr_zona, nr_local_votacao,
                                    ds_cargo_pergunta, sg_partido,
                                    nm_votavel, nr_votavel)]

  results18_loc[, local_code := paste(sg_uf, cd_municipio, nr_zona, nr_local_votacao)]
  results18_loc
}



clean_2014_elec <- function(dir){

  elec_result14 <- lapply(fs::dir_ls(dir, regexp = "1t"), fread,
                          encoding = "Latin-1",
                          select = c("V5", "V7", "V8", "V9", "V10", "V12",
                                     "V13", "V14", "V16", "V17", "V18",
                                     "V22", "V23", "V24")) %>%
    rbindlist() %>%
    rename("sg_uf" = "V5", "ds_cargo_pergunta" = "V7", "nr_zona" = "V8", "nr_secao" = "V9",
           "nr_local_votacao" = "V10", "sg_partido" = "V12", "cd_municipio" = "V13",
           "nm_municipio" = "V14", "qt_aptos" = "V16", "qt_abstencoes" = "V17",
           "qt_comparecimento" = "V18", "nr_votavel" = "V22", "nm_votavel" = "V23",
           "qt_votos" = "V24")
  elec_result14[,nr_turno := 1]


  results14_loc <- elec_result14[, .(qt_votos = sum(qt_votos),
                                     qt_aptos = sum(qt_aptos),
                                     qt_comparecimento = sum(qt_comparecimento),
                                     qt_abstencoes = sum(qt_abstencoes)),
                                 by = .(sg_uf, cd_municipio, nr_turno, nr_zona, nr_local_votacao,
                                        ds_cargo_pergunta, sg_partido,
                                        nm_votavel, nr_votavel)]

  results14_loc[, local_code := paste(sg_uf, cd_municipio, nr_zona,  nr_local_votacao)]


  elec_result14_2t <- lapply(fs::dir_ls(dir, regexp = "2t"), fread,
                             encoding = "Latin-1",
                             select = c("V5", "V7", "V8", "V9", "V10", "V12",
                                        "V13", "V14", "V16", "V17", "V18",
                                        "V22", "V23", "V24")) %>%
    rbindlist() %>%
    rename("sg_uf" = "V5", "ds_cargo_pergunta" = "V7", "nr_zona" = "V8", "nr_secao" = "V9",
           "nr_local_votacao" = "V10", "sg_partido" = "V12", "cd_municipio" = "V13",
           "nm_municipio" = "V14", "qt_aptos" = "V16", "qt_abstencoes" = "V17",
           "qt_comparecimento" = "V18", "nr_votavel" = "V22", "nm_votavel" = "V23",
           "qt_votos" = "V24")
  elec_result14_2t[,nr_turno := 2]


  results14_loc_2t <- elec_result14_2t[, .(qt_votos = sum(qt_votos),
                                           qt_aptos = sum(qt_aptos),
                                           qt_comparecimento = sum(qt_comparecimento),
                                           qt_abstencoes = sum(qt_abstencoes)),
                                       by = .(sg_uf, cd_municipio, nr_turno, nr_zona, nr_local_votacao,
                                              ds_cargo_pergunta, sg_partido,
                                              nm_votavel, nr_votavel)]

  results14_loc_2t[, local_code := paste(sg_uf, cd_municipio, nr_zona, nr_local_votacao)]

  rbind(results14_loc, results14_loc_2t)
}


clean_2012_elec <- function(dir){
  elec_result12 <- lapply(fs::dir_ls(dir, regexp = "1t"), fread,
                          encoding = "Latin-1",
                          select = c("V5", "V7", "V8", "V9", "V10", "V12",
                                     "V13", "V14", "V16", "V17", "V18",
                                     "V23", "V24", "V25")) %>%
    rbindlist() %>%
    rename("sg_uf" = "V5", "ds_cargo_pergunta" = "V7", "nr_zona" = "V8", "nr_secao" = "V9",
           "nr_local_votacao" = "V10", "sg_partido" = "V12", "cd_municipio" = "V13",
           "nm_municipio" = "V14", "qt_aptos" = "V16", "qt_abstencoes" = "V17",
           "qt_comparecimento" = "V18", "nr_votavel" = "V23", "nm_votavel" = "V24",
           "qt_votos" = "V25")
  elec_result12[,nr_turno := 1]


  results12_loc <- elec_result12[, .(qt_votos = sum(qt_votos),
                                     qt_aptos = sum(qt_aptos),
                                     qt_comparecimento = sum(qt_comparecimento),
                                     qt_abstencoes = sum(qt_abstencoes)),
                                 by = .(sg_uf, cd_municipio, nr_turno, nr_zona, nr_local_votacao,
                                        ds_cargo_pergunta, sg_partido,
                                        nm_votavel, nr_votavel)]

  results12_loc[, local_code := paste(sg_uf, cd_municipio, nr_zona, nr_local_votacao)]
  results12_loc
}



clean_2016_elec <- function(dir){
  elec_result16 <- lapply(fs::dir_ls(dir, regexp = "1t"), fread,
                          encoding = "Latin-1",
                          select = c("V5", "V7", "V8", "V9", "V10", "V12",
                                     "V13", "V14", "V16", "V17", "V18",
                                     "V22", "V23", "V24")) %>%
    rbindlist() %>%
    rename("sg_uf" = "V5", "ds_cargo_pergunta" = "V7", "nr_zona" = "V8", "nr_secao" = "V9",
           "nr_local_votacao" = "V10", "sg_partido" = "V12", "cd_municipio" = "V13",
           "nm_municipio" = "V14", "qt_aptos" = "V16", "qt_abstencoes" = "V17",
           "qt_comparecimento" = "V18", "nr_votavel" = "V22", "nm_votavel" = "V23",
           "qt_votos" = "V24")
  elec_result16[,nr_turno := 1]


  results16_loc <- elec_result16[, .(qt_votos = sum(qt_votos),
                                     qt_aptos = sum(qt_aptos),
                                     qt_comparecimento = sum(qt_comparecimento),
                                     qt_abstencoes = sum(qt_abstencoes)),
                                 by = .(sg_uf, cd_municipio, nr_turno, nr_zona, nr_local_votacao,
                                        ds_cargo_pergunta, sg_partido,
                                        nm_votavel, nr_votavel)]

  results16_loc[, local_code := paste(sg_uf, cd_municipio, nr_zona, nr_local_votacao)]
  results16_loc
}

combine_locais <- function(muni_ids, locais16, locais14, locais12, locais18){
  locais18$year <- 2018
  locais16 <- locais16 %>%
    left_join(select(muni_ids, cd_localidade_tse = id_TSE, cod_localidade_ibge = id_munic_7)) %>%
    select(sgl_uf = sg_uf, cod_localidade_ibge, zona = nr_zona, num_local = nr_locvot, local_votacao = nm_locvot,
           endereco = ds_endereco, bairro_local_vot = ds_bairro, normalized_name, normalized_addr,
           normalized_st, normalized_bairro, municipio = nm_localidade) %>%
    mutate(year = 2016)

  locais14 <- locais14 %>%
    select(sgl_uf = uf,  cod_localidade_ibge = cod_mun_ibge, zona = num_zona, num_local = num_local_votacao,
           local_votacao = nome_local_votacao, endereco = descricao_endereco, bairro_local_vot = descricao_bairro,
           normalized_name, normalized_addr, normalized_st,
           normalized_bairro, municipio = nome_municipio) %>%
    mutate(year = 2014)

  locais12 <- locais12 %>%
    select(sgl_uf = uf,  cod_localidade_ibge = cod_mun_ibge, zona = num_zona, num_local = num_local_votacao,
           local_votacao = nome_local_votacao, endereco = descricao_endereco, bairro_local_vot = descricao_bairro,
           normalized_name, normalized_addr, normalized_st,
           normalized_bairro, municipio = nome_municipio) %>%
    mutate(year = 2012)

  locais <- bind_rows(locais18, locais16, locais14, locais12)
  locais$local_id <- 1:nrow(locais)
  locais
}

make_cisterns_rd_data <- function(cisterns_setores, muni_shp, tract_centroids, setor_census_pop){

  setor_census_pop$cod_setor <- as.character(setor_census_pop$cod_setor)

  sazbrdr_tract_centroids <- tract_centroids %>%
    mutate(code_muni = as.numeric(substr(setor_code, 1, 7))) %>%
    filter(code_muni %in%
             unique(muni_shp$code_muni[muni_shp$sample_status %in% c("Semi-Arid Border", "Semi-Arid Neighbor")])) %>%
    left_join(select(ungroup(cisterns_setores), setor_code = code_tract, num_cisterns)) %>%
    mutate(num_cisterns = ifelse(is.na(num_cisterns), 0, num_cisterns)) %>%
    left_join(select(st_drop_geometry(muni_shp), code_muni, sample_status, semiarid05)) %>%
    left_join(select(setor_census_pop, setor_code = cod_setor, situacao_setor, households)) %>%
    st_as_sf(coords = c("tract_centroid_long", "tract_centroid_lat"), crs = 4674) %>%
    left_join(select(tract_centroids, setor_code, tract_centroid_lat, tract_centroid_long))

  #Calculate Distance to SAZ Border
  dist_in_border <- apply(st_distance(sazbrdr_tract_centroids,
                                      filter(muni_shp, sample_status %in% c("Semi-Arid Neighbor"))), 1, min) / 1000
  dist_nb_border <- apply(st_distance(sazbrdr_tract_centroids,
                                      filter(muni_shp, sample_status %in% c("Semi-Arid Border"))), 1, min) / 1000
  sazbrdr_tract_centroids$dist_saz_brdr <- ifelse(sazbrdr_tract_centroids$semiarid05 == FALSE,
                                                  dist_nb_border, dist_in_border)

  sazbrdr_tract_centroids <- st_drop_geometry(sazbrdr_tract_centroids)
  sazbrdr_tract_centroids
}

