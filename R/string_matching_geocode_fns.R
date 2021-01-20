match_inep_muni <- function(locais_muni, inep_muni){
  #this function operates on a single municipality

  if(nrow(inep_muni) == 0){
    return(NULL)
  }

  #Match on inep name
  name_inep_dists <- stringdist::stringdistmatrix(locais_muni$normalized_name, inep_muni$norm_school, method = "lv")
  #Normalize for string length
  name_inep_dists <- name_inep_dists / outer(nchar(locais_muni$normalized_name), nchar(inep_muni$norm_school), FUN = "pmax")

  mindist_name_inep <- apply(name_inep_dists, 1, min)
  match_inep_name <- inep_muni$norm_school[apply(name_inep_dists, 1, which.min)]
  match_long_inep_name <- inep_muni$longitude[apply(name_inep_dists, 1, which.min)]
  match_lat_inep_name <- inep_muni$latitude[apply(name_inep_dists, 1, which.min)]

  #Match on inep address
  addr_inep_dists <- stringdist::stringdistmatrix(locais_muni$normalized_addr, inep_muni$norm_addr,  method = "lv")
  #Normalize for string length
  addr_inep_dists <- addr_inep_dists / outer(nchar(locais_muni$normalized_addr), nchar(inep_muni$norm_addr), FUN = "pmax")
  mindist_addr_inep <- apply(addr_inep_dists, 1, min)
  match_inep_addr <- inep_muni$norm_addr[apply(addr_inep_dists, 1, which.min)]
  match_long_inep_addr <- inep_muni$longitude[apply(addr_inep_dists, 1, which.min)]
  match_lat_inep_addr <- inep_muni$latitude[apply(addr_inep_dists, 1, which.min)]

  local_id <- locais_muni$local_id

  data.table(local_id, match_inep_name, mindist_name_inep, match_long_inep_name, match_lat_inep_name,
             match_inep_addr, mindist_addr_inep, match_long_inep_addr, match_lat_inep_addr)

}

match_schools_cnefe_muni <- function(locais_muni, schools_cnefe_muni){
  #this function operates on a single municipality

  if(nrow(schools_cnefe_muni) == 0){
    return(NULL)
  }

  #Match on schools_cnefe name
  name_schools_cnefe_dists <- stringdist::stringdistmatrix(locais_muni$normalized_name, schools_cnefe_muni$norm_desc, method = "lv")
  #Normalize for string length
  name_schools_cnefe_dists <- name_schools_cnefe_dists / outer(nchar(locais_muni$normalized_name), nchar(schools_cnefe_muni$norm_desc), FUN = "pmax")

  mindist_name_schools_cnefe <- apply(name_schools_cnefe_dists, 1, min)
  match_schools_cnefe_name <- schools_cnefe_muni$norm_desc[apply(name_schools_cnefe_dists, 1, which.min)]
  match_long_schools_cnefe_name <- schools_cnefe_muni$cnefe_long[apply(name_schools_cnefe_dists, 1, which.min)]
  match_lat_schools_cnefe_name <- schools_cnefe_muni$cnefe_lat[apply(name_schools_cnefe_dists, 1, which.min)]

  #Match on schools_cnefe address
  addr_schools_cnefe_dists <- stringdist::stringdistmatrix(locais_muni$normalized_addr, schools_cnefe_muni$norm_addr,  method = "lv")
  #Normalize for string length
  addr_schools_cnefe_dists <- addr_schools_cnefe_dists / outer(nchar(locais_muni$normalized_addr), nchar(schools_cnefe_muni$norm_addr), FUN = "pmax")

  mindist_addr_schools_cnefe <- apply(addr_schools_cnefe_dists, 1, min)
  match_schools_cnefe_addr <- schools_cnefe_muni$norm_addr[apply(addr_schools_cnefe_dists, 1, which.min)]
  match_long_schools_cnefe_addr <- schools_cnefe_muni$cnefe_long[apply(addr_schools_cnefe_dists, 1, which.min)]
  match_lat_schools_cnefe_addr <- schools_cnefe_muni$cnefe_lat[apply(addr_schools_cnefe_dists, 1, which.min)]

  local_id <- locais_muni$local_id

  data.table(local_id, match_schools_cnefe_name, mindist_name_schools_cnefe,
             match_long_schools_cnefe_name, match_lat_schools_cnefe_name,
             match_schools_cnefe_addr, mindist_addr_schools_cnefe,
             match_long_schools_cnefe_addr, match_lat_schools_cnefe_addr)

}

match_stbairro_cnefe_muni <- function(locais_muni, cnefe_st_muni, cnefe_bairro_muni){
  #this function operates on a single municipality

  if(nrow(cnefe_st_muni) == 0){
    return(NULL)
  }

  locais_muni$normalized_st <- str_replace(locais_muni$normalized_st, ",.*$| sn$", "")
  st_cnefe_dists <- stringdist::stringdistmatrix(locais_muni$normalized_st, cnefe_st_muni$norm_street, method = "lv")
  #Normalize for string length
  st_cnefe_dists <- st_cnefe_dists / outer(nchar(locais_muni$normalized_st), nchar(cnefe_st_muni$norm_street), FUN = "pmax")

  mindist_st_cnefe <- apply(st_cnefe_dists, 1, min)
  match_st_cnefe <- cnefe_st_muni$norm_street[apply(st_cnefe_dists, 1, which.min)]
  match_long_st_cnefe <- cnefe_st_muni$long[apply(st_cnefe_dists, 1, which.min)]
  match_lat_st_cnefe <- cnefe_st_muni$lat[apply(st_cnefe_dists, 1, which.min)]
  local_id <- locais_muni$local_id

  locais_muni[normalized_bairro == "", normalized_bairro := normalized_st]
  bairro_cnefe_dists <- stringdist::stringdistmatrix(locais_muni$normalized_bairro, cnefe_bairro_muni$norm_bairro, method = "lv")
  #Normalize for string length
  bairro_cnefe_dists <- bairro_cnefe_dists / outer(nchar(locais_muni$normalized_bairro), nchar(cnefe_bairro_muni$norm_bairro), FUN = "pmax")

  mindist_bairro_cnefe <- apply(bairro_cnefe_dists, 1, min)
  match_bairro_cnefe <- cnefe_bairro_muni$norm_bairro[apply(bairro_cnefe_dists, 1, which.min)]
  match_long_bairro_cnefe <- cnefe_bairro_muni$long[apply(bairro_cnefe_dists, 1, which.min)]
  match_lat_bairro_cnefe <- cnefe_bairro_muni$lat[apply(bairro_cnefe_dists, 1, which.min)]

  data.table(local_id, mindist_st_cnefe, match_st_cnefe, match_long_st_cnefe, match_lat_st_cnefe,
             mindist_bairro_cnefe, match_bairro_cnefe, match_long_bairro_cnefe, match_lat_bairro_cnefe)

}

match_stbairro_agrocnefe_muni <- function(locais_muni, agrocnefe_st_muni, agrocnefe_bairro_muni){
  #this function operates on a single municipality

  if(nrow(agrocnefe_st_muni) == 0){
    return(NULL)
  }

  locais_muni$normalized_st <- str_replace(locais_muni$normalized_st, ",.*$| sn$", "")
  st_agrocnefe_dists <- stringdist::stringdistmatrix(locais_muni$normalized_st, agrocnefe_st_muni$norm_street, method = "lv")
  #Normalize for string length
  st_agrocnefe_dists <- st_agrocnefe_dists / outer(nchar(locais_muni$normalized_st), nchar(agrocnefe_st_muni$norm_street), FUN = "pmax")
  mindist_st_agrocnefe <- apply(st_agrocnefe_dists, 1, min)
  match_st_agrocnefe <- agrocnefe_st_muni$norm_street[apply(st_agrocnefe_dists, 1, which.min)]
  match_long_st_agrocnefe <- agrocnefe_st_muni$long[apply(st_agrocnefe_dists, 1, which.min)]
  match_lat_st_agrocnefe <- agrocnefe_st_muni$lat[apply(st_agrocnefe_dists, 1, which.min)]
  local_id <- locais_muni$local_id

  locais_muni[normalized_bairro == "", normalized_bairro := normalized_st]
  bairro_agrocnefe_dists <- stringdist::stringdistmatrix(locais_muni$normalized_bairro, agrocnefe_bairro_muni$norm_bairro, method = "lv")
  #Normalize for string length
  bairro_agrocnefe_dists <- bairro_agrocnefe_dists / outer(nchar(locais_muni$normalized_bairro), nchar(agrocnefe_bairro_muni$norm_bairro), FUN = "pmax")

  mindist_bairro_agrocnefe <- apply(bairro_agrocnefe_dists, 1, min)
  match_bairro_agrocnefe <- agrocnefe_bairro_muni$norm_bairro[apply(bairro_agrocnefe_dists, 1, which.min)]
  match_long_bairro_agrocnefe <- agrocnefe_bairro_muni$long[apply(bairro_agrocnefe_dists, 1, which.min)]
  match_lat_bairro_agrocnefe <- agrocnefe_bairro_muni$lat[apply(bairro_agrocnefe_dists, 1, which.min)]

  data.table(local_id, mindist_st_agrocnefe, match_st_agrocnefe, match_long_st_agrocnefe, match_lat_st_agrocnefe,
             mindist_bairro_agrocnefe, match_bairro_agrocnefe, match_long_bairro_agrocnefe, match_lat_bairro_agrocnefe)


}

predict_distance <- function(cnefe_stbairro_match, inep_string_match, schools_cnefe_match,
                                  agrocnefe_stbairro_match, locais, tsegeocoded_locais18,
                                  muni_demo, muni_area){

  cnefe_stbairro <- cnefe_stbairro_match %>%
    select(local_id, contains("long"), contains("lat"), contains("mindist")) %>%
    pivot_longer(-local_id) %>%
    mutate(var = case_when(grepl("long", name) ~ "long",
                           grepl("lat", name) ~ "lat",
                           grepl("mindist", name) ~ "mindist"),
           type = case_when(grepl("_st_", name) ~ "st_cnefe",
                            grepl("_bairro_", name) ~ "bairro_cnefe")) %>%
    pivot_wider(id_cols = c(local_id, type), names_from = var)

  schools_cnefe <- schools_cnefe_match %>%
    select(local_id, contains("long"), contains("lat"), contains("mindist")) %>%
    pivot_longer(-local_id) %>%
    mutate(var = case_when(grepl("long", name) ~ "long",
                           grepl("lat", name) ~ "lat",
                           grepl("mindist", name) ~ "mindist"),
           type = case_when(grepl("_name", name) ~ "schools_cnefe_name",
                            grepl("_addr", name) ~ "schools_cnefe_addr")) %>%
    pivot_wider(id_cols = c(local_id, type), names_from = var)

  agrocnefe_stbairro <- agrocnefe_stbairro_match %>%
    select(local_id, contains("long"), contains("lat"), contains("mindist")) %>%
    pivot_longer(-local_id) %>%
    mutate(var = case_when(grepl("long", name) ~ "long",
                           grepl("lat", name) ~ "lat",
                           grepl("mindist", name) ~ "mindist"),
           type = case_when(grepl("_st_", name) ~ "st_agrocnefe",
                            grepl("_bairro_", name) ~ "bairro_agrocnefe")) %>%
    pivot_wider(id_cols = c(local_id, type), names_from = var)

  inep <- inep_string_match %>%
    select(local_id, contains("long"), contains("lat"), contains("mindist")) %>%
    pivot_longer(-local_id) %>%
    mutate(var = case_when(grepl("long", name) ~ "long",
                           grepl("lat", name) ~ "lat",
                           grepl("mindist", name) ~ "mindist"),
           type = case_when(grepl("_name", name) ~ "schools_inep_name",
                            grepl("_addr", name) ~ "schools_inep_addr")) %>%
    pivot_wider(id_cols = c(local_id, type), names_from = var)

  muni_demo <- muni_demo %>%
    mutate(logpop = log(pop),
           pct_rural = 100 * peso_rur / pop) %>%
    select(cod_localidade_ibge = id_munic_7, logpop, pct_rural)

  school_syns <- c("e m e i", "esc inf", "esc mun", "unidade escolar", "centro educacional", "escola municipal",
                   "colegio estadual", "cmei", "emeif", "grupo escolar", "escola estadual", "erem", "colegio municipal",
                   "centro de ensino infantil", "escola mul", "e m", "grupo municipal", "e e", "creche", "escola",
                   "colegio", "em", "de referencia", "centro comunitario", "grupo", "de referencia em ensino medio",
                   "intermediaria", "ginasio municipal", "ginasio", "emef", "centro de educacao infantil", "esc", "ee",
                   "e f", "cei", "emei", "ensino fundamental", "ensino medio", "eeief", "eef", "e f", "ens fun",
                   "eem", "eeem", "est ens med", "est ens fund", "ens fund", "mul", "professora", "professor",
                   "eepg", "eemg", "prof", "ensino fundamental")

  addr_features <- select(locais, local_id, nm_locvot, ds_endereco, ds_bairro, normalized_addr, normalized_name) %>%
    mutate(norm_name = stringi::stri_trans_general(nm_locvot, "Latin-ASCII") %>%
             str_to_lower() %>%
             str_remove_all("\\.") %>%
             str_remove_all("[[:punct:]]") %>%
             str_squish(),
           centro = ifelse(grepl("\\bcentro\\b", normalized_addr) == TRUE, 1, 0),
           zona_rural = ifelse(grepl("\\brural\\b", ds_endereco, ignore.case = TRUE) == TRUE |
                                 grepl("\\brural\\b", ds_bairro, ignore.case = TRUE) == TRUE, 1, 0),
           school = ifelse(grepl(paste0("\\b", school_syns, "\\b", collapse = "|"), norm_name) == TRUE, 1, 0)) %>%
    select(local_id, centro, zona_rural, school)

  matching_data <- bind_rows(cnefe_stbairro, schools_cnefe, agrocnefe_stbairro, inep) %>%
    left_join(select(locais, local_id, ano, cod_localidade_ibge)) %>%
    filter(!is.na(type) & !is.na(mindist)) %>%
    mutate(type = as.factor(type)) %>%
    left_join(muni_demo) %>%
    left_join(addr_features) %>%
    left_join(muni_area)

  training_set <- left_join(tsegeocoded_locais18, matching_data) %>%
    rowwise() %>%
    mutate(dist = geosphere::distHaversine(p1 = c(long, lat),
                                           p2 = c(tse_long, tse_lat))/1000) %>%
    select(-c(nr_zona, nr_locvot, tse_lat, tse_long, long, lat, ano)) %>%
    filter(!is.na(dist))

  ranger_recipe <-
    recipe(formula = dist ~ ., data = training_set) %>%
    update_role(cod_localidade_ibge, new_role = "id variable") %>%
    update_role(local_id, new_role = "id variable") %>%
    step_medianimpute(logpop, pct_rural, area)

  ranger_spec <-
    rand_forest(mtry = tune(), min_n = tune(), trees = 1000) %>%
    set_mode("regression") %>%
    set_engine("ranger")

  ranger_workflow <-
    workflow() %>%
    add_recipe(ranger_recipe) %>%
    add_model(ranger_spec)

  cvfolds <- vfold_cv(training_set, v = 5)

  ranger_tune <-
    tune_grid(ranger_workflow, resamples = cvfolds, grid = 5, control = control_grid(verbose = TRUE))

  best_rmse <- select_best(ranger_tune, "rmse")

  final_model <- finalize_workflow(ranger_workflow, best_rmse)

  fitted_model <- fit(final_model, data = training_set)

  matching_data$pred_dist <- predict(fitted_model, matching_data)$.pred

  matching_data %>%
    group_by(local_id) %>%
    arrange(local_id, pred_dist) %>%
    select(local_id, match_type = type, mindist, long, lat, pred_dist)
}

