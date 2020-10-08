match_inep_muni <- function(locais_muni, inep_muni){
  #this function operates on a single municipality

  if(nrow(inep_muni) == 0){
    return(NULL)
  }

  #Match on inep name
  name_inep_dists <- stringdist::stringdistmatrix(locais_muni$normalized_name, inep_muni$norm_school, method = "lv")
  #Normalize for string length
  name_inep_dists <- name_inep_dists / pmax(nchar(locais_muni$normalized_name), nchar(inep_muni$norm_school))
  mindist_name_inep <- apply(name_inep_dists, 1, min)
  match_inep_name <- inep_muni$norm_school[apply(name_inep_dists, 1, which.min)]
  match_long_inep_name <- inep_muni$longitude[apply(name_inep_dists, 1, which.min)]
  match_lat_inep_name <- inep_muni$latitude[apply(name_inep_dists, 1, which.min)]

  #Match on inep address
  addr_inep_dists <- stringdist::stringdistmatrix(locais_muni$normalized_addr, inep_muni$norm_addr,  method = "lv")
  #Normalize for string length
  addr_inep_dists <- addr_inep_dists / pmax(nchar(locais_muni$normalized_addr), nchar(inep_muni$norm_addr))
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
  name_schools_cnefe_dists <- name_schools_cnefe_dists / pmax(nchar(locais_muni$normalized_name), nchar(schools_cnefe_muni$norm_desc))
  mindist_name_schools_cnefe <- apply(name_schools_cnefe_dists, 1, min)
  match_schools_cnefe_name <- schools_cnefe_muni$norm_desc[apply(name_schools_cnefe_dists, 1, which.min)]
  match_long_schools_cnefe_name <- schools_cnefe_muni$cnefe_long[apply(name_schools_cnefe_dists, 1, which.min)]
  match_lat_schools_cnefe_name <- schools_cnefe_muni$cnefe_lat[apply(name_schools_cnefe_dists, 1, which.min)]

  #Match on schools_cnefe address
  addr_schools_cnefe_dists <- stringdist::stringdistmatrix(locais_muni$normalized_addr, schools_cnefe_muni$norm_addr,  method = "lv")
  #Normalize for string length
  addr_schools_cnefe_dists <- addr_schools_cnefe_dists / pmax(nchar(locais_muni$normalized_addr), nchar(schools_cnefe_muni$norm_addr))
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
  st_cnefe_dists <- st_cnefe_dists / pmax(nchar(locais_muni$normalized_st), nchar(cnefe_st_muni$norm_street))

  mindist_st_cnefe <- apply(st_cnefe_dists, 1, min)
  match_st_cnefe <- cnefe_st_muni$norm_street[apply(st_cnefe_dists, 1, which.min)]
  match_long_st_cnefe <- cnefe_st_muni$long[apply(st_cnefe_dists, 1, which.min)]
  match_lat_st_cnefe <- cnefe_st_muni$lat[apply(st_cnefe_dists, 1, which.min)]
  local_id <- locais_muni$local_id

  locais_muni[normalized_bairro == "", normalized_bairro := normalized_st]
  bairro_cnefe_dists <- stringdist::stringdistmatrix(locais_muni$normalized_bairro, cnefe_bairro_muni$norm_bairro, method = "lv")
  #Normalize for string length
  bairro_cnefe_dists <- bairro_cnefe_dists / pmax(nchar(locais_muni$normalized_bairro), nchar(cnefe_bairro_muni$norm_bairro))
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
  st_agrocnefe_dists <- st_agrocnefe_dists / pmax(nchar(locais_muni$normalized_st), nchar(agrocnefe_st_muni$norm_street))
  mindist_st_agrocnefe <- apply(st_agrocnefe_dists, 1, min)
  match_st_agrocnefe <- agrocnefe_st_muni$norm_street[apply(st_agrocnefe_dists, 1, which.min)]
  match_long_st_agrocnefe <- agrocnefe_st_muni$long[apply(st_agrocnefe_dists, 1, which.min)]
  match_lat_st_agrocnefe <- agrocnefe_st_muni$lat[apply(st_agrocnefe_dists, 1, which.min)]
  local_id <- locais_muni$local_id

  locais_muni[normalized_bairro == "", normalized_bairro := normalized_st]
  bairro_agrocnefe_dists <- stringdist::stringdistmatrix(locais_muni$normalized_bairro, agrocnefe_bairro_muni$norm_bairro, method = "lv")
  #Normalize for string length
  bairro_agrocnefe_dists <- bairro_agrocnefe_dists / pmax(nchar(locais_muni$normalized_bairro), nchar(agrocnefe_bairro_muni$norm_bairro))
  mindist_bairro_agrocnefe <- apply(bairro_agrocnefe_dists, 1, min)
  match_bairro_agrocnefe <- agrocnefe_bairro_muni$norm_bairro[apply(bairro_agrocnefe_dists, 1, which.min)]
  match_long_bairro_agrocnefe <- agrocnefe_bairro_muni$long[apply(bairro_agrocnefe_dists, 1, which.min)]
  match_lat_bairro_agrocnefe <- agrocnefe_bairro_muni$lat[apply(bairro_agrocnefe_dists, 1, which.min)]

  data.table(local_id, mindist_st_agrocnefe, match_st_agrocnefe, match_long_st_agrocnefe, match_lat_st_agrocnefe,
             mindist_bairro_agrocnefe, match_bairro_agrocnefe, match_long_bairro_agrocnefe, match_lat_bairro_agrocnefe)


}

get_best_string_match <- function(cnefe_stbairro_match, inep_string_match, schools_cnefe_match,
                                  agrocnefe_stbairro_match){
  best_string_match <- left_join(cnefe_stbairro_match, inep_string_match) %>%
    left_join(schools_cnefe_match) %>%
    left_join(agrocnefe_stbairro_match) %>%
    select( local_id, contains("mindist"), contains("match_long"), contains("match_lat")) %>%
    tidyr::pivot_longer(cols = mindist_st_cnefe:match_lat_bairro_agrocnefe) %>%
    mutate(match_type = case_when(str_detect(name, "st_cnefe") ~ "6_st_cnefe",
                                  str_detect(name, "bairro_cnefe") ~ "8_bairro_cnefe",
                                  str_detect(name, "inep_name") ~ "1_inep_name",
                                  str_detect(name, "inep_addr") ~ "2_inep_addr",
                                  str_detect(name, "name_inep") ~ "1_inep_name",
                                  str_detect(name, "addr_inep") ~ "2_inep_addr",
                                  str_detect(name, "name_schools_cnefe") ~ "3_name_schools_cnefe",
                                  str_detect(name, "addr_schools_cnefe") ~ "4_addr_schools_cnefe",
                                  str_detect(name, "schools_cnefe_name") ~ "3_name_schools_cnefe",
                                  str_detect(name, "schools_cnefe_addr") ~ "4_addr_schools_cnefe",
                                  str_detect(name, "st_agrocnefe") ~ "5_st_agrocnefe",
                                  str_detect(name, "bairro_agrocnefe") ~ "7_bairro_agrocnefe"),
           variable = case_when(str_detect(name, "mindist") ~ "mindist",
                                str_detect(name, "long") ~ "long",
                                str_detect(name, "lat") ~ "lat")) %>%
    select(-name) %>%
    tidyr::pivot_wider(names_from = variable, values_from = value) %>%
    ##The following line sets the minimum distance to 0 for school name matches that are close, but not exact
    mutate(mindist_match = ifelse(mindist <= .1 &
                              match_type %in% c("1_inep_name", "2_inep_addr",
                                                "3_name_schools_cnefe", "4_addr_schools_cnefe"),
                            0, mindist)) %>%
    arrange(local_id, mindist_match, match_type) %>%
    select(-mindist_match) %>%
    group_by(local_id) %>%
    slice(1)
  as.data.table(best_string_match)
}

