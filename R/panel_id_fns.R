
calc_shared_words <- function(string, string_vec){
  ##this function calculates the normalized share of words shared between two strings
  ##Note that this disregards word order
  string <- string %>%
    str_extract_all(pattern = "\\w+") %>% unlist()
  n_common_words <- map_int(.x = str_extract_all(string_vec, pattern = "\\w+"),
                            .f = ~ length(intersect(string, .x )))
  max_words <- map_int(.x = str_extract_all(string_vec, pattern = "\\w+"),
                       .f = ~ length(unique(c(string,.x ))))
  prop_common <- n_common_words / max_words
  prop_common
}

match_muni_locais <- function(muni_source, muni_target){
  #function calculates distance metric between a given string and set of candidate words
  shared_words_prop <- map(muni_source$local_string, ~ calc_shared_words(.x, muni_target$local_string))
  tibble(
    local_id = muni_source$local_id,
    local_string = muni_source$local_string,
    local_id_match = map_int(shared_words_prop, ~ muni_target$local_id[which.max(.x)]),
    string_match = map_chr(shared_words_prop, ~ muni_target$local_string[which.max(.x)]),
    word_dist = map_dbl(shared_words_prop, max)
  ) %>%
    arrange(local_id_match, desc(word_dist)) %>%
    group_by(local_id_match) %>% #For multiple matches, use only best match
    mutate(local_id_match = replace(local_id_match, which(word_dist != max(word_dist)), NA),
           string_match = replace(string_match, which(word_dist != max(word_dist)), NA),
           word_dist = replace(word_dist, which(word_dist != max(word_dist)), NA))
}

gen_panel_ids <- function(muni_data, year){
  #This function takes a given start year and finds matches in subsequent years
  group_by(muni_data, ano) %>%
    filter(ano > year) %>%
    nest() %>%
    mutate(matches = map(data, ~ match_muni_locais(filter(muni_data, ano == year), .x))) %>%
    unnest(matches) %>%
    select(-data) %>%
    filter(!is.na(local_id_match)) %>%
    select(ano, panel_id = local_id, local_id = local_id_match, panel_match_prop = word_dist) %>%
    bind_rows(select(filter(muni_data, ano == year), ano, panel_id = local_id, local_id = local_id)) %>%
    arrange(local_id, ano)
}

create_panel_ids <- function(muni_data){
  #This operates on a single municipality
  #This function starts at earliest year, creates panel identifiers and then proceeds to subsequent years.
  #Only locais with at least one match in subsequent years are returned, so locais with no matches are not part of output
  muni_data <- muni_data %>%
    mutate(local_string = paste(normalized_name, normalized_st))
  for(i in sort(unique(muni_data$ano))){
    if(i == max(unique(muni_data$ano))) break
    if(exists("panel_ids") == FALSE){
      panel_ids <- gen_panel_ids(muni_data, i)
      unmatched_locais <- filter(muni_data, local_id %in% panel_ids$local_id == FALSE & ano > i)
      if(nrow(unmatched_locais) <= 1 | length(unique(unmatched_locais$ano)) == 1) break
    }
    if(exists("panel_ids") == TRUE){
      if(i %in% unmatched_locais$ano == FALSE) next
      panel_ids <- bind_rows(panel_ids, gen_panel_ids(unmatched_locais, i))
      unmatched_locais <- filter(muni_data, local_id %in% panel_ids$local_id == FALSE & ano > i)
      if(nrow(unmatched_locais) <= 1 | length(unique(unmatched_locais$ano)) == 1) break
    }
  }
  if(exists("panel_ids") == TRUE){
    arrange(panel_ids, panel_id, ano) %>%
      ungroup()
    }
}

create_panel_ids_munis <- function(locais, prop_match_cutoff){
  panel_ids <- parallel::mclapply(unique(locais$cod_localidade_ibge),
                        function(x){
                          create_panel_ids(filter(locais, cod_localidade_ibge == x))},
                        mc.cores = parallel::detectCores() - 2) %>%
    bind_rows() %>%
    filter(panel_match_prop >= prop_match_cutoff)
  panel_ids
}
