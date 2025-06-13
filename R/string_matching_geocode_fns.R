# Check if memory-efficient mode is enabled
if (isTRUE(getOption("geocode_br.use_memory_efficient", FALSE))) {
  # Source memory-efficient version if enabled
  if (!exists("match_strings_memory_efficient")) {
    source("R/memory_efficient_string_matching.R")
    source("R/string_matching_geocode_fns_memory_efficient.R")
  }
  # The memory-efficient versions will override these functions
} else {
  # Original implementations follow

match_inep_muni <- function(locais_muni, inep_muni) {
  # this function operates on a single municipality

  if (nrow(inep_muni) == 0) {
    return(NULL) # Return NULL if inep_muni is empty
  }

  # Match on inep name
  name_inep_dists <- stringdist::stringdistmatrix(
    locais_muni$normalized_name,
    inep_muni$norm_school,
    method = "jw"
  )
  # Normalize for string length
  name_inep_dists <- name_inep_dists /
    outer(
      nchar(locais_muni$normalized_name),
      nchar(inep_muni$norm_school),
      FUN = "pmax"
    )

  mindist_name_inep <- apply(name_inep_dists, 1, min)
  # Find the minimum distance for each row
  match_inep_name <- inep_muni$norm_school[apply(name_inep_dists, 1, which.min)]
  # Get the matching inep name
  match_long_inep_name <- inep_muni$longitude[apply(
    name_inep_dists,
    1,
    which.min
  )]
  # Get the longitude of the matching inep name
  match_lat_inep_name <- inep_muni$latitude[apply(
    name_inep_dists,
    1,
    which.min
  )]
  # Get the latitude of the matching inep name

  # Match on inep address
  addr_inep_dists <- stringdist::stringdistmatrix(
    locais_muni$normalized_addr,
    inep_muni$norm_addr,
    method = "jw"
  )
  # Normalize for string length
  addr_inep_dists <- addr_inep_dists /
    outer(
      nchar(locais_muni$normalized_addr),
      nchar(inep_muni$norm_addr),
      FUN = "pmax"
    )
  mindist_addr_inep <- apply(addr_inep_dists, 1, min)
  # Find the minimum distance for each row
  match_inep_addr <- inep_muni$norm_addr[apply(addr_inep_dists, 1, which.min)]
  # Get the matching inep address
  match_long_inep_addr <- inep_muni$longitude[apply(
    addr_inep_dists,
    1,
    which.min
  )]
  # Get the longitude of the matching inep address
  match_lat_inep_addr <- inep_muni$latitude[apply(
    addr_inep_dists,
    1,
    which.min
  )]
  # Get the latitude of the matching inep address

  local_id <- locais_muni$local_id

  data.table(
    local_id,
    match_inep_name,
    mindist_name_inep,
    match_long_inep_name,
    match_lat_inep_name,
    match_inep_addr,
    mindist_addr_inep,
    match_long_inep_addr,
    match_lat_inep_addr
  )
  # Return a data.table with the matching results
}

match_schools_cnefe_muni <- function(locais_muni, schools_cnefe_muni) {
  # this function operates on a single municipality

  if (nrow(schools_cnefe_muni) == 0) {
    return(NULL) # Return NULL if there are no school data in that municipality
  }

  # Match on school name
  name_schools_cnefe_dists <- stringdist::stringdistmatrix(
    locais_muni$normalized_name,
    schools_cnefe_muni$norm_desc,
    method = "jw"
  )
  # Normalize for string length
  name_schools_cnefe_dists <- name_schools_cnefe_dists /
    outer(
      nchar(locais_muni$normalized_name),
      nchar(schools_cnefe_muni$norm_desc),
      FUN = "pmax"
    )

  mindist_name_schools_cnefe <- apply(name_schools_cnefe_dists, 1, min)
  match_schools_cnefe_name <- schools_cnefe_muni$norm_desc[apply(
    name_schools_cnefe_dists,
    1,
    which.min
  )]
  match_long_schools_cnefe_name <- schools_cnefe_muni$cnefe_long[apply(
    name_schools_cnefe_dists,
    1,
    which.min
  )]
  match_lat_schools_cnefe_name <- schools_cnefe_muni$cnefe_lat[apply(
    name_schools_cnefe_dists,
    1,
    which.min
  )]

  # Match on school address
  addr_schools_cnefe_dists <- stringdist::stringdistmatrix(
    locais_muni$normalized_addr,
    schools_cnefe_muni$norm_addr,
    method = "jw"
  )
  # Normalize for string length
  addr_schools_cnefe_dists <- addr_schools_cnefe_dists /
    outer(
      nchar(locais_muni$normalized_addr),
      nchar(schools_cnefe_muni$norm_addr),
      FUN = "pmax"
    )

  mindist_addr_schools_cnefe <- apply(addr_schools_cnefe_dists, 1, min)
  match_schools_cnefe_addr <- schools_cnefe_muni$norm_addr[apply(
    addr_schools_cnefe_dists,
    1,
    which.min
  )]
  match_long_schools_cnefe_addr <- schools_cnefe_muni$cnefe_long[apply(
    addr_schools_cnefe_dists,
    1,
    which.min
  )]
  match_lat_schools_cnefe_addr <- schools_cnefe_muni$cnefe_lat[apply(
    addr_schools_cnefe_dists,
    1,
    which.min
  )]

  local_id <- locais_muni$local_id

  data.table(
    local_id,
    match_schools_cnefe_name,
    mindist_name_schools_cnefe,
    match_long_schools_cnefe_name,
    match_lat_schools_cnefe_name,
    match_schools_cnefe_addr,
    mindist_addr_schools_cnefe,
    match_long_schools_cnefe_addr,
    match_lat_schools_cnefe_addr
  )
}

match_stbairro_cnefe_muni <- function(
  locais_muni,
  cnefe_st_muni,
  cnefe_bairro_muni
) {
  # this function operates on a single municipality

  if (nrow(cnefe_st_muni) == 0) {
    return(NULL)
  }

  locais_muni$normalized_st <- str_replace(
    locais_muni$normalized_st,
    ",.*$| sn$",
    ""
  )

  # Calculate the string distance between normalized street names
  st_cnefe_dists <- stringdist::stringdistmatrix(
    locais_muni$normalized_st,
    cnefe_st_muni$norm_street,
    method = "jw"
  )

  # Normalize for string length
  st_cnefe_dists <- st_cnefe_dists /
    outer(
      nchar(locais_muni$normalized_st),
      nchar(cnefe_st_muni$norm_street),
      FUN = "pmax"
    )

  mindist_st_cnefe <- apply(st_cnefe_dists, 1, min)
  match_st_cnefe <- cnefe_st_muni$norm_street[apply(
    st_cnefe_dists,
    1,
    which.min
  )]
  match_long_st_cnefe <- cnefe_st_muni$long[apply(st_cnefe_dists, 1, which.min)]
  match_lat_st_cnefe <- cnefe_st_muni$lat[apply(st_cnefe_dists, 1, which.min)]
  local_id <- locais_muni$local_id

  locais_muni[normalized_bairro == "", normalized_bairro := normalized_st]

  # Calculate the string distance between normalized bairro names
  bairro_cnefe_dists <- stringdist::stringdistmatrix(
    locais_muni$normalized_bairro,
    cnefe_bairro_muni$norm_bairro,
    method = "jw"
  )

  # Normalize for string length
  bairro_cnefe_dists <- bairro_cnefe_dists /
    outer(
      nchar(locais_muni$normalized_bairro),
      nchar(cnefe_bairro_muni$norm_bairro),
      FUN = "pmax"
    )

  mindist_bairro_cnefe <- apply(bairro_cnefe_dists, 1, min)
  match_bairro_cnefe <- cnefe_bairro_muni$norm_bairro[apply(
    bairro_cnefe_dists,
    1,
    which.min
  )]
  match_long_bairro_cnefe <- cnefe_bairro_muni$long[apply(
    bairro_cnefe_dists,
    1,
    which.min
  )]
  match_lat_bairro_cnefe <- cnefe_bairro_muni$lat[apply(
    bairro_cnefe_dists,
    1,
    which.min
  )]

  data.table(
    local_id,
    mindist_st_cnefe,
    match_st_cnefe,
    match_long_st_cnefe,
    match_lat_st_cnefe,
    mindist_bairro_cnefe,
    match_bairro_cnefe,
    match_long_bairro_cnefe,
    match_lat_bairro_cnefe
  )
}

match_stbairro_agrocnefe_muni <- function(
  locais_muni,
  agrocnefe_st_muni,
  agrocnefe_bairro_muni
) {
  # this function operates on a single municipality

  if (nrow(agrocnefe_st_muni) == 0) {
    return(NULL)
  }

  locais_muni$normalized_st <- str_replace(
    locais_muni$normalized_st,
    ",.*$| sn$",
    ""
  )
  # Calculate the string distance between normalized street names
  st_agrocnefe_dists <- stringdist::stringdistmatrix(
    locais_muni$normalized_st,
    agrocnefe_st_muni$norm_street,
    method = "jw"
  )
  # Normalize for string length
  st_agrocnefe_dists <- st_agrocnefe_dists /
    outer(
      nchar(locais_muni$normalized_st),
      nchar(agrocnefe_st_muni$norm_street),
      FUN = "pmax"
    )
  mindist_st_agrocnefe <- apply(st_agrocnefe_dists, 1, min)
  match_st_agrocnefe <- agrocnefe_st_muni$norm_street[apply(
    st_agrocnefe_dists,
    1,
    which.min
  )]
  match_long_st_agrocnefe <- agrocnefe_st_muni$long[apply(
    st_agrocnefe_dists,
    1,
    which.min
  )]
  match_lat_st_agrocnefe <- agrocnefe_st_muni$lat[apply(
    st_agrocnefe_dists,
    1,
    which.min
  )]
  local_id <- locais_muni$local_id

  locais_muni[normalized_bairro == "", normalized_bairro := normalized_st]
  # Calculate the string distance between normalized bairro names
  bairro_agrocnefe_dists <- stringdist::stringdistmatrix(
    locais_muni$normalized_bairro,
    agrocnefe_bairro_muni$norm_bairro,
    method = "jw"
  )
  # Normalize for string length
  bairro_agrocnefe_dists <- bairro_agrocnefe_dists /
    outer(
      nchar(locais_muni$normalized_bairro),
      nchar(agrocnefe_bairro_muni$norm_bairro),
      FUN = "pmax"
    )

  mindist_bairro_agrocnefe <- apply(bairro_agrocnefe_dists, 1, min)
  match_bairro_agrocnefe <- agrocnefe_bairro_muni$norm_bairro[apply(
    bairro_agrocnefe_dists,
    1,
    which.min
  )]
  match_long_bairro_agrocnefe <- agrocnefe_bairro_muni$long[apply(
    bairro_agrocnefe_dists,
    1,
    which.min
  )]
  match_lat_bairro_agrocnefe <- agrocnefe_bairro_muni$lat[apply(
    bairro_agrocnefe_dists,
    1,
    which.min
  )]

  out_data <- data.table(
    local_id,
    mindist_st_agrocnefe,
    match_st_agrocnefe,
    match_long_st_agrocnefe,
    match_lat_st_agrocnefe,
    mindist_bairro_agrocnefe,
    match_bairro_agrocnefe,
    match_long_bairro_agrocnefe,
    match_lat_bairro_agrocnefe
  )
  # Remove "agro" prefix
  setnames(out_data, names(out_data), gsub("agro", "", names(out_data)))
  out_data
}


make_model_data <- function(
  cnefe10_stbairro_match,
  cnefe22_stbairro_match,
  schools_cnefe10_match,
  schools_cnefe22_match,
  agrocnefe_stbairro_match,
  inep_string_match,
  geocodebr_match = NULL,  # New parameter with NULL default for backward compatibility
  muni_demo,
  muni_area,
  locais,
  tsegeocoded_locais
) {
  # Assign the year to each dataset (check for NULL/empty first)
  if (!is.null(cnefe10_stbairro_match) && nrow(cnefe10_stbairro_match) > 0) {
    cnefe10_stbairro_match[, ano := 2010]
  }
  if (!is.null(agrocnefe_stbairro_match) && nrow(agrocnefe_stbairro_match) > 0) {
    agrocnefe_stbairro_match[, ano := 2017]
  }
  if (!is.null(cnefe22_stbairro_match) && nrow(cnefe22_stbairro_match) > 0) {
    cnefe22_stbairro_match[, ano := 2022]
  }
  if (!is.null(schools_cnefe10_match) && nrow(schools_cnefe10_match) > 0) {
    schools_cnefe10_match[, ano := 2010]
  }
  if (!is.null(schools_cnefe22_match) && nrow(schools_cnefe22_match) > 0) {
    schools_cnefe22_match[, ano := 2022]
  }

  # Combine CNEFE neighborhood and address data
  cnefe_list <- list(
    cnefe10_stbairro_match,
    cnefe22_stbairro_match,
    agrocnefe_stbairro_match
  )
  # Remove NULL entries
  cnefe_list <- cnefe_list[!sapply(cnefe_list, is.null)]
  cnefe_list <- cnefe_list[sapply(cnefe_list, nrow) > 0]
  
  if (length(cnefe_list) > 0) {
    cnefe_stbairro_match <- rbindlist(cnefe_list, use.names = TRUE, fill = TRUE)
  } else {
    cnefe_stbairro_match <- data.table()
  }
  
  schools_list <- list(schools_cnefe10_match, schools_cnefe22_match)
  schools_list <- schools_list[!sapply(schools_list, is.null)]
  schools_list <- schools_list[sapply(schools_list, nrow) > 0]
  
  if (length(schools_list) > 0) {
    schools_cnefe_match <- rbindlist(schools_list, use.names = TRUE, fill = TRUE)
  } else {
    schools_cnefe_match <- data.table()
  }

  # Melt the CNEFE data to long format
  if (nrow(cnefe_stbairro_match) > 0) {
    cnefe_stbairro_match <- melt(
      cnefe_stbairro_match,
      id.vars = c("local_id", "ano"),
      measure.vars = patterns(long = "long", lat = "lat", mindist = "mindist"),
      variable.name = "type",
      value.name = "value",
      variable.factor = FALSE
    )
    cnefe_stbairro_match[,
      type := paste0(fifelse(type == 1, "st_cnefe", "bairro_cnefe"), "_", ano)
    ]
    cnefe_stbairro_match[, ano := NULL]
  }

  # Melt the schools CNEFE data to long format
  if (nrow(schools_cnefe_match) > 0) {
    schools_cnefe_match <- melt(
      schools_cnefe_match,
      id.vars = c("local_id", "ano"),
      measure.vars = patterns(long = "long", lat = "lat", mindist = "mindist"),
      variable.name = "type",
      value.name = "value",
      variable.factor = FALSE
    )
    schools_cnefe_match[,
      type := paste0(
        fifelse(type == 1, "schools_cnefe_name", "schools_cnefe_addr"),
        "_",
        ano
      )
    ]
    schools_cnefe_match[, ano := NULL]
  }

  # Melt the INEP data to long format
  if (!is.null(inep_string_match) && nrow(inep_string_match) > 0) {
    inep_string_match <- melt(
      inep_string_match,
      id.vars = c("local_id"),
      measure.vars = patterns(long = "long", lat = "lat", mindist = "mindist"),
      variable.name = "type",
      value.name = "value",
      variable.factor = FALSE
    )
    inep_string_match[,
      type := fifelse(type == 1, "schools_inep_name", "schools_inep_addr")
    ]
  } else {
    inep_string_match <- data.table()
  }
  
  # Process geocodebr data to long format
  if (!is.null(geocodebr_match) && nrow(geocodebr_match) > 0) {
    # Create long format with precision info - use same column names as melted data
    geocodebr_long <- geocodebr_match[!is.na(match_lat_geocodebr), .(
      local_id,
      type = "geocodebr",
      long = match_long_geocodebr,
      lat = match_lat_geocodebr,
      mindist = mindist_geocodebr,  # Always 0 for geocodebr
      # Add precision as a numeric score (higher = better)
      precision_score = fcase(
        precisao_geocodebr == "numero", 3,
        precisao_geocodebr == "logradouro", 2,
        precisao_geocodebr == "municipio", 1,
        default = 0
      )
    )]
    # Adjust mindist to reflect precision (lower = better in the model)
    # Municipality precision gets penalty, street/number precision gets bonus
    geocodebr_long[, mindist := (3 - precision_score) * 0.1]
  } else {
    geocodebr_long <- data.table()
  }

  # Prepare municipal demographic data
  muni_demo[, logpop := log(POP)]
  muni_demo[, pct_rural := 100 * pesoRUR / POP]
  muni_demo <- muni_demo[
    ANO == 2010,
    .(cod_localidade_ibge = Codmun7, logpop, pct_rural)
  ]

  # Define synonyms for school names
  school_syns <- c(
    "e m e i",
    "esc inf",
    "esc mun",
    "unidade escolar",
    "centro educacional",
    "escola municipal",
    "colegio estadual",
    "cmei",
    "emeif",
    "emeief",
    "grupo escolar",
    "escola estadual",
    "erem",
    "colegio municipal",
    "centro de ensino infantil",
    "escola mul",
    "e m",
    "grupo municipal",
    "e e",
    "creche",
    "escola",
    "colegio",
    "em",
    "de referencia",
    "centro comunitario",
    "grupo",
    "de referencia em ensino medio",
    "intermediaria",
    "ginasio municipal",
    "ginasio",
    "emef",
    "centro de educacao infantil",
    "esc",
    "ee",
    "e f",
    "cei",
    "emei",
    "ensino fundamental",
    "ensino medio",
    "eeief",
    "eef",
    "e f",
    "ens fun",
    "eem",
    "eeem",
    "est ens med",
    "est ens fund",
    "ens fund",
    "mul",
    "professora",
    "professor",
    "eepg",
    "eemg",
    "prof",
    "ensino fundamental"
  )

  # Prepare address features
  addr_features <- locais[, .(
    local_id,
    nm_locvot,
    ds_endereco,
    ds_bairro,
    normalized_addr,
    normalized_name
  )]
  addr_features[,
    norm_name := stringi::stri_trans_general(nm_locvot, "Latin-ASCII") |>
      str_to_lower() |>
      str_remove_all("\\.") |>
      str_remove_all("[[:punct:]]") |>
      str_squish()
  ]
  addr_features[,
    centro := fifelse(grepl("\\bcentro\\b", normalized_addr) == TRUE, 1, 0)
  ]
  addr_features[,
    zona_rural := fifelse(
      grepl("\\brural\\b", ds_endereco, ignore.case = TRUE, useBytes = TRUE) ==
        TRUE |
        grepl("\\brural\\b", ds_bairro, ignore.case = TRUE, useBytes = TRUE) ==
          TRUE,
      1,
      0
    )
  ]
  addr_features[,
    school := fifelse(
      grepl(paste0("\\b", school_syns, "\\b", collapse = "|"), norm_name) ==
        TRUE,
      1,
      0
    )
  ]
  addr_features[, length_norm_name := nchar(norm_name)]
  addr_features[, length_norm_addr := nchar(normalized_addr)]

  addr_features <- addr_features[, .(
    local_id,
    centro,
    zona_rural,
    school,
    length_norm_name,
    length_norm_addr
  )]

  # Combine string matching data from multiple sources
  # Build list of non-empty data.tables
  match_list <- list(
    cnefe_stbairro_match,
    schools_cnefe_match,
    inep_string_match,
    geocodebr_long
  )
  # Remove empty data.tables
  match_list <- match_list[sapply(match_list, function(x) !is.null(x) && nrow(x) > 0)]
  
  if (length(match_list) == 0) {
    warning("No matching data available for model training")
    return(NULL)
  }
  
  matching_data <- rbindlist(match_list, use.names = TRUE, fill = TRUE) |>
    merge(
      locais[, .(local_id, ano, cod_localidade_ibge)],
      all.x = TRUE,
      all.y = FALSE
    ) |>
    merge(muni_demo, by = "cod_localidade_ibge", all.x = TRUE, all.y = FALSE) |>
    merge(addr_features, by = "local_id", all.x = TRUE, all.y = FALSE) |>
    merge(muni_area, by = "cod_localidade_ibge", all.x = TRUE, all.y = FALSE)

  matching_data[, area := as.double(area)]
  matching_data[, logpop := as.double(logpop)]
  matching_data[, pct_rural := as.double(pct_rural)]

  # Combine matching data with TSE geocoded data
  model_data <- merge(
    tsegeocoded_locais[, .(local_id, tse_lat, tse_long)],
    matching_data,
    by = "local_id",
    all.x = TRUE,
    all.y = TRUE
  )
  model_data[,
    dist := geosphere::distHaversine(
      cbind(long, lat),
      cbind(tse_long, tse_lat),
      r = 6378.137
    )
  ]
  model_data[, ano := NULL]
  model_data[, tse_lat := NULL]
  model_data[, tse_long := NULL]

  # Filter out rows with missing values
  model_data <- model_data[!is.na(mindist) & !is.na(long) & !is.na(lat)]

  model_data
}

train_model <- function(model_data, grid_n = 10, sample = NULL) {
  # Function to train a model using the provided data

  library(bonsai)

  ## Remove data with missing outcome and covariate
  model_data <- model_data[!is.na(dist)]
  model_data <- model_data[!is.na(mindist)]

  if (is.null(sample) == FALSE) {
    # Sample the data if a sample size is provided
    model_data <- model_data[sample(1:nrow(model_data), sample), ]
  }

  ## Split the data into training and testing sets
  splits <- rsample::group_initial_split(
    model_data,
    group = cod_localidade_ibge,
    prop = c(.5)
  )

  training_set <- rsample::training(splits)
  testing_set <- rsample::testing(splits)

  ## Create a cross-validation plan
  vfolds <- rsample::group_vfold_cv(
    model_data,
    group = cod_localidade_ibge,
    v = 10
  )

  ## Define the model recipe
  gbm_recipe <- recipes::recipe(
    formula = dist ~ .,
    data = training_set
  ) |>
    recipes::update_role(cod_localidade_ibge, new_role = "id variable") |>
    recipes::update_role(local_id, new_role = "id variable") |>
    recipes::step_impute_median(logpop, pct_rural, area) |>
    ## Log transform the outcome variable to deal w ith outliers
    recipes::step_log(recipes::all_outcomes(), offset = .0001, skip = TRUE)

  ## Define the model specification
  gbm_spec <-
    parsnip::boost_tree(
      trees = tune(),
      min_n = tune(),
      mtry = tune(),
      learn_rate = tune(),
      loss_reduction = tune()
    ) |>
    parsnip::set_mode("regression") |>
    parsnip::set_engine("lightgbm", num_leaves = tune())

  gbm_workflow <-
    workflows::workflow() |>
    workflows::add_recipe(gbm_recipe) |>
    workflows::add_model(gbm_spec)

  metrics <- yardstick::metric_set(
    yardstick::rmse,
    yardstick::mae,
    yardstick::rsq
  )

  ### Use racing models to tune hyperparameters
  gbm_tune <- finetune::tune_race_anova(
    gbm_workflow,
    resamples = vfolds,
    grid = grid_n,
    # metrics = metrics,
    control = finetune::control_race(
      verbose_elim = TRUE,
      verbose = TRUE,
      allow_par = FALSE
    )
  )
  best_rmse <- tune::select_best(gbm_tune, metric = "rmse")

  final_model <- tune::finalize_workflow(gbm_workflow, best_rmse)

  final_fit <- tune::last_fit(final_model, split = splits, metrics = metrics)

  list(
    tune_out = gbm_tune,
    final_fit = final_fit
  )
}

get_predictions <- function(trained_model, model_data) {
  fitted <- tune::extract_workflow(trained_model$final_fit)

  ## Make predictions
  model_data$pred_logdist <- predict(fitted, new_data = model_data)$.pred
  ## Transform back to original scale
  model_data$pred_dist <- exp(model_data$pred_logdist) - .0001

  model_data[
    order(local_id, pred_dist),
    .(
      local_id,
      match_type = type,
      mindist,
      long,
      lat,
      dist,
      pred_dist,
      pred_logdist
    )
  ]
}

} # End of else block for non-memory-efficient mode
