## String matching and geocoding functions

library(data.table)
library(stringr)
source("R/data_table_utils.R")

match_inep_muni <- function(locais_muni, inep_muni) {
  # Return NULL if inep_muni is empty
  if (nrow(inep_muni) == 0) {
    return(NULL)
  }
  
  # Standardize column names if needed
  if ("normalized_name" %in% names(locais_muni)) {
    setnames(locais_muni, "normalized_name", "norm_name", skip_absent = TRUE)
  }
  if ("normalized_addr" %in% names(locais_muni)) {
    setnames(locais_muni, "normalized_addr", "norm_addr", skip_absent = TRUE)
  }
  
  # Calculate distance matrices
  name_dists <- stringdist::stringdistmatrix(
    locais_muni$norm_name,
    inep_muni$norm_school,
    method = "lv"
  )
  
  # Normalize for string length
  name_dists <- name_dists /
    outer(
      nchar(locais_muni$norm_name),
      nchar(inep_muni$norm_school),
      FUN = "pmax"
    )
  
  addr_dists <- stringdist::stringdistmatrix(
    locais_muni$norm_addr,
    inep_muni$norm_addr,
    method = "lv"
  )
  
  # Normalize for string length
  addr_dists <- addr_dists /
    outer(
      nchar(locais_muni$norm_addr),
      nchar(inep_muni$norm_addr),
      FUN = "pmax"
    )
  
  # Find minimum distances and indices
  name_min_idx <- max.col(-name_dists, ties.method = "first")
  addr_min_idx <- max.col(-addr_dists, ties.method = "first")
  
  n_locais <- nrow(locais_muni)
  
  # Extract results
  result <- data.table(
    local_id = locais_muni$local_id,
    
    # Name matching results
    mindist_name_inep = name_dists[cbind(seq_len(n_locais), name_min_idx)],
    match_inep_name = inep_muni$norm_school[name_min_idx],
    match_long_inep_name = inep_muni$longitude[name_min_idx],
    match_lat_inep_name = inep_muni$latitude[name_min_idx],
    
    # Address matching results
    mindist_addr_inep = addr_dists[cbind(seq_len(n_locais), addr_min_idx)],
    match_inep_addr = inep_muni$norm_addr[addr_min_idx],
    match_long_inep_addr = inep_muni$longitude[addr_min_idx],
    match_lat_inep_addr = inep_muni$latitude[addr_min_idx]
  )
  
  return(result)
}

match_schools_cnefe_muni <- function(locais_muni, schools_cnefe_muni) {
  if (nrow(schools_cnefe_muni) == 0) {
    return(NULL)
  }
  
  # Standardize column names
  if ("normalized_name" %in% names(locais_muni)) {
    setnames(locais_muni, "normalized_name", "norm_name", skip_absent = TRUE)
  }
  if ("normalized_addr" %in% names(locais_muni)) {
    setnames(locais_muni, "normalized_addr", "norm_addr", skip_absent = TRUE)
  }
  
  # Calculate distance matrices
  name_dists <- stringdist::stringdistmatrix(
    locais_muni$norm_name,
    schools_cnefe_muni$norm_est,
    method = "lv"
  )
  
  name_dists <- name_dists /
    outer(
      nchar(locais_muni$norm_name),
      nchar(schools_cnefe_muni$norm_est),
      FUN = "pmax"
    )
  
  addr_dists <- stringdist::stringdistmatrix(
    locais_muni$norm_addr,
    schools_cnefe_muni$norm_addr,
    method = "lv"
  )
  
  addr_dists <- addr_dists /
    outer(
      nchar(locais_muni$norm_addr),
      nchar(schools_cnefe_muni$norm_addr),
      FUN = "pmax"
    )
  
  # Find minimum indices
  name_min_idx <- max.col(-name_dists, ties.method = "first")
  addr_min_idx <- max.col(-addr_dists, ties.method = "first")
  
  n_locais <- nrow(locais_muni)
  
  # Create result
  result <- data.table(
    local_id = locais_muni$local_id,
    
    # Name matching
    mindist_name_scnefe = name_dists[cbind(seq_len(n_locais), name_min_idx)],
    match_scnefe_name = schools_cnefe_muni$norm_est[name_min_idx],
    match_long_scnefe_name = schools_cnefe_muni$longitude[name_min_idx],
    match_lat_scnefe_name = schools_cnefe_muni$latitude[name_min_idx],
    
    # Address matching
    mindist_addr_scnefe = addr_dists[cbind(seq_len(n_locais), addr_min_idx)],
    match_scnefe_addr = schools_cnefe_muni$norm_addr[addr_min_idx],
    match_long_scnefe_addr = schools_cnefe_muni$longitude[addr_min_idx],
    match_lat_scnefe_addr = schools_cnefe_muni$latitude[addr_min_idx]
  )
  
  return(result)
}

merge_datasets <- function(inep_match, scnefe_match, locais_muni, tse_muni, cnefe22_muni) {
  # Standardize column names
  standardize_column_names(locais_muni, inplace = TRUE)
  standardize_column_names(tse_muni, inplace = TRUE)
  standardize_column_names(cnefe22_muni, inplace = TRUE)
  
  # Join datasets
  matching_data <- locais_muni[
    inep_match,
    on = .(local_id),
    nomatch = NA
  ]
  
  matching_data <- scnefe_match[
    matching_data,
    on = .(local_id),
    nomatch = NA
  ]
  
  matching_data <- tse_muni[
    matching_data,
    on = .(local_id),
    nomatch = NA
  ]
  
  matching_data <- cnefe22_muni[
    matching_data,
    on = .(local_id),
    nomatch = NA
  ]
  
  return(matching_data)
}

process_muni_demo <- function(atlas_dt) {
  # Rename columns
  old_new_mapping <- c(
    "pesoRUR" = "peso_rur",
    "Codmun7" = "cod_mun_7", 
    "ANO" = "ano",
    "POP" = "pop"
  )
  
  # Apply renaming
  for (old_name in names(old_new_mapping)) {
    if (old_name %in% names(atlas_dt)) {
      setnames(atlas_dt, old_name, old_new_mapping[[old_name]])
    }
  }
  
  # Transform data
  muni_demo <- atlas_dt[, `:=`(
    logpop = log(pop),
    pct_rural = 100 * peso_rur / pop
  )][ano == 2010, .(
    cod_localidade_ibge = cod_mun_7, 
    logpop, 
    pct_rural
  )]
  
  return(muni_demo)
}

make_model_data <- function(matching_data, muni_demo) {
  # Join using data.table syntax
  model_data <- muni_demo[
    matching_data,
    on = .(cod_localidade_ibge = cd_localidade_tse),
    nomatch = NA
  ]
  
  # Select distance columns
  distance_cols <- c(
    "mindist_name_inep", "mindist_addr_inep",
    "mindist_name_scnefe", "mindist_addr_scnefe"
  )
  
  coord_pairs <- list(
    tse = c("long_tse", "lat_tse"),
    inep_name = c("match_long_inep_name", "match_lat_inep_name"),
    inep_addr = c("match_long_inep_addr", "match_lat_inep_addr"),
    scnefe_name = c("match_long_scnefe_name", "match_lat_scnefe_name"),
    scnefe_addr = c("match_long_scnefe_addr", "match_lat_scnefe_addr"),
    cnefe = c("longitude", "latitude")
  )
  
  # Calculate distances for all coordinate pairs
  for (pair_name in names(coord_pairs)) {
    cols <- coord_pairs[[pair_name]]
    if (all(cols %in% names(model_data))) {
      model_data[, paste0("dist_", pair_name) := sqrt(
        (get(cols[1]) - long_tse)^2 + (get(cols[2]) - lat_tse)^2
      )]
    }
  }
  
  return(model_data)
}

# Wrapper function for processing a single municipality
process_single_muni <- function(muni_code, locais, inep_data, cnefe22, tse_geocoded, muni_demo) {
  # Filter data for this municipality
  locais_muni <- locais[cd_localidade_tse == muni_code]
  inep_muni <- inep_data[co_municipio == muni_code]
  
  # Get schools from CNEFE
  schools_cnefe_muni <- cnefe22[
    cd_localidade_tse == muni_code & cod_especie %in% c(1, 4)
  ]
  
  tse_muni <- tse_geocoded[cd_localidade_tse == muni_code]
  cnefe22_muni <- cnefe22[cd_localidade_tse == muni_code]
  
  # Apply matching functions
  inep_match <- match_inep_muni(locais_muni, inep_muni)
  scnefe_match <- match_schools_cnefe_muni(locais_muni, schools_cnefe_muni)
  
  if (is.null(inep_match) || is.null(scnefe_match)) {
    return(NULL)
  }
  
  # Merge datasets
  matching_data <- merge_datasets(
    inep_match, scnefe_match, locais_muni, tse_muni, cnefe22_muni
  )
  
  # Create model data
  model_data <- make_model_data(matching_data, muni_demo)
  
  return(model_data)
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
    method = "lv"
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
    method = "lv"
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
    method = "lv"
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
    method = "lv"
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


