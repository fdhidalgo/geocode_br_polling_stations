## Model Functions for Geocoding Prediction
##
## Functions for training and using gradient boosted tree models (LightGBM)
## to select the best geocoding match from multiple candidate coordinates.
## The model learns from TSE ground truth data to predict which string
## matching result is most likely to be correct.

library(data.table)
library(stringr)

# Offset inside the outcome log-transform so log(distance) is defined at distance 0;
# shared by the forward transform and both inverse paths so they cannot drift apart.
GBM_LOG_OFFSET <- 1e-4

# Melt one match table's (match_long_*, match_lat_*, mindist_*) column triples into one row
# per candidate coordinate, labelled by `types` in column order. `year` tags the source
# vintage; the INEP table spans no single year and passes NULL.
melt_match_candidates <- function(matches, types, year = NULL) {
  n_groups <- sum(startsWith(names(matches), "match_long_"))
  if (n_groups != length(types)) {
    stop(sprintf(
      "melt_match_candidates(): %d coordinate column group(s) but %d label(s); the match table's columns changed.",
      n_groups,
      length(types)
    ))
  }

  long <- melt(
    matches,
    id.vars = "local_id",
    measure.vars = patterns(long = "match_long_", lat = "match_lat_", mindist = "mindist_"),
    variable.name = "type",
    variable.factor = FALSE
  )
  labels <- if (is.null(year)) types else paste0(types, "_", year)
  long[, type := labels[as.integer(type)]]
  long[]
}

# geocodebr returns an exact address match rather than a distance, so its candidates get a
# synthetic mindist ranked by geocoding precision: house number, then street, then
# municipality centroid. Lower is better, matching the string-matching distances.
geocodebr_candidates <- function(geocodebr_match) {
  candidates <- geocodebr_match[
    !is.na(match_lat_geocodebr),
    .(
      local_id,
      type = "geocodebr",
      long = match_long_geocodebr,
      lat = match_lat_geocodebr,
      precision_score = fcase(
        precisao_geocodebr == "numero"     , 3 ,
        precisao_geocodebr == "logradouro" , 2 ,
        precisao_geocodebr == "municipio"  , 1 ,
        default = 0
      )
    )
  ]
  candidates[, mindist := (3 - precision_score) * 0.1]
  candidates[]
}

# Assemble the modeling table: all candidate matches, address/municipal features, TSE distance.
make_model_data <- function(
  cnefe10_stbairro_match,
  cnefe22_stbairro_match,
  schools_cnefe10_match,
  schools_cnefe22_match,
  agrocnefe_stbairro_match,
  inep_string_match,
  geocodebr_match,
  muni_demo,
  muni_area,
  locais,
  tsegeocoded_locais
) {
  # Each source names its coordinate columns after itself, so the tables are melted
  # separately and stacked afterwards rather than row-bound first.
  match_list <- list(
    melt_match_candidates(cnefe10_stbairro_match, c("st_cnefe", "bairro_cnefe"), 2010),
    melt_match_candidates(cnefe22_stbairro_match, c("st_cnefe", "bairro_cnefe"), 2022),
    melt_match_candidates(schools_cnefe10_match, "schools_cnefe_name", 2010),
    melt_match_candidates(schools_cnefe22_match, "schools_cnefe_name", 2022),
    melt_match_candidates(inep_string_match, c("schools_inep_name", "schools_inep_addr")),
    geocodebr_candidates(geocodebr_match)
  )
  # agro match is tolerated empty until re-verified on a full production run
  if (nrow(agrocnefe_stbairro_match) > 0L) {
    match_list <- c(
      match_list,
      list(melt_match_candidates(
        agrocnefe_stbairro_match,
        c("st_agrocnefe", "bairro_agrocnefe"),
        2017
      ))
    )
  }

  # A fresh table, so the pipeline's muni_demo target is not mutated by reference.
  muni_demo <- muni_demo[
    ANO == 2010,
    .(
      cod_localidade_ibge = Codmun7,
      logpop = log(POP),
      pct_rural = 100 * pesoRUR / POP
    )
  ]

  # Address and name features; school_synonyms comes from R/data_cleaning.R.
  addr_features <- locais[, .(local_id, nm_locvot, ds_endereco, ds_bairro, normalized_addr)]
  # The generic school terms are what this feature looks for, so it needs the name before
  # normalize_school() strips them.
  addr_features[, norm_name := normalize_name(nm_locvot)]
  addr_features[, centro := fifelse(grepl("\\bcentro\\b", normalized_addr), 1, 0)]
  addr_features[,
    zona_rural := fifelse(
      grepl("\\brural\\b", ds_endereco, ignore.case = TRUE, useBytes = TRUE) |
        grepl("\\brural\\b", ds_bairro, ignore.case = TRUE, useBytes = TRUE),
      1,
      0
    )
  ]
  addr_features[,
    school := fifelse(
      grepl(paste0("\\b", school_synonyms, "\\b", collapse = "|"), norm_name),
      1,
      0
    )
  ]
  addr_features <- addr_features[, .(
    local_id,
    centro,
    zona_rural,
    school,
    length_norm_name = nchar(norm_name),
    length_norm_addr = nchar(normalized_addr)
  )]

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

build_gbm_workflow <- function(data) {
  # Build the unfitted tunable LightGBM workflow (recipe + spec) used for match selection.

  ## Define the model recipe
  gbm_recipe <- recipes::recipe(
    formula = dist ~ .,
    data = data
  ) |>
    recipes::update_role(cod_localidade_ibge, new_role = "id variable") |>
    recipes::update_role(local_id, new_role = "id variable") |>
    recipes::step_impute_median(logpop, pct_rural, area) |>
    ## Log transform the outcome variable to deal with outliers
    recipes::step_log(recipes::all_outcomes(), offset = GBM_LOG_OFFSET, skip = TRUE)

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

  workflows::workflow() |>
    workflows::add_recipe(gbm_recipe) |>
    workflows::add_model(gbm_spec)
}

train_model <- function(model_data, grid_n, dev_mode) {
  # tune_race_anova needs more than its 3 burn-in resamples, so 4 is the dev-mode floor.
  n_folds <- if (dev_mode) 4 else 10
  message(sprintf("Training model with %d CV folds and grid_n = %d", n_folds, grid_n))

  if (nrow(model_data) == 0) {
    stop("No data available for model training")
  }

  ## Remove data with missing outcome and covariate
  model_data <- model_data[!is.na(dist)]
  model_data <- model_data[!is.na(mindist)]

  if (nrow(model_data) == 0) {
    stop("No data left after filtering missing values")
  }

  ## Split the data into training and testing sets
  splits <- rsample::group_initial_split(
    model_data,
    group = cod_localidade_ibge,
    prop = c(.5)
  )

  training_set <- rsample::training(splits)
  testing_set <- rsample::testing(splits)

  vfolds <- rsample::group_vfold_cv(
    training_set,
    group = cod_localidade_ibge,
    v = n_folds
  )

  ## Build the tunable workflow (recipe + model spec) from the training data.
  gbm_workflow <- build_gbm_workflow(training_set)

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

# Score every candidate match with the fitted model, back-transformed to the distance scale.
get_predictions <- function(trained_model, model_data) {
  fitted <- tune::extract_workflow(trained_model$final_fit)

  ## Make predictions
  model_data$pred_logdist <- predict(fitted, new_data = model_data)$.pred
  ## Transform back to original scale
  model_data$pred_dist <- exp(model_data$pred_logdist) - GBM_LOG_OFFSET

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
