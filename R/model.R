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

# The distance quantile the selector predicts and the pipeline publishes: the exported
# bound claims the true error falls below it for this share of stations.
SELECTOR_QUANTILE <- 0.9

# Earth radius in km, so every haversine distance in this file lands on one scale.
EARTH_RADIUS_KM <- 6378.137

# Melt one match table's per-candidate column groups -- coordinates, the whole-string
# distance that selected the candidate, and the four field similarities -- into one row per
# candidate coordinate. `types` maps each match table's column suffix (its names) to the
# candidate type the model knows it by (its values).
#
# The columns are listed rather than matched by prefix because melt() pads a short group
# with NA and assigns group members by position: one missing similarity column would
# silently move another candidate's value onto the wrong row. Listing them makes it an error.
melt_match_candidates <- function(matches, types) {
  cols <- function(prefix) paste0(prefix, "_", names(types))
  # Every candidate in the table must be named, or melt would quietly drop one. mindist_ is
  # the one group every match function emits exactly once per candidate.
  stopifnot(setequal(cols("mindist"), grep("^mindist_", names(matches), value = TRUE)))

  long <- melt(
    matches,
    id.vars = "local_id",
    measure.vars = list(
      long = cols("match_long"),
      lat = cols("match_lat"),
      mindist = cols("mindist"),
      sim_name = cols("sim_name"),
      sim_street = cols("sim_street"),
      sim_bairro = cols("sim_bairro"),
      sim_addr = cols("sim_addr")
    ),
    variable.name = "type",
    variable.factor = FALSE
  )
  long[, type := unname(types)[as.integer(type)]]
  long[]
}

# geocodebr scores a candidate by an uncertainty radius in km, not a string distance, so it
# reports on its own axis and leaves `mindist` to the string-matched sources.
geocodebr_candidates <- function(geocodebr_match) {
  candidates <- geocodebr_match[
    !is.na(match_lat_geocodebr),
    .(
      local_id,
      type = "geocodebr",
      long = match_long_geocodebr,
      lat = match_lat_geocodebr,
      # geocodebr resolves an address line, not separable name/street/bairro fields.
      sim_name = NA_real_,
      sim_street = NA_real_,
      sim_bairro = NA_real_,
      sim_addr = sim_addr_geocodebr,
      mindist = NA_real_,
      desvio_km = desvio_km_geocodebr
    )
  ]
  candidates[]
}

# Reference dataset each candidate type is drawn from. Two types out of one dataset often
# agree because they resolve the same record, so agreement is counted across datasets.
CANDIDATE_DATASET <- c(
  st_cnefe_2010 = "cnefe_2010",
  bairro_cnefe_2010 = "cnefe_2010",
  schools_cnefe_name_2010 = "cnefe_2010",
  st_cnefe_2022 = "cnefe_2022",
  bairro_cnefe_2022 = "cnefe_2022",
  schools_cnefe_name_2022 = "cnefe_2022",
  st_agrocnefe_2017 = "agrocnefe_2017",
  bairro_agrocnefe_2017 = "agrocnefe_2017",
  schools_inep_name = "inep",
  schools_inep_addr = "inep",
  geocodebr = "geocodebr"
)

# Measure each candidate against the rest of its station's candidate cloud, so the model can
# tell a location several independent datasets agree on from a lone outlier.
add_consensus_features <- function(candidates) {
  stopifnot(
    "a source contributed two candidates for one station" = anyDuplicated(candidates, by = c("local_id", "type")) == 0L,
    # One missing coordinate would poison its whole station's cloud, not just its own row.
    "consensus needs a coordinate on every candidate" = !anyNA(candidates$long) && !anyNA(candidates$lat)
  )

  cloud <- candidates[, .(local_id, type, long, lat)]
  cloud[, dataset := unname(CANDIDATE_DATASET[type])]
  stopifnot("candidate type belongs to no reference dataset" = !anyNA(cloud$dataset))

  # A cloud holds at most a dozen points, so all pairwise distances are computed outright.
  pairs <- merge(cloud, cloud, by = "local_id", allow.cartesian = TRUE, suffixes = c("", "_other"))
  # Drops each candidate's pairing with itself, and only that: one candidate per source
  # means `type` identifies a candidate within its station.
  pairs <- pairs[type != type_other]
  pairs[,
    pair_km := geosphere::distHaversine(
      cbind(long, lat),
      cbind(long_other, lat_other),
      r = EARTH_RADIUS_KM
    )
  ]

  # Coordinate-wise median: a cloud centre that a single wild candidate cannot drag. A lone
  # candidate is trivially its own centre, which would read as the tightest agreement there is.
  cloud[, `:=`(n_cand = .N, med_long = median(long), med_lat = median(lat)), by = local_id]
  cloud[,
    dist_to_cloud_median_km := geosphere::distHaversine(
      cbind(long, lat),
      cbind(med_long, med_lat),
      r = EARTH_RADIUS_KM
    )
  ]
  cloud[n_cand == 1L, dist_to_cloud_median_km := NA_real_]

  # Corroboration is measured only across datasets, at the 500 m the released accuracy is
  # reported at. dataset_other breaks distance ties, so the feature does not depend on the
  # order candidates happened to arrive in.
  cross <- pairs[dataset != dataset_other]
  setorder(cross, local_id, type, pair_km, dataset_other)
  nearest <- cross[,
    .(nearest_other_km = pair_km[1], nearest_other_dataset = dataset_other[1]),
    by = .(local_id, type)
  ]
  # Filtering ahead of the grouping rather than inside it keeps data.table's optimized
  # aggregation; written as uniqueN(dataset_other[pair_km <= 0.5]) this step is ~15x slower.
  agreeing <- cross[pair_km <= 0.5, .(n_datasets_within_500m = uniqueN(dataset_other)), by = .(local_id, type)]
  # Dispersion describes the whole cloud, siblings included. The median over ordered pairs
  # equals the median over unordered ones -- each pair appears twice.
  dispersion <- pairs[, .(cloud_dispersion_km = median(pair_km)), by = local_id]

  # Update joins rather than merges: they leave the candidate table's rows and their order
  # alone, so nothing downstream depends on the order consensus happened to sort them into.
  out <- copy(candidates)
  out[cloud, on = .(local_id, type), `:=`(n_cand = i.n_cand, dist_to_cloud_median_km = i.dist_to_cloud_median_km)]
  out[
    nearest,
    on = .(local_id, type),
    `:=`(nearest_other_km = i.nearest_other_km, nearest_other_dataset = i.nearest_other_dataset)
  ]
  out[agreeing, on = .(local_id, type), n_datasets_within_500m := i.n_datasets_within_500m]
  out[dispersion, on = .(local_id), cloud_dispersion_km := i.cloud_dispersion_km]
  # A station whose candidates all come from one dataset has nothing to corroborate it:
  # zero agreeing datasets is observed, whereas a distance to a candidate that does not
  # exist is not.
  out[is.na(n_datasets_within_500m), n_datasets_within_500m := 0L]
  out[]
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
    melt_match_candidates(
      cnefe10_stbairro_match,
      c(st = "st_cnefe_2010", bairro = "bairro_cnefe_2010")
    ),
    melt_match_candidates(
      cnefe22_stbairro_match,
      c(st = "st_cnefe_2022", bairro = "bairro_cnefe_2022")
    ),
    melt_match_candidates(schools_cnefe10_match, c(schools_cnefe = "schools_cnefe_name_2010")),
    melt_match_candidates(schools_cnefe22_match, c(schools_cnefe = "schools_cnefe_name_2022")),
    melt_match_candidates(
      inep_string_match,
      c(inep_name = "schools_inep_name", inep_addr = "schools_inep_addr")
    ),
    melt_match_candidates(
      agrocnefe_stbairro_match,
      c(st = "st_agrocnefe_2017", bairro = "bairro_agrocnefe_2017")
    ),
    geocodebr_candidates(geocodebr_match)
  )

  # A fresh table, so the pipeline's muni_demo target is not mutated by reference.
  muni_demo <- muni_demo[
    ANO == 2010,
    .(
      cod_localidade_ibge = Codmun7,
      logpop = log(POP),
      pct_rural = 100 * pesoRUR / POP
    )
  ]

  # Address and name features; SCHOOL_SYNONYM_PATTERN comes from R/data_cleaning.R.
  addr_features <- locais[, .(local_id, nm_locvot, ds_endereco, ds_bairro, normalized_addr)]
  # normalize_name(), not normalize_school(): the school feature looks for exactly the
  # generic terms normalize_school() strips out.
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
      grepl(SCHOOL_SYNONYM_PATTERN, norm_name),
      1,
      0
    )
  ]
  # The address gives no house number, which flags informal or rural addressing.
  # normalize_address() folds "s/n" and "s n" onto the token "sn" but leaves "sem numero"
  # spelled out, so both forms are matched here.
  addr_features[, addr_sn := fifelse(grepl("\\b(sn|sem numero)\\b", normalized_addr), 1, 0)]
  addr_features <- addr_features[, .(
    local_id,
    centro,
    zona_rural,
    school,
    addr_sn,
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
      r = EARTH_RADIUS_KM
    )
  ]
  # The model's outcome. A column rather than a recipe step: recipes skips outcome
  # transforms when baking new data, which silently leaves resampling metrics comparing
  # log-scale predictions against kilometre-scale truth.
  model_data[, log_dist := log(dist + GBM_LOG_OFFSET)]
  model_data[, ano := NULL]
  model_data[, tse_lat := NULL]
  model_data[, tse_long := NULL]

  # A candidate is usable when it has coordinates and its source scored it: a string
  # distance for the matched sources, an uncertainty radius for geocodebr.
  model_data <- model_data[
    !is.na(long) & !is.na(lat) & (!is.na(mindist) | !is.na(desvio_km))
  ]

  # After the filter, which is what leaves every candidate with a coordinate.
  add_consensus_features(model_data)
}

build_gbm_workflow <- function(data) {
  # Build the unfitted tunable LightGBM workflow (recipe + spec) used for match selection.

  ## Define the model recipe
  gbm_recipe <- recipes::recipe(
    formula = log_dist ~ .,
    data = data
  ) |>
    recipes::update_role(cod_localidade_ibge, new_role = "id variable") |>
    recipes::update_role(local_id, new_role = "id variable") |>
    # dist is log_dist untransformed, so as a predictor it would be the answer. It rides
    # along as an id variable because evaluation scores against it.
    recipes::update_role(dist, new_role = "id variable") |>
    recipes::step_impute_median(logpop, pct_rural, area)

  ## Quantile objective at SELECTOR_QUANTILE, so the log fit back-transforms to a distance
  ## quantile rather than a geometric mean.
  gbm_spec <-
    parsnip::boost_tree(
      trees = tune(),
      min_n = tune(),
      mtry = tune(),
      learn_rate = tune(),
      loss_reduction = tune()
    ) |>
    parsnip::set_mode("regression") |>
    parsnip::set_engine(
      "lightgbm",
      num_leaves = tune(),
      objective = "quantile",
      alpha = SELECTOR_QUANTILE
    )

  workflows::workflow() |>
    workflows::add_recipe(gbm_recipe) |>
    workflows::add_model(gbm_spec)
}

# Pinball (quantile) loss at SELECTOR_QUANTILE: the loss the model is trained on, so
# hyperparameters are selected on it too. Under-prediction is charged tau, over-prediction
# 1 - tau, which is what pulls the fit to the quantile rather than the mean.
pinball_loss_vec <- function(truth, estimate, na_rm = TRUE, case_weights = NULL, ...) {
  yardstick::check_numeric_metric(truth, estimate, case_weights)
  if (na_rm) {
    kept <- yardstick::yardstick_remove_missing(truth, estimate, case_weights)
    truth <- kept$truth
    estimate <- kept$estimate
    case_weights <- kept$case_weights
  }
  resid <- truth - estimate
  loss <- pmax(SELECTOR_QUANTILE * resid, (SELECTOR_QUANTILE - 1) * resid)
  if (is.null(case_weights)) mean(loss) else stats::weighted.mean(loss, case_weights)
}

pinball_loss <- function(data, ...) {
  UseMethod("pinball_loss")
}
pinball_loss <- yardstick::new_numeric_metric(pinball_loss, direction = "minimize")

pinball_loss.data.frame <- function(data, truth, estimate, na_rm = TRUE, case_weights = NULL, ...) {
  yardstick::numeric_metric_summarizer(
    name = "pinball_loss",
    fn = pinball_loss_vec,
    data = data,
    truth = !!rlang::enquo(truth),
    estimate = !!rlang::enquo(estimate),
    na_rm = na_rm,
    case_weights = !!rlang::enquo(case_weights)
  )
}

# The pipeline's pick: one candidate per station, the lowest predicted quantile, ties to the
# first row. Selection, conformal calibration, and evaluation all read it here so they cannot
# diverge. Keyed on pred_logq rather than the kilometre bound, which conformal_bound_km()
# clamps at 0 and so would tie every near-exact match together.
select_best_candidate <- function(scored) {
  stopifnot("candidates are missing a predicted quantile" = !anyNA(scored$pred_logq))
  unique(scored[order(local_id, pred_logq)], by = "local_id")
}

# The split-conformal order statistic: given calibration residuals (truth minus predicted
# quantile), the smallest correction that leaves at most 1 - SELECTOR_QUANTILE of them
# uncovered. The ceiling is what makes the guarantee finite-sample rather than asymptotic --
# with n residuals the bound must clear the ceiling((n+1)*tau)-th, not the n*tau-th, and
# below n = 9 (at tau = 0.9) that index runs past the sample entirely.
conformal_offset_from_residuals <- function(resid) {
  stopifnot("calibration residuals must be complete" = !anyNA(resid))
  n <- length(resid)
  k <- ceiling((n + 1) * SELECTOR_QUANTILE)
  if (k > n) {
    stop(sprintf("conformal offset: %d calibration points cannot attain %.2f coverage", n, SELECTOR_QUANTILE))
  }
  sort(resid)[k]
}

# The conformal correction for a fitted model, on the log scale. Computed on each station's
# *chosen* candidate rather than all of them: the chosen one is the argmin of the predicted
# quantile, so it skews toward candidates whose quantile came out too low, and calibrating
# over every candidate would leave the published number under-covering.
conformal_log_offset <- function(fitted_workflow, cal_data) {
  scored <- as.data.table(cal_data)[, .(local_id, log_dist)]
  scored[, pred_logq := predict(fitted_workflow, new_data = cal_data)$.pred]
  winners <- select_best_candidate(scored)
  conformal_offset_from_residuals(winners$log_dist - winners$pred_logq)
}

# The published bound in kilometres: the predicted log-quantile lifted by the conformal
# correction, back on the distance scale. Exp is monotone, so a quantile of log-distance
# back-transforms to that quantile of distance -- which a mean would not. Clamped at 0
# because a negative correction could otherwise produce a negative distance.
conformal_bound_km <- function(pred_logq, offset) {
  pmax(exp(pred_logq + offset) - GBM_LOG_OFFSET, 0)
}

train_model <- function(model_data, dev_mode) {
  # tune_race_anova needs more than its 3 burn-in resamples, so 4 is the dev-mode floor.
  n_folds <- if (dev_mode) 4 else 10
  grid_n <- if (dev_mode) 5 else 50
  message(sprintf("Training model with %d CV folds and a grid of %d candidates", n_folds, grid_n))

  if (nrow(model_data) == 0) {
    stop("No data available for model training")
  }

  ## Remove data with missing outcome; make_model_data() already dropped unscored candidates.
  model_data <- model_data[!is.na(dist)]

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

  ## Hyperparameters are selected on the loss the model is trained on; mae on the log scale
  ## rides along as a readable companion. Racing uses the first metric.
  metrics <- yardstick::metric_set(pinball_loss, yardstick::mae)

  ### Use racing models to tune hyperparameters
  gbm_tune <- finetune::tune_race_anova(
    gbm_workflow,
    resamples = vfolds,
    grid = grid_n,
    metrics = metrics,
    control = finetune::control_race(
      verbose_elim = TRUE,
      verbose = TRUE,
      allow_par = FALSE
    )
  )
  best_params <- tune::select_best(gbm_tune, metric = "pinball_loss")

  final_model <- tune::finalize_workflow(gbm_workflow, best_params)

  final_fit <- tune::last_fit(final_model, split = splits, metrics = metrics)

  ## last_fit() fits on the training half only, so the testing half is already held out and
  ## serves as the conformal calibration set at no extra fitting cost. Municipality-grouped,
  ## so no station's candidates straddle the fit/calibrate boundary.
  offset <- conformal_log_offset(tune::extract_workflow(final_fit), testing_set)
  message(sprintf(
    "Conformal offset for %.0f%% coverage: %.3f log-km over %d calibration stations",
    100 * SELECTOR_QUANTILE,
    offset,
    uniqueN(testing_set$local_id)
  ))

  list(
    tune_out = gbm_tune,
    final_fit = final_fit,
    conformal_offset = offset
  )
}

# Score every candidate match and attach its calibrated distance bound in kilometres.
# Projects first and predicts into the narrow table: model_data carries every predictor and
# runs to millions of rows, so scoring it in place would double the target's peak memory.
get_predictions <- function(trained_model, model_data) {
  fitted <- tune::extract_workflow(trained_model$final_fit)
  out <- model_data[, .(
    local_id,
    match_type = type,
    mindist,
    desvio_km,
    long,
    lat,
    dist
  )]
  out[, pred_logq := predict(fitted, new_data = model_data)$.pred]
  out[, conf_dist_km := conformal_bound_km(pred_logq, trained_model$conformal_offset)]
  out[]
}
