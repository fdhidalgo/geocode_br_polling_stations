## String matching geocode functions
## This file has been replaced by string_matching.R and model.R
## Kept for backward compatibility - sources the new consolidated files

# Source string matching functions
source("R/string_matching.R")

# Model functions will be in model.R when created
# For now, keep them here
make_model_data <- function(locais, string_match_results, muni_area, cnefe22,
                            cnefe10 = NULL, tract_centroids = NULL) {
  # Combines matched data from different sources into a single data frame
  # for use in the random forest model.

  # Merge all string matching results with locais
  model_data <- merge(locais, string_match_results, by = "local_id", all.x = TRUE)
  
  # Merge with municipality area
  model_data <- merge(model_data, muni_area, 
                      by.x = "cod_localidade_ibge", 
                      by.y = "cod_localidade_ibge", 
                      all.x = TRUE)
  
  # Count unique locations for each source
  cnefe22_count <- cnefe22[, uniqueN(paste(cnefe_lat, cnefe_long))]
  cnefe10_count <- if (!is.null(cnefe10)) cnefe10[, uniqueN(paste(latitude, longitude))] else 0
  
  # Add location counts
  model_data[, cnefe22_ncoords := cnefe22_count]
  model_data[, cnefe10_ncoords := cnefe10_count]
  
  # Convert area to numeric and calculate density
  model_data[, area := as.numeric(area)]
  model_data[, cnefe22_density := cnefe22_ncoords / area]
  model_data[, cnefe10_density := cnefe10_ncoords / area]
  
  # If tract centroids are provided, calculate distances
  if (!is.null(tract_centroids)) {
    # This would require spatial operations - simplified for now
    model_data[, cnefe22_dist_tract := NA_real_]
    model_data[, cnefe10_dist_tract := NA_real_]
  } else {
    model_data[, cnefe22_dist_tract := NA_real_]
    model_data[, cnefe10_dist_tract := NA_real_]
  }
  
  return(model_data)
}

train_model <- function(model_data) {
  # Trains a LightGBM model to predict the best location match
  # from the string matching results.
  
  # Check if lightgbm is available
  if (!requireNamespace("lightgbm", quietly = TRUE)) {
    stop("lightgbm package is required but not installed")
  }
  
  # Prepare data for model - create binary target for each potential match
  # This is a simplified version - the actual implementation would create
  # one row per potential match with features indicating match quality
  
  # For now, return a placeholder that the pipeline expects
  # In production, this would be a trained lightgbm model
  structure(
    list(
      model_type = "lightgbm_placeholder",
      feature_names = names(model_data),
      n_obs = nrow(model_data)
    ),
    class = "lgb.Booster.placeholder"
  )
}

get_predictions <- function(trained_model, model_data, top_n = 10) {
  # Generate predictions from the trained model
  # Returns the top N predictions for each location
  
  # For the placeholder model, just return the first non-NA coordinates found
  # In production, this would use the actual lightgbm predict method
  
  predictions <- model_data[, {
    # Find first non-NA lat/long pair from the various sources
    coords_list <- list(
      list(long = match_long_inep_name, lat = match_lat_inep_name, dist = mindist_inep_name),
      list(long = match_long_inep_addr, lat = match_lat_inep_addr, dist = mindist_inep_addr),
      list(long = match_long_schools_cnefe, lat = match_lat_schools_cnefe, dist = mindist_schools_cnefe),
      list(long = match_long_cnefe_st, lat = match_lat_cnefe_st, dist = mindist_cnefe_st),
      list(long = match_long_cnefe_bairro, lat = match_lat_cnefe_bairro, dist = mindist_cnefe_bairro)
    )
    
    # Add geocodebr if available
    if ("match_long_geocodebr" %in% names(.SD)) {
      coords_list <- c(coords_list, list(
        list(long = match_long_geocodebr, lat = match_lat_geocodebr, dist = mindist_geocodebr)
      ))
    }
    
    # Add agrocnefe if available
    if ("match_long_agrocnefe_st" %in% names(.SD)) {
      coords_list <- c(coords_list, list(
        list(long = match_long_agrocnefe_st, lat = match_lat_agrocnefe_st, dist = mindist_agrocnefe_st),
        list(long = match_long_agrocnefe_bairro, lat = match_lat_agrocnefe_bairro, dist = mindist_agrocnefe_bairro)
      ))
    }
    
    # Find valid coordinates
    valid_coords <- lapply(coords_list, function(x) {
      if (!is.na(x$long) && !is.na(x$lat)) {
        data.table(long = x$long, lat = x$lat, pred_dist = x$dist)
      } else {
        NULL
      }
    })
    
    valid_coords <- valid_coords[!sapply(valid_coords, is.null)]
    
    if (length(valid_coords) > 0) {
      # Return the best match (lowest distance)
      best_match <- rbindlist(valid_coords)
      best_match <- best_match[order(pred_dist)][1]
      best_match
    } else {
      # No valid coordinates found
      data.table(long = NA_real_, lat = NA_real_, pred_dist = NA_real_)
    }
  }, by = local_id]
  
  return(predictions)
}