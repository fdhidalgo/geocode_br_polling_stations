# target_factories.R
#
# Factory functions for creating targets with identical patterns in the geocoding pipeline.
# This reduces code duplication and makes the _targets.R file more maintainable.

# Required libraries (these are loaded in _targets.R but needed for factory functions)
if (!requireNamespace("targets", quietly = TRUE)) {
  stop("targets package is required for factory functions")
}
if (!requireNamespace("tarchetypes", quietly = TRUE)) {
  stop("tarchetypes package is required for factory functions") 
}

# Factory function for creating string matching batch targets
#' Create a string matching batch target
#'
#' @param target_name Name of the batch target
#' @param match_function Name of the matching function to call
#' @param data_config List with data source configurations
#' @param controller Crew controller name (optional)
#' @return A tar_target object for batch processing
create_string_match_batch_target <- function(target_name, 
                                           match_function, 
                                           data_config,
                                           controller = NULL) {
  
  # Build the function call based on data configuration
  function_args <- list()
  
  # Always include locais_muni as first argument
  function_args$locais_muni <- substitute(
    locais_filtered[cod_localidade_ibge == muni_code]
  )
  
  # Add other data arguments based on configuration
  for (arg_name in names(data_config)) {
    if (arg_name != "locais_data") {  # Skip locais_data as it's handled above
      data_source <- data_config[[arg_name]]$source
      filter_col <- data_config[[arg_name]]$filter_col %||% "id_munic_7"
      
      function_args[[arg_name]] <- substitute(
        data_source[filter_col == muni_code],
        list(
          data_source = as.name(data_source),
          filter_col = as.name(filter_col)
        )
      )
    }
  }
  
  # Create the matching function call
  match_call <- as.call(c(as.name(match_function), function_args))
  
  # Build the target command
  command_expr <- substitute({
    # Get municipalities for this batch
    batch_munis <- municipality_batch_assignments[
      batch_id == batch_ids
    ]$muni_code
    
    # Process all municipalities in this batch
    batch_results <- lapply(batch_munis, function(muni_code) {
      MATCH_CALL
    })
    
    # Remove NULL results and combine
    batch_results <- batch_results[!sapply(batch_results, is.null)]
    if (length(batch_results) > 0) {
      data.table::rbindlist(batch_results, use.names = TRUE, fill = TRUE)
    } else {
      data.table::data.table()
    }
  }, list(MATCH_CALL = match_call))
  
  # Build target arguments, excluding NULL values for resources
  target_args <- list(
    name = target_name,
    command = command_expr,
    pattern = quote(map(batch_ids)),
    iteration = "list",
    deployment = "worker",
    storage = "worker", 
    retrieval = "worker"
  )
  
  # Only add resources if controller is specified
  if (!is.null(controller)) {
    target_args$resources <- tar_resources(crew = tar_resources_crew(controller = controller))
  }
  
  # Create and return the target using tar_target (not tar_target_raw)
  do.call(tar_target, target_args)
}

# Factory function for creating string matching combine targets  
#' Create a string matching combine target
#'
#' @param target_name Name of the combine target
#' @param batch_target_name Name of the batch target to combine
#' @param use_names rbindlist parameter (default TRUE)
#' @param fill rbindlist parameter (default TRUE) 
#' @param deployment Deployment setting (default "main")
#' @param storage Storage setting (default NULL)
#' @param retrieval Retrieval setting (default NULL)
#' @return A tar_target object for combining batch results
create_string_match_combine_target <- function(target_name,
                                             batch_target_name,
                                             use_names = TRUE,
                                             fill = TRUE,
                                             deployment = "main",
                                             storage = NULL,
                                             retrieval = NULL) {
  
  # Use tar_target instead of tar_target_raw to avoid dependency validation issues
  # This matches how the original _targets.R handles it
  target_args <- list(
    name = target_name,
    command = substitute(
      data.table::rbindlist(BATCH_TARGET, use.names = USE_NAMES, fill = FILL),
      list(
        BATCH_TARGET = as.name(batch_target_name),
        USE_NAMES = use_names,
        FILL = fill
      )
    ),
    deployment = deployment
  )
  
  # Only add storage and retrieval if they're not NULL
  if (!is.null(storage)) {
    target_args$storage <- storage
  }
  if (!is.null(retrieval)) {
    target_args$retrieval <- retrieval
  }
  
  do.call(tar_target, target_args)
}

# Configuration for all string matching targets
get_string_match_configs <- function() {
  list(
    inep = list(
      batch_name = "inep_string_match_batch",
      combine_name = "inep_string_match",
      match_function = "match_inep_muni",
      data_config = list(
        inep_muni = list(source = "inep_data", filter_col = "id_munic_7")
      ),
      controller = "standard",
      combine_deployment = "main"
    ),
    
    schools_cnefe10 = list(
      batch_name = "schools_cnefe10_match_batch", 
      combine_name = "schools_cnefe10_match",
      match_function = "match_schools_cnefe_muni",
      data_config = list(
        schools_cnefe_muni = list(source = "schools_cnefe10", filter_col = "id_munic_7")
      ),
      controller = "standard",
      combine_deployment = "worker",
      combine_storage = "worker",
      combine_retrieval = "worker"
    ),
    
    schools_cnefe22 = list(
      batch_name = "schools_cnefe22_match_batch",
      combine_name = "schools_cnefe22_match", 
      match_function = "match_schools_cnefe_muni",
      data_config = list(
        schools_cnefe_muni = list(source = "schools_cnefe22", filter_col = "id_munic_7")
      ),
      controller = "standard",
      combine_deployment = "main"
    ),
    
    cnefe10_stbairro = list(
      batch_name = "cnefe10_stbairro_match_batch",
      combine_name = "cnefe10_stbairro_match",
      match_function = "match_stbairro_cnefe_muni", 
      data_config = list(
        cnefe_st_muni = list(source = "cnefe10_st", filter_col = "id_munic_7"),
        cnefe_bairro_muni = list(source = "cnefe10_bairro", filter_col = "id_munic_7")
      ),
      controller = NULL,
      combine_deployment = "worker", 
      combine_storage = "worker",
      combine_retrieval = "worker"
    ),
    
    cnefe22_stbairro = list(
      batch_name = "cnefe22_stbairro_match_batch",
      combine_name = "cnefe22_stbairro_match",
      match_function = "match_stbairro_cnefe_muni",
      data_config = list(
        cnefe_st_muni = list(source = "cnefe22_st", filter_col = "id_munic_7"),
        cnefe_bairro_muni = list(source = "cnefe22_bairro", filter_col = "id_munic_7")
      ),
      controller = NULL,
      combine_deployment = "worker",
      combine_storage = "worker", 
      combine_retrieval = "worker"
    ),
    
    agrocnefe_stbairro = list(
      batch_name = "agrocnefe_stbairro_match_batch",
      combine_name = "agrocnefe_stbairro_match",
      match_function = "match_stbairro_agrocnefe_muni",
      data_config = list(
        agrocnefe_st_muni = list(source = "agrocnefe_st", filter_col = "id_munic_7"),
        agrocnefe_bairro_muni = list(source = "agrocnefe_bairro", filter_col = "id_munic_7")
      ),
      controller = "standard",
      combine_deployment = "worker",
      combine_storage = "worker",
      combine_retrieval = "worker"
    ),
    
    geocodebr = list(
      batch_name = "geocodebr_match_batch",
      combine_name = "geocodebr_match",
      match_function = "match_geocodebr_muni",
      data_config = list(
        muni_ids = list(source = "muni_ids", filter_col = "id_munic_7")
      ),
      controller = "standard",
      combine_deployment = "worker",
      combine_storage = "worker",
      combine_retrieval = "worker"
    )
  )
}

# Function to create all string matching targets
create_all_string_match_targets <- function() {
  configs <- get_string_match_configs()
  
  targets <- list()
  
  for (config_name in names(configs)) {
    config <- configs[[config_name]]
    
    # Create batch target
    batch_target <- create_string_match_batch_target(
      target_name = config$batch_name,
      match_function = config$match_function,
      data_config = config$data_config,
      controller = config$controller
    )
    
    # Create combine target
    combine_target <- create_string_match_combine_target(
      target_name = config$combine_name,
      batch_target_name = config$batch_name,
      deployment = config$combine_deployment,
      storage = config$combine_storage,
      retrieval = config$combine_retrieval
    )
    
    targets <- c(targets, list(batch_target, combine_target))
  }
  
  return(targets)
}