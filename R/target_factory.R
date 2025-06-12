#' Target Factory Functions
#'
#' This file contains factory functions to create repetitive targets
#' with consistent patterns, reducing code duplication in _targets.R

#' Create string matching target
#'
#' Factory function to create string matching targets with consistent pattern
#' @param name Target name
#' @param match_fn Matching function to use
#' @param locais_data Polling stations data (usually locais_filtered)
#' @param match_data Data to match against
#' @param batch_ids Batch IDs for dynamic branching
#' @param batch_assignments Municipality batch assignments
#' @param controller Controller to use (default: "standard")
#' @return A tar_target object
#' @export
create_string_match_target <- function(name, match_fn, locais_data, match_data,
                                     batch_ids, batch_assignments,
                                     controller = "standard") {
  # Get function name as string for special handling
  fn_name <- deparse(substitute(match_fn))
  
  tar_target(
    name = !!sym(name),
    command = process_string_match_batch(
      batch_ids = batch_ids,
      batch_assignments = batch_assignments,
      locais_data = locais_data,
      match_data = match_data,
      match_fn = match_fn
    ),
    pattern = map(batch_ids),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "worker",
    resources = tar_resources(
      crew = tar_resources_crew(controller = controller)
    )
  )
}

#' Create street/neighborhood matching target
#'
#' Factory for street and neighborhood matching targets
#' @param name Target name
#' @param match_fn Matching function
#' @param locais_data Polling stations data
#' @param st_data Street-level data
#' @param bairro_data Neighborhood-level data
#' @param batch_ids Batch IDs
#' @param batch_assignments Municipality batch assignments
#' @param controller Controller to use
#' @return A tar_target object
#' @export
create_stbairro_match_target <- function(name, match_fn, locais_data, 
                                       st_data, bairro_data,
                                       batch_ids, batch_assignments,
                                       controller = "standard") {
  tar_target(
    name = !!sym(name),
    command = process_stbairro_match_batch(
      batch_ids = batch_ids,
      batch_assignments = batch_assignments,
      locais_data = locais_data,
      st_data = st_data,
      bairro_data = bairro_data,
      match_fn = match_fn
    ),
    pattern = map(batch_ids),
    iteration = "list",
    deployment = "worker",
    storage = "worker",
    retrieval = "worker",
    resources = tar_resources(
      crew = tar_resources_crew(controller = controller)
    )
  )
}

#' Create validation target
#'
#' Factory function to create validation targets
#' @param name Target name
#' @param data_to_validate Data or expression to validate
#' @param validation_fn Validation function to use
#' @param stage_name Name of the validation stage
#' @param ... Additional arguments passed to validation function
#' @return A tar_target object
#' @export
create_validation_target <- function(name, data_to_validate, validation_fn, 
                                   stage_name, ...) {
  tar_target(
    name = !!sym(name),
    command = {
      result <- validation_fn(
        data = data_to_validate,
        stage_name = stage_name,
        ...
      )
      
      if (!result$passed) {
        warning(paste(stage_name, "validation failed - check data quality"))
      }
      
      result
    }
  )
}

#' Create CNEFE processing target
#'
#' Factory for CNEFE state-by-state processing targets
#' @param name Target name
#' @param states States to process
#' @param year CNEFE year (2010 or 2022)
#' @param muni_ids Municipality identifiers
#' @param tract_centroids Tract centroids (2010 only)
#' @param extract_schools Whether to extract schools (2010 only)
#' @param controller Controller to use
#' @return A tar_target object
#' @export
create_cnefe_processing_target <- function(name, states, year, muni_ids,
                                         tract_centroids = NULL,
                                         extract_schools = FALSE,
                                         controller = "memory_limited") {
  tar_target(
    name = !!sym(name),
    command = process_cnefe_state(
      state = states,
      year = year,
      muni_ids = muni_ids,
      tract_centroids = tract_centroids,
      extract_schools = extract_schools
    ),
    pattern = map(states),
    format = "qs",
    iteration = "list",
    resources = tar_resources(
      crew = tar_resources_crew(controller = controller)
    )
  )
}

#' Create combination target
#'
#' Factory for targets that combine results from dynamic branches
#' @param name Target name
#' @param branch_data Name of the branched target to combine
#' @param storage Storage mode (default: "worker")
#' @param retrieval Retrieval mode (default: "worker")
#' @return A tar_target object
#' @export
create_combination_target <- function(name, branch_data, 
                                    storage = "worker", 
                                    retrieval = "worker") {
  tar_target(
    name = !!sym(name),
    command = {
      # Combine all state/batch results
      combined <- rbindlist(
        branch_data,
        use.names = TRUE,
        fill = TRUE
      )
      
      # Force garbage collection
      gc(verbose = FALSE)
      
      combined
    },
    format = "qs",
    storage = storage,
    retrieval = retrieval
  )
}

#' Create file target
#'
#' Simple factory for file targets
#' @param name Target name
#' @param path File path
#' @return A tar_target object
#' @export
create_file_target <- function(name, path) {
  tar_target(
    name = !!sym(name),
    command = path,
    format = "file"
  )
}

#' Create data import target
#'
#' Factory for data import targets
#' @param name Target name
#' @param file_target Name of the file target
#' @param import_fn Import function (default: fread)
#' @param ... Additional arguments to import function
#' @return A tar_target object
#' @export
create_import_target <- function(name, file_target, import_fn = fread, ...) {
  tar_target(
    name = !!sym(name),
    command = import_fn(file_target, ...)
  )
}