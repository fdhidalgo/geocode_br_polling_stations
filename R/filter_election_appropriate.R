#' Filter Polling Stations by Election-Appropriate Jurisdictions
#' 
#' Removes polling station records for jurisdictions that don't participate
#' in certain election types. Specifically, removes Federal District (Brasília)
#' and state districts from municipal election years.
#' 
#' @param data data.table with polling station data
#' @param keep_all logical; if TRUE, keeps all data with a flag instead of filtering
#' @return data.table with filtered data or with election_appropriate flag
#' @export
filter_election_appropriate <- function(data, keep_all = FALSE) {
  # Ensure we're working with a copy
  data <- copy(data)
  
  # Define special jurisdictions that don't have municipal elections
  # These are federal/state districts without mayors or city councils
  special_districts <- c(
    5300108,  # Brasília (DF) - Federal District
    2605459   # Fernando de Noronha (PE) - State District
  )
  
  # Define election years by type
  municipal_years <- c(2008, 2012, 2016, 2020, 2024)
  federal_state_years <- c(2006, 2010, 2014, 2018, 2022)
  
  if (keep_all) {
    # Add a flag indicating if the record is election-appropriate
    data[, election_appropriate := !(
      cod_localidade_ibge %in% special_districts & ano %in% municipal_years
    )]
    
    # Add election type for clarity
    data[, election_type := fcase(
      ano %in% municipal_years, "Municipal",
      ano %in% federal_state_years, "Federal/State",
      default = "Unknown"
    )]
    
    # Add jurisdiction type
    data[, jurisdiction_type := fcase(
      cod_localidade_ibge == 5300108, "Federal District",
      cod_localidade_ibge == 2605459, "State District", 
      default = "Municipality"
    )]
    
    # Log summary
    inappropriate <- data[election_appropriate == FALSE]
    if (nrow(inappropriate) > 0) {
      message(sprintf(
        "Flagged %d polling station records as election-inappropriate:\n",
        nrow(inappropriate)
      ))
      summary_table <- inappropriate[, .N, by = .(
        jurisdiction = nm_localidade,
        uf = sg_uf,
        year = ano,
        election_type
      )][order(year)]
      print(summary_table)
    }
    
  } else {
    # Filter out inappropriate records
    n_before <- nrow(data)
    data <- data[!(cod_localidade_ibge %in% special_districts & ano %in% municipal_years)]
    n_after <- nrow(data)
    n_removed <- n_before - n_after
    
    if (n_removed > 0) {
      message(sprintf(
        "Removed %d election-inappropriate polling station records (%.2f%%)\n",
        n_removed, 100 * n_removed / n_before
      ))
    }
  }
  
  return(data)
}

#' Validate Election Appropriateness
#' 
#' Checks data for jurisdictions appearing in inappropriate election years
#' and returns a validation report.
#' 
#' @param data data.table with polling station data
#' @return list with validation results
#' @export
validate_election_appropriateness <- function(data) {
  # Define special jurisdictions
  special_districts <- list(
    "5300108" = "Brasília (DF)",
    "2605459" = "Fernando de Noronha (PE)"
  )
  
  municipal_years <- c(2008, 2012, 2016, 2020, 2024)
  
  # Find inappropriate entries
  issues <- data[
    cod_localidade_ibge %in% names(special_districts) & ano %in% municipal_years
  ]
  
  if (nrow(issues) > 0) {
    # Summarize issues
    summary_dt <- issues[, .(
      n_stations = uniqueN(local_id),
      jurisdiction_name = nm_localidade[1],
      state = sg_uf[1]
    ), by = .(cod_localidade_ibge, ano)]
    
    # Add jurisdiction type
    summary_dt[, jurisdiction_type := special_districts[as.character(cod_localidade_ibge)]]
    
    validation_result <- list(
      passed = FALSE,
      n_issues = nrow(issues),
      message = sprintf(
        "Found %d polling stations in special districts during municipal elections",
        nrow(issues)
      ),
      summary = summary_dt[order(ano, cod_localidade_ibge)],
      recommendation = paste(
        "These entries likely represent infrastructure updates rather than actual voting.",
        "Consider using filter_election_appropriate() to handle these cases."
      )
    )
  } else {
    validation_result <- list(
      passed = TRUE,
      n_issues = 0,
      message = "No election appropriateness issues found",
      summary = data.table(),
      recommendation = NA_character_
    )
  }
  
  return(validation_result)
}