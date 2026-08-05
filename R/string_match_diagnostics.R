## Diagnostics on string-match quality: NA-coordinate rates and match distances.

library(data.table)

# Longitude/latitude column names whose NA rates a given string-match target is scored on.
string_match_coord_cols <- function(match_name) {
  cols <- list(
    inep_string_match = c(long = "match_long_inep_addr", lat = "match_lat_inep_addr"),
    cnefe10_stbairro_match = c(long = "match_long_cnefe_bairro", lat = "match_lat_cnefe_bairro"),
    cnefe22_stbairro_match = c(long = "match_long_cnefe_bairro", lat = "match_lat_cnefe_bairro"),
    schools_cnefe10_match = c(long = "match_long_schools_cnefe", lat = "match_lat_schools_cnefe"),
    schools_cnefe22_match = c(long = "match_long_schools_cnefe", lat = "match_lat_schools_cnefe"),
    agrocnefe_stbairro_match = c(long = "match_long_agrocnefe_bairro", lat = "match_lat_agrocnefe_bairro")
  )[[match_name]]

  if (is.null(cols)) {
    stop("No coordinate-column mapping for string-match target: ", match_name)
  }
  cols
}

# NA-coordinate, no-match, perfect-match and distance-quartile metrics for one match table.
calculate_string_match_diagnostics <- function(match_data, match_name, long_col, lat_col) {
  missing_cols <- setdiff(c(long_col, lat_col), names(match_data))
  if (length(missing_cols) > 0) {
    stop(sprintf(
      "calculate_string_match_diagnostics(): coordinate column(s) not found in '%s': %s",
      match_name,
      paste(missing_cols, collapse = ", ")
    ))
  }

  total_rows <- nrow(match_data)

  na_coords <- sum(is.na(match_data[[long_col]]) | is.na(match_data[[lat_col]]))
  na_pct <- round(na_coords / total_rows * 100, 2)

  # Match tables name their distance column differently; use whichever is present.
  dist_cols <- c(
    "mindist",
    "mindist_cnefe_bairro",
    "mindist_cnefe_st",
    "mindist_inep_addr",
    "mindist_inep_name",
    "distance"
  )
  dist_col <- dist_cols[dist_cols %in% names(match_data)][1]

  if (!is.na(dist_col)) {
    no_match <- sum(is.infinite(match_data[[dist_col]]))
    perfect_match <- sum(match_data[[dist_col]] == 0, na.rm = TRUE)
    perfect_match_na <- sum(
      match_data[[dist_col]] == 0 &
        (is.na(match_data[[long_col]]) | is.na(match_data[[lat_col]])),
      na.rm = TRUE
    )

    matched_dists <- match_data[[dist_col]][!is.infinite(match_data[[dist_col]])]
    if (length(matched_dists) > 0) {
      dist_quartiles <- quantile(matched_dists, probs = c(0.25, 0.5, 0.75), na.rm = TRUE)
      dist_q1 <- round(dist_quartiles[1], 4)
      dist_median <- round(dist_quartiles[2], 4)
      dist_q3 <- round(dist_quartiles[3], 4)
    } else {
      dist_q1 <- dist_median <- dist_q3 <- NA
    }
  } else {
    no_match <- NA
    perfect_match <- NA
    perfect_match_na <- NA
    dist_q1 <- dist_median <- dist_q3 <- NA
  }

  data.table(
    match_name = match_name,
    total_rows = total_rows,
    na_coords = na_coords,
    na_coords_pct = na_pct,
    no_match = no_match,
    no_match_pct = ifelse(!is.na(no_match), round(no_match / total_rows * 100, 2), NA),
    perfect_match = perfect_match,
    perfect_match_pct = ifelse(!is.na(perfect_match), round(perfect_match / total_rows * 100, 2), NA),
    perfect_match_na = perfect_match_na,
    dist_q1 = dist_q1,
    dist_median = dist_median,
    dist_q3 = dist_q3,
    coord_cols_used = paste(c(long_col, lat_col), collapse = ", ")
  )
}

# Combine diagnostics for the named string-match tables, appending an OVERALL row.
aggregate_string_match_diagnostics <- function(...) {
  match_list <- list(...)
  match_names <- names(match_list)

  if (is.null(match_names) || any(match_names == "")) {
    stop("All arguments must be named")
  }

  diagnostics <- lapply(seq_along(match_list), function(i) {
    coords <- string_match_coord_cols(match_names[i])
    calculate_string_match_diagnostics(
      match_list[[i]],
      match_names[i],
      coords[["long"]],
      coords[["lat"]]
    )
  })

  result <- rbindlist(diagnostics, fill = TRUE)

  summary_row <- data.table(
    match_name = "OVERALL",
    total_rows = sum(result$total_rows, na.rm = TRUE),
    na_coords = sum(result$na_coords, na.rm = TRUE),
    na_coords_pct = round(
      sum(result$na_coords, na.rm = TRUE) /
        sum(result$total_rows, na.rm = TRUE) *
        100,
      2
    ),
    no_match = sum(result$no_match, na.rm = TRUE),
    perfect_match = sum(result$perfect_match, na.rm = TRUE),
    perfect_match_na = sum(result$perfect_match_na, na.rm = TRUE)
  )

  rbind(result, summary_row, fill = TRUE)
}

# Render the aggregated diagnostics as a plain-text report.
format_string_match_diagnostics <- function(diagnostics) {
  lines <- character()

  lines <- c(lines, "=== String Match Coordinate Diagnostics ===\n")
  lines <- c(lines, sprintf("Generated: %s\n", Sys.time()))

  overall <- diagnostics[match_name == "OVERALL"]
  if (nrow(overall) > 0) {
    lines <- c(lines, "\nOVERALL SUMMARY:")
    lines <- c(lines, sprintf("  Total rows processed: %s", format(overall$total_rows, big.mark = ",")))
    lines <- c(
      lines,
      sprintf("  Total NA coordinates: %s (%.1f%%)", format(overall$na_coords, big.mark = ","), overall$na_coords_pct)
    )
    if (!is.na(overall$no_match)) {
      lines <- c(lines, sprintf("  Total no matches: %s", format(overall$no_match, big.mark = ",")))
    }
    lines <- c(lines, "")
  }

  details <- diagnostics[match_name != "OVERALL"]
  if (nrow(details) > 0) {
    lines <- c(lines, "\nDETAILS BY MATCH TYPE:")

    for (i in 1:nrow(details)) {
      row <- details[i]
      lines <- c(lines, sprintf("\n%s:", row$match_name))
      lines <- c(lines, sprintf("  Rows: %s", format(row$total_rows, big.mark = ",")))
      lines <- c(
        lines,
        sprintf("  NA coordinates: %s (%.1f%%)", format(row$na_coords, big.mark = ","), row$na_coords_pct)
      )

      if (!is.na(row$no_match)) {
        lines <- c(
          lines,
          sprintf("  No match found: %s (%.1f%%)", format(row$no_match, big.mark = ","), row$no_match_pct)
        )
      }

      if (!is.na(row$perfect_match)) {
        lines <- c(
          lines,
          sprintf("  Perfect matches: %s (%.1f%%)", format(row$perfect_match, big.mark = ","), row$perfect_match_pct)
        )
      }

      if (!is.na(row$perfect_match_na) && row$perfect_match_na > 0) {
        lines <- c(lines, sprintf("  WARNING: Perfect matches with NA coords: %s", row$perfect_match_na))
      }

      if (!is.na(row$dist_median)) {
        lines <- c(
          lines,
          sprintf("  Match distances (Q1/Med/Q3): %.4f / %.4f / %.4f", row$dist_q1, row$dist_median, row$dist_q3)
        )
      }
    }
  }

  paste(lines, collapse = "\n")
}

# Print the match-quality report and write it alongside the released outputs.
write_string_match_diagnostics <- function(string_match_diagnostics) {
  report_text <- format_string_match_diagnostics(string_match_diagnostics)
  cat(report_text)
  dir.create("output", showWarnings = FALSE)
  writeLines(report_text, "./output/string_match_diagnostics.txt")
  "./output/string_match_diagnostics.txt"
}
