# Function to create pairs, compute scores, and select the best match for each observation
create_and_select_best_pairs <- function(data, years, blocking_column, scoring_columns) {
        pairs_list <- list()

        # Sort years to ensure correct order
        years <- sort(years)

        for (i in seq_along(years)[-length(years)]) {
                year1 <- years[i]
                year2 <- years[i + 1]
                print(year1)

                # Subset the data for the two consecutive years
                linkexample1 <- data[ano == year1]
                linkexample2 <- data[ano == year2]

                # Create pairs using pair_blocking
                pairs <- pair_blocking(linkexample1, linkexample2, blocking_column)

                # Compare pairs to ensure the necessary variables are kept, using Jaro-Winkler Distance
                pairs <- compare_pairs(pairs,
                        on = scoring_columns,
                        default_comparator = cmp_jarowinkler(0.9), inplace = TRUE
                )

                # Rename the variables to indicate they represent a match score
                match_scoring_columns <- paste0("match_", scoring_columns)
                setnames(pairs, scoring_columns, match_scoring_columns)

                # Compute the Machine Learning Fellegi and Sunter Weights
                formula <- as.formula(paste("~", paste(match_scoring_columns, collapse = " + ")))
                m <- problink_em(formula, data = pairs)
                pairs <- predict(m, pairs, add = TRUE)

                # Add local_id to the pairs
                pairs[, `:=`(
                        .x_local_id = linkexample1$local_id[.x],
                        .y_local_id = linkexample2$local_id[.y]
                )]

                # Select the best match for each observation
                best_pairs <- select_n_to_m(pairs, threshold = 0, score = "weights", var = "match", n = 1, m = 1)

                # Keep only the pairs where match is TRUE
                best_pairs <- best_pairs[match == TRUE]

                # Store the final matched pairs in the list
                pairs_list[[paste0(year1, "_", year2)]] <- best_pairs
        }

        return(pairs_list)
}

process_year_pairs <- function(panel, best_pairs, year_from, year_to) {
        # Rename the columns in best_pairs to match the years
        clean_pairs <- best_pairs[, .(local_id_from = .x_local_id, local_id_to = .y_local_id)]
        setnames(
                clean_pairs, c("local_id_from", "local_id_to"),
                c(paste0("local_id_", year_from), paste0("local_id_", year_to))
        )

        # Identify missing ids from the previous year that are not in the current panel
        missing_ids <- setdiff(
                clean_pairs[[paste0("local_id_", year_from)]],
                panel[[paste0("local_id_", year_from)]]
        )

        # Create rows for missing ids
        missing_rows <- data.table(matrix(NA, nrow = length(missing_ids), ncol = ncol(panel)))
        setnames(missing_rows, names(panel))
        missing_rows[[paste0("local_id_", year_from)]] <- missing_ids

        # Add missing rows to the panel
        panel <- rbindlist(list(panel, missing_rows), fill = TRUE)

        # Merge the panel with the clean pairs
        panel <- merge(panel, clean_pairs, by = paste0("local_id_", year_from), all = TRUE)

        return(panel)
}

create_panel_dataset <- function(final_pairs_list, years) {
        # Ensure the years are sorted
        years <- sort(years)

        # Extract the best pairs for the first two years
        first_year <- years[1]
        second_year <- years[2]
        best_pairs_first <- final_pairs_list[[paste0(first_year, "_", second_year)]]

        # Create the initial panel dataset with the first two years' local_id columns
        panel <- best_pairs_first[, .(
                local_id_first = .x_local_id,
                local_id_second = .y_local_id
        )]
        setnames(
                panel, c("local_id_first", "local_id_second"),
                c(paste0("local_id_", first_year), paste0("local_id_", second_year))
        )

        # Process each subsequent pair of years using the helper function
        for (i in seq(2, length(years) - 1)) {
                year_from <- years[i]
                year_to <- years[i + 1]
                best_pairs <- final_pairs_list[[paste0(year_from, "_", year_to)]]
                panel <- process_year_pairs(panel, best_pairs, year_from, year_to)
        }

        # Add a new variable panel_id that is equal to the smallest value in each row (among the local_id columns)
        panel[, panel_id := apply(.SD, 1, min, na.rm = TRUE), .SDcols = patterns("local_id_")]

        # Create the final dataset with local_id and panel_id
        panel <- melt(panel,
                id.vars = "panel_id", measure.vars = patterns("local_id_"),
                variable.name = "year", value.name = "local_id"
        )[, .(local_id, panel_id)]

        # Remove rows with NA local_id
        panel <- panel[!is.na(local_id)]

        return(panel)
}

make_panel_1block <- function(block, years, blocking_column, scoring_columns) {
        print(unique(block$sg_uf))
        pairs_list <- create_and_select_best_pairs(block, years, blocking_column, scoring_columns)
        panel <- create_panel_dataset(pairs_list, years)
        return(panel)
}

make_panel_ids <- function(panel_ids_df, panel_ids_states, geocoded_locais) {
        panel_ids <- rbindlist(list(panel_ids_df, panel_ids_states))


        ## Merge panel ids with geocoded locais
        panel_ids <- merge(panel_ids, geocoded_locais[, .(local_id, ano, long, lat, pred_dist)],
                by = "local_id", all.x = TRUE
        )

        ## For each panel id, long and lat with smallest pred_dist, break ties by choosing most recent year
        panel_ids <- panel_ids[order(panel_id, pred_dist, -ano), .SD[1], by = .(panel_id)]
        # Remove ano
        panel_ids[, ano := NULL]
        panel_ids
}

export_panel_ids <- function(panel_ids) {
        fwrite(panel_ids, "./output/panel_ids.csv.gz")
        "./output/panel_ids.csv.gz"
}
