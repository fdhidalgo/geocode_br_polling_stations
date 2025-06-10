#!/usr/bin/env Rscript
# Analyze Year-over-Year Changes in Municipalities
# Purpose: Investigate extreme changes across all years (2006-2024)

library(targets)
library(data.table)
library(ggplot2)

cat("\n=== YEAR-OVER-YEAR MUNICIPALITY CHANGES ANALYSIS ===\n")
cat("Date:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")

# Load data
cat("Loading data from targets pipeline...\n")
tar_load(geocoded_locais, store = "_targets")

# Get all years
years <- sort(unique(geocoded_locais$ano))
cat("Years in dataset:", paste(years, collapse = ", "), "\n\n")

# Function to calculate changes between two years
calculate_year_changes <- function(year1, year2, data) {
  # Get municipality counts for each year
  munis_year1 <- data[ano == year1, .(
    n_stations_y1 = uniqueN(local_id),
    n_records_y1 = .N
  ), by = cod_localidade_ibge]
  
  munis_year2 <- data[ano == year2, .(
    n_stations_y2 = uniqueN(local_id),
    n_records_y2 = .N
  ), by = cod_localidade_ibge]
  
  # Merge
  changes <- merge(munis_year1, munis_year2, 
                   by = "cod_localidade_ibge", 
                   all = TRUE)
  
  # Replace NA with 0 for municipalities that appear/disappear
  changes[is.na(n_stations_y1), ':='(n_stations_y1 = 0, n_records_y1 = 0)]
  changes[is.na(n_stations_y2), ':='(n_stations_y2 = 0, n_records_y2 = 0)]
  
  # Calculate changes
  changes[, ':='(
    absolute_change = n_stations_y2 - n_stations_y1,
    pct_change = ifelse(n_stations_y1 > 0, 
                       (n_stations_y2 - n_stations_y1) / n_stations_y1 * 100,
                       ifelse(n_stations_y2 > 0, Inf, 0)),
    year1 = year1,
    year2 = year2,
    year_pair = paste0(year1, "-", year2)
  )]
  
  # Categorize changes
  changes[, change_category := fcase(
    n_stations_y1 == 0 & n_stations_y2 > 0, "New municipality",
    n_stations_y1 > 0 & n_stations_y2 == 0, "Disappeared",
    pct_change >= 100, "Doubled or more",
    pct_change >= 50, "Major increase (50-100%)",
    pct_change >= 20, "Moderate increase (20-50%)",
    pct_change > -20 & pct_change < 20, "Stable (-20% to +20%)",
    pct_change <= -50, "Major decrease (>50%)",
    pct_change < -20, "Moderate decrease (20-50%)",
    default = "Other"
  )]
  
  return(changes)
}

# Calculate changes for all year pairs
cat("=== CALCULATING YEAR-OVER-YEAR CHANGES ===\n\n")
all_changes <- list()

for (i in 1:(length(years) - 1)) {
  year1 <- years[i]
  year2 <- years[i + 1]
  
  cat(sprintf("Analyzing %d → %d...\n", year1, year2))
  changes <- calculate_year_changes(year1, year2, geocoded_locais)
  all_changes[[paste0(year1, "-", year2)]] <- changes
}

# Combine all results
all_changes_dt <- rbindlist(all_changes)

# Add municipality names
muni_names <- geocoded_locais[, .(
  nm_localidade = nm_localidade[which.max(.N)],
  sg_uf = sg_uf[which.max(.N)]
), by = cod_localidade_ibge]

all_changes_dt <- merge(all_changes_dt, muni_names, 
                       by = "cod_localidade_ibge", all.x = TRUE)

# Summary statistics for each year transition
cat("\n=== SUMMARY BY YEAR TRANSITION ===\n")
summary_by_year <- all_changes_dt[, .(
  total_municipalities = .N,
  new_municipalities = sum(change_category == "New municipality"),
  disappeared = sum(change_category == "Disappeared"),
  major_increases = sum(change_category %in% c("Doubled or more", "Major increase (50-100%)")),
  major_decreases = sum(change_category %in% c("Major decrease (>50%)")),
  stable = sum(change_category == "Stable (-20% to +20%)"),
  avg_pct_change = mean(pct_change[is.finite(pct_change)], na.rm = TRUE),
  median_pct_change = median(pct_change[is.finite(pct_change)], na.rm = TRUE)
), by = year_pair]

print(summary_by_year)

# Identify problematic municipalities
cat("\n=== MUNICIPALITIES WITH FREQUENT EXTREME CHANGES ===\n")
problem_munis <- all_changes_dt[
  change_category %in% c("New municipality", "Disappeared", "Doubled or more", "Major decrease (>50%)"),
  .(n_extreme_changes = .N,
    years_affected = paste(unique(year_pair), collapse = ", "),
    change_types = paste(unique(change_category), collapse = ", ")
  ), by = .(cod_localidade_ibge, nm_localidade, sg_uf)
][order(-n_extreme_changes)]

cat("\nTop 20 municipalities with most extreme changes:\n")
print(problem_munis[1:min(20, .N)])

# Analyze patterns
cat("\n=== PATTERN ANALYSIS ===\n")

# 1. Municipalities that appear and disappear multiple times
appearing_disappearing <- all_changes_dt[
  change_category %in% c("New municipality", "Disappeared"),
  .(appearances = sum(change_category == "New municipality"),
    disappearances = sum(change_category == "Disappeared"),
    affected_years = paste(unique(year_pair), collapse = ", ")
  ), by = .(cod_localidade_ibge, nm_localidade, sg_uf)
][appearances > 0 | disappearances > 0][order(-(appearances + disappearances))]

if (nrow(appearing_disappearing) > 0) {
  cat("\nMunicipalities that appear/disappear multiple times:\n")
  print(appearing_disappearing[1:min(10, .N)])
}

# 2. State-level patterns
state_patterns <- all_changes_dt[, .(
  n_extreme_changes = sum(change_category %in% c("New municipality", "Disappeared", 
                                                 "Doubled or more", "Major decrease (>50%)")),
  n_municipalities = uniqueN(cod_localidade_ibge),
  pct_with_extreme = round(uniqueN(cod_localidade_ibge[
    change_category %in% c("New municipality", "Disappeared", 
                          "Doubled or more", "Major decrease (>50%)")
  ]) / uniqueN(cod_localidade_ibge) * 100, 1)
), by = sg_uf][order(-n_extreme_changes)]

cat("\nStates with most extreme changes:\n")
print(state_patterns)

# 3. Time trends
time_trends <- all_changes_dt[, .(
  pct_extreme = sum(change_category %in% c("New municipality", "Disappeared", 
                                          "Doubled or more", "Major decrease (>50%)")) / .N * 100
), by = year_pair]

cat("\nPercentage of municipalities with extreme changes by year:\n")
print(time_trends)

# Create visualizations
cat("\n=== CREATING VISUALIZATIONS ===\n")

# 1. Extreme changes over time
p1 <- ggplot(summary_by_year, aes(x = year_pair)) +
  geom_col(aes(y = new_municipalities), fill = "darkgreen", alpha = 0.7) +
  geom_col(aes(y = -disappeared), fill = "darkred", alpha = 0.7) +
  geom_hline(yintercept = 0, linetype = "solid") +
  labs(title = "Municipalities Appearing and Disappearing by Year",
       x = "Year Transition", 
       y = "Number of Municipalities (Positive = New, Negative = Disappeared)") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggsave("output/investigation/municipalities_appearing_disappearing.png", p1, 
       width = 10, height = 6, dpi = 300)

# 2. Distribution of percentage changes
finite_changes <- all_changes_dt[is.finite(pct_change) & abs(pct_change) < 200]
p2 <- ggplot(finite_changes, aes(x = pct_change)) +
  geom_histogram(bins = 50, fill = "steelblue", alpha = 0.7) +
  geom_vline(xintercept = c(-50, 50), linetype = "dashed", color = "red") +
  facet_wrap(~year_pair, scales = "free_y") +
  labs(title = "Distribution of Year-over-Year Percentage Changes",
       x = "Percentage Change", y = "Count") +
  theme_minimal()

ggsave("output/investigation/pct_change_distribution.png", p2, 
       width = 12, height = 8, dpi = 300)

# Export detailed results
cat("\n=== EXPORTING RESULTS ===\n")

# Save detailed municipality-level changes
fwrite(all_changes_dt[abs(pct_change) > 50 | !is.finite(pct_change)][
  order(year_pair, -abs(pct_change))
], "output/investigation/extreme_changes_all_years.csv")

# Save summary
saveRDS(list(
  summary_by_year = summary_by_year,
  problem_municipalities = problem_munis,
  appearing_disappearing = appearing_disappearing,
  state_patterns = state_patterns,
  time_trends = time_trends,
  timestamp = Sys.time()
), "output/investigation/yearoveryear_analysis_results.rds")

cat("\nResults saved to output/investigation/\n")

# Key findings summary
cat("\n=== KEY FINDINGS ===\n")
cat(sprintf("1. Total extreme changes across all years: %d\n", 
            nrow(all_changes_dt[change_category %in% c("New municipality", "Disappeared", 
                                                      "Doubled or more", "Major decrease (>50%)")])))
cat(sprintf("2. Municipalities that appeared at some point: %d\n",
            sum(summary_by_year$new_municipalities)))
cat(sprintf("3. Municipalities that disappeared at some point: %d\n",
            sum(summary_by_year$disappeared)))
cat(sprintf("4. Year with most instability: %s (%d new, %d disappeared)\n",
            summary_by_year[which.max(new_municipalities + disappeared), year_pair],
            summary_by_year[which.max(new_municipalities + disappeared), new_municipalities],
            summary_by_year[which.max(new_municipalities + disappeared), disappeared]))

# Specific cases to investigate
cat("\n=== CASES REQUIRING INVESTIGATION ===\n")
investigate_cases <- all_changes_dt[
  (change_category %in% c("New municipality", "Disappeared") & year2 >= 2020) |
  (pct_change > 200 & n_stations_y2 > 10),
  .(cod_localidade_ibge, nm_localidade, sg_uf, year_pair, 
    n_stations_y1, n_stations_y2, pct_change, change_category)
][order(year_pair, -abs(pct_change))]

if (nrow(investigate_cases) > 0) {
  cat("\nRecent extreme cases (2020+) or >200% increase:\n")
  print(investigate_cases[1:min(20, .N)])
}