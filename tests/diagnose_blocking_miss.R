# Diagnose why blocking is missing matches

library(targets)
library(data.table)
library(reclin2)

# Source necessary functions
source("R/data_cleaning_fns.R")
source("R/panel_id_fns.R") 
source("R/panel_id_municipality_fns.R")
source("R/panel_id_blocking_fns.R")

cat("Loading test data...\n")

# Load data
tar_load(locais_filtered)

# Use the same test municipality
test_muni <- 1200179
test_data <- locais_filtered[cod_localidade_ibge == test_muni]

# Focus on 2006-2008 where we're missing a match
data_2006 <- test_data[ano == 2006]
data_2008 <- test_data[ano == 2008]

cat("\n2006 data (", nrow(data_2006), " records):\n", sep = "")
cat("2008 data (", nrow(data_2008), " records):\n", sep = "")

# Standardize column names
standardize_column_names(data_2006, inplace = TRUE)
standardize_column_names(data_2008, inplace = TRUE)

# Extract words for all records
cat("\nExtracting words from 2006 data:\n")
words_2006_name <- extract_significant_words(data_2006$normalized_name)
words_2006_addr <- extract_significant_words(data_2006$normalized_addr)

cat("\nExtracting words from 2008 data:\n")
words_2008_name <- extract_significant_words(data_2008$normalized_name)
words_2008_addr <- extract_significant_words(data_2008$normalized_addr)

# Combine words
words_2006_all <- mapply(function(n, a) unique(c(n, a)), 
                        words_2006_name, words_2006_addr, 
                        SIMPLIFY = FALSE)
words_2008_all <- mapply(function(n, a) unique(c(n, a)), 
                        words_2008_name, words_2008_addr, 
                        SIMPLIFY = FALSE)

# Find records with no or few words
few_words_2006 <- which(sapply(words_2006_all, length) < 2)
few_words_2008 <- which(sapply(words_2008_all, length) < 2)

if (length(few_words_2006) > 0) {
  cat("\n2006 records with < 2 significant words:\n")
  for (i in few_words_2006[1:min(5, length(few_words_2006))]) {
    cat("  [", i, "] '", data_2006$normalized_name[i], "' / '", 
        data_2006$normalized_addr[i], "'\n", sep = "")
    cat("    Words: ", paste(words_2006_all[[i]], collapse = ", "), "\n", sep = "")
  }
}

if (length(few_words_2008) > 0) {
  cat("\n2008 records with < 2 significant words:\n")
  for (i in few_words_2008[1:min(5, length(few_words_2008))]) {
    cat("  [", i, "] '", data_2008$normalized_name[i], "' / '", 
        data_2008$normalized_addr[i], "'\n", sep = "")
    cat("    Words: ", paste(words_2008_all[[i]], collapse = ", "), "\n", sep = "")
  }
}

# Now run the blocking and find what's excluded
cat("\n\nRunning standard blocking...\n")
pairs_all <- pair_blocking(data_2006, data_2008, "cod_localidade_ibge")
cat("Total pairs: ", nrow(pairs_all), "\n")

# Apply word blocking manually to see what gets excluded
keep_pair <- logical(nrow(pairs_all))
no_match_pairs <- list()

for (i in seq_len(nrow(pairs_all))) {
  x_idx <- pairs_all$.x[i]
  y_idx <- pairs_all$.y[i]
  
  x_words <- words_2006_all[[x_idx]]
  y_words <- words_2008_all[[y_idx]]
  
  if (length(x_words) == 0 || length(y_words) == 0) {
    keep_pair[i] <- TRUE
  } else if (length(x_words) < 2 || length(y_words) < 2) {
    keep_pair[i] <- TRUE  # Conservative
  } else {
    has_match <- any(x_words %in% y_words)
    keep_pair[i] <- has_match
    
    if (!has_match) {
      no_match_pairs[[length(no_match_pairs) + 1]] <- list(
        x_idx = x_idx,
        y_idx = y_idx,
        x_name = data_2006$normalized_name[x_idx],
        y_name = data_2008$normalized_name[y_idx],
        x_addr = data_2006$normalized_addr[x_idx],
        y_addr = data_2008$normalized_addr[y_idx],
        x_words = x_words,
        y_words = y_words
      )
    }
  }
}

filtered_count <- sum(keep_pair)
cat("After word blocking: ", filtered_count, " pairs\n")
cat("Excluded: ", length(no_match_pairs), " pairs\n")

# Show excluded pairs
if (length(no_match_pairs) > 0) {
  cat("\nExcluded pairs (no shared words):\n")
  for (i in seq_len(min(10, length(no_match_pairs)))) {
    p <- no_match_pairs[[i]]
    cat("\nPair ", i, ":\n", sep = "")
    cat("  2006 [", p$x_idx, "]: '", p$x_name, "' / '", p$x_addr, "'\n", sep = "")
    cat("    Words: ", paste(p$x_words, collapse = ", "), "\n", sep = "")
    cat("  2008 [", p$y_idx, "]: '", p$y_name, "' / '", p$y_addr, "'\n", sep = "")
    cat("    Words: ", paste(p$y_words, collapse = ", "), "\n", sep = "")
  }
}

# Check if these excluded pairs would have matched with the original method
cat("\n\nChecking if excluded pairs would match with full comparison...\n")

# Run the full matching process without blocking
pairs_blocked <- pairs_all[keep_pair]
pairs_excluded <- pairs_all[!keep_pair]

if (nrow(pairs_excluded) > 0) {
  # Compare the excluded pairs
  pairs_excluded <- compare_pairs(pairs_excluded,
    on = c("normalized_name", "normalized_addr"),
    default_comparator = cmp_jarowinkler(0.9),
    inplace = TRUE
  )
  
  # Check string distances
  high_similarity <- which(pairs_excluded$normalized_name > 0.8 | 
                          pairs_excluded$normalized_addr > 0.8)
  
  if (length(high_similarity) > 0) {
    cat("\nWARNING: Found high-similarity pairs that were excluded:\n")
    for (i in high_similarity) {
      x_idx <- pairs_excluded$.x[i]
      y_idx <- pairs_excluded$.y[i]
      cat("\nExcluded pair with high similarity:\n")
      cat("  2006: '", data_2006$normalized_name[x_idx], "'\n", sep = "")
      cat("  2008: '", data_2008$normalized_name[y_idx], "'\n", sep = "")
      cat("  Name similarity: ", round(pairs_excluded$normalized_name[i], 3), "\n", sep = "")
      cat("  Addr similarity: ", round(pairs_excluded$normalized_addr[i], 3), "\n", sep = "")
    }
  } else {
    cat("Good: No high-similarity pairs were excluded.\n")
  }
}