library(targets)

# Get metadata
meta <- tar_meta()

# Find targets with warnings
warnings_df <- meta[!is.na(meta$warnings), c("name", "warnings")]

if (nrow(warnings_df) > 0) {
  cat("Targets with warnings:\n\n")
  for (i in 1:nrow(warnings_df)) {
    cat("Target:", warnings_df$name[i], "\n")
    cat("Warning:", warnings_df$warnings[i], "\n\n")
  }
} else {
  cat("No warnings found.\n")
}

# Also check errors
errors_df <- meta[!is.na(meta$error), c("name", "error")]
if (nrow(errors_df) > 0) {
  cat("\nTargets with errors:\n\n")
  for (i in 1:nrow(errors_df)) {
    cat("Target:", errors_df$name[i], "\n")
    cat("Error:", errors_df$error[i], "\n\n")
  }
}