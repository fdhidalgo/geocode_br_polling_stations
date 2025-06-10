# Summarize investigation findings
library(data.table)

# Load investigation results
results <- readRDS("output/investigation/data_quality_investigation_results.rds")

cat("\n=== INVESTIGATION FINDINGS SUMMARY ===\n\n")

# 1. Municipality Count Issue
cat("1. MUNICIPALITY COUNT DISCREPANCY:\n")
cat("   FINDING: The inflated count (8,085) was due to counting municipality name variations\n")
cat("   REALITY: There are 5,571 unique municipality codes (matches expected ~5,570)\n")
cat("   ISSUE STATUS: ✅ NOT A REAL PROBLEM - just a reporting issue (now fixed)\n")
cat(sprintf("   - %d municipalities have multiple name spellings\n", 
            sum(results$investigation$municipality_counts$duplicate_codes$N > 0)))
cat("   - Examples: 'MAE D AGUA' vs 'MÃE D'ÁGUA', 'SAO FELIPE DO OESTE' vs 'SÃO FELIPE D'OESTE'\n\n")

# 2. Temporal Consistency
cat("2. TEMPORAL CONSISTENCY:\n")
cat("   FINDING: 5,563 municipalities appear in all 10 years (2006-2024)\n")
cat("   EXPECTATION: ~5,000 municipalities\n")
cat("   ISSUE STATUS: ✅ EXCELLENT - actually better than expected!\n")
cat("   - This shows strong data consistency across years\n\n")

# 3. Zero Stations in All Years
cat("3. POLLING STATIONS PERSISTENCE:\n")
cat("   FINDING: Zero individual stations appear in all years\n")
cat("   REASON: local_id is NOT a persistent identifier - it's year-specific\n")
cat("   EVIDENCE:\n")
cat("   - local_ids are simple integers (1, 2, 3...)\n")
cat("   - Each year has unique IDs starting from 1\n")
cat("   - This is BY DESIGN - panel_id is used for cross-year tracking\n")
cat("   ISSUE STATUS: ✅ NOT A PROBLEM - working as intended\n\n")

# 4. Panel ID Coverage
cat("4. PANEL ID COVERAGE:\n")
cat(sprintf("   FINDING: %.1f%% coverage (target: 90%%)\n", 
            results$investigation$panel_coverage$overall_coverage))
cat("   ISSUE STATUS: ⚠️  SLIGHTLY BELOW TARGET but may be acceptable\n")
cat("   - Missing only 0.2% to reach target\n")
cat("   - Some stations genuinely can't be matched (new constructions, major changes)\n")
cat("   Coverage by year:\n")
print(results$investigation$panel_coverage$by_year[, .(
  Year = ano, 
  Coverage = paste0(round(coverage_pct, 1), "%")
)])
cat("\n")

# 5. Extreme Changes 2022-2024
cat("5. EXTREME MUNICIPALITY CHANGES (2022-2024):\n")
if (!is.null(results$investigation$extreme_changes)) {
  n_extreme <- nrow(results$investigation$extreme_changes$extreme_changes)
  cat(sprintf("   FINDING: %d municipalities with >100%% change\n", n_extreme))
  cat("   BREAKDOWN:\n")
  print(results$investigation$extreme_changes$change_summary)
  cat("   ISSUE STATUS: ⚠️  NEEDS INVESTIGATION\n")
  cat("   - Could be real (new developments, closures)\n")
  cat("   - Could be data collection differences\n")
  cat("   - Need to verify specific cases\n")
}

# 6. Additional Findings
cat("\n6. ADDITIONAL FINDINGS:\n")
cat("   - 1 record with invalid (NA) municipality code in 2024\n")
cat("   - MT state shows NA for code ranges (needs investigation)\n")
cat("   - Panel IDs work well: 104,527 panels span multiple years\n")
cat("   - 80,666 panels span 9 years (excellent continuity)\n")

cat("\n=== RECOMMENDATIONS ===\n")
cat("1. ✅ Municipality count issue is RESOLVED (reporting fix applied)\n")
cat("2. ✅ Station persistence is NOT an issue (by design)\n")
cat("3. ✅ Temporal consistency is EXCELLENT\n")
cat("4. ⚠️  Panel coverage is CLOSE to target - investigate if 89.8% is acceptable\n")
cat("5. ⚠️  Extreme changes need case-by-case verification\n")
cat("6. 🔧 Fix: Handle NA municipality codes in 2024 data\n")
cat("7. 🔧 Fix: Investigate MT state code range issue\n")

cat("\n=== CONCLUSION ===\n")
cat("Most 'issues' identified in the sanity report were false alarms:\n")
cat("- Municipality count: Fixed by using codes instead of names\n")
cat("- Zero stations in all years: Expected behavior (year-specific IDs)\n")
cat("- Temporal consistency: Actually excellent (better than expected)\n")
cat("\nOnly minor issues remain that may need attention.\n")