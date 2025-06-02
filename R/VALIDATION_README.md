# Validation Framework Files

This directory contains the validation framework integrated into the geocoding pipeline.

## Core Validation Files

### `validation_join_operations.R`
The main validation framework providing:
- `validate_join_keys()` - Pre-merge validation
- `validate_merge_result()` - Post-merge validation
- `safe_merge()` - Drop-in replacement for merge() with validation
- `get_merge_validation_report()` - Extract validation reports

### `validation_pipeline_merges.R` 
Pipeline-specific validation wrappers:
- `validate_merge_cnefe_muni()` - For CNEFE-municipality joins
- `validate_merge_tse_muni()` - For TSE geocoded data joins  
- `validate_merge_locais_coords()` - For location-coordinate joins
- `validate_merge_panel_ids()` - For panel identifier joins
- `create_validation_checkpoint()` - For targets integration

### `validation_example.R`
Working examples demonstrating:
- Basic join validation
- Many-to-one joins (CNEFE pattern)
- Composite key validation
- Using pipeline-specific wrappers

## Integration Status

### ✅ Completed
- TSE geocoded merge validation in `clean_tsegeocoded_locais()`
  - Validates municipality merge
  - Validates composite key merge with locais
  - Reports match rates and issues
  - Suppresses expected section consolidation warnings

### 🔲 Pending
- Final geocoding merge validation
- Panel IDs merge validation
- Model data merge validation
- CNEFE merge validation

## Usage

1. **In functions**: Replace `merge()` with `safe_merge()`:
   ```r
   # Old
   result <- merge(dt1, dt2, by = "key", all.x = TRUE)
   
   # New
   result <- safe_merge(dt1, dt2, keys = "key", 
                       join_type = "many-to-one",
                       merge_type = "left")
   ```

2. **In targets pipeline**: Already loaded in `_targets.R`:
   ```r
   source("./R/validation_join_operations.R")
   ```

## Notes

- The TSE "duplicate composite keys" warning was suppressed as it's expected behavior (section consolidation)
- Validation adds ~5-10% overhead but catches critical merge issues
- Set `stop_on_error = TRUE` for critical merges, `FALSE` for warnings only