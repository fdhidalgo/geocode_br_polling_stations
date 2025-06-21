# R Function Analysis and Consolidation Plan

**Date**: 2025-06-20  
**Total Functions**: 103  
**Total Files**: 22  
**Functions Already Moved to Backup**: 18

## Executive Summary

This document provides a comprehensive analysis of all R functions in the geocode_br_polling_stations project. The codebase contains 103 functions across 22 files, with 18 functions already moved to backup folders as unused. The analysis reveals opportunities for consolidation, particularly around:

1. **Duplicate Implementations**: String matching functions have both regular and memory-efficient versions
2. **Mixed Concerns**: Some files contain functions from multiple categories (e.g., target_helpers.R)
3. **Empty Files**: functions_validate.R is now mostly a placeholder
4. **Scattered Functions**: Related functions are spread across multiple files

## Consolidation Recommendations

### High Priority
1. **Merge string matching implementations**: Consolidate `string_matching_geocode_fns.R` and `string_matching_geocode_fns_memory_efficient.R` into a single file with a parameter to control memory usage
2. **Consolidate validation functions**: Merge validation functions from 4 different files into a unified validation module
3. **Reorganize target_helpers.R**: Split into separate files for processing, export, and configuration functions

### Medium Priority
1. **Combine panel ID functions**: Merge related panel ID functions from 3 files
2. **Unify data cleaning functions**: Consider splitting the large data_cleaning_fns.R by data source
3. **Consolidate parallel processing**: Merge parallel processing utilities

### Low Priority
1. **Remove empty placeholder files** after verifying no dependencies
2. **Group utility functions** into a single utilities file

## Function Inventory by Category

### 1. Data Cleaning Functions (13 functions)

**File: R/data_cleaning_fns.R**
- `clean_cnefe22()` - Clean CNEFE 2022 census data
- `clean_tsegeocoded_locais()` - Clean TSE geocoded polling station data
- `clean_agro_cnefe()` - Clean agricultural CNEFE data
- `import_locais()` - Import polling station data with municipality filtering
- `finalize_coords()` - Finalize coordinates using model predictions
- `make_tract_centroids()` - Calculate census tract centroids
- `normalize_address()` - Normalize address strings
- `normalize_school()` - Normalize school names
- `clean_inep()` - Clean INEP education data
- `calc_muni_area()` - Calculate municipality area
- `export_geocoded_locais()` - Export final geocoded data
- `get_cnefe22_schools()` - Extract schools from CNEFE22
- `convert_coord()` - Convert coordinate formats

**File: R/memory_efficient_cnefe.R**
- `clean_cnefe10()` - Clean CNEFE 2010 data (wrapper for efficient version)
- `clean_cnefe10_efficient()` - Memory-efficient CNEFE 2010 cleaning

**File: R/geocodebr_matching.R**
- `clean_text_for_geocodebr()` - Clean text for geocodebr compatibility
- `simplify_address_for_geocodebr()` - Simplify addresses for geocodebr

### 2. String Matching Functions (15 functions - with duplicates)

**File: R/string_matching_geocode_fns.R**
- `match_inep_muni()` - Match polling stations to INEP schools
- `match_schools_cnefe_muni()` - Match to CNEFE schools
- `match_stbairro_cnefe_muni()` - Match by street/neighborhood in CNEFE
- `match_stbairro_agrocnefe_muni()` - Match by street/neighborhood in Agro CNEFE
- `make_model_data()` - Prepare data for model training
- `train_model()` - Train LightGBM model
- `get_predictions()` - Get model predictions

**File: R/string_matching_geocode_fns_memory_efficient.R** (Duplicates with memory optimization)
- `match_inep_muni()` - Memory-efficient version
- `match_schools_cnefe_muni()` - Memory-efficient version
- `match_stbairro_cnefe_muni()` - Memory-efficient version
- `match_stbairro_agrocnefe_muni()` - Memory-efficient version

**File: R/memory_efficient_string_matching.R**
- `prefilter_by_common_words()` - Pre-filter targets by common words
- `chunk_string_match()` - Process string matching in chunks
- `match_strings_memory_efficient()` - Main memory-efficient matching function
- `get_adaptive_chunk_size()` - Calculate optimal chunk size

**File: R/geocodebr_matching.R**
- `match_geocodebr_muni()` - Match using geocodebr package

### 3. Panel ID Functions (17 functions)

**File: R/panel_id_fns.R**
- `process_year_pairs()` - Process panel pairs between years
- `make_panel_ids()` - Create final panel IDs
- `create_panel_dataset()` - Create panel dataset structure
- `create_and_select_best_pairs()` - Create and select best matching pairs
- `make_panel_1block()` - Process single block for panel creation
- `export_panel_ids()` - Export panel IDs
- `combine_state_panel_ids()` - Combine panel IDs from multiple states

**File: R/panel_id_municipality_fns.R**
- `create_panel_municipality_batches()` - Create municipality batches for processing
- `process_panel_ids_municipality_batch()` - Process panel IDs for municipality batch
- `create_and_select_best_pairs_optimized()` - Optimized pair selection

**File: R/panel_id_blocking_fns.R**
- `extract_significant_words()` - Extract significant words for blocking
- `create_word_blocking_keys()` - Create blocking keys from words
- `find_shared_word_pairs()` - Find pairs with shared words
- `create_two_level_blocked_pairs()` - Create two-level blocked pairs
- `calculate_blocking_stats()` - Calculate blocking statistics

**File: R/panel_id_blocking_conservative.R**
- `extract_words_and_fragments()` - Extract words and fragments for conservative blocking

### 4. Validation Functions (14 functions)

**File: R/validation_stages.R**
- `validate_import_stage()` - Validate data import
- `validate_cleaning_stage()` - Validate data cleaning
- `validate_string_match_stage()` - Validate string matching results
- `validate_merge_stage()` - Validate merge operations
- `validate_prediction_stage()` - Validate model predictions
- `validate_output_stage()` - Validate final output

**File: R/validation_target_functions.R**
- `validate_merge_simple()` - Simplified merge validation
- `validate_predictions_simple()` - Simplified prediction validation
- `validate_final_output()` - Validate final output data
- `validate_inputs_consolidated()` - Consolidated input validation

**File: R/validation_report_helpers.R**
- `ensure_quarto_path()` - Ensure Quarto is available
- `render_sanity_check_report()` - Render sanity check report
- `get_report_output_files()` - Get report output file paths
- `create_validation_report_target()` - Create validation report target
- `create_validation_report()` - Create validation report
- `generate_validation_report_simplified()` - Generate simplified report

**File: R/functions_validate.R**
- (Empty placeholder file - original functions moved to backup)

### 5. Pipeline Infrastructure Functions (20 functions)

**File: R/target_helpers.R**
- `get_crew_controllers()` - Get crew controllers for pipeline
- `configure_targets_options()` - Configure targets options
- `process_string_match_batch()` - Process string matching batch
- `process_stbairro_match_batch()` - Process street/neighborhood batch
- `process_cnefe_state()` - Process CNEFE by state
- `process_inep_batch()` - Process INEP batch
- `process_schools_cnefe_batch()` - Process schools CNEFE batch
- `process_geocodebr_batch()` - Process geocodebr batch
- `process_cnefe_stbairro_batch()` - Process CNEFE street/neighborhood batch
- `process_agrocnefe_stbairro_batch()` - Process Agro CNEFE batch
- `export_geocoded_with_validation()` - Export with validation
- `export_panel_ids_with_validation()` - Export panel IDs with validation

**File: R/pipeline_config.R**
- `get_pipeline_config()` - Get pipeline configuration
- `get_states_for_processing()` - Get states to process
- `get_agro_cnefe_files()` - Get Agro CNEFE file paths

### 6. Utility Functions (8 functions)

**File: R/data_table_utils.R**
- `standardize_column_names()` - Standardize column names
- `apply_padding_batch()` - Apply zero-padding to columns

**File: R/expected_municipality_counts.R**
- `get_expected_municipality_count()` - Get expected municipality count by year
- `get_expected_municipality_range()` - Get expected range with tolerance

**File: R/filtering_helpers.R**
- `filter_by_dev_mode()` - Filter by development mode
- `filter_data_by_state()` - Filter data by state
- `filter_data_by_municipalities()` - Filter by municipality codes
- `get_muni_codes_for_states()` - Get municipality codes for states
- `filter_geographic_by_state()` - Filter geographic data by state
- `apply_dev_mode_filters()` - Apply development mode filters
- `apply_brasilia_filters()` - Apply Brasília-specific filters

### 7. Parallel Processing Functions (10 functions)

**File: R/parallel_processing_fns.R**
- `create_crew_controllers()` - Create differentiated crew controllers
- `route_task()` - Route task to appropriate controller
- `monitor_resource_usage()` - Monitor system resources
- `adjust_workers()` - Adjust worker count based on resources
- `cleanup_controllers()` - Clean up controllers
- `initialize_crew_controllers()` - Initialize controllers
- `print_controller_status()` - Print controller status

**File: R/parallel_integration_fns.R**
- `create_municipality_batches()` - Create municipality batches
- `create_municipality_batch_assignments()` - Create batch assignments

### 8. Monitoring and Reporting Functions (6 functions)

**File: R/data_quality_monitor_v2.R**
- `create_data_quality_monitor()` - Create data quality monitor report

**File: R/memory_efficient_cnefe.R**
- `read_cnefe_chunked()` - Read CNEFE in chunks
- `monitor_memory()` - Monitor memory usage

## Function Dependencies

### Core Dependencies
1. `normalize_address()` and `normalize_school()` - Used by all cleaning functions
2. `standardize_column_names()` - Used throughout the pipeline
3. `filter_by_dev_mode()` - Controls data subset for development

### String Matching Dependencies
1. Memory-efficient functions depend on `memory_efficient_string_matching.R`
2. All matching functions feed into `make_model_data()`
3. Model training flow: matching → `make_model_data()` → `train_model()` → `get_predictions()`

### Pipeline Flow Dependencies
1. Import functions → Cleaning functions → String matching → Model → Final output
2. Validation functions are called after each major stage
3. Export functions depend on validation results

## Proposed New File Structure

```
R/
├── core/
│   ├── config.R              # Pipeline configuration
│   ├── utils.R               # General utilities
│   └── constants.R           # Shared constants
├── data_cleaning/
│   ├── cnefe_cleaning.R      # All CNEFE cleaning functions
│   ├── tse_cleaning.R        # TSE data cleaning
│   ├── inep_cleaning.R       # INEP data cleaning
│   └── normalization.R       # Address/name normalization
├── geocoding/
│   ├── string_matching.R     # Unified string matching (with memory param)
│   ├── geocodebr.R          # GeocodeR integration
│   └── model.R              # Model training and prediction
├── panel_creation/
│   ├── panel_core.R         # Core panel ID functions
│   ├── blocking.R           # Blocking strategies
│   └── municipality_processing.R
├── validation/
│   ├── stages.R             # Stage validation functions
│   ├── reports.R            # Report generation
│   └── rules.R              # Validation rules
├── parallel/
│   ├── controllers.R        # Crew controller management
│   ├── batching.R          # Batch creation functions
│   └── routing.R           # Task routing
└── pipeline/
    ├── targets_helpers.R    # Target-specific helpers
    └── export.R            # Export functions
```

## Migration Checklist

- [ ] Back up current R/ directory
- [ ] Create new directory structure
- [ ] Move functions to appropriate new files
- [ ] Update source() statements
- [ ] Update _targets.R imports
- [ ] Test each module independently
- [ ] Run full pipeline in DEV_MODE
- [ ] Update documentation
- [ ] Remove empty/placeholder files
- [ ] Run full pipeline test

## Notes on Already Moved Functions

The following counts of unused functions were already moved to backup:
- `expected_municipality_counts.R`: 1 function
- `panel_id_blocking_conservative.R`: 2 functions  
- `panel_id_fns.R`: 3 functions
- `panel_id_municipality_fns.R`: 2 functions
- `parallel_integration_fns.R`: 5 functions
- `parallel_processing_fns.R`: 2 functions
- `target_helpers.R`: 1 function
- `validation_report_helpers.R`: 3 functions
- `validation_stages.R`: 1 function
- `validation_target_functions.R`: 5 functions

Total: 25 functions marked as moved (but only 18 unique moves)