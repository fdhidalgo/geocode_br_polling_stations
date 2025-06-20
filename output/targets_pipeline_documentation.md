# Targets Pipeline Documentation

This document provides a concise description of each target in the `_targets.R` pipeline for geocoding Brazilian polling stations. The pipeline processes multiple data sources to create a comprehensive dataset of polling station coordinates and panel identifiers.

**Last Updated**: After validation simplification (reduced from 85 to 80 targets)

## Table of Contents

1. [Data Import Targets](#data-import-targets)
2. [Geographic Features](#geographic-features)
3. [CNEFE Data Processing](#cnefe-data-processing)
4. [Polling Station Data](#polling-station-data)
5. [String Matching Targets](#string-matching-targets)
6. [Model Training and Prediction](#model-training-and-prediction)
7. [Final Geocoding](#final-geocoding)
8. [Validation and Reporting](#validation-and-reporting)
9. [Data Export](#data-export)

---

## Data Import Targets

### `muni_ids_file`
Points to the municipality identifiers CSV file (`./data/muni_identifiers.csv`). Contains mapping between different municipality ID formats used across Brazilian datasets.

### `muni_ids_all`
Reads the municipality identifiers file into memory. Includes all Brazilian municipalities with their various ID codes (7-digit IBGE, 6-digit, TSE codes).

### `muni_ids`
Filters municipalities based on development mode setting. In dev mode, only includes AC and RR states for faster testing.

### `inep_codes_file`
Points to the INEP school codes CSV file (`./data/inep_codes.csv`). Maps INEP education codes to municipality IDs.

### `inep_codes`
Reads the INEP codes file into memory. Contains the mapping between INEP school codes and IBGE municipality codes.

### `tract_shp_file`
Points to the census tract shapefile RDS (`./data/census_tracts2010_shp.rds`). Contains 2010 census tract geometries for all of Brazil.

### `tract_shp_all`
Reads and validates census tract geometries. Applies `st_make_valid()` to fix any invalid geometries.

### `tract_shp`
Filters census tract shapefile by development mode. In dev mode, only includes tracts from AC and RR states.

### `muni_shp_file`
Points to the municipality shapefile RDS (`./data/muni_shp.rds`). Contains municipality boundary geometries.

### `muni_shp_all`
Reads and validates municipality geometries. Ensures all geometries are valid for spatial operations.

### `muni_shp`
Filters municipality shapefile by development mode. Reduces dataset to selected states when in development mode.

### `muni_demo_file`
Points to the Atlas Brasil demographic data (`./data/atlas_brasil_census_data.csv.gz`). Contains socioeconomic indicators by municipality.

### `muni_demo_all`
Reads the compressed demographic data file. Includes various development indicators from the Atlas Brasil project.

### `muni_demo`
Filters demographic data by development mode municipalities. Ensures demographic data matches the municipalities being processed.

---

## Geographic Features

### `tract_centroids`
Calculates centroids for each census tract polygon. Used for approximating locations when exact addresses aren't available.

### `muni_area`
Calculates the area of each municipality in square kilometers. Used as a feature in the prediction model to account for urban density.

---

## CNEFE Data Processing

### `cnefe10_states`
Determines which states to process for CNEFE 2010 based on mode. Returns list of state abbreviations for parallel processing.

### `cnefe10_cleaned_by_state`
Processes CNEFE 2010 data state by state to manage memory usage. Cleans addresses, normalizes text, and adds geographic coordinates from census tracts.

### `schools_cnefe10_by_state`
Extracts school records from CNEFE 2010 by state. Identifies establishments likely to be schools based on name patterns.

### `cnefe10`
Combines all state-level CNEFE 2010 cleaned data into single dataset. Uses `rbindlist` for efficient memory usage with large datasets.

### `cnefe22_states`
Determines which states to process for CNEFE 2022 based on mode. Enables selective processing for development testing.

### `cnefe22_cleaned_by_state`
Processes CNEFE 2022 data state by state with updated cleaning logic. Handles the newer CNEFE format and data structure changes.

### `cnefe22`
Combines all state-level CNEFE 2022 cleaned data. Creates unified dataset for string matching operations.

### `schools_cnefe10`
Combines school extracts from all states for CNEFE 2010. Creates comprehensive school location dataset for matching.

### `cnefe10_st`
Aggregates CNEFE 2010 by street name within municipalities. Calculates median coordinates for each unique street as reference points.

### `cnefe10_bairro`
Aggregates CNEFE 2010 by neighborhood within municipalities. Provides neighborhood-level coordinate estimates for fuzzy matching.

### `agro_cnefe_files`
Gets list of agricultural CNEFE 2017 files based on mode. Returns file paths for states to be processed.

### `agro_cnefe`
Cleans and standardizes agricultural census CNEFE data from 2017. Integrates rural establishment data with consistent formatting.

### `agrocnefe_st`
Aggregates agricultural CNEFE by street name. Creates street-level reference points from rural census data.

### `agrocnefe_bairro`
Aggregates agricultural CNEFE by neighborhood. Provides rural neighborhood coordinate references.

### `schools_cnefe22`
Extracts school records from CNEFE 2022 data. Uses updated patterns to identify educational establishments.

### `cnefe22_st`
Aggregates CNEFE 2022 by street name within municipalities. Creates 2022 street-level reference coordinates.

### `cnefe22_bairro`
Aggregates CNEFE 2022 by neighborhood. Provides updated neighborhood centroids for matching.

### `inep_file`
Points to the INEP school catalog file (`./data/inep_catalogo_das_escolas.csv.gz`). Contains official school registry with coordinates.

### `inep_data_all`
Cleans INEP school data and adds municipality codes. Standardizes school names and addresses for matching.

### `inep_data`
Filters INEP data by development mode municipalities. Reduces dataset size for faster development iterations.

---

## Polling Station Data

### `locais_file`
Points to the polling station addresses file (`./data/polling_stations_2006_2024.csv.gz`). Contains historical polling location data from TSE.

### `locais_all`
Imports and initially processes all polling station data. Standardizes formats and adds unique identifiers.

### `locais`
Filters polling stations by development mode states. Reduces dataset for testing while maintaining data relationships.

### `locais_filtered`
Applies Brasília filtering for municipal election years. Removes federal district entries that lack municipal elections.

### `validate_inputs`
Consolidated validation of all input datasets focusing on size checks. Verifies municipalities, INEP schools, and polling stations meet expected counts for the pipeline mode.

### `tse_files`
Lists TSE geocoded polling station files for 2018, 2020, and 2022. These files contain ground truth coordinates for validation.

### `tsegeocoded_locais`
Cleans and standardizes TSE geocoded polling station data. Creates unified ground truth dataset for model training.

### `panel_municipality_batches`
Creates batches of municipalities for panel ID processing. Groups municipalities to balance memory usage and processing time.

### `panel_batch_ids`
Extracts unique batch identifiers for dynamic branching. Enables parallel processing of panel ID creation.

### `panel_ids_by_batch`
Processes panel IDs for each municipality batch in parallel. Uses Fellegi-Sunter method to link polling stations across years.

### `panel_ids_combined`
Combines panel IDs from all batches into single dataset. Merges results while preserving linkage quality scores.

### `panel_ids`
Finalizes panel IDs by adding coordinate information. Enriches panel data with geocoded locations where available.

---

## String Matching Targets

### `municipalities_for_matching`
Extracts unique municipality codes for string matching operations. Creates list for parallel processing distribution.

### `municipality_sizes`
Calculates number of polling stations per municipality. Used for intelligent batch creation to balance workloads.

### `municipality_batch_assignments`
Creates balanced batches of municipalities for parallel string matching. Groups municipalities to optimize processing time and memory usage.

### `batch_ids`
Extracts unique batch identifiers for dynamic branching. Enables parallel execution of string matching tasks.

### `inep_string_match_batch`
Performs fuzzy string matching between polling stations and INEP schools by batch. Uses Levenshtein distance to find potential matches.

### `inep_string_match`
Combines INEP string matching results from all batches. Creates unified dataset of polling station to school matches.

### `schools_cnefe10_match_batch`
Matches polling stations to CNEFE 2010 schools by batch. Identifies schools in census data that could be polling locations.

### `schools_cnefe10_match`
Combines CNEFE 2010 school matching results from all batches. Aggregates matches for model training.

### `schools_cnefe22_match_batch`
Matches polling stations to CNEFE 2022 schools by batch. Uses updated census data for more recent matches.

### `schools_cnefe22_match`
Combines CNEFE 2022 school matching results. Creates comprehensive school match dataset.

### `cnefe10_stbairro_match_batch`
Matches polling stations to CNEFE 2010 streets and neighborhoods by batch. Provides fallback location estimates when exact matches fail.

### `cnefe10_stbairro_match`
Combines CNEFE 2010 street/neighborhood matches from all batches. Aggregates geographic reference matches.

### `cnefe22_stbairro_match_batch`
Matches polling stations to CNEFE 2022 streets and neighborhoods by batch. Uses latest census geographic references.

### `cnefe22_stbairro_match`
Combines CNEFE 2022 street/neighborhood matches. Creates updated geographic reference dataset.

### `agrocnefe_stbairro_match_batch`
Matches rural polling stations to agricultural CNEFE locations by batch. Handles polling stations in rural areas.

### `agrocnefe_stbairro_match`
Combines agricultural CNEFE matching results. Aggregates rural location matches.

### `geocodebr_match_batch`
Performs geocoding using external geocodebr service by batch. Attempts to geocode addresses through web service when available.

### `geocodebr_match`
Combines geocodebr matching results from all batches. Aggregates external geocoding results.

---

## Model Training and Prediction

### `model_data`
Combines all string matching results with demographic and geographic features. Creates comprehensive training dataset for machine learning model.

### `validate_model_data`
Validates the critical one-to-many merge creating the model training data. Ensures no data loss from left table and calculates expansion factor showing average matches per polling station.

### `trained_model`
Trains LightGBM model to select best coordinate matches. Uses gradient boosting to learn patterns in successful matches.

### `model_predictions`
Generates predictions for all potential polling station locations. Scores each candidate match based on learned patterns.

### `validate_predictions`
Critical validation ensuring all polling stations receive predictions. Stops the pipeline if any predictions are missing, preventing incomplete exports.

---

## Final Geocoding

### `geocoded_locais`
Finalizes polling station coordinates based on model predictions and ground truth. Selects best available coordinates for each location.

### `validate_geocoded_output`
Final critical validation ensuring output quality. Verifies unique key combinations, required columns present, and stops pipeline on failure to prevent bad exports.

---

## Validation and Reporting

The pipeline uses a simplified validation strategy focusing on critical quality checks:
- **Input validation**: Consolidated check of dataset sizes (municipalities, schools, polling stations)
- **Merge validation**: Ensures the critical one-to-many merge preserves all data
- **Prediction validation**: Stops pipeline if any polling station lacks predictions
- **Output validation**: Final quality gate checking uniqueness and completeness

### `validation_report`
Generates simplified validation report aggregating all critical checks. Reports on input data sizes, model merge integrity, prediction completeness, and final output quality with clear pass/fail status.

---

## Data Export

### `geocoded_export`
Exports final geocoded polling stations with validation metadata. Saves compressed CSV with all coordinate information.

### `panelid_export`
Exports panel identifiers linking polling stations across time. Creates temporal linkage file for longitudinal analysis.

### `data_quality_monitoring`
Runs continuous data quality monitoring and generates alerts. Tracks quality metrics over time and identifies degradation.

### `sanity_check_report`
Generates human-readable sanity check report using Quarto. Creates visual summaries of geocoding results and quality.

### `geocode_writeup`
Renders methodology documentation (production mode only). Compiles technical documentation of geocoding procedures.

---

Total targets: 80 (78 tar_target, 1 tar_render, 1 create_validation_report_target)

**Note**: The pipeline validation has been simplified to focus on critical checks only:
- Input data size validation (consolidated)
- Model merge integrity validation  
- Prediction completeness validation (pipeline-stopping)
- Final output quality validation (pipeline-stopping)