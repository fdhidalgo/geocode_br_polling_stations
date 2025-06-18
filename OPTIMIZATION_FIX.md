# Pipeline Optimization Fix

## Issue
The pipeline failed with error: `Column 'uf' not found in data` when processing the `muni_ids` target.

## Root Cause
The `filter_by_dev_mode` function was using default column name "uf" but the actual column in muni_identifiers.csv is "estado_abrev".

## Fix Applied
Updated _targets.R line 117:
```r
# Before:
command = filter_by_dev_mode(muni_ids_all, pipeline_config)

# After:
command = filter_by_dev_mode(muni_ids_all, pipeline_config, state_col = "estado_abrev")
```

## Verification
The pipeline is now running successfully with all optimizations:
- 8 workers in DEV_MODE (was 2)
- 28 workers in production (was 4)
- Smaller batch sizes for better parallelization
- Memory-limited controller for CNEFE operations

## Next Steps
1. Monitor pipeline execution with `targets::tar_progress_summary()`
2. Check memory usage during CNEFE operations
3. Review runtime after completion with `targets::tar_meta()`