# Panel ID Processing Optimization Summary

## Overview
Optimized the panel ID creation process by switching from state-level to municipality-level parallelization, potentially improving performance by 10-50x.

## Key Changes

### 1. Municipality-Based Parallelization
- **Before**: Processing 27 states in parallel (uneven workloads)
- **After**: Processing ~5,570 municipalities in parallel (balanced workloads)
- **Files Modified**: `_targets.R`, new file `R/panel_id_municipality_fns.R`

### 2. Smart Batching by Size
- Municipalities grouped into batches based on polling station count:
  - **Mega cities** (>10,000 stations): Individual processing with dedicated workers
  - **Large cities** (>5,000 stations): Paired processing with memory-limited workers
  - **Medium/Small** (<5,000 stations): Batched to ~5,000 stations per batch
- Different controller types assigned based on batch complexity

### 3. Memory Optimizations
- Pre-compute year subsets to avoid repeated filtering
- Keep only necessary columns during processing
- Explicit garbage collection after each year-pair
- Process municipalities sequentially within batches

### 4. Performance Monitoring
- New `monitor_panel_progress()` function tracks:
  - Total municipalities and polling stations
  - Batch distribution by type
  - Processing progress (if tracking completed batches)

## Implementation Details

### New Functions in `R/panel_id_municipality_fns.R`:
1. `create_municipality_batches()` - Creates optimized batch assignments
2. `process_panel_ids_municipality_batch()` - Processes a batch of municipalities
3. `get_batch_controller()` - Determines appropriate worker type
4. `monitor_panel_progress()` - Tracks processing progress
5. `create_and_select_best_pairs_optimized()` - Optimized pair matching

### Modified Targets:
- Replaced `panel_states_all` with `panel_municipality_batches`
- Replaced `panel_ids_by_state` with `panel_ids_by_batch`
- Added `panel_batch_summary` for monitoring

## Expected Benefits

1. **Better Load Balancing**: São Paulo city processes separately from small towns
2. **Increased Parallelization**: Up to 200x more processing units
3. **Memory Efficiency**: Large cities use specialized workers
4. **Progress Visibility**: Real-time monitoring of batch completion

## Usage

In development mode:
```r
# Processes only AC and RR states with smaller batches
targets::tar_make(panel_municipality_batches)
```

In production mode:
```r
# Processes all Brazilian municipalities in optimized batches
targets::tar_make(panel_ids)
```

## Next Steps

1. Test with development data to verify correctness
2. Monitor memory usage with large municipalities
3. Consider further optimizations:
   - Year-pair parallelization within municipalities
   - Approximate string matching for initial filtering
   - Caching of distance computations