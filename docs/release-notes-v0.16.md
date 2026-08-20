
## Version 0.16 

This release improves the quality of the coordinates: a series of 
methodology changes cut the median error by 40%. All coordinates and panel identifiers were regenerated, so this release supersedes 0.15
in its entirety.

### What's new

- **More accurate coordinates.** Out-of-fold median error fell from 46 m to **28 m**, and
  the share of stations within 500 m of their true location rose from 74.6% to **85.1%**.
  Rural stations improved most: median error fell from 211 m to 46 m.
- **A calibrated error bound replaces `pred_dist`.** The new `conf_dist_km` column is an
  upper bound on the coordinate's error that holds for at least 90% of stations
  (achieved out-of-fold coverage: 90.0%). This error bound is calculated using conformal inference.
- **A new section-to-panel file.** `section_panel_mapping.csv.gz` maps each electoral
  section (seção) directly to a `panel_id` — 4.37 million section-election records —
  so section-level TSE election results can be joined to the panel without first
  aggregating them to the polling-station level. Column documentation is in the README.

### ⚠️ Interface changes

- **`pred_dist` is gone; use `conf_dist_km`** (in both files).  `conf_dist_km` is a calibrated upper bound: the true
  location is within `conf_dist_km` of the published coordinate for at least 90% of
  stations. Use it to filter by accuracy — e.g. `conf_dist_km <= 1` keeps stations whose
  error is very likely under a kilometre. It is 0 for stations with a TSE-provided
  coordinate. 
- **`local_id` is still not comparable across releases.** It is stable within a release
  but re-derived on each rebuild — do not join on `local_id` across releases. Use
  `panel_id` to track a station over time.

### Accuracy (honest, out-of-fold)

Measured on the 258,555 TSE-covered station-years, with each station scored by a model
refit on other municipalities (never on itself). Errors are haversine distances to the
TSE coordinate:

| Stratum | Median error | Within 500 m | Within 1 km |
| --- | --- | --- | --- |
| Overall | 28 m | 85.1% | 89.4% |
| Urban | 26 m | 92.0% | 95.3% |
| Rural | 46 m | 69.7% | 76.1% |



### Changes


- `geocodebr` added as a candidate coordinate source, fed the full structured address and
  contributing its own uncertainty radius as a model feature (#38)
- Per-field string-similarity features: school name, street, neighborhood, and whole
  address line scored separately for every candidate (#39)
- Cross-source consensus features, letting the model treat independent datasets agreeing
  on a location as evidence (#43)
- Candidate ranking on trigram Jaccard similarity instead of Jaro-Winkler (#149)
- Separate models for picking the best candidate (expected error) and for the published
  bound (`conf_dist_km`), so the selection rule no longer favors predictable-but-worse
  candidates (#143, #44)
- Panel coordinates chosen by the same expected-error rule, with TSE-provided coordinates
  always taking precedence (#147)

### Files

- `geocoded_polling_stations.csv.gz` — geocoded coordinates (2006–2024)
- `panel_ids.csv.gz` — panel identifiers linking stations across elections
- `section_panel_mapping.csv.gz` — electoral sections mapped to panel identifiers (new in 0.16)

Methodology and column documentation: [README](https://github.com/fdhidalgo/geocode_br_polling_stations#readme)
and the [methodology document](https://raw.githack.com/fdhidalgo/geocode_br_polling_stations/master/doc/geocoding_procedure.html).
