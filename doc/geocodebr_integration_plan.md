# geocodebr Integration Plan

## Overview
This document outlines the plan for integrating geocodebr as an additional coordinate source in the polling station geocoding pipeline, with precision levels as ML features.

## Current Pipeline Architecture

### Existing Coordinate Sources
1. **INEP Schools** - School coordinates from Ministry of Education
2. **CNEFE 2010/2022** - Census street and neighborhood coordinates  
3. **Agro CNEFE 2017** - Agricultural census coordinates
4. **TSE Geocoded** - Ground truth from electoral authority

### Pipeline Flow
1. Each source has string matching functions (e.g., `match_inep_muni()`)
2. Dynamic branching processes municipalities in parallel
3. Results combined in `make_model_data()` as long format
4. ML model selects best coordinates using features
5. `finalize_coords()` produces final geocoded output

## geocodebr Integration Design

### Key Characteristics
- **100% success rate** with high street-level precision when addresses are simplified
- Returns precision levels: `municipio`, `logradouro`, `numero`
- Address simplification (removing numbers/suffixes) dramatically improves matching
- São Paulo test: 92% street-level precision with simplified addresses vs 0% with full addresses
- Valuable coordinate source, not just a fallback

### Integration Strategy

#### 1. Add geocodebr Matching Function
Create `R/geocodebr_matching.R`:

```r
match_geocodebr_muni <- function(locais_muni, muni_ids) {
  # Prepare data for geocodebr
  dt_geocode <- locais_muni[, .(
    local_id = local_id,
    estado = sg_uf,
    municipio = nm_localidade,
    logradouro = ds_endereco,
    localidade = ds_bairro
  )]
  
  # Clean text (geocodebr is sensitive to encoding)
  dt_geocode[, `:=`(
    municipio = clean_text_for_geocodebr(municipio),
    logradouro = clean_text_for_geocodebr(logradouro),
    localidade = clean_text_for_geocodebr(localidade)
  )]
  
  # Batch geocoding with error handling
  result <- tryCatch({
    geocode(
      dt_geocode[, .(estado, municipio, logradouro)],
      campos_endereco = definir_campos(
        estado = "estado",
        municipio = "municipio",
        logradouro = "logradouro"
      ),
      resolver_empates = TRUE,
      verboso = FALSE,
      n_cores = 1
    )
  }, error = function(e) {
    # Return empty result on error
    data.table(
      lat = numeric(),
      lon = numeric(),
      precisao = character(),
      tipo_resultado = character()
    )
  })
  
  # Combine with local_id
  if (nrow(result) > 0) {
    result[, local_id := dt_geocode$local_id]
    
    # Create output matching existing format
    return(data.table(
      local_id = result$local_id,
      match_geocodebr = result$municipio,
      mindist_geocodebr = 0, # No distance metric
      match_long_geocodebr = result$lon,
      match_lat_geocodebr = result$lat,
      precisao_geocodebr = result$precisao,
      tipo_resultado_geocodebr = result$tipo_resultado
    ))
  } else {
    return(NULL)
  }
}
```

#### 2. Add to _targets.R Pipeline

```r
# After other matching targets, add:
tar_target(
  name = geocodebr_match_muni,
  command = {
    match_geocodebr_muni(
      locais_muni = locais[cod_localidade_ibge == municipalities_for_matching],
      muni_ids = muni_ids[id_munic_7 == municipalities_for_matching]
    )
  },
  pattern = map(municipalities_for_matching),
  iteration = "list"
),

tar_target(
  name = geocodebr_match,
  command = rbindlist(geocodebr_match_muni),
  storage = "worker",
  retrieval = "worker"
),

tar_target(
  name = validate_geocodebr_match,
  command = {
    result <- validate_string_match_stage(
      match_data = geocodebr_match,
      stage_name = "geocodebr_match",
      id_col = "local_id",
      score_col = "mindist_geocodebr"
    )
    
    # Report precision breakdown
    if (nrow(geocodebr_match) > 0) {
      precision_summary <- geocodebr_match[, .N, by = precisao_geocodebr]
      message("geocodebr precision breakdown:")
      print(precision_summary)
    }
    
    result
  }
)
```

#### 3. Update make_model_data()

Add geocodebr to the function signature and processing:

```r
make_model_data <- function(
  # ... existing parameters ...
  geocodebr_match,  # NEW
  muni_demo,
  muni_area,
  locais,
  tsegeocoded_locais
) {
  # ... existing code ...
  
  # Process geocodebr match data
  if (!is.null(geocodebr_match) && nrow(geocodebr_match) > 0) {
    # Melt to long format (only lat/lon, no distance)
    geocodebr_long <- geocodebr_match[, .(
      local_id,
      type = "geocodebr",
      value.long = match_long_geocodebr,
      value.lat = match_lat_geocodebr,
      value.mindist = 0,  # No distance metric
      precisao = precisao_geocodebr  # Keep precision for features
    )]
    
    # Add to final list
    final_list <- c(final_list, list(geocodebr_long))
  }
  
  # ... rest of function ...
}
```

#### 4. Add Precision Features

In the feature engineering section of `make_model_data()`:

```r
# Add geocodebr precision features
if (!is.null(geocodebr_match) && nrow(geocodebr_match) > 0) {
  # Create precision indicator features
  precision_features <- geocodebr_match[, .(
    local_id,
    geocodebr_precisao_municipio = as.integer(precisao_geocodebr == "municipio"),
    geocodebr_precisao_logradouro = as.integer(precisao_geocodebr == "logradouro"),
    geocodebr_precisao_numero = as.integer(precisao_geocodebr == "numero"),
    # Ordinal encoding (higher = more precise)
    geocodebr_precisao_score = fcase(
      precisao_geocodebr == "municipio", 1,
      precisao_geocodebr == "logradouro", 2,
      precisao_geocodebr == "numero", 3,
      default = 0
    )
  )]
  
  # Merge with addr_features
  addr_features <- merge(
    addr_features,
    precision_features,
    by = "local_id",
    all.x = TRUE
  )
  
  # Fill NAs with 0 (no geocodebr result)
  setnafill(addr_features, 
    cols = c("geocodebr_precisao_municipio", "geocodebr_precisao_logradouro", 
             "geocodebr_precisao_numero", "geocodebr_precisao_score"),
    fill = 0
  )
}
```

#### 5. Update model_data Target

```r
tar_target(
  name = model_data,
  command = make_model_data(
    cnefe10_stbairro_match = cnefe10_stbairro_match,
    cnefe22_stbairro_match = cnefe22_stbairro_match,
    schools_cnefe10_match = schools_cnefe10_match,
    schools_cnefe22_match = schools_cnefe22_match,
    agrocnefe_stbairro_match = agrocnefe_stbairro_match,
    inep_string_match = inep_string_match,
    geocodebr_match = geocodebr_match,  # NEW
    muni_demo = muni_demo,
    muni_area = muni_area,
    locais = locais,
    tsegeocoded_locais = tsegeocoded_locais
  ),
  storage = "worker",
  retrieval = "worker"
)
```

## ML Model Considerations

### New Features for Model
1. **One-hot precision encoding**: `geocodebr_precisao_municipio`, etc.
2. **Ordinal precision score**: 1-3 scale (higher = more precise)
3. **Interaction features**: `geocodebr_precisao_score * zona_rural`

### Expected Model Behavior
- Model should learn geocodebr's strengths:
  - High street-level precision in urban areas
  - Consistent coordinate quality
  - Good coverage for standard street names
  
- Model should consider geocodebr alongside other sources:
  - Use precision level as confidence indicator
  - `logradouro` precision indicates street-level match
  - `municipio` precision indicates city centroid
  - Let ML algorithm weigh all sources based on features

## Implementation Steps

1. **Create geocodebr functions** (`R/geocodebr_matching.R`)
   - Text cleaning function
   - Matching function
   - Helper utilities

2. **Update _targets.R**
   - Add geocodebr targets
   - Update model_data target
   - Add to validation report

3. **Update make_model_data()**
   - Add geocodebr parameter
   - Process geocodebr results
   - Create precision features

4. **Test in DEV_MODE**
   - Process subset of states
   - Verify integration works
   - Check feature generation

5. **Evaluate Model Performance**
   - Compare with/without geocodebr
   - Analyze when geocodebr is selected
   - Validate precision feature importance

## Memory and Performance Considerations

- geocodebr downloads ~2GB CNEFE data
- Process in batches to avoid memory issues
- Use single core (`n_cores = 1`) for stability
- Cache results to avoid re-geocoding

## Validation and Monitoring

- Track geocodebr success rate by state
- Monitor precision distribution
- Compare coordinates when geocodebr selected
- Log encoding issues and failures

## Rollback Plan

If issues arise:
1. Set `geocodebr_match = NULL` in make_model_data
2. Remove geocodebr targets from pipeline
3. Retrain model without geocodebr features

## Future Enhancements

1. **Use Parquet files directly** instead of geocode()
2. **Add CEP matching** when postal codes available
3. **Implement confidence scores** based on precision
4. **Cache geocoding results** by address hash