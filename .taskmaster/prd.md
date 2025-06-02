# Project Requirements Document: Brazilian Polling Station Geocoding

## Overview  
This project creates a comprehensive, validated dataset of geocoded Brazilian polling stations (2006-2022) using administrative data sources and machine learning techniques. The research addresses critical gaps in spatial electoral data for Brazil, enabling fine-grained analysis of voting patterns, electoral accessibility, and democratic participation across urban and rural contexts.

**Research Problem**: Existing commercial geocoding solutions (Google Maps API) perform poorly for Brazilian addresses, particularly in rural areas, with median errors exceeding 2km. Electoral research requires precise polling station locations to analyze spatial voting patterns, accessibility, and democratic representation.

**Academic Value**: Provides the research community with the most accurate spatial electoral dataset for Brazil, with documented methodology and validated error rates significantly lower than commercial alternatives.

## Core Features  

### 1. Multi-Source Administrative Data Integration
**What it does**: Systematically processes and normalizes addresses from CNEFE (Census), INEP (school catalog), and TSE (electoral) datasets
**Why it's important**: Leverages government administrative records that contain local knowledge and consistent formatting standards
**Implementation**: Robust ETL pipeline handling multiple data formats, encoding issues, and temporal inconsistencies

### 2. Advanced String Matching Framework
**What it does**: Implements fuzzy string matching using normalized Levenshtein distances across multiple address components (name, street, neighborhood)
**Why it's important**: Handles inconsistent address formatting and Portuguese-specific linguistic challenges
**Implementation**: Configurable matching algorithms with systematic normalization rules for Brazilian address conventions

### 3. Machine Learning Coordinate Selection
**What it does**: Trains boosted tree models to predict geocoding accuracy and select optimal coordinates from multiple candidates
**Why it's important**: Provides principled, data-driven approach to choosing among conflicting geocoding results
**Implementation**: LightGBM models with features including string similarity, municipal demographics, and data source characteristics

### 4. Panel Identifier Creation
**What it does**: Creates longitudinal identifiers tracking polling stations across elections using Fellegi-Sunter record linkage
**Why it's important**: Enables panel studies of electoral changes and institutional stability over time
**Implementation**: EM algorithm with Jaro-Winkler similarity and 1:1 matching constraints

### 5. Systematic Validation Framework
**What it does**: Implements comprehensive data quality checks and error estimation using TSE ground truth data
**Why it's important**: Ensures research reproducibility and provides uncertainty estimates for downstream analyses
**Implementation**: Automated validation targets with statistical testing and comparison benchmarks

## User Experience  

### Primary Users
- **Academic Researchers**: Political scientists, economists, sociologists studying Brazilian electoral behavior
- **Policy Analysts**: Government agencies and NGOs analyzing electoral accessibility and representation
- **Graduate Students**: Researchers needing reliable spatial electoral data for dissertations

### Key User Flows
1. **Data Download**: Simple access to final geocoded dataset with clear documentation
2. **Methodology Validation**: Transparent error rates and comparison with commercial alternatives
3. **Replication**: Complete pipeline reproducibility using `targets` framework
4. **Extension**: Clear interfaces for adding new years or data sources

### Research Integration Requirements
- **Documentation**: Comprehensive methodology documentation suitable for peer review
- **Replicability**: Complete code availability with dependency management (`renv`)
- **Validation**: Statistical evidence of accuracy with uncertainty quantification

## Technical Architecture  

### System Components
- **Data Pipeline**: `targets`-based workflow for reproducible analysis
- **Parallel Processing**: `future`/`mirai` framework for municipal-level parallelization
- **Storage**: Efficient data formats (`fst`, `qs`) for large administrative datasets
- **Validation**: `validate` package integration with custom checks

### Data Models
- **Polling Stations**: Normalized addresses with temporal and spatial identifiers
- **Administrative Sources**: Cleaned CNEFE, INEP, and TSE datasets with common schemas
- **Matching Results**: String similarity measures and coordinate candidates
- **Panel Structure**: Longitudinal identifiers with probability estimates

### Infrastructure Requirements
- **Memory**: 50GB+ RAM for full pipeline execution
- **Storage**: ~100GB for complete administrative datasets
- **Computing**: Multi-core system for parallel string matching operations
- **Dependencies**: R 4.4+ with specific package versions managed by `renv`

## Development Roadmap  

### Phase 1: Code Quality and Reproducibility (Foundation)
**Scope**: Establish robust development practices and eliminate technical debt
- Implement comprehensive test suite covering core functions
- Standardize coding style across all modules (data.table conventions)
- Complete migration from `future_lapply` to targets dynamic branching
- Consolidate string matching implementations into unified framework
- Add systematic validation checkpoints between all pipeline stages
- Add systematic validation for all joins. 

### Phase 2: Enhanced Data Integration (Expansion)
**Scope**: Improve data coverage and quality through additional sources
- Add 2024 election data processing capabilities
- Implement automated duplicate detection and resolution
- Enhance address normalization for Portuguese linguistic variations

### Phase 3: Advanced Methodological Features (Innovation)
**Scope**: Implement cutting-edge techniques for improved accuracy
- **Examine geocodebr Package**: Comprehensive analysis of IPEA's geocodebr methodology and implementation
- Develop ensemble models combining multiple ML approaches
- Add uncertainty quantification to coordinate predictions
- Implement active learning for iterative model improvement
- Create automated outlier detection for quality control

### Phase 4: Research Dissemination Tools (Community)
**Scope**: Enable broader research community adoption
- Develop R package for easy dataset access and manipulation
- Create interactive web interface for data exploration
- Build automated pipeline for regular dataset updates
- Implement citation tracking and usage analytics

## Logical Dependency Chain

### Foundation (Build First)
1. **Testing Infrastructure**: Essential for all subsequent development
2. **Code Standardization**: Required before major refactoring
3. **Validation Framework**: Needed to ensure quality during changes

### Core Pipeline Improvements (Build Second)
1. **Dynamic Branching Migration**: Improves scalability for larger datasets
2. **String Matching Consolidation**: Simplifies maintenance and enhances performance
3. **Duplicate Resolution**: Critical for data quality

### Advanced Features (Build Third)
1. **geocodebr Analysis**: Systematic comparison of methodologies and identification of applicable improvements
2. **Enhanced ML Models**: Depends on clean pipeline and validation framework
3. **Uncertainty Quantification**: Requires robust model infrastructure
4. **Additional Data Sources**: Benefits from improved processing capabilities

### Community Tools (Build Last)
1. **R Package**: Requires stable core functionality
2. **Web Interface**: Depends on reliable data processing pipeline
3. **Automated Updates**: Needs all quality control measures in place

## Risks and Mitigations  

### Technical Challenges
**Risk**: Memory constraints limiting dataset processing
**Mitigation**: Implement chunked processing and optimize data.table operations

**Risk**: String matching performance degradation with larger datasets
**Mitigation**: Profile bottlenecks and implement parallel processing optimizations

### Research Validity Risks
**Risk**: Systematic bias in geocoding accuracy across regions/demographics
**Mitigation**: Stratified validation analysis and bias correction techniques

**Risk**: Model overfitting to TSE ground truth data
**Mitigation**: Hold-out validation sets and cross-validation across time periods

**Risk**: Methodological improvements from geocodebr analysis may require significant pipeline restructuring
**Mitigation**: Incremental adoption of improvements with backward compatibility testing

### Reproducibility Concerns
**Risk**: Package dependency conflicts breaking pipeline
**Mitigation**: Comprehensive `renv` lockfiles and containerization options

**Risk**: Data source availability changes
**Mitigation**: Archive all source data and implement fallback procedures

## Appendix  

### geocodebr Package Analysis
**Target for Phase 3**: Comprehensive examination of IPEA's geocodebr package (https://github.com/ipeaGIT/geocodebr) for methodological improvements

**Key Features to Evaluate**:
- **Precision Classification System**: Six-level hierarchy (numero → numero_aproximado → logradouro → cep → localidade → municipio) with 4-character coding system distinguishing deterministic vs probabilistic matches
- **Automated Tie Resolution**: Uses CNEFE visit frequency data to break ties between competing matches
- **Spatial Interpolation**: For approximate number matching when exact numbers aren't found in CNEFE
- **Simplified User Interface**: Two-step process (field mapping + geocoding) vs. current complex pipeline
- **Direct CNEFE Integration**: More streamlined access to census data than current approach
- **Performance Claims**: Millions of addresses geocoded in minutes with 3GB cached dataset

**Potential Integration Opportunities**:
1. **Precision Taxonomy**: Adopt their precision classification system for better uncertainty quantification
2. **Tie-Breaking Algorithms**: Implement visit-frequency based tie resolution for polling stations
3. **Interpolation Methods**: Add spatial interpolation for number-based matching
4. **API Design**: Develop simplified interface for research community using their field-mapping approach
5. **Caching Strategy**: Implement efficient data caching similar to their 3GB CNEFE cache
6. **Validation Framework**: Compare accuracy against geocodebr results for overlapping addresses

**Research Questions**:
- How does geocodebr's accuracy compare to your ML-based approach for polling station addresses?
- Can their precision categories improve your uncertainty estimates?
- Are their tie-breaking methods applicable to educational institution matching?
- What performance optimizations can be borrowed from their implementation?

### Current Performance Benchmarks
- Median geocoding error: 0.89km (vs 2.83km for Google Maps API)
- Rural area improvement: 30x better accuracy than commercial solutions
- Processing time: ~6 hours for complete dataset on 16-core system

### Key Research Publications
- Methodology paper documenting approach and validation
- Comparison study with commercial geocoding services
- Application papers demonstrating electoral research uses

### Technical Dependencies
- R 4.4+ with specific package ecosystem
- Administrative data access agreements with IBGE
- Computational resources for large-scale string matching

### Future Research Extensions
- Integration with demographic census data for voter accessibility analysis
- Temporal analysis of polling station location changes
- International applicability to other federal electoral systems