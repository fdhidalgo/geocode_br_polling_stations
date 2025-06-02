# Geocoding Brazilian Polling Stations with Administrative Data Sets

This repository contains the code to geocode polling stations in Brazil. We leverage administrative datasets to geocode all polling stations used in elections from 2006 to 2022.

## Overview

This project provides:
- **Geocoded coordinates** for ~400,000 Brazilian polling stations (2006-2022)
- **Panel identifiers** to track polling stations across elections
- **Reproducible pipeline** using R and the `targets` package
- **Fuzzy string matching** algorithms that often outperform commercial geocoding services

We detail our methodology and limitations of our method in this [document](https://raw.githack.com/fdhidalgo/geocode_br_polling_stations/master/doc/geocoding_procedure.html). As we explain in that document, our method often performs better than commercial solutions like the [Google Maps Geocoding Service](https://developers.google.com/maps/documentation/geocoding/overview), particularly in rural areas. Despite our best efforts, however, it is important to note that this procedure inevitably will make mistakes and consequently some coordinates will be incorrect. 

The latest dataset of geocoded polling stations can be found in the compressed csv file  linked to on the [release page](https://github.com/fdhidalgo/geocode_br_polling_stations/releases/latest). Version notes can be found [here](https://github.com/fdhidalgo/geocode_br_polling_stations/releases).

## Data

The dataset (`geocoded_polling_stations.csv.gz`) contains the following variables:

- `local_id`: Unique identifier for the polling station in a given election. This will vary across time, even for polling stations that are active in multiple elections.

- `ano`: Election year

- `sg_uf`: State abbreviation

- `cd_localidade_tse`: Municipal identifier used by the TSE.

- `cd_localidade_ibge`: Municipal identifier used by the IBGE

- `nr_zona`: Electoral zone number

- `nr_locvot`: Polling station number

- `nr_cep`: Brazilian postal code

- `nm_localidade`: Municipality

- `nm_locvot`: Name of polling station

- `ds_endereco`: Street address

- `ds_bairro`: neighborhood

- `pred_long`: Longitude as selected by our model.

- `pred_lat`: Latitude as selected by our model

- `pred_dist`: Predicted distance between chosen longitude and latitude and true longitude and latitude. For polling stations with coordinates provided by the TSE, this is set to 0.  This variable can be used to filter coordinates based on their likely accuracy.

- `tse_lat`: Latitude provided by the TSE. This is only available for a  subset of data.

- `tse_long`: Longitude provided by the TSE. This is only available for a subset of data.

- `long`: Longitude as predicted by the model or provided by the TSE.

- `lat`: Latitude as predicted by the model or provided by the TSE.

### Panel Identifiers
We also created panel identifiers that track a given polling station over time. Because panel identifiers provided by the electoral authorities can change over time, we must use a fuzzy matching procedure to create our own panel identifiers. The process implemented to generate the panel identifiers consists of six stages. First, we subset the data at the state level for each electoral year. Then, we generate every possible pair of polling stations at the municipality level for every consecutive electoral year. This can be as few as three possible pairs for the least populous municipality in Brazil, Serra da Saudade-MG, which had one polling station in 2006 and three in 2008, or as many as millions of pairs for the most populous municipality, São Paulo-SP, which has over 1,500 polling stations in each electoral year. The next step is to calculate the [Jaro-Winkler](https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance) string similarity for each possible pair on two strings: the normalized name and the normalized address of the location. 

Subsequently, we use the Fellegi-Sunter framework for record linkage to choose the best matches as implemented in the [`reclin2`](https://github.com/djvanderlaan/reclin2) package. Specifically, we use an Expectation-Maximization (EM) algorithm to calculate the probabilities of a given pair being a match. We retain pairs with a probability greater than 0.5. To choose the final matches, we select the best matches under the constraint that each polling station can only be matched once. Finally, we construct the panel by combining the pairs matched in each consecutive year and establishing a unique panel identifier for those observations.

 The dataset `panel_ids.csv.gz` has the following variables:

- `ano`: year
- `panel_id`: unique panel identifier. Units with the same `panel_id` are classified to be the same polling station in two different election years according to our fuzzy matching procedure. 
- `local_id`: polling station identifier. Use this variable to merge with the coordinates data. 
- `long`: This is a longitude variable that is constant for all observations with the same `panel_id` across years. To choose among coordinates from different years, we select the one with the smallest predicted distance to the true location. Ties are broken by selecting the longitude from the latest year.
- `lat`: This is a latitude variable that is constant for all observations with the same `panel_id` across years. To choose among coordinates from different years, we select the one with the smallest predicted distance to the true location. Ties are broken by selecting the latitude from the latest year.

## Development Setup

### Prerequisites

- R >= 4.4.0
- 50GB+ RAM (required for processing large administrative datasets)
- Git for version control

### Initial Setup

1. Clone the repository:
```bash
git clone https://github.com/fdhidalgo/geocode_br_polling_stations.git
cd geocode_br_polling_stations
```

2. Restore R package dependencies using renv:
```r
renv::restore()
```

3. Download required administrative datasets (see [Data Sources](#data-sources) section)

### Development Environment

This project uses:
- **`renv`** for reproducible package management
- **`targets`** for pipeline orchestration
- **`data.table`** for efficient data manipulation
- **`future`** for parallel processing

### Project Structure

```
├── _targets.R           # Pipeline configuration
├── R/                   # Core functions
│   ├── data_cleaning_fns.R
│   ├── string_matching_geocode_fns.R
│   ├── panel_id_fns.R
│   └── functions_validate.R
├── data/               # Input data
├── output/             # Generated outputs
└── doc/                # Documentation
```

## Running the Pipeline

### Quick Start

We used the open source language *R* (version 4.4.0) to process the files and geocode the polling stations. To manage the pipeline that imports and processes all the data, we use the [`targets`](https://github.com/ropensci/targets) package.

Assuming all the relevant data is in the `./data` folder, you can reconstruct the dataset using:

```r
# Run the complete pipeline
targets::tar_make()

# Run specific targets
targets::tar_make(names = "target_name")

# Visualize pipeline
targets::tar_visnetwork()

# Check pipeline status
targets::tar_outdated()
```

### Pipeline Configuration

The pipeline can be configured in `_targets.R`:
- **Parallel workers**: Adjust `future::plan()` settings
- **Memory limits**: Currently set to 2GB for future globals
- **Target-specific options**: Modify individual target settings

### Common Pipeline Commands

```r
# Clean and rebuild everything
targets::tar_destroy()
targets::tar_make()

# Debug a specific target
targets::tar_load(target_name)

# View target dependencies
targets::tar_deps(target_name)
```

## Testing

*Note: Test infrastructure is currently being implemented (Task #1)*

Once implemented, tests can be run with:
```r
# Run all tests
devtools::test()

# Run specific test file
testthat::test_file("tests/testthat/test-string_matching.R")
```

## Code Style Guide

### R Code Conventions

- **Naming**: Use snake_case for functions and variables
- **Functions**: Document with Roxygen2 comments
- **Data manipulation**: Use `data.table` syntax consistently
- **File paths**: Use relative paths or `here::here()`

### Pre-commit Hooks

*Note: Pre-commit hooks are being implemented*

The project will use pre-commit hooks for:
- Code formatting with `styler`
- Linting with `lintr`
- Spell checking documentation

### Best Practices

1. **Memory Management**: Monitor memory usage with large datasets
2. **Parallel Processing**: Use municipality-level parallelization
3. **Validation**: Add checks after major data transformations
4. **Documentation**: Update function documentation when modifying code

## Working with the Data

### Merging Coordinates with Electoral Data
While one can get disaggregated electoral data directly from the TSE, I recommend obtaining polling station-level data from  [CEPESP DATA](https://www.cepespdata.io), as it has been cleaned, aggregated, and standardized. 

For merging with electoral data provided by the TSE, you will typically have to work with data reported at the "seção" level, which is below the polling station level. Generally, one will need to aggregate the "seção"-level data to the polling station level, using municipality code, electoral zone code, and polling station code. Once aggregated, you can then merge with the coordinates data provided here. 

As an example, I provide code for merging the [2018 electorate data](https://dadosabertos.tse.jus.br/dataset/eleitorado-2018/resource/368612e7-fa5d-420a-9013-7ee9d1dbd16a), which is reported at the "seção" level, with the coordinates data.

``` r
library(data.table) #for importing and aggregating data

polling_coord <- fread("geocoded_polling_stations.csv.gz")
#Subset on 2018 polling stations
coord_2018 <- polling_coord[ano == 2018, ]

#import 2018 electorate data from TSE
electorate_2018 <- fread("eleitorado_local_votacao_2018.csv", encoding = "Latin-1")

#aggregate data to the polling station level
electorate_local18 <- electorate_2018[, .(electorate = sum(QT_ELEITOR)),
        by = c("CD_MUNICIPIO", "NR_ZONA", "NR_LOCAL_VOTACAO")
]

#merge by municipality, zone, and polling station identifier
coord_electorate18 <- merge(coord_2018, electorate_local18,
        by.x = c("cd_localidade_tse", "nr_zona", "nr_locvot"),
        by.y = c("CD_MUNICIPIO", "NR_ZONA", "NR_LOCAL_VOTACAO")
)
```


## Data Sources
Because of the size of some of the administrative datasets, we cannot host all the data necessary to run the code on Github.
Datasets marked with a \* can be found at the associated link in the table below but not in this Github repo.
All other data can be found in the `data` folder.

| Data                               | Source                                                                                                                                                                                                 |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 2010 CNEFE\*                       | [IBGE FTP Server](https://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Cadastro_Nacional_de_Enderecos_Fins_Estatisticos/)                                                                             |
| 2017 CNEFE\*                       | [IBGE Website](https://www.ibge.gov.br/estatisticas/economicas/agricultura-e-pecuaria/21814-2017-censo-agropecuario.html?edicao=23751&t=resultados)                                                    |
| 2022 CNEFE\*                       | [IBGE Website](https://www.ibge.gov.br/estatisticas/sociais/populacao/38734-cadastro-nacional-de-enderecos-para-fins-estatisticos.html?=&t=downloads)                                                  |
| INEP School Catalog                | [INEP Website](https://inepdata.inep.gov.br/analytics/saw.dll?dashboard&NQUser=inepdata&NQPassword=Inep2014&PortalPath=%2Fshared%2FCenso%20da%20Educação%20Básica%2F_portal%2FCatálogo%20de%20Escolas) |
| Polling Stations Geocoded by TSE\* | [TSE](https://www.tse.jus.br/hotsites/pesquisas-eleitorais/eleitorado_anos/2018.html)                                                                                                                  |
| Polling Station Addresses          | [Centro de Política e Economia do Setor Público](https://www.cepespdata.io)                                                                                                                            |
| Census Tract Shape Files\*         | [`geobr` Package](https://github.com/ipeaGIT/geobr)                                                                                                                                                    |
| Municipal Demographic Variables    | [Atlas do Desenvolvimento Humano no Brasil](http://www.atlasbrasil.org.br)                                                                                                                             |

## Contributing

We welcome contributions!

### Reporting Issues

Please report bugs or request features through [GitHub Issues](https://github.com/fdhidalgo/geocode_br_polling_stations/issues).

## Troubleshooting

### Common Issues

1. **Memory errors**: Reduce parallel workers in `_targets.R`
2. **Package conflicts**: Run `renv::status()` and `renv::restore()`
3. **Missing data files**: Check [Data Sources](#data-sources) for download links
4. **Pipeline failures**: Use `targets::tar_meta()` to inspect errors

### Getting Help

- Check existing [GitHub Issues](https://github.com/fdhidalgo/geocode_br_polling_stations/issues)
- Review pipeline status with `targets::tar_visnetwork()`
- Inspect specific target errors with `targets::tar_meta(target_name)$error`

## Acknowledgements

Thanks to:

- Lucas Nobrega for help improving the panel identifier code. 

- [Yuri Kasahara](https://www.researchgate.net/profile/Yuri_Kasahara2) for ideas and assistance in debugging

- George Avelino, Mauricio Izumi, Gabriel Caseiro, and Daniel Travassos Ferreira at [FGV/CEPESP](https://www.cepespdata.io) for data and advice
- Marco Antonio Faganello for excellent assistance at the early stages of the project. 

## License

*License information to be added*

## Citation

If you use this data in your research, please cite:

*Citation format to be added*

## Other Approaches

- Spatial Maps at <http://spatial2.cepesp.io>

- [Pindograma](https://github.com/pindograma/mapa)
