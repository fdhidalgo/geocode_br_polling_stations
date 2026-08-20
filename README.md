# Geocoding Brazilian Polling Stations with Administrative Data Sets

This repository contains the code to geocode polling stations in Brazil. We leverage administrative datasets to geocode all polling stations used in elections from 2006 to 2024.

## Overview

This project provides:
- **Geocoded coordinates** for over 944,000 Brazilian polling station records across ten elections (2006-2024)
- **Panel identifiers** to track polling stations across elections
- **Reproducible pipeline** using R and the `targets` package
- **Fuzzy string matching** algorithms that often outperform commercial geocoding services

We detail our methodology and limitations of our method in this [document](https://raw.githack.com/fdhidalgo/geocode_br_polling_stations/master/doc/geocoding_procedure.html). As we explain in that document, our method often performs better than commercial solutions like the [Google Maps Geocoding Service](https://developers.google.com/maps/documentation/geocoding/overview), particularly in rural areas. Despite our best efforts, however, it is important to note that this procedure inevitably will make mistakes and consequently some coordinates will be incorrect. 

The latest dataset of geocoded polling stations can be found in the compressed csv file  linked to on the [release page](https://github.com/fdhidalgo/geocode_br_polling_stations/releases/latest). Version notes can be found [here](https://github.com/fdhidalgo/geocode_br_polling_stations/releases).

## Data

The dataset (`geocoded_polling_stations.csv.gz`) contains the following variables:

- `local_id`: Unique identifier for the polling station in a given election. This will vary across time, even for polling stations that are active in multiple elections. **Note:** `local_id` values are stable within a single release but are **not comparable across releases** — do not use them to join data from different releases. Use `panel_id` (see below) to track a station across elections.

- `ano`: Election year

- `sg_uf`: State abbreviation

- `cd_localidade_tse`: Municipal identifier used by the TSE.

- `cod_localidade_ibge`: Municipal identifier used by the IBGE. Complete for every record as of v0.16.

- `nr_zona`: Electoral zone number

- `nr_locvot`: Polling station number

- `nr_cep`: Brazilian postal code

- `nm_localidade`: Municipality

- `nm_locvot`: Name of polling station

- `ds_endereco`: Street address

- `ds_bairro`: neighborhood

- `pred_long`: Longitude as selected by our model.

- `pred_lat`: Latitude as selected by our model

- `conf_dist_km`: Calibrated upper bound, in kilometres, on the error of the coordinate in `long`/`lat`. The true location is within `conf_dist_km` of the published one for at least 90% of polling stations. Use it to filter coordinates by accuracy — e.g. keeping `conf_dist_km <= 1` keeps stations whose error is very likely under a kilometre. Set to 0 for stations with a TSE-provided coordinate, which is field-collected rather than estimated.

- `tse_lat`: Latitude provided by the TSE. This is only available for a  subset of data.

- `tse_long`: Longitude provided by the TSE. This is only available for a subset of data.

- `long`: Longitude to use for analysis. This is the coordinate provided by the TSE when available (`tse_long`), and otherwise the coordinate selected by our model (`pred_long`).

- `lat`: Latitude to use for analysis. This is the coordinate provided by the TSE when available (`tse_lat`), and otherwise the coordinate selected by our model (`pred_lat`).

### Panel Identifiers
We also created panel identifiers that track a given polling station over time. Because panel identifiers provided by the electoral authorities can change over time, we must use a fuzzy matching procedure to create our own panel identifiers. The process implemented to generate the panel identifiers consists of six stages. First, we subset the data at the state level for each electoral year. Then, we generate every possible pair of polling stations at the municipality level for every consecutive electoral year. The next step is to calculate the [Jaro-Winkler](https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance) string similarity for each possible pair on two strings: the normalized name and the normalized address of the location. 

Subsequently, we use the Fellegi-Sunter framework for record linkage to choose the best matches as implemented in the [`reclin2`](https://github.com/djvanderlaan/reclin2) package. Specifically, we use an Expectation-Maximization (EM) algorithm to calculate the probabilities of a given pair being a match. We retain pairs with a probability greater than 0.5. To choose the final matches, we select the best matches under the constraint that each polling station can only be matched once. Finally, we construct the panel by combining the pairs matched in each consecutive year and establishing a unique panel identifier for those observations.

 The dataset `panel_ids.csv.gz` has the following variables:

- `panel_id`: unique panel identifier. Units with the same `panel_id` are classified to be the same polling station in two different election years according to our fuzzy matching procedure. 
- `local_id`: polling station identifier. Use this variable to merge with the coordinates data (one `local_id` per polling-station-election, so it also identifies the election year via that join). 
- `long`: This is a longitude variable that is constant for all observations with the same `panel_id` across years. To choose among coordinates from different years, we select the one whose expected error is smallest, the same criterion used to pick each polling station's own coordinate. A TSE-provided coordinate, being field-collected, always wins. Ties are broken by selecting the longitude from the latest year.
- `lat`: This is a latitude variable that is constant for all observations with the same `panel_id` across years. Chosen by the same rule as `long`, and from the same year.
- `conf_dist_km`: The distance bound of the chosen coordinate, defined as in the coordinates file above. Constant within a `panel_id`, since the whole panel shares one coordinate. It is the bound of the year that was selected, which is not necessarily the smallest bound in the panel — the selection ranks on expected error, not on the bound.

### Section-to-Panel Mapping

Election results from the TSE are reported at the "seção" (section) level. The dataset `section_panel_mapping.csv.gz` maps each section directly to a `panel_id`, so section-level results can be joined to the panel without aggregating to the polling-station level first. It has the following variables:

- `nr_secao`: Section number
- `nr_zona`: Electoral zone number
- `nr_local_votacao`: Polling station number
- `ano`: Election year
- `estado_abrev`: State abbreviation
- `cd_localidade_tse`: Municipal identifier used by the TSE. Part of the join key, because zone and section numbers are reused across municipalities.
- `nm_localidade`: Municipality
- `panel_id`: Panel identifier, as in `panel_ids.csv.gz`

Each section appears once per election year (unique on `nr_secao`, `nr_zona`, `ano`, `estado_abrev`, `cd_localidade_tse`). Sections whose polling station could not be linked to a panel are omitted rather than published with a guess, as are a handful of sections listed at more than one polling place in the source data.

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
- **`crew`** for parallel processing (two controllers: standard and memory-limited)

### Project Structure

```
├── _targets.R           # Pipeline manifest
├── R/                   # Core functions
│   ├── config.R         # Pipeline config + crew controllers
│   ├── data_cleaning.R
│   ├── string_matching.R
│   ├── model.R          # Match-selection and error-bound models
│   ├── panel_creation.R
│   ├── evaluation.R     # Out-of-fold accuracy evaluation
│   ├── validation.R
│   └── utilities.R
├── data/               # Input data
├── output/             # Generated outputs
└── doc/                # Methodology documentation
```


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
| `geocodebr` Address Geocoder       | [`geocodebr` Package](https://ipea.github.io/geocodebr/) (resolves addresses against IBGE's address database; used as one of the candidate coordinate sources)                                          |



### Reporting Issues

Please report bugs or request features through [GitHub Issues](https://github.com/fdhidalgo/geocode_br_polling_stations/issues).


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
