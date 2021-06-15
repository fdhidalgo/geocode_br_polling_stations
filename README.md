# Geocoding Brazilian Polling Stations with Administrative Data Sets

This repository contains the code to geocode polling stations in Brazil. We leverage administrative datasets to geocode all polling stations used in elections from 2006 to 2018.

We detail our methodology and limitations of our method in this [document](https://raw.githack.com/fdhidalgo/geocode_br_polling_stations/master/doc/geocoding_procedure.html). As we explain in that document, our method often performs better than commercial solutions like the [Google Maps Geocoding Service](https://developers.google.com/maps/documentation/geocoding/overview), particularly in rural areas. Despite our best efforts, however, it is important to note that this procedure inevitably will make mistakes and consequently some coordinates will be incorrect. 

The latest dataset of geocoded polling stations can be found in the compressed csv file  linked to on the [release page](https://github.com/fdhidalgo/geocode_br_polling_stations/releases/latest). Version notes can be found [here](https://github.com/fdhidalgo/geocode_br_polling_stations/releases).

## Data

The dataset (`geocoded_polliing_stations.csv.gz`) contains the following variables:

-   `local_id`: Unique identifier for the polling station in a given election. This will vary across time, even for polling stations that are active in multiple elections.

-   `ano`: Election year

-   `sg_uf`: State abbreviation

-   `cd_localidade_tse`: Municipal identifier used by the TSE.

-   `cd_localidade_ibge`: Municipal identifier used by the IBGE

-   `nr_zona`: Electoral zone number

-   `nr_locvot`: Polling station number

-   `nr_cep`: Brazilian postal code

-   `nm_localidade`: Municipality

-   `nm_locvot`: Name of polling station

-   `ds_endereco`: Street address

-   `ds_bairro`: neighborhood

-   `pred_long`: Longitude as selected by our model.

-   `pred_lat`: Latitude as selected by our model

-   `pred_dist`: Predicted distance between chosen longitude and latitude and true longitude and latitude. This variable can be used to filter coordinates based on their likely accuracy.

-   `tse_lat`: Latitude provided by the TSE. This is only available for a small subset of data.

-   `tse_long`: Longitude provided by the TSE. This is only available for a small subset of data.

-   `long`: Longitude as predicted by the model or provided by the TSE.

-   `lat`: Latitude as predicted by the model or provided by the TSE.

We also created panel identifiers that track a given polling station over time. Note to construct this, we had to use fuzzy string matching of address and polling station name. The dataset `panel_ids.csv.gz` has the following variables:

- `ano`: year
- `panel_id`: unique panel identifier. Units with the same `panel_id` are classified to be the same polling station in two different election years according to our fuzzy matching procedure. 
- `local_id`: polling station identifier. Use this variable to merge with the coordinates data. 
- `panel_match_prop`: this variable measures the quality of the match. This is the proportion of words in the pollling station name and address that are exactly the same across years.  A 1 indicates a perfect match between polling station name and address. 

Note that for a small number of cases, a given polling station can be matched to multiple polling stations from an earlier year. This occurs when a later potential match is the best match for multiple polling stations in an earlier election. 

## Code
### Running the Geocoding Pipeline

We used the open source language *R* (version 4.0.3) to process the files and geocode the polling stations. To manage the pipeline that imports and processes all the data, we use the [`drake`](https://github.com/ropensci/drake) package.

Assuming all the relevant data is in the `./data` folder, you can reconstruct the dataset using the following code:

``` r
#Set working directory to project directory
setwd(".")
renv::restore() #to install necessary packages
drake::r_make() # to run pipepeline
```

Options to modify how the pipeline runs (e.g. parallel processing options) can be found in the [`_drake.R`](./_drake.R) file. The pipeline is in the [`plan.R`](./R/plan.R) file. We use the [`renv`](https://rstudio.github.io/renv/index.html) package to manage package dependencies. To ensure that you are using the right package versions, invoke `renv::restore()` when the working directory is set to the github repo directory.

Given the size of some of the data files, you will likely need at least 50GB of RAM to run the code.

## Data Sources
Because of the size of some of the administrative datasets, we cannot host all the data necessary to run the code on Github.
Datasets marked with a \* can be found at the associated link in the table below but not in this Github repo.
All other data can be found in the `data` folder.

| Data                             | Source                                                                                                                                                                                                 |
|----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 2010 CNEFE\*                     | [IBGE FTP Server](https://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Cadastro_Nacional_de_Enderecos_Fins_Estatisticos/)                                                                               |
| 2017 CNEFE\*                     | [IBGE Website](https://www.ibge.gov.br/estatisticas/economicas/agricultura-e-pecuaria/21814-2017-censo-agropecuario.html?edicao=23751&t=resultados)                                                    |
| INEP School Catalog              | [INEP Website](https://inepdata.inep.gov.br/analytics/saw.dll?dashboard&NQUser=inepdata&NQPassword=Inep2014&PortalPath=%2Fshared%2FCenso%20da%20Educação%20Básica%2F_portal%2FCatálogo%20de%20Escolas) |
| Polling Stations Geocoded by TSE\* | [TSE](https://www.tse.jus.br/hotsites/pesquisas-eleitorais/eleitorado_anos/2018.html)                                                                |
| Polling Station Addresses        | [Centro de Política e Economia do Setor Público](https://www.cepespdata.io)                                                                                                                            |
| Census Tract Shape Files\*       | [`geobr` Package](https://github.com/ipeaGIT/geobr)                                                                                                                                                    |
| Municipal Demographic Variables  | [Atlas do Desenvolvimento Humano no Brasil](http://www.atlasbrasil.org.br)                                                                                                                             |

## Acknowledgements

Thanks to:

-   [Yuri Kasahara](https://www.researchgate.net/profile/Yuri_Kasahara2) for ideas and assistance in debugging

-   George Avelino, Mauricio Izumi, Gabriel Caseiro, and Daniel Travassos Ferreira at [FGV/CEPESP](https://www.cepespdata.io) for data and advice
-  Marco Antonio Faganello for excellent assistance at the early stages of the project. 

## Other Approaches

-   Spatial Maps at <http://spatial2.cepesp.io>

-   [Pindograma](https://github.com/pindograma/mapa)
