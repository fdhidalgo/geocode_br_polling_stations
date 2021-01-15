# Geocoding Brazilian Polling Stations with Administrative Data Sets

This repository contains the code to geocode polling stations in Brazil.
We leverage administrative datasets, as well as open-source geocode APIs, to geocode all polling stations used in elections from 2006 to 2018.

We detail our methodology and discuss limitations of our method in this [document](https://rawcdn.githack.com/fdhidalgo/geocode_br_polling_stations/f701cfe790128ed4d62967258336dc71ab769c4c/doc/geocode_polling_stations/geocode_polling_stations.html).

The dataset of geocoded polling stations can be found in this [compressed csv file](./geocoded_polling_stations.csv.gz).

## Code

We used the open source language *R* (version 4.0.3) to process the filesand geocode the pollin stations.
We use the [`drake`](https://github.com/ropensci/drake) package to create a pipeline to import and process all the data.
Assuming all the relevant data is in the `./data` folder, you can reconstruct the dataset using the following code:

``` r
library(drake)
r_make()
```

Required packages that need to be installed can be found in the [`package.R`](./packages.R) file.
Given the size of some of the data files, you will likely need at least 50GB of RAM to run the code.

## Data Sources

Because of the size of some of the administrative datasets, we cannot host all the data necessary to run the code on Github.
Datasets marked with a \* can be found at the associated link in the table below.
All other data can be found in the `data` folder.

| Data                             | Source                                                                                                                                                                                                 |
|----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 2010 CNEFE\*                     | [IBGE FTP Server](ftp://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Cadastro_Nacional_de_Enderecos_Fins_Estatisticos/)                                                                               |
| 2017 CNEFE\*                     | [IBGE Website](https://www.ibge.gov.br/estatisticas/economicas/agricultura-e-pecuaria/21814-2017-censo-agropecuario.html?edicao=23751&t=resultados)                                                    |
| INEP School Catalog              | [INEP Website](https://inepdata.inep.gov.br/analytics/saw.dll?dashboard&NQUser=inepdata&NQPassword=Inep2014&PortalPath=%2Fshared%2FCenso%20da%20Educação%20Básica%2F_portal%2FCatálogo%20de%20Escolas) |
| Polling Stations Geocoded by TSE | [Estado de Sāo Paulo Github](https://github.com/estadao/como-votou-sua-vizinhanca/blob/master/data/locais/local-votacao-08-08-2018.csv)                                                                |
| Polling Station Addresses        | [Centro de Política e Economia do Setor Público](https://www.cepespdata.io)                                                                                                                            |
| Census Tract Shape Files\*       | [`geobr` Package](https://github.com/ipeaGIT/geobr)                                                                                                                                                    |
| Municipal Demographic Variables  | [Atlas do Desenvolvimento Humano no Brasil](http://www.atlasbrasil.org.br)                                                                                                                             |

## Acknowledgements 

Thanks to:

-   [Yuri Kasahara](https://www.researchgate.net/profile/Yuri_Kasahara2) for ideas and debugging

-   George Avelino, Mauricio Izumi, Gabriel Caseiro, and Daniel Travassos Ferreira at [FGV/CEPESP](https://www.cepespdata.io) for data and advice

## Other Approaches

-   Spatial Maps at [http://spatial2.cepesp.io](http://spatial2.cepesp.io)

-   [Pindograma](https://github.com/pindograma/mapa)
