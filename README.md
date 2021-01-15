# Geocoding Brazilian Polling Stations with Administrative Data Sets

This repository contains the code to geocode polling stations in Brazil.
We leverage administrative datasets, as well as open-source geocode APIs, to geocode all polling stations used in elections from 2006 to 2018.

We detail our methodology and discuss limitations to our method in this [document](https://rawcdn.githack.com/fdhidalgo/geocode_br_polling_stations/f701cfe790128ed4d62967258336dc71ab769c4c/doc/geocode_polling_stations/geocode_polling_stations.html).

The dataset of geocoded polling stations can be found in this [compressed csv file](./geocoded_polling_stations.csv.gz).

## Code 

We used the open source language *R* to process the files.
We use the [`drake`](https://github.com/ropensci/drake) package to create a pipeline to import and process all the data.
Assuming all the relevant data is in the `./data` folder, you can reconstruct the dataset using the following code:

``` r
library(drake)
r_make()
```

Required packages that need to be installed can be found in the [`package.R`](./packages.R) file.     

## Data Sources

Because of the size of the original administrative datasets, we cannot host most of them on github.
