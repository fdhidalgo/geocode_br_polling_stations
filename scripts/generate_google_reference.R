#!/usr/bin/env Rscript
## Generate the frozen Google reference-validation artifact (spec section 8).
##
## ONE-TIME, MANUAL script. It geocodes a stratified covered-only sample of
## polling stations with the Google Geocoding API and writes the results to
## data/google_geocoded.csv, which is then COMMITTED as a frozen artifact so
## any analysis reading it stays reproducible even though the API call is not
## free to rerun. This is a reference-validation
## (Google's error budget vs field-GPS TSE), NOT a pipeline accuracy number and
## NOT a release gate.
##
## Usage:
##   export GOOGLE_MAPS_API_KEY=...          # required
##   Rscript scripts/generate_google_reference.R [n_per_stratum]
##
## Cost note: Google Geocoding is ~$5 / 1,000 requests; a few thousand points is
## a few dollars. Sampling is for rate limits and a tractable frozen artifact,
## not for cost.

suppressPackageStartupMessages({
  library(data.table)
  library(targets)
  library(httr2)
})

`%||%` <- function(a, b) if (is.null(a)) b else a

args <- commandArgs(trailingOnly = TRUE)
n_per_stratum <- if (length(args) >= 1) as.integer(args[[1]]) else 200L

api_key <- Sys.getenv("GOOGLE_MAPS_API_KEY")
if (!nzchar(api_key)) {
  stop("Set GOOGLE_MAPS_API_KEY in the environment before running.")
}

## ---- Build the stratified covered sample ---------------------------------
# oof_selected_matches carries the covered universe with strata; locais and
# tsegeocoded_locais supply the address to send and the TSE coord to compare.
tar_load(c(oof_selected_matches, locais, tsegeocoded_locais))

sel <- as.data.table(oof_selected_matches)
covered <- sel[!is.na(urban_rural)] # need a stratum to sample within

set.seed(20260710L)
sampled <- covered[,
  .SD[sample(.N, min(.N, n_per_stratum))],
  by = .(urban_rural, region)
]

addr <- as.data.table(locais)[
  local_id %in% sampled$local_id,
  .(local_id, nr_zona, nr_locvot, cod_localidade_ibge, ds_endereco, ds_bairro, nm_localidade = nm_localidade, sg_uf)
]
tse <- as.data.table(tsegeocoded_locais)[,
  .(local_id, tse_long, tse_lat)
]

query_dt <- Reduce(
  function(x, y) merge(x, y, by = "local_id"),
  list(sampled[, .(local_id, urban_rural, region)], addr, tse)
)
query_dt[, address := paste(ds_endereco, ds_bairro, nm_localidade, sg_uf, "Brazil", sep = ", ")]

## ---- Geocode one address via the Google Geocoding API --------------------
geocode_one <- function(address) {
  resp <- request("https://maps.googleapis.com/maps/api/geocode/json") |>
    req_url_query(address = address, key = api_key, region = "br") |>
    req_perform()
  body <- resp_body_json(resp)
  if (identical(body$status, "OK") && length(body$results) > 0) {
    loc <- body$results[[1]]$geometry$location
    return(list(google_lat = loc$lat, google_long = loc$lng, google_status = body$status))
  }
  list(google_lat = NA_real_, google_long = NA_real_, google_status = body$status %||% "ERROR")
}

## ---- Run (rate-limited) ---------------------------------------------------
message("Geocoding ", nrow(query_dt), " stations via Google...")
results <- vector("list", nrow(query_dt))
for (i in seq_len(nrow(query_dt))) {
  results[[i]] <- geocode_one(query_dt$address[i])
  Sys.sleep(0.05) # stay under the per-second rate limit
  if (i %% 200 == 0) message("  ", i, " / ", nrow(query_dt))
}
res_dt <- rbindlist(results)
out <- cbind(
  query_dt[, .(local_id, cod_localidade_ibge, nr_zona, nr_locvot, urban_rural, region, tse_long, tse_lat)],
  res_dt
)

dir.create("data", showWarnings = FALSE)
fwrite(out, "data/google_geocoded.csv")
message("Wrote data/google_geocoded.csv (", nrow(out), " rows). ", "Commit it as the frozen artifact.")
