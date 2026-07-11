## This File updates the mapping between the TSE's vote section and localidade codes
library(data.table)
library(electionsBR)
library(purrr)


secc_loc_map <- fread("data/secao_local_2006_20_mapping.csv.gz")

get_secc_loc_map <- function(year, uf) {
  secc_dets <- elections_tse(
    year = year,
    type = "vote_section",
    data_table = TRUE,
    uf = uf
  ) |>
    _[, .(
      ANO_ELEICAO,
      SG_UF,
      CD_MUNICIPIO,
      NM_MUNICIPIO,
      NR_ZONA,
      NR_SECAO,
      NR_LOCAL_VOTACAO,
      NM_LOCAL_VOTACAO,
      DS_LOCAL_VOTACAO_ENDERECO
    )] |>
    unique()
}

years <- c(2022, 2024)
ufs <- c(
  "AC",
  "AL",
  "AM",
  "AP",
  "BA",
  "CE",
  "DF",
  "ES",
  "GO",
  "MA",
  "MG",
  "MS",
  "MT",
  "PA",
  "PB",
  "PE",
  "PI",
  "PR",
  "RJ",
  "RN",
  "RO",
  "RR",
  "RS",
  "SC",
  "SE",
  "SP",
  "TO"
)


## Data table of years x ufs
secc_loc_map_dt <- data.table(
  year = rep(years, each = length(ufs)),
  uf = rep(ufs, times = length(years))
)


secc_loc_map22 <- map(
  ufs,
  \(i) get_secc_loc_map(2022, i),
  .progress = TRUE
) |>
  rbindlist()
setnames(
  secc_loc_map22,
  c(
    "ANO_ELEICAO",
    "SG_UF",
    "CD_MUNICIPIO",
    "NM_MUNICIPIO",
    "NR_ZONA",
    "NR_SECAO",
    "NR_LOCAL_VOTACAO",
    "NM_LOCAL_VOTACAO",
    "DS_LOCAL_VOTACAO_ENDERECO"
  ),
  c(
    "ano",
    "sg_uf",
    "cd_localidade_tse",
    "nm_localidade",
    "nr_zona",
    "nr_secao",
    "nr_locvot",
    "nm_locvot",
    "ds_endereco"
  )
)
##Change ordering of columns to match secc_loc_map
setcolorder(
  secc_loc_map22,
  intersect(names(secc_loc_map22), names(secc_loc_map))
)
fwrite(secc_loc_map22, "data/secc_loc_map/secc_loc_map_2022.csv.gz")


secc_loc_map24 <- map(
  ufs[-7], # Exclude DF (2024)
  \(i) get_secc_loc_map(2024, i),
  .progress = TRUE
) |>
  rbindlist()
setnames(
  secc_loc_map24,
  c(
    "ANO_ELEICAO",
    "SG_UF",
    "CD_MUNICIPIO",
    "NM_MUNICIPIO",
    "NR_ZONA",
    "NR_SECAO",
    "NR_LOCAL_VOTACAO",
    "NM_LOCAL_VOTACAO",
    "DS_LOCAL_VOTACAO_ENDERECO"
  ),
  c(
    "ano",
    "sg_uf",
    "cd_localidade_tse",
    "nm_localidade",
    "nr_zona",
    "nr_secao",
    "nr_locvot",
    "nm_locvot",
    "ds_endereco"
  )
)
##Change ordering of columns to match secc_loc_map
setcolorder(
  secc_loc_map24,
  intersect(names(secc_loc_map24), names(secc_loc_map))
)
fwrite(secc_loc_map24, "data/secc_loc_map/secc_loc_map_2024.csv.gz")


secc_loc_map_2006_24 <- rbindlist(
  list(secc_loc_map, secc_loc_map22, secc_loc_map24),
  fill = TRUE
)
fwrite(secc_loc_map_2006_24, "data/secc_loc_map/secc_loc_map_2006_24.csv.gz")
