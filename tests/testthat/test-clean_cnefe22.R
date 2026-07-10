## Fail-loud spec test for clean_cnefe22() (R/data_cleaning.R), cleanup phase 3,
## finding H1. When the municipality crosswalk (muni_ids) is empty, the former
## code filled id_TSE/municipio/estado_abrev with NA and continued; that hides a
## structural failure (the crosswalk failed to load). The fixed contract stops.

make_min_cnefe22 <- function() {
  # The smallest CNEFE-2022 record that processes cleanly up to the muni_ids
  # attachment step: only the columns referenced before that step are needed.
  data.table::data.table(
    cod_municipio = 1200401L,
    num_endereco = 100L,
    dsc_modificador = "APT 1",
    nom_tipo_seglogr = "RUA",
    nom_titulo_seglogr = "",
    nom_seglogr = "PRINCIPAL",
    dsc_estabelecimento = "ESCOLA"
  )
}

test_that("clean_cnefe22 stops when the municipality crosswalk is empty", {
  expect_error(
    clean_cnefe22(make_min_cnefe22(), muni_ids = data.table::data.table()),
    "muni_ids is empty"
  )
})
