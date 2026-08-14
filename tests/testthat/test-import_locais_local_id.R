## Spec tests for the deterministic local_id in import_locais() (R/data_cleaning.R,
## H6, #36/#48). local_id was `:= .I` on the input row order, so two runs whose
## input rows differed in order produced different ids (non-reproducible). The
## fixed contract assigns local_id from an ordering on the natural station-year
## key (ano, cod_localidade_ibge, nr_zona, nr_locvot), which is verified unique
## across the full input; a duplicate key stop()s.

# Minimal polling-station CSV with the columns import_locais() expects, written in
# a caller-controlled row order so the test can shuffle it.
write_locais_csv <- function(rows) {
  path <- withr::local_tempfile(fileext = ".csv.gz", .local_envir = parent.frame())
  data.table::fwrite(rows, path)
  path
}

muni_fixture <- data.table::data.table(
  id_TSE = c(1120L, 1570L),
  id_munic_7 = c(1200013L, 1200054L),
  estado_abrev = "AC"
)

station_rows <- function() {
  data.table::data.table(
    ANO = c(2024L, 2024L, 2022L, 2024L),
    CD_LOCALIDADE_TSE = c(1120L, 1120L, 1120L, 1570L),
    NR_ZONA = c(1L, 1L, 1L, 2L),
    NR_LOCVOT = c(1015L, 1023L, 1015L, 1015L),
    NR_CEP = 69900000L,
    SG_UF = "AC",
    NM_LOCALIDADE = "RIO BRANCO",
    NM_LOCVOT = "ESCOLA A",
    DS_ENDERECO = "RUA X 100",
    DS_BAIRRO = "CENTRO"
  )
}

test_that("local_id is invariant to input row order", {
  rows <- station_rows()
  a <- import_locais(write_locais_csv(rows), muni_fixture)
  b <- import_locais(write_locais_csv(rows[rev(seq_len(.N))]), muni_fixture)

  key <- c("ano", "cod_localidade_ibge", "nr_zona", "nr_locvot")
  a_key <- a[, c(key, "local_id"), with = FALSE]
  b_key <- b[, c(key, "local_id"), with = FALSE]
  merged <- merge(a_key, b_key, by = key, suffixes = c("_a", "_b"))

  expect_equal(nrow(merged), nrow(rows))
  # Same station-year key -> same local_id regardless of input order.
  expect_equal(merged$local_id_a, merged$local_id_b)
  # local_id is a dense 1..N integer key.
  expect_setequal(a$local_id, seq_len(nrow(rows)))
})

test_that("import_locais stops loudly when a TSE municipality has no IBGE code", {
  # A TSE code absent from the crosswalk used to leave cod_localidade_ibge NA, which
  # fwrite published as "" -- a key that joins to nothing instead of erroring (#122).
  rows <- station_rows()
  rows[1L, CD_LOCALIDADE_TSE := 73709L] # newly created municipality, not in muni_fixture
  expect_error(
    import_locais(write_locais_csv(rows), muni_fixture),
    "no 7-digit cod_localidade_ibge"
  )
})

test_that("import_locais keeps stations abroad out of the IBGE-code check", {
  # sg_uf == "ZZ" stations have no IBGE municipality by construction; they are dropped
  # before the check rather than tripping it.
  rows <- rbind(
    station_rows(),
    data.table::data.table(
      ANO = 2024L,
      CD_LOCALIDADE_TSE = 30449L,
      NR_ZONA = 1L,
      NR_LOCVOT = 1031L,
      NR_CEP = NA_integer_,
      SG_UF = "ZZ",
      NM_LOCALIDADE = "ROMA",
      NM_LOCVOT = "CONSULADO",
      DS_ENDERECO = "VIA X 1",
      DS_BAIRRO = "CENTRO"
    )
  )
  result <- import_locais(write_locais_csv(rows), muni_fixture)
  expect_equal(nrow(result), nrow(station_rows()))
  expect_false(any(result$sg_uf == "ZZ"))
})

test_that("import_locais stops loudly on a duplicated station-year key", {
  rows <- station_rows()
  dup <- rbind(rows, rows[1L]) # exact duplicate natural key
  expect_error(
    import_locais(write_locais_csv(dup), muni_fixture),
    "local_id key not unique"
  )
})
