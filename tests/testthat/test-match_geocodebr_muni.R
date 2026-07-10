## Fail-loud spec tests for match_geocodebr_muni() (R/string_matching.R),
## cleanup phase 3, finding C5. The former tryCatch wrappers converted any
## error (including a missing geocodebr package) into a warning + NULL, silently
## dropping a municipality from geocodebr coverage. The fixed contract:
##   - a missing geocodebr package stops immediately (structural precondition);
##   - a legitimately empty input (no polling stations) returns NULL, not an error;
##   - geocoding errors propagate to the caller (verified via the batch driver in
##     test-process_geocodebr_batch.R, which need not reach the external DB).

make_locais_muni <- function() {
  data.table::data.table(
    cod_localidade_ibge = 1200401L,
    nm_localidade = "RIO BRANCO",
    local_id = 1L,
    sg_uf = "AC",
    ds_endereco = "RUA X",
    ds_bairro = "CENTRO"
  )
}

test_that("match_geocodebr_muni stops when geocodebr is not installed", {
  # This path only exists to exercise the structural-precondition stop(); when
  # geocodebr is installed we cannot trigger it without mocking, so skip.
  skip_if(
    requireNamespace("geocodebr", quietly = TRUE),
    "geocodebr installed; cannot exercise the missing-package path"
  )
  expect_error(
    match_geocodebr_muni(make_locais_muni()),
    "geocodebr package not installed"
  )
})

test_that("match_geocodebr_muni returns NULL for an empty municipality", {
  skip_if_not_installed("geocodebr")
  # Zero polling stations is a legitimate empty case, not a failure.
  expect_null(match_geocodebr_muni(make_locais_muni()[0]))
})
