## Fail-loud spec tests for process_panel_ids_municipality_batch()
## (R/panel_creation.R), cleanup phase 3, finding H3. The former handler wrapped
## make_panel_1block() in a tryCatch that cat()'d the error and returned NULL;
## the NULL was then filtered out, so any municipality that errored was silently
## excluded from published panel IDs. The fixed contract is collect-and-stop: a
## genuine error is recorded and surfaced at batch end, while a NULL result (a
## municipality with no cross-year pairs) stays a legitimate empty case.
##
## make_panel_1block() runs reclin2 record linkage; to exercise the error path
## deterministically without depending on those internals, these tests swap in a
## stub via the global binding the batch function resolves, restoring it after.

make_batch_locais <- function() {
  data.table::data.table(
    cod_localidade_ibge = rep(c(1L, 2L), each = 4),
    local_id = 1:8,
    ano = rep(c(2018L, 2022L), times = 4),
    sg_uf = "AC"
  )
}

# Swap make_panel_1block for `stub` for the duration of the calling test, so the
# collect-and-stop path is exercised without depending on reclin2 internals. The
# binding is resolved from the environment tar_source() loaded the pipeline into
# (globalenv when sourced directly, a dedicated env under test_dir), so reassign
# it there; withr::defer restores it when the test frame exits.
local_stub_make_panel_1block <- function(stub, frame = parent.frame()) {
  fn_env <- environment(process_panel_ids_municipality_batch)
  original <- get("make_panel_1block", envir = fn_env)
  withr::defer(assign("make_panel_1block", original, envir = fn_env), envir = frame)
  assign("make_panel_1block", stub, envir = fn_env)
}

test_that("a per-municipality error is collected and stops the batch, naming the municipality", {
  local_stub_make_panel_1block(function(
    block,
    years,
    blocking_column,
    scoring_columns,
    use_word_blocking = FALSE,
    panel_weight_threshold = 0
  ) {
    if (unique(block$cod_localidade_ibge)[1] == 2L) {
      stop("synthetic linkage failure")
    }
    data.table::data.table(panel_id = 1L, local_id_2018 = 1L, local_id_2022 = 2L)
  })

  batch <- data.table::data.table(cod_localidade_ibge = c(1L, 2L))
  err <- expect_error(
    process_panel_ids_municipality_batch(
      make_batch_locais(),
      batch,
      years = c(2018L, 2022L),
      blocking_column = "cod_localidade_ibge",
      scoring_columns = "normalized_name"
    ),
    "Panel ID creation failed for"
  )
  expect_match(conditionMessage(err), "2: synthetic linkage failure")
})

test_that("a NULL result is treated as legitimately empty, not a failure", {
  local_stub_make_panel_1block(function(
    block,
    years,
    blocking_column,
    scoring_columns,
    use_word_blocking = FALSE,
    panel_weight_threshold = 0
  ) {
    # Municipality 2 has no cross-year pairs: make_panel_1block returns NULL.
    if (unique(block$cod_localidade_ibge)[1] == 2L) {
      return(NULL)
    }
    data.table::data.table(panel_id = 1L, local_id_2018 = 1L, local_id_2022 = 2L)
  })

  batch <- data.table::data.table(cod_localidade_ibge = c(1L, 2L))
  out <- process_panel_ids_municipality_batch(
    make_batch_locais(),
    batch,
    years = c(2018L, 2022L),
    blocking_column = "cod_localidade_ibge",
    scoring_columns = "normalized_name"
  )
  expect_true(data.table::is.data.table(out))
  expect_equal(nrow(out), 1L) # only municipality 1 contributed; no error raised
})
