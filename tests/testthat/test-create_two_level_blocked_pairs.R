## Spec tests for create_two_level_blocked_pairs() (R/panel_creation.R).
## It pairs records that share a municipality, then keeps only those whose name/address
## share at least one significant word (stopwords and words under 3 characters do not
## count). A record left with fewer than two significant words cannot be judged on
## overlap, so all of its pairs are kept.
##
## The function prints progress via cat(); capture.output() keeps test output clean.

blocked <- function(data1, data2) {
  invisible(capture.output(out <- create_two_level_blocked_pairs(data1, data2)))
  data.table::data.table(x = data1$local_id[out$.x], y = data2$local_id[out$.y])
}

test_that("create_two_level_blocked_pairs keeps only pairs sharing a significant word", {
  year1 <- data.table::data.table(
    local_id = c("a_escola", "a_hospital"),
    cod_localidade_ibge = 1L,
    normalized_name = c("escola central", "hospital norte"),
    normalized_addr = c("rua amapa", "avenida bahia")
  )
  year2 <- data.table::data.table(
    local_id = c("b_escola", "b_hospital"),
    cod_localidade_ibge = 1L,
    normalized_name = c("escola central", "hospital norte"),
    normalized_addr = c("rua amapa", "avenida bahia")
  )

  kept <- blocked(year1, year2)

  # "escola", "rua" and "avenida" are stopwords, so only the distinguishing words pair up.
  expect_setequal(paste(kept$x, kept$y), c("a_escola b_escola", "a_hospital b_hospital"))
})

test_that("create_two_level_blocked_pairs never pairs across municipalities", {
  year1 <- data.table::data.table(
    local_id = c("m1", "m2"),
    cod_localidade_ibge = c(1L, 2L),
    normalized_name = "escola central",
    normalized_addr = "rua amapa"
  )
  year2 <- data.table::data.table(
    local_id = c("m1_later", "m2_later"),
    cod_localidade_ibge = c(1L, 2L),
    normalized_name = "escola central",
    normalized_addr = "rua amapa"
  )

  kept <- blocked(year1, year2)

  expect_setequal(paste(kept$x, kept$y), c("m1 m1_later", "m2 m2_later"))
})

test_that("create_two_level_blocked_pairs keeps every pair of a word-thin record", {
  year1 <- data.table::data.table(
    local_id = c("thin", "rich"),
    cod_localidade_ibge = 1L,
    # "thin" is left with one significant word once stopwords and short words go.
    normalized_name = c("escola", "hospital norte"),
    normalized_addr = c("rua", "avenida bahia")
  )
  year2 <- data.table::data.table(
    local_id = c("other", "match"),
    cod_localidade_ibge = 1L,
    normalized_name = c("creche amazonas", "hospital norte"),
    normalized_addr = c("rua parana", "avenida bahia")
  )

  kept <- blocked(year1, year2)

  # The thin record shares no word with either year-2 record but is kept against both;
  # the rich record is filtered on overlap as usual.
  expect_setequal(
    paste(kept$x, kept$y),
    c("thin other", "thin match", "rich match")
  )
})
