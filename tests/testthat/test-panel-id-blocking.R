# Unit tests for panel ID two-level blocking optimization

library(testthat)
library(data.table)
library(reclin2)

# Source the blocking functions
source("R/panel_id_blocking_fns.R")

test_that("extract_significant_words removes Portuguese stopwords correctly", {
  # Test basic stopword removal
  text1 <- "ESCOLA MUNICIPAL SANTOS DUMONT"
  words1 <- extract_significant_words(text1)[[1]]
  expect_equal(words1, c("SANTOS", "DUMONT"))
  
  # Test with addresses
  text2 <- "RUA PRINCIPAL 123 CENTRO"
  words2 <- extract_significant_words(text2)[[1]]
  expect_equal(words2, "PRINCIPAL")
  
  # Test with multiple texts
  texts <- c("ESCOLA ESTADUAL MARIA SILVA", "COLEGIO DOM PEDRO")
  words <- extract_significant_words(texts)
  expect_equal(words[[1]], c("MARIA", "SILVA"))
  expect_equal(words[[2]], c("DOM", "PEDRO"))
  
  # Test edge cases
  expect_equal(extract_significant_words("")[[1]], character(0))
  expect_equal(extract_significant_words(NA)[[1]], character(0))
  expect_equal(extract_significant_words("DE DA DO E EM")[[1]], character(0))
})

test_that("extract_significant_words respects minimum word length", {
  text <- "ESCOLA AB ABC ABCD"
  
  # Default min length = 3
  words_default <- extract_significant_words(text)[[1]]
  expect_equal(words_default, c("ABC", "ABCD"))
  
  # Min length = 2
  words_2 <- extract_significant_words(text, min_word_length = 2)[[1]]
  expect_equal(words_2, c("AB", "ABC", "ABCD"))
  
  # Min length = 4
  words_4 <- extract_significant_words(text, min_word_length = 4)[[1]]
  expect_equal(words_4, "ABCD")
})

test_that("create_word_blocking_keys generates consistent keys", {
  name_words <- list(
    c("SANTOS", "DUMONT"),
    c("MARIA", "SILVA"),
    character(0)
  )
  
  addr_words <- list(
    c("PRINCIPAL", "SUL"),
    c("BRASIL", "NORTE"),
    c("CENTRO")
  )
  
  keys <- create_word_blocking_keys(name_words, addr_words)
  
  # Check that words are sorted for consistent keys
  expect_equal(keys[1], "DUMONT_PRINCIPAL_SANTOS_SUL")
  expect_equal(keys[2], "BRASIL_MARIA_NORTE_SILVA")
  expect_equal(keys[3], "CENTRO")
  
  # Test empty inputs
  empty_keys <- create_word_blocking_keys(list(), list())
  expect_equal(length(empty_keys), 0)
})

test_that("find_shared_word_pairs identifies correct pairs", {
  words1 <- list(
    c("SANTOS", "DUMONT"),
    c("MARIA", "SILVA"),
    c("PEDRO", "ALVARES")
  )
  
  words2 <- list(
    c("SANTOS", "AIRPORT"),
    c("JOSE", "SILVA"),
    c("FRANCISCO", "XAVIER")
  )
  
  # Find pairs with at least 1 shared word
  pairs <- find_shared_word_pairs(words1, words2, min_shared_words = 1)
  
  # Should find: (1,1) for SANTOS, (2,2) for SILVA
  expect_true(nrow(pairs) >= 2)
  expect_true(all(c(1, 2) %in% pairs$x))
  expect_true(all(c(1, 2) %in% pairs$y))
  
  # Test with min_shared_words = 2
  words3 <- list(
    c("SANTOS", "DUMONT", "ESCOLA"),
    c("MARIA", "SILVA", "PROFESSORA")
  )
  
  words4 <- list(
    c("SANTOS", "DUMONT", "AEROPORTO"),
    c("MARIA", "SANTOS", "ESCOLA")
  )
  
  pairs2 <- find_shared_word_pairs(words3, words4, min_shared_words = 2)
  
  # Should find (1,1) with SANTOS+DUMONT, (1,2) with SANTOS+ESCOLA
  expect_true(1 %in% pairs2$x)
  expect_true(all(c(1, 2) %in% pairs2$y))
})

test_that("create_two_level_blocked_pairs reduces pairs correctly", {
  # Create test data
  set.seed(123)
  n1 <- 20
  n2 <- 20
  
  data1 <- data.table(
    cod_localidade_ibge = rep(1234567, n1),
    normalized_name = c(
      rep("ESCOLA SANTOS DUMONT", 5),
      rep("COLEGIO MARIA SILVA", 5),
      rep("INSTITUTO PEDRO ALVARES", 5),
      rep("CENTRO EDUCACIONAL BRASIL", 5)
    ),
    normalized_addr = paste("RUA", sample(c("PRINCIPAL", "BRASIL", "CENTRO", "NORTE"), n1, replace = TRUE))
  )
  
  data2 <- data.table(
    cod_localidade_ibge = rep(1234567, n2),
    normalized_name = c(
      rep("E M SANTOS DUMONT", 5),
      rep("ESCOLA JOSE SILVA", 5),
      rep("COLEGIO DOM PEDRO", 5),
      rep("UNIDADE ESCOLAR BRASIL", 5)
    ),
    normalized_addr = paste("AVENIDA", sample(c("PRINCIPAL", "SUL", "CENTRO", "LESTE"), n2, replace = TRUE))
  )
  
  # Get pairs without word blocking
  pairs_no_blocking <- pair_blocking(data1, data2, "cod_localidade_ibge")
  expect_equal(nrow(pairs_no_blocking), n1 * n2)  # All combinations
  
  # Get pairs with word blocking
  pairs_blocked <- create_two_level_blocked_pairs(
    data1, data2,
    municipality_col = "cod_localidade_ibge",
    name_col = "normalized_name",
    addr_col = "normalized_addr"
  )
  
  # Should have fewer pairs
  expect_lt(nrow(pairs_blocked), nrow(pairs_no_blocking))
  expect_gt(nrow(pairs_blocked), 0)
  
  # Check that blocked pairs are a subset of original pairs
  blocked_set <- paste(pairs_blocked$.x, pairs_blocked$.y)
  original_set <- paste(pairs_no_blocking$.x, pairs_no_blocking$.y)
  expect_true(all(blocked_set %in% original_set))
})

test_that("calculate_blocking_stats provides correct statistics", {
  stats <- calculate_blocking_stats(
    original_pairs = 1000,
    blocked_pairs = 250,
    municipality_code = "1234567"
  )
  
  expect_equal(stats$original_pairs, 1000)
  expect_equal(stats$blocked_pairs, 250)
  expect_equal(stats$reduction_pct, 75)
  expect_equal(stats$speedup_factor, 4)
  expect_equal(stats$municipality, "1234567")
})

test_that("fallback mechanism works when no shared words exist", {
  # Create data with no shared words
  data1 <- data.table(
    cod_localidade_ibge = 1234567,
    normalized_name = "AAA BBB CCC",
    normalized_addr = "DDD EEE FFF"
  )
  
  data2 <- data.table(
    cod_localidade_ibge = 1234567,
    normalized_name = "XXX YYY ZZZ",
    normalized_addr = "UUU VVV WWW"
  )
  
  # With fallback enabled (default)
  pairs_with_fallback <- create_two_level_blocked_pairs(
    data1, data2,
    fallback_on_empty = TRUE
  )
  
  expect_equal(nrow(pairs_with_fallback), 1)  # Should include the pair
  
  # Without fallback
  pairs_no_fallback <- create_two_level_blocked_pairs(
    data1, data2,
    fallback_on_empty = FALSE
  )
  
  expect_equal(nrow(pairs_no_fallback), 0)  # Should exclude the pair
})