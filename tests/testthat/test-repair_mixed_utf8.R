## Spec test for repair_mixed_utf8() (R/data_cleaning.R). The consolidated
## polling-station export mixes UTF-8 and Latin-1 bytes; the helper must repair
## invalid-UTF-8 strings (reinterpreting their bytes as Latin-1) while leaving
## genuine UTF-8 untouched, so downstream stringi ops don't error on invalid
## input (the "input string N is invalid" failure surfaced by the fail-loud sweep).

# "Nº" as Latin-1 bytes (0x4e, 0xBA) tagged UTF-8 — the lone high byte 0xBA is
# invalid UTF-8, exactly as fread(encoding="UTF-8") leaves it on a Latin-1 file.
make_mislabeled_no_sign <- function() {
  bad <- rawToChar(as.raw(c(0x4e, 0xba)))
  Encoding(bad) <- "UTF-8"
  bad
}

test_that("repair_mixed_utf8 fixes a Latin-1 byte mislabeled as UTF-8", {
  bad <- make_mislabeled_no_sign()
  expect_false(stringi::stri_enc_isutf8(bad))

  fixed <- repair_mixed_utf8(bad)
  expect_true(stringi::stri_enc_isutf8(fixed))
  expect_identical(charToRaw(fixed), as.raw(c(0x4e, 0xc2, 0xba)))  # proper UTF-8 'º'
})

test_that("repair_mixed_utf8 leaves genuine UTF-8 unchanged", {
  good <- enc2utf8("GUAPORÉ")
  expect_true(stringi::stri_enc_isutf8(good))
  expect_identical(repair_mixed_utf8(good), good)
})

test_that("repair_mixed_utf8 repairs a mixed vector element-wise", {
  x <- c("ASCII ONLY", enc2utf8("SÃO PAULO"), make_mislabeled_no_sign())

  fixed <- repair_mixed_utf8(x)
  expect_true(all(stringi::stri_enc_isutf8(fixed)))
  expect_identical(fixed[1], "ASCII ONLY")
  expect_identical(fixed[2], enc2utf8("SÃO PAULO"))
  # stri_trans_general (used in clean_text_for_geocodebr) must no longer error.
  expect_no_error(stringi::stri_trans_general(fixed, "Latin-ASCII"))
})

test_that("repair_mixed_utf8 handles NA alongside an invalid byte", {
  # stri_enc_isutf8(NA) is NA; a logical index with NA would abort the
  # subassignment. A column with a missing value plus a bad byte must still repair.
  x <- c(make_mislabeled_no_sign(), NA_character_)
  fixed <- repair_mixed_utf8(x)
  expect_true(stringi::stri_enc_isutf8(fixed[1]))
  expect_true(is.na(fixed[2]))
})
