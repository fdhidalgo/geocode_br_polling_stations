## Spec tests for convert_coords_dms() (R/data_cleaning.R).
## convert_coords_dms parses a vector of "degrees minutes seconds direction" DMS
## strings into decimal degrees, negating for S/W/O (Sul/West/Oeste), and returns
## NA_real_ for any malformed element (fewer than 4 tokens, or non-numeric D/M/S).
## It is the vectorized replacement for the former per-element convert_coord()
## (perf ticket #61); the equivalence test below pins that replacement to the
## original scalar behavior.

test_that("convert_coords_dms parses valid DMS strings to decimal degrees", {
  expect_equal(convert_coords_dms("23 30 0 S"), -23.5)   # southern hemisphere is negative
  expect_equal(convert_coords_dms("10 0 0 N"), 10)       # northern hemisphere stays positive
  expect_equal(convert_coords_dms("0 0 36 N"), 0.01)     # seconds contribute 36/3600
})

test_that("convert_coords_dms negates for all western/southern direction codes", {
  expect_equal(convert_coords_dms("10 0 0 O"), -10)   # Oeste
  expect_equal(convert_coords_dms("10 0 0 W"), -10)   # West
  expect_equal(convert_coords_dms("10 0 0 S"), -10)   # Sul
})

test_that("convert_coords_dms returns NA_real_ on malformed input", {
  expect_identical(convert_coords_dms("abc"), NA_real_)          # single token
  expect_identical(convert_coords_dms("10 20"), NA_real_)        # fewer than 4 parts
  expect_identical(convert_coords_dms("xx 20 30 N"), NA_real_)   # non-numeric degrees
  expect_identical(convert_coords_dms("10 yy 30 N"), NA_real_)   # non-numeric minutes
})

test_that("convert_coords_dms handles a mixed vector element-wise", {
  out <- convert_coords_dms(c("23 30 0 S", "bad", "10 0 0 N", "10 20"))
  expect_identical(out, c(-23.5, NA_real_, 10, NA_real_))
})

test_that("convert_coords_dms returns an empty numeric vector for empty input", {
  expect_identical(convert_coords_dms(character(0)), numeric(0))
})

test_that("convert_coords_dms maps every short row to NA when no row reaches 4 tokens", {
  # With no row reaching 4 tokens, tstrsplit yields < 4 columns; every element
  # must still map to NA (one per input), not collapse.
  expect_identical(convert_coords_dms(c("10 20", "30 40")), c(NA_real_, NA_real_))
})

test_that("convert_coords_dms is element-wise identical to the original scalar convert_coord", {
  # Equivalence oracle: a verbatim copy of the per-element implementation that
  # convert_coords_dms replaced. The vectorized function must match it exactly,
  # NA for NA, over representative and malformed real-shaped inputs (perf #61).
  reference_convert_coord <- function(coord) {
    tryCatch({
      parts <- unlist(strsplit(coord, " "))
      if (length(parts) < 4) {
        return(NA_real_)
      }
      degrees <- suppressWarnings(as.numeric(parts[1]))
      minutes <- suppressWarnings(as.numeric(parts[2]))
      seconds <- suppressWarnings(as.numeric(parts[3]))
      direction <- gsub("[^NSWO]", "", parts[4])
      if (is.na(degrees) || is.na(minutes) || is.na(seconds)) {
        return(NA_real_)
      }
      decimal_degrees <- degrees + (minutes / 60) + (seconds / 3600)
      if (direction %in% c("S", "W", "O")) {
        decimal_degrees <- -decimal_degrees
      }
      decimal_degrees
    }, error = function(e) NA_real_)
  }

  samples <- c(
    # valid coordinates spanning both hemispheres and all direction codes
    "23 30 0 S", "10 0 0 N", "0 0 36 N", "10 0 0 O", "10 0 0 W",
    "5 15 30 N", "47 55 12 W", "3 8 45 S", "22 54 30 S", "43 12 27 W",
    # multi-character direction tokens: gsub keeps only NSWO chars
    "12 34 56 SUL",     # -> "S": negated
    "12 34 56 NORTE",   # -> "NO": not in {S,W,O}, stays positive
    "10 20 30 X",       # -> "": stays positive
    # malformed: too few tokens
    "10 20 30", "10 20", "abc", "",
    # malformed: non-numeric components
    "xx 20 30 N", "10 yy 30 N", "10 20 zz N",
    # whitespace edge cases (single-space split)
    " 10 20 30 N",      # leading space -> empty first token -> NA
    "10 20 30 N ",      # trailing space -> extra empty token, still valid
    "10  20 30 N",      # double space -> empty second token -> NA
    # NA input
    NA_character_
  )

  expected <- vapply(samples, reference_convert_coord, numeric(1), USE.NAMES = FALSE)
  expect_identical(convert_coords_dms(samples), expected)
})
