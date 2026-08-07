## Spec tests for add_consensus_features() (R/model.R).
## Measures each candidate against the rest of its station's candidate cloud. Fixtures put
## every candidate on one meridian, where a haversine distance is just the latitude gap:
## one degree is 111.3194907 km at the pipeline's earth radius, so the expected values below
## are read off the offsets rather than recomputed by the thing under test.

DEG_KM <- 111.3194907

make_cloud <- function() {
  data.table::data.table(
    local_id = c(1L, 1L, 1L, 1L, 2L, 2L, 3L),
    type = c(
      # Station 1: an INEP sibling pair, one CNEFE 2022 street match, and a distant geocodebr.
      "schools_inep_name",
      "schools_inep_addr",
      "st_cnefe_2022",
      "geocodebr",
      # Station 2: two candidates, both out of the 2010 census.
      "st_cnefe_2010",
      "bairro_cnefe_2010",
      # Station 3: nothing but geocodebr.
      "geocodebr"
    ),
    long = -60,
    lat = c(-9.000, -9.001, -9.003, -9.100, -20.000, -20.002, -30.000),
    mindist = c(0.1, 0.2, 0.3, NA, 0.5, 0.6, NA)
  )
}

test_that("add_consensus_features measures agreement across datasets, not sibling types", {
  out <- add_consensus_features(make_cloud())
  inep <- out[local_id == 1L & type == "schools_inep_name"]

  # The nearest candidate of any kind is the INEP address match 0.001 degrees away, but it
  # is the same dataset resolving the same school -- corroboration starts at CNEFE 2022,
  # 0.003 degrees off. geocodebr, 0.1 degrees away, corroborates nothing.
  expect_equal(inep$nearest_other_km, 0.003 * DEG_KM, tolerance = 1e-6)
  expect_equal(inep$nearest_other_dataset, "cnefe_2022")
  expect_equal(inep$n_datasets_within_500m, 1L)
})

test_that("add_consensus_features describes the shape of the whole cloud", {
  out <- add_consensus_features(make_cloud())
  station <- out[local_id == 1L]

  # Six pairwise gaps, siblings included: 0.001, 0.002, 0.003, 0.097, 0.099, 0.100 degrees.
  expect_equal(station[, unique(n_cand)], 4L)
  expect_equal(station[, unique(cloud_dispersion_km)], 0.050 * DEG_KM, tolerance = 1e-6)
  # Median latitude of the four candidates is -9.002.
  expect_equal(
    station[type == "schools_inep_name", dist_to_cloud_median_km],
    0.002 * DEG_KM,
    tolerance = 1e-6
  )
  expect_equal(
    station[type == "geocodebr", dist_to_cloud_median_km],
    0.098 * DEG_KM,
    tolerance = 1e-6
  )
})

test_that("add_consensus_features reports an uncorroborated station as unopposed, not unknown", {
  out <- add_consensus_features(make_cloud())

  # Two candidates, one dataset: the cloud has a spread, but nothing independent agrees.
  same_dataset <- out[local_id == 2L]
  expect_equal(same_dataset[, unique(n_cand)], 2L)
  expect_equal(same_dataset$n_datasets_within_500m, c(0L, 0L))
  expect_true(all(is.na(same_dataset$nearest_other_km)))
  expect_true(all(is.na(same_dataset$nearest_other_dataset)))
  expect_equal(same_dataset[, unique(cloud_dispersion_km)], 0.002 * DEG_KM, tolerance = 1e-6)

  # A lone candidate is trivially its own cloud centre, which is not agreement.
  solo <- out[local_id == 3L]
  expect_equal(solo$n_cand, 1L)
  expect_equal(solo$n_datasets_within_500m, 0L)
  expect_true(is.na(solo$nearest_other_km))
  expect_true(is.na(solo$cloud_dispersion_km))
  expect_true(is.na(solo$dist_to_cloud_median_km))
})

test_that("add_consensus_features counts agreement at 500 m, not looser", {
  # 400 m corroborates, 700 m does not.
  cloud <- data.table::data.table(
    local_id = 1L,
    type = c("schools_inep_name", "st_cnefe_2022", "geocodebr"),
    long = -60,
    lat = -5 - c(0, 0.4, 0.7) / DEG_KM,
    mindist = 0.1
  )
  inep <- add_consensus_features(cloud)[type == "schools_inep_name"]
  expect_equal(inep$n_datasets_within_500m, 1L)
  expect_equal(inep$nearest_other_km, 0.4, tolerance = 1e-6)
})

test_that("add_consensus_features does not depend on the order candidates arrive in", {
  # Two datasets equidistant from the INEP candidate: the nearest-dataset tie has to break
  # on something stable, or an unrelated upstream reordering silently changes the feature.
  cloud <- data.table::data.table(
    local_id = 1L,
    type = c("schools_inep_name", "st_cnefe_2022", "geocodebr"),
    long = -60,
    lat = c(-8.000, -8.001, -7.999),
    mindist = 0.1
  )
  first <- add_consensus_features(cloud)
  shuffled <- add_consensus_features(cloud[c(3L, 1L, 2L)])
  expect_equal(
    as.data.frame(first[order(local_id, type)]),
    as.data.frame(shuffled[order(local_id, type)])
  )
  expect_equal(first[type == "schools_inep_name", nearest_other_dataset], "cnefe_2022")
})

test_that("add_consensus_features fails on a missing coordinate rather than poisoning a cloud", {
  # The other two guards fire on real data the moment they are violated, so a pipeline run
  # is their test. This one the pipeline can never reach, because it filters NA coordinates
  # out before calling -- and its failure is silent, wiping out the cloud centre for every
  # candidate of the station and sorting ahead of the true nearest neighbour.
  cloud <- make_cloud()
  cloud[type == "st_cnefe_2022", lat := NA_real_]
  expect_error(add_consensus_features(cloud), "coordinate on every candidate")
})
