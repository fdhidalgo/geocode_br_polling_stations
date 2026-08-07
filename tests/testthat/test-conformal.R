## Spec tests for the calibrated distance bound (R/model.R): the pinball loss the
## selector is trained and tuned on, the conformal order statistic that turns a
## predicted quantile into a bound with finite-sample coverage, the back-transform
## to kilometres, and the per-station candidate pick all three feed.

library(testthat)
library(data.table)

test_that("pinball_loss_vec charges under- and over-prediction asymmetrically", {
  # tau = 0.9, so a truth above the estimate (under-prediction) costs 9x a miss
  # of the same size in the other direction. That asymmetry is what pulls the fit
  # up to the 90th percentile instead of the mean.
  expect_equal(pinball_loss_vec(truth = 1, estimate = 0), 0.9 * 1)
  expect_equal(pinball_loss_vec(truth = 0, estimate = 1), 0.1 * 1)
  expect_equal(pinball_loss_vec(truth = 5, estimate = 5), 0)

  truth <- c(1, 2, 3, 4)
  estimate <- c(1.5, 1.5, 2.5, 5)
  # residuals -0.5, 0.5, 0.5, -1 -> losses 0.05, 0.45, 0.45, 0.10
  expect_equal(pinball_loss_vec(truth, estimate), mean(c(0.05, 0.45, 0.45, 0.10)))

  # missing pairs drop out rather than poisoning the mean
  expect_equal(
    pinball_loss_vec(c(truth, NA), c(estimate, 1)),
    pinball_loss_vec(truth, estimate)
  )
})

test_that("pinball_loss is a yardstick metric usable in a metric_set", {
  d <- data.frame(truth = c(1, 2, 3, 4), estimate = c(1.5, 1.5, 2.5, 5))
  res <- yardstick::metric_set(pinball_loss)(d, truth = truth, estimate = estimate)
  expect_equal(res$.metric, "pinball_loss")
  # racing and select_best() minimize it; a wrong direction would pick the worst model
  expect_equal(attr(pinball_loss, "direction"), "minimize")
})

test_that("conformal_offset_from_residuals takes the finite-sample order statistic", {
  # n = 19, tau = 0.9 -> ceiling(20 * 0.9) = 18, the 18th of 19 sorted residuals.
  # Deliberately not the 90th percentile of the sample (which would be smaller) and
  # not the max: the (n+1) correction is what buys the finite-sample guarantee.
  resid <- as.numeric(1:19)
  expect_equal(conformal_offset_from_residuals(resid), 18)
  expect_equal(conformal_offset_from_residuals(rev(resid)), 18) # order-independent

  # n = 9 is the floor: ceiling(10 * 0.9) = 9 is attainable, 8 is not.
  expect_equal(conformal_offset_from_residuals(as.numeric(1:9)), 9)
  expect_error(conformal_offset_from_residuals(as.numeric(1:8)), "cannot attain")

  # A missing residual would silently shrink n and inflate coverage.
  expect_error(conformal_offset_from_residuals(c(1:18, NA_real_)), "must be complete")
})

test_that("conformal_bound_km inverts the log transform and never goes negative", {
  # An exact match sits at log(0 + offset); with no correction it comes back to 0 km.
  expect_equal(conformal_bound_km(log(GBM_LOG_OFFSET), 0), 0)
  expect_equal(conformal_bound_km(log(2 + GBM_LOG_OFFSET), 0), 2)
  # The correction is multiplicative on the distance scale, which is what keeps it
  # proportionate across matches spanning metres to tens of kilometres.
  expect_equal(conformal_bound_km(log(2 + GBM_LOG_OFFSET), log(3)), 3 * (2 + GBM_LOG_OFFSET) - GBM_LOG_OFFSET)
  # A negative correction can push the bound below zero; a distance cannot be negative.
  expect_equal(conformal_bound_km(log(GBM_LOG_OFFSET), -5), 0)
})

test_that("select_best_candidate keeps one lowest-bound candidate per station", {
  scored <- data.table(
    local_id = c("l1", "l1", "l1", "l2", "l2"),
    type = c("a", "b", "c", "a", "b"),
    pred_logq = c(2.0, 0.5, 1.0, 3.0, 3.0)
  )
  best <- select_best_candidate(scored)
  expect_equal(nrow(best), 2L)
  expect_equal(best[local_id == "l1"]$type, "b") # smallest predicted quantile
  expect_equal(best[local_id == "l2"]$type, "a") # tie -> first row

  # An unscored candidate must error: it would sort ahead of or behind every real
  # one depending on the sort, silently changing which coordinate ships.
  expect_error(
    select_best_candidate(data.table(local_id = "l1", pred_logq = NA_real_)),
    "missing a predicted quantile"
  )
})
