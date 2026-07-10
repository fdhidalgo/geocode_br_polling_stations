## Regression test for get_pipeline_config() (R/config.R), cleanup phase 3,
## Medium. The panel record-linkage weight threshold is now an explicit, tracked
## config field (replacing an untracked getOption). Its effective value is kept
## at 0 pending an evaluation decision (ticket #25); guard that here.

test_that("get_pipeline_config exposes panel_weight_threshold defaulting to 0", {
  expect_equal(get_pipeline_config(dev_mode = TRUE)$panel_weight_threshold, 0)
  expect_equal(get_pipeline_config(dev_mode = FALSE)$panel_weight_threshold, 0)
})
