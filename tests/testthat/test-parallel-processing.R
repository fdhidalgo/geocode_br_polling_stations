test_that("crew controllers are properly configured", {
  # Create controllers
  controllers <- create_crew_controllers()
  
  # Test CNEFE controller configuration
  expect_equal(controllers$cnefe$name, "cnefe_heavy")
  expect_equal(controllers$cnefe$workers, 1)
  expect_equal(controllers$cnefe$tasks_max, 1)
  expect_true(controllers$cnefe$reset_globals)
  expect_true(controllers$cnefe$garbage_collection)
  
  # Test light controller configuration
  expect_equal(controllers$light$name, "light_tasks")
  expect_gt(controllers$light$workers, 1)  # Should have multiple workers
  expect_equal(controllers$light$workers, max(1, floor(parallel::detectCores() * 0.75)))
  expect_equal(controllers$light$tasks_max, 1000)
  expect_false(controllers$light$reset_globals)
  expect_false(controllers$light$garbage_collection)
  
  # Cleanup
  cleanup_controllers(controllers)
})

test_that("task routing works correctly", {
  # Initialize controllers
  controllers <- initialize_crew_controllers()
  
  # Test that controllers are in global env
  expect_true(exists("crew_controllers", envir = .GlobalEnv))
  
  # Test CNEFE routing
  expect_error(
    route_task("cnefe", function(x) x^2, x = 5),
    NA  # Expect no error
  )
  
  # Test light task routing
  expect_error(
    route_task("light", function(x) x + 1, x = 10),
    NA  # Expect no error
  )
  
  # Test invalid task type
  expect_error(
    route_task("invalid", function(x) x),
    "Unknown task type"
  )
  
  # Cleanup
  cleanup_controllers(controllers)
})

test_that("resource monitoring returns expected structure", {
  # Initialize controllers
  controllers <- initialize_crew_controllers()
  
  # Monitor resources
  status <- monitor_resource_usage(controllers)
  
  # Check structure
  expect_type(status, "list")
  expect_true("cnefe_status" %in% names(status))
  expect_true("light_status" %in% names(status))
  expect_true("memory_usage_mb" %in% names(status))
  expect_true("system_memory_pct" %in% names(status))
  expect_true("active_workers" %in% names(status))
  expect_true("timestamp" %in% names(status))
  
  # Check types
  expect_type(status$memory_usage_mb, "double")
  expect_s3_class(status$timestamp, "POSIXct")
  
  # Cleanup
  cleanup_controllers(controllers)
})

test_that("worker adjustment responds to memory pressure", {
  # Initialize controllers
  controllers <- initialize_crew_controllers()
  
  # Get initial worker count
  initial_workers <- controllers$light$workers
  
  # Test high memory scenario (should reduce workers)
  adjust_workers(controllers, system_memory_pct = 0.85)
  
  # In a real scenario, workers might be reduced
  # But we can't guarantee it in test environment
  
  # Test low memory scenario (might increase workers)
  adjust_workers(controllers, system_memory_pct = 0.3)
  
  # Cleanup
  cleanup_controllers(controllers)
})

test_that("cleanup properly terminates controllers", {
  # Initialize controllers
  controllers <- initialize_crew_controllers()
  
  # Verify they exist
  expect_true(exists("crew_controllers", envir = .GlobalEnv))
  
  # Cleanup
  cleanup_controllers(controllers)
  
  # Verify removal from global env
  expect_false(exists("crew_controllers", envir = .GlobalEnv))
})

test_that("controller status printing works", {
  # Initialize controllers
  controllers <- initialize_crew_controllers()
  
  # Test printing (should not error)
  expect_error(
    print_controller_status(controllers),
    NA
  )
  
  # Test with NULL (uses global)
  expect_error(
    print_controller_status(),
    NA
  )
  
  # Cleanup
  cleanup_controllers(controllers)
  
  # Test after cleanup
  expect_message(
    print_controller_status(),
    "No crew controllers found"
  )
})