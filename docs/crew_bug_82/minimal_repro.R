# Minimal repro: does crew 1.3.1 local launcher lose live workers to
# processx handle GC after launcher$scale() drops "irrelevant" launch rows?
suppressMessages(library(crew))

x <- crew_controller_local(
  name = "repro",
  workers = 3,
  seconds_idle = 2 # induce churn like the pipeline's seconds_idle = 30
)
x$start()

# One long task (the "Sao Paulo batch") ...
x$push(
  command = {
    pid <- Sys.getpid()
    for (i in 1:60) {
      Sys.sleep(1)
    }
    pid
  },
  name = "long_task"
)
# ... plus short tasks so workers finish, idle-exit, and get replaced.
for (i in 1:2) {
  x$push(command = Sys.sleep(0.5), name = paste0("short_", i))
}

t0 <- Sys.time()
round1 <- 0L
crashed <- FALSE
while (difftime(Sys.time(), t0, units = "secs") < 55) {
  out <- x$pop() # pop() also triggers scale()
  if (is.null(out) == FALSE) {
    cat(
      format(Sys.time(), "%H:%M:%S"),
      "popped:",
      out$name,
      "status:",
      out$status,
      "error:",
      substr(paste(out$error, collapse = " "), 1, 80),
      "\n"
    )
    if (identical(out$name, "long_task")) {
      crashed <- identical(out$status, "crash") || crashed
      if (out$status %in% c("success", "crash", "error")) break
    }
  }
  # After the first wave of short tasks completes and idles out,
  # push more short tasks to force NEW launches + connections
  el <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  if (el > 8 && round1 < 4L) {
    round1 <- round1 + 1L
    x$push(command = Sys.sleep(0.5), name = paste0("wave2_", round1))
  }
  gc(verbose = FALSE) # main-process GC, as targets does with garbage_collection = TRUE
  Sys.sleep(0.5)
}

cat("\nRESULT: long task crashed =", crashed, "\n")
cat("launcher rows remaining:", nrow(x$launcher$launches %||% data.frame()), "\n")
x$terminate()
