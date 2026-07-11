# Does keep_crew_launch_handles() (R/config.R) still defeat the crew launcher
# GC-kill bug? Runs the exact minimal_repro.R scenario (churn + main-process
# gc) through the shipped keeper; the long task must SUCCEED where the
# unpatched repro crashes it in ~10s. Run from the repo root:
#   Rscript docs/crew_bug_82/keeper_check.R
# Exits non-zero on failure. Re-run together with minimal_repro.R on any
# crew/mirai/processx upgrade: once minimal_repro.R stops reproducing the
# crash upstream is fixed and the keeper can be dropped; until then this
# script gates that the workaround still works.
suppressMessages(library(crew))
suppressMessages(library(data.table)) # loaded by R/config.R
source("R/config.R")

x <- crew_controller_local(
  name = "keeper_check",
  workers = 3,
  seconds_idle = 2 # induce churn like minimal_repro.R
)
keep_crew_launch_handles(x)
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
status <- NA_character_
while (difftime(Sys.time(), t0, units = "secs") < 90) {
  out <- x$pop() # pop() also triggers scale()
  if (is.null(out) == FALSE) {
    cat(
      format(Sys.time(), "%H:%M:%S"),
      "popped:",
      out$name,
      "status:",
      out$status,
      "\n"
    )
    if (identical(out$name, "long_task")) {
      status <- out$status
      break
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

x$terminate()
cat("\nRESULT: long task status =", status, "\n")
if (identical(status, "success") == FALSE) {
  stop("keeper_check FAILED: the handle keeper no longer prevents the GC-kill")
}
cat("PASS: keep_crew_launch_handles() defeats the GC-kill scenario\n")
