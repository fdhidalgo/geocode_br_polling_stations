# Scaled reproduction: does the crew gc-kill bug still fire at pipeline scale
# (28 workers, seconds_idle = Inf) — and does removing the main-process gc() stop it?
# Usage: Rscript scaled_repro.R <MAIN_GC: yes|no>
suppressMessages(library(crew))
args <- commandArgs(trailingOnly = TRUE)
main_gc <- !identical(args[1], "no") # default yes

x <- crew_controller_local(
  name = "scaled",
  workers = 28,
  seconds_idle = Inf, # committed fix
  seconds_wall = Inf, # committed fix
  garbage_collection = TRUE # crew worker-side gc (as in config)
)
x$start()

# One long task (the "Sao Paulo panel batch") ...
x$push(
  command = {
    for (i in 1:90) {
      Sys.sleep(1)
    }
    "LONG_DONE"
  },
  name = "long_task"
)
# ... amid a big stream of short tasks (the ~150 small panel batches), so many
# workers spin up and complete tasks -> connection churn / launcher pruning.
for (i in 1:250) {
  x$push(command = Sys.sleep(0.3), name = paste0("short_", i))
}

cat("MAIN_GC =", main_gc, "| 28 workers, seconds_idle=Inf\n")
t0 <- Sys.time()
long_status <- NA_character_
popped <- 0L
while (difftime(Sys.time(), t0, units = "secs") < 100) {
  out <- x$pop()
  if (!is.null(out)) {
    popped <- popped + 1L
    if (identical(out$name, "long_task")) {
      long_status <- out$status
      cat(
        format(Sys.time(), "%H:%M:%S"),
        "LONG task ->",
        out$status,
        "| error:",
        substr(paste(out$error, collapse = " "), 1, 60),
        "\n"
      )
      break
    }
  }
  if (main_gc) {
    gc(verbose = FALSE)
  } # mimic targets garbage_collection = TRUE in main process
  Sys.sleep(0.2)
}
cat("RESULT: long_task status =", long_status, "| short popped =", popped, "\n")
x$terminate()
