source("renv/activate.R")

options(Ncpus = parallel::detectCores())
#Prefer binary packages to avoid compilation issues
options(pkgType = "binary")
options(renv.config.pak.enabled = TRUE)

# Only enable rspm on Linux systems.
# Guard with requireNamespace() so a fresh clone (where rspm is not yet
# installed) can still evaluate .Rprofile and run renv::restore() to bootstrap.
if (Sys.info()[["sysname"]] == "Linux" &&
  requireNamespace("rspm", quietly = TRUE)) {
  suppressMessages(rspm::enable())
}

if (interactive()) {
  if (requireNamespace("mcptools", quietly = TRUE)) {
    tryCatch(
      {
        mcptools::mcp_session()
        message("mcptools: MCP session registered")
      },
      error = function(e) {
        message("mcptools error: ", conditionMessage(e))
      }
    )
  } else {
    message("mcptools not installed - MCP features unavailable")
  }
}
