source("renv/activate.R")

options(Ncpus = parallel::detectCores())
#Prefer binary packages to avoid compilation issues
options(pkgType = "binary")
options(renv.config.pak.enabled = TRUE)

# Only enable rspm on Linux systems
if (Sys.info()[["sysname"]] == "Linux") {
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
