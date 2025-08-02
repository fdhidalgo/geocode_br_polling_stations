source("renv/activate.R")

options(Ncpus = parallel::detectCores())
##rspm should only be activated on linux

# Only enable rspm on Linux systems
if (Sys.info()[["sysname"]] == "Linux") {
  suppressMessages(rspm::enable())
}

if (interactive()) {
  if (requireNamespace("mcptools", quietly = TRUE)) {
    tryCatch(
      {cl
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
