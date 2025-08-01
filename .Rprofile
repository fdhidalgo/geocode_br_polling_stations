source("renv/activate.R")

options(Ncpus = parallel::detectCores())
suppressMessages(rspm::enable())

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
