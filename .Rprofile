source("renv/activate.R")

options(Ncpus=parallel::detectCores())
suppressMessages(rspm::enable())

if (interactive()) {
  if (requireNamespace("acquaint", quietly = TRUE)) {
    tryCatch({
      acquaint::mcp_session()
      message("acquaint: MCP session registered")
    }, error = function(e) {
      message("acquaint error: ", conditionMessage(e))
    })
  } else {
    message("acquaint not installed - MCP features unavailable")
  }
}