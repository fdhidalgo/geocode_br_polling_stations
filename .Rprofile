source("renv/activate.R")

options(Ncpus = parallel::detectCores())

# geocodebr runs DuckDB inside a fresh subprocess per call. Without a persistent extension
# directory each one re-downloads its DuckDB extensions into a temp dir that dies with the
# process -- thousands of downloads over a production run. A user-level path (not the repo)
# so every worktree shares one copy. An existing setting wins.
if (!nzchar(Sys.getenv("DUCKDB_EXTENSION_DIRECTORY"))) {
  local({
    dir <- tools::R_user_dir("duckdb", "cache")
    dir.create(dir, recursive = TRUE, showWarnings = FALSE)
    Sys.setenv(DUCKDB_EXTENSION_DIRECTORY = dir)
  })
}
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
