# crew 1.3.1 launcher GC-kill bug — reproduction scripts

Debugging artifacts for [issue #82](https://github.com/fdhidalgo/geocode_br_polling_stations/issues/82)
(surfaced during the v0.15 release run, #48). Kept so the bug can be filed
upstream at [wlandau/crew](https://github.com/wlandau/crew) and re-verified when
`crew`/`mirai` are upgraded.

## The bug (one paragraph)

`crew` 1.3.1's local launcher starts each worker with
`processx::process$new(..., cleanup = TRUE)`, so the worker is **SIGKILLed when
its handle object is garbage-collected in the main process**. `crew`'s launcher
prunes launch-handle rows under worker churn, and those pruned rows can still
reference **live, busy** workers; the next `gc()` in the main `tar_make()`
process then kills them mid-task. Symptom: `the crew worker of task '<TARGET>'
crashed 6 consecutive time(s)` with a silent death — no R error, no segfault
banner, and no kernel/`systemd-oomd` OOM entry. The reliable victim is the
**longest-running** batch of the active stage (here the São Paulo panel-linkage
batch, 25+ min).

## Scripts

- `minimal_repro.R` — 3 workers, zero memory pressure. `seconds_idle = 2` (churn)
  + a `gc()` in the main poll loop kills a live 60s worker with
  `crash ... Connection reset` in ~10s. Run the controls to confirm the
  mechanism:
  - set `seconds_idle = Inf` → the long task **succeeds** (no churn → no pruning).
  - remove the `gc()` from the loop → **no crash** (pruning alone is harmless).
  So churn **and** main-process GC are both necessary.
- `scaled_repro.R` — 28 workers, `seconds_idle = Inf` (the committed config),
  one long task among 250 short ones, with/without main-process `gc()`. NOTE:
  this did **not** reproduce the crash, which is why `seconds_idle = Inf` alone
  was insufficient for the real pipeline: the production cascade needs a *first*
  kill (from scale-up / heavy-task timing) that trivial sleep tasks don't
  produce. Left here as a documented dead-end.

```sh
Rscript docs/crew_bug_82/minimal_repro.R          # reproduces (churn + gc)
Rscript docs/crew_bug_82/scaled_repro.R yes       # 28 workers, Inf idle, WITH main gc  (did not repro)
Rscript docs/crew_bug_82/scaled_repro.R no         # WITHOUT main gc
```

## Environment where observed

`crew` 1.3.1 (latest on CRAN as of 2026-07-11), `targets` 1.12.0, R 4.5.3,
Ubuntu 24.04, local `crew_controller_local` workers.

## Workarounds applied in this repo

1. **Handle keeper (primary)** — `keep_crew_launch_handles()` in `R/config.R`,
   applied to both controllers: wraps the launcher's `launch_worker()` so every
   processx handle it creates is also retained in a keeper list. The SIGKILL
   finalizer can then never fire on a live worker, no matter which launch rows
   crew prunes. Verified against `minimal_repro.R`: with the keeper the long
   task survives churn + main-process `gc()`, and `terminate()` still reaps all
   workers (no orphans). This made it safe to move `panel_ids_by_batch` back to
   crew workers (it was pinned to `deployment = "main"` during the v0.15
   release run).
2. `get_crew_controllers()` in `R/config.R`: `seconds_idle = Inf` and
   `seconds_wall = Inf` on both controllers (stops idle/wall churn).
   Belt-and-braces; necessary but not sufficient on its own.

Filed upstream as [wlandau/crew#253](https://github.com/wlandau/crew/issues/253).
On any crew upgrade, re-run `minimal_repro.R`; once it stops reproducing, the
handle keeper and the `Inf` timers can be dropped.

See issue #82 for the full history, including the initial (wrong) memory
diagnosis and how it was corrected.
