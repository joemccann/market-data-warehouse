# Active Plan

Use this file for the current task only. Replace it at the start of each non-trivial task.

## Objective
- Update the relevant local Python instance to 3.13 without breaking the repo workflow.

## Success Criteria
- Homebrew `python@3.13` is installed on this Mac.
- The resulting `python3.13` executable is verified.
- If safe, the repo venv is upgraded or a clear blocker/tradeoff is identified.

## Dependency Graph
- T1 -> T2
- T2 -> T3

## Tasks
- [x] T1 Inspect current Python installations and the repo venv wiring
  depends_on: []
- [x] T2 Install Homebrew Python 3.13 and verify the binary
  depends_on: [T1]
- [x] T3 Decide and apply the safest repo-local upgrade path for the venv, or stop with a precise reason
  depends_on: [T2]

## Review
- Outcome:
  - Installed Homebrew `python@3.13`, which provides `/opt/homebrew/bin/python3.13`.
  - Rebuilt the repo venv on Python `3.13.12` and swapped it into place at `~/market-warehouse/.venv`.
  - Preserved the previous venv as `~/market-warehouse/.venv-3.12-backup`.
- Verification:
  - `/opt/homebrew/bin/python3.13 -V`
  - `~/market-warehouse/.venv/bin/python -V`
  - `~/market-warehouse/.venv/bin/python -m pip show rich ib-insync duckdb`
  - `~/market-warehouse/.venv/bin/python -c 'import rich, ib_insync, polars, pyarrow, duckdb, requests, pandas; print("imports-ok")'`
- Residual risk:
  - Your shell-wide `python` and `python3` defaults still point at older interpreters unless you update your PATH; only the repo venv is now on 3.13.
  - The old frozen dependency list referenced an obsolete editable `doob` Python package path; that line was intentionally dropped because the current `doob` checkout is not a Python package.
