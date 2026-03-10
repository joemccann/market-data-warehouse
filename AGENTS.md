# Codex Agent Guide

This file is the repo-root startup guide for Codex. Keep it concise, durable, and aligned with the live codebase.

## Session Start

At the start of every new Codex session in this repo:

1. Read [CLAUDE.md](/Users/joemccann/dev/apps/finance/market-data-warehouse/CLAUDE.md) for implementation details, repo layout, and testing rules.
2. Read [README.md](/Users/joemccann/dev/apps/finance/market-data-warehouse/README.md) for the current architecture, runtime behavior, and operator-facing commands.
3. Read [.codex/project-memory.md](/Users/joemccann/dev/apps/finance/market-data-warehouse/.codex/project-memory.md) for durable project-specific memory that should persist across sessions.
4. Read [tasks/lessons.md](/Users/joemccann/dev/apps/finance/market-data-warehouse/tasks/lessons.md) when the task touches workflow, operational recovery, or a recently corrected mistake.
5. Run `git status --short` before making assumptions about the worktree.

## Project Purpose

This repo is a local-first market data warehouse optimized for single-machine operation.

Current live shape:
- Canonical storage is per-ticker bronze Parquet under `~/market-warehouse/data-lake/bronze/asset_class=equity/symbol=<ticker>/data.parquet`
- DuckDB is a local analytical and rebuild target, not the live write path
- Interactive Brokers is the primary source for ingestion
- Daily syncs can recover unresolved target-day gaps for the current U.S. equity universe with a narrow external fallback chain
- The long-term direction is broader multi-asset support and future ClickHouse publishing

## Working Rules

- For non-trivial work, write a plan to [tasks/todo.md](/Users/joemccann/dev/apps/finance/market-data-warehouse/tasks/todo.md) first.
- Every plan must include a dependency graph and `depends_on: []` task annotations.
- Use `rg` for search and `rg --files` for file discovery.
- Use `apply_patch` for manual file edits.
- Do not revert unrelated user changes.
- Treat bronze Parquet as the system of record unless the task explicitly says otherwise.
- Keep changes minimal and direct. Prefer the smallest coherent fix over speculative refactors.

## Coding Expectations

- Prefer Python 3.12-compatible code.
- Preserve the current parquet-first write path.
- Keep data integrity explicit: validate before publish, keep atomic file replacement semantics intact.
- Keep runtime behavior observable. If you add a recovery path or new branch, expose enough counters or logs to make it diagnosable.
- Do not introduce a second canonical write path for the same data.

## Testing Expectations

- All code in `clients/` and `scripts/` needs tests.
- The repo enforces `100%` coverage for the configured source set.
- Before finishing meaningful changes, run:
  - `source ~/market-warehouse/.venv/bin/activate`
  - `python -m pytest tests -q --cov=clients --cov=scripts --cov-report=term-missing`
- When script tests mock async runners such as `ib.ib.run(...)`, also run:
  - `python -m pytest tests -q -W error::RuntimeWarning`
- When fixing a bug, add or update a regression test if it fits.

## Bug Fixing

- Start from the actual failing behavior: logs, tests, or reproducible commands.
- Fix the root cause, not just the symptom.
- If the issue is in a test seam, prefer fixing the seam instead of adding runtime-only workaround logic.
- If the user corrects a prior assumption or answer, update [tasks/lessons.md](/Users/joemccann/dev/apps/finance/market-data-warehouse/tasks/lessons.md).

## Operational Facts

- For this repo, IB Gateway is expected on `127.0.0.1:4001` via the global machine-local secure IBC service installed under `~/ibc`, `~/ibc-install`, and `~/Library/LaunchAgents/local.ibc-gateway.plist`.
- That secure IBC service is required for IB-backed workflows in this repo, but the service itself is not scoped to this repo and should be treated as shared machine-local infrastructure.
- `IBClient.connect()` already retries successive `clientId` values after IB error `326`.
- `scripts/daily_update.py` is the scheduled parquet-first daily sync.
- `scripts/rebuild_duckdb_from_parquet.py` rebuilds DuckDB from bronze when a local DB file is needed.
- Daily fallback provider order is:
  - Nasdaq historical quote API with `assetclass=stocks`
  - Nasdaq historical quote API with `assetclass=etf`
  - Stooq U.S. daily CSV

## Memory Files

- Use [.codex/project-memory.md](/Users/joemccann/dev/apps/finance/market-data-warehouse/.codex/project-memory.md) for durable, cross-session project memory.
- Do not put ephemeral task state there. Use [tasks/todo.md](/Users/joemccann/dev/apps/finance/market-data-warehouse/tasks/todo.md) for active work and [tasks/lessons.md](/Users/joemccann/dev/apps/finance/market-data-warehouse/tasks/lessons.md) for correction-driven lessons.
- If a project rule, architecture decision, or stable operational fact changes, update `.codex/project-memory.md` in the same task.
