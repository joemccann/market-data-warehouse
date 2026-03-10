# Project Memory

Use this file for durable, cross-session project memory only.

Do not store:
- ephemeral task status
- one-off debugging notes
- temporary counts, dates, or command output

Use this file for:
- stable architecture decisions
- durable workflow rules
- operational facts that future Codex sessions should not have to rediscover

## Durable Facts

- Canonical storage is bronze Parquet, not DuckDB.
- Live equity data is stored per ticker at `~/market-warehouse/data-lake/bronze/asset_class=equity/symbol=<ticker>/data.parquet`.
- DuckDB is rebuilt from bronze parquet when a local analytical DB file is needed.
- `scripts/daily_update.py` is parquet-first and does not hold the live DuckDB write path.
- Daily syncs use IB as the primary source and only use fallback recovery for unresolved target-day gaps.
- Current fallback scope is the repo's U.S. equity and ETF universe on the NYSE trading calendar.
- Current fallback provider order is:
  - Nasdaq historical quote API with `assetclass=stocks`
  - Nasdaq historical quote API with `assetclass=etf`
  - Stooq U.S. daily CSV
- `IBClient.connect()` already retries successive `clientId` values after IB error `326`.
- Preferred IBC startup on macOS is the machine-local secure service installed by `scripts/install_ibc_secure_service.py`, which writes wrappers under `~/ibc/bin`, a LaunchAgent under `~/Library/LaunchAgents/local.ibc-gateway.plist`, and renders a temporary runtime config from `~/ibc/config.secure.ini` plus Keychain secrets instead of storing IB credentials in plaintext config.
- For this repo, the secure IBC service is a required machine-local dependency for IB-backed workflows, but the service itself is global to the user's Mac rather than scoped to this repo.
- `symbol_id` for new symbols is a stable 53-bit `blake2b(symbol)`-derived value.

## Durable Workflow Rules

- For non-trivial work, write a fresh plan to `tasks/todo.md` before editing.
- Every plan must include a dependency graph and `depends_on: []` task annotations.
- If the user corrects an assumption or prior answer, update `tasks/lessons.md`.
- Use `apply_patch` for manual file edits.
- Run coverage for changes in `clients/` or `scripts/`.
- When script tests mock async runners like `ib.ib.run(...)`, also run `-W error::RuntimeWarning` so leaked coroutine warnings fail fast.

## Update Policy

- Update this file only when a rule or fact should survive across future sessions.
- If a detail belongs to operators or contributors generally, also update `README.md` or `CLAUDE.md`.
- If a detail is just about the current task, put it in `tasks/todo.md` instead.
