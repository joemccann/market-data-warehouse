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
- Delisted symbols that should no longer participate in future syncs or backfills are archived outside the canonical sync path under `~/market-warehouse/data-lake/bronze-delisted/asset_class=equity/symbol=<ticker>/data.parquet`.
- DuckDB is rebuilt from bronze parquet when a local analytical DB file is needed.
- `scripts/daily_update.py` is parquet-first and does not hold the live DuckDB write path.
- `scripts/daily_update.py` supports `--target-date YYYY-MM-DD` for fixed-date catch-up runs and only publishes bars with `latest < trade_date <= target`.
- Scheduled daily syncs now run through `scripts/run_daily_update_job.py`, which retries failures before sending Nodemailer-based terminal alerts.
- A separate `scripts/check_daily_update_watchdog.py` watchdog is available to alert when the scheduled daily sync never starts or never writes a completion marker.
- Failure alerts can now generate a human-readable Markdown incident report and include a Cerebras-generated summary plus proposed remediation in the email body when the AI config is available.
- Daily syncs use IB as the primary source and only use fallback recovery for unresolved target-day gaps.
- Current fallback scope is the repo's U.S. equity and ETF universe on the NYSE trading calendar.
- Current fallback provider order is:
  - Nasdaq historical quote API with `assetclass=stocks`
  - Nasdaq historical quote API with `assetclass=etf`
  - Stooq U.S. daily CSV
- `IBClient.connect()` already retries successive `clientId` values after IB error `326`.
- `DBClient.replace_equities_from_parquet()` recreates the analytical tables from scratch on each rebuild so repeat DuckDB rebuilds are safe against an existing DB file.
- Preferred IBC startup on macOS is the machine-local secure service installed by `scripts/install_ibc_secure_service.py`, which writes wrappers under `~/ibc/bin`, a LaunchAgent under `~/Library/LaunchAgents/local.ibc-gateway.plist`, and renders a temporary runtime config from `~/ibc/config.secure.ini` plus Keychain secrets instead of storing IB credentials in plaintext config.
- For this repo, the secure IBC service is a required machine-local dependency for IB-backed workflows, but the service itself is global to the user's Mac rather than scoped to this repo.
- `symbol_id` for new symbols is a stable 53-bit `blake2b(symbol)`-derived value.
- The native macOS client work now lives under `macos/` as a repo-local Swift package that Xcode can open directly.
- The selected initial macOS UI direction is `Option 3: Operator Pilot`.
- The current macOS client includes first-run setup, a Settings scene, local session restore, provider-backed chat through installed CLIs, and raw DuckDB CLI passthrough via `/duckdb ...` plus the diagnostics drawer.
- The macOS client now follows a hybrid SwiftUI plus MetalKit architecture: native shell controls with `MTKView`-backed workspace panels driven by a compact render snapshot model.
- The local app bundle precompiles `OperatorPilotMetalShaders.metallib` with `macos/scripts/compile_metal_library.sh`; if the compiler is missing locally, install the optional Xcode component with `xcodebuild -downloadComponent metalToolchain`.
- The macOS client now has a repo-local UI smoke harness at `macos/scripts/run_ui_smoke_tests.sh` that drives the built app through keyboard shortcuts and validates visible window text via OCR in an isolated throwaway session.
- For future macOS work, start with `macos/README.md` as the package index; it points at the live build paths, research docs, render artifacts, and launcher flow.
- The repo-local macOS research set now includes `macos/docs/ai-chat-ux-best-practices.html`, `macos/docs/setup-and-settings-research.md`, and `macos/docs/metal-best-practices.md`.
- The repo-local macOS assistant UX audit skill lives at `macos/.codex/skills/ai-chat-ux-best-practices/` and is the project-specific reference point for future UX review work inside `macos/`.
- The repo-local quant backtesting skill lives at `.codex/skills/quant-backtest/` and should be used for future backtesting or systematic strategy tasks in this repo.

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
