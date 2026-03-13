# Market Data Warehouse

![Market Data Warehouse](https://raw.githubusercontent.com/joemccann/market-data-warehouse/main/.github/github-banner.png)

A local-first financial data warehouse for universe-scale market data.

The project is designed to store and analyze historical **OHLCV data across equities, options, and futures** with a path from **daily bars today to intraday data later**. It uses a **partitioned Parquet data lake** as the canonical storage layer, **DuckDB** as the fast local analytical engine for research and backtesting, and **ClickHouse** as the production-oriented warehouse for large-scale aggregation, serving, and concurrency.

Today, the implemented ingestion path is Python-first and daily-equity-focused: Interactive Brokers data lands directly in per-ticker bronze snapshots under `data-lake/bronze/asset_class=equity/symbol=<ticker>/data.parquet`, and DuckDB is rebuilt from parquet when you want a local analytical file. Delisted symbols that should no longer participate in future syncs can be archived out of the canonical sync path under `data-lake/bronze-delisted/asset_class=equity/symbol=<ticker>/data.parquet` while preserving their history. The broader staged-Parquet and multi-asset orchestration remains the target architecture, but the live service path no longer writes `market.duckdb`. For scheduled daily syncs, IB remains the primary source and the service now has a narrow external fallback chain for unresolved target-day gaps in the current U.S. equity universe.

The goal is to give you:

* a **high-performance local quant research environment** on Apple Silicon
* a clean **multi-asset schema** that can handle very large datasets
* a **polyglot workflow** across Python, Rust, and Node.js
* a straightforward **path to cloud production** without rebuilding the data model from scratch

In practice, this project is meant to be the foundation for:

* historical market data storage
* factor research
* backtesting
* rolling analytics like VWAP, moving averages, and cross-sectional signals
* future expansion into intraday and production-grade analytical serving

In one sentence:

**It’s a local-first, production-ready market data warehouse for serious quantitative research and analytics.**

## Native macOS Client

A native macOS client now ships in [`macos/`](macos/) as a repo-local Swift package that Xcode can open directly. The current implementation follows the selected `Option 3: Operator Pilot` direction and is built for local parquet and DuckDB exploration inside a chat-first desktop shell.

Current live app behavior:

* first launch is gated by setup until a default provider is configured
* a native Settings scene is available both in-app and through standard macOS Settings
* provider-backed chat runs through the installed local `claude`, `codex`, or `gemini` CLI, with Keychain-stored API keys as fallback
* raw DuckDB CLI passthrough is available in chat via `/duckdb ...` and in the diagnostics drawer
* session state persists across relaunches, including sources, settings, and transcripts
* the native client now uses a hybrid SwiftUI plus MetalKit rendering model for its workspace surfaces
* command-key navigation is wired for the main destinations and app controls

Build, test, and open the native client locally:

```bash
cd macos
swift build
swift test
./scripts/run_ui_smoke_tests.sh
./scripts/build_local_macos_app.sh
open "build/Market Data Warehouse.app"
```

If the local Metal compiler is missing, install the Xcode component once:

```bash
xcodebuild -downloadComponent metalToolchain
```

For Finder-based local testing without Terminal, generate the launcher once:

```bash
cd macos
./scripts/build_local_launcher.sh
open "launcher/Launch Market Data Warehouse.app"
```

Primary keyboard commands:

* `Cmd-O` open a parquet, DuckDB, or SQLite source
* `Cmd-Shift-R` rerun setup
* `Cmd-Shift-D` toggle the diagnostics drawer
* `Cmd-L` focus the chat composer
* `Cmd-1` through `Cmd-4` switch between Assistant, Transcripts, Setup, and Settings

## Dependencies

- **Python 3.12+** — [python.org](https://www.python.org/downloads/) or `brew install python@3.12`
- **Node.js 22+** — [nodejs.org](https://nodejs.org/) or `brew install node@22` (required for scheduled failure emails)
- **DuckDB** — [duckdb.org](https://duckdb.org/docs/installation/) or `brew install duckdb`
- **Interactive Brokers account** — [interactivebrokers.com](https://www.interactivebrokers.com/) (live or paper trading account required for market data API access)
- **IB Gateway** (offline version) — [download page](https://www.interactivebrokers.com/en/trading/ibgateway-stable.php) (included with TWS installer; must be the offline/stable version)
- **IBC** (IB Controller) — [github.com/IbcAlpha/IBC](https://github.com/IbcAlpha/IBC/releases/latest) (automates Gateway login, reconnection, and daily restarts)
- **ClickHouse** (optional) — [clickhouse.com](https://clickhouse.com/docs/en/install) or `brew install clickhouse` (only needed for production-style benchmarking)
- **Homebrew** (macOS) — [brew.sh](https://brew.sh/)

### Python packages (installed in `~/market-warehouse/.venv/`)

- `duckdb` — local analytical engine
- `ib_insync` — async IB API client
- `polars`, `pandas`, `pyarrow` — data manipulation and Parquet I/O
- `requests` — public fallback quote lookups during daily sync recovery
- `rich` — terminal UI (progress bars, logging)
- `pytest`, `pytest-cov`, `responses` — testing


## IB Gateway with IBC (Automated Connection Management)

IBC automates IB Gateway startup, login, reconnection, and daily restarts — making API connections robust and hands-free.

For this repo, IBC is a required machine-local dependency for any workflow that talks to Interactive Brokers. The secure service lives under `~/ibc`, `~/ibc-install`, and `~/Library/LaunchAgents` on the user's Mac. It is required for this project, but it is not scoped to this repo and should be treated as a global local service.

### Installation

1. Download the latest macOS release from [IBC Releases](https://github.com/IbcAlpha/IBC/releases/latest)
2. Unzip to `~/ibc-install/`:
   ```bash
   mkdir -p ~/ibc-install && unzip IBCMacos-*.zip -d ~/ibc-install
   chmod +x ~/ibc-install/*.sh ~/ibc-install/scripts/*.sh
   ```
3. Copy the IBC config to the secure launcher config path:
   ```bash
   mkdir -p ~/ibc ~/ibc/logs
   cp ~/ibc-install/config.ini ~/ibc/config.secure.ini
   ```
4. Edit `~/ibc/config.secure.ini` with the non-secret settings only, and remove any `IbLoginId=` / `IbPassword=` lines:
   ```ini
   TradingMode=live
   AcceptIncomingConnectionAction=accept
   ExistingSessionDetectedAction=primary
   ReloginAfterSecondFactorAuthenticationTimeout=yes
   AutoRestartTime=11:58 PM
   ColdRestartTime=07:05
   CommandServerPort=7462
   ```
5. Store the IB username and password in the macOS Keychain and trust `/usr/bin/security` for non-interactive reads:
   ```bash
   security add-generic-password -a ibc -s com.market-warehouse.ibc.username -w 'YOUR_IB_USERNAME' -U -T /usr/bin/security
   security add-generic-password -a ibc -s com.market-warehouse.ibc.password -w 'YOUR_IB_PASSWORD' -U -T /usr/bin/security
   ```
   If you already created these items without `-T /usr/bin/security`, re-run the same commands with `-U` to update the trusted-app access list.
   These keychain service names are compatibility defaults used by the installer and launcher. They are configurable, and the installed IBC service itself remains a machine-global service rather than a repo-scoped one.

6. Install the machine-local secure service:

```bash
python scripts/install_ibc_secure_service.py
```

This installer provisions the global local IBC service required by this project. It writes:

- `~/ibc/bin/run-secure-ibc-gateway.sh`
- `~/ibc/bin/start-secure-ibc-service.sh`
- `~/ibc/bin/stop-secure-ibc-service.sh`
- `~/ibc/bin/restart-secure-ibc-service.sh`
- `~/ibc/bin/status-secure-ibc-service.sh`
- `~/Library/LaunchAgents/local.ibc-gateway.plist`

If a legacy `com.market-warehouse.ibc-gateway` or `com.convex-scavenger.ibc-gateway` LaunchAgent already exists, the installer preserves its `StartCalendarInterval`, moves the old plist aside, and bootstraps `local.ibc-gateway` instead.

You can override paths with flags if your install differs:

```bash
python scripts/install_ibc_secure_service.py \
  --ibc-dir ~/ibc \
  --ibc-install-dir ~/ibc-install \
  --applications-dir ~/Applications \
  --tws-settings-path ~/Jts
```

The installed runner reads the username and password from Keychain at runtime, writes a temporary `0600` runtime config file, passes only the temp config path to IBC, and removes the temp file after IBC exits. Once installed, the service no longer depends on another repo checkout path and should be managed as a machine-level service rather than a project-owned daemon.

### Keychain Access Behavior

- You should not need to approve access on every launch if the items were created with `-T /usr/bin/security` and your login keychain is already unlocked.
- Run this as your logged-in user, for example from a `LaunchAgent`, not a system `LaunchDaemon`.
- If the login keychain is locked or the items were created without the trusted-app entry, macOS can still prompt you.

### Secure IBC Service Commands

```bash
~/ibc/bin/start-secure-ibc-service.sh
~/ibc/bin/stop-secure-ibc-service.sh
~/ibc/bin/restart-secure-ibc-service.sh
~/ibc/bin/status-secure-ibc-service.sh
```

The LaunchAgent label is `local.ibc-gateway`.

### Project Dependency Model

- This project requires a working local IBC service whenever you run IB-backed ingestion or daily update flows.
- The service is global to the machine, not scoped to this repo, and can be shared by multiple local projects that use the same IB Gateway install.
- The repo provides an installer and documentation for that service, but the installed artifacts live under the user's home directory rather than inside the repo.

### IBC Commands (while Gateway is running)

```bash
~/ibc-install/stop.sh              # Clean shutdown
~/ibc-install/reconnectdata.sh     # Reconnect market data (Ctrl+Alt+F)
~/ibc-install/reconnectaccount.sh  # Reconnect to IB login server (Ctrl+Alt+R)
```

### What IBC handles automatically

- Auto-login with credentials on startup
- Auto-accepts incoming API connections
- Auto-restarts Gateway daily (default 11:58 PM) without re-authentication
- Cold restart on Sundays (07:05) with full re-authentication
- Retries 2FA if you miss the IBKR Mobile alert
- Overrides existing sessions to maintain primary connection

### IBC Logs

```bash
cat ~/ibc/logs/ibc-*.txt   # Current session log from the stock IBC wrapper
```

The secure LaunchAgent writes stdout/stderr to `~/ibc/logs/ibc-gateway-service.log`.

### Low-Level Direct Launch

For troubleshooting, the repo still includes a direct launcher:

```bash
python scripts/start_ibc_gateway_keychain.py --tws-major-version 10.44
```

This is a low-level fallback. The installed machine-local service above is the preferred operational path.

### Legacy Plaintext Startup

IBC's stock `~/ibc-install/gatewaystartmacos.sh` still works with a plaintext `~/ibc/config.ini`, but this repo now recommends the global secure service above instead of storing `IbLoginId` and `IbPassword` on disk.

### IB Connection Behavior in This Repo

- `IBClient.connect()` still defaults to `clientId=0`
- If IB Gateway rejects the connection with error `326` (`client id already in use`), the client now retries successive `clientId` values automatically before giving up
- This makes launchd- or crash-restart scenarios more resilient, but explicit `client_id` assignment is still recommended when you need stable concurrent clients

## Data Ingestion

### Prerequisites

Requires **IB Gateway** running on localhost via IBC. For this repo, the recommended and documented path is the installed secure machine-local service `local.ibc-gateway`; manual startup is only a fallback. Default port: `4001`.

### Fetch Historical OHLCV

```bash
source ~/market-warehouse/.venv/bin/activate

# Mag 7 default, inception to present:
python scripts/fetch_ib_historical.py

# Specific tickers:
python scripts/fetch_ib_historical.py --tickers AAPL NVDA

# From a preset file (with cursor-based resume):
python scripts/fetch_ib_historical.py --preset presets/sp500.json

# Custom years, port, concurrency:
python scripts/fetch_ib_historical.py --years 10 --port 7497 --max-concurrent 4

# Backfill missing older data for tickers already in bronze parquet:
python scripts/fetch_ib_historical.py --preset presets/sp500.json --backfill
```

Current behavior:
- Normal mode atomically rewrites the canonical per-ticker bronze snapshot
- The writer uses `temp -> validate -> os.replace()` for crash-safe publication
- If IB returns an empty head timestamp for a symbol, the fetcher falls back to the earliest supported IB historical date instead of skipping the symbol entirely

### Backfill Mode

When tickers were initially fetched with limited history (e.g., `--years 10`), use `--backfill` to fill in older data without re-fetching everything:

```bash
python scripts/fetch_ib_historical.py --preset presets/sp500.json --backfill
```

Backfill mode:
- Detects each ticker's oldest existing date in bronze parquet
- Fetches only the gap from IB inception to that oldest date
- Merges older bars into the existing per-ticker snapshot without deleting newer data
- Uses a separate cursor JSON file (`cursor_backfill_{name}.json`) for independent tracking
- Skips tickers not yet in bronze parquet (use normal fetch for those first)

### Auto-Restarting Runner

For large-scale fetches across multiple presets with stall detection and auto-restart:

```bash
source ~/market-warehouse/.venv/bin/activate
bash scripts/run_backfill_all.sh
```

This script:
1. Finishes any incomplete normal fetches for sp500, ndx100, r2k
2. Runs backfill for all three presets
3. Monitors cursor file progress; restarts with cooldown if stalled
4. Gives up after 3 consecutive failures with no new completions

This publishes:
- **Bronze Parquet** → `~/market-warehouse/data-lake/bronze/asset_class=equity/symbol=<ticker>/data.parquet`
- **DuckDB** remains an optional rebuild target from bronze parquet

### Daily Updates

`daily_update.py` is a lightweight script for daily scheduled runs (~2,500 tickers). It discovers tickers from bronze parquet, detects gaps vs the latest trading day, fetches only missing bars, validates OHLCV integrity, and atomically rewrites only the affected per-ticker snapshots. If IB still cannot supply the target trading day for a symbol, the script can recover that missing day from a narrow public fallback chain before publishing parquet.

```bash
source ~/market-warehouse/.venv/bin/activate

# Normal daily run (all tickers in bronze parquet):
python scripts/daily_update.py

# Dry-run — report gaps without fetching:
python scripts/daily_update.py --dry-run

# Force run on a non-trading day (manual catch-up):
python scripts/daily_update.py --force

# Recover through a fixed trading date without publishing later bars:
python scripts/daily_update.py --target-date 2026-03-11

# Limit to a specific preset:
python scripts/daily_update.py --preset presets/sp500.json

# Custom IB port and concurrency:
python scripts/daily_update.py --port 7497 --max-concurrent 4 --batch-size 25
```

**Key design:**
- Discovers tickers from bronze parquet via `BronzeClient.get_latest_dates()` — no hardcoded lists
- `--target-date YYYY-MM-DD` lets operators run a fixed-date catch-up, and the publish path caps inserted bars to that target date
- Parquet-first live writes avoid DuckDB file-lock contention during service runs
- Bar validation: checks OHLCV relationships, positive prices, valid trading days, duplicate dates
- Uses atomic per-ticker snapshot publication after each successful merge
- The active sync universe is the canonical bronze tree only; archive delisted symbols outside that tree if they should stop participating in future syncs/backfills
- Fallback chain for unresolved target-day gaps: Nasdaq historical quote API (`assetclass=stocks`, then `assetclass=etf`) and finally Stooq U.S. daily CSV (`symbol.us`)
- Fallback bars go through the same validation and `BronzeClient.merge_ticker_rows(...)` path as IB bars
- Run summary exposes `Fallback attempts`, `Fallback successes`, and `Fallback symbols`
- Pure-Python NYSE trading calendar (no new dependencies)
- Logs to `~/market-warehouse/logs/daily_update_YYYY-MM-DD.log`

Fallback scope and limits:
- Scoped to the repo's current live universe: U.S. equities and ETFs on the NYSE trading calendar
- Recovery mechanism only; IB remains the system-preferred source
- If IB is broadly down and many symbols need fallback at once, public endpoints may rate-limit or show small volume differences versus IB

### Rebuild DuckDB From Bronze

When you want a fresh queryable DuckDB file without making the service hold a write lock:

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/rebuild_duckdb_from_parquet.py
```

This rebuilds `~/market-warehouse/duckdb/market.duckdb` from the canonical bronze parquet tree using set-based `INSERT INTO ... SELECT FROM read_parquet(...)`. The rebuild path recreates the analytical tables from scratch on each run, so it is safe to rerun against an existing DuckDB file.

## Strategies (Extracted to doob)

All backtesting and strategy code has been extracted to the standalone **[doob](../doob/)** package (`~/dev/apps/finance/doob`). The `strategies/` directory in this repo is kept for reference but is no longer the canonical source.

The doob package includes:
- **Breadth washout** — oversold/overbought signal modes across named universes (ndx100, sp500, r2k, all-stocks), custom presets, and explicit ticker lists
- **NDX-100 SMA breadth** — breadth-analysis helpers and NDX-focused utilities
- **Overnight drift** — buy SPY at close, sell next open, with VIX regime filter
- **Intraday drift** — buy at open, sell at close (long or short)
- **Shared metrics** — CAGR, Sharpe, max drawdown, VaR, IBKR fee model

### Quick start

```bash
cd ~/dev/apps/finance/doob
source ~/market-warehouse/.venv/bin/activate
pip install -e ".[all]"

python -m doob list-strategies
python -m doob run breadth-washout --universe sp500 --signal-mode oversold --end-date 2026-03-11
python -m doob run overnight-drift --help
```

### Scheduling with launchd (macOS)

```bash
# Copy examples and substitute your repo path
sed "s|/path/to/repo|$(pwd)|g" scripts/com.market-warehouse.daily-update.plist.example > ~/Library/LaunchAgents/com.market-warehouse.daily-update.plist
sed "s|/path/to/repo|$(pwd)|g" scripts/com.market-warehouse.daily-update-watchdog.plist.example > ~/Library/LaunchAgents/com.market-warehouse.daily-update-watchdog.plist
launchctl load ~/Library/LaunchAgents/com.market-warehouse.daily-update.plist
launchctl load ~/Library/LaunchAgents/com.market-warehouse.daily-update-watchdog.plist
```

`scripts/run_daily_update.sh` now loads `.env` files, activates the warehouse venv, and runs `scripts/run_daily_update_job.py`, which retries failed sync attempts before marking the day as failed.

The main sync runs at **13:05 Pacific local time daily** (**4:05 PM Eastern year-round**). The watchdog runs at **18:30 Pacific** by default and sends an alert if the main job never started or never logged a successful completion marker. Non-trading days are harmless no-ops because `daily_update.py` checks `is_trading_day()` internally and exits early.

### Daily Update Failure Alerts

The failure mailer lives at `scripts/send_daily_update_failure_email.mjs`.

Alert flow:

- `scripts/run_daily_update_job.py` retries the sync up to `MDW_DAILY_UPDATE_MAX_ATTEMPTS` times with `MDW_DAILY_UPDATE_RETRY_DELAY_SECONDS` delays between attempts.
- If the sync still fails terminally, the runner sends a rich HTML plus plain-text failure email with the final error summary, a Cerebras-generated human-readable incident summary, a proposed fix, and the raw log tail.
- `scripts/check_daily_update_watchdog.py` runs later in the day and sends the same style of email if the scheduled sync never started or never wrote a completion marker.
- The mailer writes a sibling human-readable Markdown incident report next to the raw log file, for example `daily_update_YYYY-MM-DD.human.md`.

Install the Node dependency once from the repo root:

```bash
npm install
```

Configure the mailer with environment variables in the same environment that launches the daily update job:

```bash
MDW_DAILY_UPDATE_MAX_ATTEMPTS="3"
MDW_DAILY_UPDATE_RETRY_DELAY_SECONDS="300"
MDW_NODE_BIN="/opt/homebrew/bin/node"

MDW_ALERT_EMAIL_FROM="market-warehouse@example.com"
MDW_ALERT_EMAIL_TO="you@example.com"

# Option 1: full SMTP URL
MDW_ALERT_SMTP_URL="smtp://user:pass@mail.example.com:587"

# Option 2: explicit SMTP fields
MDW_ALERT_SMTP_HOST="mail.example.com"
MDW_ALERT_SMTP_PORT="587"
MDW_ALERT_SMTP_SECURE="false"
MDW_ALERT_SMTP_USER="smtp-user"
MDW_ALERT_SMTP_PASS="smtp-password"

# Optional Cerebras AI incident summaries
# The mailer prefers CEREBRAS_API_KEY_FREE, then CEREBRAS_API_KEY, and can also
# fall back to matching exports in ~/.zshrc when those env vars are absent.
CEREBRAS_API_KEY_FREE="csk-..."
CEREBRAS_API_KEY="csk-..."
MDW_CEREBRAS_MODEL="gpt-oss-120b"
MDW_CEREBRAS_REASONING_EFFORT="low"
MDW_CEREBRAS_VERSION_PATCH="2"
```

Optional fields:

```bash
MDW_ALERT_EMAIL_CC=""
MDW_ALERT_EMAIL_BCC=""
MDW_ALERT_EMAIL_REPLY_TO=""
MDW_ALERT_EMAIL_SUBJECT_PREFIX="[Market Data Warehouse]"
```

The email body includes the run date, attempts, exit code, log path, raw error summary, a Cerebras-generated human-readable incident summary, probable cause, proposed solution, and a recent tail of the daily update log for quick diagnosis.
Watchdog-triggered emails omit attempts and exit code when the sync never reached a terminal runner result.

## Testing

All code in `clients/` and `scripts/` must have tests. Coverage is enforced at **100%** for the source currently included by `pyproject.toml`; `clients/ib_client.py` is still omitted from the fail-under coverage gate and covered by focused tests separately.

```bash
source ~/market-warehouse/.venv/bin/activate

# Run all tests
python -m pytest tests/ -v

# Run with coverage report
python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing

# Run only unit tests (skip DB integration tests)
python -m pytest tests/ -v -m "not integration"

# Keep coroutine leaks from mocked async runners from slipping back in
python -m pytest tests/ -v -W error::RuntimeWarning
```

For native macOS work, also run:

```bash
cd macos
swift test
./scripts/run_ui_smoke_tests.sh
```

### Adding new code

When adding new modules or scripts:

1. Write tests in `tests/test_<module>.py`
2. Add shared fixtures to `tests/conftest.py`
3. Mock external I/O (HTTP, file system) in unit tests
4. Use the `tmp_duckdb` fixture for DB integration tests
5. Run coverage and verify 100% for the configured coverage set before committing
6. Run `-W error::RuntimeWarning` at least once before committing when a script test mocks async runners like `ib.ib.run(...)`
7. `pyproject.toml` enforces `fail_under = 100`; `if __name__ == "__main__"` blocks are excluded

Test dependencies: `pytest`, `pytest-cov`, `responses`

## Pre-commit Hook (Secrets Scanner)

A pre-commit hook scans all staged files for secrets before every commit. Install it after cloning:

```bash
ln -sf ../../scripts/pre-commit-secrets-scan.sh .git/hooks/pre-commit
```

**What it catches:**
- AWS access keys and secret keys
- API key, secret, and password assignments
- Private key headers (`-----BEGIN RSA PRIVATE KEY-----`)
- GitHub tokens (`ghp_`, `gho_`, etc.), Slack tokens, Google API keys
- Database connection strings with embedded credentials
- Hardcoded IB credentials (`IbLoginId`, `IbPassword`)
- Staged `.env` files

**False positive handling:** Allowlists test files with dummy tokens, placeholder values (`YOUR_...`), comments, `os.environ` reads, error messages, and YAML spec examples. Bypass with `git commit --no-verify` if needed.

## Project Setup
This project bootstraps a **local-first financial data warehouse** on Apple Silicon macOS.

It sets up:

* **DuckDB** for local analytics and research
* **ClickHouse** for production-style local benchmarking
* a **partitioned Parquet data lake** for canonical storage
* a Python environment with:

  * `duckdb`
  * `polars`
  * `pandas`
  * `pyarrow`
  * `clickhouse-connect`
  * `numpy`
  * `scipy`
  * `jupyterlab`

## Architecture

The intended workflow is:

* **Raw vendor data** → `data-lake/raw/`
* **Normalized canonical Parquet** → `data-lake/bronze/`
* **Cleaned / adjusted / deduped datasets** → `data-lake/silver/`
* **Derived analytics / factor tables / marts** → `data-lake/gold/`

### Local stack

* **Parquet** is the system of record
* **DuckDB** is the default local query engine
* **ClickHouse** is optional and used for warehouse-style benchmarking and production-like schema testing

## Directory layout

```text
~/market-warehouse/
├── .venv/
├── clickhouse/
├── data-lake/
│   ├── raw/
│   │   ├── asset_class=equity/
│   │   ├── asset_class=option/
│   │   └── asset_class=future/
│   ├── bronze/
│   │   ├── asset_class=equity/
│   │   │   └── symbol=AAPL/data.parquet
│   │   ├── asset_class=option/
│   │   └── asset_class=future/
│   ├── silver/
│   │   ├── asset_class=equity/
│   │   ├── asset_class=option/
│   │   └── asset_class=future/
│   └── gold/
│       ├── asset_class=equity/
│       ├── asset_class=option/
│       └── asset_class=future/
├── duckdb/
│   └── market.duckdb
├── logs/
├── scripts/
│   ├── activate_env.sh
│   ├── bootstrap_clickhouse.sql
│   ├── bootstrap_duckdb.sql
│   ├── init_clickhouse.sh
│   ├── query_parquet_duckdb.sql
│   ├── start_clickhouse.sh
│   ├── stop_clickhouse.sh
│   └── write_sample_parquet.py
└── tmp_duckdb/
```

## Requirements

* macOS
* Apple Silicon recommended
* Homebrew
* internet access for package installation

## Setup

Save the setup script as:

```bash
setup_market_warehouse.sh
```

Make it executable:

```bash
chmod +x setup_market_warehouse.sh
```

## Flags

The setup script supports these flags:

* `--start-clickhouse`
  Starts ClickHouse after setup.

* `--init-clickhouse`
  Initializes the ClickHouse schema after setup. This also implies `--start-clickhouse`.

* `--with-sample-data`
  Generates sample Parquet data under the bronze layer.

* `--smoke-test`
  Runs basic validation checks after setup.

* `--help`
  Prints usage information.

## Common commands

### Minimal install

```bash
./setup_market_warehouse.sh
```

### Install + sample data + validation

```bash
./setup_market_warehouse.sh --with-sample-data --smoke-test
```

### Install + ClickHouse startup + ClickHouse schema

```bash
./setup_market_warehouse.sh --start-clickhouse --init-clickhouse
```

### Full bootstrap

```bash
./setup_market_warehouse.sh --start-clickhouse --init-clickhouse --with-sample-data --smoke-test
```

## What the script does

The script:

1. verifies macOS / Apple Silicon assumptions
2. installs required Homebrew packages
3. installs optional Rust and Node.js tooling if missing
4. creates the warehouse directory structure
5. creates a Python virtual environment
6. installs Python dependencies
7. creates the DuckDB schema
8. writes ClickHouse schema/bootstrap files
9. writes helper scripts for ClickHouse lifecycle management
10. optionally creates sample Parquet data
11. optionally starts and initializes ClickHouse
12. optionally runs smoke tests

## Split-Adjusted Shares

All share volume stored in `md.equities_daily` reflects **split-adjusted totals**, not notional (unadjusted) shares traded on the day.

This means historical volume is retroactively scaled to be consistent with the post-split share count. For example, TSLA executed a **5-for-1 split on August 31, 2020** and a **3-for-1 split on August 25, 2022**. A bar recorded before the 2020 split with a raw volume of 4M shares would appear as 60M shares after both adjustments are applied (4M × 5 × 3).

This normalization ensures that volume comparisons across time are meaningful — you are always comparing shares on the same post-split basis rather than mixing pre- and post-split counts.

## DuckDB schema

The DuckDB bootstrap creates schema `md` with:

* `md.symbols`
* `md.equities_daily`
* `md.futures_daily`
* `md.options_daily`

Notable change:

* the options column is named **`option_right`** instead of `right` to avoid reserved keyword issues

## ClickHouse schema

The ClickHouse bootstrap creates database `md` with:

* `md.equities_daily`
* `md.futures_daily`
* `md.options_daily`

All tables use **MergeTree** and are partitioned by `toYYYYMM(trade_date)`.

## Activating Python later

The setup script creates the virtual environment, but it cannot keep your terminal activated after the script exits.

To activate it in a new shell:

```bash
source ~/market-warehouse/.venv/bin/activate
```

Or:

```bash
~/market-warehouse/scripts/activate_env.sh
```

## Helper scripts

### Start ClickHouse

```bash
~/market-warehouse/scripts/start_clickhouse.sh
```

### Initialize ClickHouse schema

```bash
~/market-warehouse/scripts/init_clickhouse.sh
```

### Stop ClickHouse

```bash
~/market-warehouse/scripts/stop_clickhouse.sh
```

## Sample data

To generate sample Parquet data manually:

```bash
python ~/market-warehouse/scripts/write_sample_parquet.py
```

This writes a small equities dataset into:

```text
~/market-warehouse/data-lake/bronze/asset_class=equity/year=2025/month=01/
```

That sample layout is for bootstrap/demo data. The current IB ingestion scripts write live bronze data as per-ticker snapshots under `symbol=<ticker>/data.parquet`.

## Querying sample Parquet with DuckDB

Run:

```bash
duckdb ~/market-warehouse/duckdb/market.duckdb < ~/market-warehouse/scripts/query_parquet_duckdb.sql
```

The sample query calculates:

* average close
* total volume

grouped by `symbol_id`.

## Smoke tests

When `--smoke-test` is enabled, the script:

* checks `duckdb --version`
* checks `python --version`
* verifies Python imports for core packages
* runs the sample DuckDB query if sample data exists
* checks ClickHouse connectivity if ClickHouse was started

## Troubleshooting

### DuckDB bootstrap appears to stop after “Bootstrapping DuckDB...”

That was previously caused by loading the `httpfs` extension during bootstrap. The current script no longer does that.

### DuckDB parser error near `primary_key`

DuckDB expects:

```sql
symbol_id BIGINT PRIMARY KEY
```

not:

```sql
primary_key (symbol_id)
```

### DuckDB parser error near `right`

`right` is a reserved keyword. The schema now uses:

```sql
option_right
```

### ClickHouse does not start

Try running the helper directly:

```bash
~/market-warehouse/scripts/start_clickhouse.sh
```

Then verify:

```bash
clickhouse-client --query "SELECT version()"
```

## Recommended workflow

For everyday local work:

1. keep canonical data in partitioned Parquet
2. use DuckDB for research, backtests, and local analytics
3. use ClickHouse only when you need:

   * larger-scale benchmarking
   * concurrency
   * production-like testing
   * intraday warehouse experiments

## Recommended command

```bash
./setup_market_warehouse.sh --start-clickhouse --init-clickhouse --with-sample-data --smoke-test
```
