# Market Data Warehouse

Local-first financial data warehouse for quantitative research. Parquet data lake as system of record, DuckDB for analytics, ClickHouse for production benchmarking.

## Project Layout

Two directory trees: this **git repo** and the **data warehouse** at `~/market-warehouse/`.

```
market-data-warehouse/              # Git repo
├── clients/
│   ├── __init__.py                 # Exports BronzeClient, DailyBarFallbackClient, IBClient, DBClient
│   ├── bronze_client.py            # Canonical per-ticker bronze parquet client
│   ├── daily_bar_fallback.py       # Public daily-bar fallback chain for U.S. equities/ETFs
│   ├── ib_client.py                # Interactive Brokers API client (ib_insync)
│   ├── uw_client.py                # Unusual Whales REST API client (kept, not used for historical)
│   └── db_client.py                # DuckDB client for md.* schema
├── scripts/
│   ├── setup_market_warehouse.sh   # One-time system bootstrap
│   ├── fetch_ib_historical.py      # Bulk historical OHLCV ingestion from IB (supports --backfill)
│   ├── run_backfill_all.sh         # Auto-restarting runner for all presets
│   ├── daily_update.py             # Daily parquet-first incremental update
│   ├── run_daily_update_job.py     # Retrying scheduled daily-update runner
│   ├── check_daily_update_watchdog.py # Watchdog for missed/incomplete daily syncs
│   ├── rebuild_duckdb_from_parquet.py # Offline DuckDB rebuild from bronze parquet
│   ├── run_daily_update.sh         # Shell wrapper for launchd/cron
│   ├── run_daily_update_watchdog.sh # Shell wrapper for the daily-update watchdog
│   ├── cerebras_client.mjs         # Cerebras incident-summary client for failure alerts
│   ├── send_daily_update_failure_email.mjs # Nodemailer failure alert CLI
│   ├── com.market-warehouse.daily-update.plist.example  # macOS launchd template
│   ├── com.market-warehouse.daily-update-watchdog.plist.example # macOS launchd watchdog template
│   └── pre-commit-secrets-scan.sh  # Pre-commit hook: secrets scanner
├── strategies/                     # Legacy — extracted to standalone doob package
│   ├── breadth_washout.py          # (migrated to doob.strategies.breadth_washout)
│   ├── ndx100_breadth_washout.py   # (migrated to doob.strategies.ndx100_breadth_washout)
│   ├── ndx100_sma_breadth.py       # (migrated to doob.strategies.ndx100_sma_breadth)
│   ├── overnight_drift.py          # (migrated to doob.strategies.overnight_drift)
│   └── intraday_drift.py           # (migrated to doob.strategies.intraday_drift)
├── tests/
│   ├── conftest.py                 # Shared fixtures: tmp_duckdb, db
│   ├── test_daily_bar_fallback.py  # Unit tests for fallback providers
│   ├── test_uw_client.py           # Unit tests — HTTP mocked via `responses`
│   ├── test_db_client.py           # Integration tests — temp DuckDB per test
│   ├── test_fetch_ib_historical.py # Tests for IB fetch script
│   ├── test_daily_update.py        # Tests for daily update script
│   └── test_ib_client.py           # Focused tests for IB client connect fallback
├── macos/                          # Legacy — extracted to standalone Sift app at ~/dev/apps/util/sift
├── pyproject.toml                  # pytest config, coverage enforcement
├── .env.example
└── README.md

~/market-warehouse/                 # Data warehouse (created by setup script)
├── .venv/                          # Python 3.12 venv
├── data-lake/
│   ├── bronze/asset_class=equity/  # Per-ticker Hive-partitioned Parquet (symbol=AAPL/data.parquet)
│   ├── bronze-delisted/asset_class=equity/  # Archived delisted symbols excluded from future sync/backfill runs
│   ├── silver/                     # Cleaned / adjusted
│   └── gold/                       # Derived analytics / factor tables
├── duckdb/market.duckdb            # Analytical DB
├── clickhouse/                     # Optional ClickHouse data
├── scripts/                        # Bootstrap SQL, helper scripts
└── logs/
```

## Architecture

- **Parquet** is the system of record, not DuckDB
- **Data lake tiers**: bronze (normalized Parquet) -> silver (cleaned) -> gold (derived)
- **DuckDB** is the local query engine for research and backtesting
- **ClickHouse** is optional, for production-style benchmarking and concurrency testing
- **Python env** lives at `~/market-warehouse/.venv/` — activate with `source ~/market-warehouse/.venv/bin/activate`

## Strategies (Extracted)

All backtesting and strategy code has been extracted to the standalone **doob** package at `~/dev/apps/finance/doob`. This includes:
- Breadth washout (oversold/overbought signal modes, universe selection)
- NDX-100 SMA breadth analysis
- Overnight drift (buy close, sell next open)
- Intraday drift (buy open, sell close)
- Shared metrics (CAGR, Sharpe, max drawdown, VaR) and IBKR fee model

The `strategies/` directory in this repo is kept for reference but is no longer the canonical source. Use `python -m doob run <strategy>` from the doob package instead.

See the [doob CLAUDE.md](~/dev/apps/finance/doob/CLAUDE.md) for strategy catalog and usage.

## Native macOS Client (Extracted)

The native macOS client has been extracted to the standalone **Sift** app at `~/dev/apps/util/sift/`. The `macos/` directory in this repo is kept for reference but is no longer the canonical source.

See the [Sift CLAUDE.md](~/dev/apps/util/sift/CLAUDE.md) for module layout, build instructions, and testing.

## DuckDB Schema

Schema `md` with four tables:

- `md.symbols` — `symbol_id BIGINT PK`, `symbol`, `asset_class`, `venue`
- `md.equities_daily` — `trade_date DATE`, `symbol_id BIGINT`, OHLCV + `adj_close`; unique index on `(trade_date, symbol_id)` for dedup
- `md.futures_daily` — trade_date, contract_id, root_symbol, expiry, OHLCV + settlement + OI
- `md.options_daily` — trade_date, contract_id, underlier_id, expiry, strike, `option_right` (not `right` — reserved keyword), OHLCV + OI + implied_vol

ClickHouse mirrors the same schema with MergeTree engines partitioned by `toYYYYMM(trade_date)`.

## IB Gateway / IBC

IB Gateway is managed by **IBC** (IB Controller) for automated login, reconnection, and daily restarts.

For this repo, IBC is a required machine-local dependency for IB-backed workflows. The secure service is installed globally under the user's home directory and is not scoped to this repo.

- **IBC install**: `~/ibc-install/` (IBC.jar + scripts)
- **IBC secure config**: `~/ibc/config.secure.ini` (settings only; no credentials)
- **IBC secure service installer**: `python scripts/install_ibc_secure_service.py`
- **IBC machine-local wrappers**: `~/ibc/bin/start-secure-ibc-service.sh`, `stop-secure-ibc-service.sh`, `restart-secure-ibc-service.sh`, `status-secure-ibc-service.sh`
- **IBC LaunchAgent**: `~/Library/LaunchAgents/local.ibc-gateway.plist`
- **IBC logs**: `~/ibc/logs/ibc-gateway-service.log` for the secure LaunchAgent, or `~/ibc/logs/` for the stock wrapper
- **Start Gateway**: installed machine-local secure service (preferred and project-required for IB workflows), repo Keychain launcher for low-level troubleshooting, or `~/ibc-install/gatewaystartmacos.sh` for legacy plaintext config
- **Stop Gateway**: `~/ibc-install/stop.sh`
- **Reconnect data**: `~/ibc-install/reconnectdata.sh`
- **Command server**: port 7462
- **Gateway API port**: 4001
- **Auto-restart**: 11:58 PM daily, cold restart Sundays 07:05

## Data Ingestion

Data source: **Interactive Brokers** via `ib_insync`. Requires IB Gateway running on localhost via the global machine-local IBC service.

- `IBClient` wraps `ib_insync.IB` with connection management, historical data, and contract qualification
- `IBClient.connect()` defaults to `clientId=0` and automatically retries successive `clientId` values if IB reports error `326` (`client id already in use`)
- `IBClient.get_historical_data()` fetches daily bars via `reqHistoricalData`
- `BronzeClient` is the live service storage client: it discovers symbols from parquet, merges or replaces per-ticker snapshots, and publishes with `temp -> validate -> os.replace()`
- `DailyBarFallbackClient` is a narrow recovery client for unresolved target-day gaps in the current U.S. equity universe. Provider order: Nasdaq `assetclass=stocks`, Nasdaq `assetclass=etf`, then Stooq U.S. daily CSV.
- `DBClient` is now the offline analytical-file client: it can still manage/query `md.*`, and it rebuilds DuckDB from bronze parquet with set-based `INSERT INTO ... SELECT`
- `adj_close` is set to `close` (IB TRADES data doesn't provide adjusted prices)

### IB BarData → Bronze mapping

| IB BarData field | Bronze column | Transform |
|---|---|---|
| `bar.date` | `trade_date` | `str(bar.date)` |
| (from ticker) | `symbol_id` | Read existing parquet ID or derive stable ID |
| `bar.open` | `open` | Already float |
| `bar.high` | `high` | Already float |
| `bar.low` | `low` | Already float |
| `bar.close` | `close` | Already float |
| `bar.close` | `adj_close` | Same value |
| `bar.volume` | `volume` | `int(bar.volume)` |

### Running the pipeline

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/fetch_ib_historical.py                                  # Mag 7 default
python scripts/fetch_ib_historical.py --tickers AAPL NVDA              # Custom tickers
python scripts/fetch_ib_historical.py --preset presets/sp500.json      # From preset with cursor resume
python scripts/fetch_ib_historical.py --years 0 --skip-existing        # Inception, skip existing
python scripts/fetch_ib_historical.py --preset presets/sp500.json --backfill  # Backfill older data
```

Current fetch behavior:
- Normal mode atomically replaces the per-ticker bronze snapshot
- Backfill mode merges older bars into the same per-ticker bronze snapshot
- The live service path does not open `market.duckdb`
- If IB returns an empty head timestamp, the fetcher falls back to `IB_EARLIEST_DATE` instead of skipping the symbol

### Backfill mode

`--backfill` fetches only missing older data for tickers already in bronze parquet:
- Queries each ticker's oldest existing `trade_date` from parquet
- Fetches IB inception → oldest_date gap
- Merges older rows into the canonical parquet snapshot
- Uses separate cursor JSON: `cursor_backfill_{name}.json`
- Skips tickers not in bronze parquet (use normal fetch first)

### Auto-restarting runner

```bash
bash scripts/run_backfill_all.sh   # Runs all presets with stall detection + auto-restart
```

Output: per-ticker bronze Parquet at `data-lake/bronze/asset_class=equity/symbol=<ticker>/data.parquet`. DuckDB is rebuilt separately when needed.

Delisted symbols that should no longer participate in future syncs or backfills should be archived outside the canonical sync path under `data-lake/bronze-delisted/asset_class=equity/symbol=<ticker>/data.parquet`.

### Daily updates

`daily_update.py` is a lightweight script for daily scheduled runs (~2,500 tickers). It discovers tickers from bronze parquet, detects gaps vs the latest trading day, fetches only missing bars, validates OHLCV integrity, and atomically rewrites only the affected per-ticker snapshots. If IB leaves unresolved target trading days after validation, the script can recover those dates from the fallback chain before publishing parquet.

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/daily_update.py                                  # Normal daily run
python scripts/daily_update.py --dry-run                        # Report gaps without fetching
python scripts/daily_update.py --force                          # Run on non-trading day
python scripts/daily_update.py --target-date 2026-03-11        # Recover through a fixed trading date
python scripts/daily_update.py --preset presets/sp500.json      # Limit to preset tickers
python scripts/daily_update.py --port 7497 --max-concurrent 4   # Custom IB config
python scripts/daily_update.py --batch-size 25                  # Custom batch size
```

**Scheduling with launchd** (macOS):
```bash
# Copy examples, replace /path/to/repo with your actual repo path
sed "s|/path/to/repo|$(pwd)|g" scripts/com.market-warehouse.daily-update.plist.example > ~/Library/LaunchAgents/com.market-warehouse.daily-update.plist
sed "s|/path/to/repo|$(pwd)|g" scripts/com.market-warehouse.daily-update-watchdog.plist.example > ~/Library/LaunchAgents/com.market-warehouse.daily-update-watchdog.plist
launchctl load ~/Library/LaunchAgents/com.market-warehouse.daily-update.plist
launchctl load ~/Library/LaunchAgents/com.market-warehouse.daily-update-watchdog.plist
```
`scripts/run_daily_update.sh` now loads `.env` files, activates the warehouse venv, and runs `scripts/run_daily_update_job.py`, which retries failed sync attempts before terminal failure.

The main sync runs at 13:05 Pacific local time daily (4:05 PM Eastern year-round). The watchdog runs at 18:30 Pacific by default and alerts if the scheduled sync never started or never logged a completion marker. Non-trading days are harmless no-ops.

**Key design:**
- Discovers tickers from parquet via `BronzeClient.get_latest_dates()` — no hardcoded lists
- `--target-date YYYY-MM-DD` lets operators run a fixed-date catch-up and prevents bars later than the requested target from being published
- Live service writes avoid DuckDB file-lock contention
- Bar validation: checks OHLCV relationships, positive prices, valid trading days, duplicate dates
- Atomically rewrites a per-ticker bronze snapshot after each successful merge
- The active sync universe is the canonical bronze tree only; archive delisted symbols outside that tree if they should stop participating in future syncs/backfills
- Recovery path for unresolved target-day gaps: Nasdaq historical quote API (`stocks`, then `etf`) and then Stooq `symbol.us`
- Fallback bars use the same validation and bronze merge path as IB bars
- Run summary exposes `Fallback attempts`, `Fallback successes`, and `Fallback symbols`
- Pure-Python NYSE trading calendar — no new dependencies
- Logs to `~/market-warehouse/logs/daily_update_YYYY-MM-DD.log`
- Terminal scheduled failures use the Nodemailer CLI at `scripts/send_daily_update_failure_email.mjs`
- Failure alerts can write a sibling `.human.md` incident report and optionally enrich the email body with a Cerebras-generated summary plus proposed remediation
- Failure emails can include Cerebras-generated human-readable incident summaries and write a sibling `*.human.md` incident report beside the raw log

### Rebuilding DuckDB

```bash
source ~/market-warehouse/.venv/bin/activate
python scripts/rebuild_duckdb_from_parquet.py
```

This repopulates `~/market-warehouse/duckdb/market.duckdb` from the canonical bronze parquet tree when you want a fresh analytical DB file. The rebuild path recreates the analytical tables from scratch on each run, so rerunning it against an existing DuckDB file is safe.

### Querying

```bash
duckdb ~/market-warehouse/duckdb/market.duckdb \
  "SELECT s.symbol, count(*) FROM md.symbols s JOIN md.equities_daily e ON s.symbol_id = e.symbol_id GROUP BY s.symbol"
```

## Testing

**All new code in `clients/` and `scripts/` must have tests. Coverage is enforced at 100% for the source currently included by `pyproject.toml`; `clients/ib_client.py` is still omitted from the fail-under gate and covered by focused tests separately.**

```bash
source ~/market-warehouse/.venv/bin/activate
python -m pytest tests/ -v                                                        # Run all
python -m pytest tests/ -v --cov=clients --cov=scripts --cov-report=term-missing  # With coverage
python -m pytest tests/ -v -m "not integration"                                   # Unit tests only
python -m pytest tests/ -v -W error::RuntimeWarning                               # Catch leaked coroutine warnings
# Native macOS tests are now in the standalone Sift repo at ~/dev/apps/util/sift
```

### Rules for new code

1. Add tests in `tests/test_<module>.py`
2. Mock all external I/O (IB connections via `MagicMock`, file paths via `patch`)
3. Use `tmp_duckdb` / `db` fixtures from `conftest.py` for DB tests
4. Mark DB tests with `@pytest.mark.integration`
5. Run coverage and confirm 100% before committing
6. Run `-W error::RuntimeWarning` at least once before committing when script tests mock async runners such as `ib.ib.run(...)`
7. `pyproject.toml` enforces `fail_under = 100`; `if __name__ == "__main__"` blocks are excluded
8. `clients/ib_client.py` is excluded from the coverage fail-under gate, but focused behavior tests now live in `tests/test_ib_client.py`

### Test deps

`pytest`, `pytest-cov`, `responses` (installed in `~/market-warehouse/.venv/`)

## Pre-commit Hook

A secrets scanner runs on every commit, checking staged files for API keys, passwords, private keys, tokens, and credentials. Install with:

```bash
ln -sf ../../scripts/pre-commit-secrets-scan.sh .git/hooks/pre-commit
```

Catches: AWS keys, API key/secret/password assignments, private key headers, GitHub/Slack tokens, Google API keys, connection strings with credentials, hardcoded IB credentials, staged `.env` files. Allowlists test files, placeholders, comments, `os.environ` reads, and error messages to avoid false positives. Bypass with `git commit --no-verify` if needed.

## Key Implementation Details

- IB BarData provides native float/int types — no string parsing needed
- `symbol_id` is now a stable 53-bit hash from `blake2b(symbol)` for new symbols
- Live ingestion writes bronze parquet directly; DuckDB is rebuilt from bronze when needed
- Empty IB head timestamps now fall back to the earliest supported IB historical date instead of skipping the symbol
- Bronze Parquet uses per-ticker Hive-partitioned layout: `data-lake/bronze/asset_class=equity/symbol=AAPL/data.parquet`
- Bronze publication is atomic at the file level: write temp parquet, validate it, then `os.replace()` into place
- `IBClient.connect()` auto-retries successive `clientId` values after IB error `326`, then records the actual connected ID
