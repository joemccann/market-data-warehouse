# Observability-Driven Refactor Blueprint

## Objective

Refactor the local-first ingestion stack so the runtime is measurable, recoverable, and fast on a single machine. The target path is:

`IBKR -> bounded work unit -> staged parquet -> validation -> atomic bronze publish -> DuckDB load -> optional future ClickHouse publish`

The main change is architectural: bronze becomes a first-class publish target, not a full-table export from DuckDB after every run.

## Current Pressure Points

- The codebase has already improved two failure modes: `fetch_ib_historical.py` falls back to the earliest supported IB historical date when head timestamps are empty, and `IBClient.connect()` retries successive `clientId` values on IB error `326`.
- `scripts/fetch_ib_historical.py` and `scripts/daily_update.py` now publish directly into per-ticker bronze parquet, which removed the live DuckDB lock from the hot path.
- `scripts/daily_update.py` now also has a narrow public fallback chain for unresolved target-day gaps in the U.S. equity universe (`nasdaq:stocks` -> `nasdaq:etf` -> `stooq:us`). That improves recoverability for symbol-level IB failures, but it introduces source-order policy and per-provider observability needs.
- `clients/bronze_client.py` still rewrites a full per-ticker snapshot for each affected symbol, so merge cost scales with ticker history rather than with the incremental bar count.
- `clients/db_client.py` is now an offline analytical-file client and rebuild target, not the service system of record.
- `scripts/fetch_ib_historical.py` now falls back to `IB_EARLIEST_DATE` when IB returns no head timestamp. That keeps more symbols fetchable, but it also makes request volume and empty-window behavior more important to observe.
- Cursor state only records completed tickers. It does not track run ownership, window-level progress, lease expiry, or per-mode conflicts, so overlapping backfills are possible.
- Preset loading is duplicated in ingestion scripts. Universe metadata exists in JSON already, but is not treated as a domain model.
- Bronze publication now rewrites one canonical snapshot per ticker under `symbol=<ticker>/data.parquet` and uses a file-level atomic publish. That removed DuckDB lock contention, but it still rewrites full ticker history on each update and does not yet use staged manifests or lease-backed orchestration.
- `clients/ib_client.py` now retries successive `clientId` values on IB error `326`, but that path is not yet instrumented with retry counts, actual connected ID, or launchd-friendly diagnostics.

## Design Principles

- Treat staged Parquet as the interchange boundary between fetch, validation, and storage.
- Keep queues bounded so the machine never accumulates more bars than it can publish quickly.
- Count everything that matters before optimizing anything else.
- Move dedup and merge work into DuckDB SQL, not Python loops.
- Separate run state from warehouse state.
- Keep asset-specific logic behind adapters so equities, options, and futures can share orchestration.

## Target Package Layout

```text
market_warehouse/
├── observability/
│   ├── metrics.py
│   ├── events.py
│   └── logging.py
├── state/
│   ├── models.py
│   ├── sqlite_store.py
│   └── lease.py
├── universes/
│   ├── models.py
│   ├── repository.py
│   ├── resolver.py
│   └── manifest.py
├── assets/
│   ├── base.py
│   ├── equities.py
│   └── options.py
├── ingest/
│   ├── rate_limit.py
│   ├── tasks.py
│   ├── fetcher.py
│   ├── spool.py
│   ├── validate.py
│   ├── pipeline.py
│   └── coordinator.py
├── storage/
│   ├── duckdb_loader.py
│   ├── bronze_publisher.py
│   └── schema.py
└── publishers/
    └── clickhouse.py
```

## 1. Instrumentation Layer

### What to measure

- `rows_fetched`
- `rows_published`
- `elapsed_seconds`
- `rows_per_second`
- `requests_sent`
- `connect_retries`
- `connected_client_id`
- `api_errors_total`
- `pacing_violations_total`
- `bytes_written`
- `validation_failures`
- `queue_depth`
- `run_id`, `asset_class`, `universe`, `symbol`, `window_start`, `window_end`

### Lightweight wrapper

`market_warehouse/observability/metrics.py`

```python
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import logging
import time
from typing import Iterable


PACE_CODES = {162, 420}


@dataclass
class FetchCounters:
    run_id: str
    asset_class: str
    universe: str
    requests_sent: int = 0
    rows_fetched: int = 0
    pacing_violations_total: int = 0
    api_errors_total: int = 0
    started_at: float = field(default_factory=time.perf_counter)

    def mark_error(self, code: int | None, message: str) -> None:
        self.api_errors_total += 1
        msg = message.lower()
        if (code in PACE_CODES) or ("pacing" in msg):
            self.pacing_violations_total += 1

    def rows_per_second(self) -> float:
        elapsed = max(time.perf_counter() - self.started_at, 0.001)
        return self.rows_fetched / elapsed


class ObservedIBFetcher:
    def __init__(self, ib_client, logger: logging.Logger, counters: FetchCounters):
        self.ib_client = ib_client
        self.logger = logger
        self.counters = counters
        self.ib_client.ib.errorEvent += self._on_ib_error

    def close(self) -> None:
        self.ib_client.ib.errorEvent -= self._on_ib_error

    def _on_ib_error(self, req_id, error_code, error_string, contract=None) -> None:
        code = int(error_code) if error_code else None
        self.counters.mark_error(code, error_string or "")

    async def historical(self, contract, **kwargs) -> list:
        t0 = time.perf_counter()
        bars = await self.ib_client.get_historical_data_async(contract, **kwargs)
        rows = len(bars or [])
        self.counters.requests_sent += 1
        self.counters.rows_fetched += rows
        elapsed = max(time.perf_counter() - t0, 0.001)

        self.logger.info(
            json.dumps(
                {
                    **asdict(self.counters),
                    "symbol": getattr(contract, "symbol", None),
                    "rows": rows,
                    "request_elapsed_s": round(elapsed, 3),
                    "request_rows_per_second": round(rows / elapsed, 2),
                    "cumulative_rows_per_second": round(self.counters.rows_per_second(), 2),
                }
            )
        )
        return bars or []
```

### Why this is enough

- It is drop-in around the current async fetch path.
- It logs both per-request throughput and cumulative throughput.
- It counts pacing issues from the same IB event channel the client already uses.
- It gives you a baseline before touching concurrency or loader code.

## 2. Atomic Write Pattern

### Target flow

1. Resolve `UniverseSlice` into fetch tasks.
2. Fetch one ticker window at a time.
3. Normalize directly into a small Arrow table.
4. Write a stage file under `data-lake/_stage/run_id=.../asset_class=.../`.
5. Validate the stage file with DuckDB SQL and file-level checks.
6. Atomically move the validated file into bronze.
7. Load DuckDB from the same validated stage file.
8. Mark the work item committed in the state store.

### File layout

```text
~/market-warehouse/data-lake/
├── _stage/
│   └── run_id=20260309T210500Z/
│       └── asset_class=equity/
│           └── universe=sp500/
│               └── symbol=AAPL/
│                   └── window=20150101_20250101.parquet
└── bronze/
    └── asset_class=equity/
        └── source=ibkr/
            └── universe=sp500/
                └── symbol=AAPL/
                    └── trade_year=2025/
                        └── part-20260309T210500Z-0001.parquet
```

### Validation gates

`market_warehouse/ingest/validate.py`

- Schema validation: required columns, types, nullability.
- Domain validation: positive prices, `high >= low`, trading-day checks, duplicate dates.
- File validation: row count > 0, min/max date within expected window.
- Optional publish manifest: `sha256`, row count, min/max trade date, symbol count, source metadata.

### Atomic publish rule

- Write stage and bronze files on the same filesystem.
- Use `os.replace()` for the final move.
- Only move validated files.
- Never publish directly from Python lists into bronze.

### Memory-pressure benefit

- Each worker only holds one bounded batch in memory.
- Rows leave Python quickly and land in a file boundary.
- DuckDB and any future ClickHouse publisher both consume the same stage artifact instead of duplicating row transforms.

## 3. State-File Refactor

### Recommendation

Use SQLite, not JSON, as the authoritative state store. JSON is still fine for operator-facing snapshots, but it is too weak for leases and overlapping-run protection.

### Why SQLite wins on a single node

- Safe multi-process coordination under accidental duplicate launchd starts.
- Unique constraints prevent duplicate work claims.
- Window-level progress survives crashes cleanly.
- Queryable history is useful for observability and replay.

### Proposed schema

`market_warehouse/state/sqlite_store.py`

```sql
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL,
    asset_class TEXT NOT NULL,
    universe TEXT NOT NULL,
    mode TEXT NOT NULL,
    host TEXT NOT NULL,
    pid INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS work_items (
    asset_class TEXT NOT NULL,
    universe TEXT NOT NULL,
    symbol TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    mode TEXT NOT NULL,
    stage_path TEXT,
    bronze_path TEXT,
    status TEXT NOT NULL,
    lease_run_id TEXT,
    lease_expires_at TEXT,
    row_count INTEGER DEFAULT 0,
    PRIMARY KEY (asset_class, universe, symbol, window_start, window_end, mode)
);

CREATE INDEX IF NOT EXISTS idx_work_items_status
ON work_items (status, lease_expires_at);
```

### Lease rules

- A run must heartbeat every `N` seconds.
- A work item can only be claimed if its lease is empty or expired.
- Backfill and normal fetch use distinct `mode` values, so they never trample each other.
- Commit means bronze published and DuckDB loaded. Anything else remains replayable.

## 4. Thin DuckDB Client

### What the thin client should own

- Connection lifecycle.
- Schema bootstrap.
- Staging-file reads.
- Set-based merge SQL.
- Small metadata queries for latest and oldest dates.

### What it should not own

- Fetch retries.
- Universe selection.
- Validation rules.
- Cursor logic.
- File path decisions.
- Per-row Python transforms after parquet exists.

### Proposed split

- `clients/db_client.py` becomes a compatibility shell or is replaced by `market_warehouse/storage/duckdb_loader.py`.
- Loader API accepts paths to validated parquet, not Python row dict lists.

### Loader API

```python
class DuckDBLoader:
    def register_symbols_from_parquet(self, stage_path: str) -> int: ...
    def merge_equities_daily_from_parquet(self, stage_path: str) -> int: ...
    def latest_trade_dates(self, asset_class: str, symbols: list[str] | None = None) -> dict[str, str]: ...
    def oldest_trade_dates(self, asset_class: str, symbols: list[str] | None = None) -> dict[str, str]: ...
```

### Set-based load pattern

```sql
CREATE TEMP TABLE stage_equity AS
SELECT *
FROM read_parquet($stage_path);

INSERT INTO md.symbols (symbol_id, symbol, asset_class, venue)
SELECT DISTINCT
    abs(hash(symbol)) % CAST(pow(2, 53) AS BIGINT) AS symbol_id,
    symbol,
    asset_class,
    venue
FROM stage_equity s
WHERE NOT EXISTS (
    SELECT 1 FROM md.symbols d WHERE d.symbol = s.symbol
);

INSERT INTO md.equities_daily
    (trade_date, symbol_id, open, high, low, close, adj_close, volume)
SELECT
    s.trade_date,
    m.symbol_id,
    s.open,
    s.high,
    s.low,
    s.close,
    s.adj_close,
    s.volume
FROM stage_equity s
JOIN md.symbols m
  ON m.symbol = s.symbol
WHERE NOT EXISTS (
    SELECT 1
    FROM md.equities_daily d
    WHERE d.trade_date = s.trade_date
      AND d.symbol_id = m.symbol_id
);
```

### Why this is faster

- DuckDB reads Parquet directly.
- Dedup is set-based.
- Python no longer loops row-by-row.
- The same staged file can be replayed safely after a crash.

## 5. Throttled Producer-Consumer Concurrency

### Pattern

- `Producer`: resolves a universe into ticker-window tasks and inserts them into the state store.
- `Fetch workers`: acquire request permits, fetch from IBKR, write stage parquet immediately.
- `Validator/publisher worker`: validates, moves to bronze, loads DuckDB, marks committed.

### Queue topology

```text
UniverseResolver -> task_q(maxsize=256)
task_q -> N fetch workers -> stage_q(maxsize=32)
stage_q -> 1..2 publish workers -> commit
```

### Controls

- Token bucket for IB messages, target `<= 45 msg/sec` instead of riding the `50` hard ceiling.
- Separate semaphore for concurrent historical requests, start at `4` and tune with telemetry.
- Bounded queues so fetchers slow down automatically when disk or DuckDB becomes the bottleneck.
- Adaptive backoff when pacing violations rise above a threshold in a rolling window.

### Module ownership

- `market_warehouse/ingest/rate_limit.py`: token bucket, concurrency semaphore, adaptive slowdown.
- `market_warehouse/ingest/tasks.py`: task model with symbol, window, asset class, universe, mode.
- `market_warehouse/ingest/fetcher.py`: uses `ObservedIBFetcher` and asset adapters.
- `market_warehouse/ingest/spool.py`: writes Arrow tables to stage parquet.
- `market_warehouse/ingest/coordinator.py`: queue wiring, worker startup, shutdown, heartbeats.

## 6. Decoupled Ticker Universe Model

### Problem in the current scripts

- `load_preset()` exists in multiple scripts.
- Ingestion logic knows where preset files live and what keys to read.
- Universe metadata like `groups`, `pairs`, `description`, and `source` is ignored operationally.

### Proposed model

`market_warehouse/universes/models.py`

```python
from dataclasses import dataclass, field


@dataclass(frozen=True)
class UniverseManifest:
    name: str
    asset_class: str
    source: str
    symbols: tuple[str, ...]
    description: str | None = None
    groups: tuple[str, ...] = ()
    pairs: tuple[tuple[str, str], ...] = ()
    metadata: dict[str, str] = field(default_factory=dict)
```

### Repository design

- `repository.py`: loads manifests from `presets/` and validates schema.
- `resolver.py`: returns a `UniverseSlice` for a requested asset class, preset, group, or symbol subset.
- `manifest.py`: serializes a normalized manifest shape so new asset classes do not invent new JSON layouts.

### JSON direction

Keep backward compatibility with current preset files, but normalize them internally into:

```json
{
  "name": "sp500",
  "asset_class": "equity",
  "source": "ibkr",
  "symbols": ["AAPL", "MSFT"],
  "groups": ["sector-information-technology"],
  "pairs": [["AAPL", "MSFT"]]
}
```

### Why this matters for multi-asset

- Equities can resolve by ticker.
- Options can resolve by root symbol plus chain filter metadata.
- Futures can resolve by root plus roll convention.
- The coordinator only sees `UniverseSlice` and `AssetAdapter`, not hardcoded equity assumptions.

## 7. Asset Adapters

### Base interface

`market_warehouse/assets/base.py`

```python
class AssetAdapter:
    asset_class: str

    def build_contract(self, work_item): ...
    def normalize_bars(self, bars, work_item): ...
    def validate_table(self, stage_path: str): ...
    def bronze_partition_keys(self, table): ...
    def duckdb_target_table(self) -> str: ...
```

### Implementations

- `assets/equities.py`: current daily OHLCV flow.
- `assets/options.py`: contract expansion and option-specific schema later.

This avoids baking equity-only fields into the queueing, state, or loader layers.

## 8. ClickHouse-Compatible Decisions Now

- Keep bronze parquet schema explicit and typed; do not make DuckDB the schema authority.
- Include natural keys like `symbol`, `asset_class`, `venue`, `source`, and `trade_date` in parquet. Let DuckDB and ClickHouse derive their own surrogate IDs if needed.
- Store publish manifests alongside parquet so a future ClickHouse publisher can replay from bronze without querying DuckDB.
- Keep ingestion state in SQLite, not DuckDB, so warehouse engine changes do not affect orchestration.
- Make `publishers/clickhouse.py` consume the same validated stage or bronze artifacts the DuckDB loader sees.

## 9. Implementation Order

1. Add `ObservedIBFetcher` and structured logs without changing the data model.
2. Introduce SQLite state store with leases and heartbeat.
3. Write stage parquet directly from fetch workers.
4. Validate and atomically publish to bronze.
5. Replace row-loop inserts with `DuckDBLoader.merge_*_from_parquet`.
6. Extract universes into `market_warehouse/universes/`.
7. Move equity-specific logic into `assets/equities.py`.
8. Add `publishers/clickhouse.py` only after bronze contracts stabilize.

## 10. First Practical Milestone

If you want the smallest high-value first cut:

- Add the instrumentation wrapper.
- Replace cursor JSON with SQLite leases.
- Write per-ticker stage parquet.
- Load DuckDB from staged parquet with `INSERT INTO ... SELECT FROM read_parquet(...)`.

That gives you better throughput visibility, crash recovery, and lower Python overhead before you attempt a broader package split.
