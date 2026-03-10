"""Parquet bronze client for per-ticker equity snapshots."""

from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Optional

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from clients.symbol_ids import stable_symbol_id

log = logging.getLogger(__name__)

_DEFAULT_BRONZE_DIR = (
    Path.home() / "market-warehouse" / "data-lake" / "bronze" / "asset_class=equity"
)

_BASE_COLUMNS = (
    "trade_date",
    "symbol_id",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
)

_PARQUET_SCHEMA = pa.schema(
    [
        ("trade_date", pa.date32()),
        ("symbol_id", pa.int64()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("adj_close", pa.float64()),
        ("volume", pa.int64()),
    ]
)


class BronzeClient:
    """Manage canonical per-ticker bronze parquet snapshots."""

    def __init__(self, bronze_dir: Optional[str | Path] = None):
        self._bronze_dir = Path(bronze_dir or _DEFAULT_BRONZE_DIR)
        self._conn = duckdb.connect(":memory:")

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "BronzeClient":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    @property
    def bronze_dir(self) -> Path:
        return self._bronze_dir

    def get_existing_symbols(self) -> set[str]:
        """Return symbols that currently have canonical bronze parquet snapshots."""
        if not self._bronze_dir.exists():
            return set()

        symbols: set[str] = set()
        for path in self._bronze_dir.glob("symbol=*/data.parquet"):
            partition = path.parent.name
            if partition.startswith("symbol="):
                symbols.add(partition.split("=", 1)[1])
        return symbols

    def get_latest_dates(self) -> dict[str, str]:
        """Return ``{symbol: latest_trade_date}`` from the bronze layer."""
        return {
            row["symbol"]: row["latest"]
            for row in self._query_symbol_aggregates("MAX(trade_date)", "latest")
        }

    def get_oldest_dates(self) -> dict[str, str]:
        """Return ``{symbol: oldest_trade_date}`` from the bronze layer."""
        return {
            row["symbol"]: row["oldest"]
            for row in self._query_symbol_aggregates("MIN(trade_date)", "oldest")
        }

    def get_summary(self) -> list[dict[str, Any]]:
        """Return row counts and date coverage for each symbol in bronze."""
        if not self.get_existing_symbols():
            return []

        sql = f"""
            SELECT
                symbol,
                count(*) AS rows,
                CAST(min(trade_date) AS VARCHAR) AS earliest,
                CAST(max(trade_date) AS VARCHAR) AS latest
            FROM read_parquet('{self._escaped_glob()}', hive_partitioning=true)
            GROUP BY symbol
            ORDER BY symbol
        """
        return self._query(sql)

    def get_symbol_id(self, symbol: str) -> int:
        """Return an existing symbol_id from bronze, or derive a stable one."""
        path = self._symbol_path(symbol)
        if not path.exists():
            return stable_symbol_id(symbol)

        table = pq.read_table(path, columns=["symbol_id"])
        if table.num_rows == 0:
            return stable_symbol_id(symbol)
        return int(table.column("symbol_id")[0].as_py())

    def read_symbol_rows(self, symbol: str) -> list[dict[str, Any]]:
        """Read the canonical base columns for a single symbol snapshot."""
        path = self._symbol_path(symbol)
        if not path.exists():
            return []

        table = pq.read_table(path, columns=list(_BASE_COLUMNS))
        rows = table.to_pylist()
        for row in rows:
            trade_date = row["trade_date"]
            if isinstance(trade_date, date):
                row["trade_date"] = trade_date.isoformat()
        return rows

    def replace_ticker_rows(self, symbol: str, rows: list[dict[str, Any]]) -> int:
        """Atomically replace a symbol snapshot with *rows*."""
        normalized = self._normalize_rows(rows, symbol)
        if not normalized:
            raise ValueError(f"{symbol}: cannot publish an empty parquet snapshot")

        self._publish_symbol_rows(symbol, normalized)
        return len(normalized)

    def merge_ticker_rows(self, symbol: str, rows: list[dict[str, Any]]) -> int:
        """Merge *rows* into an existing symbol snapshot and publish atomically."""
        incoming = self._normalize_rows(rows, symbol)
        if not incoming:
            return 0

        existing = self.read_symbol_rows(symbol)
        existing_dates = {row["trade_date"] for row in existing}
        merged: dict[str, dict[str, Any]] = {row["trade_date"]: row for row in existing}

        for row in incoming:
            merged[row["trade_date"]] = row

        inserted = sum(
            1 for trade_date in {row["trade_date"] for row in incoming}
            if trade_date not in existing_dates
        )
        ordered = [merged[trade_date] for trade_date in sorted(merged)]
        self._publish_symbol_rows(symbol, ordered)
        return inserted

    def _query_symbol_aggregates(self, aggregate_sql: str, alias: str) -> list[dict[str, Any]]:
        if not self.get_existing_symbols():
            return []

        sql = f"""
            SELECT symbol, CAST({aggregate_sql} AS VARCHAR) AS {alias}
            FROM read_parquet('{self._escaped_glob()}', hive_partitioning=true)
            GROUP BY symbol
            ORDER BY symbol
        """
        return self._query(sql)

    def _query(self, sql: str, params: Optional[list[Any]] = None) -> list[dict[str, Any]]:
        result = self._conn.execute(sql, params or [])
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def _symbol_path(self, symbol: str) -> Path:
        return self._bronze_dir / f"symbol={symbol}" / "data.parquet"

    def _escaped_glob(self) -> str:
        return str(self._bronze_dir / "symbol=*/data.parquet").replace("'", "''")

    def _normalize_rows(self, rows: list[dict[str, Any]], symbol: str) -> list[dict[str, Any]]:
        symbol_id = self.get_symbol_id(symbol)
        normalized: dict[str, dict[str, Any]] = {}

        for row in rows:
            trade_date = self._normalize_trade_date(row["trade_date"])
            trade_date_str = trade_date.isoformat()
            normalized[trade_date_str] = {
                "trade_date": trade_date_str,
                "symbol_id": symbol_id,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "adj_close": float(row["adj_close"]),
                "volume": int(row["volume"]),
            }

        return [normalized[trade_date] for trade_date in sorted(normalized)]

    def _publish_symbol_rows(self, symbol: str, rows: list[dict[str, Any]]) -> Path:
        out_path = self._symbol_path(symbol)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = out_path.with_name(f".data.parquet.{os.getpid()}.{time.time_ns()}.tmp")
        table = self._table_from_rows(rows)

        try:
            pq.write_table(table, tmp_path, compression="snappy")
            self._validate_parquet_file(tmp_path, expected_rows=len(rows))
            os.replace(tmp_path, out_path)
        finally:
            if tmp_path.exists():
                tmp_path.unlink()

        log.info("Published %s", out_path)
        return out_path

    def _table_from_rows(self, rows: list[dict[str, Any]]) -> pa.Table:
        payload = [
            {
                "trade_date": self._normalize_trade_date(row["trade_date"]),
                "symbol_id": int(row["symbol_id"]),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "adj_close": float(row["adj_close"]),
                "volume": int(row["volume"]),
            }
            for row in rows
        ]
        return pa.Table.from_pylist(payload, schema=_PARQUET_SCHEMA)

    def _validate_parquet_file(self, path: Path, expected_rows: int) -> None:
        table = pq.read_table(path, columns=list(_BASE_COLUMNS))
        if table.num_rows != expected_rows:
            raise ValueError(
                f"{path}: expected {expected_rows} rows, found {table.num_rows}"
            )

        trade_dates = [
            value.isoformat() if isinstance(value, date) else str(value)
            for value in table.column("trade_date").to_pylist()
        ]
        if trade_dates != sorted(trade_dates):
            raise ValueError(f"{path}: trade_date values are not sorted ascending")
        if len(trade_dates) != len(set(trade_dates)):
            raise ValueError(f"{path}: duplicate trade_date values detected")

    def _normalize_trade_date(self, value: Any) -> date:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            return date.fromisoformat(value)
        raise TypeError(f"unsupported trade_date type: {type(value)!r}")
