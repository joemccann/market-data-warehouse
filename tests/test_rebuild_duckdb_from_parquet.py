"""Tests for scripts/rebuild_duckdb_from_parquet.py."""

from __future__ import annotations

from pathlib import Path

import pytest

from clients.bronze_client import BronzeClient
from clients.db_client import DBClient
from scripts.rebuild_duckdb_from_parquet import main


def _row(trade_date: str, symbol_id: int, close: float) -> dict:
    return {
        "trade_date": trade_date,
        "symbol_id": symbol_id,
        "open": close - 1.0,
        "high": close + 1.0,
        "low": close - 2.0,
        "close": close,
        "adj_close": close,
        "volume": 1000,
    }


class TestRebuildDuckDBFromParquet:
    @pytest.mark.integration
    def test_rebuilds_duckdb_from_bronze(self, tmp_bronze, tmp_path, monkeypatch):
        db_path = tmp_path / "rebuilt.duckdb"

        with BronzeClient(bronze_dir=tmp_bronze) as bronze:
            aapl_id = bronze.get_symbol_id("AAPL")
            msft_id = bronze.get_symbol_id("MSFT")
            bronze.replace_ticker_rows(
                "AAPL",
                [_row("2025-01-02", aapl_id, 102.0), _row("2025-01-03", aapl_id, 103.0)],
            )
            bronze.replace_ticker_rows("MSFT", [_row("2025-01-05", msft_id, 205.0)])

        monkeypatch.setattr(
            "sys.argv",
            [
                "rebuild_duckdb_from_parquet.py",
                "--bronze-dir",
                str(tmp_bronze),
                "--db-path",
                str(db_path),
            ],
        )

        main()

        with DBClient(db_path=db_path) as db:
            assert db.get_latest_dates() == {"AAPL": "2025-01-03", "MSFT": "2025-01-05"}
            counts = db.query("SELECT count(*) AS cnt FROM md.equities_daily")
            assert counts == [{"cnt": 3}]

    @pytest.mark.integration
    def test_missing_bronze_dir_raises(self, tmp_path, monkeypatch):
        missing_dir = tmp_path / "missing"
        db_path = tmp_path / "rebuilt.duckdb"
        monkeypatch.setattr(
            "sys.argv",
            [
                "rebuild_duckdb_from_parquet.py",
                "--bronze-dir",
                str(missing_dir),
                "--db-path",
                str(db_path),
            ],
        )

        with pytest.raises(FileNotFoundError, match="bronze directory does not exist"):
            main()

    @pytest.mark.integration
    def test_empty_bronze_dir_raises(self, tmp_bronze, tmp_path, monkeypatch):
        db_path = tmp_path / "rebuilt.duckdb"
        monkeypatch.setattr(
            "sys.argv",
            [
                "rebuild_duckdb_from_parquet.py",
                "--bronze-dir",
                str(tmp_bronze),
                "--db-path",
                str(db_path),
            ],
        )

        with pytest.raises(FileNotFoundError, match="no bronze parquet snapshots found"):
            main()
