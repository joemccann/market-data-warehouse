"""Tests for scripts/rebuild_duckdb_from_parquet.py."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

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
    def test_rebuilds_existing_duckdb_file(self, tmp_bronze, tmp_path, monkeypatch):
        db_path = tmp_path / "rebuilt.duckdb"

        with BronzeClient(bronze_dir=tmp_bronze) as bronze:
            aapl_id = bronze.get_symbol_id("AAPL")
            bronze.replace_ticker_rows("AAPL", [_row("2025-01-02", aapl_id, 102.0)])

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

        with BronzeClient(bronze_dir=tmp_bronze) as bronze:
            aapl_id = bronze.get_symbol_id("AAPL")
            bronze.replace_ticker_rows(
                "AAPL",
                [
                    _row("2025-01-02", aapl_id, 102.0),
                    _row("2025-01-03", aapl_id, 103.0),
                ],
            )

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
            assert db.get_latest_dates() == {"AAPL": "2025-01-03"}
            counts = db.query("SELECT count(*) AS cnt FROM md.equities_daily")
            assert counts == [{"cnt": 2}]

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

    @pytest.mark.integration
    def test_asset_class_volatility(self, tmp_path, monkeypatch):
        """--asset-class volatility uses CBOE venue and derives bronze dir."""
        data_lake = tmp_path / "data-lake"
        vol_bronze = data_lake / "bronze" / "asset_class=volatility"
        db_path = tmp_path / "rebuilt.duckdb"

        with BronzeClient(bronze_dir=vol_bronze) as bronze:
            vix_id = bronze.get_symbol_id("VIX")
            bronze.replace_ticker_rows("VIX", [_row("2025-01-02", vix_id, 19.0)])

        monkeypatch.setattr(
            "sys.argv",
            [
                "rebuild_duckdb_from_parquet.py",
                "--asset-class", "volatility",
                "--db-path", str(db_path),
            ],
        )

        with patch("scripts.rebuild_duckdb_from_parquet.DATA_LAKE", data_lake):
            main()

        with DBClient(db_path=db_path) as db:
            symbols = db.query("SELECT * FROM md.symbols WHERE symbol = 'VIX'")
            assert symbols[0]["asset_class"] == "volatility"
            assert symbols[0]["venue"] == "CBOE"
            assert db.get_latest_dates() == {"VIX": "2025-01-02"}

    @pytest.mark.integration
    def test_default_bronze_dir_derived_from_asset_class(self, tmp_path, monkeypatch):
        """When --bronze-dir is not specified, it is derived from --asset-class."""
        data_lake = tmp_path / "data-lake"
        eq_bronze = data_lake / "bronze" / "asset_class=equity"
        db_path = tmp_path / "rebuilt.duckdb"

        with BronzeClient(bronze_dir=eq_bronze) as bronze:
            aapl_id = bronze.get_symbol_id("AAPL")
            bronze.replace_ticker_rows("AAPL", [_row("2025-01-02", aapl_id, 150.0)])

        monkeypatch.setattr(
            "sys.argv",
            [
                "rebuild_duckdb_from_parquet.py",
                "--db-path", str(db_path),
            ],
        )

        with patch("scripts.rebuild_duckdb_from_parquet.DATA_LAKE", data_lake):
            main()

        with DBClient(db_path=db_path) as db:
            assert db.get_latest_dates() == {"AAPL": "2025-01-02"}

    @pytest.mark.integration
    def test_asset_class_futures(self, tmp_path, monkeypatch):
        """--asset-class futures uses replace_futures_from_parquet."""
        data_lake = tmp_path / "data-lake"
        fut_bronze = data_lake / "bronze" / "asset_class=futures"
        db_path = tmp_path / "rebuilt.duckdb"

        from clients.bronze_client import BronzeClient
        from clients.symbol_ids import stable_symbol_id
        contract_id = stable_symbol_id("ES_202506")
        with BronzeClient(bronze_dir=fut_bronze, asset_class="futures") as bronze:
            bronze.replace_ticker_rows("ES_202506", [
                {
                    "trade_date": "2025-01-02",
                    "contract_id": contract_id,
                    "root_symbol": "ES",
                    "expiry_date": "2025-06-01",
                    "open": 4500.0, "high": 4550.0, "low": 4480.0,
                    "close": 4520.0, "settlement": 4520.0,
                    "volume": 500000, "open_interest": 0,
                },
            ])

        monkeypatch.setattr(
            "sys.argv",
            [
                "rebuild_duckdb_from_parquet.py",
                "--asset-class", "futures",
                "--db-path", str(db_path),
            ],
        )

        with patch("scripts.rebuild_duckdb_from_parquet.DATA_LAKE", data_lake):
            main()

        with DBClient(db_path=db_path) as db:
            rows = db.query("SELECT * FROM md.futures_daily")
            assert len(rows) == 1
            assert rows[0]["root_symbol"] == "ES"
