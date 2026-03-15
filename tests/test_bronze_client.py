"""Tests for clients/bronze_client.py."""

from __future__ import annotations

from datetime import date, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from clients.bronze_client import BronzeClient
from clients.symbol_ids import stable_symbol_id


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


class TestBronzeClient:
    @pytest.mark.integration
    def test_empty_state_helpers(self, bronze):
        assert bronze.bronze_dir.name == "bronze"
        assert bronze.get_existing_symbols() == set()
        assert bronze.get_latest_dates() == {}
        assert bronze.get_oldest_dates() == {}
        assert bronze.get_summary() == []
        assert bronze.read_symbol_rows("MISSING") == []

    @pytest.mark.integration
    def test_replace_ticker_rows_replaces_snapshot_atomically(self, bronze):
        first = [_row("2025-01-02", 999, 153.0)]
        second = [_row("2025-01-03", 111, 156.0)]

        assert bronze.replace_ticker_rows("AAPL", first) == 1
        assert bronze.replace_ticker_rows("AAPL", second) == 1

        rows = bronze.read_symbol_rows("AAPL")
        assert rows == [
            {
                "trade_date": "2025-01-03",
                "symbol_id": stable_symbol_id("AAPL"),
                "open": 155.0,
                "high": 157.0,
                "low": 154.0,
                "close": 156.0,
                "adj_close": 156.0,
                "volume": 1000,
            }
        ]
        assert list(bronze.bronze_dir.glob("symbol=AAPL/.data.parquet.*.tmp")) == []

    @pytest.mark.integration
    def test_replace_ticker_rows_sorts_and_deduplicates(self, bronze):
        symbol_id = bronze.get_symbol_id("AAPL")
        inserted = bronze.replace_ticker_rows(
            "AAPL",
            [
                _row("2025-01-03", symbol_id, 103.0),
                _row("2025-01-02", symbol_id, 102.0),
                _row("2025-01-02", symbol_id, 202.0),
            ],
        )

        assert inserted == 2
        assert bronze.get_existing_symbols() == {"AAPL"}
        assert bronze.read_symbol_rows("AAPL") == [
            _row("2025-01-02", symbol_id, 202.0),
            _row("2025-01-03", symbol_id, 103.0),
        ]

    @pytest.mark.integration
    def test_replace_empty_snapshot_raises(self, bronze):
        with pytest.raises(ValueError, match="cannot publish an empty parquet snapshot"):
            bronze.replace_ticker_rows("AAPL", [])

    @pytest.mark.integration
    def test_merge_ticker_rows_counts_only_new_dates(self, bronze):
        symbol_id = bronze.get_symbol_id("MSFT")
        bronze.replace_ticker_rows(
            "MSFT",
            [_row("2025-01-02", symbol_id, 303.0)],
        )

        inserted = bronze.merge_ticker_rows(
            "MSFT",
            [
                _row("2025-01-02", symbol_id, 304.0),
                _row("2025-01-03", symbol_id, 306.0),
            ],
        )

        assert inserted == 1
        rows = bronze.read_symbol_rows("MSFT")
        assert [row["trade_date"] for row in rows] == ["2025-01-02", "2025-01-03"]
        assert rows[0]["close"] == 304.0

    @pytest.mark.integration
    def test_merge_empty_rows_returns_zero(self, bronze):
        assert bronze.merge_ticker_rows("AAPL", []) == 0

    @pytest.mark.integration
    def test_get_symbol_id_reuses_existing_snapshot_id(self, bronze):
        first_id = bronze.get_symbol_id("AAPL")
        bronze.replace_ticker_rows("AAPL", [_row("2025-01-02", first_id, 102.0)])

        assert bronze.get_symbol_id("AAPL") == first_id

    @pytest.mark.integration
    def test_get_symbol_id_empty_snapshot_falls_back(self, bronze, tmp_bronze, monkeypatch):
        path = tmp_bronze / "symbol=AAPL" / "data.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"placeholder")

        monkeypatch.setattr(
            "clients.bronze_client.pq.read_table",
            lambda *args, **kwargs: pa.table({"symbol_id": pa.array([], type=pa.int64())}),
        )

        assert bronze.get_symbol_id("AAPL") == stable_symbol_id("AAPL")

    @pytest.mark.integration
    def test_summary_queries(self, bronze):
        aapl_id = bronze.get_symbol_id("AAPL")
        msft_id = bronze.get_symbol_id("MSFT")
        bronze.replace_ticker_rows(
            "AAPL",
            [_row("2025-01-02", aapl_id, 102.0), _row("2025-01-03", aapl_id, 103.0)],
        )
        bronze.replace_ticker_rows("MSFT", [_row("2025-01-05", msft_id, 205.0)])

        assert bronze.get_latest_dates() == {"AAPL": "2025-01-03", "MSFT": "2025-01-05"}
        assert bronze.get_oldest_dates() == {"AAPL": "2025-01-02", "MSFT": "2025-01-05"}
        assert bronze.get_summary() == [
            {"symbol": "AAPL", "rows": 2, "earliest": "2025-01-02", "latest": "2025-01-03"},
            {"symbol": "MSFT", "rows": 1, "earliest": "2025-01-05", "latest": "2025-01-05"},
        ]

    @pytest.mark.integration
    def test_publish_cleans_temp_file_on_replace_error(self, bronze, monkeypatch):
        def _boom(src, dst):
            raise OSError("replace failed")

        monkeypatch.setattr("clients.bronze_client.os.replace", _boom)

        with pytest.raises(OSError, match="replace failed"):
            bronze.replace_ticker_rows(
                "AAPL",
                [_row("2025-01-02", bronze.get_symbol_id("AAPL"), 102.0)],
            )

        assert list(bronze.bronze_dir.glob("symbol=AAPL/.data.parquet.*.tmp")) == []

    @pytest.mark.integration
    def test_validate_parquet_file_errors(self, bronze, tmp_path):
        mismatch_path = tmp_path / "mismatch.parquet"
        pq.write_table(
            pa.Table.from_pylist(
                [{"trade_date": date(2025, 1, 2), "symbol_id": 1, "open": 1.0, "high": 2.0,
                  "low": 0.5, "close": 1.5, "adj_close": 1.5, "volume": 100}],
            ),
            mismatch_path,
        )
        with pytest.raises(ValueError, match="expected 2 rows, found 1"):
            bronze._validate_parquet_file(mismatch_path, expected_rows=2)

        unsorted_path = tmp_path / "unsorted.parquet"
        pq.write_table(
            pa.Table.from_pylist(
                [
                    _row("2025-01-03", 1, 103.0) | {"trade_date": date(2025, 1, 3)},
                    _row("2025-01-02", 1, 102.0) | {"trade_date": date(2025, 1, 2)},
                ]
            ),
            unsorted_path,
        )
        with pytest.raises(ValueError, match="not sorted ascending"):
            bronze._validate_parquet_file(unsorted_path, expected_rows=2)

        duplicate_path = tmp_path / "duplicate.parquet"
        pq.write_table(
            pa.Table.from_pylist(
                [
                    _row("2025-01-02", 1, 102.0) | {"trade_date": date(2025, 1, 2)},
                    _row("2025-01-02", 1, 202.0) | {"trade_date": date(2025, 1, 2)},
                ]
            ),
            duplicate_path,
        )
        with pytest.raises(ValueError, match="duplicate trade_date"):
            bronze._validate_parquet_file(duplicate_path, expected_rows=2)

    @pytest.mark.integration
    def test_normalize_trade_date_variants(self, bronze):
        assert bronze._normalize_trade_date(datetime(2025, 1, 2, 10, 30)) == date(2025, 1, 2)
        assert bronze._normalize_trade_date(date(2025, 1, 3)) == date(2025, 1, 3)
        assert bronze._normalize_trade_date("2025-01-04") == date(2025, 1, 4)
        with pytest.raises(TypeError, match="unsupported trade_date type"):
            bronze._normalize_trade_date(123)


# ── Futures-specific tests ────────────────────────────────────────────


def _futures_row(trade_date: str, contract_id: int, close: float) -> dict:
    return {
        "trade_date": trade_date,
        "contract_id": contract_id,
        "root_symbol": "ES",
        "expiry_date": "2025-06-01",
        "open": close - 1.0,
        "high": close + 1.0,
        "low": close - 2.0,
        "close": close,
        "settlement": close,
        "volume": 5000,
        "open_interest": 0,
    }


class TestBronzeClientFutures:
    @pytest.fixture()
    def futures_bronze(self, tmp_bronze):
        client = BronzeClient(bronze_dir=tmp_bronze, asset_class="futures")
        yield client
        client.close()

    @pytest.mark.integration
    def test_invalid_asset_class_raises(self, tmp_bronze):
        with pytest.raises(ValueError, match="unsupported asset_class"):
            BronzeClient(bronze_dir=tmp_bronze, asset_class="nope")

    @pytest.mark.integration
    def test_futures_replace_and_read(self, futures_bronze):
        cid = futures_bronze.get_symbol_id("ESM5")
        row = _futures_row("2025-03-10", cid, 5200.0)
        assert futures_bronze.replace_ticker_rows("ESM5", [row]) == 1

        rows = futures_bronze.read_symbol_rows("ESM5")
        assert len(rows) == 1
        r = rows[0]
        assert r["trade_date"] == "2025-03-10"
        assert r["contract_id"] == cid
        assert r["root_symbol"] == "ES"
        assert r["expiry_date"] == "2025-06-01"
        assert r["open"] == 5199.0
        assert r["high"] == 5201.0
        assert r["low"] == 5198.0
        assert r["close"] == 5200.0
        assert r["settlement"] == 5200.0
        assert r["volume"] == 5000
        assert r["open_interest"] == 0

    @pytest.mark.integration
    def test_futures_merge_counts_new_dates(self, futures_bronze):
        cid = futures_bronze.get_symbol_id("ESM5")
        futures_bronze.replace_ticker_rows(
            "ESM5", [_futures_row("2025-03-10", cid, 5200.0)]
        )

        inserted = futures_bronze.merge_ticker_rows(
            "ESM5",
            [
                _futures_row("2025-03-10", cid, 5210.0),
                _futures_row("2025-03-11", cid, 5220.0),
            ],
        )
        assert inserted == 1

        rows = futures_bronze.read_symbol_rows("ESM5")
        assert len(rows) == 2
        assert [r["trade_date"] for r in rows] == ["2025-03-10", "2025-03-11"]

    @pytest.mark.integration
    def test_futures_get_symbol_id_uses_contract_id(self, futures_bronze):
        cid = stable_symbol_id("ESM5")
        futures_bronze.replace_ticker_rows(
            "ESM5", [_futures_row("2025-03-10", cid, 5200.0)]
        )

        retrieved = futures_bronze.get_symbol_id("ESM5")
        assert retrieved == cid
