"""Integration tests for clients/db_client.py — 100% coverage target.

Uses a temporary DuckDB file per test (via conftest.py fixtures).
No production data is touched.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from clients.db_client import DBClient


# ══════════════════════════════════════════════════════════════════════
# Construction / lifecycle
# ══════════════════════════════════════════════════════════════════════


class TestInit:
    @pytest.mark.integration
    def test_connects_and_creates_index(self, tmp_duckdb):
        client = DBClient(db_path=tmp_duckdb)
        # Verify the unique index exists by checking system catalog
        indexes = client.query(
            "SELECT index_name FROM duckdb_indexes() WHERE index_name = 'idx_equities_daily_dedup'"
        )
        assert len(indexes) == 1
        client.close()

    @pytest.mark.integration
    def test_ensure_schema_handles_catalog_exception(self, tmp_path):
        """If the md schema doesn't exist, _ensure_schema swallows the CatalogException."""
        db_path = tmp_path / "empty.duckdb"
        conn = duckdb.connect(str(db_path))
        conn.close()
        # This should not raise — CatalogException is caught
        client = DBClient(db_path=db_path)
        client.close()


class TestLifecycle:
    @pytest.mark.integration
    def test_context_manager(self, tmp_duckdb):
        with DBClient(db_path=tmp_duckdb) as client:
            assert isinstance(client, DBClient)

    @pytest.mark.integration
    def test_close(self, tmp_duckdb):
        client = DBClient(db_path=tmp_duckdb)
        client.close()


# ══════════════════════════════════════════════════════════════════════
# upsert_symbol
# ══════════════════════════════════════════════════════════════════════


class TestUpsertSymbol:
    @pytest.mark.integration
    def test_insert_new_symbol(self, db):
        sid = db.upsert_symbol("AAPL", "equity", "NASDAQ")
        assert isinstance(sid, int)
        assert sid > 0

    @pytest.mark.integration
    def test_returns_existing_symbol(self, db):
        sid1 = db.upsert_symbol("AAPL", "equity", "NASDAQ")
        sid2 = db.upsert_symbol("AAPL", "equity", "NASDAQ")
        assert sid1 == sid2

    @pytest.mark.integration
    def test_different_symbols_get_different_ids(self, db):
        sid1 = db.upsert_symbol("AAPL", "equity", "NASDAQ")
        sid2 = db.upsert_symbol("MSFT", "equity", "NASDAQ")
        assert sid1 != sid2


# ══════════════════════════════════════════════════════════════════════
# insert_equities_daily
# ══════════════════════════════════════════════════════════════════════


class TestInsertEquitiesDaily:
    @pytest.mark.integration
    def test_empty_list_returns_zero(self, db):
        assert db.insert_equities_daily([]) == 0

    @pytest.mark.integration
    def test_inserts_rows(self, db):
        sid = db.upsert_symbol("AAPL", "equity", "US")
        rows = [
            {
                "trade_date": "2025-01-02",
                "symbol_id": sid,
                "open": 150.0,
                "high": 155.0,
                "low": 149.0,
                "close": 153.0,
                "adj_close": 153.0,
                "volume": 1000000,
            },
            {
                "trade_date": "2025-01-03",
                "symbol_id": sid,
                "open": 153.0,
                "high": 157.0,
                "low": 152.0,
                "close": 156.0,
                "adj_close": 156.0,
                "volume": 1200000,
            },
        ]
        inserted = db.insert_equities_daily(rows)
        assert inserted == 2

    @pytest.mark.integration
    def test_dedup_skips_duplicates(self, db):
        sid = db.upsert_symbol("AAPL", "equity", "US")
        row = {
            "trade_date": "2025-01-02",
            "symbol_id": sid,
            "open": 150.0,
            "high": 155.0,
            "low": 149.0,
            "close": 153.0,
            "adj_close": 153.0,
            "volume": 1000000,
        }
        assert db.insert_equities_daily([row]) == 1
        assert db.insert_equities_daily([row]) == 0  # Duplicate


# ══════════════════════════════════════════════════════════════════════
# delete_equities_daily
# ══════════════════════════════════════════════════════════════════════


class TestDeleteEquitiesDaily:
    @pytest.mark.integration
    def test_deletes_rows_for_symbol(self, db):
        sid = db.upsert_symbol("AAPL", "equity", "US")
        rows = [
            {
                "trade_date": f"2025-01-0{d}",
                "symbol_id": sid,
                "open": 100.0,
                "high": 105.0,
                "low": 99.0,
                "close": 102.0,
                "adj_close": 102.0,
                "volume": 1000000,
            }
            for d in range(2, 5)
        ]
        db.insert_equities_daily(rows)
        deleted = db.delete_equities_daily(sid)
        assert deleted == 3
        remaining = db.query("SELECT count(*) AS cnt FROM md.equities_daily WHERE symbol_id = ?", [sid])
        assert remaining[0]["cnt"] == 0

    @pytest.mark.integration
    def test_only_deletes_target_symbol(self, db):
        sid_a = db.upsert_symbol("AAPL", "equity", "US")
        sid_m = db.upsert_symbol("MSFT", "equity", "US")
        for sid in [sid_a, sid_m]:
            db.insert_equities_daily(
                [
                    {
                        "trade_date": "2025-01-02",
                        "symbol_id": sid,
                        "open": 100.0,
                        "high": 105.0,
                        "low": 99.0,
                        "close": 102.0,
                        "adj_close": 102.0,
                        "volume": 1000000,
                    }
                ]
            )
        db.delete_equities_daily(sid_a)
        remaining = db.query("SELECT count(*) AS cnt FROM md.equities_daily")
        assert remaining[0]["cnt"] == 1  # Only MSFT remains

    @pytest.mark.integration
    def test_returns_zero_when_no_rows(self, db):
        assert db.delete_equities_daily(999999) == 0


# ══════════════════════════════════════════════════════════════════════
# query
# ══════════════════════════════════════════════════════════════════════


class TestQuery:
    @pytest.mark.integration
    def test_returns_list_of_dicts(self, db):
        result = db.query("SELECT 1 AS a, 'hello' AS b")
        assert result == [{"a": 1, "b": "hello"}]

    @pytest.mark.integration
    def test_with_params(self, db):
        result = db.query("SELECT ? AS val", [42])
        assert result == [{"val": 42}]

    @pytest.mark.integration
    def test_empty_result(self, db):
        result = db.query("SELECT * FROM md.symbols WHERE 1=0")
        assert result == []


# ══════════════════════════════════════════════════════════════════════
# get_equities_daily
# ══════════════════════════════════════════════════════════════════════


class TestGetEquitiesDaily:
    @pytest.fixture(autouse=True)
    def _seed_data(self, db):
        """Seed the DB with test data before each test in this class."""
        sid = db.upsert_symbol("TEST", "equity", "US")
        rows = [
            {
                "trade_date": f"2025-01-0{d}",
                "symbol_id": sid,
                "open": 100.0 + d,
                "high": 105.0 + d,
                "low": 99.0 + d,
                "close": 102.0 + d,
                "adj_close": 102.0 + d,
                "volume": 1000000 * d,
            }
            for d in range(2, 7)
        ]
        db.insert_equities_daily(rows)

    @pytest.mark.integration
    def test_no_date_filters(self, db):
        result = db.get_equities_daily("TEST")
        assert len(result) == 5
        assert result[0]["symbol"] == "TEST"

    @pytest.mark.integration
    def test_with_start_date(self, db):
        result = db.get_equities_daily("TEST", start_date="2025-01-04")
        assert len(result) == 3

    @pytest.mark.integration
    def test_with_end_date(self, db):
        result = db.get_equities_daily("TEST", end_date="2025-01-04")
        assert len(result) == 3

    @pytest.mark.integration
    def test_with_both_dates(self, db):
        result = db.get_equities_daily("TEST", start_date="2025-01-03", end_date="2025-01-05")
        assert len(result) == 3

    @pytest.mark.integration
    def test_nonexistent_symbol_returns_empty(self, db):
        result = db.get_equities_daily("NOPE")
        assert result == []

    @pytest.mark.integration
    def test_results_ordered_by_date(self, db):
        result = db.get_equities_daily("TEST")
        dates = [str(r["trade_date"]) for r in result]
        assert dates == sorted(dates)


# ══════════════════════════════════════════════════════════════════════
# export_to_parquet
# ══════════════════════════════════════════════════════════════════════


class TestGetLatestDates:
    @pytest.mark.integration
    def test_returns_latest_dates(self, db):
        sid = db.upsert_symbol("AAPL", "equity", "SMART")
        db.insert_equities_daily(
            [
                {
                    "trade_date": "2020-01-02",
                    "symbol_id": sid,
                    "open": 150.0, "high": 155.0, "low": 149.0,
                    "close": 153.0, "adj_close": 153.0, "volume": 1000000,
                },
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": sid,
                    "open": 200.0, "high": 205.0, "low": 199.0,
                    "close": 203.0, "adj_close": 203.0, "volume": 2000000,
                },
            ]
        )
        result = db.get_latest_dates()
        assert result == {"AAPL": "2025-01-02"}

    @pytest.mark.integration
    def test_returns_empty_when_no_data(self, db):
        result = db.get_latest_dates()
        assert result == {}

    @pytest.mark.integration
    def test_multiple_symbols(self, db):
        sid_a = db.upsert_symbol("AAPL", "equity", "SMART")
        sid_m = db.upsert_symbol("MSFT", "equity", "SMART")
        db.insert_equities_daily(
            [
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": sid_a,
                    "open": 150.0, "high": 155.0, "low": 149.0,
                    "close": 153.0, "adj_close": 153.0, "volume": 1000000,
                },
                {
                    "trade_date": "2025-01-03",
                    "symbol_id": sid_m,
                    "open": 400.0, "high": 405.0, "low": 399.0,
                    "close": 403.0, "adj_close": 403.0, "volume": 500000,
                },
            ]
        )
        result = db.get_latest_dates()
        assert result == {"AAPL": "2025-01-02", "MSFT": "2025-01-03"}


class TestExportToParquet:
    @pytest.mark.integration
    def test_creates_parquet_file(self, db, tmp_path):
        sid = db.upsert_symbol("AAPL", "equity", "US")
        db.insert_equities_daily(
            [
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": sid,
                    "open": 150.0,
                    "high": 155.0,
                    "low": 149.0,
                    "close": 153.0,
                    "adj_close": 153.0,
                    "volume": 1000000,
                }
            ]
        )
        out = tmp_path / "subdir" / "test.parquet"
        result = db.export_to_parquet("SELECT * FROM md.equities_daily", out)
        assert result == out
        assert out.exists()
        assert out.stat().st_size > 0

    @pytest.mark.integration
    def test_creates_parent_dirs(self, db, tmp_path):
        out = tmp_path / "a" / "b" / "c" / "test.parquet"
        db.export_to_parquet("SELECT 1 AS x", out)
        assert out.exists()


# ══════════════════════════════════════════════════════════════════════
# write_ticker_parquet
# ══════════════════════════════════════════════════════════════════════


class TestWriteTickerParquet:
    @pytest.mark.integration
    def test_writes_parquet_to_correct_path(self, db, tmp_path):
        sid = db.upsert_symbol("AAPL", "equity", "SMART")
        db.insert_equities_daily(
            [
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": sid,
                    "open": 150.0, "high": 155.0, "low": 149.0,
                    "close": 153.0, "adj_close": 153.0, "volume": 1000000,
                }
            ]
        )
        bronze = tmp_path / "bronze"
        result = db.write_ticker_parquet("AAPL", sid, bronze)
        expected = bronze / "symbol=AAPL" / "data.parquet"
        assert result == expected
        assert expected.exists()
        assert expected.stat().st_size > 0

    @pytest.mark.integration
    def test_parquet_contains_expected_columns(self, db, tmp_path):
        sid = db.upsert_symbol("AAPL", "equity", "SMART")
        db.insert_equities_daily(
            [
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": sid,
                    "open": 150.0, "high": 155.0, "low": 149.0,
                    "close": 153.0, "adj_close": 153.0, "volume": 1000000,
                }
            ]
        )
        bronze = tmp_path / "bronze"
        out = db.write_ticker_parquet("AAPL", sid, bronze)
        # Read it back via DuckDB
        rows = db.query(f"SELECT * FROM read_parquet('{out}')")
        assert len(rows) == 1
        assert "trade_date" in rows[0]
        assert "symbol_id" in rows[0]
        assert "open" in rows[0]
        assert "volume" in rows[0]

    @pytest.mark.integration
    def test_creates_directories(self, db, tmp_path):
        sid = db.upsert_symbol("TEST", "equity", "SMART")
        db.insert_equities_daily(
            [
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": sid,
                    "open": 100.0, "high": 105.0, "low": 99.0,
                    "close": 102.0, "adj_close": 102.0, "volume": 500000,
                }
            ]
        )
        deep_dir = tmp_path / "a" / "b" / "bronze"
        result = db.write_ticker_parquet("TEST", sid, deep_dir)
        assert result.exists()


# ══════════════════════════════════════════════════════════════════════
# storage compatibility helpers
# ══════════════════════════════════════════════════════════════════════


class TestStorageCompatibility:
    @pytest.mark.integration
    def test_get_oldest_dates(self, db):
        sid = db.upsert_symbol("AAPL", "equity", "SMART")
        db.insert_equities_daily(
            [
                {
                    "trade_date": "2020-01-02",
                    "symbol_id": sid,
                    "open": 150.0, "high": 155.0, "low": 149.0,
                    "close": 153.0, "adj_close": 153.0, "volume": 1000000,
                },
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": sid,
                    "open": 200.0, "high": 205.0, "low": 199.0,
                    "close": 203.0, "adj_close": 203.0, "volume": 2000000,
                },
            ]
        )

        assert db.get_oldest_dates() == {"AAPL": "2020-01-02"}

    @pytest.mark.integration
    def test_get_existing_symbols_and_get_symbol_id(self, db):
        sid = db.get_symbol_id("AAPL")
        db.insert_equities_daily(
            [
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": sid,
                    "open": 150.0, "high": 155.0, "low": 149.0,
                    "close": 153.0, "adj_close": 153.0, "volume": 1000000,
                }
            ]
        )

        assert db.get_symbol_id("AAPL") == sid
        assert db.get_existing_symbols() == {"AAPL"}

    @pytest.mark.integration
    def test_replace_and_merge_ticker_rows(self, db):
        first = [
            {
                "trade_date": "2025-01-02",
                "symbol_id": 999,
                "open": 150.0, "high": 155.0, "low": 149.0,
                "close": 153.0, "adj_close": 153.0, "volume": 1000000,
            }
        ]
        second = [
            {
                "trade_date": "2025-01-03",
                "symbol_id": 111,
                "open": 153.0, "high": 157.0, "low": 152.0,
                "close": 156.0, "adj_close": 156.0, "volume": 1200000,
            }
        ]

        assert db.replace_ticker_rows("AAPL", first) == 1
        assert db.merge_ticker_rows("AAPL", second) == 1
        assert db.get_summary() == [
            {
                "symbol": "AAPL",
                "rows": 2,
                "earliest": date(2025, 1, 2),
                "latest": date(2025, 1, 3),
            }
        ]

    @pytest.mark.integration
    def test_replace_equities_from_parquet_rolls_back_on_error(self, db, tmp_path):
        sid = db.upsert_symbol("AAPL", "equity", "SMART")
        db.insert_equities_daily(
            [
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": sid,
                    "open": 150.0, "high": 155.0, "low": 149.0,
                    "close": 153.0, "adj_close": 153.0, "volume": 1000000,
                }
            ]
        )

        broken_bronze = tmp_path / "bronze" / "symbol=AAPL"
        broken_bronze.mkdir(parents=True, exist_ok=True)
        (broken_bronze / "data.parquet").write_text("not parquet")

        with pytest.raises(Exception):
            db.replace_equities_from_parquet(tmp_path / "bronze")

        assert db.get_latest_dates() == {"AAPL": "2025-01-02"}

    @pytest.mark.integration
    def test_replace_equities_from_parquet_custom_asset_class_venue(self, db, tmp_path):
        """replace_equities_from_parquet respects custom asset_class and venue."""
        # Seed a parquet file via the DB export path
        sid = db.upsert_symbol("VIX", "volatility", "CBOE")
        db.insert_equities_daily(
            [
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": sid,
                    "open": 18.0, "high": 20.0, "low": 17.0,
                    "close": 19.0, "adj_close": 19.0, "volume": 0,
                }
            ]
        )
        bronze = tmp_path / "bronze"
        db.write_ticker_parquet("VIX", sid, bronze)

        # Rebuild from parquet with custom asset_class/venue
        counts = db.replace_equities_from_parquet(
            bronze, asset_class="volatility", venue="CBOE",
        )
        assert counts["symbols"] == 1
        assert counts["rows"] == 1

        # Verify the symbol was inserted with the correct asset_class and venue
        symbols = db.query("SELECT * FROM md.symbols WHERE symbol = 'VIX'")
        assert symbols[0]["asset_class"] == "volatility"
        assert symbols[0]["venue"] == "CBOE"


# ══════════════════════════════════════════════════════════════════════
# futures_daily
# ══════════════════════════════════════════════════════════════════════


class TestFuturesDaily:
    @pytest.mark.integration
    def test_ensure_schema_creates_futures_table(self, db):
        indexes = db.query(
            "SELECT index_name FROM duckdb_indexes() WHERE index_name = 'idx_futures_daily_dedup'"
        )
        assert len(indexes) == 1

    @pytest.mark.integration
    def test_replace_futures_from_parquet(self, db, tmp_path):
        # Create futures bronze parquet via BronzeClient
        futures_bronze = tmp_path / "futures-bronze"
        from clients.bronze_client import BronzeClient
        from clients.symbol_ids import stable_symbol_id
        contract_id = stable_symbol_id("ES_202506")
        with BronzeClient(bronze_dir=futures_bronze, asset_class="futures") as bronze:
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

        counts = db.replace_futures_from_parquet(futures_bronze)
        assert counts["rows"] == 1

        rows = db.query("SELECT * FROM md.futures_daily")
        assert len(rows) == 1
        assert rows[0]["root_symbol"] == "ES"

    @pytest.mark.integration
    def test_replace_futures_from_parquet_empty(self, db, tmp_path):
        empty_bronze = tmp_path / "empty-bronze"
        empty_bronze.mkdir(parents=True)
        counts = db.replace_futures_from_parquet(empty_bronze)
        assert counts["rows"] == 0

    @pytest.mark.integration
    def test_replace_futures_from_parquet_rollback_on_error(self, db, tmp_path):
        broken_bronze = tmp_path / "bronze" / "symbol=ES_202506"
        broken_bronze.mkdir(parents=True, exist_ok=True)
        (broken_bronze / "data.parquet").write_text("not parquet")

        with pytest.raises(Exception):
            db.replace_futures_from_parquet(tmp_path / "bronze")
