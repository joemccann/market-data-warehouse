"""Compatibility tests for script storage-client selection and hooks."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import scripts.daily_update as daily_script
import scripts.fetch_ib_historical as fetch_script


def _bar(trade_date="2025-01-03", close=156.0):
    return SimpleNamespace(
        date=trade_date,
        open=close - 2.0,
        high=close + 1.0,
        low=close - 3.0,
        close=close,
        volume=1000,
    )


class _CompatStorage:
    def __init__(self):
        self.write_calls = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get_latest_dates(self):
        return {"AAPL": "2025-01-02"}

    def get_symbol_id(self, symbol):
        return 1

    def replace_ticker_rows(self, symbol, rows):
        return len(rows)

    def merge_ticker_rows(self, symbol, rows):
        return len(rows)

    def write_ticker_parquet(self, symbol, symbol_id, bronze_dir):
        self.write_calls.append((symbol, symbol_id, bronze_dir))


class TestFetchScriptCompat:
    def test_storage_client_defaults_to_bronze(self):
        assert fetch_script._storage_client() is fetch_script.BronzeClient

    def test_storage_client_can_switch_to_db_alias(self, monkeypatch):
        sentinel = object()
        monkeypatch.setattr(fetch_script, "DBClient", sentinel)
        assert fetch_script._storage_client() is sentinel

    @pytest.mark.integration
    def test_fetch_helpers_write_parquet_for_compat_storage(self, tmp_path, monkeypatch):
        storage = _CompatStorage()
        monkeypatch.setattr(fetch_script, "BRONZE_DIR", tmp_path / "bronze")

        inserted = fetch_script.fetch_ticker("AAPL", [_bar()], storage)
        backfilled = fetch_script.backfill_ticker("AAPL", [_bar("2024-01-02", 120.0)], storage)

        assert inserted == 1
        assert backfilled == 1
        assert storage.write_calls == [
            ("AAPL", 1, tmp_path / "bronze"),
            ("AAPL", 1, tmp_path / "bronze"),
        ]


class TestDailyScriptCompat:
    def test_storage_client_defaults_to_bronze(self):
        assert daily_script._storage_client() is daily_script.BronzeClient

    def test_storage_client_can_switch_to_db_alias(self, monkeypatch):
        sentinel = object()
        monkeypatch.setattr(daily_script, "DBClient", sentinel)
        assert daily_script._storage_client() is sentinel

    @pytest.mark.integration
    def test_main_calls_write_ticker_parquet_for_compat_storage(self, tmp_path, monkeypatch):
        storage = _CompatStorage()
        mock_ib = MagicMock()
        mock_ib.__enter__ = MagicMock(return_value=mock_ib)
        mock_ib.__exit__ = MagicMock(return_value=False)
        mock_ib.ib.run.side_effect = lambda awaitable: (awaitable.close(), {"AAPL": [_bar()]})[1]

        monkeypatch.setattr("sys.argv", ["daily_update.py"])
        monkeypatch.setattr(daily_script, "DBClient", lambda **kwargs: storage)
        monkeypatch.setattr(daily_script, "is_trading_day", lambda d: True)
        monkeypatch.setattr(daily_script, "IBClient", lambda: mock_ib)
        monkeypatch.setattr(daily_script, "BRONZE_DIR", tmp_path / "bronze")
        monkeypatch.setattr(daily_script, "date", SimpleNamespace(
            today=lambda: date(2025, 1, 3),
            fromisoformat=date.fromisoformat,
        ))

        daily_script.main()

        assert storage.write_calls == [("AAPL", 1, tmp_path / "bronze")]
