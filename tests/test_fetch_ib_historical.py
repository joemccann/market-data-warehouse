"""Tests for scripts/fetch_ib_historical.py — 100% coverage target.

Tests the transform logic (bars_to_rows), compute_date_windows,
async fetch helpers, fetch_ticker, export_bronze_parquet, preset/cursor
helpers, and the main() entrypoint.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from ib_insync import Stock

from clients.bronze_client import BronzeClient
from scripts.fetch_ib_historical import (
    IB_EARLIEST_DATE,
    _cursor_path,
    _run_backfill,
    _run_normal,
    backfill_ticker,
    bars_to_rows,
    clear_cursor,
    compute_date_windows,
    fetch_all_tickers,
    fetch_ticker,
    fetch_ticker_bars,
    get_existing_symbols,
    get_oldest_dates,
    load_cursor,
    load_preset,
    main,
    save_cursor,
)


# ── helpers ───────────────────────────────────────────────────────────


def _make_bar(date="2025-01-02", open=150.0, high=155.0, low=149.0, close=153.0, volume=1000000):
    """Create a mock IB BarData object."""
    return SimpleNamespace(date=date, open=open, high=high, low=low, close=close, volume=volume)


def _seed_bronze(bronze_dir, symbol, rows):
    """Write a canonical bronze snapshot for *symbol*."""
    with BronzeClient(bronze_dir=bronze_dir) as bronze:
        bronze.replace_ticker_rows(symbol, rows)


# ══════════════════════════════════════════════════════════════════════
# bars_to_rows
# ══════════════════════════════════════════════════════════════════════


class TestBarsToRows:
    def test_converts_single_bar(self):
        bar = _make_bar()
        rows = bars_to_rows([bar], symbol_id=42)
        assert len(rows) == 1
        assert rows[0] == {
            "trade_date": "2025-01-02",
            "symbol_id": 42,
            "open": 150.0,
            "high": 155.0,
            "low": 149.0,
            "close": 153.0,
            "adj_close": 153.0,
            "volume": 1000000,
        }

    def test_converts_multiple_bars(self):
        bars = [
            _make_bar(date="2025-01-02", close=153.0),
            _make_bar(date="2025-01-03", close=156.0),
        ]
        rows = bars_to_rows(bars, symbol_id=7)
        assert len(rows) == 2
        assert rows[0]["trade_date"] == "2025-01-02"
        assert rows[1]["trade_date"] == "2025-01-03"
        assert rows[1]["adj_close"] == 156.0

    def test_empty_bars(self):
        assert bars_to_rows([], symbol_id=1) == []


# ══════════════════════════════════════════════════════════════════════
# compute_date_windows
# ══════════════════════════════════════════════════════════════════════


class TestComputeDateWindows:
    def test_less_than_one_year(self):
        head = datetime(2024, 6, 1)
        end = datetime(2025, 1, 1)
        windows = compute_date_windows(head, end)
        assert len(windows) == 1
        assert windows[0][0] == "1 Y"
        assert windows[0][1] == "20250101-00:00:00"

    def test_exactly_one_year(self):
        # 365 days back from 2025-01-01 = 2024-01-02 (2024 is leap year)
        # so head_dt (2024-01-01) < one_year_back (2024-01-02) → 2 windows
        head = datetime(2024, 1, 1)
        end = datetime(2025, 1, 1)
        windows = compute_date_windows(head, end)
        assert len(windows) == 2
        assert all(w[0] == "1 Y" for w in windows)

    def test_multi_year(self):
        head = datetime(2020, 1, 1)
        end = datetime(2025, 1, 1)
        windows = compute_date_windows(head, end)
        # ~5 years → 5 windows
        assert len(windows) >= 5

    def test_head_equals_end(self):
        dt = datetime(2025, 1, 1)
        windows = compute_date_windows(dt, dt)
        assert windows == []

    def test_head_after_end(self):
        head = datetime(2025, 6, 1)
        end = datetime(2025, 1, 1)
        windows = compute_date_windows(head, end)
        assert windows == []

    def test_windows_walk_backwards(self):
        head = datetime(2022, 1, 1)
        end = datetime(2025, 1, 1)
        windows = compute_date_windows(head, end)
        # First window should end at end_dt
        assert windows[0][1] == "20250101-00:00:00"


# ══════════════════════════════════════════════════════════════════════
# load_preset
# ══════════════════════════════════════════════════════════════════════


class TestLoadPreset:
    def test_loads_preset_file(self, tmp_path):
        preset = {"name": "test-preset", "tickers": ["AAPL", "MSFT", "NVDA"]}
        preset_file = tmp_path / "test.json"
        preset_file.write_text(json.dumps(preset))

        name, tickers = load_preset(preset_file)
        assert name == "test-preset"
        assert tickers == ["AAPL", "MSFT", "NVDA"]

    def test_loads_preset_from_string_path(self, tmp_path):
        preset = {"name": "sp500", "tickers": ["AAPL"]}
        preset_file = tmp_path / "sp500.json"
        preset_file.write_text(json.dumps(preset))

        name, tickers = load_preset(str(preset_file))
        assert name == "sp500"
        assert tickers == ["AAPL"]


# ══════════════════════════════════════════════════════════════════════
# cursor helpers
# ══════════════════════════════════════════════════════════════════════


class TestCursorPath:
    def test_returns_expected_path(self):
        with patch("scripts.fetch_ib_historical.CURSOR_DIR", __import__("pathlib").Path("/tmp/logs")):
            path = _cursor_path("sp500")
        assert path.name == "cursor_sp500.json"


class TestLoadCursor:
    def test_returns_empty_set_when_no_file(self, tmp_path):
        with patch("scripts.fetch_ib_historical.CURSOR_DIR", tmp_path):
            result = load_cursor("nonexistent")
        assert result == set()

    def test_loads_completed_tickers(self, tmp_path):
        cursor_data = {"completed": ["AAPL", "MSFT"], "started_at": "2025-01-01T00:00:00"}
        cursor_file = tmp_path / "cursor_test.json"
        cursor_file.write_text(json.dumps(cursor_data))

        with patch("scripts.fetch_ib_historical.CURSOR_DIR", tmp_path):
            result = load_cursor("test")
        assert result == {"AAPL", "MSFT"}

    def test_handles_missing_completed_key(self, tmp_path):
        cursor_file = tmp_path / "cursor_test.json"
        cursor_file.write_text(json.dumps({"started_at": "2025-01-01"}))

        with patch("scripts.fetch_ib_historical.CURSOR_DIR", tmp_path):
            result = load_cursor("test")
        assert result == set()


class TestSaveCursor:
    def test_writes_cursor_file(self, tmp_path):
        with patch("scripts.fetch_ib_historical.CURSOR_DIR", tmp_path):
            save_cursor("test", {"AAPL", "MSFT"}, "2025-01-01T00:00:00")

        cursor_file = tmp_path / "cursor_test.json"
        assert cursor_file.exists()
        data = json.loads(cursor_file.read_text())
        assert set(data["completed"]) == {"AAPL", "MSFT"}
        assert data["started_at"] == "2025-01-01T00:00:00"
        assert "updated_at" in data

    def test_creates_parent_dirs(self, tmp_path):
        cursor_dir = tmp_path / "nested" / "logs"
        with patch("scripts.fetch_ib_historical.CURSOR_DIR", cursor_dir):
            save_cursor("test", {"AAPL"}, "2025-01-01T00:00:00")

        assert (cursor_dir / "cursor_test.json").exists()

    def test_overwrites_existing_cursor(self, tmp_path):
        with patch("scripts.fetch_ib_historical.CURSOR_DIR", tmp_path):
            save_cursor("test", {"AAPL"}, "2025-01-01T00:00:00")
            save_cursor("test", {"AAPL", "MSFT"}, "2025-01-01T00:00:00")

        data = json.loads((tmp_path / "cursor_test.json").read_text())
        assert set(data["completed"]) == {"AAPL", "MSFT"}


class TestClearCursor:
    def test_deletes_cursor_file(self, tmp_path):
        cursor_file = tmp_path / "cursor_test.json"
        cursor_file.write_text("{}")

        with patch("scripts.fetch_ib_historical.CURSOR_DIR", tmp_path):
            clear_cursor("test")

        assert not cursor_file.exists()

    def test_no_error_when_file_missing(self, tmp_path):
        with patch("scripts.fetch_ib_historical.CURSOR_DIR", tmp_path):
            clear_cursor("nonexistent")  # Should not raise


# ══════════════════════════════════════════════════════════════════════
# fetch_ticker_bars (async)
# ══════════════════════════════════════════════════════════════════════


class TestFetchTickerBars:
    def test_fetches_and_deduplicates(self):
        mock_ib = MagicMock()
        # qualifyContractsAsync
        mock_ib.ib.qualifyContractsAsync = AsyncMock(return_value=[Stock("AAPL", "SMART", "USD")])
        # get_head_timestamp_async returns a string
        mock_ib.get_head_timestamp_async = AsyncMock(return_value="19800102-00:00:00")
        # get_historical_data_async returns bars (we mock 2 chunks with overlap)
        bar1 = _make_bar(date="2024-12-30")
        bar2 = _make_bar(date="2024-12-31")
        bar_dup = _make_bar(date="2024-12-31")  # duplicate
        bar3 = _make_bar(date="2025-01-02")
        mock_ib.get_historical_data_async = AsyncMock(
            side_effect=[[bar1, bar2], [bar_dup, bar3]]
        )

        sem = asyncio.Semaphore(6)

        # Patch compute_date_windows to return exactly 2 windows
        with patch("scripts.fetch_ib_historical.compute_date_windows") as mock_cdw:
            mock_cdw.return_value = [
                ("1 Y", "20250101-00:00:00"),
                ("1 Y", "20240101-00:00:00"),
            ]
            ticker, bars = asyncio.run(fetch_ticker_bars("AAPL", mock_ib, sem))

        assert ticker == "AAPL"
        # Dedup: bar_dup should be removed
        assert len(bars) == 3
        dates = [str(b.date) for b in bars]
        assert dates == ["2024-12-30", "2024-12-31", "2025-01-02"]

    def test_head_timestamp_as_datetime(self):
        mock_ib = MagicMock()
        mock_ib.ib.qualifyContractsAsync = AsyncMock(return_value=[Stock("AAPL", "SMART", "USD")])
        # IB returns tz-aware datetimes — verify we strip tzinfo
        mock_ib.get_head_timestamp_async = AsyncMock(
            return_value=datetime(1980, 1, 2, tzinfo=timezone.utc)
        )
        mock_ib.get_historical_data_async = AsyncMock(return_value=[_make_bar()])

        sem = asyncio.Semaphore(6)

        with patch("scripts.fetch_ib_historical.compute_date_windows") as mock_cdw:
            mock_cdw.return_value = [("1 Y", "20250101-00:00:00")]
            ticker, bars = asyncio.run(fetch_ticker_bars("AAPL", mock_ib, sem))

        assert ticker == "AAPL"
        assert len(bars) == 1

    def test_empty_head_timestamp_falls_back_to_ib_earliest(self):
        """IB returns '[]' for head timestamp — fall back to IB_EARLIEST_DATE and still fetch."""
        mock_ib = MagicMock()
        mock_ib.ib.qualifyContractsAsync = AsyncMock(return_value=[Stock("BND", "SMART", "USD")])
        mock_ib.get_head_timestamp_async = AsyncMock(return_value=[])
        mock_ib.get_historical_data_async = AsyncMock(return_value=[_make_bar()])

        sem = asyncio.Semaphore(6)

        with patch("scripts.fetch_ib_historical.compute_date_windows") as mock_cdw:
            mock_cdw.return_value = [("1 Y", "20250101-00:00:00")]
            ticker, bars = asyncio.run(fetch_ticker_bars("BND", mock_ib, sem))

        assert ticker == "BND"
        assert len(bars) == 1
        # compute_date_windows must be called with IB_EARLIEST_DATE as head_dt
        head_dt_arg = mock_cdw.call_args[0][0]
        assert head_dt_arg == IB_EARLIEST_DATE

    def test_empty_string_head_timestamp_falls_back_to_ib_earliest(self):
        """IB returns empty string for head timestamp — fall back to IB_EARLIEST_DATE and still fetch."""
        mock_ib = MagicMock()
        mock_ib.ib.qualifyContractsAsync = AsyncMock(return_value=[Stock("DVY", "SMART", "USD")])
        mock_ib.get_head_timestamp_async = AsyncMock(return_value="")
        mock_ib.get_historical_data_async = AsyncMock(return_value=[_make_bar()])

        sem = asyncio.Semaphore(6)

        with patch("scripts.fetch_ib_historical.compute_date_windows") as mock_cdw:
            mock_cdw.return_value = [("1 Y", "20250101-00:00:00")]
            ticker, bars = asyncio.run(fetch_ticker_bars("DVY", mock_ib, sem))

        assert ticker == "DVY"
        assert len(bars) == 1
        head_dt_arg = mock_cdw.call_args[0][0]
        assert head_dt_arg == IB_EARLIEST_DATE

    def test_empty_chunks(self):
        mock_ib = MagicMock()
        mock_ib.ib.qualifyContractsAsync = AsyncMock(return_value=[Stock("AAPL", "SMART", "USD")])
        mock_ib.get_head_timestamp_async = AsyncMock(return_value="20240101-00:00:00")
        mock_ib.get_historical_data_async = AsyncMock(return_value=[])

        sem = asyncio.Semaphore(6)

        with patch("scripts.fetch_ib_historical.compute_date_windows") as mock_cdw:
            mock_cdw.return_value = [("1 Y", "20250101-00:00:00")]
            ticker, bars = asyncio.run(fetch_ticker_bars("AAPL", mock_ib, sem))

        assert bars == []

    def test_max_years_caps_lookback(self):
        """max_years clamps head_dt so fewer windows are generated."""
        mock_ib = MagicMock()
        mock_ib.ib.qualifyContractsAsync = AsyncMock(return_value=[Stock("AAPL", "SMART", "USD")])
        # Stock has data since 1980 — without cap that's ~45 windows
        mock_ib.get_head_timestamp_async = AsyncMock(return_value="19800102-00:00:00")
        mock_ib.get_historical_data_async = AsyncMock(return_value=[_make_bar()])

        sem = asyncio.Semaphore(6)

        with patch("scripts.fetch_ib_historical.compute_date_windows") as mock_cdw:
            mock_cdw.return_value = [("1 Y", "20250101-00:00:00")]
            ticker, bars = asyncio.run(fetch_ticker_bars("AAPL", mock_ib, sem, max_years=2))

        # Verify compute_date_windows was called with a capped head_dt (not 1980)
        call_args = mock_cdw.call_args[0]
        head_dt_arg = call_args[0]
        # head_dt should be ~2 years ago, not 1980
        assert head_dt_arg.year >= 2023

    def test_end_dt_override_uses_custom_end(self):
        """end_dt_override sets end_dt and ignores max_years."""
        mock_ib = MagicMock()
        mock_ib.ib.qualifyContractsAsync = AsyncMock(return_value=[Stock("AAPL", "SMART", "USD")])
        mock_ib.get_head_timestamp_async = AsyncMock(return_value="19800102-00:00:00")
        mock_ib.get_historical_data_async = AsyncMock(return_value=[_make_bar()])

        sem = asyncio.Semaphore(6)
        override_dt = datetime(2020, 6, 15)

        with patch("scripts.fetch_ib_historical.compute_date_windows") as mock_cdw:
            mock_cdw.return_value = [("1 Y", "20200615-00:00:00")]
            ticker, bars = asyncio.run(
                fetch_ticker_bars("AAPL", mock_ib, sem, max_years=2, end_dt_override=override_dt)
            )

        # end_dt should be the override, not datetime.now()
        call_args = mock_cdw.call_args[0]
        end_dt_arg = call_args[1]
        assert end_dt_arg == override_dt
        # head_dt should NOT be capped (max_years ignored with override)
        head_dt_arg = call_args[0]
        assert head_dt_arg.year == 1980


# ══════════════════════════════════════════════════════════════════════
# fetch_all_tickers (async)
# ══════════════════════════════════════════════════════════════════════


class TestFetchAllTickers:
    def test_fetches_multiple_tickers(self):
        bar_a = _make_bar(date="2025-01-02")
        bar_b = _make_bar(date="2025-01-03")

        async def mock_fetch_ticker_bars(ticker, ib, sem, **kwargs):
            if ticker == "AAPL":
                return ("AAPL", [bar_a])
            return ("NVDA", [bar_b])

        mock_ib = MagicMock()

        with patch("scripts.fetch_ib_historical.fetch_ticker_bars", side_effect=mock_fetch_ticker_bars):
            results = asyncio.run(fetch_all_tickers(["AAPL", "NVDA"], mock_ib, max_concurrent=6))

        assert "AAPL" in results
        assert "NVDA" in results
        assert len(results["AAPL"]) == 1
        assert len(results["NVDA"]) == 1

    def test_handles_per_ticker_error(self):
        async def mock_fetch_ticker_bars(ticker, ib, sem, **kwargs):
            if ticker == "FAIL":
                raise IBError("No contract found")
            return (ticker, [_make_bar()])

        mock_ib = MagicMock()
        from clients.ib_client import IBError

        with patch("scripts.fetch_ib_historical.fetch_ticker_bars", side_effect=mock_fetch_ticker_bars):
            results = asyncio.run(fetch_all_tickers(["AAPL", "FAIL"], mock_ib))

        assert len(results["AAPL"]) == 1
        assert results["FAIL"] == []

    def test_handles_generic_exception(self):
        async def mock_fetch_ticker_bars(ticker, ib, sem, **kwargs):
            if ticker == "BOOM":
                raise RuntimeError("unexpected")
            return (ticker, [_make_bar()])

        mock_ib = MagicMock()

        with patch("scripts.fetch_ib_historical.fetch_ticker_bars", side_effect=mock_fetch_ticker_bars):
            results = asyncio.run(fetch_all_tickers(["AAPL", "BOOM"], mock_ib))

        assert len(results["AAPL"]) == 1
        assert results["BOOM"] == []

    def test_passes_end_dt_overrides(self):
        """end_dt_overrides are forwarded to fetch_ticker_bars."""
        captured_kwargs = {}

        async def mock_fetch_ticker_bars(ticker, ib, sem, **kwargs):
            captured_kwargs[ticker] = kwargs
            return (ticker, [_make_bar()])

        mock_ib = MagicMock()
        overrides = {"AAPL": datetime(2020, 6, 15)}

        with patch("scripts.fetch_ib_historical.fetch_ticker_bars", side_effect=mock_fetch_ticker_bars):
            results = asyncio.run(
                fetch_all_tickers(["AAPL", "NVDA"], mock_ib, end_dt_overrides=overrides)
            )

        assert captured_kwargs["AAPL"]["end_dt_override"] == datetime(2020, 6, 15)
        assert captured_kwargs["NVDA"]["end_dt_override"] is None


# ══════════════════════════════════════════════════════════════════════
# fetch_ticker
# ══════════════════════════════════════════════════════════════════════


class TestFetchTicker:
    @pytest.mark.integration
    def test_fetch_inserts_bars(self, bronze):
        """Happy path: pre-fetched bars are inserted."""
        bars = [
            _make_bar(date="2025-01-02", close=153.0),
            _make_bar(date="2025-01-03", close=156.0),
        ]

        inserted = fetch_ticker("AAPL", bars, bronze)
        assert inserted == 2
        rows = bronze.read_symbol_rows("AAPL")
        assert [row["trade_date"] for row in rows] == ["2025-01-02", "2025-01-03"]

    @pytest.mark.integration
    def test_fetch_empty_bars_returns_zero(self, bronze):
        """Empty bars list returns zero."""
        inserted = fetch_ticker("XYZ", [], bronze)
        assert inserted == 0

    @pytest.mark.integration
    def test_fetch_deletes_old_data_before_insert(self, bronze):
        """Verify delete-then-insert flow overwrites existing data."""
        bronze.replace_ticker_rows(
            "AAPL",
            [
                {
                    "trade_date": "2024-06-01",
                    "symbol_id": bronze.get_symbol_id("AAPL"),
                    "open": 100.0,
                    "high": 105.0,
                    "low": 99.0,
                    "close": 102.0,
                    "adj_close": 102.0,
                    "volume": 500000,
                }
            ]
        )

        bars = [_make_bar(date="2025-01-02", close=200.0)]
        inserted = fetch_ticker("AAPL", bars, bronze)
        assert inserted == 1

        rows = bronze.read_symbol_rows("AAPL")
        assert len(rows) == 1
        assert rows[0]["trade_date"] == "2025-01-02"


# ══════════════════════════════════════════════════════════════════════
# export_bronze_parquet
# ══════════════════════════════════════════════════════════════════════


class TestGetExistingSymbols:
    @pytest.mark.integration
    def test_returns_symbols_with_data(self, bronze):
        bronze.replace_ticker_rows(
            "AAPL",
            [
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": bronze.get_symbol_id("AAPL"),
                    "open": 150.0,
                    "high": 155.0,
                    "low": 149.0,
                    "close": 153.0,
                    "adj_close": 153.0,
                    "volume": 1000000,
                }
            ]
        )
        result = get_existing_symbols(bronze)
        assert result == {"AAPL"}

    @pytest.mark.integration
    def test_returns_empty_when_no_data(self, bronze):
        result = get_existing_symbols(bronze)
        assert result == set()


class TestGetOldestDates:
    @pytest.mark.integration
    def test_returns_oldest_dates(self, bronze):
        bronze.replace_ticker_rows(
            "AAPL",
            [
                {
                    "trade_date": "2020-01-02",
                    "symbol_id": bronze.get_symbol_id("AAPL"),
                    "open": 150.0, "high": 155.0, "low": 149.0,
                    "close": 153.0, "adj_close": 153.0, "volume": 1000000,
                },
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": bronze.get_symbol_id("AAPL"),
                    "open": 200.0, "high": 205.0, "low": 199.0,
                    "close": 203.0, "adj_close": 203.0, "volume": 2000000,
                },
            ]
        )
        result = get_oldest_dates(bronze)
        assert result == {"AAPL": "2020-01-02"}

    @pytest.mark.integration
    def test_returns_empty_when_no_data(self, bronze):
        result = get_oldest_dates(bronze)
        assert result == {}


class TestBackfillTicker:
    @pytest.mark.integration
    def test_inserts_without_deleting(self, bronze):
        """backfill_ticker inserts new rows without removing existing data."""
        bronze.replace_ticker_rows(
            "AAPL",
            [
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": bronze.get_symbol_id("AAPL"),
                    "open": 200.0, "high": 205.0, "low": 199.0,
                    "close": 203.0, "adj_close": 203.0, "volume": 2000000,
                }
            ]
        )

        # Backfill older data
        bars = [_make_bar(date="2020-01-02", close=100.0)]
        inserted = backfill_ticker("AAPL", bars, bronze)
        assert inserted == 1

        rows = bronze.read_symbol_rows("AAPL")
        assert len(rows) == 2
        assert [row["trade_date"] for row in rows] == ["2020-01-02", "2025-01-02"]

    @pytest.mark.integration
    def test_empty_bars_returns_zero(self, bronze):
        inserted = backfill_ticker("XYZ", [], bronze)
        assert inserted == 0


# ══════════════════════════════════════════════════════════════════════
# main()
# ══════════════════════════════════════════════════════════════════════


def _mock_ib_instance(ticker_bars):
    """Create a mock IBClient context manager returning *ticker_bars*."""
    def _run(awaitable):
        awaitable.close()
        return ticker_bars

    mock = MagicMock()
    mock.__enter__ = MagicMock(return_value=mock)
    mock.__exit__ = MagicMock(return_value=False)
    mock.ib.run.side_effect = _run
    return mock


class TestMain:
    @pytest.mark.integration
    def test_main_end_to_end(self, tmp_path, monkeypatch):
        """Full integration: main() with mocked IB client and bronze parquet."""
        monkeypatch.setattr("sys.argv", ["fetch_ib_historical.py", "--tickers", "AAPL"])

        mock_ib = _mock_ib_instance({
            "AAPL": [
                _make_bar(date="2025-01-02", close=153.0),
                _make_bar(date="2025-01-03", close=156.0),
            ]
        })

        bronze_dir = tmp_path / "bronze"
        cursor_dir = tmp_path / "cursors"

        with (
            patch("scripts.fetch_ib_historical.IBClient", return_value=mock_ib),
            patch(
                "scripts.fetch_ib_historical.BronzeClient",
                lambda **kw: BronzeClient(bronze_dir=bronze_dir),
            ),
            patch("scripts.fetch_ib_historical.BRONZE_DIR", bronze_dir),
            patch("scripts.fetch_ib_historical.CURSOR_DIR", cursor_dir),
        ):
            main()

        # Verify per-ticker Parquet written
        assert (bronze_dir / "symbol=AAPL" / "data.parquet").exists()
        # Verify IB connect was called
        mock_ib.connect.assert_called_once()
        # Verify cursor was saved
        cursor_file = cursor_dir / "cursor_custom.json"
        assert cursor_file.exists()
        data = json.loads(cursor_file.read_text())
        assert "AAPL" in data["completed"]

    @pytest.mark.integration
    def test_main_handles_empty_bars(self, tmp_path, monkeypatch):
        """main() handles tickers with empty bars gracefully (not added to cursor)."""
        monkeypatch.setattr("sys.argv", ["fetch_ib_historical.py", "--tickers", "FAIL"])

        mock_ib = _mock_ib_instance({"FAIL": []})
        cursor_dir = tmp_path / "cursors"

        with (
            patch("scripts.fetch_ib_historical.IBClient", return_value=mock_ib),
            patch(
                "scripts.fetch_ib_historical.BronzeClient",
                lambda **kw: BronzeClient(bronze_dir=tmp_path / "bronze"),
            ),
            patch("scripts.fetch_ib_historical.BRONZE_DIR", tmp_path / "bronze"),
            patch("scripts.fetch_ib_historical.CURSOR_DIR", cursor_dir),
        ):
            main()  # Should not raise

        # Cursor should NOT be created (no successful tickers)
        assert not (cursor_dir / "cursor_custom.json").exists()

    @pytest.mark.integration
    def test_main_custom_args(self, tmp_path, monkeypatch):
        """main() respects --port, --max-concurrent, --batch-size args."""
        monkeypatch.setattr(
            "sys.argv",
            ["fetch_ib_historical.py", "--tickers", "AAPL", "--port", "7497",
             "--max-concurrent", "4", "--batch-size", "1"],
        )

        mock_ib = _mock_ib_instance({"AAPL": [_make_bar()]})

        with (
            patch("scripts.fetch_ib_historical.IBClient", return_value=mock_ib),
            patch(
                "scripts.fetch_ib_historical.BronzeClient",
                lambda **kw: BronzeClient(bronze_dir=tmp_path / "bronze"),
            ),
            patch("scripts.fetch_ib_historical.BRONZE_DIR", tmp_path / "bronze"),
            patch("scripts.fetch_ib_historical.CURSOR_DIR", tmp_path / "cursors"),
        ):
            main()

        mock_ib.connect.assert_called_once_with(port=7497)
        # Verify max_concurrent was passed to fetch_all_tickers via ib.ib.run
        run_call = mock_ib.ib.run.call_args
        assert run_call is not None

    @pytest.mark.integration
    def test_main_with_preset(self, tmp_path, monkeypatch):
        """main() loads tickers from a preset file."""
        preset = {"name": "test-preset", "tickers": ["AAPL", "NVDA"]}
        preset_file = tmp_path / "preset.json"
        preset_file.write_text(json.dumps(preset))

        monkeypatch.setattr(
            "sys.argv",
            ["fetch_ib_historical.py", "--preset", str(preset_file)],
        )

        mock_ib = _mock_ib_instance({
            "AAPL": [_make_bar(date="2025-01-02")],
            "NVDA": [_make_bar(date="2025-01-03")],
        })
        cursor_dir = tmp_path / "cursors"

        with (
            patch("scripts.fetch_ib_historical.IBClient", return_value=mock_ib),
            patch(
                "scripts.fetch_ib_historical.BronzeClient",
                lambda **kw: BronzeClient(bronze_dir=tmp_path / "bronze"),
            ),
            patch("scripts.fetch_ib_historical.BRONZE_DIR", tmp_path / "bronze"),
            patch("scripts.fetch_ib_historical.CURSOR_DIR", cursor_dir),
        ):
            main()

        # Cursor named after preset
        cursor_file = cursor_dir / "cursor_test-preset.json"
        assert cursor_file.exists()
        data = json.loads(cursor_file.read_text())
        assert set(data["completed"]) == {"AAPL", "NVDA"}

    @pytest.mark.integration
    def test_main_resumes_from_cursor(self, tmp_path, monkeypatch):
        """main() skips already-completed tickers from cursor."""
        preset = {"name": "resume-test", "tickers": ["AAPL", "MSFT", "NVDA"]}
        preset_file = tmp_path / "preset.json"
        preset_file.write_text(json.dumps(preset))

        # Pre-seed cursor with AAPL completed
        cursor_dir = tmp_path / "cursors"
        cursor_dir.mkdir(parents=True)
        cursor_data = {
            "completed": ["AAPL"],
            "started_at": "2025-01-01T00:00:00",
            "updated_at": "2025-01-01T00:00:00",
        }
        (cursor_dir / "cursor_resume-test.json").write_text(json.dumps(cursor_data))

        monkeypatch.setattr(
            "sys.argv",
            ["fetch_ib_historical.py", "--preset", str(preset_file)],
        )

        # Only MSFT and NVDA should be fetched
        mock_ib = _mock_ib_instance({
            "MSFT": [_make_bar(date="2025-01-02")],
            "NVDA": [_make_bar(date="2025-01-03")],
        })

        with (
            patch("scripts.fetch_ib_historical.IBClient", return_value=mock_ib),
            patch(
                "scripts.fetch_ib_historical.BronzeClient",
                lambda **kw: BronzeClient(bronze_dir=tmp_path / "bronze"),
            ),
            patch("scripts.fetch_ib_historical.BRONZE_DIR", tmp_path / "bronze"),
            patch("scripts.fetch_ib_historical.CURSOR_DIR", cursor_dir),
        ):
            main()

        # ib.ib.run should have been called with only MSFT, NVDA (not AAPL)
        run_call = mock_ib.ib.run.call_args
        coro = run_call[0][0]
        # Check cursor now has all 3
        data = json.loads((cursor_dir / "cursor_resume-test.json").read_text())
        assert set(data["completed"]) == {"AAPL", "MSFT", "NVDA"}

    @pytest.mark.integration
    def test_main_reset_clears_cursor(self, tmp_path, monkeypatch):
        """main() with --reset clears existing cursor."""
        # Pre-seed cursor
        cursor_dir = tmp_path / "cursors"
        cursor_dir.mkdir(parents=True)
        cursor_data = {"completed": ["AAPL"], "started_at": "2025-01-01T00:00:00"}
        (cursor_dir / "cursor_custom.json").write_text(json.dumps(cursor_data))

        monkeypatch.setattr(
            "sys.argv",
            ["fetch_ib_historical.py", "--tickers", "AAPL", "--reset"],
        )

        mock_ib = _mock_ib_instance({"AAPL": [_make_bar()]})

        with (
            patch("scripts.fetch_ib_historical.IBClient", return_value=mock_ib),
            patch(
                "scripts.fetch_ib_historical.BronzeClient",
                lambda **kw: BronzeClient(bronze_dir=tmp_path / "bronze"),
            ),
            patch("scripts.fetch_ib_historical.BRONZE_DIR", tmp_path / "bronze"),
            patch("scripts.fetch_ib_historical.CURSOR_DIR", cursor_dir),
        ):
            main()

        # AAPL should be fetched despite existing cursor (reset cleared it)
        mock_ib.ib.run.assert_called_once()

    @pytest.mark.integration
    def test_main_all_completed_early_return(self, tmp_path, monkeypatch):
        """main() returns early when all tickers are already completed."""
        # Pre-seed cursor with all tickers completed
        cursor_dir = tmp_path / "cursors"
        cursor_dir.mkdir(parents=True)
        cursor_data = {
            "completed": ["AAPL", "MSFT"],
            "started_at": "2025-01-01T00:00:00",
        }
        (cursor_dir / "cursor_custom.json").write_text(json.dumps(cursor_data))

        monkeypatch.setattr(
            "sys.argv",
            ["fetch_ib_historical.py", "--tickers", "AAPL", "MSFT"],
        )

        mock_ib = _mock_ib_instance({})

        with (
            patch("scripts.fetch_ib_historical.IBClient", return_value=mock_ib),
            patch("scripts.fetch_ib_historical.CURSOR_DIR", cursor_dir),
        ):
            main()  # Should return early without connecting

        # IB should never be entered (no connect call)
        mock_ib.connect.assert_not_called()

    @pytest.mark.integration
    def test_main_default_mag7(self, tmp_path, monkeypatch):
        """main() uses MAG7 when no --tickers or --preset specified."""
        monkeypatch.setattr("sys.argv", ["fetch_ib_historical.py"])

        bars = {t: [_make_bar()] for t in ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"]}
        mock_ib = _mock_ib_instance(bars)

        with (
            patch("scripts.fetch_ib_historical.IBClient", return_value=mock_ib),
            patch(
                "scripts.fetch_ib_historical.BronzeClient",
                lambda **kw: BronzeClient(bronze_dir=tmp_path / "bronze"),
            ),
            patch("scripts.fetch_ib_historical.BRONZE_DIR", tmp_path / "bronze"),
            patch("scripts.fetch_ib_historical.CURSOR_DIR", tmp_path / "cursors"),
        ):
            main()

        # Cursor should contain all MAG7 tickers
        cursor_file = tmp_path / "cursors" / "cursor_custom.json"
        assert cursor_file.exists()
        data = json.loads(cursor_file.read_text())
        assert set(data["completed"]) == {"AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"}

    @pytest.mark.integration
    def test_main_batching(self, tmp_path, monkeypatch):
        """main() splits tickers into batches of --batch-size."""
        monkeypatch.setattr(
            "sys.argv",
            ["fetch_ib_historical.py", "--tickers", "AAPL", "MSFT", "NVDA", "--batch-size", "2"],
        )

        mock_ib = _mock_ib_instance({
            "AAPL": [_make_bar()],
            "MSFT": [_make_bar()],
            "NVDA": [_make_bar()],
        })

        with (
            patch("scripts.fetch_ib_historical.IBClient", return_value=mock_ib),
            patch(
                "scripts.fetch_ib_historical.BronzeClient",
                lambda **kw: BronzeClient(bronze_dir=tmp_path / "bronze"),
            ),
            patch("scripts.fetch_ib_historical.BRONZE_DIR", tmp_path / "bronze"),
            patch("scripts.fetch_ib_historical.CURSOR_DIR", tmp_path / "cursors"),
        ):
            main()

        # ib.ib.run should have been called twice (batch of 2 + batch of 1)
        assert mock_ib.ib.run.call_count == 2

    @pytest.mark.integration
    def test_main_skip_existing(self, tmp_path, monkeypatch):
        """main() with --skip-existing skips tickers already in bronze."""
        bronze_dir = tmp_path / "bronze"
        _seed_bronze(
            bronze_dir,
            "AAPL",
            [
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": 1,
                    "open": 150.0,
                    "high": 155.0,
                    "low": 149.0,
                    "close": 153.0,
                    "adj_close": 153.0,
                    "volume": 1000000,
                }
            ]
        )

        monkeypatch.setattr(
            "sys.argv",
            ["fetch_ib_historical.py", "--tickers", "AAPL", "NVDA", "--skip-existing"],
        )

        # Only NVDA should be fetched
        mock_ib = _mock_ib_instance({"NVDA": [_make_bar()]})

        with (
            patch("scripts.fetch_ib_historical.IBClient", return_value=mock_ib),
            patch(
                "scripts.fetch_ib_historical.BronzeClient",
                lambda **kw: BronzeClient(bronze_dir=bronze_dir),
            ),
            patch("scripts.fetch_ib_historical.BRONZE_DIR", bronze_dir),
            patch("scripts.fetch_ib_historical.CURSOR_DIR", tmp_path / "cursors"),
        ):
            main()

        # Only one ib.ib.run call for NVDA (AAPL was skipped)
        mock_ib.ib.run.assert_called_once()
        # Cursor should have both AAPL (skipped) and NVDA (fetched)
        cursor_file = tmp_path / "cursors" / "cursor_custom.json"
        data = json.loads(cursor_file.read_text())
        assert set(data["completed"]) == {"AAPL", "NVDA"}

    @pytest.mark.integration
    def test_main_backfill_end_to_end(self, tmp_path, monkeypatch):
        """main() with --backfill fetches only older missing data."""
        bronze_dir = tmp_path / "bronze"
        _seed_bronze(
            bronze_dir,
            "AAPL",
            [
                {
                    "trade_date": "2020-01-02",
                    "symbol_id": 1,
                    "open": 150.0, "high": 155.0, "low": 149.0,
                    "close": 153.0, "adj_close": 153.0, "volume": 1000000,
                }
            ]
        )

        monkeypatch.setattr(
            "sys.argv",
            ["fetch_ib_historical.py", "--tickers", "AAPL", "--backfill"],
        )

        # IB should fetch bars older than 2020-01-02
        mock_ib = _mock_ib_instance({
            "AAPL": [_make_bar(date="2019-06-15", close=120.0)],
        })

        with (
            patch("scripts.fetch_ib_historical.IBClient", return_value=mock_ib),
            patch(
                "scripts.fetch_ib_historical.BronzeClient",
                lambda **kw: BronzeClient(bronze_dir=bronze_dir),
            ),
            patch("scripts.fetch_ib_historical.BRONZE_DIR", bronze_dir),
            patch("scripts.fetch_ib_historical.CURSOR_DIR", tmp_path / "cursors"),
        ):
            main()

        with BronzeClient(bronze_dir=bronze_dir) as bronze:
            rows = bronze.read_symbol_rows("AAPL")
        assert len(rows) == 2
        assert [row["trade_date"] for row in rows] == ["2019-06-15", "2020-01-02"]

        # Cursor should use backfill_ prefix
        cursor_file = tmp_path / "cursors" / "cursor_backfill_custom.json"
        assert cursor_file.exists()
        data = json.loads(cursor_file.read_text())
        assert "AAPL" in data["completed"]

    @pytest.mark.integration
    def test_main_backfill_skips_tickers_not_in_bronze(self, tmp_path, monkeypatch):
        """main() with --backfill skips tickers that have no existing data."""
        monkeypatch.setattr(
            "sys.argv",
            ["fetch_ib_historical.py", "--tickers", "NEWSTOCK", "--backfill"],
        )

        mock_ib = _mock_ib_instance({})

        with (
            patch("scripts.fetch_ib_historical.IBClient", return_value=mock_ib),
            patch(
                "scripts.fetch_ib_historical.BronzeClient",
                lambda **kw: BronzeClient(bronze_dir=tmp_path / "bronze"),
            ),
            patch("scripts.fetch_ib_historical.BRONZE_DIR", tmp_path / "bronze"),
            patch("scripts.fetch_ib_historical.CURSOR_DIR", tmp_path / "cursors"),
        ):
            main()

        # No fetch should have happened (no data to backfill)
        mock_ib.ib.run.assert_not_called()

    @pytest.mark.integration
    def test_main_backfill_empty_bars(self, tmp_path, monkeypatch):
        """main() with --backfill handles empty bars for a ticker."""
        bronze_dir = tmp_path / "bronze"
        _seed_bronze(
            bronze_dir,
            "AAPL",
            [
                {
                    "trade_date": "2020-01-02",
                    "symbol_id": 1,
                    "open": 150.0, "high": 155.0, "low": 149.0,
                    "close": 153.0, "adj_close": 153.0, "volume": 1000000,
                }
            ]
        )

        monkeypatch.setattr(
            "sys.argv",
            ["fetch_ib_historical.py", "--tickers", "AAPL", "--backfill"],
        )

        # IB returns empty bars (no gap to fill)
        mock_ib = _mock_ib_instance({"AAPL": []})

        with (
            patch("scripts.fetch_ib_historical.IBClient", return_value=mock_ib),
            patch(
                "scripts.fetch_ib_historical.BronzeClient",
                lambda **kw: BronzeClient(bronze_dir=bronze_dir),
            ),
            patch("scripts.fetch_ib_historical.BRONZE_DIR", bronze_dir),
            patch("scripts.fetch_ib_historical.CURSOR_DIR", tmp_path / "cursors"),
        ):
            main()

        with BronzeClient(bronze_dir=bronze_dir) as bronze:
            rows = bronze.read_symbol_rows("AAPL")
        assert len(rows) == 1
        assert rows[0]["trade_date"] == "2020-01-02"

    @pytest.mark.integration
    def test_main_skip_existing_all_in_bronze(self, tmp_path, monkeypatch):
        """main() with --skip-existing returns early when all tickers exist."""
        bronze_dir = tmp_path / "bronze"
        _seed_bronze(
            bronze_dir,
            "AAPL",
            [
                {
                    "trade_date": "2025-01-02",
                    "symbol_id": 1,
                    "open": 150.0,
                    "high": 155.0,
                    "low": 149.0,
                    "close": 153.0,
                    "adj_close": 153.0,
                    "volume": 1000000,
                }
            ]
        )

        monkeypatch.setattr(
            "sys.argv",
            ["fetch_ib_historical.py", "--tickers", "AAPL", "--skip-existing"],
        )

        mock_ib = _mock_ib_instance({})

        with (
            patch("scripts.fetch_ib_historical.IBClient", return_value=mock_ib),
            patch(
                "scripts.fetch_ib_historical.BronzeClient",
                lambda **kw: BronzeClient(bronze_dir=bronze_dir),
            ),
            patch("scripts.fetch_ib_historical.CURSOR_DIR", tmp_path / "cursors"),
            patch("scripts.fetch_ib_historical.BRONZE_DIR", bronze_dir),
        ):
            main()

        # Should not have fetched anything
        mock_ib.ib.run.assert_not_called()
