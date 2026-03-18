"""Tests for CBOE volatility index fetcher."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow.parquet as pq
import pytest

from scripts.fetch_cboe_volatility import (
    _symbol_id,
    bars_to_table,
    fetch_cboe_historical,
    load_preset,
    write_bronze_parquet,
)


class TestLoadPreset:
    def test_loads_tickers_from_preset(self, tmp_path):
        preset = tmp_path / "test.json"
        preset.write_text('{"tickers": ["VIX", "VVIX", "VXHYG"]}')
        
        symbols = load_preset(preset)
        assert symbols == ["VIX", "VVIX", "VXHYG"]

    def test_returns_empty_list_if_no_tickers(self, tmp_path):
        preset = tmp_path / "test.json"
        preset.write_text('{"name": "test"}')
        
        symbols = load_preset(preset)
        assert symbols == []


class TestSymbolId:
    def test_stable_hash(self):
        """Symbol ID should be stable across calls."""
        id1 = _symbol_id("VXHYG")
        id2 = _symbol_id("VXHYG")
        assert id1 == id2

    def test_different_symbols_different_ids(self):
        """Different symbols should have different IDs."""
        assert _symbol_id("VXHYG") != _symbol_id("VXSMH")


class TestFetchCboeHistorical:
    def test_fetch_success(self):
        """Successful API call returns bars."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [
                {"date": "2025-01-02", "open": "10.0", "high": "11.0", "low": "9.0", "close": "10.5", "volume": "0.0"},
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch("scripts.fetch_cboe_volatility.httpx.get", return_value=mock_response):
            bars = fetch_cboe_historical("VXHYG")

        assert len(bars) == 1
        assert bars[0]["date"] == "2025-01-02"

    def test_fetch_empty_data(self):
        """Empty data returns empty list."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": []}
        mock_response.raise_for_status = MagicMock()

        with patch("scripts.fetch_cboe_volatility.httpx.get", return_value=mock_response):
            bars = fetch_cboe_historical("UNKNOWN")

        assert bars == []


class TestBarsToTable:
    def test_converts_bars_to_table(self):
        """JSON bars are converted to PyArrow table."""
        bars = [
            {"date": "2025-01-02", "open": "10.0", "high": "11.0", "low": "9.0", "close": "10.5", "volume": "0.0"},
            {"date": "2025-01-03", "open": "10.5", "high": "12.0", "low": "10.0", "close": "11.0", "volume": "0.0"},
        ]
        table = bars_to_table("VXHYG", bars)

        assert table.num_rows == 2
        # asset_class and symbol are in hive partition path, not in parquet
        assert set(table.column_names) == {
            "trade_date", "symbol_id", "open", "high", "low",
            "close", "adj_close", "volume"
        }
        assert table.column("close")[0].as_py() == 10.5

    def test_empty_bars_returns_none(self):
        """Empty bars list returns None."""
        assert bars_to_table("VXHYG", []) is None


def _read_single_parquet(path: Path):
    """Read a single parquet file without hive partitioning discovery."""
    return pq.ParquetFile(path).read()


class TestWriteBronzeParquet:
    def test_writes_new_file(self, tmp_path):
        """Creates new parquet file when none exists."""
        bars = [
            {"date": "2025-01-02", "open": "10.0", "high": "11.0", "low": "9.0", "close": "10.5", "volume": "0.0"},
        ]
        table = bars_to_table("VXHYG", bars)
        
        path = write_bronze_parquet(table, "VXHYG", tmp_path)
        
        assert path.exists()
        read_table = _read_single_parquet(path)
        assert read_table.num_rows == 1

    def test_merges_with_existing(self, tmp_path):
        """Merges new data with existing parquet file."""
        # Write initial data
        bars1 = [
            {"date": "2025-01-02", "open": "10.0", "high": "11.0", "low": "9.0", "close": "10.5", "volume": "0.0"},
        ]
        table1 = bars_to_table("VXHYG", bars1)
        write_bronze_parquet(table1, "VXHYG", tmp_path)

        # Write overlapping + new data
        bars2 = [
            {"date": "2025-01-02", "open": "10.0", "high": "11.0", "low": "9.0", "close": "10.5", "volume": "0.0"},  # duplicate
            {"date": "2025-01-03", "open": "10.5", "high": "12.0", "low": "10.0", "close": "11.0", "volume": "0.0"},  # new
        ]
        table2 = bars_to_table("VXHYG", bars2)
        path = write_bronze_parquet(table2, "VXHYG", tmp_path)

        read_table = _read_single_parquet(path)
        assert read_table.num_rows == 2  # Only 2 unique dates

    def test_sorts_by_date(self, tmp_path):
        """Output is sorted by trade_date ascending."""
        bars = [
            {"date": "2025-01-05", "open": "10.0", "high": "11.0", "low": "9.0", "close": "10.5", "volume": "0.0"},
            {"date": "2025-01-02", "open": "10.0", "high": "11.0", "low": "9.0", "close": "10.5", "volume": "0.0"},
            {"date": "2025-01-10", "open": "10.0", "high": "11.0", "low": "9.0", "close": "10.5", "volume": "0.0"},
        ]
        table = bars_to_table("VXHYG", bars)
        path = write_bronze_parquet(table, "VXHYG", tmp_path)

        read_table = _read_single_parquet(path)
        dates = [d.as_py() for d in read_table.column("trade_date")]
        assert dates == sorted(dates)
