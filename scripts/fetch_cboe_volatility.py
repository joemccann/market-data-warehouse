#!/usr/bin/env python3
"""Fetch CBOE volatility index historical data directly from CBOE's API.

Primary daily sync source for all CBOE volatility indices. Also used for
historical backfill of indices not available via IB (e.g., VXHYG, VXSMH).
Writes to bronze parquet in the standard warehouse format.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from rich.console import Console

console = Console()

CBOE_HISTORICAL_URL = "https://cdn.cboe.com/api/global/delayed_quotes/charts/historical/_{symbol}.json"

DEFAULT_WAREHOUSE = Path.home() / "market-warehouse"
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_PRESET = SCRIPT_DIR.parent / "presets" / "volatility.json"
ASSET_CLASS = "volatility"


def _symbol_id(symbol: str) -> int:
    """Generate a stable numeric ID from symbol string."""
    h = hashlib.sha256(symbol.encode()).hexdigest()
    return int(h[:14], 16)


def fetch_cboe_historical(symbol: str) -> list[dict[str, Any]]:
    """Fetch historical OHLCV data from CBOE's public API."""
    url = CBOE_HISTORICAL_URL.format(symbol=symbol)
    console.print(f"  Fetching {symbol} from {url}")
    
    resp = httpx.get(url, timeout=30)
    resp.raise_for_status()
    
    data = resp.json()
    bars = data.get("data", [])
    console.print(f"  {symbol}: received {len(bars)} bars")
    return bars


def bars_to_table(symbol: str, bars: list[dict[str, Any]]) -> pa.Table:
    """Convert CBOE JSON bars to PyArrow table matching bronze schema.
    
    Note: asset_class and symbol are NOT included in the parquet file;
    they're encoded in the hive partition path (asset_class=X/symbol=Y/).
    """
    if not bars:
        return None
    
    symbol_id = _symbol_id(symbol)
    
    records = []
    for bar in bars:
        records.append({
            "trade_date": date.fromisoformat(bar["date"]),
            "symbol_id": symbol_id,
            "open": float(bar["open"]),
            "high": float(bar["high"]),
            "low": float(bar["low"]),
            "close": float(bar["close"]),
            "adj_close": float(bar["close"]),  # No adjustment for indices
            "volume": int(float(bar["volume"])),
        })
    
    schema = pa.schema([
        ("trade_date", pa.date32()),
        ("symbol_id", pa.int64()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("adj_close", pa.float64()),
        ("volume", pa.int64()),
    ])
    
    return pa.Table.from_pylist(records, schema=schema)


def write_bronze_parquet(
    table: pa.Table,
    symbol: str,
    warehouse_dir: Path,
) -> Path:
    """Write table to bronze parquet, merging with existing data."""
    bronze_dir = warehouse_dir / "data-lake" / "bronze" / f"asset_class={ASSET_CLASS}" / f"symbol={symbol}"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = bronze_dir / "data.parquet"
    
    # Merge with existing data if present
    if parquet_path.exists():
        existing = pq.ParquetFile(parquet_path).read()
        existing_dates = set(
            d.as_py() for d in existing.column("trade_date")
        )
        
        # Filter to only new dates
        new_dates_mask = pa.compute.invert(
            pa.compute.is_in(
                table.column("trade_date"),
                pa.array(list(existing_dates), type=pa.date32()),
            )
        )
        new_rows = table.filter(new_dates_mask)
        
        if new_rows.num_rows > 0:
            table = pa.concat_tables([existing, new_rows])
            console.print(f"  {symbol}: merged {new_rows.num_rows} new rows with {existing.num_rows} existing")
        else:
            console.print(f"  {symbol}: no new rows to add")
            return parquet_path
    
    # Sort by date
    indices = pa.compute.sort_indices(table, sort_keys=[("trade_date", "ascending")])
    table = table.take(indices)
    
    pq.write_table(table, parquet_path)
    console.print(f"  {symbol}: wrote {table.num_rows} rows to {parquet_path}")
    return parquet_path


def load_preset(preset_path: Path) -> list[str]:
    """Load ticker symbols from a preset JSON file."""
    with preset_path.open() as f:
        data = json.load(f)
    return data.get("tickers", [])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--symbols",
        nargs="+",
        help="CBOE volatility index symbols to fetch",
    )
    group.add_argument(
        "--preset",
        type=Path,
        help=f"Path to preset JSON file (default: {DEFAULT_PRESET})",
    )
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=DEFAULT_WAREHOUSE,
        help=f"Warehouse directory (default: {DEFAULT_WAREHOUSE})",
    )
    args = parser.parse_args()
    
    # Determine symbols to fetch
    if args.symbols:
        symbols = args.symbols
    elif args.preset:
        symbols = load_preset(args.preset)
    elif DEFAULT_PRESET.exists():
        symbols = load_preset(DEFAULT_PRESET)
    else:
        symbols = ["VIX", "VVIX"]  # Minimal fallback
    
    console.print(f"\n[bold]Fetching CBOE volatility indices: {symbols}[/bold]\n")
    
    for symbol in symbols:
        try:
            bars = fetch_cboe_historical(symbol)
            if not bars:
                console.print(f"  [yellow]{symbol}: no data returned[/yellow]")
                continue
            
            table = bars_to_table(symbol, bars)
            write_bronze_parquet(table, symbol, args.warehouse)
            
            # Show date range
            dates = [date.fromisoformat(b["date"]) for b in bars]
            console.print(f"  {symbol}: {min(dates)} → {max(dates)}\n")
            
        except Exception as e:
            console.print(f"  [red]{symbol}: error - {e}[/red]")
    
    console.print("[bold green]Done.[/bold green]")


if __name__ == "__main__":
    main()
