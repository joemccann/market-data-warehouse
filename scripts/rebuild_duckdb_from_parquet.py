#!/usr/bin/env python3
"""Rebuild the DuckDB analytical file from canonical bronze parquet."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from rich.console import Console
from rich.logging import RichHandler

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from clients.db_client import DBClient

DATA_LAKE = Path.home() / "market-warehouse" / "data-lake"
DEFAULT_BRONZE_DIR = DATA_LAKE / "bronze" / "asset_class=equity"
DEFAULT_DB_PATH = Path.home() / "market-warehouse" / "duckdb" / "market.duckdb"

VENUE_MAP = {"equity": "SMART", "volatility": "CBOE", "futures": "CME"}

console = Console()


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild market.duckdb from bronze parquet")
    parser.add_argument(
        "--bronze-dir",
        type=Path,
        default=None,
        help=f"Bronze parquet root (default: derived from --asset-class)",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"DuckDB path to rebuild (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--asset-class",
        choices=["equity", "volatility", "futures"],
        default="equity",
        help="Asset class to rebuild (default: equity)",
    )
    args = parser.parse_args()

    if args.bronze_dir is None:
        args.bronze_dir = DATA_LAKE / "bronze" / f"asset_class={args.asset_class}"

    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )

    if not args.bronze_dir.exists():
        raise FileNotFoundError(f"bronze directory does not exist: {args.bronze_dir}")
    if not any(args.bronze_dir.glob("symbol=*/data.parquet")):
        raise FileNotFoundError(f"no bronze parquet snapshots found under: {args.bronze_dir}")

    args.db_path.parent.mkdir(parents=True, exist_ok=True)
    venue = VENUE_MAP[args.asset_class]

    with DBClient(db_path=args.db_path) as db:
        if args.asset_class == "futures":
            counts = db.replace_futures_from_parquet(args.bronze_dir)
            console.print(
                f"[green]Rebuilt[/green] {args.db_path} from {args.bronze_dir}"
                f" with {counts['rows']:,} futures rows"
            )
        else:
            counts = db.replace_equities_from_parquet(
                args.bronze_dir, asset_class=args.asset_class, venue=venue,
            )
            console.print(
                f"[green]Rebuilt[/green] {args.db_path} from {args.bronze_dir}"
                f" with {counts['symbols']:,} symbols and {counts['rows']:,} rows"
            )


if __name__ == "__main__":
    main()
