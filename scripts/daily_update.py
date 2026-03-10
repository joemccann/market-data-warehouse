#!/usr/bin/env python3
"""Daily market data update — append latest bars for tickers in bronze parquet.

Lightweight alternative to fetch_ib_historical.py for daily scheduled runs.
Discovers tickers from bronze parquet, detects gaps, fetches only missing bars,
uses a narrow public fallback chain for unresolved trading dates after IB, and
atomically rewrites per-ticker snapshots.

Requires IB Gateway or TWS running on localhost.

Usage:
    source ~/market-warehouse/.venv/bin/activate

    # Normal daily run (discovers all tickers from bronze parquet):
    python scripts/daily_update.py

    # Dry-run — show gap report without fetching:
    python scripts/daily_update.py --dry-run

    # Force run on a non-trading day (e.g., manual catch-up):
    python scripts/daily_update.py --force

    # Limit to a specific preset:
    python scripts/daily_update.py --preset presets/sp500.json

    # Custom IB port and concurrency:
    python scripts/daily_update.py --port 7497 --max-concurrent 4
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from ib_insync import Stock
from rich.console import Console
from rich.logging import RichHandler

# Add project root to path so clients module is importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from clients.bronze_client import BronzeClient
from clients.daily_bar_fallback import DailyBarFallbackClient
from clients.ib_client import IBClient, IBError

_DEFAULT_STORAGE_CLIENT = BronzeClient
DBClient = BronzeClient


def _storage_client():
    """Return the live storage client, allowing tests to patch either name."""
    if BronzeClient is not _DEFAULT_STORAGE_CLIENT:
        return BronzeClient
    if DBClient is not _DEFAULT_STORAGE_CLIENT:
        return DBClient
    return BronzeClient


_DEFAULT_FALLBACK_CLIENT = DailyBarFallbackClient
FallbackClient = DailyBarFallbackClient


def _fallback_client():
    """Return the live fallback client, allowing tests to patch either name."""
    if DailyBarFallbackClient is not _DEFAULT_FALLBACK_CLIENT:
        return DailyBarFallbackClient()
    if FallbackClient is not _DEFAULT_FALLBACK_CLIENT:
        return FallbackClient()
    return DailyBarFallbackClient()

# ── Config ─────────────────────────────────────────────────────────────

DATA_LAKE = Path.home() / "market-warehouse" / "data-lake"
BRONZE_DIR = DATA_LAKE / "bronze" / "asset_class=equity"

console = Console()

# ── Trading calendar ───────────────────────────────────────────────────


def get_nyse_holidays(year: int) -> set[date]:
    """Compute NYSE observed holidays for *year*.

    Covers: New Year's, MLK Day, Presidents Day, Good Friday,
    Memorial Day, Juneteenth, Independence Day, Labor Day,
    Thanksgiving, Christmas. Applies weekend-observed rules.
    """
    holidays: set[date] = set()

    def _observed(d: date) -> date:
        """Shift Saturday→Friday, Sunday→Monday for observed holidays."""
        if d.weekday() == 5:  # Saturday
            return d - timedelta(days=1)
        if d.weekday() == 6:  # Sunday
            return d + timedelta(days=1)
        return d

    # New Year's Day
    holidays.add(_observed(date(year, 1, 1)))

    # MLK Day — 3rd Monday of January
    jan1 = date(year, 1, 1)
    first_monday = jan1 + timedelta(days=(7 - jan1.weekday()) % 7)
    mlk = first_monday + timedelta(weeks=2)
    holidays.add(mlk)

    # Presidents Day — 3rd Monday of February
    feb1 = date(year, 2, 1)
    first_monday_feb = feb1 + timedelta(days=(7 - feb1.weekday()) % 7)
    presidents = first_monday_feb + timedelta(weeks=2)
    holidays.add(presidents)

    # Good Friday — 2 days before Easter Sunday
    holidays.add(_easter(year) - timedelta(days=2))

    # Memorial Day — last Monday of May
    may31 = date(year, 5, 31)
    memorial = may31 - timedelta(days=(may31.weekday()) % 7)
    holidays.add(memorial)

    # Juneteenth — observed since 2022
    if year >= 2021:
        holidays.add(_observed(date(year, 6, 19)))

    # Independence Day
    holidays.add(_observed(date(year, 7, 4)))

    # Labor Day — 1st Monday of September
    sep1 = date(year, 9, 1)
    labor = sep1 + timedelta(days=(7 - sep1.weekday()) % 7)
    holidays.add(labor)

    # Thanksgiving — 4th Thursday of November
    nov1 = date(year, 11, 1)
    first_thu = nov1 + timedelta(days=(3 - nov1.weekday()) % 7)
    thanksgiving = first_thu + timedelta(weeks=3)
    holidays.add(thanksgiving)

    # Christmas
    holidays.add(_observed(date(year, 12, 25)))

    return holidays


def _easter(year: int) -> date:
    """Compute Easter Sunday using the Anonymous Gregorian algorithm."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7  # noqa: E741
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return date(year, month, day + 1)


def is_trading_day(d: date) -> bool:
    """Return True if *d* is a NYSE trading day (not weekend, not holiday)."""
    if d.weekday() >= 5:
        return False
    return d not in get_nyse_holidays(d.year)


def previous_trading_day(d: date) -> date:
    """Walk backwards from *d* to find the most recent trading day."""
    d = d - timedelta(days=1)
    while not is_trading_day(d):
        d = d - timedelta(days=1)
    return d


def trading_days_between(start: date, end: date) -> int:
    """Count trading days in the half-open range (start, end]."""
    count = 0
    d = start + timedelta(days=1)
    while d <= end:
        if is_trading_day(d):
            count += 1
        d += timedelta(days=1)
    return count


# ── Gap detection ──────────────────────────────────────────────────────


def classify_gaps(
    latest_dates: dict[str, str], target_date: date
) -> tuple[list[str], list[str], list[str]]:
    """Classify tickers into up_to_date, single_day_gap, multi_day_gap.

    Returns (up_to_date, single_day_gap, multi_day_gap).
    """
    up_to_date: list[str] = []
    single_day_gap: list[str] = []
    multi_day_gap: list[str] = []

    for symbol, latest_str in latest_dates.items():
        latest = date.fromisoformat(latest_str)
        gap = trading_days_between(latest, target_date)
        if gap == 0:
            up_to_date.append(symbol)
        elif gap == 1:
            single_day_gap.append(symbol)
        else:
            multi_day_gap.append(symbol)

    return (up_to_date, single_day_gap, multi_day_gap)


def compute_ib_duration(latest_date: date, target_date: date) -> str:
    """Compute the IB duration string to fetch bars from *latest_date* to *target_date*.

    Returns e.g. "5 D", "1 M", "3 M", "1 Y".
    """
    cal_days = (target_date - latest_date).days
    if cal_days <= 0:
        return "1 D"
    # Add a small buffer for safety
    cal_days += 2
    if cal_days <= 180:
        return f"{cal_days} D"
    elif cal_days <= 365:
        return "1 Y"
    else:
        return "2 Y"


def get_missing_trading_dates(
    latest_date: date,
    target_date: date,
    bars: list,
) -> list[date]:
    """Return unresolved trading dates in ``(latest_date, target_date]``."""
    covered = {
        date.fromisoformat(str(bar.date))
        for bar in bars
        if latest_date < date.fromisoformat(str(bar.date)) <= target_date
    }

    missing: list[date] = []
    cursor = latest_date + timedelta(days=1)
    while cursor <= target_date:
        if is_trading_day(cursor) and cursor not in covered:
            missing.append(cursor)
        cursor += timedelta(days=1)
    return missing


# ── Bar validation ─────────────────────────────────────────────────────


def validate_bars(
    bars: list, ticker: str
) -> tuple[list, list[str]]:
    """Validate bar data quality. Returns (valid_bars, issues).

    Checks: non-null OHLCV, high >= low, high >= open/close,
    low <= open/close, volume >= 0, positive open/close,
    valid trading day, no duplicate dates.
    """
    valid: list = []
    issues: list[str] = []
    seen_dates: set[str] = set()

    for bar in bars:
        bar_date = str(bar.date)
        problems: list[str] = []

        # Duplicate date check
        if bar_date in seen_dates:
            problems.append(f"duplicate date {bar_date}")
        seen_dates.add(bar_date)

        # Null checks
        for field in ("open", "high", "low", "close", "volume"):
            if getattr(bar, field, None) is None:
                problems.append(f"{field} is null")

        if not problems:  # Only check relationships if fields are present
            if bar.high < bar.low:
                problems.append(f"high ({bar.high}) < low ({bar.low})")
            if bar.high < bar.open:
                problems.append(f"high ({bar.high}) < open ({bar.open})")
            if bar.high < bar.close:
                problems.append(f"high ({bar.high}) < close ({bar.close})")
            if bar.low > bar.open:
                problems.append(f"low ({bar.low}) > open ({bar.open})")
            if bar.low > bar.close:
                problems.append(f"low ({bar.low}) > close ({bar.close})")
            if bar.volume < 0:
                problems.append(f"negative volume ({bar.volume})")
            if bar.open <= 0:
                problems.append(f"non-positive open ({bar.open})")
            if bar.close <= 0:
                problems.append(f"non-positive close ({bar.close})")

            # Trading day check
            try:
                bar_d = date.fromisoformat(bar_date)
                if not is_trading_day(bar_d):
                    problems.append(f"{bar_date} is not a trading day")
            except ValueError:
                problems.append(f"invalid date format: {bar_date}")

        if problems:
            issues.append(f"{ticker} {bar_date}: {'; '.join(problems)}")
        else:
            valid.append(bar)

    return (valid, issues)


# ── Transform (reused from fetch_ib_historical) ───────────────────────


def bars_to_rows(bars: list, symbol_id: int) -> list[dict]:
    """Convert IB BarData objects to bronze row dicts."""
    rows = []
    for bar in bars:
        rows.append(
            {
                "trade_date": str(bar.date),
                "symbol_id": symbol_id,
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "adj_close": float(bar.close),
                "volume": int(bar.volume),
            }
        )
    return rows


def fetch_fallback_bars(
    ticker: str,
    missing_dates: list[date],
    fallback_client,
) -> tuple[list, list[str]]:
    """Fetch fallback bars for unresolved trading dates."""
    bars: list = []
    sources: list[str] = []

    for trade_date in missing_dates:
        fallback_bar = fallback_client.get_daily_bar(ticker, trade_date)
        if fallback_bar is None:
            continue
        bars.append(fallback_bar)
        sources.append(fallback_bar.source)

    return (bars, sources)


# ── Async fetching ─────────────────────────────────────────────────────


async def fetch_ticker_update(
    ticker: str,
    duration: str,
    ib: IBClient,
    semaphore: asyncio.Semaphore,
) -> tuple[str, list]:
    """Fetch daily bars for *ticker* with the given *duration*.

    Returns ``(ticker, bars)``.
    """
    contract = Stock(ticker, "SMART", "USD")
    async with semaphore:
        await ib.ib.qualifyContractsAsync(contract)
        bars = await ib.get_historical_data_async(
            contract,
            duration=duration,
            bar_size="1 day",
            what_to_show="TRADES",
        )
    return (ticker, bars if bars else [])


async def fetch_batch(
    tickers_with_durations: list[tuple[str, str]],
    ib: IBClient,
    max_concurrent: int = 6,
) -> dict[str, list]:
    """Fetch bars for a batch of tickers. Returns ``{ticker: bars}``."""
    semaphore = asyncio.Semaphore(max_concurrent)
    results: dict[str, list] = {}

    async def _safe_fetch(ticker: str, duration: str) -> tuple[str, list]:
        try:
            return await fetch_ticker_update(ticker, duration, ib, semaphore)
        except (IBError, Exception) as exc:
            console.print(f"    [red]{ticker}: {type(exc).__name__} — {exc}[/red]")
            return (ticker, [])

    gathered = await asyncio.gather(
        *[_safe_fetch(t, d) for t, d in tickers_with_durations]
    )
    for ticker, bars in gathered:
        results[ticker] = bars

    return results


# ── Preset loading ─────────────────────────────────────────────────────


def load_preset(path: str | Path) -> tuple[str, list[str]]:
    """Read a preset JSON file and return ``(name, tickers)``."""
    p = Path(path)
    with p.open() as f:
        data = json.load(f)
    return (data["name"], data["tickers"])


# ── Main ───────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Daily market data update")
    parser.add_argument(
        "--port", type=int, default=4001,
        help="IB Gateway port (default: 4001)",
    )
    parser.add_argument(
        "--max-concurrent", type=int, default=6,
        help="Max concurrent IB requests (default: 6)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=50,
        help="Tickers per async batch (default: 50)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Report gaps without fetching",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Run even if not a trading day",
    )
    parser.add_argument(
        "--preset", type=str, default=None,
        help="Limit to tickers in a specific preset file",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )

    today = date.today()

    # ── Trading day check ───────────────────────────────────────────
    if not args.force and not is_trading_day(today):
        console.print(f"[yellow]{today} is not a trading day. Use --force to override.[/yellow]")
        return

    target = today if is_trading_day(today) else previous_trading_day(today)
    console.print(f"\n[bold]Daily Update[/bold]  target_date={target}  force={args.force}")

    # ── Load preset filter (if any) ─────────────────────────────────
    preset_tickers: set[str] | None = None
    if args.preset:
        preset_name, preset_list = load_preset(args.preset)
        preset_tickers = set(preset_list)
        console.print(f"[bold]Preset:[/bold] {preset_name} ({len(preset_tickers)} tickers)")

    # ── Gap detection ───────────────────────────────────────────────
    with _storage_client()(bronze_dir=BRONZE_DIR) as bronze:
        latest_dates = bronze.get_latest_dates()

        if not latest_dates:
            console.print(
                "[yellow]No tickers found in bronze parquet. Run fetch_ib_historical.py first.[/yellow]"
            )
            return

        # Filter to preset tickers if specified
        if preset_tickers is not None:
            latest_dates = {k: v for k, v in latest_dates.items() if k in preset_tickers}
            if not latest_dates:
                console.print("[yellow]No preset tickers found in bronze parquet.[/yellow]")
                return

        up_to_date, single_gap, multi_gap = classify_gaps(latest_dates, target)
        need_update = single_gap + multi_gap

        console.print(f"\n[bold]Gap Report ({len(latest_dates)} tickers):[/bold]")
        console.print(f"  Up to date: {len(up_to_date)}")
        console.print(f"  Single-day gap: {len(single_gap)}")
        console.print(f"  Multi-day gap:  {len(multi_gap)}")

        if not need_update:
            console.print("\n[green bold]All tickers up to date.[/green bold]\n")
            return

        if args.dry_run:
            console.print("\n[bold]Dry run — tickers needing update:[/bold]")
            for ticker in sorted(need_update):
                latest = latest_dates[ticker]
                gap = trading_days_between(date.fromisoformat(latest), target)
                console.print(f"  {ticker:6s}  latest={latest}  gap={gap} trading days")
            console.print(f"\n[yellow]Dry run complete. {len(need_update)} tickers need updating.[/yellow]\n")
            return

        # ── Build fetch plan ────────────────────────────────────────
        tickers_with_durations: list[tuple[str, str]] = []
        for ticker in need_update:
            latest = date.fromisoformat(latest_dates[ticker])
            duration = compute_ib_duration(latest, target)
            tickers_with_durations.append((ticker, duration))

        # ── Fetch and insert ────────────────────────────────────────
        total_inserted = 0
        total_validated = 0
        total_issues: list[str] = []
        tickers_updated = 0
        tickers_failed = 0
        fallback_attempts = 0
        fallback_successes = 0
        fallback_symbols = 0

        with IBClient() as ib, _fallback_client() as fallback:
            ib.connect(port=args.port)

            batches = [
                tickers_with_durations[i:i + args.batch_size]
                for i in range(0, len(tickers_with_durations), args.batch_size)
            ]

            for batch_idx, batch in enumerate(batches):
                console.print(
                    f"\n[bold]Batch {batch_idx + 1}/{len(batches)}"
                    f" ({len(batch)} tickers)[/bold]"
                )

                ticker_bars = ib.ib.run(
                    fetch_batch(batch, ib, max_concurrent=args.max_concurrent)
                )

                for ticker, duration in batch:
                    bars = ticker_bars.get(ticker, [])
                    valid_bars, issues = validate_bars(bars, ticker)
                    total_issues.extend(issues)
                    total_validated += len(bars)

                    # Filter to only bars after the latest parquet date
                    latest = date.fromisoformat(latest_dates[ticker])
                    valid_bars = [
                        b for b in valid_bars
                        if date.fromisoformat(str(b.date)) > latest
                    ]

                    missing_dates = get_missing_trading_dates(latest, target, valid_bars)
                    fallback_attempts += len(missing_dates)
                    fallback_bars, fallback_sources = fetch_fallback_bars(
                        ticker, missing_dates, fallback,
                    )
                    if fallback_bars:
                        recovered_bars, fallback_issues = validate_bars(fallback_bars, ticker)
                        total_issues.extend(fallback_issues)
                        total_validated += len(fallback_bars)
                        if recovered_bars:
                            valid_bars.extend(recovered_bars)
                            fallback_successes += len(recovered_bars)
                            fallback_symbols += 1
                            console.print(
                                f"  [cyan]{ticker}[/cyan]: recovered "
                                f"{len(recovered_bars)} missing trading day"
                                f"{'s' if len(recovered_bars) != 1 else ''} via "
                                f"{', '.join(sorted(set(fallback_sources)))}"
                            )

                    if not valid_bars:
                        if bars:
                            console.print(
                                f"  [yellow]{ticker}[/yellow]: no valid target bar from IB or fallback"
                            )
                        else:
                            console.print(
                                f"  [yellow]{ticker}[/yellow]: no bars from IB and no fallback bar"
                            )
                        tickers_failed += 1
                        continue

                    symbol_id = bronze.get_symbol_id(ticker)
                    rows = bars_to_rows(valid_bars, symbol_id)
                    inserted = bronze.merge_ticker_rows(ticker, rows)
                    if hasattr(bronze, "write_ticker_parquet"):
                        bronze.write_ticker_parquet(ticker, symbol_id, BRONZE_DIR)
                    remaining_dates = get_missing_trading_dates(latest, target, valid_bars)
                    total_inserted += inserted

                    if remaining_dates:
                        console.print(
                            f"  [yellow]{ticker}[/yellow]: "
                            f"{inserted} bar{'s' if inserted != 1 else ''} published, "
                            f"still missing {', '.join(d.isoformat() for d in remaining_dates)}"
                        )
                        tickers_failed += 1
                        continue

                    tickers_updated += 1
                    console.print(
                        f"  [green]{ticker}[/green]: {inserted} bar{'s' if inserted != 1 else ''} published"
                    )

    # ── Summary ─────────────────────────────────────────────────────
    console.print(f"\n{'═' * 60}")
    console.print(f"[bold]Daily Update Complete[/bold]")
    console.print(f"  Tickers updated:    {tickers_updated}")
    console.print(f"  Tickers failed:     {tickers_failed}")
    console.print(f"  Fallback attempts:  {fallback_attempts}")
    console.print(f"  Fallback successes: {fallback_successes}")
    console.print(f"  Fallback symbols:   {fallback_symbols}")
    console.print(f"  Bars inserted:      {total_inserted}")
    console.print(f"  Bars validated:     {total_validated}")
    console.print(f"  Validation issues:  {len(total_issues)}")
    if total_issues:
        console.print("\n[bold]Validation issues:[/bold]")
        for issue in total_issues[:20]:
            console.print(f"  [yellow]{issue}[/yellow]")
        if len(total_issues) > 20:  # pragma: no cover
            console.print(f"  ... and {len(total_issues) - 20} more")
    console.print()


if __name__ == "__main__":
    main()
