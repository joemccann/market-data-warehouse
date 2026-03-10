"""Fallback daily-bar client for U.S. equity symbols."""

from __future__ import annotations

import csv
import io
import logging
import time
from dataclasses import dataclass
from datetime import date
from typing import Any

import requests
from requests import Response
from requests.exceptions import ConnectionError as ReqConnectionError, Timeout as ReqTimeout

log = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 15
_DEFAULT_MAX_RETRIES = 2
_DEFAULT_BACKOFF_FACTOR = 1.0
_DEFAULT_MIN_INTERVAL_SECONDS = 0.2
_NASDAQ_BASE_URL = "https://api.nasdaq.com/api/quote"
_STOOQ_DAILY_URL = "https://stooq.com/q/d/l/"
_USER_AGENT = "market-data-warehouse/1.0"
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class FallbackDailyBar:
    """Normalized fallback daily bar."""

    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    source: str

    @property
    def date(self) -> str:
        """Expose an IB BarData-like ``date`` attribute for script reuse."""
        return self.trade_date.isoformat()


class DailyBarFallbackClient:
    """Fetch daily OHLCV bars from public fallback providers.

    Provider order:
    1. Nasdaq historical quote API (`stocks`, then `etf`)
    2. Stooq U.S. daily CSV endpoint
    """

    def __init__(
        self,
        timeout: int = _DEFAULT_TIMEOUT,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        backoff_factor: float = _DEFAULT_BACKOFF_FACTOR,
        min_interval_seconds: float = _DEFAULT_MIN_INTERVAL_SECONDS,
        session: requests.Session | None = None,
    ):
        self._timeout = timeout
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor
        self._min_interval_seconds = min_interval_seconds
        self._last_request_monotonic = 0.0
        self._session = session or requests.Session()
        self._session.headers.update(
            {
                "Accept": "application/json,text/csv,*/*",
                "User-Agent": _USER_AGENT,
            }
        )

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "DailyBarFallbackClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def get_daily_bar(self, symbol: str, trade_date: date) -> FallbackDailyBar | None:
        """Return a fallback OHLCV bar for ``symbol`` on ``trade_date``."""
        lookup_symbol = symbol.upper()

        for asset_class in ("stocks", "etf"):
            bar = self._fetch_nasdaq_bar(lookup_symbol, trade_date, asset_class)
            if bar is not None:
                return bar

        return self._fetch_stooq_bar(lookup_symbol, trade_date)

    def fetch_daily_bar(self, symbol: str, trade_date: date) -> FallbackDailyBar | None:
        """Backward-compatible alias for callers that use the fetch_* name."""
        return self.get_daily_bar(symbol, trade_date)

    def _fetch_nasdaq_bar(
        self,
        symbol: str,
        trade_date: date,
        asset_class: str,
    ) -> FallbackDailyBar | None:
        payload = self._get_json(
            f"{_NASDAQ_BASE_URL}/{symbol}/historical",
            params={
                "assetclass": asset_class,
                "fromdate": trade_date.isoformat(),
                "limit": "10",
            },
        )
        if payload is None:
            return None
        if payload.get("status", {}).get("rCode") != 200:
            return None

        rows = payload.get("data", {}).get("tradesTable", {}).get("rows", [])
        target = trade_date.strftime("%m/%d/%Y")
        for row in rows:
            if row.get("date") != target:
                continue
            return FallbackDailyBar(
                trade_date=trade_date,
                open=self._parse_decimal(row.get("open")),
                high=self._parse_decimal(row.get("high")),
                low=self._parse_decimal(row.get("low")),
                close=self._parse_decimal(row.get("close")),
                volume=self._parse_integer(row.get("volume")),
                source=f"nasdaq:{asset_class}",
            )
        return None

    def _fetch_stooq_bar(self, symbol: str, trade_date: date) -> FallbackDailyBar | None:
        text = self._get_text(_STOOQ_DAILY_URL, params={"s": f"{symbol.lower()}.us", "i": "d"})
        if not text:
            return None

        reader = csv.DictReader(io.StringIO(text))
        target = trade_date.isoformat()
        for row in reader:
            if row.get("Date") != target:
                continue
            return FallbackDailyBar(
                trade_date=trade_date,
                open=self._parse_decimal(row.get("Open")),
                high=self._parse_decimal(row.get("High")),
                low=self._parse_decimal(row.get("Low")),
                close=self._parse_decimal(row.get("Close")),
                volume=self._parse_integer(row.get("Volume")),
                source="stooq:us",
            )
        return None

    def _get_json(self, url: str, params: dict[str, Any]) -> dict | None:
        response = self._get(url, params=params, provider_name="nasdaq")
        if response is None:
            return None
        payload = self._safe_json(response)
        return payload or None

    def _get_text(self, url: str, params: dict[str, Any]) -> str | None:
        response = self._get(url, params=params, provider_name="stooq")
        if response is None:
            return None
        return response.text

    def _get(
        self,
        url: str,
        params: dict[str, Any],
        provider_name: str,
    ) -> Response | None:
        if self._max_retries < 0:
            return None

        for attempt in range(self._max_retries + 1):
            self._throttle()
            try:
                response = self._session.get(url, params=params, timeout=self._timeout)
            except (ReqConnectionError, ReqTimeout) as exc:
                if attempt < self._max_retries:
                    self._sleep_backoff(attempt)
                    continue
                log.warning(
                    "Fallback %s request failed for %s: %s",
                    provider_name, url, exc,
                )
                return None

            if response.status_code == 200:
                return response

            if response.status_code in _RETRYABLE_STATUS_CODES and attempt < self._max_retries:
                self._sleep_backoff(attempt, response)
                continue

            log.warning(
                "Fallback %s request returned HTTP %s for %s",
                provider_name, response.status_code, url,
            )
            return None

    def _throttle(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_monotonic
        remaining = self._min_interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)
            now = time.monotonic()
        self._last_request_monotonic = now

    def _sleep_backoff(self, attempt: int, response: Response | None = None) -> None:
        retry_after = None if response is None else response.headers.get("Retry-After")
        if retry_after is not None:
            try:
                time.sleep(float(retry_after))
                return
            except (TypeError, ValueError):
                pass
        time.sleep(self._backoff_factor * (2 ** attempt))

    @staticmethod
    def _safe_json(response: Response) -> dict:
        try:
            return response.json()
        except Exception:
            return {}

    @staticmethod
    def _parse_decimal(value: Any) -> float:
        if value is None:
            raise ValueError("missing decimal field")
        cleaned = str(value).replace("$", "").replace(",", "").strip()
        return float(cleaned)

    @staticmethod
    def _parse_integer(value: Any) -> int:
        if value is None:
            raise ValueError("missing integer field")
        cleaned = str(value).replace(",", "").strip()
        return int(cleaned)


def _parse_float(value: Any) -> float:
    if value is None:
        raise ValueError("missing float field")
    return DailyBarFallbackClient._parse_decimal(value)


def _parse_int(value: Any) -> int:
    if value is None:
        raise ValueError("missing int field")
    return DailyBarFallbackClient._parse_integer(value)
