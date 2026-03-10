"""Unit tests for clients/daily_bar_fallback.py."""

from __future__ import annotations

from unittest.mock import patch

import requests
import responses

from clients.daily_bar_fallback import (
    DailyBarFallbackClient,
    FallbackDailyBar,
    _parse_float,
    _parse_int,
)


def _make_client(**kwargs) -> DailyBarFallbackClient:
    return DailyBarFallbackClient(timeout=1, **kwargs)


def _nasdaq_url(symbol: str) -> str:
    return f"https://api.nasdaq.com/api/quote/{symbol}/historical"


def _nasdaq_payload(symbol: str, row: dict | None, *, asset_class: str = "stocks", rcode: int = 200) -> dict:
    rows = [] if row is None else [row]
    return {
        "data": {
            "symbol": symbol,
            "assetClass": asset_class,
            "totalRecords": len(rows),
            "tradesTable": {"rows": rows},
        },
        "message": None,
        "status": {"rCode": rcode, "bCodeMessage": None, "developerMessage": None},
    }


class TestLifecycle:
    def test_session_headers(self):
        client = _make_client()
        assert client._session.headers["User-Agent"] == "market-data-warehouse/1.0"
        assert "application/json" in client._session.headers["Accept"]
        client.close()

    def test_context_manager(self):
        with _make_client() as client:
            assert isinstance(client, DailyBarFallbackClient)


class TestGetDailyBar:
    @responses.activate
    def test_returns_nasdaq_stock_bar(self):
        responses.add(
            responses.GET,
            _nasdaq_url("AAPL"),
            json=_nasdaq_payload(
                "AAPL",
                {
                    "date": "03/09/2026",
                    "close": "$259.88",
                    "volume": "38,218,530",
                    "open": "$255.69",
                    "high": "$261.15",
                    "low": "$253.6805",
                },
            ),
            status=200,
        )

        with _make_client() as client:
            bar = client.get_daily_bar("AAPL", __import__("datetime").date(2026, 3, 9))

        assert bar == FallbackDailyBar(
            trade_date=__import__("datetime").date(2026, 3, 9),
            open=255.69,
            high=261.15,
            low=253.6805,
            close=259.88,
            volume=38218530,
            source="nasdaq:stocks",
        )
        assert bar.date == "2026-03-09"

    @responses.activate
    def test_falls_back_to_nasdaq_etf_when_stock_lookup_has_no_row(self):
        responses.add(
            responses.GET,
            _nasdaq_url("SPY"),
            json=_nasdaq_payload("SPY", None),
            status=200,
        )
        responses.add(
            responses.GET,
            _nasdaq_url("SPY"),
            json=_nasdaq_payload(
                "SPY",
                {
                    "date": "03/09/2026",
                    "close": "$585.12",
                    "volume": "80,000,000",
                    "open": "$580.00",
                    "high": "$586.00",
                    "low": "$579.50",
                },
                asset_class="etf",
            ),
            status=200,
        )

        with _make_client() as client:
            bar = client.get_daily_bar("SPY", __import__("datetime").date(2026, 3, 9))

        assert bar is not None
        assert bar.source == "nasdaq:etf"

    @responses.activate
    def test_falls_back_to_stooq_after_nasdaq_miss(self):
        responses.add(
            responses.GET,
            _nasdaq_url("KO"),
            json=_nasdaq_payload("KO", None, rcode=400),
            status=200,
        )
        responses.add(
            responses.GET,
            _nasdaq_url("KO"),
            json=_nasdaq_payload("KO", None, asset_class="etf", rcode=400),
            status=200,
        )
        responses.add(
            responses.GET,
            "https://stooq.com/q/d/l/",
            body=(
                "Date,Open,High,Low,Close,Volume\n"
                "2026-03-09,76.62,78.07,76.53,77.80,18554043\n"
            ),
            status=200,
        )

        with _make_client() as client:
            bar = client.get_daily_bar("KO", __import__("datetime").date(2026, 3, 9))

        assert bar is not None
        assert bar.source == "stooq:us"
        assert bar.close == 77.8

    @responses.activate
    def test_returns_none_when_all_providers_miss(self):
        responses.add(responses.GET, _nasdaq_url("MISS"), status=404)
        responses.add(responses.GET, _nasdaq_url("MISS"), status=404)
        responses.add(
            responses.GET,
            "https://stooq.com/q/d/l/",
            body="Date,Open,High,Low,Close,Volume\n2026-03-06,1,2,1,2,3\n",
            status=200,
        )

        with _make_client() as client:
            bar = client.get_daily_bar("MISS", __import__("datetime").date(2026, 3, 9))

        assert bar is None

    @responses.activate
    def test_fetch_daily_bar_alias(self):
        responses.add(
            responses.GET,
            _nasdaq_url("EB"),
            json=_nasdaq_payload(
                "EB",
                {
                    "date": "03/09/2026",
                    "close": "$4.51",
                    "volume": "19,592,190",
                    "open": "$4.49",
                    "high": "$4.51",
                    "low": "$4.49",
                },
            ),
            status=200,
        )

        with _make_client() as client:
            bar = client.fetch_daily_bar("EB", __import__("datetime").date(2026, 3, 9))

        assert bar is not None
        assert bar.source == "nasdaq:stocks"

    @responses.activate
    def test_nasdaq_skips_nonmatching_row_date(self):
        responses.add(
            responses.GET,
            _nasdaq_url("AAPL"),
            json=_nasdaq_payload(
                "AAPL",
                {
                    "date": "03/06/2026",
                    "close": "$259.88",
                    "volume": "38,218,530",
                    "open": "$255.69",
                    "high": "$261.15",
                    "low": "$253.6805",
                },
            ),
            status=200,
        )
        responses.add(
            responses.GET,
            _nasdaq_url("AAPL"),
            json=_nasdaq_payload("AAPL", None, asset_class="etf", rcode=400),
            status=200,
        )
        responses.add(
            responses.GET,
            "https://stooq.com/q/d/l/",
            body="Date,Open,High,Low,Close,Volume\n",
            status=200,
        )

        with _make_client() as client:
            bar = client.get_daily_bar("AAPL", __import__("datetime").date(2026, 3, 9))

        assert bar is None

    @responses.activate
    def test_stooq_none_when_text_request_fails(self):
        responses.add(
            responses.GET,
            _nasdaq_url("AAPL"),
            json=_nasdaq_payload("AAPL", None, rcode=400),
            status=200,
        )
        responses.add(
            responses.GET,
            _nasdaq_url("AAPL"),
            json=_nasdaq_payload("AAPL", None, asset_class="etf", rcode=400),
            status=200,
        )
        responses.add(
            responses.GET,
            "https://stooq.com/q/d/l/",
            body=requests.exceptions.ConnectionError("down"),
        )

        with _make_client() as client:
            bar = client.get_daily_bar("AAPL", __import__("datetime").date(2026, 3, 9))

        assert bar is None


class TestRequestHelpers:
    @responses.activate
    def test_get_json_returns_none_on_invalid_json(self):
        responses.add(
            responses.GET,
            _nasdaq_url("BROKEN"),
            body="not-json",
            status=200,
        )

        with _make_client() as client:
            payload = client._get_json(_nasdaq_url("BROKEN"), params={})

        assert payload is None

    @responses.activate
    def test_get_text_returns_none_on_request_error(self):
        responses.add(
            responses.GET,
            "https://stooq.com/q/d/l/",
            body=requests.exceptions.ConnectionError("down"),
        )

        with _make_client() as client:
            payload = client._get_text("https://stooq.com/q/d/l/", params={})

        assert payload is None

    @responses.activate
    def test_get_returns_none_with_negative_retries(self):
        with _make_client() as client:
            client._max_retries = -1
            payload = client._get(_nasdaq_url("AAPL"), params={}, provider_name="nasdaq")
        assert payload is None

    @responses.activate
    def test_get_retries_retryable_status(self):
        responses.add(
            responses.GET,
            _nasdaq_url("EB"),
            status=429,
            headers={"Retry-After": "0"},
        )
        responses.add(
            responses.GET,
            _nasdaq_url("EB"),
            json=_nasdaq_payload(
                "EB",
                {
                    "date": "03/09/2026",
                    "close": "$4.51",
                    "volume": "19,592,190",
                    "open": "$4.49",
                    "high": "$4.51",
                    "low": "$4.49",
                },
            ),
            status=200,
        )

        with _make_client() as client:
            client._max_retries = 1
            client._min_interval_seconds = 0
            with patch("clients.daily_bar_fallback.time.sleep") as mock_sleep:
                payload = client._get_json(
                    _nasdaq_url("EB"),
                    params={"assetclass": "stocks", "fromdate": "2026-03-09", "limit": 10},
                )

        assert payload is not None
        mock_sleep.assert_called_with(0.0)

    def test_sleep_backoff_invalid_retry_after_falls_back_to_default(self):
        response = requests.models.Response()
        response.headers["Retry-After"] = "not-a-number"

        with _make_client() as client, patch("clients.daily_bar_fallback.time.sleep") as mock_sleep:
            client._sleep_backoff(1, response)

        mock_sleep.assert_called_with(2.0)


class TestParseHelpers:
    def test_parse_float(self):
        assert _parse_float("$1,234.56") == 1234.56

    def test_parse_float_raises_on_missing_value(self):
        try:
            _parse_float(None)
        except ValueError as exc:
            assert "missing float field" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("ValueError was not raised")

    def test_parse_int(self):
        assert _parse_int("1,234") == 1234

    def test_parse_int_raises_on_missing_value(self):
        try:
            _parse_int(None)
        except ValueError as exc:
            assert "missing int field" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("ValueError was not raised")

    def test_static_parse_decimal_raises_on_missing_value(self):
        try:
            DailyBarFallbackClient._parse_decimal(None)
        except ValueError as exc:
            assert "missing decimal field" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("ValueError was not raised")

    def test_static_parse_integer_raises_on_missing_value(self):
        try:
            DailyBarFallbackClient._parse_integer(None)
        except ValueError as exc:
            assert "missing integer field" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("ValueError was not raised")
