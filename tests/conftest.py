"""Shared fixtures for the test suite."""

from __future__ import annotations

import duckdb
import pytest

from clients.bronze_client import BronzeClient
from clients.db_client import DBClient


# ── DuckDB fixtures ────────────────────────────────────────────────────

BOOTSTRAP_SQL = """
CREATE SCHEMA IF NOT EXISTS md;

CREATE TABLE IF NOT EXISTS md.symbols (
    symbol_id BIGINT PRIMARY KEY,
    symbol VARCHAR,
    asset_class VARCHAR,
    venue VARCHAR
);

CREATE TABLE IF NOT EXISTS md.equities_daily (
    trade_date DATE,
    symbol_id BIGINT,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    adj_close DOUBLE,
    volume BIGINT
);

CREATE TABLE IF NOT EXISTS md.futures_daily (
    trade_date DATE,
    contract_id BIGINT,
    root_symbol VARCHAR,
    expiry_date DATE,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    settlement DOUBLE,
    volume BIGINT,
    open_interest BIGINT
);
"""


@pytest.fixture()
def tmp_duckdb(tmp_path):
    """Create a temporary DuckDB file with the md schema bootstrapped."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    for stmt in BOOTSTRAP_SQL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)
    conn.close()
    return db_path


@pytest.fixture()
def db(tmp_duckdb):
    """Provide a DBClient connected to a fresh temp DuckDB."""
    client = DBClient(db_path=tmp_duckdb)
    yield client
    client.close()


@pytest.fixture()
def tmp_bronze(tmp_path):
    """Create a temporary bronze parquet root."""
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    return bronze_dir


@pytest.fixture()
def bronze(tmp_bronze):
    """Provide a BronzeClient connected to a fresh temp bronze root."""
    client = BronzeClient(bronze_dir=tmp_bronze)
    yield client
    client.close()

