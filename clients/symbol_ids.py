"""Stable symbol ID helpers."""

from __future__ import annotations

import hashlib


def stable_symbol_id(symbol: str) -> int:
    """Return a stable 53-bit positive integer for *symbol*."""
    digest = hashlib.blake2b(symbol.encode("utf-8"), digest_size=8).digest()
    value = int.from_bytes(digest, "big") & ((1 << 53) - 1)
    return value or 1
