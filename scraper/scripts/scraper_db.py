"""Compatibility wrapper for the Postgres helpers."""

from __future__ import annotations

from gatc_scraper.db import (
    ensure_asset_row,
    link_asset_success,
    persist_click_url,
    record_error,
    record_status,
    sql_connect,
    upsert_pending,
)

__all__ = [
    "ensure_asset_row",
    "link_asset_success",
    "persist_click_url",
    "record_error",
    "record_status",
    "sql_connect",
    "upsert_pending",
]
