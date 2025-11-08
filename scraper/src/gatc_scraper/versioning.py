"""Scraper version resolution helpers."""

from __future__ import annotations

import os


def get_scraper_version(script_name: str, script_version: str) -> str:
    """Return a human-readable version string with an env override."""

    return os.getenv("AD_SCRAPER_VERSION", f"{script_name}:{script_version}")


__all__ = ["get_scraper_version"]
