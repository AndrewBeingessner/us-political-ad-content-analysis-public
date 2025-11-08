"""Compatibility wrapper around :mod:`gatc_scraper` helpers."""

from __future__ import annotations

from gatc_scraper import (
    CHROMIUM_LAUNCH_ARGS,
    adlog,
    build_gcs_metadata,
    canonical_asset_path_image,
    canonical_asset_path_text,
    dump_frame_html,
    dump_frame_inventory,
    ensure_debug_dir,
    ensure_debug_html,
    get_scraper_version,
    jlog,
    normalize_and_hash,
    normalize_click_url,
    parse_ids_from_url,
    select_primary_click_url,
    stable_int_hash,
    upload_png_image,
    upload_png_text,
    wait_policy_or_errors,
)
from gatc_scraper import (
    cleanup_playwright as _cleanup_playwright,
)
from gatc_scraper import (
    wait_assets_ready as _wait_assets_ready,
)

# Backwards-compatible exports expected by the legacy scripts
canonical_asset_path_image.__module__ = __name__
canonical_asset_path_text.__module__ = __name__
select_primary_click_url.__module__ = __name__
normalize_click_url.__module__ = __name__
parse_ids_from_url.__module__ = __name__
normalize_and_hash.__module__ = __name__
wait_policy_or_errors.__module__ = __name__

__all__ = [
    "CHROMIUM_LAUNCH_ARGS",
    "_cleanup_playwright",
    "_wait_assets_ready",
    "adlog",
    "build_gcs_metadata",
    "canonical_asset_path_image",
    "canonical_asset_path_text",
    "dump_frame_html",
    "dump_frame_inventory",
    "ensure_debug_dir",
    "ensure_debug_html",
    "get_scraper_version",
    "jlog",
    "normalize_and_hash",
    "normalize_click_url",
    "parse_ids_from_url",
    "select_primary_click_url",
    "stable_int_hash",
    "upload_png_image",
    "upload_png_text",
    "wait_policy_or_errors",
]
