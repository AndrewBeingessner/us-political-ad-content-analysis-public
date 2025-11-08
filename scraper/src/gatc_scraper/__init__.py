"""High-level utilities shared across the GATC scrapers."""

from .debug import dump_frame_html, dump_frame_inventory, ensure_debug_dir, ensure_debug_html
from .hashing import normalize_and_hash, stable_int_hash
from .logging import adlog, jlog
from .metadata import build_gcs_metadata
from .playwright import CHROMIUM_LAUNCH_ARGS, cleanup_playwright, element_is_visibly_displayed, wait_assets_ready, wait_policy_or_errors
from .storage import canonical_asset_path_image, canonical_asset_path_text, upload_png_image, upload_png_text
from .urls import GATC_URL_RE, normalize_click_url, parse_ids_from_url, select_primary_click_url
from .versioning import get_scraper_version

__all__ = [
    "adlog",
    "build_gcs_metadata",
    "canonical_asset_path_image",
    "canonical_asset_path_text",
    "cleanup_playwright",
    "dump_frame_html",
    "dump_frame_inventory",
    "ensure_debug_dir",
    "ensure_debug_html",
    "get_scraper_version",
    "GATC_URL_RE",
    "jlog",
    "normalize_and_hash",
    "normalize_click_url",
    "parse_ids_from_url",
    "select_primary_click_url",
    "stable_int_hash",
    "upload_png_image",
    "upload_png_text",
    "wait_assets_ready",
    "wait_policy_or_errors",
    "element_is_visibly_displayed",
    "CHROMIUM_LAUNCH_ARGS",
]
