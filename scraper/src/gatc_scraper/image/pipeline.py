#!/usr/bin/env python3
"""
scrape_image_ads.py

Image creative scraper for Google Ads Transparency Center (GATC).

Overview
--------
This script captures U.S. political IMAGE creatives from GATC and stores:
- a normalized canonical PNG (trimmed borders where appropriate),
- cryptographic and perceptual hashes (sha256, avg‑hash),
- basic creative metadata (dimensions, renderer, variant),
- and a single canonical click‑through URL when available.

The pipeline is deterministic and restartable. It favors HTML extraction (no OCR),
and uses exact SHA256 to deduplicate assets. Near‑duplicate (aHash) is recorded
for analysis but not used for storage.

Main features
-------------
- Input: BigQuery stream, a single ad ID, or a direct creative URL.
- Renderers supported: sadbundle (image-in-iframe), fletch (discover tile), and plain <img>.
- Click URL extraction with an "anchor-first" policy for discover tiles.
- Structured JSON logs designed for Cloud Logging / analysis.

Requirements
-----------
- Python 3.12+
- Playwright (Chromium), Pillow, requests
- google-cloud-bigquery, google-cloud-storage, psycopg2-binary

Environment
-----------
- Cloud SQL Postgres DB `adsdb` with tables `ads` and `assets`.
- GCS bucket for canonical assets and optional debug artifacts.

Usage (examples)
----------------
# Single ad by ID (lookup via BigQuery)
python scripts/scrape_image_ads.py \
  --project-id your-gcp-project \
  --gcs-bucket your-scraper-bucket \
  --db-host 127.0.0.1 \
  --ad-id CR01234567890123456789 --concurrency 1

# Single ad by full URL (bypass BigQuery)
python scripts/scrape_image_ads.py \
  --project-id your-gcp-project \
  --gcs-bucket your-scraper-bucket \
  --db-host 127.0.0.1 \
  --ad-url "https://adstransparency.google.com/advertiser/AR.../creative/CR...?region=US&topic=political&format=IMAGE" \
  --concurrency 1 --trace

# Stream a recent slice (safe test)
python scripts/scrape_image_ads.py \
  --project-id your-gcp-project \
  --gcs-bucket your-scraper-bucket \
  --db-host 127.0.0.1 \
  --since-days 30 --order-by date_desc --sql-limit 200 --concurrency 2
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import html
import importlib
import json
import logging
import os
import re
import urllib.parse
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from io import BytesIO
from types import ModuleType
from typing import TYPE_CHECKING, Any, cast

from gatc_scraper import (
    CHROMIUM_LAUNCH_ARGS,
    adlog,
    build_gcs_metadata,
    cleanup_playwright,
    dump_frame_inventory,
    element_is_visibly_displayed,
    ensure_debug_html,
    jlog,
    normalize_and_hash,
    normalize_click_url,
    parse_ids_from_url,
    select_primary_click_url,
    stable_int_hash,
    wait_assets_ready,
    wait_policy_or_errors,
)
from gatc_scraper import (
    canonical_asset_path_image as canonical_asset_path,
)
from gatc_scraper import (
    get_scraper_version as resolve_version,
)
from gatc_scraper import (
    upload_png_image as upload_png,
)
from gatc_scraper.db import (
    ensure_asset_row as db_ensure_asset_row,
)
from gatc_scraper.db import (
    link_asset_success as db_link_asset_success,
)
from gatc_scraper.db import (
    persist_click_url as db_persist_click_url,
)
from gatc_scraper.db import (
    record_error as db_record_error,
)
from gatc_scraper.db import (
    record_status as db_record_status,
)
from gatc_scraper.db import (
    sql_connect,
)
from gatc_scraper.db import (
    upsert_pending as db_upsert_pending,
)
from gatc_scraper.ocr import OCRResult, extract_text_from_image, sanitize_ocr_text
from google.cloud import bigquery, storage  # type: ignore[attr-defined]
from PIL import Image
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page, TimeoutError, async_playwright

requests: Any
if TYPE_CHECKING:
    requests = cast(Any, ModuleType("requests"))
else:
    requests = importlib.import_module("requests")

# Back-compat alias for Python 3.10+ (datetime.UTC exists on 3.11+)
UTC = getattr(datetime, "UTC", timezone.utc)

# ============================
# Constants & configuration
# ============================
DEFAULT_USER_AGENT = "yale-researcher/1.0"
DEFAULT_GCS_BUCKET = "your-scraper-bucket"
DEFAULT_PROJECT_ID = "your-gcp-project"
DEFAULT_CONCURRENCY = 2
DEFAULT_BATCH_SIZE = 5000
DEFAULT_SQL_CONN = "your-project:your-region:your-instance"
IMAGE_VARIANT_ID = "v1"  # primary variant identifier for IMAGE creatives

# Rendering / timeout defaults (overridable via CLI or env)
DEFAULT_DEVICE_SCALE_FACTOR = int(os.getenv("DEVICE_SCALE_FACTOR", "3"))
DEFAULT_PAGE_TIMEOUT_MS = int(os.getenv("PAGE_TIMEOUT_MS", "30000"))  # top-level page navigations
DEFAULT_IFRAME_TIMEOUT_MS = int(os.getenv("IFRAME_TIMEOUT_MS", "15000"))  # waits inside creative iframes
DEFAULT_SETTLE_MAX_MS = int(os.getenv("GATC_SETTLE_MAX_MS", "12000"))
DEFAULT_SETTLE_MIN_STABLE_MS = int(os.getenv("GATC_SETTLE_MIN_MS", "1200"))
DEFAULT_SETTLE_INTERVAL_MS = int(os.getenv("GATC_SETTLE_INTERVAL_MS", "180"))
DEFAULT_SETTLE_MIN_OBSERVE_MS = int(os.getenv("GATC_SETTLE_MIN_OBSERVE_MS", "600"))
FLETCH_SETTLE_MAX_MS = int(os.getenv("GATC_FLETCH_SETTLE_MAX_MS", "1800"))
FLETCH_SETTLE_MIN_STABLE_MS = int(os.getenv("GATC_FLETCH_SETTLE_MIN_MS", "600"))
FLETCH_SETTLE_MIN_OBSERVE_MS = int(os.getenv("GATC_FLETCH_SETTLE_MIN_OBSERVE_MS", "400"))

# Capture provenance constants
CAPTURE_METHOD_IMG = "img"
CAPTURE_METHOD_SCREENSHOT = "screenshot"

# Module-level logger used throughout the pipeline.  Configuration is provided by
# the CLI shim via :func:`gatc_scraper.logging.configure_logging`.
log = logging.getLogger("scraper")

# Scraper provenance
SCRIPT_NAME = "image"
SCRIPT_VERSION = "2025-10-26.1"


def get_scraper_version() -> str:
    return resolve_version(SCRIPT_NAME, SCRIPT_VERSION)


# ============================
# Argument parsing & validation
# ============================


@dataclass(frozen=True)
class CliArgs:
    project_id: str
    gcs_bucket: str
    sql_conn: str
    db_host: str | None
    db_port: int | None
    max_ads: int | None
    sql_limit: int | None
    user_agent: str
    concurrency: int
    batch_size: int
    start_date: str | None
    end_date: str | None
    order_by: str
    skip_advertisers: list[str]
    ad_id: str | None
    ad_url: str | None
    advertiser_id: str | None
    manifest_path: str | None
    all_variants: bool
    trace: bool
    since_days: int | None
    limit: int | None
    max_retries: int
    retry_base_ms: int
    dry_run: bool
    shard: int
    shard_count: int
    debug_frames: bool
    debug_html: bool
    bq_location: str | None
    device_scale_factor: int
    page_timeout_ms: int
    iframe_timeout_ms: int
    rescrape_done: bool


def _coerce_dates(args: argparse.Namespace) -> None:
    if getattr(args, "since_days", None):
        today = datetime.now(UTC).date()
        args.start_date = date.fromordinal(today.toordinal() - args.since_days).isoformat()
    if getattr(args, "limit", None) and not args.sql_limit:
        args.sql_limit = args.limit


def validate_args(args: argparse.Namespace) -> None:
    """Emit warnings/errors for likely mistakes or expensive runs."""
    # Start/end consistency
    if args.start_date and args.end_date and args.start_date > args.end_date:
        raise ValueError(f"start_date ({args.start_date}) is after end_date ({args.end_date})")

    # If a direct ad is specified, we're not scanning a table; skip scan warnings.
    if args.ad_url or args.ad_id or args.manifest_path:
        return

    # Potential full‑table scans
    no_dates = not args.start_date and not args.since_days and not args.end_date
    no_limit = not args.sql_limit and not args.max_ads
    if no_dates and no_limit:
        jlog(
            "warning",
            event="potential_large_scan",
            message=(
                "No date filter or limit supplied; this may scan the full BigQuery table. "
                "Consider --since-days, --start-date/--end-date, or --sql-limit for faster tests."
            ),
        )
    if args.shard_count and args.shard_count > 1 and no_dates and no_limit:
        jlog(
            "warning",
            event="sharded_full_scan",
            message=(
                "Sharding is enabled but no date/limit provided; each shard will read pages of the full dataset. "
                "Add --since-days or --sql-limit unless you truly want a whole-table pass."
            ),
            shard=args.shard,
            shard_count=args.shard_count,
        )


def parse_args() -> CliArgs:
    p = argparse.ArgumentParser(description="Download IMAGE ads (US only)")
    p.add_argument("--project-id", default=DEFAULT_PROJECT_ID)
    p.add_argument("--gcs-bucket", default=DEFAULT_GCS_BUCKET)
    p.add_argument("--sql-conn", default=DEFAULT_SQL_CONN)
    p.add_argument("--db-host")
    p.add_argument("--db-port", type=int)
    p.add_argument("--max-ads", type=int)
    p.add_argument("--sql-limit", type=int)
    p.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    p.add_argument("--start-date")
    p.add_argument("--end-date")
    p.add_argument(
        "--order-by",
        choices=["none", "date_asc", "date_desc", "advertiser"],
        default="none",
    )
    p.add_argument("--skip-advertisers", nargs="*", default=[])
    p.add_argument("--ad-id", help="Process a single Ad_ID (smoke test)")
    p.add_argument(
        "--ad-url",
        help=(
            "Direct GATC creative URL. If provided, BigQuery is bypassed. "
            "If --ad-id/--advertiser-id are missing, they will be parsed from this URL."
        ),
    )
    p.add_argument(
        "--manifest-path",
        help=(
            "Path to a manifest (CSV with headers ad_id,ad_url,advertiser_id or JSONL) "
            "listing creatives to scrape. Bypasses BigQuery and streams directly from the file."
        ),
    )
    p.add_argument(
        "--advertiser-id",
        help="Advertiser_ID (e.g., AR123...). Required if --ad-url does not include it.",
    )
    p.add_argument(
        "--all-variants",
        action="store_true",
        default=True,
        help=("Capture and persist every detected variant (v1, v2, ...) instead of stopping after the first success."),
    )
    p.add_argument(
        "--trace",
        action="store_true",
        help="Save Playwright trace and debug artifacts for each ad.",
    )
    p.add_argument(
        "--since-days",
        type=int,
        help="Shorthand for --start-date=UTC_TODAY-minus-N-days",
    )
    p.add_argument("--limit", type=int, help="Alias for --sql-limit")
    p.add_argument(
        "--max-retries",
        type=int,
        default=2,
        help="Max retries for transient failures per ad (default: 2)",
    )
    p.add_argument(
        "--retry-base-ms",
        type=int,
        default=500,
        help="Base backoff in ms for retries (default: 500)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write to DB or GCS; only validate capture path",
    )
    p.add_argument("--shard", type=int, default=0, help="Shard index (0-based)")
    p.add_argument("--shard-count", type=int, default=1, help="Total number of shards")
    p.add_argument("--debug-frames", action="store_true", help="Log iframe/img inventory under the creative container for debugging.")
    p.add_argument(
        "--debug-html",
        action="store_true",
        help="Dump page HTML to media/debug/page_<ad_id>.html for debugging.",
    )
    p.add_argument(
        "--bq-location",
        help="BigQuery location/region for query jobs (e.g., US, EU)",
    )
    p.add_argument(
        "--device-scale-factor",
        type=int,
        default=DEFAULT_DEVICE_SCALE_FACTOR,
        help="Playwright deviceScaleFactor for sharper screenshots (default from DEVICE_SCALE_FACTOR env or 3).",
    )
    p.add_argument(
        "--page-timeout-ms",
        type=int,
        default=DEFAULT_PAGE_TIMEOUT_MS,
        help="Timeout (ms) for top-level page navigations (default from PAGE_TIMEOUT_MS env or 30000).",
    )
    p.add_argument(
        "--iframe-timeout-ms",
        type=int,
        default=DEFAULT_IFRAME_TIMEOUT_MS,
        help="Timeout (ms) for waits inside creative iframes (default from IFRAME_TIMEOUT_MS env or 15000).",
    )
    p.add_argument(
        "--rescrape-done",
        action="store_true",
        help="Re-run ads even if the ads table shows status='done' (default: skip existing successes)",
    )

    ns = p.parse_args()
    _coerce_dates(ns)
    validate_args(ns)

    # If running as a Cloud Run Job with multiple tasks, prefer task-index sharding by default.
    # Only apply when the user did not explicitly set --shard or --shard-count.
    try:
        task_index = os.getenv("CLOUD_RUN_TASK_INDEX")
        task_count = os.getenv("CLOUD_RUN_TASK_COUNT")
        if task_index is not None and task_count is not None:
            # Respect explicit CLI overrides: only use env if user left defaults (0 and 1).
            if getattr(ns, "shard", 0) == 0 and getattr(ns, "shard_count", 1) == 1:
                ns.shard = int(task_index)
                ns.shard_count = int(task_count)
    except Exception:
        pass

    return CliArgs(
        project_id=ns.project_id,
        gcs_bucket=ns.gcs_bucket,
        sql_conn=ns.sql_conn,
        db_host=ns.db_host,
        db_port=ns.db_port,
        max_ads=ns.max_ads,
        sql_limit=ns.sql_limit,
        user_agent=ns.user_agent,
        concurrency=ns.concurrency,
        batch_size=ns.batch_size,
        start_date=ns.start_date,
        end_date=ns.end_date,
        order_by=ns.order_by,
        skip_advertisers=list(ns.skip_advertisers or []),
        ad_id=ns.ad_id,
        ad_url=ns.ad_url,
        advertiser_id=ns.advertiser_id,
        manifest_path=ns.manifest_path,
        all_variants=ns.all_variants,
        trace=ns.trace,
        since_days=ns.since_days,
        limit=ns.limit,
        max_retries=ns.max_retries,
        retry_base_ms=ns.retry_base_ms,
        dry_run=ns.dry_run,
        shard=ns.shard,
        shard_count=ns.shard_count,
        debug_frames=ns.debug_frames,
        debug_html=ns.debug_html,
        device_scale_factor=ns.device_scale_factor,
        page_timeout_ms=ns.page_timeout_ms,
        iframe_timeout_ms=ns.iframe_timeout_ms,
        bq_location=ns.bq_location,
        rescrape_done=ns.rescrape_done,
    )


# ============================
# SQL helpers
# ============================

# --- Thin adapters to centralize DB shape in scripts.scraper_db ---


def upsert_pending(
    con,
    ad_id: str,
    advertiser_id: str,
    variant_id: str = IMAGE_VARIANT_ID,
    *,
    source_url: str,
    dry_run: bool = False,
) -> None:
    return db_upsert_pending(
        con,
        ad_type="IMAGE",
        ad_id=ad_id,
        advertiser_id=advertiser_id,
        variant_id=variant_id,
        source_url=source_url,
        scraper_version=get_scraper_version(),
        dry_run=dry_run,
    )


def link_asset_success(
    con,
    adv_id: str,
    ad_id: str,
    asset_id: str,
    rmethod: str,
    w: int,
    h: int,
    fbytes: int,
    phash: str,
    canonical_gcs: str,
    variant_id: str = IMAGE_VARIANT_ID,
    *,
    dry_run: bool = False,
    variant: int | None = None,  # ignored by DB; kept for logging parity
    capture_method: str | None = None,
    capture_target: str | None = None,
    click_url: str | None = None,
    ocr_text: str | None = None,
    ocr_language: str | None = None,
    ocr_confidence: float | None = None,
) -> None:
    return db_link_asset_success(
        con,
        ad_type="IMAGE",
        advertiser_id=adv_id,
        ad_id=ad_id,
        variant_id=variant_id,
        asset_id=asset_id,
        render_method=rmethod,
        width_px=w,
        height_px=h,
        file_bytes=fbytes,
        phash=phash,
        gcs_path=canonical_gcs,
        capture_method=capture_method,
        capture_target=capture_target,
        scraper_version=get_scraper_version(),
        click_url=click_url,
        ocr_text=ocr_text,
        ocr_language=ocr_language,
        ocr_confidence=ocr_confidence,
        dry_run=dry_run,
    )


def record_status(
    con,
    adv_id: str,
    ad_id: str,
    status: str,
    last_error: str | None = None,
    variant_id: str = IMAGE_VARIANT_ID,
    *,
    dry_run: bool = False,
) -> None:
    return db_record_status(
        con,
        advertiser_id=adv_id,
        ad_id=ad_id,
        variant_id=variant_id,
        status=status,
        last_error=last_error,
        dry_run=dry_run,
    )


def record_error(
    con,
    adv_id: str,
    ad_id: str,
    msg: str,
    variant_id: str = IMAGE_VARIANT_ID,
    *,
    dry_run: bool = False,
) -> None:
    return db_record_error(
        con,
        advertiser_id=adv_id,
        ad_id=ad_id,
        variant_id=variant_id,
        msg=msg,
        dry_run=dry_run,
    )


def ensure_asset_row(
    con,
    asset_id: str,
    phash: str,
    w: int,
    h: int,
    fbytes: int,
    canonical_gcs: str,
    *,
    dry_run: bool = False,
) -> None:
    return db_ensure_asset_row(
        con,
        asset_id=asset_id,
        phash=phash,
        width_px=w,
        height_px=h,
        file_bytes=fbytes,
        gcs_path=canonical_gcs,
        dry_run=dry_run,
    )


def persist_click_url(
    con,
    adv_id: str,
    ad_id: str,
    variant_id: str,
    click_url: str | None,
    *,
    dry_run: bool = False,
) -> None:
    return db_persist_click_url(
        con,
        advertiser_id=adv_id,
        ad_id=ad_id,
        variant_id=variant_id,
        click_url=click_url or "",
        dry_run=dry_run,
    )


async def _persist_primary_click(
    con,
    adv: str,
    ad_id: str,
    variant_id: str,
    render_method: str,
    clicks: list[str],
    *,
    dry_run: bool,
) -> str | None:
    primary = select_primary_click_url(clicks)
    if not primary:
        return None
    norm = normalize_click_url(primary)
    if not norm:
        return None
    try:
        jlog(
            "info",
            event="click_url",
            ad_id=ad_id,
            advertiser_id=str(adv),
            render_method=render_method,
            url=norm,
            variant_id=variant_id,
        )
    except Exception:
        pass
    persist_click_url(con, str(adv), ad_id, variant_id, norm, dry_run=dry_run)
    return norm


def _is_meaningful_src(u: str | None) -> bool:
    if not u:
        return False
    u = u.strip().lower()
    return not (u == "about:blank" or u.startswith("data:"))


def _tile_selector_candidates(dom_index: int, variant_idx: int) -> list[str]:
    """
    Generate selector candidates that cover both carousel and stand-alone layouts.
    """
    if dom_index <= 0 or variant_idx <= 0:
        return []
    return [
        f".creative-container.creative-carousel > div:nth-child({dom_index})",
        f".creative-container > div.creative-sub-container:nth-of-type({variant_idx})",
        f".creative-container > div.creative-sub-container:nth-child({dom_index})",
        f".creative-container > div:nth-child({dom_index})",
    ]


async def _activate_and_probe_tile(page: Page, dom_index: int, variant_idx: int, ad_url: str) -> dict[str, str]:
    selectors = _tile_selector_candidates(dom_index, variant_idx)
    sel_variant = selectors[0] if selectors else ""
    for sel in selectors:
        if not sel:
            continue
        try:
            handle = await page.query_selector(sel)
        except Exception:
            handle = None
        if handle:
            sel_variant = sel
            try:
                await handle.dispose()
            except Exception:
                pass
            break
    if not sel_variant:
        sel_variant = f".creative-container > div:nth-child({max(dom_index, 1)})"
    try:
        await page.evaluate(
            "(sel)=>{const el=document.querySelector(sel); if(el){"
            " el.scrollIntoView({block:'center'}); try{el.click();}catch{} "
            " el.classList?.remove('hidden'); el.removeAttribute('hidden'); el.setAttribute('aria-hidden','false'); }}",
            sel_variant,
        )
        await page.wait_for_timeout(300)
    except Exception:
        pass
    try:
        probed = await page.evaluate(
            "(sel)=>{const el=document.querySelector(sel); if(!el) return {iframe:'',img:''}; "
            "const ifr=el.querySelector('iframe'); const im=el.querySelector('img'); "
            "return {iframe:(ifr&&ifr.getAttribute('src'))||'', img:(im&&im.getAttribute('src'))||''};}",
            sel_variant,
        )
    except Exception:
        probed = {"iframe": "", "img": ""}
    iframe_src = probed.get("iframe") or ""
    img_src = probed.get("img") or ""
    try:
        if iframe_src:
            iframe_src = urllib.parse.urljoin(ad_url, iframe_src)
        if img_src:
            img_src = urllib.parse.urljoin(ad_url, img_src)
    except Exception:
        pass
    return {"iframe": iframe_src, "img": img_src}


@dataclass(frozen=True)
class Timeouts:
    page_ms: int
    iframe_ms: int


def _timeouts(args: CliArgs | None) -> Timeouts:
    return Timeouts(
        page_ms=args.page_timeout_ms if args else DEFAULT_PAGE_TIMEOUT_MS,
        iframe_ms=args.iframe_timeout_ms if args else DEFAULT_IFRAME_TIMEOUT_MS,
    )


def _stop_after_first(args: CliArgs | None) -> bool:
    # default True means capture all variants; stop only when explicitly disabled
    return not (args and args.all_variants)


def _make_http(user_agent: str, referer: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": user_agent, "Referer": referer})
    return s


async def fetch_sadbundle_image_bytes(
    frame_page: Page,
    frame_url: str,
    http: requests.Session,
    iframe_timeout_ms: int,
) -> bytes | None:
    """
    Fetch raw image bytes from a sadbundle frame if the creative is image-based.

    We locate an <img> inside the sadbundle frame, resolve its `src` against the
    frame URL, and download the bytes directly (no screenshots).
    """
    try:
        await frame_page.wait_for_selector("img.img_ad, a#aw0 > img, #google_image_div img", timeout=iframe_timeout_ms)
        # Allow lazy attributes/srcset to settle
        try:
            await wait_assets_ready(frame_page)
        except Exception:
            pass
        try:
            await frame_page.wait_for_timeout(200)
        except Exception:
            pass
    except TimeoutError:
        return None
    try:
        src = await frame_page.evaluate(
            """
            () => {
                const el = document.querySelector('img.img_ad, a#aw0 > img, #google_image_div img');
                return el ? el.getAttribute('src') : null;
            }
            """
        )
    except Exception:
        src = None
    if not src:
        return None
    try:
        # Resolve relative src against the sadbundle frame URL
        abs_url = urllib.parse.urljoin(frame_url, src)
        resp = http.get(abs_url, timeout=20)
        resp.raise_for_status()
        return resp.content
    except Exception:
        return None


# --- Animation freezing helper for evidence-based screenshots ---
async def _freeze_animations(page: Page) -> None:
    """Pause Web Animations and CSS transitions so we capture the visible state."""
    try:
        await page.evaluate(
            """
            () => {
                try {
                    const anims = (document.getAnimations ? document.getAnimations() : []);
                    for (const a of anims) { try { a.pause(); } catch(_) {} }
                } catch(_) {}
                try {
                    const all = document.querySelectorAll('*');
                    for (const el of all) {
                        el.style.animationPlayState = 'paused';
                        el.style.transition = 'none';
                    }
                } catch(_) {}
            }
            """
        )
    except Exception:
        pass


# --- Lightweight visual similarity helpers (no external deps beyond Pillow) ---


def _ahash64(png_bytes: bytes) -> int:
    """Compute a simple 64-bit average-hash for a PNG image; -1 on failure."""
    try:
        with Image.open(BytesIO(png_bytes)) as im:
            im = im.convert("L").resize((8, 8))
            pixels = list(im.getdata())
            avg = sum(pixels) / 64.0
            bits = 0
            for i, p in enumerate(pixels):
                if p >= avg:
                    bits |= 1 << i
            return bits
    except Exception:
        return -1


def _hamdist64(a: int, b: int) -> int:
    if a < 0 or b < 0:
        return 64
    return (a ^ b).bit_count()


async def _settle_screenshot(
    get_png_coro,
    *,
    max_wait_ms: int = DEFAULT_SETTLE_MAX_MS,
    min_stable_ms: int = DEFAULT_SETTLE_MIN_STABLE_MS,
    interval_ms: int = DEFAULT_SETTLE_INTERVAL_MS,
    near_epsilon: int = 3,
    min_observe_ms: int = DEFAULT_SETTLE_MIN_OBSERVE_MS,
) -> bytes:
    """
    Observe repeated screenshots until the image is visually stable.
    - Stability = aHash distance <= near_epsilon over a continuous window of `min_stable_ms`.
    - Warmup = observe at least `min_observe_ms` total OR see at least one meaningful change
      before accepting *any* stability window (prevents biting early micro-pauses).
    - IMPORTANT: We do **not** return on the first stable window; we keep sampling until
      `max_wait_ms` and return the **last** qualifying stable frame observed. This biases us
      toward the true end-of-sequence when animations include intermittent pauses.
    Returns the best stable PNG (last window) or the latest sample on timeout.
    """
    loop = asyncio.get_event_loop()
    t0 = loop.time()
    stable_start = None
    saw_meaningful_change = False

    # Track the last stable window that satisfied the threshold
    best_png: bytes | None = None

    png_last = await get_png_coro()
    h_last = _ahash64(png_last)

    while (loop.time() - t0) * 1000.0 < max_wait_ms:
        await asyncio.sleep(max(0, interval_ms) / 1000.0)
        try:
            png_now = await get_png_coro()
        except Exception:
            break
        h_now = _ahash64(png_now)

        near_identical = (png_now == png_last) or (_hamdist64(h_now, h_last) <= near_epsilon)
        now = loop.time()
        if near_identical:
            if stable_start is None:
                stable_start = now
            long_enough = (now - stable_start) * 1000.0 >= min_stable_ms
            observed_enough = ((now - t0) * 1000.0 >= min_observe_ms) or saw_meaningful_change
            if long_enough and observed_enough:
                # Update the candidate **without returning**; we want the last stable window
                best_png = png_now
        else:
            # Motion detected; reset stability window and record that we've seen movement
            stable_start = None
            saw_meaningful_change = True
            png_last = png_now
            h_last = h_now

    # Prefer the **last** stable window if any; otherwise return latest observed frame
    if best_png is not None:
        return best_png
    return png_last


async def sniff_gwd(page: Page, iframe_timeout_ms: int) -> dict[str, object]:
    """
    Minimal GWD detector.
    Returns:
      {
        "is_gwd": bool,
        "marker": str | None,     # 'admetadata' | 'gwd-element' | 'child:admetadata' | 'child:gwd-element'
        "found_in_child": bool,
    }
    """

    async def _check(doc) -> tuple[bool, str | None]:
        try:
            has_meta = await doc.evaluate("() => !!document.querySelector(\"script[type='text/gwd-admetadata']\")")
        except Exception:
            has_meta = False
        if has_meta:
            return True, "admetadata"

        try:
            has_elem = await doc.evaluate("() => !!document.querySelector('gwd-google-ad, gwd-page, #gwd-ad, .gwd-page-content')")
        except Exception:
            has_elem = False

        return (bool(has_elem), "gwd-element" if has_elem else None)

    # Check current document
    is_gwd, marker = await _check(page)
    if is_gwd:
        return {"is_gwd": True, "marker": marker, "found_in_child": False}

    # Cheap peek into same-origin children (no re-navigation)
    for fr in page.frames:
        if fr == page.main_frame:
            continue
        try:
            ok, m = await _check(fr)
        except Exception:
            ok, m = False, None  # cross-origin or other failures; ignore
        if ok:
            return {"is_gwd": True, "marker": f"child:{m}", "found_in_child": True}

    return {"is_gwd": False, "marker": None, "found_in_child": False}


# --- Stability-based fallback screenshot for host iframe ---
async def stabilized_host_iframe_screenshot(
    fe,
    *,
    max_wait_ms: int = DEFAULT_SETTLE_MAX_MS,
    min_stable_ms: int = DEFAULT_SETTLE_MIN_STABLE_MS,
    interval_ms: int = DEFAULT_SETTLE_INTERVAL_MS,
    near_epsilon: int = 3,
    min_observe_ms: int = DEFAULT_SETTLE_MIN_OBSERVE_MS,
) -> bytes | None:
    """
    Take repeated screenshots of a (cross-origin) host iframe until the visual changes settle
    (similarity window), then return the **last** qualifying stable frame within the window.
    """

    async def _grab():
        return await fe.screenshot(type="png")

    try:
        return await _settle_screenshot(
            _grab,
            max_wait_ms=max_wait_ms,
            min_stable_ms=min_stable_ms,
            interval_ms=interval_ms,
            near_epsilon=near_epsilon,
            min_observe_ms=min_observe_ms,
        )
    except Exception:
        return None


async def screenshot_sadbundle_element(
    frame_page: Page,
    iframe_timeout_ms: int,
    *,
    preferred_selectors: list[str] | None = None,
) -> tuple[bytes | None, str | None]:
    """
    Evidence-based capture of SADBUNDLE DOM animations (e.g., Google Web Designer).
    Prefer caller-supplied `preferred_selectors`. Returns (png, css_selector) or (None, None).
    """
    # Allow assets to settle (fonts, images) briefly
    try:
        await wait_assets_ready(frame_page)
    except Exception:
        pass
    try:
        await frame_page.wait_for_timeout(300)
    except Exception:
        pass

    selectors = preferred_selectors or [
        # GWD-ish
        "#page1",
        ".gwd-page-content",
        "gwd-page",
        ".gwd-page-container",
        "gwd-pagedeck",
        "#pagedeck",
        "gwd-google-ad",
        "div[class*='gwd-page-']",
        # Templated SADBUNDLE (non-GWD) — the ones you pasted
        "#mys-content",
        "#mys-wrapper",
        ".x-layout",
        "svg.image",
        # Image-wrapper container (as last resort when fetch fails)
        "#google_image_div",
    ]

    try:
        await frame_page.wait_for_selector(", ".join(selectors), timeout=iframe_timeout_ms)
    except Exception:
        return None, None

    try:
        sel = await frame_page.evaluate(
            "(sels)=>{ for (const s of sels){ const el = document.querySelector(s); if (el) return s; } return null; }",
            selectors,
        )
    except Exception:
        sel = None
    if not sel:
        return None, None

    try:
        await frame_page.evaluate(
            "(s)=>{ const el=document.querySelector(s); if(el) el.scrollIntoView({block:'center', inline:'center'}); }",
            sel,
        )
    except Exception:
        pass

    # Phase 1: let the element visually settle without freezing (handles brief intentional pauses)
    locator = frame_page.locator(sel).first
    try:
        png_settled = await _settle_screenshot(
            lambda: locator.screenshot(type="png"),
            max_wait_ms=DEFAULT_SETTLE_MAX_MS,
            min_stable_ms=DEFAULT_SETTLE_MIN_STABLE_MS,
            interval_ms=DEFAULT_SETTLE_INTERVAL_MS,
            near_epsilon=3,
            min_observe_ms=DEFAULT_SETTLE_MIN_OBSERVE_MS,
        )
    except Exception:
        return None, None

    # Phase 2: gently freeze and confirm stability did not jump to a different end-state
    await _freeze_animations(frame_page)
    try:
        await frame_page.wait_for_timeout(200)
    except Exception:
        pass
    try:
        png_frozen = await locator.screenshot(type="png")
    except Exception:
        return png_settled, sel

    # If freezing changed the frame materially, re-settle briefly post-freeze
    if _hamdist64(_ahash64(png_settled), _ahash64(png_frozen)) > 4:
        try:
            png_final = await _settle_screenshot(
                lambda: locator.screenshot(type="png"),
                max_wait_ms=FLETCH_SETTLE_MAX_MS,
                min_stable_ms=FLETCH_SETTLE_MIN_STABLE_MS,
                interval_ms=DEFAULT_SETTLE_INTERVAL_MS,
                near_epsilon=3,
                min_observe_ms=FLETCH_SETTLE_MIN_OBSERVE_MS,
            )
            return png_final, sel
        except Exception:
            return png_frozen, sel
    return png_frozen, sel


async def extract_click_urls_from_frame(frame) -> list[str]:
    """
    Extract click-through URLs from a sadbundle frame.

    Sources scanned:
      1) <meta data-asoch-meta="..."> payloads (may embed googleadservices links)
      2) Anchor tags inside the frame (e.g., a#aw0) and other links
         - If href is a googleadservices URL and includes an 'adurl=' parameter, we
           decode and return the 'adurl' value.
         - Otherwise, if href is a direct http(s) link (not a Google ads redirect), we
           return the href itself.
    We never navigate; we only parse DOM attributes.
    """
    found: list[str] = []

    def _add(url: str) -> None:
        norm = normalize_click_url(url)
        if not norm:
            return
        try:
            parsed = urllib.parse.urlparse(norm)
            host = (parsed.netloc or "").lower()
            # Reject obvious non-destination or infra hosts
            if host.endswith("google.com") and not host.endswith("googleadservices.com"):
                return
            if host.endswith("g.doubleclick.net") or host.endswith("doubleclick.net"):
                return
            if host.endswith("tpc.googlesyndication.com") or host.endswith("pagead2.googlesyndication.com"):
                return
            # After normalization, googleadservices links should have been unwrapped to their adurl target.
            if host.endswith("googleadservices.com"):
                return
        except Exception:
            pass
        if norm not in found:
            found.append(norm)

    # ---- 1) meta[data-asoch-meta] path ----
    try:
        meta = await frame.evaluate(
            """
            () => {
              const m = document.querySelector('meta[data-asoch-meta]');
              return m ? m.getAttribute('data-asoch-meta') : null;
            }
            """
        )
        if meta:
            try:
                data = json.loads(meta)
            except Exception:
                data = None
            if data is not None:

                def _walk(obj):
                    if isinstance(obj, str) and obj.startswith("http"):
                        try:
                            parsed = urllib.parse.urlparse(obj)
                            qs = urllib.parse.parse_qs(parsed.query)
                            # Prefer adurl if present (Google redirector)
                            if parsed.netloc.endswith("googleadservices.com") and "adurl" in qs and qs["adurl"]:
                                _add(qs["adurl"][0])
                            elif parsed.scheme in ("http", "https") and not parsed.netloc.endswith("googleadservices.com"):
                                _add(obj)
                        except Exception:
                            pass
                    elif isinstance(obj, list):
                        for x in obj:
                            _walk(x)
                    elif isinstance(obj, dict):
                        for v in obj.values():
                            _walk(v)

                _walk(data)
    except Exception:
        pass

    # ---- 2) Anchor scan path (image-only creatives often use a#aw0) ----
    try:
        hrefs = await frame.evaluate(
            """
            () => Array.from(document.querySelectorAll('a[href]'))
                       .map(a => a.getAttribute('href'))
                       .filter(Boolean)
            """
        )
        for href in hrefs or []:
            try:
                parsed = urllib.parse.urlparse(href)
                if parsed.scheme not in ("http", "https"):
                    continue
                qs = urllib.parse.parse_qs(parsed.query)
                if parsed.netloc.endswith("googleadservices.com") and "adurl" in qs and qs["adurl"]:
                    _add(qs["adurl"][0])
                elif not parsed.netloc.endswith("googleadservices.com"):
                    _add(href)
            except Exception:
                continue
    except Exception:
        pass

    if not found:
        try:
            meta_payloads = await frame.evaluate(
                "() => Array.from(document.querySelectorAll('meta[data-asoch-meta]') || [])"
                ".map(m => m.getAttribute('data-asoch-meta') || '')"
            )
        except Exception:
            meta_payloads = []

        def _walk_meta(obj) -> None:
            if isinstance(obj, str):
                if obj.startswith("http"):
                    _add(obj)
                else:
                    for match in re.findall(r"https?://[^\s'\"]+", obj):
                        _add(match)
            elif isinstance(obj, list):
                for item in obj:
                    _walk_meta(item)
            elif isinstance(obj, dict):
                for value in obj.values():
                    _walk_meta(value)

        for raw in meta_payloads or []:
            if not raw:
                continue
            decoded = html.unescape(raw).replace("&amp;", "&")
            parsed_meta: Any
            try:
                parsed_meta = json.loads(decoded)
            except Exception:
                parsed_meta = None
            if parsed_meta is not None:
                _walk_meta(parsed_meta)
            _walk_meta(decoded)

    return found


# ============================
# Fletch click helpers (deterministic: screenshot only)
# ============================


async def extract_click_urls_from_fletch(frame) -> list[str]:
    """
    Extract click-through URLs from a FLETCH image creative (discover tile).

    Policy:
      1) Anchor-first: if any of #image-anchor, #header, or #visurl exist, return ONLY those hrefs
         (priority order, de-duplicated).
      2) Otherwise, consult `adData` fields and fall back to limited scanning as needed.

    Redirector noise is filtered. For googleadservices/doubleclick URLs, only the decoded
    `adurl` target is returned.
    """
    out: list[str] = []

    def _maybe_add(url: str) -> None:
        if not url:
            return
        try:
            parsed = urllib.parse.urlparse(url)
            if parsed.scheme not in ("http", "https"):
                return
            # Filter obvious noise
            host = parsed.netloc.lower()
            path = (parsed.path or "").lower()
            if host.endswith("tpc.googlesyndication.com") and path.startswith("/simgad"):
                return
            if host.endswith("tpc.googlesyndication.com") and "discover_ads" in path:
                return
            if host.endswith("googleads.g.doubleclick.net") and "pagead/conversion" in path:
                # keep only if it encodes an adurl
                qs = urllib.parse.parse_qs(parsed.query)
                if qs.get("adurl"):
                    url = qs["adurl"][0]
                else:
                    return
            if host.endswith("googleadservices.com"):
                qs = urllib.parse.parse_qs(parsed.query)
                if qs.get("adurl"):
                    url = qs["adurl"][0]
                else:
                    return
            # De-duplicate while preserving order
            if url not in out:
                out.append(url)
        except Exception:
            return

    # ---- 0) Anchor-first: honor the three well-known IDs if present ----
    try:
        anchor_hrefs = await frame.evaluate(
            "() => ['#image-anchor','#header','#visurl']"
            ".map(sel => { const a = document.querySelector(sel); return a ? a.getAttribute('href') : null; })"
            ".filter(Boolean)"
        )
    except Exception:
        anchor_hrefs = None

    if anchor_hrefs:
        for href in anchor_hrefs:
            _maybe_add(href)
        return out

    # ---- 1) adData path (fallback) ----
    try:
        data = await frame.evaluate(
            "() => { try { return (typeof adData === 'object' && adData) ? ({ "
            "redirect_url: adData.redirect_url || null, "
            "destination_url: adData.destination_url || null, "
            "google_click_url: adData.google_click_url || null "
            "}) : null; } catch { return null; } }"
        )
    except Exception:
        data = None

    if isinstance(data, dict):
        _maybe_add(data.get("redirect_url") or "")
        _maybe_add(data.get("destination_url") or "")
        _maybe_add(data.get("google_click_url") or "")

    # ---- 2) Legacy anchors (rare for FLETCH image) ----
    if not out:
        for sel in ("#image-anchor", "#header", "#visurl"):
            try:
                href = await frame.evaluate(
                    "(s) => { const a = document.querySelector(s); return a ? a.getAttribute('href') : null; }",
                    sel,
                )
            except Exception:
                href = None
            if href:
                _maybe_add(href)

    # ---- 3) Fallback: meta[data-asoch-meta] payloads ----
    if not out:
        try:
            meta_payloads = await frame.evaluate(
                "() => Array.from(document.querySelectorAll('meta[data-asoch-meta]') || [])"
                ".map(m => m.getAttribute('data-asoch-meta') || '')"
            )
        except Exception:
            meta_payloads = []

        def _walk_meta(obj) -> None:
            if isinstance(obj, str):
                if obj.startswith("http"):
                    _maybe_add(obj)
                else:
                    for match in re.findall(r"https?://[^\s\'\"]+", obj):
                        _maybe_add(match)
            elif isinstance(obj, list):
                for item in obj:
                    _walk_meta(item)
            elif isinstance(obj, dict):
                for value in obj.values():
                    _walk_meta(value)

        for raw in meta_payloads or []:
            if not raw:
                continue
            decoded = html.unescape(raw).replace("&amp;", "&")
            try:
                parsed: Any = json.loads(decoded)
            except Exception:
                parsed = None
            if parsed is not None:
                _walk_meta(parsed)
            _walk_meta(decoded)

    # ---- 4) Fallback: scan inline <script> tags for URL-like strings ----
    if not out:
        try:
            scripts_text = await frame.evaluate("() => Array.from(document.scripts || []).map(s => s.textContent || '').join('\\n')")
        except Exception:
            scripts_text = None
        if scripts_text:
            try:
                candidates = re.findall(r"https?://[^\s\'\"]+", scripts_text)
                for u in candidates:
                    _maybe_add(u)
            except Exception:
                pass

    return out


async def extract_click_urls_from_fletch_on_host(page: Page) -> list[str]:
    """
    Click URL extraction for FLETCH rendered on the host page (no inner navigation).
    Scope to the creative container; prefer well-known anchors; unwrap adurl targets.
    """
    found: list[str] = []

    def _maybe_add(url: str) -> None:
        if not url:
            return
        try:
            p = urllib.parse.urlparse(url)
            if p.scheme not in ("http", "https"):
                return
            host = p.netloc.lower()
            path = (p.path or "").lower()
            if host.endswith("tpc.googlesyndication.com") and (path.startswith("/simgad") or "discover_ads" in path):
                return
            if host.endswith("googleads.g.doubleclick.net") and "pagead/conversion" in path:
                qs = urllib.parse.parse_qs(p.query)
                url = qs.get("adurl", [None])[0] or ""
                if not url:
                    return
            if host.endswith("googleadservices.com"):
                qs = urllib.parse.parse_qs(p.query)
                url = qs.get("adurl", [None])[0] or ""
                if not url:
                    return
            if url not in found:
                found.append(url)
        except Exception:
            return

    try:
        hrefs = await page.evaluate(
            "() => {"
            "  const c = document.querySelector('creative-details, .creative-details-container');"
            "  if (!c) return [];"
            "  const out = [];"
            "  const push = (v) => { if (v) out.push(v); };"
            "  ['#image-anchor', '#header', '#visurl']"
            "    .forEach(sel => { const a = c.querySelector(sel); push(a && a.getAttribute('href')); });"
            "  const ifr = c.querySelector("
            "    \"iframe[id^='google_ad_'], iframe[name^='google_ads_iframe_'], "
            "     iframe[src*='discover_ads'], iframe[src*='googleads']\");"
            "  if (ifr) { const s = ifr.getAttribute('src'); if (s) out.push(s); }"
            "  return out.filter(Boolean);"
            "}"
        )
    except Exception:
        hrefs = []

    for h in hrefs or []:
        try:
            norm = normalize_click_url(h)
        except Exception:
            norm = None
        if norm:
            _maybe_add(norm)

    return found


# ============================
# FLETCH detection helper
# ============================
async def is_fletch(doc) -> bool:
    """
    Lightweight detector for FLETCH (Discover tile) frames.
    Signals (any one): id^='fletch-render', well-known anchors, painted inner ad iframe,
    or a global adData object. Works on a Frame or a Page.
    """
    try:
        return await doc.evaluate(
            "() => {"
            "  const hasFletchRender = !!document.querySelector(\"[id^='fletch-render']\");"
            "  const hasAnchors = ['#image-anchor','#header','#visurl']"
            "    .some((s) => !!document.querySelector(s));"
            "  const selInner = "
            "    \"iframe[id^='google_ad_'], iframe[name^='google_ads_iframe_'], \" + "
            "    \"iframe[src*='discover_ads'], iframe[src*='googleads']\";"
            "  const hasInner = !!document.querySelector(selInner);"
            "  let hasData = false; try { hasData = typeof adData === 'object' && adData; } catch(e) {}"
            "  return !!(hasFletchRender || hasAnchors || hasInner || hasData);"
            "}"
        )
    except Exception:
        return False


# ============================
# Discover page click helper
# ============================


async def extract_click_urls_from_discover_page(page: Page, *, bound_img_src: str | None = None) -> list[str]:
    """
    Conservative click URL extraction for the plain-<img> path on the creative details page.

    Policy (very strict to avoid false positives):
      - If bound_img_src is provided, only accept anchors that either:
          (a) directly wrap the target <img>, or
          (b) visually overlay the target <img> with intersection_area / img_area >= 0.6.
      - If bound_img_src is not provided, only accept well-known anchors (#image-anchor, #header, #visurl)
        within the creative container.
      - All candidates are normalized and infra/redirector hosts are filtered out.
    """
    # 1) Collect raw href candidates from the page using DOM geometry rules
    try:
        result = await page.evaluate(
            """
            (boundSrc) => {
              const container = document.querySelector('creative-details, .creative-details-container');
              if (!container) return { hrefs: [], why: 'no_container' };

              const getAbs = (url) => {
                try { const a = document.createElement('a'); a.href = url; return a.href; } catch { return url; }
              };

              const anchors = Array.from(container.querySelectorAll('a[href]'));
              const wellKnown = ['#image-anchor', '#header', '#visurl']
                .map(sel => { const a = container.querySelector(sel); return a && a.getAttribute('href'); })
                .filter(Boolean);

              if (!boundSrc) {
                return { hrefs: Array.from(new Set(wellKnown.map(getAbs))), why: 'no_bound_src' };
              }

              const imgs = Array.from(container.querySelectorAll('img'));
              // Loose match: exact src, or src without query, or startsWith when query params differ
              const matchImg = (src, cand) => {
                if (!src || !cand) return false;
                if (src === cand) return true;
                try {
                  const s0 = src.split('#')[0];
                  const s1 = cand.split('#')[0];
                  const [p0, q0] = s0.split('?');
                  const [p1, q1] = s1.split('?');
                  if (p0 && p1 && p0 === p1) return true;
                } catch {}
                return false;
              };

              const target = imgs.find(img => matchImg(boundSrc, img.getAttribute('src') || '')) || null;
              if (!target) return { hrefs: Array.from(new Set(wellKnown.map(getAbs))), why: 'no_target_img' };

              const rImg = target.getBoundingClientRect();
              const imgArea = Math.max(1, (rImg.width || 0) * (rImg.height || 0));

              const hrefs = new Set();
              for (const a of anchors) {
                const href = a.getAttribute('href');
                if (!href) continue;
                // Rule (a): anchor wraps the image
                if (a.contains(target)) { hrefs.add(getAbs(href)); continue; }
                // Rule (b): overlay intersection >= 0.6 of image area
                try {
                  const rA = a.getBoundingClientRect();
                  const ix = Math.max(0, Math.min(rImg.right, rA.right) - Math.max(rImg.left, rA.left));
                  const iy = Math.max(0, Math.min(rImg.bottom, rA.bottom) - Math.max(rImg.top, rA.top));
                  const inter = ix * iy;
                  if (inter / imgArea >= 0.6) { hrefs.add(getAbs(href)); continue; }
                } catch {}
              }

              // Always consider well-known IDs as a last resort
              for (const w of wellKnown) {
                const el = container.querySelector(w);
                if (el) { const h = el.getAttribute('href'); if (h) hrefs.add(getAbs(h)); }
              }
              return { hrefs: Array.from(hrefs), why: 'bound_src' };
            }
            """,
            bound_img_src or None,
        )
    except Exception:
        result = {"hrefs": [], "why": "eval_error"}

    raw_hrefs = result.get("hrefs", []) if isinstance(result, dict) else []

    # 2) Normalize and filter infra/redirector hosts
    out: list[str] = []
    for href in raw_hrefs:
        try:
            norm = normalize_click_url(href)
            if not norm:
                continue
            parsed = urllib.parse.urlparse(norm)
            host = (parsed.netloc or "").lower()
            # Filter infra/non-destination hosts
            if host.endswith("google.com") and not host.endswith("googleadservices.com"):
                continue
            if host.endswith("googleadservices.com"):
                # normalize_click_url should have unwrapped adurl already; if not, skip
                continue
            if host.endswith("doubleclick.net") or host.endswith("g.doubleclick.net"):
                continue
            if host.endswith("tpc.googlesyndication.com") or host.endswith("pagead2.googlesyndication.com"):
                continue
            if norm not in out:
                out.append(norm)
        except Exception:
            continue

    return out


# ============================
# Plain <img> variant helper
# ============================


async def find_plain_image_variants(page: Page) -> list[str]:
    """Return a de-duplicated list of <img> src URLs that are part of the creative.

    Some IMAGE creatives are a single <img> element rendered directly in the DOM
    (no sadbundle/fletch iframe). We consider any IMG that appears under
    <creative-details> (or its container) and exclude elements within targeting panels.
    """
    try:
        srcs: list[str] = await page.evaluate(
            """
            () => {
              const nodes = Array.from(
                document.querySelectorAll(
                  'creative-details img, .creative-details-container img'
                )
              );
              const out = [];
              for (const n of nodes) {
                if (!n) continue;
                if (n.closest('targeting-criteria')) continue;
                const inCreative = !!(n.closest('creative-details') || n.closest('.creative-details-container'));
                if (!inCreative) continue;
                const src = n.getAttribute('src') || '';
                if (!src) continue;
                out.push(src);
              }
              // preserve first-seen order but remove duplicates
              return Array.from(new Set(out));
            }
            """
        )
        return [s for s in srcs if isinstance(s, str) and s.startswith("http")]
    except Exception:
        return []


async def enumerate_gatc_carousel_variants(page: Page) -> list[dict]:
    """
    Strict GATC carousel enumerator.
    Returns a list of dicts with:
      - idx        : 1-based variant index (v1..v3)
      - dom_index  : 2..4 (the nth-child inside the carousel)
      - kind       : 'img' | 'iframe' | 'skip'  (never 'unknown')
      - src        : the iframe/img src (may be relative)
    We only consider the three creative tiles that GATC uses.
    """
    tiles = await page.query_selector_all("div.creative-container.creative-carousel > div.creative-sub-container")
    if not tiles:
        # Some layouts drop the creative-carousel wrapper and only emit creative-sub-container-si.
        tiles = await page.query_selector_all("div.creative-container .creative-sub-container")
    out: list[dict] = []
    for i, tile in enumerate(tiles[:3], start=1):
        dom_index = i + 1  # v1->2, v2->3, v3->4

        # Make sure hidden class doesn't block content discovery
        try:
            await tile.evaluate("(el)=>{ el.hidden=false; el.classList && el.classList.remove('hidden'); }")
        except Exception:
            pass

        iframe = await tile.query_selector(
            "creative > div > div > html-renderer > div > iframe, "
            "creative > div > html-renderer > div > iframe, "
            "creative html-renderer iframe"
        )
        if iframe:
            src = (await iframe.get_attribute("src")) or ""
            out.append({"idx": i, "dom_index": dom_index, "kind": "iframe", "src": src})
            continue

        img = await tile.query_selector(
            "creative > div > div > html-renderer > div > img, " "creative > div > html-renderer > div > img, " "creative html-renderer img"
        )
        if img:
            src = (await img.get_attribute("src")) or ""
            out.append({"idx": i, "dom_index": dom_index, "kind": "img", "src": src})
            continue

        fletch = await tile.query_selector(
            "fletch-renderer, creative fletch-renderer, creative > div fletch-renderer, creative > div > div > fletch-renderer"
        )
        if fletch:
            out.append({"idx": i, "dom_index": dom_index, "kind": "fletch", "src": ""})
            continue

        # Tile exists but has neither iframe nor img — preserve ordering but mark as skippable.
        out.append({"idx": i, "dom_index": dom_index, "kind": "skip", "src": ""})

    if out:
        return out

    # Stand-alone FLETCH creatives without carousel wrappers
    lone_fletch = await page.query_selector_all("div.creative-container fletch-renderer")
    if lone_fletch:
        return [{"idx": i, "dom_index": 0, "kind": "fletch", "src": ""} for i, _ in enumerate(lone_fletch, start=1)]

    return out


async def find_single_iframe_variant(page: Page) -> list[dict]:
    """
    Detect a single creative rendered as an <iframe> (no carousel).
    Returns [{idx:1, dom_index:0, kind:'iframe', src:<string or ''>}] or [].
    """
    selectors = [
        "creative-details html-renderer iframe",
        ".creative-details-container creative html-renderer iframe",
        "creative html-renderer iframe",
        "div.creative-container html-renderer iframe",
        "div.creative-container iframe",
    ]
    for sel in selectors:
        try:
            iframe = await page.wait_for_selector(sel, timeout=4000)
            if iframe:
                src = (await iframe.get_attribute("src")) or ""
                return [{"idx": 1, "dom_index": 0, "kind": "iframe", "src": src}]
        except Exception:
            continue
    return []


# ============================
# Core ad processing
# ============================


async def _process_with_browser(
    active_browser,
    owns_browser: bool,
    *,
    con,
    storage_client: storage.Client,
    bucket_name: str,
    user_agent: str,
    ad_id: str,
    ad_url: str,
    adv: str,
    dry_run: bool,
    trace: bool,
    args: CliArgs | None,
) -> str:
    if args is None:
        raise ValueError("CliArgs must be provided to _process_with_browser")
    # Do not upsert_pending here; instead, upsert per-variant below.
    adlog("ad_start", ad_id=ad_id, advertiser_id=adv, url=ad_url, ad_type="IMAGE")
    upsert_pending(
        con,
        ad_id,
        adv,
        variant_id=IMAGE_VARIANT_ID,
        source_url=ad_url,
        dry_run=dry_run,
    )

    context = None
    try:
        if hasattr(active_browser, "is_connected") and not active_browser.is_connected():
            raise BrowserRestartRequired("browser disconnected")
        try:
            context = await active_browser.new_context(
                user_agent=user_agent,
                device_scale_factor=(args.device_scale_factor if args is not None else DEFAULT_DEVICE_SCALE_FACTOR),
            )
        except PlaywrightError as exc:
            raise BrowserRestartRequired(str(exc)) from exc
        t = _timeouts(args)
        context.set_default_timeout(t.page_ms)
        if trace:
            await context.tracing.start(screenshots=True, snapshots=True, sources=True)

        try:
            page = await context.new_page()
        except PlaywrightError as exc:
            raise BrowserRestartRequired(str(exc)) from exc
        http = _make_http(user_agent, ad_url)

        try:
            await page.goto(
                ad_url,
                wait_until="domcontentloaded",
                timeout=t.page_ms,
            )
        except PlaywrightError as exc:
            raise BrowserRestartRequired(str(exc)) from exc

        # Optional: dump HTML for debugging
        if args.debug_html:
            await ensure_debug_html(page, ad_id)

        # Optional: iframe inventory for diagnosis
        if args.debug_frames:
            frames_info = await dump_frame_inventory(page)
            jlog("info", event="frame_inventory", ad_id=ad_id, frames=frames_info)

        # Early terminal-state check (policy / 429 / not found)
        err = await wait_policy_or_errors(page)
        if err in {"removed_for_policy_violation", "rate_limited_429", "not_found", "variation_unavailable"}:
            record_status(con, adv, ad_id, err, variant_id=IMAGE_VARIANT_ID, dry_run=dry_run)
            return "terminal"
        elif err:
            record_status(con, adv, ad_id, "error", err, variant_id=IMAGE_VARIANT_ID, dry_run=dry_run)
            return "error"
        captured_any = False

        # --- Capture path: first try carousel, then single-iframe, then plain <img> ---
        variants = await enumerate_gatc_carousel_variants(page)

        if args is not None and getattr(args, "debug_frames", False):
            try:
                jlog("info", event="carousel_variants", ad_id=ad_id, advertiser_id=str(adv), variants=variants)
            except Exception:
                pass

        # If no carousel, try single <iframe> creative
        if not variants:
            single_ifr = await find_single_iframe_variant(page)
            if single_ifr:
                variants = single_ifr

        # If still nothing, try host-page FLETCH detection (Discover tile rendered in host DOM)
        if not variants:
            try:
                if await is_fletch(page):
                    variants = [{"idx": 1, "dom_index": 0, "kind": "fletch", "src": ""}]
                    try:
                        jlog("info", event="fletch_host_detected", ad_id=ad_id, advertiser_id=str(adv))
                    except Exception:
                        pass
            except Exception:
                pass

        # If still nothing, try plain <img> in DOM
        if not variants:
            try:
                await page.wait_for_selector(
                    "creative-details img, .creative-details-container img",
                    timeout=t.iframe_ms,
                )
            except TimeoutError:
                pass
            plain_imgs = await find_plain_image_variants(page)
            if plain_imgs:
                for i, src in enumerate(plain_imgs, start=1):
                    variant_id = f"v{i}"
                    upsert_pending(con, ad_id, adv, variant_id=variant_id, source_url=ad_url, dry_run=dry_run)
                    adlog("variant_attempt", ad_id=ad_id, advertiser_id=adv, url=ad_url, render_method="img", variant_id=variant_id)
                    try:
                        resp = http.get(src, timeout=20)
                        resp.raise_for_status()
                        try:
                            clicks = await extract_click_urls_from_discover_page(page, bound_img_src=src)
                        except Exception:
                            clicks = []
                        norm_click = await _persist_primary_click(con, adv, ad_id, variant_id, "img", clicks, dry_run=dry_run)
                        await _finalize_capture(
                            con,
                            storage_client,
                            bucket_name,
                            adv,
                            ad_id,
                            "img",
                            resp.content,
                            dry_run,
                            source_url=ad_url,
                            variant_id=variant_id,
                            click_url=norm_click,
                            capture_method=CAPTURE_METHOD_IMG,
                            capture_target="img[src]",
                        )
                        captured_any = True
                        if _stop_after_first(args):
                            return "done"
                    except Exception as e:
                        record_error(con, adv, ad_id, f"plain_img_fetch_error: {e}", variant_id=variant_id, dry_run=dry_run)
            if not captured_any:
                err = await wait_policy_or_errors(page)
                if err in {"removed_for_policy_violation", "rate_limited_429", "not_found", "variation_unavailable"}:
                    record_status(con, adv, ad_id, err, variant_id=IMAGE_VARIANT_ID, dry_run=dry_run)
                    return "terminal"
                record_status(con, adv, ad_id, "error", "no_renderer_or_img_in_creative", variant_id=IMAGE_VARIANT_ID, dry_run=dry_run)
                return "error"

        # We have variants; iterate in UI order (idx=v1..v3) regardless of lazy-load state.
        for i, v in enumerate(variants, start=1):
            # Prefer stable idx from the enumerator; fall back to enumeration order.
            vidx = int(v.get("idx") or i)
            variant_id = f"v{vidx}"
            kind = (v.get("kind") or "").lower()
            src_abs = v.get("src") or ""
            dom_index = int(v.get("dom_index") or 0)

            # Always activate the tile before any DOM reads (normalizes lazy-load and visibility)
            if dom_index > 0:
                try:
                    probed = await _activate_and_probe_tile(page, dom_index, vidx, ad_url)
                    if (kind in ("sadbundle", "iframe")) and not src_abs:
                        src_abs = probed.get("iframe") or ""
                    if (kind == "img") and not src_abs:
                        src_abs = probed.get("img") or ""
                except Exception:
                    pass

            upsert_pending(con, ad_id, adv, variant_id=variant_id, source_url=ad_url, dry_run=dry_run)

            # --- FLETCH (Discover tile): simple host-element screenshot + inner-frame click extraction
            if kind == "fletch":
                # Emit a variant_attempt log for parity with other renderers
                adlog(
                    "variant_attempt",
                    ad_id=ad_id,
                    advertiser_id=adv,
                    url=ad_url,
                    render_method="fletch",
                    variant_id=variant_id,
                )

                # Scope to the carousel tile if present
                selector_prefixes = _tile_selector_candidates(dom_index, vidx) if dom_index > 0 else []
                sel_variant = ""
                for candidate in selector_prefixes:
                    if not candidate:
                        continue
                    try:
                        handle = await page.query_selector(candidate)
                    except Exception:
                        handle = None
                    if handle:
                        sel_variant = candidate
                        try:
                            await handle.dispose()
                        except Exception:
                            pass
                        break
                if not sel_variant and selector_prefixes:
                    sel_variant = selector_prefixes[0]

                # Make the tile visible/active and unhide it if needed
                if sel_variant:
                    try:
                        await page.evaluate(
                            "(sel) => { const el = document.querySelector(sel); if (el) {"
                            "  el.scrollIntoView({block:'center'});"
                            "  el.classList?.remove('hidden'); el.removeAttribute('hidden');"
                            "} }",
                            sel_variant,
                        )
                    except Exception:
                        pass

                # Locate the FLETCH container on the host page
                fe = None
                root_selector_candidates: list[str] = []
                seen: set[str] = set()
                ordered_prefixes = [sel_variant] if sel_variant else []
                ordered_prefixes.extend([p for p in selector_prefixes if p and p != sel_variant])
                for prefix in ordered_prefixes:
                    for suffix in (" fletch-renderer", " [id^='fletch-render']"):
                        candidate = f"{prefix}{suffix}"
                        if candidate not in seen:
                            root_selector_candidates.append(candidate.strip())
                            seen.add(candidate)
                for fallback in ("fletch-renderer", "[id^='fletch-render']"):
                    if fallback not in seen:
                        root_selector_candidates.append(fallback)
                        seen.add(fallback)
                for candidate in root_selector_candidates:
                    try:
                        fe = await page.query_selector(candidate)
                    except Exception:
                        fe = None
                    if fe:
                        break

                # If policy banner is now visible, treat as removed
                try:
                    policy_banner = await page.query_selector("div.policy-violation-banner")
                except Exception:
                    policy_banner = None
                if await element_is_visibly_displayed(policy_banner):
                    record_status(
                        con,
                        adv,
                        ad_id,
                        "removed_for_policy_violation",
                        variant_id=variant_id,
                        dry_run=dry_run,
                    )
                    return "terminal"

                # Helper: unclip ancestors and center an element
                async def _unclip_and_center_element(el):
                    try:
                        await page.evaluate(
                            "(e) => {"
                            "  let p = e; "
                            "  while (p) {"
                            "    try {"
                            "      const s = p.style || {}; "
                            "      s.overflow = 'visible'; s.clipPath = 'none'; s.webkitClipPath = 'none';"
                            "      s.mask = 'none'; s.webkitMask = 'none'; s.maxHeight = 'none'; s.maxWidth = 'none';"
                            "    } catch(_) {}"
                            "    p = p.parentElement;"
                            "  }"
                            "  try { e.scrollIntoView({block:'center', inline:'center'}); } catch(_) {}"
                            "}",
                            fe,
                        )
                    except Exception:
                        pass

                # Take a stability-window screenshot of the FLETCH root (host DOM)
                png_f = None
                if fe:
                    try:
                        await _unclip_and_center_element(fe)
                        loc = fe.locator(":scope")
                        png_f = await _settle_screenshot(
                            lambda: loc.screenshot(type="png"),
                            max_wait_ms=FLETCH_SETTLE_MAX_MS,
                            min_stable_ms=FLETCH_SETTLE_MIN_STABLE_MS,
                            interval_ms=DEFAULT_SETTLE_INTERVAL_MS,
                            near_epsilon=3,
                            min_observe_ms=FLETCH_SETTLE_MIN_OBSERVE_MS,
                        )
                    except Exception:
                        try:
                            png_f = await fe.screenshot(type="png")
                        except Exception:
                            png_f = None

                # Try to extract click URL(s) from the inner iframe inside the FLETCH root (same-origin /adframe)
                clicks_f: list[str] = []
                try:
                    if fe:
                        inner_ifr = await fe.query_selector("iframe")
                        if inner_ifr:
                            try:
                                fr = await inner_ifr.content_frame()
                            except Exception:
                                fr = None
                            if fr:
                                try:
                                    clicks_f = await extract_click_urls_from_fletch(fr)
                                except Exception:
                                    clicks_f = []
                except Exception:
                    clicks_f = []

                # If inner-frame yielded nothing, fall back to host anchors
                if not clicks_f:
                    try:
                        clicks_f = await extract_click_urls_from_fletch_on_host(page)
                    except Exception:
                        clicks_f = []

                norm_click_f = await _persist_primary_click(con, adv, ad_id, variant_id, "fletch", clicks_f, dry_run=dry_run)

                if png_f:
                    await _finalize_capture(
                        con,
                        storage_client,
                        bucket_name,
                        adv,
                        ad_id,
                        "fletch",
                        png_f,
                        dry_run,
                        source_url=ad_url,
                        variant_id=variant_id,
                        click_url=norm_click_f,
                        capture_method=CAPTURE_METHOD_SCREENSHOT,
                        capture_target=("fletch_tile" if dom_index > 0 else "fletch_host"),
                    )
                    captured_any = True
                    if _stop_after_first(args):
                        return "done"
                    continue

            adlog(
                "variant_attempt",
                ad_id=ad_id,
                advertiser_id=adv,
                url=ad_url,
                render_method=("sadbundle" if kind in ("sadbundle", "iframe") else (kind or "unknown")),
                variant_id=variant_id,
            )

            # --- Direct <img> path
            if kind == "img" and src_abs:
                try:
                    resp = http.get(src_abs, timeout=20)
                    resp.raise_for_status()
                    try:
                        clicks = await extract_click_urls_from_discover_page(page, bound_img_src=src_abs)
                    except Exception:
                        clicks = []
                    norm_click = await _persist_primary_click(con, adv, ad_id, variant_id, "img", clicks, dry_run=dry_run)
                    await _finalize_capture(
                        con,
                        storage_client,
                        bucket_name,
                        adv,
                        ad_id,
                        "img",
                        resp.content,
                        dry_run,
                        source_url=ad_url,
                        variant_id=variant_id,
                        click_url=norm_click,
                        capture_method=CAPTURE_METHOD_IMG,
                        capture_target="img[src]",
                    )
                    captured_any = True
                    if _stop_after_first(args):
                        return "done"
                    continue
                except Exception as e:
                    record_error(con, adv, ad_id, f"img_variant_fetch_error: {e}", variant_id=variant_id, dry_run=dry_run)
                    continue

            # --- Iframe/sadbundle path
            # If the iframe URL is missing (lazy-load), activate this carousel child first.
            if kind in ("sadbundle", "iframe") and not src_abs and dom_index > 0:
                try:
                    probed = await _activate_and_probe_tile(page, dom_index, vidx, ad_url)
                    src_abs = probed.get("iframe") or ""
                except Exception:
                    pass

            if kind in ("sadbundle", "iframe") and not _is_meaningful_src(src_abs):
                src_abs = ""

            if kind in ("sadbundle", "iframe"):
                if src_abs:
                    iframe_page = await context.new_page()
                    captured_variant = False
                    try:
                        await iframe_page.goto(
                            src_abs,
                            wait_until="domcontentloaded",
                            timeout=t.page_ms,
                        )
                        # Optional: dump inner iframe HTML for debugging
                        if args and getattr(args, "debug_html", False):
                            try:
                                await ensure_debug_html(iframe_page, f"{ad_id}_{variant_id}")
                            except Exception:
                                pass

                        # If the creative DOM isn't in this document, hop one level down if there's a child iframe.
                        try:
                            has_creative = await iframe_page.evaluate(
                                "() => !!document.querySelector("
                                "'#mys-content, #google_image_div, gwd-google-ad, gwd-page, "
                                "#page1, .gwd-page-content, svg.image'"
                                ")"
                            )
                        except Exception:
                            has_creative = False

                        if not has_creative:
                            try:
                                nested_src = await iframe_page.evaluate(
                                    "() => { const fr = document.querySelector('iframe[src]'); " "return fr && fr.getAttribute('src'); }"
                                )
                            except Exception:
                                nested_src = None

                            if nested_src:
                                try:
                                    await iframe_page.goto(
                                        urllib.parse.urljoin(iframe_page.url, nested_src),
                                        wait_until="domcontentloaded",
                                        timeout=t.page_ms,
                                    )
                                except Exception:
                                    pass

                        # FLETCH inside inner iframe: intentionally ignored here; the host-page branch handles FLETCH
                        # Fall through to SADBUNDLE/GWD logic below.
                        # 1) Try raw image bytes first (pure image SADBUNDLE case)
                        img_bytes = await fetch_sadbundle_image_bytes(
                            iframe_page,
                            frame_url=iframe_page.url,
                            http=http,
                            iframe_timeout_ms=t.iframe_ms,
                        )
                        if img_bytes:
                            try:
                                clicks = await extract_click_urls_from_frame(iframe_page)
                            except Exception:
                                clicks = []
                            norm_click = await _persist_primary_click(con, adv, ad_id, variant_id, "sadbundle", clicks, dry_run=dry_run)
                            await _finalize_capture(
                                con,
                                storage_client,
                                bucket_name,
                                adv,
                                ad_id,
                                "sadbundle",
                                img_bytes,
                                dry_run,
                                source_url=ad_url,
                                variant_id=variant_id,
                                click_url=norm_click,
                                capture_method=CAPTURE_METHOD_IMG,
                                capture_target="sadbundle_img",
                            )
                            captured_any = True
                            captured_variant = True
                            if _stop_after_first(args):
                                return "done"

                        # 2) DOM screenshot path (GWD or templated DOM)
                        if not captured_variant:
                            gwd = await sniff_gwd(iframe_page, t.iframe_ms)

                            png, sel = await screenshot_sadbundle_element(
                                iframe_page,
                                t.iframe_ms,
                                preferred_selectors=None,  # '#mys-content' et al. covered internally
                            )

                            try:
                                clicks = await extract_click_urls_from_frame(iframe_page)
                            except Exception:
                                clicks = []
                            norm_click = await _persist_primary_click(con, adv, ad_id, variant_id, "sadbundle", clicks, dry_run=dry_run)

                            if png:
                                cap_target = sel or "sad_dom"
                                if gwd.get("is_gwd"):
                                    cap_target = f"{cap_target}[gwd:{gwd.get('marker')}]"
                                await _finalize_capture(
                                    con,
                                    storage_client,
                                    bucket_name,
                                    adv,
                                    ad_id,
                                    "sadbundle",
                                    png,
                                    dry_run,
                                    source_url=ad_url,
                                    variant_id=variant_id,
                                    click_url=norm_click,
                                    capture_method=CAPTURE_METHOD_SCREENSHOT,
                                    capture_target=cap_target,
                                )
                                captured_any = True
                                captured_variant = True
                                if _stop_after_first(args):
                                    return "done"

                        # 3) Last-ditch fallback: full-page screenshot of the iframe document
                        if not captured_variant:
                            png_generic = None
                            try:
                                # Try to center likely containers if present
                                try:
                                    await iframe_page.evaluate(
                                        "() => { const el = document.querySelector('#mys-content, #google_image_div'); "
                                        "if (el) { el.scrollIntoView({block:'center', inline:'center'}); } }"
                                    )
                                except Exception:
                                    pass
                                png_generic = await _settle_screenshot(
                                    lambda: iframe_page.screenshot(type="png", full_page=True),
                                    max_wait_ms=8000,
                                    min_stable_ms=1200,
                                    interval_ms=250,
                                    near_epsilon=3,
                                    min_observe_ms=800,
                                )
                            except Exception:
                                png_generic = None

                            if png_generic:
                                try:
                                    jlog(
                                        "info",
                                        event="sadbundle_fullpage_fallback",
                                        ad_id=ad_id,
                                        advertiser_id=str(adv),
                                        variant_id=variant_id,
                                    )
                                except Exception:
                                    pass
                                await _finalize_capture(
                                    con,
                                    storage_client,
                                    bucket_name,
                                    adv,
                                    ad_id,
                                    "sadbundle",
                                    png_generic,
                                    dry_run,
                                    source_url=ad_url,
                                    variant_id=variant_id,
                                    click_url=None,
                                    capture_method=CAPTURE_METHOD_SCREENSHOT,
                                    capture_target="sad_fullpage",
                                )
                                captured_any = True
                                captured_variant = True
                                if _stop_after_first(args):
                                    return "done"
                    finally:
                        await iframe_page.close()

                    # Only skip to the next variant if we actually captured something in this branch.
                    if captured_variant:
                        continue  # handled success; next variant

                    # No URL even after activation: fall back to host element screenshot of that child.
                    if dom_index > 0:
                        try:
                            sel_variant = f".creative-container.creative-carousel > div:nth-child({dom_index})"
                            # Prefer the child's iframe if present; otherwise the child itself.
                            fe = await page.query_selector(f"{sel_variant} iframe") or await page.query_selector(sel_variant)
                            if fe:
                                try:
                                    await fe.scroll_into_view_if_needed()
                                except Exception:
                                    pass
                                try:
                                    await page.evaluate(
                                        "(el) => { "
                                        "  let p = el; "
                                        "  while (p) { "
                                        "    try { "
                                        "      const s = p.style; "
                                        "      s.overflow = 'visible'; "
                                        "      s.clipPath = 'none'; "
                                        "      s.webkitClipPath = 'none'; "
                                        "      s.mask = 'none'; "
                                        "      s.webkitMask = 'none'; "
                                        "      s.maxHeight = 'none'; "
                                        "      s.maxWidth = 'none'; "
                                        "    } catch(_) {} "
                                        "    p = p.parentElement; "
                                        "  } "
                                        "}",
                                        fe,
                                    )
                                except Exception:
                                    pass
                                png = await stabilized_host_iframe_screenshot(fe)
                            else:
                                png = None
                        except Exception:
                            png = None

                    if png:
                        await _finalize_capture(
                            con,
                            storage_client,
                            bucket_name,
                            adv,
                            ad_id,
                            "sadbundle",
                            png,
                            dry_run,
                            source_url=ad_url,
                            variant_id=variant_id,
                            click_url=None,
                            capture_method=CAPTURE_METHOD_SCREENSHOT,
                            capture_target="host_iframe",
                        )
                        captured_any = True
                        if _stop_after_first(args):
                            return "done"
                        continue
        if captured_any:
            return "done"
        # If we got here, no captures succeeded; fall back to policy/error checks
        err = await wait_policy_or_errors(page)
        if err in {"removed_for_policy_violation", "rate_limited_429", "not_found", "variation_unavailable"}:
            record_status(con, adv, ad_id, err, variant_id=IMAGE_VARIANT_ID, dry_run=dry_run)
            return "terminal"
        record_status(con, adv, ad_id, "error", "no_variant_captured", variant_id=IMAGE_VARIANT_ID, dry_run=dry_run)
        return "error"
    except PlaywrightError as exc:
        raise BrowserRestartRequired(str(exc)) from exc
    finally:
        await cleanup_playwright(context, active_browser if owns_browser else None, trace, ad_id)


async def process_ad(
    con,
    storage_client: storage.Client,
    bucket_name: str,
    user_agent: str,
    ad_id: str,
    ad_url: str,
    adv: str,
    dry_run: bool,
    trace: bool,
    args=None,
    *,
    browser=None,
) -> str:
    """Process one ad end‑to‑end. Returns 'done' | 'terminal' | 'error'."""

    manage_browser = browser is None
    try:
        if manage_browser:
            async with async_playwright() as pw:
                active_browser = await pw.chromium.launch(headless=True, args=CHROMIUM_LAUNCH_ARGS)
                return await _process_with_browser(
                    active_browser,
                    True,
                    con=con,
                    storage_client=storage_client,
                    bucket_name=bucket_name,
                    user_agent=user_agent,
                    ad_id=ad_id,
                    ad_url=ad_url,
                    adv=adv,
                    dry_run=dry_run,
                    trace=trace,
                    args=args,
                )
        return await _process_with_browser(
            browser,
            False,
            con=con,
            storage_client=storage_client,
            bucket_name=bucket_name,
            user_agent=user_agent,
            ad_id=ad_id,
            ad_url=ad_url,
            adv=adv,
            dry_run=dry_run,
            trace=trace,
            args=args,
        )
    except BrowserRestartRequired as exc:
        if manage_browser:
            record_error(con, adv, ad_id, f"exception: {exc}", variant_id=IMAGE_VARIANT_ID, dry_run=dry_run)
            return "error"
        raise
    except Exception as e:  # outermost safety net per ad
        record_error(con, adv, ad_id, f"exception: {e}", variant_id=IMAGE_VARIANT_ID, dry_run=dry_run)
        return "error"


async def _finalize_capture(
    con,
    storage_client,
    bucket_name: str,
    adv: str,
    ad_id: str,
    render_method: str,
    png_bytes: bytes,
    dry_run: bool,
    *,
    capture_method: str,
    capture_target: str,
    source_url: str | None,
    variant_id: str,
    click_url: str | None = None,
) -> None:
    """
    Canonicalize PNG, upload to GCS with ordered metadata, and mark DB success.
    Assumes caller has already upserted 'pending' for (ad_id, variant_id).
    """
    # Normalize and hash
    norm_png, sha256, phash, width, height = normalize_and_hash(png_bytes, trim=True)

    ocr_result: OCRResult | None = None
    if not dry_run:
        try:
            ocr_result = await asyncio.to_thread(extract_text_from_image, norm_png)
        except Exception as exc:
            jlog(
                "error",
                event="ocr_failed",
                ad_type="IMAGE",
                advertiser_id=adv,
                ad_id=ad_id,
                variant_id=variant_id,
                error=repr(exc),
            )
    ocr_text = sanitize_ocr_text(ocr_result.text) if ocr_result else None
    ocr_language = ocr_result.language if ocr_result else None
    ocr_confidence = ocr_result.confidence if ocr_result else None

    # Canonical asset identity & storage path
    asset_id = sha256
    fbytes = len(norm_png)
    canonical_gcs = canonical_asset_path(bucket_name, asset_id)

    # Ordered object metadata
    metadata = build_gcs_metadata(
        ad_type="IMAGE",
        ad_id=ad_id,
        advertiser_id=adv,
        variant_id=variant_id,
        render_method=render_method,
        capture_method=capture_method,
        capture_target=capture_target,
        width=width,
        height=height,
        sha256=asset_id,
        phash=phash,
        scraper_version=get_scraper_version(),
        source_url=source_url,
        click_url=click_url,
    )

    # Upload canonical asset
    upload_png(
        storage_client,
        bucket_name,
        canonical_gcs,
        norm_png,
        metadata=metadata,
        dry_run=dry_run,
    )

    # Ensure assets row exists
    ensure_asset_row(
        con,
        asset_id=asset_id,
        phash=phash,
        w=width,
        h=height,
        fbytes=fbytes,
        canonical_gcs=canonical_gcs,
        dry_run=dry_run,
    )

    # Link success to ads row (variant-scoped)
    link_asset_success(
        con,
        adv_id=adv,
        ad_id=ad_id,
        asset_id=asset_id,
        rmethod=render_method,
        w=width,
        h=height,
        fbytes=fbytes,
        phash=phash,
        canonical_gcs=canonical_gcs,
        variant_id=variant_id,
        dry_run=dry_run,
        capture_method=capture_method,
        capture_target=capture_target,
        click_url=click_url,
        ocr_text=ocr_text,
        ocr_language=ocr_language,
        ocr_confidence=ocr_confidence,
    )


# ============================
# BigQuery streaming & workers
# ============================


def iter_manifest_entries(path: str) -> Iterable[tuple[str, str, str]]:
    """
    Yield (ad_id, ad_url, advertiser_id) tuples from a manifest file.

    Accepted formats:
    - CSV with headers (ad_id, ad_url, advertiser_id)
    - JSON Lines where each object provides the same keys (camelCase variants allowed)
    """
    resolved = os.path.expanduser(path)
    try:
        with open(resolved, "r", encoding="utf-8") as fh:
            probe = ""
            while True:
                pos = fh.tell()
                line = fh.readline()
                if not line:
                    break
                stripped = line.strip()
                if stripped:
                    probe = stripped
                    fh.seek(pos)
                    break
            if not probe:
                return
            if probe.startswith("{") or resolved.endswith((".jsonl", ".json")):
                for raw in fh:
                    raw = raw.strip()
                    if not raw:
                        continue
                    data = json.loads(raw)
                    ad = str(data.get("ad_id") or data.get("adId") or "").strip()
                    url = str(data.get("ad_url") or data.get("adUrl") or "").strip()
                    adv = str(data.get("advertiser_id") or data.get("advertiserId") or "").strip()
                    if not ad or not adv or not url:
                        raise ValueError(f"Manifest entry missing ad_id/ad_url/advertiser_id: {raw}")
                    yield ad, url, adv
            else:
                fh.seek(0)
                reader = csv.DictReader(fh)
                if not reader.fieldnames:
                    raise ValueError("Manifest CSV must include headers ad_id, ad_url, advertiser_id.")
                for row in reader:
                    if not row:
                        continue
                    ad = (row.get("ad_id") or row.get("Ad_ID") or "").strip()
                    url = (row.get("ad_url") or row.get("Ad_URL") or "").strip()
                    adv = (row.get("advertiser_id") or row.get("Advertiser_ID") or "").strip()
                    if not ad or not adv or not url:
                        raise ValueError(f"Manifest CSV row missing ad_id/ad_url/advertiser_id: {row}")
                    yield ad, url, adv
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Manifest file not found: {resolved}") from exc


def fetch_image_ads_stream(
    client: bigquery.Client,
    batch_size: int,
    start_date: str | None,
    end_date: str | None,
    order_by: str,
    sql_limit: int | None,
    bq_location: str | None,
) -> Iterable[tuple[str, str, str]]:
    """Yield (Ad_ID, Ad_URL, Advertiser_ID) respecting filters and order."""
    filters = ["Ad_Type='IMAGE'", r"REGEXP_CONTAINS(Regions, r'\bUS\b')"]
    if start_date:
        filters.append(f"date_range_start >= '{start_date}'")
    if end_date:
        filters.append(f"date_range_start <= '{end_date}'")
    where_clause = " AND ".join(filters)

    order_clause = ""
    if order_by == "date_asc":
        order_clause = "ORDER BY date_range_start ASC"
    elif order_by == "date_desc":
        order_clause = "ORDER BY date_range_start DESC"
    elif order_by == "advertiser":
        order_clause = "ORDER BY Advertiser_ID ASC"

    sql = f"""
    SELECT Ad_ID, Ad_URL, Advertiser_ID
      FROM `bigquery-public-data.google_political_ads.creative_stats`
     WHERE {where_clause}
      {order_clause}
      {('LIMIT ' + str(sql_limit)) if sql_limit else ''}
    """.strip()
    jlog("info", event="bq_query", sql=sql)

    page_sz = batch_size
    if sql_limit:
        page_sz = max(1, min(batch_size, sql_limit))
    job_config = None  # location is passed to Client.query(), not QueryJobConfig
    job = client.query(sql, job_config=job_config, location=bq_location)
    iterator = job.result(page_size=page_sz)
    for page in iterator.pages:
        for r in page:
            yield (str(r.Ad_ID), str(r.Ad_URL), str(r.Advertiser_ID))


async def monitor_summary(con, interval: int = 60) -> None:
    """Periodic status + renderer summaries for quick situational awareness."""
    while True:
        with con.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) FROM ads GROUP BY status ORDER BY 2 DESC")
            rows = cur.fetchall()
            cur.execute(
                (
                    "SELECT COALESCE(render_method,'(null)'), COUNT(*) "
                    "FROM ads WHERE status='done' "
                    "GROUP BY COALESCE(render_method,'(null)') ORDER BY 2 DESC"
                )
            )
            r_rows = cur.fetchall()
        jlog("info", event="status_summary", rows=rows)
        jlog("info", event="renderer_summary", rows=r_rows)
        await asyncio.sleep(interval)


async def producer(queue: asyncio.Queue, con, bq_client: bigquery.Client, args: CliArgs) -> None:
    """Enqueue ad jobs from URL/ID or streaming BigQuery iterator."""
    sent = 0

    if args.manifest_path:
        jlog(
            "info",
            event="manifest_stream_start",
            path=args.manifest_path,
            shard=args.shard,
            shard_count=args.shard_count,
        )
        for ad_id, ad_url, adv in iter_manifest_entries(args.manifest_path):
            if args.skip_advertisers and adv in args.skip_advertisers:
                continue
            if args.shard_count > 1 and (stable_int_hash(ad_id) % args.shard_count != args.shard):
                continue
            if not ad_url:
                raise ValueError(f"Manifest entry missing ad_url for ad_id={ad_id}")
            await queue.put((ad_id, ad_url, adv))
            sent += 1
            if args.max_ads and sent >= args.max_ads:
                break
        for _ in range(args.concurrency):
            await queue.put(None)
        return

    # Direct URL: bypass BigQuery
    if args.ad_url:
        adv_from_url, ad_from_url = parse_ids_from_url(args.ad_url)
        ad_candidate = args.ad_id or ad_from_url
        adv_candidate = args.advertiser_id or adv_from_url
        if not ad_candidate or not adv_candidate:
            raise RuntimeError(
                "When using --ad-url, either the URL must contain both IDs or you must also provide --ad-id and --advertiser-id."
            )
        ad = ad_candidate
        adv = adv_candidate
        if args.shard_count > 1 and (stable_int_hash(ad) % args.shard_count != args.shard):
            for _ in range(args.concurrency):
                await queue.put(None)
            return
        jlog(
            "info",
            event="enqueue_direct_url",
            ad_id=ad,
            advertiser_id=adv,
            url=args.ad_url,
            shard=args.shard,
            shard_count=args.shard_count,
        )
        await queue.put((ad, args.ad_url, adv))
        for _ in range(args.concurrency):
            await queue.put(None)
        return

    # One‑ad by BigQuery lookup
    if args.ad_id:
        sql = """
        SELECT Ad_ID, Ad_URL, Advertiser_ID
          FROM `bigquery-public-data.google_political_ads.creative_stats`
         WHERE Ad_ID = @ad_id AND Ad_Type='IMAGE' AND REGEXP_CONTAINS(Regions, r'\\bUS\\b')
         LIMIT 1
        """.strip()
        qcfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("ad_id", "STRING", args.ad_id)])
        job = bq_client.query(sql, job_config=qcfg, location=args.bq_location)
        rows = list(job.result())
        if rows:
            r = rows[0]
            adid, url, adv = str(r.Ad_ID), str(r.Ad_URL), str(r.Advertiser_ID)
            if args.shard_count <= 1 or (stable_int_hash(adid) % args.shard_count == args.shard):
                jlog(
                    "info",
                    event="enqueue_single_ad",
                    ad_id=adid,
                    advertiser_id=adv,
                    url=url,
                    shard=args.shard,
                    shard_count=args.shard_count,
                )
                await queue.put((adid, url, adv))
        for _ in range(args.concurrency):
            await queue.put(None)
        return

    # Streaming mode
    jlog(
        "info",
        event="enqueue_streaming_start",
        start_date=args.start_date,
        end_date=args.end_date,
        order_by=args.order_by,
        sql_limit=args.sql_limit,
        batch_size=args.batch_size,
        shard=args.shard,
        shard_count=args.shard_count,
    )
    for ad_id, ad_url, adv in fetch_image_ads_stream(
        bq_client,
        args.batch_size,
        args.start_date,
        args.end_date,
        args.order_by,
        args.sql_limit,
        args.bq_location,
    ):
        if adv in args.skip_advertisers:
            continue
        if args.shard_count > 1 and (stable_int_hash(ad_id) % args.shard_count != args.shard):
            continue
        row: tuple[str] | None = None
        with con.cursor() as cur:
            cur.execute(
                "SELECT status FROM ads WHERE ad_id=%s AND variant_id=%s",
                (ad_id, IMAGE_VARIANT_ID),
            )
            row = cur.fetchone()
        if not args.rescrape_done and row and row[0] not in ("pending", "error", "rate_limited_429"):
            jlog(
                "info",
                event="skip_existing_success",
                ad_id=ad_id,
                advertiser_id=adv,
                status=row[0],
            )
            continue
        await queue.put((ad_id, ad_url, adv))
        sent += 1
        if args.max_ads and sent >= args.max_ads:
            break

    for _ in range(args.concurrency):
        await queue.put(None)


async def consumer(queue: asyncio.Queue, con, storage_client: storage.Client, args: CliArgs, *, bucket_name: str) -> None:
    """Worker loop: process items with bounded retries and polite pacing."""
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=CHROMIUM_LAUNCH_ARGS)
        try:
            while True:
                item = await queue.get()
                if item is None:
                    queue.task_done()
                    break
                ad_id, ad_url, adv = item
                attempt = 0
                while True:
                    try:
                        status = await process_ad(
                            con,
                            storage_client,
                            bucket_name,
                            args.user_agent,
                            ad_id,
                            ad_url,
                            adv,
                            args.dry_run,
                            args.trace,
                            args=args,
                            browser=browser,
                        )
                    except BrowserRestartRequired as exc:
                        try:
                            await browser.close()
                        except Exception:
                            pass
                        browser = await pw.chromium.launch(headless=True, args=CHROMIUM_LAUNCH_ARGS)
                        try:
                            jlog(
                                "warning",
                                event="browser_restarted",
                                ad_id=ad_id,
                                advertiser_id=adv,
                                reason=str(exc),
                            )
                        except Exception:
                            pass
                        continue  # retry without consuming an attempt
                    if status in ("done", "terminal") or attempt >= args.max_retries:
                        break
                    delay = (args.retry_base_ms / 1000.0) * (2**attempt)
                    jlog(
                        "info",
                        event="retry_backoff",
                        ad_id=ad_id,
                        attempt=attempt + 1,
                        delay_s=round(delay, 3),
                    )
                    await asyncio.sleep(delay)
                    attempt += 1
                queue.task_done()
        finally:
            try:
                await browser.close()
            except Exception:
                pass


# ============================
# Entrypoint
# ============================


async def run(
    args: CliArgs,
    *,
    bq_client: bigquery.Client | None = None,
    storage_client: storage.Client | None = None,
) -> None:
    """Execute the IMAGE pipeline for the supplied CLI arguments."""

    project_id = args.project_id or DEFAULT_PROJECT_ID
    bq_client = bq_client or bigquery.Client(project=project_id)
    storage_client = storage_client or storage.Client(project=project_id)

    bucket_name = args.gcs_bucket or DEFAULT_GCS_BUCKET

    jlog(
        "info",
        event="gcp_context",
        project_id=project_id,
        gcs_bucket=bucket_name,
        bq_location=args.bq_location,
    )

    con = sql_connect(args.sql_conn, args.db_host, args.db_port)
    con.autocommit = True

    mon = asyncio.create_task(monitor_summary(con, interval=60))
    queue: asyncio.Queue = asyncio.Queue()
    prod = asyncio.create_task(producer(queue, con, bq_client, args))
    workers = [asyncio.create_task(consumer(queue, con, storage_client, args, bucket_name=bucket_name)) for _ in range(args.concurrency)]

    await prod
    await queue.join()
    for w in workers:
        w.cancel()
    mon.cancel()
    con.close()


class BrowserRestartRequired(RuntimeError):
    """Signal that the shared browser died and the worker should relaunch it."""
