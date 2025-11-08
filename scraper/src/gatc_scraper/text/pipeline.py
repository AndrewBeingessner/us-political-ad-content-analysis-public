"""scrape_text_ads.py

Gold‑standard TEXT creative scraper for Google Ads Transparency Center (GATC).

This module implements a robust, restartable pipeline that:
- Reads U.S. political TEXT ads from BigQuery (public GATC dataset) or a single URL/Ad_ID.
- Renders creatives with Playwright (sadbundle canvas and fletch/adframe iframes).
- Normalizes images (trim borders) and computes sha256 + perceptual hash (avg‑hash).
- Deduplicates by exact sha256 only; also stores a perceptual hash (avg‑hash) for offline analysis.
- Stores a single canonical asset to GCS and links rows in Cloud SQL (Postgres).
- Emits structured JSON logs suitable for Cloud Logging.

The script is deliberately explicit: functions are short, typed, documented, and grouped
from fundamentals → helpers → capture paths → pipeline → CLI. Use this file as the
reference for subsequent scrapers (e.g., IMAGE, VIDEO) to maintain consistency.

Requirements
------------
- Python 3.12+
- Playwright with Chromium installed (see README / Makefile)
- google‑cloud‑bigquery, google‑cloud‑storage, psycopg2‑binary, Pillow, requests

Environment & Infra
-------------------
- Cloud SQL Postgres DB `adsdb` with tables `ads` and `assets`.
- GCS bucket for canonical assets.

Usage (examples)
----------------
# Single ad by ID (lookup via BigQuery)
python scripts/scrape_text_ads.py \
  --project-id your-gcp-project \
  --gcs-bucket your-scraper-bucket \
  --db-host 127.0.0.1 \
  --ad-id CR01234567890123456789 --concurrency 1

# Single ad by full URL (bypass BigQuery)
python scripts/scrape_text_ads.py \
  --project-id your-gcp-project \
  --gcs-bucket your-scraper-bucket \
  --db-host 127.0.0.1 \
  --ad-url "https://adstransparency.google.com/advertiser/AR.../creative/CR...?region=US&topic=political&format=TEXT" \
  --concurrency 1 --trace

# Stream a recent slice (safe test)
python scripts/scrape_text_ads.py \
  --project-id your-gcp-project \
  --gcs-bucket your-scraper-bucket \
  --db-host 127.0.0.1 \
  --since-days 30 --order-by date_desc --sql-limit 200 --concurrency 2
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timezone

from gatc_scraper import (
    CHROMIUM_LAUNCH_ARGS,
    adlog,
    build_gcs_metadata,
    cleanup_playwright,
    dump_frame_inventory,
    ensure_debug_html,
    jlog,
    normalize_and_hash,
    parse_ids_from_url,
    stable_int_hash,
    wait_policy_or_errors,
)
from gatc_scraper import (
    canonical_asset_path_text as canonical_asset_path,
)
from gatc_scraper import (
    get_scraper_version as resolve_version,
)
from gatc_scraper import (
    upload_png_text as upload_png,
)
from gatc_scraper.db import (
    ensure_asset_row as db_ensure_asset_row,
)
from gatc_scraper.db import (
    link_asset_success as db_link_asset_success,
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
from playwright.async_api import Page, TimeoutError, async_playwright

# Back-compat for Python < 3.11 (no datetime.UTC)
UTC = timezone.utc

# ============================
# Constants & configuration
# ============================
DEFAULT_USER_AGENT = "yale-researcher/1.0"
DEFAULT_GCS_BUCKET = "your-scraper-bucket"
DEFAULT_PROJECT_ID = "your-gcp-project"
DEFAULT_CONCURRENCY = 2
DEFAULT_BATCH_SIZE = 5000
DEFAULT_DEVICE_SCALE_FACTOR = 3
DEFAULT_PAGE_TIMEOUT_MS = 30_000
DEFAULT_IFRAME_TIMEOUT_MS = 15_000
DEFAULT_SQL_CONN = "your-project:your-region:your-instance"
TEXT_VARIANT_ID = "v1"  # Primary variant identifier for TEXT creatives

# Log format: human‑readable time + JSON payload per line
# Module-level logger configured by the CLI shim at runtime.
log = logging.getLogger("scraper")

# Scraper provenance
SCRIPT_NAME = "text"
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
    all_variants: bool
    trace: bool
    since_days: int | None
    limit: int | None
    max_retries: int
    retry_base_ms: int
    dry_run: bool
    shard: int
    shard_count: int
    device_scale_factor: int
    page_timeout_ms: int
    iframe_timeout_ms: int
    debug_frames: bool
    debug_html: bool
    bq_location: str | None
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
    if args.ad_url or args.ad_id:
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
    p = argparse.ArgumentParser(description="Download TEXT ads (US only)")
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
        "--advertiser-id",
        help="Advertiser_ID (e.g., AR123...). Required if --ad-url does not include it.",
    )
    p.add_argument(
        "--all-variants",
        action="store_true",
        default=True,  # <— default to saving all variants
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
    p.add_argument("--device-scale-factor", type=int, default=DEFAULT_DEVICE_SCALE_FACTOR)
    p.add_argument("--page-timeout-ms", type=int, default=DEFAULT_PAGE_TIMEOUT_MS)
    p.add_argument("--iframe-timeout-ms", type=int, default=DEFAULT_IFRAME_TIMEOUT_MS)
    p.add_argument(
        "--rescrape-done",
        action="store_true",
        help="Re-run ads even if the ads table shows status='done' (default skips finished rows)",
    )
    p.add_argument(
        "--debug-frames",
        action="store_true",
        help="Log iframe inventory and related debug info on the creative page.",
    )
    p.add_argument(
        "--debug-html",
        action="store_true",
        help="Dump page HTML to media/debug/page_<ad_id>.html for debugging.",
    )
    p.add_argument(
        "--bq-location",
        help="BigQuery location/region for query jobs (e.g., US, EU)",
    )

    ns = p.parse_args()
    _coerce_dates(ns)
    validate_args(ns)

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


def upsert_pending(con, ad_id: str, variant_id: str, source_url: str, advertiser_id: str, *, dry_run: bool = False) -> None:
    """Record a pending TEXT variant and persist the creative page URL (source_url) for auditing."""
    return db_upsert_pending(
        con,
        ad_type="TEXT",
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
    variant_id: str,
    asset_id: str,
    rmethod: str,
    w: int,
    h: int,
    fbytes: int,
    phash: str,
    canonical_gcs: str,
    *,
    capture_method: str,
    capture_target: str,
    click_url: str | None = None,
    ocr_text: str | None = None,
    ocr_language: str | None = None,
    ocr_confidence: float | None = None,
    dry_run: bool = False,
) -> None:
    return db_link_asset_success(
        con,
        ad_type="TEXT",
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
        click_url=click_url,
        scraper_version=get_scraper_version(),
        ocr_text=ocr_text,
        ocr_language=ocr_language,
        ocr_confidence=ocr_confidence,
        dry_run=dry_run,
    )


def record_status(
    con, adv_id: str, ad_id: str, variant_id: str, status: str, last_error: str | None = None, *, dry_run: bool = False
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


def record_error(con, adv_id: str, ad_id: str, variant_id: str, msg: str, *, dry_run: bool = False) -> None:
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


# ============================
# Playwright capture helpers
# ============================


async def capture_sadbundle_variant(page: Page) -> bytes | None:
    canvas = await page.query_selector("#single-ad-canvas")
    if not canvas:
        return None
    try:
        await page.wait_for_timeout(200)  # allow last-bit layout/scale to settle
    except Exception:
        pass
    return await canvas.screenshot(type="png")


# ============================
# Core ad processing
# ============================


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
    all_variants: bool,
    device_scale_factor: int,
    page_timeout_ms: int,
    iframe_timeout_ms: int,
    *,
    debug_frames: bool = False,
    debug_html: bool = False,
) -> str:
    """Process one ad end‑to‑end. Returns 'done' | 'terminal' | 'error'."""

    def _vid(idx: int) -> str:
        return f"v{idx}"

    adlog("ad_start", ad_id=ad_id, advertiser_id=adv, url=ad_url, ad_type="TEXT")
    upsert_pending(con, ad_id, TEXT_VARIANT_ID, ad_url, adv, dry_run=dry_run)

    try:
        async with async_playwright() as pw:
            browser = None
            context = None
            try:
                # Launch + context
                browser = await pw.chromium.launch(args=CHROMIUM_LAUNCH_ARGS)
                context = await browser.new_context(user_agent=user_agent, device_scale_factor=device_scale_factor)
                context.set_default_timeout(page_timeout_ms)

                # Optional tracing
                if trace:
                    await context.tracing.start(screenshots=True, snapshots=True, sources=True)

                # Navigate main creative page
                page = await context.new_page()
                await page.goto(ad_url, wait_until="domcontentloaded", timeout=page_timeout_ms)

                # Optional: dump HTML for debugging
                if debug_html:
                    await ensure_debug_html(page, ad_id)

                if debug_frames:
                    frames_info = await dump_frame_inventory(page)
                    jlog("info", event="frame_inventory", ad_id=ad_id, frames=frames_info)

                # Terminal states (policy / 429 / not found)
                err = await wait_policy_or_errors(page)
                if err:
                    if err in {
                        "removed_for_policy_violation",
                        "rate_limited_429",
                        "not_found",
                        "variation_unavailable",
                    }:
                        record_status(con, adv, ad_id, TEXT_VARIANT_ID, err, None, dry_run=dry_run)
                        return "terminal"
                    record_status(con, adv, ad_id, TEXT_VARIANT_ID, "error", err, dry_run=dry_run)
                    return "error"

                # --- Capture path 1: rare canvas directly in main page ---
                captured_any = False
                try:
                    png = await capture_sadbundle_variant(page)
                    if png:
                        idx = 1
                        variant_id = _vid(idx)
                        upsert_pending(con, ad_id, variant_id, ad_url, adv, dry_run=dry_run)
                        await _finalize_capture(
                            con,
                            storage_client,
                            bucket_name,
                            adv,
                            ad_id,
                            "sadbundle",
                            png,
                            dry_run,
                            capture_method="canvas",
                            capture_target="#single-ad-canvas",
                            source_url=ad_url,
                            variant_id=f"v{idx}",
                        )
                        captured_any = True
                        if not all_variants:
                            return "done"
                except Exception as e:
                    jlog("error", event="mainframe_canvas_error", ad_id=ad_id, error=str(e))

                # --- Capture path 2: explicit sadbundle iframes ---
                if not captured_any:
                    sad_iframes = await page.query_selector_all("iframe[src*='sadbundle']")
                    for idx, iframe in enumerate(sad_iframes, start=1):
                        adlog(
                            "variant_attempt",
                            ad_id=ad_id,
                            advertiser_id=adv,
                            url=ad_url,
                            render_method="sadbundle",
                            variant_id=_vid(idx),
                        )
                        src = await iframe.get_attribute("src")
                        if not src:
                            continue
                        iframe_page = await context.new_page()
                        await iframe_page.goto(src, wait_until="domcontentloaded", timeout=page_timeout_ms)
                        err = await wait_policy_or_errors(iframe_page)
                        if err:
                            record_status(con, adv, ad_id, variant_id=f"v{idx}", status=err, dry_run=dry_run)
                            await iframe_page.close()
                            continue
                        try:
                            await iframe_page.wait_for_selector("#single-ad-canvas", timeout=iframe_timeout_ms)
                            png = await capture_sadbundle_variant(iframe_page)
                            if png:
                                variant_id = _vid(idx)
                                upsert_pending(con, ad_id, variant_id, ad_url, adv, dry_run=dry_run)
                                await _finalize_capture(
                                    con,
                                    storage_client,
                                    bucket_name,
                                    adv,
                                    ad_id,
                                    "sadbundle",
                                    png,
                                    dry_run,
                                    capture_method="canvas",
                                    capture_target="#single-ad-canvas",
                                    source_url=ad_url,
                                    variant_id=f"v{idx}",
                                )
                                captured_any = True
                                if not all_variants:
                                    break
                        except TimeoutError:
                            pass
                        finally:
                            await iframe_page.close()

                # --- Capture path 3: generic creative iframes (fletch/adframe) ---
                if not captured_any:
                    any_iframes = await page.query_selector_all("iframe")

                    async def _is_creative_iframe(el) -> bool:
                        return await el.evaluate(
                            """
                            (node) => {
                              const src = node.getAttribute('src') || '';
                              const id = node.id || '';
                              const inCreative = !!(node.closest('creative-details') || node.closest('.creative-details-container'));
                              const inTargeting = !!node.closest('targeting-criteria');
                              if (!inCreative || inTargeting) return false;
                              const ariaHidden = node.getAttribute('aria-hidden') === 'true';
                              const cs = getComputedStyle(node);
                              const invisible = ariaHidden
      || cs.opacity === '0' || cs.visibility === 'hidden'
      || cs.display === 'none' || cs.zIndex === '-1';
                              if (invisible) return false;
                              const looksAd = id.startsWith('fletch-render-') || id.includes('_preview_') || src.includes('/adframe');
                              if (!looksAd) return false;
                              const w = Number(node.getAttribute('width')) || node.clientWidth || 0;
                              const h = Number(node.getAttribute('height')) || node.clientHeight || 0;
                              return (w >= 200 && h >= 150);
                            }
                            """
                        )

                    creative_iframes = []
                    for el in any_iframes:
                        try:
                            if await _is_creative_iframe(el):
                                creative_iframes.append(el)
                        except Exception:
                            continue

                    for idx, iframe_elem in enumerate(creative_iframes, start=1):
                        adlog(
                            "variant_attempt",
                            ad_id=ad_id,
                            advertiser_id=adv,
                            url=ad_url,
                            render_method="iframe",
                            variant_id=_vid(idx),
                        )
                        iframe_id = (await iframe_elem.get_attribute("id")) or ""
                        target_label = f"iframe#{iframe_id}" if iframe_id else "iframe"
                        png = None
                        cap_method = None
                        cap_target = None
                        try:
                            frame = await iframe_elem.content_frame()
                            inner_url = ""
                            if frame:
                                try:
                                    inner_url = frame.url or ""
                                except Exception:
                                    inner_url = ""
                            src_attr = (await iframe_elem.get_attribute("src")) or ""
                            url_lower = (src_attr + " " + inner_url).lower()
                            label = (
                                "fletch"
                                if ("fletch" in url_lower or (await iframe_elem.get_attribute("id") or "").startswith("fletch-render-"))
                                else (
                                    "sadbundle"
                                    if "sadbundle" in url_lower
                                    else ("adframe" if "/adframe" in url_lower or src_attr == "/adframe" else "iframe")
                                )
                            )

                            if frame:
                                await frame.wait_for_load_state("domcontentloaded", timeout=page_timeout_ms)
                                for _ in range(5):
                                    if await frame.query_selector("#single-ad-canvas"):
                                        break
                                    await frame.wait_for_timeout(120)
                                preview_canvas = await frame.query_selector("#single-ad-canvas")
                                if preview_canvas:
                                    png = await preview_canvas.screenshot(type="png")
                                    cap_method = "canvas"
                                    cap_target = "#single-ad-canvas"
                                else:
                                    # Structural triad: fletch parent/id + content.js htmlParentId
                                    try:
                                        triad_ok = await iframe_elem.evaluate(
                                            """
                                            (node) => {
                                              const iframe = node;
                                              const parent = iframe.closest('div[id^="fletch-render-"]');
                                              if (!parent) return false;
                                              const fid = parent.id;
                                              const scripts = document.querySelectorAll('script[src*="content.js"][src*="htmlParentId="]');
                                              for (const s of scripts) {
                                                const src = s.getAttribute('src') || '';
                                                const hasParent = (
                                                src.includes('htmlParentId=' + encodeURIComponent(fid)) ||
                                                src.includes('htmlParentId=' + fid)
                                                );
                                                if (hasParent) {
                                                return (iframe.id || '').includes(fid + '_preview_');
                                                }
                                              }
                                              return false;
                                            }
                                            """
                                        )
                                    except Exception:
                                        triad_ok = False
                                    if triad_ok:
                                        bbox = await iframe_elem.bounding_box()
                                        if bbox and bbox.get("width", 0) >= 20 and bbox.get("height", 0) >= 20:
                                            png = await page.screenshot(
                                                type="png",
                                                clip={
                                                    "x": bbox["x"],
                                                    "y": bbox["y"],
                                                    "width": bbox["width"],
                                                    "height": bbox["height"],
                                                },
                                            )
                                            cap_method = "screenshot"
                                            cap_target = target_label
                        except Exception as e:
                            jlog(
                                "error",
                                event="iframe_inner_screenshot_error",
                                ad_id=ad_id,
                                error=str(e),
                            )
                            png = None

                        if not png:
                            try:
                                bbox = await iframe_elem.bounding_box()
                                if bbox and bbox.get("width", 0) >= 20 and bbox.get("height", 0) >= 20:
                                    png = await page.screenshot(
                                        type="png",
                                        clip={
                                            "x": bbox["x"],
                                            "y": bbox["y"],
                                            "width": bbox["width"],
                                            "height": bbox["height"],
                                        },
                                    )
                                    cap_method = "screenshot"
                                    cap_target = target_label
                            except Exception as e:
                                jlog(
                                    "error",
                                    event="iframe_parent_clip_error",
                                    ad_id=ad_id,
                                    error=str(e),
                                )

                        if png:
                            variant_id = _vid(idx)
                            upsert_pending(con, ad_id, variant_id, ad_url, adv, dry_run=dry_run)
                            await _finalize_capture(
                                con,
                                storage_client,
                                bucket_name,
                                adv,
                                ad_id,
                                label,
                                png,
                                dry_run,
                                capture_method=cap_method or "screenshot",
                                capture_target=cap_target or target_label,
                                source_url=ad_url,
                                variant_id=f"v{idx}",
                            )
                            captured_any = True
                            if not all_variants:
                                break

                if not captured_any:
                    record_status(con, adv, ad_id, TEXT_VARIANT_ID, "error", "no_variants", dry_run=dry_run)
                    return "error"

                return "done"

            finally:
                await cleanup_playwright(context, browser, trace, ad_id)

    except Exception as e:  # outermost safety net per ad
        record_error(con, adv, ad_id, TEXT_VARIANT_ID, f"exception: {e}", dry_run=dry_run)
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
                ad_type="TEXT",
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
        ad_type="TEXT",
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
        variant_id=variant_id,
        asset_id=asset_id,
        rmethod=render_method,
        w=width,
        h=height,
        fbytes=fbytes,
        phash=phash,
        canonical_gcs=canonical_gcs,
        capture_method=capture_method,
        capture_target=capture_target,
        dry_run=dry_run,
        ocr_text=ocr_text,
        ocr_language=ocr_language,
        ocr_confidence=ocr_confidence,
    )


# ============================
# BigQuery streaming & workers
# ============================


def fetch_text_ads_stream(
    client: bigquery.Client,
    batch_size: int,
    start_date: str | None,
    end_date: str | None,
    order_by: str,
    sql_limit: int | None,
) -> Iterable[tuple[str, str, str]]:
    """Yield (Ad_ID, Ad_URL, Advertiser_ID) respecting filters and order."""
    filters = ["Ad_Type='TEXT'", r"REGEXP_CONTAINS(Regions, r'\bUS\b')"]
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
    iterator = client.query(sql).result(page_size=page_sz)
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

    # Direct URL: bypass BigQuery
    if args.ad_url:
        adv_from_url, ad_from_url = parse_ids_from_url(args.ad_url)
        ad = args.ad_id or ad_from_url
        adv = args.advertiser_id or adv_from_url
        if not ad or not adv:
            raise RuntimeError(
                "When using --ad-url, either the URL must contain both IDs or you must also provide --ad-id and --advertiser-id."
            )
        if args.shard_count > 1 and (stable_int_hash(ad) % args.shard_count != args.shard):
            for _ in range(args.concurrency):
                await queue.put(None)
            return
        await queue.put((ad, args.ad_url, adv))
        for _ in range(args.concurrency):
            await queue.put(None)
        return

    # One‑ad by BigQuery lookup
    if args.ad_id:
        sql = """
        SELECT Ad_ID, Ad_URL, Advertiser_ID
          FROM `bigquery-public-data.google_political_ads.creative_stats`
         WHERE Ad_ID = @ad_id AND Ad_Type='TEXT' AND REGEXP_CONTAINS(Regions, r'\\bUS\\b')
         LIMIT 1
        """.strip()
        job = bq_client.query(
            sql,
            job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("ad_id", "STRING", args.ad_id)]),
        )
        rows = list(job.result())
        if rows:
            r = rows[0]
            adid, url, adv = str(r.Ad_ID), str(r.Ad_URL), str(r.Advertiser_ID)
            if args.shard_count <= 1 or (stable_int_hash(adid) % args.shard_count == args.shard):
                await queue.put((adid, url, adv))
        for _ in range(args.concurrency):
            await queue.put(None)
        return

    # Streaming mode
    for ad_id, ad_url, adv in fetch_text_ads_stream(
        bq_client,
        args.batch_size,
        args.start_date,
        args.end_date,
        args.order_by,
        args.sql_limit,
    ):
        if adv in args.skip_advertisers:
            continue
        if args.shard_count > 1 and (stable_int_hash(ad_id) % args.shard_count != args.shard):
            continue
        row: tuple[str] | None = None
        with con.cursor() as cur:
            cur.execute(
                "SELECT status FROM ads WHERE ad_id=%s AND variant_id=%s",
                (ad_id, TEXT_VARIANT_ID),
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


async def consumer(queue: asyncio.Queue, con, storage_client: storage.Client, args: CliArgs) -> None:
    """Worker loop: process items with bounded retries and polite pacing."""
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break
        ad_id, ad_url, adv = item
        attempt = 0
        while True:
            status = await process_ad(
                con,
                storage_client,
                args.gcs_bucket,
                args.user_agent,
                ad_id,
                ad_url,
                adv,
                args.dry_run,
                args.trace,
                args.all_variants,
                args.device_scale_factor,
                args.page_timeout_ms,
                args.iframe_timeout_ms,
                debug_frames=args.debug_frames,
                debug_html=args.debug_html,
            )
            if status in ("done", "terminal") or attempt >= args.max_retries:
                break
            delay = (args.retry_base_ms / 1000.0) * (2**attempt) + random.uniform(0, 0.3)
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
        await asyncio.sleep(random.uniform(1, 3))


# ============================
# Entrypoint
# ============================


async def run(
    args: CliArgs,
    *,
    bq_client: bigquery.Client | None = None,
    storage_client: storage.Client | None = None,
) -> None:
    """Execute the TEXT pipeline for the supplied CLI arguments."""

    bq_client = bq_client or bigquery.Client(project=args.project_id)
    storage_client = storage_client or storage.Client(project=args.project_id)
    con = sql_connect(args.sql_conn, args.db_host, args.db_port)
    con.autocommit = True

    mon = asyncio.create_task(monitor_summary(con, interval=60))
    queue: asyncio.Queue = asyncio.Queue()
    prod = asyncio.create_task(producer(queue, con, bq_client, args))
    workers = [asyncio.create_task(consumer(queue, con, storage_client, args)) for _ in range(args.concurrency)]

    await prod
    await queue.join()
    for w in workers:
        w.cancel()
    mon.cancel()
    con.close()
