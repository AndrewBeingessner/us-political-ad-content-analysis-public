"""Postgres persistence helpers used by the scrapers."""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Optional

import psycopg2

from ..logging import jlog

UTC = getattr(datetime, "UTC", timezone.utc)


def sql_connect(sql_conn: str, db_host: str | None = None, db_port: int | None = None):
    """Return a psycopg2 connection using either TCP or a Cloud SQL socket."""

    dbname = os.getenv("DB_NAME", "adsdb")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD")
    if not password:
        raise RuntimeError("DB_PASSWORD environment variable is required for database connections")

    if db_host:
        return psycopg2.connect(
            host=db_host,
            port=db_port or 5432,
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=10,
            sslmode=os.getenv("DB_SSLMODE", "prefer"),
        )

    if not sql_conn:
        raise RuntimeError("sql_conn must be provided when db_host is not set")
    socket_dir = "/cloudsql"
    host = f"{socket_dir}/{sql_conn}"
    return psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        connect_timeout=10,
    )


def upsert_pending(
    con,
    *,
    ad_type: str,
    ad_id: str,
    advertiser_id: str,
    variant_id: str,
    source_url: Optional[str] = None,
    scraper_version: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    """Insert or refresh a pending row in ``ads``."""

    if dry_run:
        jlog(
            "info",
            event="dry_run_upsert_pending",
            ad_type=ad_type,
            ad_id=ad_id,
            advertiser_id=advertiser_id,
            variant_id=variant_id,
        )
        return
    with con.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ads(ad_id, variant_id, ad_type, advertiser_id, status, scraper_version, source_url)
            VALUES (%s, %s, %s, %s, 'pending', %s, %s)
            ON CONFLICT (ad_id, variant_id) DO UPDATE
               SET advertiser_id   = EXCLUDED.advertiser_id,
                   scraper_version = EXCLUDED.scraper_version,
                   source_url      = COALESCE(EXCLUDED.source_url, ads.source_url),
                   updated_at      = NOW()
            """,
            (ad_id, variant_id, ad_type, advertiser_id, scraper_version, source_url),
        )
    con.commit()


def link_asset_success(
    con,
    *,
    ad_type: str,
    advertiser_id: str,
    ad_id: str,
    variant_id: str,
    asset_id: str,
    render_method: str,
    width_px: int,
    height_px: int,
    file_bytes: int,
    phash: str,
    gcs_path: str,
    capture_method: Optional[str] = None,
    capture_target: Optional[str] = None,
    scraper_version: Optional[str] = None,
    scraped_at: Optional[datetime] = None,
    click_url: Optional[str] = None,
    ocr_text: Optional[str] = None,
    ocr_language: Optional[str] = None,
    ocr_confidence: Optional[float] = None,
    dry_run: bool = False,
) -> None:
    """Mark a creative as done and attach the canonical asset and metrics."""

    if dry_run:
        jlog(
            "info",
            event="dry_run_link",
            ad_type=ad_type,
            advertiser_id=advertiser_id,
            ad_id=ad_id,
            variant_id=variant_id,
            asset_id=asset_id,
            render_method=render_method,
        )
        return
    assert asset_id and isinstance(asset_id, str), "asset_id required"
    assert re.fullmatch(r"[0-9a-f]{64}", asset_id), "asset_id must be 64-char lowercase hex (sha256)"
    assert isinstance(width_px, int) and width_px > 0, "width_px must be > 0"
    assert isinstance(height_px, int) and height_px > 0, "height_px must be > 0"
    assert isinstance(file_bytes, int) and file_bytes > 0, "file_bytes must be > 0"
    assert gcs_path and gcs_path.startswith("gs://"), "gcs_path must be a gs:// path"
    assert render_method, "render_method required"
    assert capture_method, "capture_method required"
    with con.cursor() as cur:
        cur.execute(
            """
            UPDATE ads
                SET render_method   = %s,
                    width_px        = %s,
                    height_px       = %s,
                    file_bytes      = %s,
                    gcs_path        = %s,
                    asset_id        = %s,
                    capture_method  = %s,
                    capture_target  = %s,
                    ocr_text        = COALESCE(%s, ocr_text),
                    ocr_language    = COALESCE(%s, ocr_language),
                    ocr_confidence  = COALESCE(%s, ocr_confidence),
                    click_url       = COALESCE(%s, click_url),
                    status          = 'done',
                    scraped_at      = %s,
                    scraper_version = %s,
                    updated_at      = NOW()
            WHERE ad_id = %s AND variant_id = %s
            """,
            (
                render_method,
                width_px,
                height_px,
                file_bytes,
                gcs_path,
                asset_id,
                capture_method,
                capture_target,
                ocr_text,
                ocr_language,
                ocr_confidence,
                click_url,
                scraped_at or datetime.now(UTC),
                scraper_version,
                ad_id,
                variant_id,
            ),
        )
    con.commit()
    jlog(
        "info",
        event="ad_done",
        ad_type=ad_type,
        advertiser_id=advertiser_id,
        ad_id=ad_id,
        variant_id=variant_id,
        width=width_px,
        height=height_px,
        bytes=file_bytes,
        asset_id=asset_id,
        gcs_path=gcs_path,
        render_method=render_method,
        capture_method=capture_method,
        capture_target=capture_target,
        ocr_chars=len(ocr_text) if ocr_text else 0,
        ocr_language=ocr_language,
        ocr_confidence=ocr_confidence,
    )


def record_status(
    con,
    *,
    advertiser_id: str,
    ad_id: str,
    variant_id: str,
    status: str,
    last_error: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    """Update status and optional ``last_error`` for a creative."""

    if dry_run:
        jlog(
            "info",
            event="dry_run_status",
            advertiser_id=advertiser_id,
            ad_id=ad_id,
            variant_id=variant_id,
            status=status,
            last_error=last_error,
        )
        return
    with con.cursor() as cur:
        cur.execute(
            """
            UPDATE ads
               SET status=%s,
                   last_error=%s,
                   updated_at=NOW()
             WHERE ad_id=%s AND variant_id=%s
            """,
            (status, last_error, ad_id, variant_id),
        )
    con.commit()
    if status in ("removed_for_policy_violation", "rate_limited_429", "not_found", "variation_unavailable"):
        jlog("info", event="ad_terminal", advertiser_id=advertiser_id, ad_id=ad_id, variant_id=variant_id, status=status)
    elif status == "error":
        jlog("error", event="ad_error", advertiser_id=advertiser_id, ad_id=ad_id, variant_id=variant_id, error=last_error or "")


def record_error(
    con,
    *,
    advertiser_id: str,
    ad_id: str,
    variant_id: str,
    msg: str,
    dry_run: bool = False,
) -> None:
    """Shortcut for ``record_status`` with ``status='error'``."""

    record_status(
        con,
        advertiser_id=advertiser_id,
        ad_id=ad_id,
        variant_id=variant_id,
        status="error",
        last_error=msg,
        dry_run=dry_run,
    )


def persist_click_url(
    con,
    *,
    advertiser_id: str,
    ad_id: str,
    variant_id: str,
    click_url: str,
    dry_run: bool = False,
) -> None:
    """Persist a click URL in an idempotent manner."""

    if not click_url:
        return
    if dry_run:
        jlog(
            "info",
            event="dry_run_click_url",
            advertiser_id=advertiser_id,
            ad_id=ad_id,
            variant_id=variant_id,
            click_url=click_url,
        )
        return
    with con.cursor() as cur:
        cur.execute(
            "UPDATE ads SET click_url=%s, updated_at=NOW() WHERE ad_id=%s AND variant_id=%s",
            (click_url, ad_id, variant_id),
        )
    con.commit()
    jlog("info", event="click_url_saved", advertiser_id=advertiser_id, ad_id=ad_id, variant_id=variant_id, click_url=click_url)


def ensure_asset_row(
    con,
    *,
    asset_id: str,
    phash: str,
    width_px: int,
    height_px: int,
    file_bytes: int,
    gcs_path: str,
    dry_run: bool = False,
) -> None:
    """Insert an asset row if it does not already exist."""

    if dry_run:
        return
    with con.cursor() as cur:
        cur.execute(
            """
            INSERT INTO assets(asset_id, sha256, phash, width_px, height_px, file_bytes, gcs_path)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (asset_id) DO NOTHING
            """,
            (asset_id, asset_id, phash, width_px, height_px, file_bytes, gcs_path),
        )
    con.commit()


__all__ = [
    "ensure_asset_row",
    "link_asset_success",
    "persist_click_url",
    "record_error",
    "record_status",
    "sql_connect",
    "upsert_pending",
]
