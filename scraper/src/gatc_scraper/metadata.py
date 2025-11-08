"""Metadata helpers for canonical GCS uploads."""

from __future__ import annotations

from collections import OrderedDict
from typing import OrderedDict as OrderedDictType


def build_gcs_metadata(
    *,
    ad_type: str,
    ad_id: str,
    advertiser_id: str,
    variant_id: str,
    render_method: str,
    capture_method: str,
    capture_target: str,
    width: int,
    height: int,
    sha256: str,
    phash: str,
    scraper_version: str,
    source_url: str | None = None,
    click_url: str | None = None,
) -> OrderedDictType[str, str]:
    """Return metadata with deterministic ordering for auditability."""

    md: OrderedDictType[str, str] = OrderedDict()
    md["ad_type"] = ad_type
    md["ad_id"] = ad_id
    md["advertiser_id"] = advertiser_id
    md["variant_id"] = variant_id
    md["render_method"] = render_method
    md["capture_method"] = capture_method
    md["capture_target"] = capture_target
    md["width"] = str(width)
    md["height"] = str(height)
    md["sha256"] = sha256
    md["phash"] = phash
    md["scraper_version"] = scraper_version
    if click_url:
        md["click_url"] = click_url
    if source_url:
        md["source_url"] = source_url
    return md


__all__ = ["build_gcs_metadata"]
