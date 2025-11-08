"""URL helpers for GATC creatives."""

from __future__ import annotations

import re
import urllib.parse

GATC_URL_RE = re.compile(r"/advertiser/(AR[0-9]+)/creative/(CR[0-9]+)")


def select_primary_click_url(urls: list[str] | None) -> str | None:
    if not urls:
        return None
    for url in urls:
        if url:
            return url
    return None


def normalize_click_url(url: str) -> str | None:
    try:
        if not url:
            return None
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ("http", "https", ""):
            return None
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()

        if host.endswith("support.google.com"):
            return None
        if host.endswith("tpc.googlesyndication.com") and (path.startswith("/simgad") or "discover_ads" in path):
            return None
        if host.endswith("pagead2.googlesyndication.com"):
            return None

        qs = urllib.parse.parse_qs(parsed.query)
        if host.endswith("googleadservices.com") or (host.endswith("googleads.g.doubleclick.net") and "pagead/conversion" in path):
            adurl = qs.get("adurl", [None])[0]
            if not adurl:
                return None
            return normalize_click_url(adurl)

        trackers = {"gclid", "dclid", "gclsrc", "fbclid", "mc_eid", "mc_cid", "_hsenc", "_hsmi"}
        clean_qs = {k: v for k, v in qs.items() if (k not in trackers and not k.startswith("utm_"))}
        clean_query = urllib.parse.urlencode([(k, vv) for k, vs in clean_qs.items() for vv in vs], doseq=True)

        scheme = parsed.scheme or "https"
        return urllib.parse.urlunparse((scheme, parsed.netloc, parsed.path, parsed.params, clean_query, parsed.fragment))
    except Exception:
        return None


def parse_ids_from_url(url: str) -> tuple[str | None, str | None]:
    try:
        match = GATC_URL_RE.search(url)
        if not match:
            return None, None
        return match.group(1), match.group(2)
    except Exception:
        return None, None


__all__ = [
    "GATC_URL_RE",
    "normalize_click_url",
    "parse_ids_from_url",
    "select_primary_click_url",
]
