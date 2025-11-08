"""Debug artifact helpers shared by the scrapers."""

from __future__ import annotations

import os
from typing import Any

from playwright.async_api import Page

from .logging import jlog

DEBUG_DIR = "media/debug"


def ensure_debug_dir() -> str:
    """Create the debug directory if it does not exist and return the path."""

    try:
        os.makedirs(DEBUG_DIR, exist_ok=True)
    except Exception:
        pass
    return DEBUG_DIR


async def ensure_debug_html(page: Page, ad_id: str) -> None:
    """Persist the current page HTML for later debugging (best effort)."""

    try:
        ensure_debug_dir()
        html = await page.content()
        with open(os.path.join(DEBUG_DIR, f"page_{ad_id}.html"), "w", encoding="utf-8") as f:
            f.write(html)
    except Exception as exc:  # pragma: no cover - logging only
        jlog("error", event="debug_save_html_error", ad_id=ad_id, error=str(exc))


async def dump_frame_html(frame, filename: str) -> None:
    """Persist the outer HTML of a frame into the debug directory."""

    try:
        ensure_debug_dir()
        html = await frame.evaluate("() => document.documentElement.outerHTML")
        with open(os.path.join(DEBUG_DIR, filename), "w", encoding="utf-8") as fh:
            fh.write(html)
    except Exception as exc:  # pragma: no cover - logging only
        jlog("error", event="debug_save_iframe_html_error", filename=filename, error=str(exc))


async def dump_frame_inventory(page: Page) -> list[dict[str, Any]]:
    """Return a structured list describing all iframes on the page."""

    try:
        return await page.evaluate(
            """
            () => {
              const out = [];
              const iframes = Array.from(document.querySelectorAll('iframe'));
              for (const fr of iframes) {
                const id = fr.id || '';
                const src = fr.getAttribute('src') || '';
                const w = Number(fr.getAttribute('width')) || fr.clientWidth || 0;
                const h = Number(fr.getAttribute('height')) || fr.clientHeight || 0;
                const rect = fr.getBoundingClientRect();
                out.push({ id, src, w, h, rect: { x: rect.x, y: rect.y, width: rect.width, height: rect.height } });
              }
              return out;
            }
            """
        )
    except Exception:
        return []


__all__ = [
    "DEBUG_DIR",
    "dump_frame_html",
    "dump_frame_inventory",
    "ensure_debug_dir",
    "ensure_debug_html",
]
