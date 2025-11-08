"""Playwright helpers shared by the scrapers."""

from __future__ import annotations

import os

from playwright.async_api import ElementHandle, Page, TimeoutError

from .debug import ensure_debug_dir


async def element_is_visibly_displayed(handle: ElementHandle | None) -> bool:
    if not handle:
        return False
    try:
        return await handle.evaluate(
            """
            (el) => {
                if (!el) return false;
                const rect = el.getBoundingClientRect();
                if (rect.width <= 1 || rect.height <= 1) return false;
                const style = window.getComputedStyle(el);
                if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') {
                    return false;
                }
                let node = el;
                while (node) {
                    if (node instanceof HTMLElement) {
                        if (node.hidden || node.getAttribute('aria-hidden') === 'true') {
                            return false;
                        }
                        const ns = window.getComputedStyle(node);
                        if (ns.display === 'none' || ns.visibility === 'hidden' || ns.opacity === '0') {
                            return false;
                        }
                    }
                    node = node.parentElement;
                }
                return true;
            }
            """
        )
    except Exception:
        return False


async def wait_policy_or_errors(page: Page) -> str | None:
    """Return a terminal status if the page exposes a known error banner."""

    try:
        banner = await page.wait_for_selector("div.policy-violation-banner", timeout=5000, state="attached")
    except TimeoutError:
        banner = None
    if await element_is_visibly_displayed(banner):
        return "removed_for_policy_violation"
    try:
        await page.wait_for_selector("div.render-failed, div.render-failed-container", timeout=2000)
        return "variation_unavailable"
    except TimeoutError:
        pass
    if "Error 429" in (await page.title()):
        return "rate_limited_429"
    if await page.query_selector("#af-error-container"):
        return "rate_limited_429"
    try:
        empty = await page.query_selector("div.empty-results")
    except Exception:
        empty = None
    if empty:
        has_creative = await page.query_selector("div.creative-container, creative-details")
        if not has_creative:
            return "not_found"
    return None


async def wait_assets_ready(page: Page) -> None:
    """Wait for fonts and images to settle before taking screenshots."""

    try:
        await page.evaluate(
            """
            () => Promise.all([
                (document.fonts && document.fonts.ready) ? document.fonts.ready : Promise.resolve(),
                Promise.all(
                    Array.from(document.images || []).map(img => {
                        if (img.complete) return Promise.resolve();
                        return new Promise(res => {
                            img.addEventListener('load', () => res(), { once: true });
                            img.addEventListener('error', () => res(), { once: true });
                        });
                    })
                )
            ])
            """
        )
    except Exception:
        pass


async def cleanup_playwright(context, browser, trace: bool, ad_id: str) -> None:
    """Stop tracing (if enabled) and close the browser resources."""

    try:
        if trace and context:
            ensure_debug_dir()
            await context.tracing.stop(path=os.path.join("media/debug", f"trace_{ad_id}.zip"))
    except Exception:
        pass
    try:
        if context:
            await context.close()
    except Exception:
        pass
    try:
        if browser:
            await browser.close()
    except Exception:
        pass


CHROMIUM_LAUNCH_ARGS = [
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-setuid-sandbox",
    "--disable-gpu",
    "--single-process",
    "--no-zygote",
]


__all__ = [
    "CHROMIUM_LAUNCH_ARGS",
    "cleanup_playwright",
    "element_is_visibly_displayed",
    "wait_assets_ready",
    "wait_policy_or_errors",
]

# Backwards-compatible aliases for legacy imports
_wait_assets_ready = wait_assets_ready
_cleanup_playwright = cleanup_playwright
