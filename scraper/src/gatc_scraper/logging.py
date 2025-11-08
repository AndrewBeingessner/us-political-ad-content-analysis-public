"""Structured logging helpers shared by the scraper entrypoints."""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

UTC = getattr(datetime, "UTC", timezone.utc)
_LOGGER_NAME = "scraper"
_configured = False
_base_context: dict[str, Any] = {}
_context_stack: list[dict[str, Any]] = []


def configure_logging(level: int = logging.INFO) -> None:
    """Configure the global logging formatter once."""

    global _configured
    if _configured:
        return
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")
    _configured = True


def set_global_context(**fields: Any) -> None:
    """Add persistent context fields that appear on every structured log."""

    _base_context.update({k: v for k, v in fields.items() if v is not None})


@contextmanager
def logging_context(**fields: Any) -> Iterator[None]:
    """Push a temporary logging context for the duration of the ``with`` block."""

    ctx = {k: v for k, v in fields.items() if v is not None}
    _context_stack.append(ctx)
    try:
        yield
    finally:
        _context_stack.pop()


def _merged_context() -> dict[str, Any]:
    merged: dict[str, Any] = {}
    merged.update(_base_context)
    for ctx in _context_stack:
        merged.update(ctx)
    return merged


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


def jlog(level: str, /, **fields: Any) -> None:
    """Emit a structured JSON log payload under the ``scraper`` logger."""

    log = logging.getLogger(_LOGGER_NAME)
    record = {"ts": _utcnow_iso(), **_merged_context(), **fields}
    getattr(log, level.lower())(json.dumps(record, ensure_ascii=False, sort_keys=True))


def adlog(event: str, *, ad_id: str, advertiser_id: str, url: str, **kw: Any) -> None:
    """Shortcut for ad-scoped JSON logging records."""

    jlog("info", event=event, ad_id=ad_id, advertiser_id=advertiser_id, url=url, **kw)


__all__ = ["adlog", "configure_logging", "jlog", "logging_context", "set_global_context"]
