"""OCR helpers backed by Google Cloud Vision."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

from google.auth.exceptions import DefaultCredentialsError
from google.cloud import vision

from .logging import jlog

MAX_OCR_TEXT_CHARS = 20000


@dataclass(frozen=True, slots=True)
class OCRResult:
    text: Optional[str]
    language: Optional[str]
    confidence: Optional[float]


_DISABLE_VALUES = {"1", "true", "yes", "on"}
_disabled_notice_emitted = False
_credentials_failed = False


@lru_cache(maxsize=1)
def _vision_client() -> vision.ImageAnnotatorClient:
    """Return a memoized Vision API client."""

    return vision.ImageAnnotatorClient()


def extract_text_from_image(image_bytes: bytes) -> OCRResult:
    """
    Run OCR for the provided PNG bytes and return the detected text payload.

    Raises RuntimeError if the Vision API reports an error payload.
    """

    global _disabled_notice_emitted, _credentials_failed

    if os.getenv("GATC_DISABLE_OCR", "1").lower() in _DISABLE_VALUES:
        if not _disabled_notice_emitted:
            jlog("info", event="ocr_disabled")
            _disabled_notice_emitted = True
        return OCRResult(text=None, language=None, confidence=None)

    if _credentials_failed:
        return OCRResult(text=None, language=None, confidence=None)

    if not image_bytes:
        return OCRResult(text=None, language=None, confidence=None)

    client = _vision_client()
    image = vision.Image(content=image_bytes)
    try:
        response = client.document_text_detection(image=image)
    except DefaultCredentialsError as exc:  # pragma: no cover - network credential path
        _credentials_failed = True
        jlog("error", event="ocr_credentials_error", error=str(exc))
        return OCRResult(text=None, language=None, confidence=None)

    if response.error.message:
        raise RuntimeError(f"vision_error: {response.error.message}")

    text = (response.full_text_annotation.text or "").strip()
    language: Optional[str] = None
    confidence: Optional[float] = None
    annotation = getattr(response, "full_text_annotation", None)
    pages = getattr(annotation, "pages", None)
    if pages:
        first_page = pages[0]
        languages = getattr(getattr(first_page, "property", None), "detected_languages", None)
        if languages:
            primary_lang = languages[0]
            language = getattr(primary_lang, "language_code", None) or None
        confidence = getattr(first_page, "confidence", None)

    if not text:
        text = None

    if text:
        jlog(
            "info",
            event="ocr_detected",
            chars=len(text),
            language=language,
            confidence=confidence,
        )
    else:
        jlog("warning", event="ocr_empty_result")

    return OCRResult(text=text, language=language, confidence=confidence)


def sanitize_ocr_text(text: Optional[str], *, max_chars: int = MAX_OCR_TEXT_CHARS) -> Optional[str]:
    """Normalize whitespace and clamp extremely large OCR payloads."""

    if not text:
        return None

    cleaned = text.strip()
    if not cleaned:
        return None

    if len(cleaned) > max_chars:
        jlog(
            "warning",
            event="ocr_truncated",
            original_length=len(cleaned),
            max_length=max_chars,
        )
        cleaned = cleaned[:max_chars]

    return cleaned


__all__ = ["MAX_OCR_TEXT_CHARS", "OCRResult", "extract_text_from_image", "sanitize_ocr_text"]
