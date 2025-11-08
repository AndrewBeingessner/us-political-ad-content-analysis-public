"""Image hashing and normalization utilities."""

from __future__ import annotations

import hashlib
from io import BytesIO
from typing import TYPE_CHECKING, Any, Literal, Union, cast

from PIL import Image, ImageChops

if TYPE_CHECKING:  # pragma: no cover - typing helper
    from PIL.Image import Resampling
else:  # Pillow < 10 compatibility where Resampling lives on Image
    Resampling = Any


LanczosType = Union["Resampling", Literal[0, 1, 2, 3, 4, 5]]


def _lanczos_filter() -> LanczosType:
    resampling: Any = getattr(Image, "Resampling", None)
    if resampling is not None:
        return cast(LanczosType, getattr(resampling, "LANCZOS"))
    return cast(LanczosType, getattr(Image, "LANCZOS"))


def stable_int_hash(s: str) -> int:
    """Return a small deterministic hash for sharding purposes."""

    return int(hashlib.sha1(s.encode("utf-8")).hexdigest()[:8], 16)


def _trim_border(img: Image.Image) -> Image.Image:
    if img.mode == "RGBA":
        bg = Image.new(img.mode, img.size, (255, 255, 255, 0))
    elif img.mode == "LA":
        bg = Image.new(img.mode, img.size, (255, 0))
    elif img.mode == "L":
        bg = Image.new(img.mode, img.size, 255)
    else:
        bg = Image.new(img.mode, img.size, "white")
    diff = ImageChops.difference(img, bg)
    diff = ImageChops.add(diff, diff, 2.0, -100)
    bbox = diff.getbbox()
    return img.crop(bbox) if bbox else img


def normalize_and_hash(png_bytes: bytes, *, trim: bool = True) -> tuple[bytes, str, str, int, int]:
    """Normalize a PNG payload and compute deterministic hashes."""

    with Image.open(BytesIO(png_bytes)) as im:
        im = im.convert("RGBA")
        if trim:
            im = _trim_border(im)
        width, height = im.size

        out = BytesIO()
        im.save(out, format="PNG", optimize=True)
        norm_png = out.getvalue()

        sha = hashlib.sha256(norm_png).hexdigest()

        ah = im.convert("L").resize((8, 8), resample=_lanczos_filter())
        pixels = list(ah.getdata())
        avg = sum(pixels) / len(pixels)
        bits = "".join("1" if p > avg else "0" for p in pixels)
        phash = f"{int(bits, 2):016x}"

        return norm_png, sha, phash, width, height


__all__ = ["normalize_and_hash", "stable_int_hash"]
