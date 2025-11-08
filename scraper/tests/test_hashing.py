from io import BytesIO

from gatc_scraper.hashing import normalize_and_hash, stable_int_hash
from PIL import Image


def _png_bytes(width: int = 10, height: int = 10, border: int = 1) -> bytes:
    img = Image.new("RGBA", (width, height), (255, 255, 255, 0))
    for x in range(border, width - border):
        for y in range(border, height - border):
            img.putpixel((x, y), (255, 0, 0, 255))
    buf = BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def test_normalize_and_hash_trims_border_by_default():
    original = _png_bytes(border=2)
    normalized, sha, phash, width, height = normalize_and_hash(original)
    assert len(normalized) > 0
    assert len(sha) == 64
    assert len(phash) == 16
    assert width == 6  # 10px image with 2px border removed on each side
    assert height == 6


def test_normalize_and_hash_respects_trim_false():
    original = _png_bytes(border=2)
    normalized, _, _, width, height = normalize_and_hash(original, trim=False)
    assert width == 10
    assert height == 10


def test_stable_int_hash_is_deterministic():
    value = stable_int_hash("hello")
    assert value == stable_int_hash("hello")
    assert value != stable_int_hash("world")
