"""Google Cloud Storage helpers."""

from __future__ import annotations

from google.cloud import storage  # type: ignore[attr-defined]

from .logging import jlog


def canonical_asset_path_image(bucket: str, sha256_hex: str) -> str:
    return f"gs://{bucket}/assets/image/{sha256_hex[:2]}/{sha256_hex}.png"


def canonical_asset_path_text(bucket: str, sha256_hex: str) -> str:
    return f"gs://{bucket}/assets/text/{sha256_hex[:2]}/{sha256_hex}.png"


def upload_png_image(
    storage_client: storage.Client,
    bucket_name: str,
    blob_path: str,
    png_bytes: bytes,
    metadata: dict[str, str],
    *,
    dry_run: bool = False,
) -> None:
    _upload_png(storage_client, bucket_name, blob_path, png_bytes, {**(metadata or {}), "ad_type": "IMAGE"}, dry_run=dry_run)


def upload_png_text(
    storage_client: storage.Client,
    bucket_name: str,
    blob_path: str,
    png_bytes: bytes,
    metadata: dict[str, str],
    *,
    dry_run: bool = False,
) -> None:
    _upload_png(storage_client, bucket_name, blob_path, png_bytes, {**(metadata or {}), "ad_type": "TEXT"}, dry_run=dry_run)


def _upload_png(
    storage_client: storage.Client,
    bucket_name: str,
    blob_path: str,
    png_bytes: bytes,
    metadata: dict[str, str],
    *,
    dry_run: bool = False,
) -> None:
    assert blob_path.startswith(f"gs://{bucket_name}/"), "blob_path must start with gs://<bucket>/"
    if dry_run:
        jlog("info", event="dry_run_upload", path=blob_path)
        return
    bucket = storage_client.bucket(bucket_name)
    name = blob_path.split(f"gs://{bucket_name}/", 1)[1]
    blob = bucket.blob(name)
    blob.cache_control = "public, max-age=31536000, immutable"
    blob.metadata = dict(metadata or {})
    blob.upload_from_string(png_bytes, content_type="image/png")


__all__ = [
    "canonical_asset_path_image",
    "canonical_asset_path_text",
    "upload_png_image",
    "upload_png_text",
]
