from gatc_scraper.storage import canonical_asset_path_image, canonical_asset_path_text


def test_canonical_asset_path_image_prefix():
    sha = "a" * 64
    path = canonical_asset_path_image("bucket", sha)
    assert path == "gs://bucket/assets/image/aa/" + sha + ".png"


def test_canonical_asset_path_text_prefix():
    sha = "b" * 64
    path = canonical_asset_path_text("bucket", sha)
    assert path == "gs://bucket/assets/text/bb/" + sha + ".png"
