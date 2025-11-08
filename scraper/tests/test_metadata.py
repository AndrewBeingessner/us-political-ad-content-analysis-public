from gatc_scraper.metadata import build_gcs_metadata


def test_build_gcs_metadata_includes_optional_fields_when_provided():
    md = build_gcs_metadata(
        ad_type="IMAGE",
        ad_id="CR123",
        advertiser_id="AR456",
        variant_id="",
        render_method="sadbundle",
        capture_method="img",
        capture_target="creative",
        width=100,
        height=200,
        sha256="a" * 64,
        phash="b" * 16,
        scraper_version="image:2024.01",
        source_url="https://ad.example",
        click_url="https://click.example",
    )
    assert md["ad_type"] == "IMAGE"
    assert md["click_url"] == "https://click.example"
    assert md["source_url"] == "https://ad.example"


def test_build_gcs_metadata_omits_optional_fields_when_absent():
    md = build_gcs_metadata(
        ad_type="TEXT",
        ad_id="CR789",
        advertiser_id="AR987",
        variant_id="v1",
        render_method="fletch",
        capture_method="screenshot",
        capture_target="frame",
        width=300,
        height=600,
        sha256="c" * 64,
        phash="d" * 16,
        scraper_version="text:2024.01",
    )
    assert "click_url" not in md
    assert "source_url" not in md
