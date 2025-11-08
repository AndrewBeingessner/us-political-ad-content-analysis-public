from gatc_scraper.urls import normalize_click_url, parse_ids_from_url, select_primary_click_url


def test_select_primary_click_url_prefers_first_non_empty():
    assert select_primary_click_url(["", "https://example.com"]) == "https://example.com"
    assert select_primary_click_url(["https://first", "https://second"]) == "https://first"
    assert select_primary_click_url([]) is None
    assert select_primary_click_url(None) is None


def test_parse_ids_from_url_handles_valid_creatives():
    advertiser, creative = parse_ids_from_url("https://adstransparency.google.com/advertiser/AR123/creative/CR999?region=US")
    assert advertiser == "AR123"
    assert creative == "CR999"


def test_parse_ids_from_url_returns_none_for_unmatched():
    advertiser, creative = parse_ids_from_url("https://example.com/nope")
    assert advertiser is None
    assert creative is None


def test_normalize_click_url_filters_trackers_and_redirects():
    nested = "https://www.googleadservices.com/pagead/aclk?adurl=https://example.com/path?utm_campaign=test&gclid=abc"
    assert normalize_click_url(nested) == "https://example.com/path"


def test_normalize_click_url_handles_invalid_inputs():
    assert normalize_click_url("") is None
    assert normalize_click_url("mailto:test@example.com") is None
