# Logging schema (JSON lines)

Common fields in most records:
- `ts` (ISO 8601, UTC)
- `event` (string)
- `ad_type` ("IMAGE" | "TEXT", when applicable)
- `ad_id`, `advertiser_id`
- `variant_id` ("", "v1", "v2", ... when applicable)

## Core context
- `gcp_context` : project_id, project_number, gcs_bucket, bq_location
- `status_summary` : rows = [[status, count], ...]
- `renderer_summary` : rows = [[render_method, count], ...]

## Per-ad lifecycle
- `ad_start` : ad_id, advertiser_id, url, ad_type
- `variant_attempt` : ad_id, advertiser_id, render_method, variant_id, url
- `ad_done` : ad_id, advertiser_id, variant_id, ad_type, render_method, width, height, bytes, asset_id, gcs_path, capture_method, capture_target
- `ad_end` : ad_id, advertiser_id, render_method
- `ad_terminal` : ad_id, advertiser_id, variant_id, status
- `ad_error` : ad_id, advertiser_id, variant_id, error

## Image-only
- `click_url` : ad_id, advertiser_id, url, variant_id
- `click_url_saved` : ad_id, advertiser_id, click_url, variant_id

## Diagnostics
- `bq_query` : sql
- `retry_backoff` : ad_id, attempt, delay_s
- `frame_inventory` : ad_id, frames (array)
- `iframe_parent_clip_error` / `iframe_inner_screenshot_error` : ad_id, error
- `debug_save_html_error` : ad_id, error
- `dry_run_*` : mirror events for dry-run mode (upload, upsert, link, status, click_url)
