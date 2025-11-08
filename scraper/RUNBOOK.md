# Ad Creative Scraper - Runbook (Gold Standard)

**Goal:** Scrape US political IMAGE & TEXT creatives from GATC.
**Writes:** Assets -> GCS, metadata -> Postgres (`ads`, `assets`).
**Analysis:** (Optional) mirror to BigQuery via views/snapshots.
**Scale:** Publish jobs to Pub/Sub; tune Cloud Run max instances.

## Components
- **Worker (Cloud Run):** `app.py` - `POST /event` -> scrape one ad -> Postgres + GCS.
- **Publisher (CLI):** bring your own orchestration (e.g., Dataflow, Cloud Functions) to enqueue GATC jobs; no publisher script ships in this repository snapshot.
- **DB (SoR):** `ads`, `assets` (+ optional `advertisers`, `events`, `scrape_runs`).

## One-time setup
1. **DB FK (optional but recommended):**
   ```sql
   ALTER TABLE ads
     ADD CONSTRAINT ads_asset_fk
     FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
     ON UPDATE CASCADE
     ON DELETE SET NULL;
   ```
2. **OCR columns:** run `scraper/sql/2025-10-26-add-ocr-columns.sql` against Cloud SQL so `ocr_text`, `ocr_language`, and `ocr_confidence` are available.
3. **Vision API:** enable `vision.googleapis.com` on the project and grant the scraper service account the `roles/vision.user` (or higher) permission. Store the JSON key securely (Secret Manager, Vault, etc.) and set `GOOGLE_APPLICATION_CREDENTIALS` wherever the scraper runs without committing credentials to source control.

## Operational defaults
- Streaming jobs skip ads already marked `done` in Cloud SQL. Pass `--rescrape-done` to override when you need to force a re-capture.
