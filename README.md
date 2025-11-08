# U.S. Political Ad Content Toolkit (Public Snapshot)

This repository is a sanitized snapshot of a project I'm using to study digital political advertising in the U.S. It currently includes the **scraper** component, while leaving the proprietary LLM analysis workflows private for now as I continue to modify them.

## What’s included
- **Scraper (`scraper/`)** – a Playwright-based ingestion pipeline that captures IMAGE and TEXT creatives, stores deterministic assets in Cloud Storage, and persists metadata in a relational database. The package is installable as `gatc_scraper` and ships with CLI entry points for batch and single-ad captures.
- **Operational docs** – runbooks, infrastructure templates, and SQL helpers that describe how we operate the scraper in production.
- **Project scaffolding** – empty or placeholder directories (for example `analysis/` and `docs/`) that show how the broader project is organized even though their private contents are not part of this snapshot.

## What’s intentionally out of scope
- **Analysis code** – downstream LLM prompts, notebooks, and pipelines remain private. Public consumers can add their own workflows under `analysis/`.
- **Production identifiers** – all project IDs, bucket names, service accounts, and connection strings were replaced with obvious placeholders. Update them to match your environment before deploying.
- **Credential material** – you must supply your own Google Cloud service-account keys and database secrets. Never commit secrets to the repository.

## Repository layout
```text
├── scraper/
│   ├── scripts/                 # CLI entry points (image + text pipelines)
│   ├── src/gatc_scraper/        # Shared library code
│   ├── sql/                     # Database schema migrations / helpers
│   ├── infra/                   # Cloud Run / deployment snippets
│   ├── tests/                   # Unit tests (offline)
│   └── RUNBOOK.md               # Operational notes for scraping
├── analysis/                    # Placeholder for downstream research workflows
├── docs/                        # Public documentation scaffold
├── media/                       # Local debug artifacts (gitignored content)
└── README.md                    # You are here
```

## Getting started
1. **Clone and create a virtual environment** using Python 3.10–3.12.
2. **Install dependencies** and the scraper package:
   ```bash
   make bootstrap
   source venv/bin/activate
   ```
3. **Provision infrastructure** in your Google Cloud project:
   - BigQuery dataset (public GATC tables) or equivalent data source
   - Cloud SQL (or local Postgres) database for state (`adsdb` by default)
   - Cloud Storage bucket for canonical assets (`your-scraper-bucket`)
   - Optional: Cloud Run / Pub/Sub / scheduler jobs if automating ingestion
4. **Set required environment variables** such as `DB_PASSWORD`, `GOOGLE_APPLICATION_CREDENTIALS`, and any overrides you need from the CLI help output.

## Database migration
Apply the OCR-ready schema before running the scrapers:
```bash
psql "host=127.0.0.1 port=5432 dbname=adsdb user=postgres" \
  -f scraper/sql/2025-10-26-add-ocr-columns.sql
```
Adjust the connection parameters to match your environment. The migration is idempotent and safe to re-run.

## Running the scrapers
The CLI supports both targeted captures and streaming ingestion. Replace the placeholder values below with your project IDs and resources.

### Capture a single IMAGE ad by ID
```bash
python scraper/scripts/scrape_image_ads.py \
  --project-id your-gcp-project \
  --gcs-bucket your-scraper-bucket \
  --db-host 127.0.0.1 \
  --ad-id CR01234567890123456789 \
  --concurrency 1
```

### Capture a single TEXT ad by ID
```bash
python scraper/scripts/scrape_text_ads.py \
  --project-id your-gcp-project \
  --gcs-bucket your-scraper-bucket \
  --db-host 127.0.0.1 \
  --ad-id CR01234567890123456789 \
  --concurrency 1
```

### Stream recent creatives (IMAGE example)
```bash
python scraper/scripts/scrape_image_ads.py \
  --project-id your-gcp-project \
  --gcs-bucket your-scraper-bucket \
  --db-host 127.0.0.1 \
  --since-days 30 --order-by date_desc --sql-limit 200 --concurrency 2
```
Use `scrape_text_ads.py` for TEXT creatives; the flag set is the same.

## Output contract
- Canonical PNG assets are uploaded to `gs://<bucket>/assets/<type>/<prefix>/<sha256>.png`.
- The `ads` table records status transitions, hashes, capture metadata, and OCR output. The `assets` table is keyed by the SHA-256 of each creative.
- Structured JSON logs (`jlog` / `adlog`) are emitted for ingestion pipelines and can be forwarded to Cloud Logging.
- Set `GATC_DISABLE_OCR=1` to skip Vision API calls if you cannot or do not wish to send creatives to Google Cloud Vision.

## Operational reminders
- Respect GATC and BigQuery rate limits; tune `--concurrency`, `--max-ads`, and `--sql-limit` accordingly.
- Ensure required secrets (database password, service-account key) are injected via environment variables or your runtime’s secret manager.
- Debug artifacts (HTML snapshots, Playwright traces) are written under `media/debug/` when trace/debug flags are enabled.

## Contributing and next steps
Contributions are welcome, but be mindful that this is a public mirror of a larger private project. Feel free to open issues or PRs that improve the scraper, documentation, or placeholder scaffolding. The analysis stack will be published separately once it has been vetted for release.

If you fork this snapshot, review your commit history to ensure you do not reintroduce production identifiers before making it public.
