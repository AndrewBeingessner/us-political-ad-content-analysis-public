-- scripts/schema.sql
CREATE TABLE IF NOT EXISTS ads (
  ad_id            TEXT NOT NULL,
  variant_id       TEXT,
  ad_type          TEXT NOT NULL,
  asset_url        TEXT NOT NULL,
  file_hash        TEXT,
  local_path       TEXT,
  gcs_path         TEXT,
  status           TEXT NOT NULL DEFAULT 'pending',
  last_error       TEXT,
  updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(ad_id, variant_id)
);
CREATE INDEX IF NOT EXISTS idx_ads_status ON ads(status);
