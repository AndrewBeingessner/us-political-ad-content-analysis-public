-- Add OCR persistence columns for creative variants.
ALTER TABLE ads
    ADD COLUMN IF NOT EXISTS ocr_text TEXT,
    ADD COLUMN IF NOT EXISTS ocr_language VARCHAR(32),
    ADD COLUMN IF NOT EXISTS ocr_confidence DOUBLE PRECISION;

COMMENT ON COLUMN ads.ocr_text IS 'Full-text OCR output captured from the canonical creative image.';
COMMENT ON COLUMN ads.ocr_language IS 'BCP-47 language code reported by the OCR provider (if any).';
COMMENT ON COLUMN ads.ocr_confidence IS 'Confidence score (0-1) reported by the OCR provider.';
