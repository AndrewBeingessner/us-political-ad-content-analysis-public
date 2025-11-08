-- Metrics and helper views for TEXT creative scraping

-- 1) Status counts
CREATE OR REPLACE VIEW v_ads_status_counts AS
SELECT status, COUNT(*) AS count
FROM ads
GROUP BY status;

-- 2) Renderer distribution among completed ads
CREATE OR REPLACE VIEW v_ads_renderer_counts AS
SELECT COALESCE(render_method, '(null)') AS renderer, COUNT(*) AS count
FROM ads
WHERE status = 'done'
GROUP BY COALESCE(render_method, '(null)');

-- 3) Daily throughput (number of 'done' rows per scraped_at date)
CREATE OR REPLACE VIEW v_ads_daily_throughput AS
SELECT DATE(scraped_at) AS day, COUNT(*) AS done
FROM ads
WHERE status = 'done' AND scraped_at IS NOT NULL
GROUP BY DATE(scraped_at)
ORDER BY day DESC;

-- 4) Top error classes
CREATE OR REPLACE VIEW v_ads_error_counts AS
SELECT last_error, COUNT(*) AS count
FROM ads
WHERE status = 'error'
GROUP BY last_error
ORDER BY count DESC;

-- 5) Renderer by year (by scraped_at)
CREATE OR REPLACE VIEW v_renderer_by_year AS
SELECT EXTRACT(YEAR FROM scraped_at)::INT AS year,
       COALESCE(render_method, '(null)') AS renderer,
       COUNT(*) AS count
FROM ads
WHERE status = 'done' AND scraped_at IS NOT NULL
GROUP BY EXTRACT(YEAR FROM scraped_at), COALESCE(render_method, '(null)')
ORDER BY year DESC, count DESC;

-- 6) Variation-unavailable tally
CREATE OR REPLACE VIEW v_variation_unavailable AS
SELECT COUNT(*) AS variation_unavailable
FROM ads
WHERE status = 'error' AND last_error = 'variation_unavailable';
