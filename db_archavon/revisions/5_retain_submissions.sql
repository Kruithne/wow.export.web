ALTER TABLE cache_submissions ADD COLUMN processed_at DATETIME NULL AFTER finalized_at;
CREATE INDEX idx_processed ON cache_submissions (processed_at);
