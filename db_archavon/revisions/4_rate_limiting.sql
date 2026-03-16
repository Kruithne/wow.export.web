ALTER TABLE cache_submissions ADD COLUMN client_ip VARCHAR(45) NULL AFTER binary_hash;
CREATE INDEX idx_machine_submitted ON cache_submissions (machine_id, submitted_at);
