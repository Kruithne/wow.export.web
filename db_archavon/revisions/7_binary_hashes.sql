CREATE TABLE IF NOT EXISTS cache_binary_hashes (
	build_key CHAR(32) NOT NULL,
	file_name VARCHAR(64) NOT NULL,
	content_hash CHAR(32) NOT NULL,
	file_size INT UNSIGNED NOT NULL,
	fetched_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (build_key, file_name, content_hash),
	INDEX idx_file_name (file_name),
	INDEX idx_build_key (build_key)
);

ALTER TABLE cache_submissions MODIFY COLUMN binary_hash TEXT NOT NULL;
