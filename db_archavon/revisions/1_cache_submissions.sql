CREATE TABLE IF NOT EXISTS cache_submissions (
	submission_id CHAR(36) PRIMARY KEY,
	machine_id CHAR(36) NOT NULL,
	product VARCHAR(32) NOT NULL,
	patch VARCHAR(16) NOT NULL,
	build_number INT NOT NULL,
	build_key CHAR(32) NOT NULL,
	cdn_key CHAR(32) NOT NULL,
	binary_hash CHAR(64) NOT NULL,
	submitted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	finalized_at DATETIME NULL,
	INDEX idx_finalized (finalized_at)
);

CREATE TABLE IF NOT EXISTS cache_submission_files (
	submission_id CHAR(36) NOT NULL,
	file_name VARCHAR(64) NOT NULL,
	locale VARCHAR(16) NOT NULL,
	file_size INT UNSIGNED NOT NULL,
	object_id VARCHAR(64) NOT NULL,
	PRIMARY KEY (submission_id, file_name, locale),
	FOREIGN KEY (submission_id) REFERENCES cache_submissions(submission_id) ON DELETE CASCADE
);
