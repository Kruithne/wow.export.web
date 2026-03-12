CREATE TABLE IF NOT EXISTS db2_table_hashes (
	table_hash INT UNSIGNED NOT NULL PRIMARY KEY,
	table_name VARCHAR(128) NOT NULL,
	UNIQUE KEY (table_name)
);

CREATE TABLE IF NOT EXISTS hotfix_entries (
	table_hash INT UNSIGNED NOT NULL,
	record_id INT UNSIGNED NOT NULL,
	push_id INT UNSIGNED NOT NULL,
	unique_id INT UNSIGNED NOT NULL DEFAULT 0,
	region_id INT UNSIGNED NOT NULL DEFAULT 0,
	status TINYINT UNSIGNED NOT NULL,
	game_build INT UNSIGNED NOT NULL,
	data_blob BLOB,
	first_seen DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (table_hash, record_id, push_id),
	INDEX idx_game_build (game_build)
);
