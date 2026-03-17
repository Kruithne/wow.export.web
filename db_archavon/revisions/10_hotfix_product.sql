ALTER TABLE hotfix_entries ADD COLUMN product VARCHAR(32) NOT NULL DEFAULT 'wow' AFTER data_blob;

ALTER TABLE hotfix_entries DROP PRIMARY KEY, ADD PRIMARY KEY (table_hash, record_id, push_id, product);

ALTER TABLE hotfix_entries ADD INDEX idx_hotfix_product (product);
