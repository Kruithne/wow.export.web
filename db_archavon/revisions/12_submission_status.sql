ALTER TABLE cache_submissions
  ADD COLUMN status ENUM('pending','finalized','processing','completed','partial','failed') NOT NULL DEFAULT 'pending' AFTER processed_at,
  ADD COLUMN status_reason VARCHAR(255) NULL AFTER status;

CREATE INDEX idx_status ON cache_submissions (status);

ALTER TABLE cache_submission_files
  ADD COLUMN status ENUM('pending','completed','rejected') NOT NULL DEFAULT 'pending',
  ADD COLUMN failure_reason ENUM(
    'download_failed',
    'checksum_mismatch',
    'invalid_magic',
    'parse_error',
    'no_records',
    'unknown_signature'
  ) NULL,
  ADD COLUMN records_added INT UNSIGNED NOT NULL DEFAULT 0;
