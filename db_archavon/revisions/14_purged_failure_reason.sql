ALTER TABLE cache_submission_files
  MODIFY COLUMN failure_reason ENUM(
    'download_failed',
    'checksum_mismatch',
    'invalid_magic',
    'parse_error',
    'no_records',
    'unknown_signature',
    'purged'
  ) NULL;
