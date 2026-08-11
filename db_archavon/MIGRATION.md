# archavon: mysql -> sqlite migration

One-off tooling to build the initial `archavon.sqlite` from the live DreamHost MySQL
database, verify it, and hand it over to the PHP app.

All three scripts must be run from the repository root (`F:\kruithne.net`) so Bun picks up
`.env.local` and `DB_URI_ARCHAVON`.

| script | purpose |
| --- | --- |
| `wow.export/archavon_migrate.ts` | stream MySQL -> SQLite |
| `wow.export/archavon_verify.ts` | compare the result against MySQL |
| `wow.export/archavon_read_check.ts` | serve the result through the real read API and compare again |

`db_archavon/schema.sql` is a copy of `F:\archavon\db\schema.sql`. **Re-copy it before
running the migration** if the archavon schema has changed; the migration asserts that the
MySQL and SQLite column sets agree and aborts on any drift.

## 1. size report

```
bun run wow.export/archavon_migrate.ts --sizes
```

Prints `information_schema` sizes and exits without touching anything.

Measured 2026-08-11 (total 2.49 GB):

| table | rows | data | index | total |
| --- | ---: | ---: | ---: | ---: |
| wdb_attestations | 6,202,290 | 745.0 MB | 0 | 745.0 MB |
| cache_quests | 500,690 | 633.8 MB | 91.8 MB | 725.6 MB |
| cache_creatures | 637,392 | 250.9 MB | 121.7 MB | 372.6 MB |
| hotfix_entries | 1,064,396 | 255.0 MB | 90.3 MB | 345.3 MB |
| cache_gameobjects | 396,815 | 163.8 MB | 76.6 MB | 240.4 MB |
| cache_quest_objectives | 795,189 | 70.6 MB | 0 | 70.6 MB |
| everything else | — | — | — | ~49 MB |

The BLOB column was the suspect for the bulk of the database; it is not. `hotfix_entries`
is 345 MB for 1.06M rows, and the blobs average 111 bytes (max 3,970, 244k rows NULL) —
roughly 90 MB of blob payload in total. The weight is in `wdb_attestations` (6.2M narrow
rows) and `cache_quests` (500k very wide rows, 155 columns).

## 2. export

```
bun run wow.export/archavon_migrate.ts --out ./archavon.sqlite
```

Options: `--resume` (continue an interrupted run), `--tables a,b` (subset; skips index
build and finalisation), `--batch N` (rows per read, default 20000), `--schema <path>`,
`--keep-state`.

Reads are keyset-paginated on each table's primary key — no `OFFSET`, no long-running
transaction, one MySQL connection. Each batch is written inside a single SQLite
transaction together with its resume cursor, so an interrupted run resumes exactly where
it stopped with no duplicates and no re-reading.

**ENUM columns in a primary key cannot be keyset-paginated with a row-value comparison.**
`ORDER BY` sorts an ENUM by its declaration index, but `col > 'literal'` compares
lexically, and the two disagree. `wdb_attestations` declares
`('creature','quest','gameobject','pagetext')`, so a naive scan reached the end of `quest`
and found nothing "greater" — silently dropping every `gameobject` and `pagetext` row,
2.3M of 6.2M. `cache_quest_conditional_texts` lost a handful of rows the same way at batch
boundaries. Enum key columns are therefore pinned by equality and iterated as separate
partitions, with only the non-enum key columns taking part in the range comparison. The
migration also hard-fails if a table copies materially fewer rows than MySQL reported, so
this class of bug cannot pass silently again.

Indexes are built after the bulk load;
finalisation seeds `sqlite_sequence`, runs `integrity_check` / `foreign_key_check`,
`VACUUM`s, `ANALYZE`s, and leaves the file in `journal_mode = DELETE` so it transfers as a
single file. `db.php` flips it to WAL on first open.

The WAL is truncated every 250k rows during the load. Left to grow, it reached gigabytes
and dragged `wdb_attestations` from 24k rows/s down to under 8k; `journal_mode = OFF` would
be faster still but would forfeit the resume guarantee.

### conversions

- **datetimes.** MySQL `DATETIME` values were all written by `CURRENT_TIMESTAMP` / `NOW()`
  or the column default, so they are in the MySQL server's local zone
  (`America/Los_Angeles`). The SQLite schema stores UTC. Every datetime is converted
  server-side with `CONVERT_TZ(col, 'SYSTEM', '+00:00')`, which is DST-aware; the script
  refuses to run if `CONVERT_TZ` returns NULL or is not DST-aware, and it pre-checks each
  column for values that cannot be converted.
- **`cache_submission_files.modified_at` is the exception** — it is a client file mtime
  bound as a JS `Date`, already stored as UTC, and is copied through unconverted.
  Confirmed empirically: `min(submitted_at - modified_at)` is exactly -420 minutes in
  every month of data, i.e. one full Pacific offset, which is only possible if the two
  columns are in different zones.
- **u64.** `cache_quests.time_allowed` and `.race_flags` were `BIGINT UNSIGNED`. SQLite has
  no unsigned integer, so values at or above 2^63 wrap to signed with the bit pattern
  intact (407,419 rows are affected; `race_flags = 0xFFFFFFFFFFFFFFFF` alone accounts for
  334k). This matches `wdb_delta.ts`, which binds the client's u64 the same way, so
  migrated rows and new delta rows agree. `api/helpers.php::sanitize_large_ints` is what
  renders them for consumers.
- `ENUM` -> `TEXT` (the schema's `CHECK` constraints validate every value on insert),
  `BOOLEAN` -> `INTEGER` 0/1, `BLOB` -> `BLOB` byte-for-byte.
- `entry_id` is preserved verbatim, because `wdb_attestations.entry_id` references it with
  no foreign key. `sqlite_sequence` is seeded past the maximum so the next insert cannot
  collide with a migrated row.
- `db_schema` (spooder's MySQL migration ledger) has no SQLite counterpart and is skipped.
  `delta_applications` has no MySQL source and is left empty.

### mysql driver

`mysql2`, not `Bun.SQL`. `Bun.SQL`'s MySQL client fails the handshake against this server
(`FailedToOpenSocket`, despite the TCP connection succeeding and PHP's PDO connecting
fine), and it cannot return `BIGINT UNSIGNED` without precision loss. `mysql2` is a dev
dependency; nothing in the running server uses it.

## 3. verify

```
bun run wow.export/archavon_verify.ts --db ./archavon.sqlite --sample 200 --drift 0.005   # dry run
bun run wow.export/archavon_verify.ts --db ./archavon.sqlite --sample 200                 # cutover
```

Exits non-zero on any failure. Checks per-table row counts, a random sample of entity rows
compared field-by-field with the same conversions applied, consensus counts and
`max(consensus_at)` per entity table, attestation counts per entity type and per sampled
entry, hotfix blobs byte-for-byte plus total blob bytes, converted min/max/null counts for
every datetime column, and SQLite structure (`integrity_check`, `foreign_key_check`,
`sqlite_sequence`, enum domains, journal mode, no leftover state table).

Counts are compared against a *live* database, so a dry run needs `--drift` to separate
concurrency from corruption. With a drift allowance, a check where SQLite is *behind*
MySQL by less than that fraction is downgraded to `WARN`; SQLite being *ahead*, or behind
by more, still fails. Mutable columns (`attestation_count`, `is_consensus`,
`consensus_at`), datetime upper bounds and orphaned junction rows are treated the same way.
Datetime *lower* bounds are historical and must match exactly regardless.

The real cutover runs with intake stopped and **no `--drift`**, where every number must
match exactly.

Note the verifier opens the file with `safeIntegers`. Without it, `race_flags` comes back
through a double and loses its low bits — the values are stored exactly, but a naive
readback reports false mismatches.

## 4. acceptance: serve it through the read API

```
bun run wow.export/archavon_read_check.ts --db ./archavon.sqlite
```

Copies the file to a scratch directory, writes a throwaway `env.php` pointing
`SQLITE_PATH` at it (`index.php` honours `ARCHAVON_ENV`), spawns `php -S` against
`F:\archavon` (override with `ARCHAVON_PHP_ROOT`), and drives the read endpoints:
`/api/v1/stats` counts and products against MySQL, entity detail routes compared
field-by-field, quest junction data, list/export pagination, hotfixes, tables,
submissions, binaryhashes and sync. Counts the snapshot is slightly behind on are warned,
not failed, for the same reason as above.

## dry run, 2026-08-11

Export 13m19s, 9.9M rows, 2.49 GB MySQL -> 2.23 GB SQLite.

- `archavon_verify.ts --drift 0.005` — 43 passed, 44 warned, 0 failed. Every warning is
  the source moving forward during the export; all datetime lower bounds matched exactly,
  and 400 sampled entity rows matched field-by-field across every column.
- `archavon_read_check.ts` — 25 passed, 5 warned, 0 failed.
- `foreign_key_check` reported 12 orphaned `cache_quest_objectives` rows. All three
  orphaned `quest_entry_id`s are higher than the highest `cache_quests.entry_id` copied,
  i.e. quests inserted into MySQL after the `cache_quests` snapshot whose objectives were
  picked up minutes later. This is the live-write race and disappears when intake is
  stopped.

## 5. transfer to dreamhost

The DB is owned by the PHP app and lives outside the web root's served path. Upload beside
the live file and swap with an atomic rename, so a request in flight never sees a partial
file.

```sh
# 1. compress locally; the file is mostly text and shrinks a long way
zstd -19 --long=27 archavon.sqlite -o archavon.sqlite.zst

# 2. upload next to the live db, under a temp name
scp archavon.sqlite.zst <user>@<host>:~/archavon.kruithne.net/db/archavon.db.incoming.zst

# 3. decompress server-side (never decompress over the live file)
ssh <user>@<host> 'cd ~/archavon.kruithne.net/db && zstd -d archavon.db.incoming.zst -o archavon.db.incoming && rm archavon.db.incoming.zst'

# 4. atomic swap; rename(2) within a filesystem is atomic
ssh <user>@<host> 'cd ~/archavon.kruithne.net/db && mv -f archavon.db archavon.db.old 2>/dev/null; mv archavon.db.incoming archavon.db'
```

If `zstd` is unavailable on the shared host, `gzip`/`gunzip` works the same way. `rsync
--partial --progress --inplace=no` is a fine substitute for step 2 on a flaky link, but
still rsync to `.incoming` and rename — never rsync onto the live file.

Notes:

- The file ships in `journal_mode = DELETE`, so there is no `-wal`/`-shm` to move. If a
  previous live db is being replaced, delete its stale `archavon.db-wal` and
  `archavon.db-shm` in the same step or the app will read a mismatched WAL.
- The db directory must be writable by the PHP user, not just the file — SQLite creates
  the WAL and shm files alongside it.
- Keep `archavon.db.old` until the cutover is confirmed, then remove it; it is another
  ~1 GB of the shared-hosting quota.

## 6. cutover order

The dry run above can be repeated as often as needed. The real cutover needs intake
stopped so nothing is written to MySQL after the snapshot:

1. Stop the VPS intake (the submission endpoints in `module.ts`) so no new rows land.
2. Re-run the export from scratch (not `--resume`) and verify. Row counts should now match
   exactly.
3. Run the read-API check.
4. Transfer and swap as above.
5. Flip `DB_BACKEND` to `sqlite` in the live `env.php` (already the default in the repo).
6. Re-enable intake, now pointed at the HMAC write API.
