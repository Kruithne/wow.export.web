/*!
	one-off migration: dreamhost mysql -> sqlite. conversions, pitfalls and procedure
	are documented in db_archavon/MIGRATION.md.

	usage:
	  bun run archavon_migrate.ts --sizes            report information_schema sizes, exit
	  bun run archavon_migrate.ts                    fresh export to ./archavon.sqlite
	  bun run archavon_migrate.ts --resume           continue an interrupted export
	  bun run archavon_migrate.ts --tables a,b       restrict to named tables
 */

import mysql from 'mysql2/promise';
import { Database } from 'bun:sqlite';
import path from 'node:path';
import fs from 'node:fs';

const DEFAULT_OUT = './archavon.sqlite';
const DEFAULT_SCHEMA = path.join(import.meta.dir, 'db_archavon', 'schema.sql');
const DEFAULT_BATCH = 20000;

const STATE_TABLE = '_migration_state';

// an uncheckpointed wal grows without bound and every subsequent write pays to scan its
// index; on wdb_attestations that took throughput from 24k rows/s to under 8k. truncating
// periodically keeps it flat and preserves the resume guarantee, which journal_mode = OFF
// would not
const CHECKPOINT_INTERVAL = 250000;

// live writes can move the count either way between planning and copying; anything worse
// than this is a bug in the scan, not concurrency
const SHORTFALL_TOLERANCE = 0.999;

// parents before children; foreign keys are off during load, but a natural order keeps
// the final foreign_key_check meaningful if the run is interrupted
const TABLES = [
	'cache_submissions',
	'cache_submission_files',
	'machines',
	'db2_table_hashes',
	'cache_binary_hashes',
	'hotfix_entries',
	'cache_creatures',
	'cache_quests',
	'cache_quest_objectives',
	'cache_quest_conditional_texts',
	'cache_gameobjects',
	'cache_pagetext',
	'wdb_attestations'
];

// mysql-only bookkeeping from spooder's db_schema migrator; has no sqlite counterpart
const IGNORED_MYSQL_TABLES = new Set(['db_schema']);

// sqlite tables with no mysql source; written by the archavon write API from now on
const SQLITE_ONLY_TABLES = new Set(['delta_applications']);

// datetime columns already stored in utc; every other datetime is server-local
const UTC_NATIVE_DATETIMES = new Set(['cache_submission_files.modified_at']);

const AUTOINCREMENT_TABLES = ['cache_creatures', 'cache_quests', 'cache_gameobjects', 'cache_pagetext'];

const U64_SIGN_BIT = 1n << 63n;
const U64_MODULUS = 1n << 64n;
const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);

const MYSQL_DATETIME_TYPES = new Set(['datetime', 'timestamp', 'date']);
const MYSQL_BIGINT_TYPES = new Set(['bigint', 'decimal']);

type MysqlColumn = {
	name: string;
	data_type: string;
	column_type: string;
	nullable: boolean;
};

type TablePlan = {
	name: string;
	columns: MysqlColumn[];
	pk: string[];
	scan_pk: string[];
	partitions: any[][];
	first_sql: string;
	next_sql: string;
	insert_sql: string;
	converters: Array<(value: any) => any>;
	total: number;
};

function parse_args() {
	const argv = process.argv.slice(2);
	const opts = {
		out: DEFAULT_OUT,
		schema: DEFAULT_SCHEMA,
		batch: DEFAULT_BATCH,
		tables: TABLES,
		resume: false,
		sizes_only: false,
		keep_state: false
	};

	for (let i = 0; i < argv.length; i++) {
		const arg = argv[i];

		if (arg === '--resume')
			opts.resume = true;
		else if (arg === '--sizes')
			opts.sizes_only = true;
		else if (arg === '--keep-state')
			opts.keep_state = true;
		else if (arg === '--out')
			opts.out = argv[++i];
		else if (arg === '--schema')
			opts.schema = argv[++i];
		else if (arg === '--batch')
			opts.batch = parseInt(argv[++i], 10);
		else if (arg === '--tables')
			opts.tables = argv[++i].split(',').map(t => t.trim()).filter(Boolean);
		else
			throw new Error('unknown argument: ' + arg);
	}

	const unknown = opts.tables.filter(t => !TABLES.includes(t));
	if (unknown.length > 0)
		throw new Error('unknown table(s): ' + unknown.join(', '));

	return opts;
}

function mq(ident: string): string {
	return '`' + ident.replaceAll('`', '``') + '`';
}

function sq_ident(ident: string): string {
	return '"' + ident.replaceAll('"', '""') + '"';
}

function fmt_bytes(n: number): string {
	if (n >= 1073741824)
		return (n / 1073741824).toFixed(2) + ' GB';

	if (n >= 1048576)
		return (n / 1048576).toFixed(1) + ' MB';

	return (n / 1024).toFixed(1) + ' KB';
}

function fmt_duration(ms: number): string {
	const s = Math.round(ms / 1000);
	if (s < 60)
		return s + 's';

	const m = Math.floor(s / 60);
	if (m < 60)
		return m + 'm' + String(s % 60).padStart(2, '0') + 's';

	return Math.floor(m / 60) + 'h' + String(m % 60).padStart(2, '0') + 'm';
}

async function connect_mysql() {
	const uri = process.env.DB_URI_ARCHAVON;
	if (uri === undefined)
		throw new Error('process.env.DB_URI_ARCHAVON not configured');

	const url = new URL(uri);

	return await mysql.createConnection({
		host: url.hostname,
		port: Number(url.port || 3306),
		user: decodeURIComponent(url.username),
		password: decodeURIComponent(url.password),
		database: url.pathname.slice(1),

		// dateStrings keeps mysql wall-clock text out of JS Date, which would reinterpret
		// it in the local zone; supportBigNumbers/bigNumberStrings preserve u64 exactly
		dateStrings: true,
		supportBigNumbers: true,
		bigNumberStrings: true,

		// one connection, well under the 100-connection cap
		connectTimeout: 30000
	});
}

async function report_sizes(conn: mysql.Connection): Promise<void> {
	const [rows]: any = await conn.query(`
		SELECT TABLE_NAME AS name, TABLE_ROWS AS est_rows, DATA_LENGTH AS data_len,
		       INDEX_LENGTH AS index_len, DATA_LENGTH + INDEX_LENGTH AS total
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE()
		ORDER BY total DESC`);

	console.log('mysql table sizes (information_schema, row counts are estimates):');

	let sum = 0;
	for (const row of rows) {
		const total = Number(row.total ?? 0);
		sum += total;

		console.log(
			'  ' + String(row.name).padEnd(32) +
			('~' + Number(row.est_rows ?? 0).toLocaleString()).padStart(12) + ' rows' +
			'  data ' + fmt_bytes(Number(row.data_len ?? 0)).padStart(10) +
			'  index ' + fmt_bytes(Number(row.index_len ?? 0)).padStart(10) +
			'  total ' + fmt_bytes(total).padStart(10)
		);
	}

	console.log('  ' + 'TOTAL'.padEnd(32) + '  total ' + fmt_bytes(sum));
}

async function describe_table(conn: mysql.Connection, table: string): Promise<{ columns: MysqlColumn[], pk: string[] }> {
	const [cols]: any = await conn.query(`
		SELECT COLUMN_NAME AS name, DATA_TYPE AS data_type, COLUMN_TYPE AS column_type, IS_NULLABLE AS nullable
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION`, [table]);

	if (cols.length === 0)
		throw new Error('mysql table not found: ' + table);

	const [keys]: any = await conn.query(`
		SELECT COLUMN_NAME AS name
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = 'PRIMARY'
		ORDER BY SEQ_IN_INDEX`, [table]);

	if (keys.length === 0)
		throw new Error('no primary key on ' + table + '; keyset pagination requires one');

	return {
		columns: cols.map((c: any) => ({
			name: c.name,
			data_type: String(c.data_type).toLowerCase(),
			column_type: String(c.column_type).toLowerCase(),
			nullable: c.nullable === 'YES'
		})),
		pk: keys.map((k: any) => k.name)
	};
}

// mysql CONVERT_TZ returns NULL when the zone tables are unavailable, which would
// silently null out every timestamp; prove it works before trusting it
async function assert_tz_conversion(conn: mysql.Connection): Promise<void> {
	const [rows]: any = await conn.query(`
		SELECT CONVERT_TZ('2026-01-15 12:00:00', 'SYSTEM', '+00:00') AS winter,
		       CONVERT_TZ('2026-07-15 12:00:00', 'SYSTEM', '+00:00') AS summer,
		       TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), NOW()) AS offset_sec`);

	const { winter, summer, offset_sec } = rows[0];

	if (winter === null || summer === null)
		throw new Error('CONVERT_TZ returned NULL; mysql timezone tables are not loaded, cannot convert datetimes');

	if (winter === summer)
		throw new Error('CONVERT_TZ is not DST-aware for this zone; refusing to convert');

	console.log('mysql clock: offset from utc ' + (Number(offset_sec) / 3600).toFixed(1) + 'h, CONVERT_TZ DST-aware (jan -> ' + winter + ', jul -> ' + summer + ')');
}

// a non-null datetime that converts to NULL means an unrepresentable value (a DST gap,
// or the zero date); fail rather than write a null into a NOT NULL column
async function assert_no_lossy_datetimes(conn: mysql.Connection, table: string, columns: MysqlColumn[]): Promise<void> {
	const checks = columns
		.filter(c => MYSQL_DATETIME_TYPES.has(c.data_type) && !UTC_NATIVE_DATETIMES.has(table + '.' + c.name))
		.map(c => `SUM(${mq(c.name)} IS NOT NULL AND CONVERT_TZ(${mq(c.name)}, 'SYSTEM', '+00:00') IS NULL) AS ${mq(c.name)}`);

	if (checks.length === 0)
		return;

	const [rows]: any = await conn.query(`SELECT ${checks.join(', ')} FROM ${mq(table)}`);

	for (const [name, count] of Object.entries(rows[0])) {
		if (Number(count) > 0)
			throw new Error(`${table}.${name}: ${count} value(s) cannot be converted to utc`);
	}
}

export function projection(table: string, c: MysqlColumn): string {
	if (!MYSQL_DATETIME_TYPES.has(c.data_type))
		return mq(c.name);

	const expr = UTC_NATIVE_DATETIMES.has(table + '.' + c.name)
		? mq(c.name)
		: `CONVERT_TZ(${mq(c.name)}, 'SYSTEM', '+00:00')`;

	return `DATE_FORMAT(${expr}, '%Y-%m-%d %H:%i:%s') AS ${mq(c.name)}`;
}

function enum_values(column_type: string): string[] {
	const body = column_type.slice(column_type.indexOf('(') + 1, column_type.lastIndexOf(')'));

	return body.split(',').map(v => v.trim().replace(/^'|'$/g, '').replaceAll("''", "'"));
}

/*
	an ENUM in the primary key cannot be keyset-paginated with a row-value comparison:
	ORDER BY sorts by the enum's declaration index, but `col > 'literal'` compares
	lexically, and the two disagree. wdb_attestations declares
	('creature','quest','gameobject','pagetext'), so the scan reached the end of `quest`
	and then found nothing "greater" -- silently dropping every gameobject and pagetext
	row, 2.3M of 6.2M.

	so enum key columns are pinned by equality and iterated as separate partitions, and
	only the non-enum key columns take part in the range comparison. equality on a low-
	cardinality column keeps the primary index usable and removes the ambiguity entirely.
*/
function build_partitions(columns: MysqlColumn[], pk: string[]): { enum_pk: string[], scan_pk: string[], partitions: any[][] } {
	const enum_pk = pk.filter(k => columns.find(c => c.name === k)?.data_type === 'enum');
	const scan_pk = pk.filter(k => !enum_pk.includes(k));

	if (enum_pk.length === 0)
		return { enum_pk, scan_pk, partitions: [[]] };

	if (scan_pk.length === 0)
		throw new Error('primary key is entirely enum columns; cannot paginate');

	let partitions: any[][] = [[]];

	for (const key of enum_pk) {
		const values = enum_values(columns.find(c => c.name === key)!.column_type);
		partitions = partitions.flatMap(prefix => values.map(v => [...prefix, v]));
	}

	return { enum_pk, scan_pk, partitions };
}

// two variants rather than one with an `OR` guard: a bare range predicate keeps the
// primary index usable, which matters at 6M rows
function build_selects(table: string, columns: MysqlColumn[], pk: string[], enum_pk: string[], scan_pk: string[]): { first: string, next: string } {
	const projections = columns.map(c => projection(table, c)).join(', ');

	const head = `SELECT ${projections} FROM ${mq(table)}`;
	const tail = ` ORDER BY ${pk.map(mq).join(', ')} LIMIT ?`;

	const pinned = enum_pk.map(k => `${mq(k)} = ?`);
	const range = `(${scan_pk.map(mq).join(', ')}) > (${scan_pk.map(() => '?').join(', ')})`;

	const where = (extra: string[]) => {
		const clauses = [...pinned, ...extra];
		return clauses.length === 0 ? '' : ' WHERE ' + clauses.join(' AND ');
	};

	return {
		first: head + where([]) + tail,
		next: head + where([range]) + tail
	};
}

function build_converters(columns: MysqlColumn[]): Array<(value: any) => any> {
	return columns.map(c => {
		if (MYSQL_BIGINT_TYPES.has(c.data_type)) {
			const unsigned = c.column_type.includes('unsigned');

			return (value: any) => {
				if (value === null || value === undefined)
					return null;

				let n = BigInt(value);

				// two's complement wrap, bit pattern preserved; matches wdb_delta.ts
				if (unsigned && n >= U64_SIGN_BIT)
					n -= U64_MODULUS;

				return (n <= MAX_SAFE && n >= -MAX_SAFE) ? Number(n) : n;
			};
		}

		if (c.data_type === 'bit') {
			return (value: any) => {
				if (value === null || value === undefined)
					return null;

				return Buffer.isBuffer(value) ? Number(value[0]) : Number(value);
			};
		}

		return (value: any) => value === undefined ? null : value;
	});
}

// splits schema.sql into table statements and index statements so indexes can be built
// after the bulk load; also surfaces duplicate-key violations as a loud CREATE UNIQUE
// INDEX failure rather than a per-row insert error
function split_schema(sql: string): { tables: string[], indexes: string[], other: string[] } {
	const stripped = sql.split('\n').map(line => {
		const at = line.indexOf('--');
		return at === -1 ? line : line.slice(0, at);
	}).join('\n');

	const tables: string[] = [];
	const indexes: string[] = [];
	const other: string[] = [];

	for (const raw of stripped.split(';')) {
		const stmt = raw.trim();
		if (stmt.length === 0)
			continue;

		const head = stmt.slice(0, 32).toUpperCase();

		if (head.startsWith('CREATE TABLE'))
			tables.push(stmt);
		else if (head.startsWith('CREATE INDEX') || head.startsWith('CREATE UNIQUE INDEX'))
			indexes.push(stmt);
		else
			other.push(stmt);
	}

	return { tables, indexes, other };
}

function open_sqlite(out: string, schema_path: string, fresh: boolean): { db: Database, indexes: string[] } {
	if (!fs.existsSync(schema_path))
		throw new Error('sqlite schema not found: ' + schema_path);

	const schema = split_schema(fs.readFileSync(schema_path, 'utf8'));

	if (fresh) {
		for (const suffix of ['', '-wal', '-shm', '-journal']) {
			if (fs.existsSync(out + suffix))
				fs.rmSync(out + suffix);
		}
	}

	const db = new Database(out, { create: true });

	// import-speed pragmas; foreign keys stay off because tables load independently and
	// wdb_attestations has no declared FK anyway. finalise() restores durable settings
	db.exec('PRAGMA journal_mode = WAL');
	db.exec('PRAGMA synchronous = OFF');
	db.exec('PRAGMA foreign_keys = OFF');
	db.exec('PRAGMA temp_store = MEMORY');
	db.exec('PRAGMA cache_size = -262144');

	for (const stmt of schema.tables)
		db.exec(stmt);

	db.exec(`CREATE TABLE IF NOT EXISTS ${STATE_TABLE} (
		table_name TEXT NOT NULL PRIMARY KEY,
		last_key TEXT NULL,
		rows_done INTEGER NOT NULL DEFAULT 0,
		done INTEGER NOT NULL DEFAULT 0,
		updated_at TEXT NOT NULL
	)`);

	return { db, indexes: schema.indexes };
}

function assert_columns_match(db: Database, table: string, columns: MysqlColumn[]): void {
	const info: any[] = db.query(`PRAGMA table_info(${sq_ident(table)})`).all() as any[];

	if (info.length === 0)
		throw new Error('sqlite table missing from schema: ' + table);

	const sqlite_cols = new Set(info.map(c => c.name));
	const mysql_cols = new Set(columns.map(c => c.name));

	const missing = [...mysql_cols].filter(c => !sqlite_cols.has(c));
	if (missing.length > 0)
		throw new Error(`${table}: column(s) present in mysql but not sqlite: ${missing.join(', ')}`);

	// a sqlite-only column is fine only if the schema can fill it itself
	const extra = info.filter(c => !mysql_cols.has(c.name) && c.notnull === 1 && c.dflt_value === null && c.pk === 0);
	if (extra.length > 0)
		throw new Error(`${table}: sqlite column(s) with no mysql source and no default: ${extra.map(c => c.name).join(', ')}`);
}

async function migrate_table(conn: mysql.Connection, db: Database, plan: TablePlan, batch: number): Promise<void> {
	const state_row: any = db.query(`SELECT last_key, rows_done, done FROM ${STATE_TABLE} WHERE table_name = ?`).get(plan.name);

	if (state_row?.done === 1) {
		console.log(`  ${plan.name.padEnd(32)} already complete (${Number(state_row.rows_done).toLocaleString()} rows)`);
		return;
	}

	const saved = state_row?.last_key ? JSON.parse(state_row.last_key) : null;

	let partition_index = Number(saved?.p ?? 0);
	let last_key: any[] | null = saved?.k ?? null;
	let rows_done = Number(state_row?.rows_done ?? 0);

	const insert = db.prepare(plan.insert_sql);
	const save_state = db.prepare(`INSERT INTO ${STATE_TABLE} (table_name, last_key, rows_done, done, updated_at)
		VALUES (?, ?, ?, ?, datetime('now'))
		ON CONFLICT (table_name) DO UPDATE SET last_key = excluded.last_key, rows_done = excluded.rows_done, done = excluded.done, updated_at = excluded.updated_at`);

	const scan_index = plan.scan_pk.map(k => plan.columns.findIndex(c => c.name === k));

	// one transaction per batch: the state write commits with the rows it describes, so
	// an interrupted run resumes exactly where it stopped with no duplicates
	const write_batch = db.transaction((values: any[][], cursor: string, done_count: number) => {
		for (const row of values)
			insert.run(...row);

		save_state.run(plan.name, cursor, done_count, 0);
	});

	const started = Bun.nanoseconds();
	let last_report = 0;
	let last_checkpoint = rows_done;

	while (partition_index < plan.partitions.length) {
		const pinned = plan.partitions[partition_index];

		const [rows]: any = last_key === null
			? await conn.query(plan.first_sql, [...pinned, batch])
			: await conn.query(plan.next_sql, [...pinned, ...last_key, batch]);

		if (rows.length === 0) {
			partition_index++;
			last_key = null;
			continue;
		}

		const values: any[][] = new Array(rows.length);

		for (let i = 0; i < rows.length; i++) {
			const row = rows[i];
			const out = new Array(plan.columns.length);

			for (let c = 0; c < plan.columns.length; c++)
				out[c] = plan.converters[c](row[plan.columns[c].name]);

			values[i] = out;
		}

		const tail = values[values.length - 1];

		// bigints round-trip through JSON.parse as numbers; keys are ids well inside the
		// safe range, but stringify them so mysql receives the same literal either way
		const next_key = scan_index.map(i => typeof tail[i] === 'bigint' ? tail[i].toString() : tail[i]);

		rows_done += rows.length;
		write_batch(values, JSON.stringify({ p: partition_index, k: next_key }), rows_done);

		last_key = next_key;

		if (rows_done - last_checkpoint >= CHECKPOINT_INTERVAL) {
			last_checkpoint = rows_done;
			db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
		}

		const elapsed = (Bun.nanoseconds() - started) / 1e9;
		if (elapsed - last_report > 2 || rows.length < batch) {
			last_report = elapsed;

			const rate = rows_done / Math.max(elapsed, 0.001);
			const remaining = Math.max(plan.total - rows_done, 0);
			const pct = plan.total > 0 ? (rows_done / plan.total * 100).toFixed(1) : '100.0';

			process.stdout.write(`\r  ${plan.name.padEnd(32)} ${rows_done.toLocaleString().padStart(10)} / ${plan.total.toLocaleString().padEnd(10)} ${pct.padStart(5)}%  ${Math.round(rate).toLocaleString()}/s  eta ${fmt_duration(remaining / Math.max(rate, 1) * 1000)}\x1b[K`);
		}

		if (rows.length < batch) {
			partition_index++;
			last_key = null;
		}
	}

	save_state.run(plan.name, JSON.stringify({ p: partition_index, k: null }), rows_done, 1);

	const elapsed = (Bun.nanoseconds() - started) / 1e9;
	process.stdout.write(`\r  ${plan.name.padEnd(32)} ${rows_done.toLocaleString().padStart(10)} rows in ${fmt_duration(elapsed * 1000)}\x1b[K\n`);

	// the source is live, so a small surplus (or a deficit from a concurrent prune) is
	// expected; a real shortfall means the scan terminated early and must not pass
	if (rows_done < plan.total * SHORTFALL_TOLERANCE)
		throw new Error(`${plan.name}: copied ${rows_done.toLocaleString()} of ${plan.total.toLocaleString()} rows; the scan ended early`);

	if (rows_done !== plan.total)
		console.log(`  ~ ${plan.name}: copied ${rows_done.toLocaleString()}, mysql reported ${plan.total.toLocaleString()} at planning time (concurrent writes)`);
}

function finalise(db: Database, indexes: string[], keep_state: boolean): void {
	console.log('\nbuilding indexes:');

	for (const stmt of indexes) {
		const name = stmt.match(/CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)/i)?.[1] ?? '?';
		const started = Bun.nanoseconds();

		db.exec(stmt);
		console.log(`  ${name.padEnd(40)} ${fmt_duration((Bun.nanoseconds() - started) / 1e6)}`);
	}

	// entry_id values are carried over verbatim, so the AUTOINCREMENT counter has to be
	// advanced past them or the next insert collides with a migrated row
	console.log('\nseeding sqlite_sequence:');

	for (const table of AUTOINCREMENT_TABLES) {
		const row: any = db.query(`SELECT MAX(entry_id) AS hi FROM ${sq_ident(table)}`).get();
		const hi = Number(row?.hi ?? 0);

		db.run('DELETE FROM sqlite_sequence WHERE name = ?', [table]);

		if (hi > 0)
			db.run('INSERT INTO sqlite_sequence (name, seq) VALUES (?, ?)', [table, hi]);

		console.log(`  ${table.padEnd(32)} seq = ${hi.toLocaleString()}`);
	}

	if (!keep_state)
		db.exec(`DROP TABLE IF EXISTS ${STATE_TABLE}`);

	console.log('\nintegrity checks:');

	const integrity: any = db.query('PRAGMA integrity_check').get();
	console.log(`  integrity_check: ${integrity.integrity_check}`);

	db.exec('PRAGMA foreign_keys = ON');
	const fk: any[] = db.query('PRAGMA foreign_key_check').all() as any[];
	console.log(`  foreign_key_check: ${fk.length === 0 ? 'ok' : fk.length + ' violation(s)'}`);

	for (const v of fk.slice(0, 10))
		console.log(`    ${v.table} rowid ${v.rowid} -> ${v.parent}`);

	console.log('\ncompacting:');
	db.exec('PRAGMA synchronous = FULL');
	db.exec('VACUUM');

	// ship a single file; db.php flips it back to WAL on first open
	db.exec('PRAGMA journal_mode = DELETE');
	db.exec('ANALYZE');
}

async function main(): Promise<void> {
	const opts = parse_args();
	const conn = await connect_mysql();

	try {
		await report_sizes(conn);

		if (opts.sizes_only)
			return;

		console.log('');
		await assert_tz_conversion(conn);

		const [mysql_tables]: any = await conn.query(`
			SELECT TABLE_NAME AS name FROM information_schema.TABLES
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'`);

		const present = new Set(mysql_tables.map((t: any) => t.name));
		const unplanned = [...present].filter(t => !TABLES.includes(t as string) && !IGNORED_MYSQL_TABLES.has(t as string));

		if (unplanned.length > 0)
			throw new Error('mysql has table(s) this migration does not know about: ' + unplanned.join(', '));

		const { db, indexes } = open_sqlite(opts.out, opts.schema, !opts.resume);

		try {
			console.log('\nplanning:');
			const plans: TablePlan[] = [];

			for (const table of opts.tables) {
				const { columns, pk } = await describe_table(conn, table);
				assert_columns_match(db, table, columns);
				await assert_no_lossy_datetimes(conn, table, columns);

				const [count]: any = await conn.query(`SELECT COUNT(*) AS c FROM ${mq(table)}`);
				const { enum_pk, scan_pk, partitions } = build_partitions(columns, pk);
				const selects = build_selects(table, columns, pk, enum_pk, scan_pk);

				plans.push({
					name: table,
					columns,
					pk,
					scan_pk,
					partitions,
					first_sql: selects.first,
					next_sql: selects.next,
					insert_sql: `INSERT INTO ${sq_ident(table)} (${columns.map(c => sq_ident(c.name)).join(', ')}) VALUES (${columns.map(() => '?').join(', ')})`,
					converters: build_converters(columns),
					total: Number(count[0].c)
				});

				const partition_note = enum_pk.length > 0 ? `  ${partitions.length} enum partition(s) on ${enum_pk.join(', ')}` : '';
				console.log(`  ${table.padEnd(32)} ${columns.length.toString().padStart(3)} cols  pk(${pk.join(', ')})  ${Number(count[0].c).toLocaleString()} rows${partition_note}`);
			}

			for (const table of SQLITE_ONLY_TABLES)
				console.log(`  ${table.padEnd(32)} no mysql source, left empty`);

			console.log('\ncopying:');
			const started = Bun.nanoseconds();

			for (const plan of plans)
				await migrate_table(conn, db, plan, opts.batch);

			const partial = opts.tables.length !== TABLES.length;

			if (partial) {
				console.log('\npartial run (--tables), skipping index build and finalisation');
				db.exec('PRAGMA wal_checkpoint(TRUNCATE)');
			} else {
				finalise(db, indexes, opts.keep_state);
			}

			const size = fs.statSync(opts.out).size;
			console.log(`\ndone in ${fmt_duration((Bun.nanoseconds() - started) / 1e6)} -> ${path.resolve(opts.out)} (${fmt_bytes(size)})`);
		} finally {
			db.close();
		}
	} finally {
		await conn.end();
	}
}

await main();
