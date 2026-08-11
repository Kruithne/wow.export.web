/*!
	verifies the exported sqlite file against the live mysql db; exits non-zero on any
	failure so it can gate the upload. check catalogue in db_archavon/MIGRATION.md.

	usage:
	  bun run archavon_verify.ts --db ./archavon.sqlite [--sample 200] [--verbose]
 */

import mysql from 'mysql2/promise';
import { Database } from 'bun:sqlite';

const DEFAULT_DB = './archavon.sqlite';
const DEFAULT_SAMPLE = 200;

const STATE_TABLE = '_migration_state';

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

const ENTITY_TABLES = ['cache_creatures', 'cache_quests', 'cache_gameobjects', 'cache_pagetext'];

const ENTITY_TYPES: Record<string, string> = {
	cache_creatures: 'creature',
	cache_quests: 'quest',
	cache_gameobjects: 'gameobject',
	cache_pagetext: 'pagetext'
};

const UTC_NATIVE_DATETIMES = new Set(['cache_submission_files.modified_at']);

const MYSQL_DATETIME_TYPES = new Set(['datetime', 'timestamp', 'date']);
const MYSQL_BIGINT_TYPES = new Set(['bigint', 'decimal']);

const U64_SIGN_BIT = 1n << 63n;
const U64_MODULUS = 1n << 64n;

// float in mysql is single precision widened to double on the wire; the same widening
// happens on both sides, so equality should hold, but allow for the last ulp
const FLOAT_EPSILON = 1e-9;

// columns the archavon write path keeps mutating after a row is created; against a live
// source these drift for reasons that have nothing to do with the migration
const MUTABLE_COLUMNS = new Set(['attestation_count', 'is_consensus', 'consensus_at']);

let pass_count = 0;
let fail_count = 0;
let warn_count = 0;
let verbose = false;

// when the source is live it only ever runs ahead of the snapshot; --drift accepts a
// shortfall of up to this fraction as concurrency rather than data loss. the real cutover
// runs with intake stopped and no --drift, where every number must match exactly
let drift_allowance = 0;

function mq(ident: string): string {
	return '`' + ident.replaceAll('`', '``') + '`';
}

function sq_ident(ident: string): string {
	return '"' + ident.replaceAll('"', '""') + '"';
}

function check(ok: boolean, label: string, detail = ''): boolean {
	if (ok)
		pass_count++;
	else
		fail_count++;

	if (!ok || verbose)
		console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${label}${detail ? '  ' + detail : ''}`);

	return ok;
}

function section(title: string): void {
	console.log('\n== ' + title + ' ==');
}

// sqlite behind mysql by a hair is concurrency; sqlite ahead of mysql, or a large gap, is
// never explained by concurrency and always fails
function check_drift(my: number, lite: number, label: string, detail = '', bidirectional = false): void {
	if (my === lite) {
		pass_count++;

		if (verbose)
			console.log(`  PASS  ${label}  ${detail}`);

		return;
	}

	const gap = bidirectional ? Math.abs(my - lite) : my - lite;

	if (drift_allowance > 0 && gap > 0 && gap / Math.max(my, 1) <= drift_allowance) {
		warn_count++;
		console.log(`  WARN  ${label}  ${detail}  (differs by ${(lite - my).toLocaleString()}, within drift allowance)`);

		return;
	}

	fail_count++;
	console.log(`  FAIL  ${label}  ${detail}`);
}

function check_bound(my_lo: any, my_hi: any, lite_lo: any, lite_hi: any, label: string, detail: string): void {
	// the lower bound is historical and must always match exactly; only the upper bound
	// can legitimately move while the export runs
	if (my_lo !== lite_lo) {
		fail_count++;
		console.log(`  FAIL  ${label}  min differs: mysql ${my_lo} sqlite ${lite_lo}`);

		return;
	}

	if (my_hi === lite_hi) {
		pass_count++;

		if (verbose)
			console.log(`  PASS  ${label}  ${detail}`);

		return;
	}

	if (drift_allowance > 0 && String(lite_hi ?? '') <= String(my_hi ?? '')) {
		warn_count++;
		console.log(`  WARN  ${label}  max mysql ${my_hi} > sqlite ${lite_hi} (source moved during export)`);

		return;
	}

	fail_count++;
	console.log(`  FAIL  ${label}  ${detail}`);
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
		dateStrings: true,
		supportBigNumbers: true,
		bigNumberStrings: true,
		connectTimeout: 30000
	});
}

function projection(table: string, column: any): string {
	const data_type = String(column.data_type).toLowerCase();

	if (!MYSQL_DATETIME_TYPES.has(data_type))
		return mq(column.name);

	const expr = UTC_NATIVE_DATETIMES.has(table + '.' + column.name)
		? mq(column.name)
		: `CONVERT_TZ(${mq(column.name)}, 'SYSTEM', '+00:00')`;

	return `DATE_FORMAT(${expr}, '%Y-%m-%d %H:%i:%s') AS ${mq(column.name)}`;
}

async function describe(conn: mysql.Connection, table: string): Promise<any[]> {
	const [cols]: any = await conn.query(`
		SELECT COLUMN_NAME AS name, DATA_TYPE AS data_type, COLUMN_TYPE AS column_type
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION`, [table]);

	return cols;
}

// mirrors archavon_migrate.ts: unsigned 64-bit values wrap to signed with the bit
// pattern intact, everything else passes through
function normalise_mysql(value: any, column: any): any {
	if (value === null || value === undefined)
		return null;

	const data_type = String(column.data_type).toLowerCase();

	if (MYSQL_BIGINT_TYPES.has(data_type)) {
		let n = BigInt(value);

		if (String(column.column_type).includes('unsigned') && n >= U64_SIGN_BIT)
			n -= U64_MODULUS;

		return n;
	}

	if (Buffer.isBuffer(value))
		return value;

	return value;
}

function normalise_sqlite(value: any): any {
	if (value === null || value === undefined)
		return null;

	return value;
}

function values_equal(a: any, b: any, column: any): boolean {
	const a_norm = normalise_mysql(a, column);
	const b_norm = normalise_sqlite(b);

	if (a_norm === null || b_norm === null)
		return a_norm === b_norm;

	if (Buffer.isBuffer(a_norm) || b_norm instanceof Uint8Array) {
		const left = Buffer.isBuffer(a_norm) ? a_norm : Buffer.from(String(a_norm));
		const right = Buffer.from(b_norm as Uint8Array);

		return left.equals(right);
	}

	const data_type = String(column.data_type).toLowerCase();

	// checked before the bigint branch: BigInt() throws on a fractional value
	if (data_type === 'float' || data_type === 'double' || data_type === 'decimal') {
		const left = Number(a_norm);
		const right = Number(b_norm);

		if (Number.isNaN(left) && Number.isNaN(right))
			return true;

		return Math.abs(left - right) <= FLOAT_EPSILON * Math.max(1, Math.abs(left));
	}

	if (typeof a_norm === 'bigint' || typeof b_norm === 'bigint')
		return BigInt(a_norm as any) === BigInt(b_norm as any);

	if (typeof a_norm === 'number' || typeof b_norm === 'number')
		return Number(a_norm) === Number(b_norm);

	return String(a_norm) === String(b_norm);
}

async function verify_counts(conn: mysql.Connection, db: Database): Promise<void> {
	section('row counts');

	for (const table of TABLES) {
		const [rows]: any = await conn.query(`SELECT COUNT(*) AS c FROM ${mq(table)}`);
		const my = Number(rows[0].c);

		const local: any = db.query(`SELECT COUNT(*) AS c FROM ${sq_ident(table)}`).get();
		const lite = Number(local.c);

		check_drift(my, lite, table.padEnd(32), `mysql ${my.toLocaleString()}  sqlite ${lite.toLocaleString()}`);
	}
}

async function verify_entity_samples(conn: mysql.Connection, db: Database, sample: number): Promise<void> {
	section(`entity row sampling (${sample} rows per table, field-by-field)`);

	for (const table of ENTITY_TABLES) {
		const columns = await describe(conn, table);
		const projections = columns.map(c => projection(table, c)).join(', ');

		// sample by id range rather than ORDER BY RAND(), which would sort the whole table
		const [bounds]: any = await conn.query(`SELECT MIN(entry_id) AS lo, MAX(entry_id) AS hi FROM ${mq(table)}`);
		const lo = Number(bounds[0].lo ?? 0);
		const hi = Number(bounds[0].hi ?? 0);

		if (hi === 0) {
			check(true, table.padEnd(32), 'empty');
			continue;
		}

		const ids: number[] = [];
		let attempts = 0;

		while (ids.length < sample && attempts < sample * 20) {
			attempts++;

			const probe = lo + Math.floor(Math.random() * (hi - lo + 1));
			const [row]: any = await conn.query(
				`SELECT entry_id FROM ${mq(table)} WHERE entry_id >= ? ORDER BY entry_id LIMIT 1`, [probe]);

			if (row.length === 0)
				continue;

			const id = Number(row[0].entry_id);
			if (!ids.includes(id))
				ids.push(id);
		}

		const [my_rows]: any = await conn.query(
			`SELECT ${projections} FROM ${mq(table)} WHERE entry_id IN (${ids.map(() => '?').join(', ')})`, ids);

		const lite_rows: any[] = db.query(
			`SELECT * FROM ${sq_ident(table)} WHERE entry_id IN (${ids.map(() => '?').join(', ')})`).all(...ids) as any[];

		const lite_by_id = new Map(lite_rows.map(r => [Number(r.entry_id), r]));

		let mismatches = 0;
		let mutable_mismatches = 0;
		let missing = 0;
		const examples: string[] = [];

		for (const my_row of my_rows) {
			const id = Number(my_row.entry_id);
			const lite_row = lite_by_id.get(id);

			if (lite_row === undefined) {
				missing++;
				examples.push(`entry_id ${id} absent from sqlite`);
				continue;
			}

			for (const column of columns) {
				if (values_equal(my_row[column.name], lite_row[column.name], column))
					continue;

				// consensus and attestation counts are recomputed continuously; against a
				// live source they say nothing about the copy
				if (drift_allowance > 0 && MUTABLE_COLUMNS.has(column.name)) {
					mutable_mismatches++;
					continue;
				}

				mismatches++;

				if (examples.length < 5)
					examples.push(`entry_id ${id}.${column.name}: mysql ${JSON.stringify(String(my_row[column.name]))} sqlite ${JSON.stringify(String(lite_row[column.name]))}`);
			}
		}

		const compared = my_rows.length * columns.length;
		const mutable_note = mutable_mismatches > 0 ? `, ${mutable_mismatches} mutable-column drift ignored` : '';

		check(mismatches === 0 && missing === 0 && my_rows.length === ids.length,
			table.padEnd(32),
			`${my_rows.length} rows x ${columns.length} cols = ${compared.toLocaleString()} fields, ${mismatches} mismatch, ${missing} missing${mutable_note}`);

		for (const example of examples)
			console.log('        ' + example);
	}
}

async function verify_consensus(conn: mysql.Connection, db: Database): Promise<void> {
	section('consensus');

	for (const table of ENTITY_TABLES) {
		const [my]: any = await conn.query(`
			SELECT SUM(is_consensus) AS consensus_rows,
			       DATE_FORMAT(CONVERT_TZ(MAX(consensus_at), 'SYSTEM', '+00:00'), '%Y-%m-%d %H:%i:%s') AS max_consensus_at,
			       SUM(consensus_at IS NOT NULL) AS stamped
			FROM ${mq(table)}`);

		const lite: any = db.query(`
			SELECT SUM(is_consensus) AS consensus_rows, MAX(consensus_at) AS max_consensus_at,
			       SUM(consensus_at IS NOT NULL) AS stamped
			FROM ${sq_ident(table)}`).get();

		const my_rows = Number(my[0].consensus_rows ?? 0);
		const lite_rows = Number(lite.consensus_rows ?? 0);

		check_drift(my_rows, lite_rows, (table + ' is_consensus').padEnd(32), `mysql ${my_rows.toLocaleString()}  sqlite ${lite_rows.toLocaleString()}`, true);
		check_drift(Number(my[0].stamped ?? 0), Number(lite.stamped ?? 0), (table + ' consensus_at set').padEnd(32), `mysql ${my[0].stamped}  sqlite ${lite.stamped}`, true);
		check_bound(null, my[0].max_consensus_at, null, lite.max_consensus_at, (table + ' max(consensus_at)').padEnd(32), `mysql ${my[0].max_consensus_at}  sqlite ${lite.max_consensus_at}`);
	}
}

async function verify_attestations(conn: mysql.Connection, db: Database, sample: number): Promise<void> {
	section('attestations');

	const [my_types]: any = await conn.query('SELECT entity_type, COUNT(*) AS c FROM wdb_attestations GROUP BY entity_type ORDER BY entity_type');
	const lite_types: any[] = db.query('SELECT entity_type, COUNT(*) AS c FROM wdb_attestations GROUP BY entity_type ORDER BY entity_type').all() as any[];
	const lite_by_type = new Map(lite_types.map(r => [r.entity_type, Number(r.c)]));

	for (const row of my_types) {
		const my = Number(row.c);
		const lite = lite_by_type.get(row.entity_type) ?? 0;

		check_drift(my, lite, ('type ' + row.entity_type).padEnd(32), `mysql ${my.toLocaleString()}  sqlite ${lite.toLocaleString()}`);
	}

	check(my_types.length === lite_types.length, 'entity_type domain'.padEnd(32), `mysql ${my_types.length} distinct, sqlite ${lite_types.length}`);

	// attestation_count is denormalised on the entity row; sample entries and confirm
	// the per-entry attestation rows travelled with them
	for (const table of ENTITY_TABLES) {
		const entity_type = ENTITY_TYPES[table];

		// anchored at a random point rather than the newest ids: the newest rows are the
		// ones the live worker is still writing, so they measure concurrency, not the copy
		const [span]: any = await conn.query(
			'SELECT MIN(entry_id) AS lo, MAX(entry_id) AS hi FROM wdb_attestations WHERE entity_type = ?', [entity_type]);

		const lo = Number(span[0].lo ?? 0);
		const hi = Number(span[0].hi ?? 0);
		const anchor = lo + Math.floor(Math.random() * Math.max(hi - lo, 1));

		const [entries]: any = await conn.query(`
			SELECT a.entry_id, COUNT(*) AS c
			FROM wdb_attestations a
			WHERE a.entity_type = ? AND a.entry_id >= ?
			GROUP BY a.entry_id
			ORDER BY a.entry_id
			LIMIT ?`, [entity_type, anchor, sample]);

		if (entries.length === 0) {
			check(true, (entity_type + ' per-entry counts').padEnd(32), 'no attestations');
			continue;
		}

		const ids = entries.map((e: any) => Number(e.entry_id));
		const lite_rows: any[] = db.query(`
			SELECT entry_id, COUNT(*) AS c FROM wdb_attestations
			WHERE entity_type = ? AND entry_id IN (${ids.map(() => '?').join(', ')})
			GROUP BY entry_id`).all(entity_type, ...ids) as any[];

		const lite_by_id = new Map(lite_rows.map(r => [Number(r.entry_id), Number(r.c)]));

		let bad = 0;
		let behind = 0;

		for (const entry of entries) {
			const my = Number(entry.c);
			const lite = lite_by_id.get(Number(entry.entry_id)) ?? 0;

			if (my === lite)
				continue;

			if (drift_allowance > 0 && lite < my)
				behind++;
			else
				bad++;
		}

		check(bad === 0, (entity_type + ' per-entry counts').padEnd(32),
			`${entries.length} entries sampled, ${bad} mismatch${behind > 0 ? `, ${behind} behind (live writes)` : ''}`);
	}
}

async function verify_hotfix_blobs(conn: mysql.Connection, db: Database, sample: number): Promise<void> {
	section('hotfix blobs');

	const [rows]: any = await conn.query(`
		SELECT table_hash, record_id, push_id, product, data_blob, LENGTH(data_blob) AS blob_len
		FROM hotfix_entries
		WHERE data_blob IS NOT NULL
		ORDER BY table_hash, record_id, push_id, product
		LIMIT ?`, [sample]);

	const stmt = db.prepare('SELECT data_blob FROM hotfix_entries WHERE table_hash = ? AND record_id = ? AND push_id = ? AND product = ?');

	let bad = 0;
	let bytes = 0;

	for (const row of rows) {
		const lite: any = stmt.get(Number(row.table_hash), Number(row.record_id), Number(row.push_id), row.product);

		if (lite === null || lite.data_blob === null) {
			bad++;
			continue;
		}

		const left = row.data_blob as Buffer;
		const right = Buffer.from(lite.data_blob as Uint8Array);
		bytes += left.length;

		if (!left.equals(right))
			bad++;
	}

	check(bad === 0 && rows.length > 0, 'blob byte equality'.padEnd(32), `${rows.length} blobs, ${bytes.toLocaleString()} bytes, ${bad} mismatch`);

	const [nulls]: any = await conn.query('SELECT COUNT(*) AS c FROM hotfix_entries WHERE data_blob IS NULL');
	const lite_nulls: any = db.query('SELECT COUNT(*) AS c FROM hotfix_entries WHERE data_blob IS NULL').get();

	check_drift(Number(nulls[0].c), Number(lite_nulls.c), 'null blob count'.padEnd(32), `mysql ${nulls[0].c}  sqlite ${lite_nulls.c}`);

	const [len]: any = await conn.query('SELECT SUM(LENGTH(data_blob)) AS total FROM hotfix_entries');
	const lite_len: any = db.query('SELECT SUM(LENGTH(data_blob)) AS total FROM hotfix_entries').get();

	check_drift(Number(len[0].total), Number(lite_len.total), 'total blob bytes'.padEnd(32), `mysql ${Number(len[0].total).toLocaleString()}  sqlite ${Number(lite_len.total).toLocaleString()}`);
}

async function verify_datetime_boundaries(conn: mysql.Connection, db: Database): Promise<void> {
	section('datetime boundaries (converted)');

	for (const table of TABLES) {
		const columns = (await describe(conn, table)).filter(c => MYSQL_DATETIME_TYPES.has(String(c.data_type).toLowerCase()));

		for (const column of columns) {
			const expr = UTC_NATIVE_DATETIMES.has(table + '.' + column.name)
				? mq(column.name)
				: `CONVERT_TZ(${mq(column.name)}, 'SYSTEM', '+00:00')`;

			const [my]: any = await conn.query(`
				SELECT DATE_FORMAT(MIN(${expr}), '%Y-%m-%d %H:%i:%s') AS lo,
				       DATE_FORMAT(MAX(${expr}), '%Y-%m-%d %H:%i:%s') AS hi,
				       SUM(${mq(column.name)} IS NULL) AS nulls
				FROM ${mq(table)}`);

			const lite: any = db.query(`
				SELECT MIN(${sq_ident(column.name)}) AS lo, MAX(${sq_ident(column.name)}) AS hi,
				       SUM(${sq_ident(column.name)} IS NULL) AS nulls
				FROM ${sq_ident(table)}`).get();

			check_bound(my[0].lo, my[0].hi, lite.lo, lite.hi, `${table}.${column.name}`.padEnd(40),
				`${my[0].lo} .. ${my[0].hi}  sqlite ${lite.lo} .. ${lite.hi}`);

			// null counts move with the row count, so they drift the same way
			check_drift(Number(my[0].nulls), Number(lite.nulls), `${table}.${column.name} nulls`.padEnd(40),
				`mysql ${my[0].nulls}  sqlite ${lite.nulls}`, true);
		}
	}
}

function verify_structure(db: Database): void {
	section('sqlite structure');

	const integrity: any = db.query('PRAGMA integrity_check').get();
	check(integrity.integrity_check === 'ok', 'integrity_check'.padEnd(32), String(integrity.integrity_check));

	db.exec('PRAGMA foreign_keys = ON');
	const fk: any[] = db.query('PRAGMA foreign_key_check').all() as any[];

	// a child table is copied after its parent, so a row inserted into mysql in between
	// arrives orphaned. only possible while the source is live
	if (fk.length > 0 && drift_allowance > 0) {
		warn_count++;
		console.log(`  WARN  ${'foreign_key_check'.padEnd(32)}  ${fk.length} orphan(s) from rows created mid-export`);
	} else {
		check(fk.length === 0, 'foreign_key_check'.padEnd(32), `${fk.length} violation(s)`);
	}

	for (const violation of fk.slice(0, 5))
		console.log(`        ${violation.table} rowid ${violation.rowid} -> ${violation.parent}`);

	for (const table of ENTITY_TABLES) {
		const max_row: any = db.query(`SELECT MAX(entry_id) AS hi FROM ${sq_ident(table)}`).get();
		const seq_row: any = db.query('SELECT seq FROM sqlite_sequence WHERE name = ?').get(table);

		const hi = Number(max_row?.hi ?? 0);
		const seq = Number(seq_row?.seq ?? -1);

		check(seq >= hi, (table + ' sqlite_sequence').padEnd(32), `seq ${seq.toLocaleString()} >= max(entry_id) ${hi.toLocaleString()}`);
	}

	const leftover: any = db.query(`SELECT COUNT(*) AS c FROM sqlite_master WHERE type = 'table' AND name = ?`).get(STATE_TABLE);
	check(Number(leftover.c) === 0, 'migration state removed'.padEnd(32), Number(leftover.c) === 0 ? '' : `${STATE_TABLE} still present`);

	const journal: any = db.query('PRAGMA journal_mode').get();
	check(journal.journal_mode === 'delete', 'journal mode'.padEnd(32), String(journal.journal_mode) + ' (single-file, ready to transfer)');

	// the schema's CHECK constraints are enforced on insert, but an explicit sweep proves
	// no enum value slipped through as something the read api cannot render
	const domains: Array<[string, string, string[]]> = [
		['cache_submissions', 'status', ['pending', 'finalized', 'processing', 'completed', 'partial', 'failed']],
		['cache_submission_files', 'status', ['pending', 'completed', 'rejected']],
		['wdb_attestations', 'entity_type', ['creature', 'quest', 'gameobject', 'pagetext']],
		['cache_quest_conditional_texts', 'text_type', ['description', 'completion']]
	];

	for (const [table, column, allowed] of domains) {
		const rows: any[] = db.query(`SELECT DISTINCT ${sq_ident(column)} AS v FROM ${sq_ident(table)}`).all() as any[];
		const bad = rows.filter(r => r.v !== null && !allowed.includes(r.v));

		check(bad.length === 0, `${table}.${column} domain`.padEnd(40), `${rows.length} distinct value(s)${bad.length ? ': ' + bad.map(b => b.v).join(', ') : ''}`);
	}
}

async function main(): Promise<void> {
	const argv = process.argv.slice(2);
	let db_path = DEFAULT_DB;
	let sample = DEFAULT_SAMPLE;

	for (let i = 0; i < argv.length; i++) {
		if (argv[i] === '--db')
			db_path = argv[++i];
		else if (argv[i] === '--sample')
			sample = parseInt(argv[++i], 10);
		else if (argv[i] === '--verbose')
			verbose = true;
		else if (argv[i] === '--drift')
			drift_allowance = parseFloat(argv[++i]);
		else
			throw new Error('unknown argument: ' + argv[i]);
	}

	// safeIntegers, or the u64 columns come back through a double and lose their low bits
	// -- the values are stored exactly, but a naive readback would report false mismatches
	const db = new Database(db_path, { readonly: true, safeIntegers: true });
	const conn = await connect_mysql();

	console.log(`verifying ${db_path} against mysql (sample size ${sample}` +
		(drift_allowance > 0 ? `, drift allowance ${(drift_allowance * 100).toFixed(2)}%` : ', strict') + ')');

	try {
		await verify_counts(conn, db);
		await verify_entity_samples(conn, db, sample);
		await verify_consensus(conn, db);
		await verify_attestations(conn, db, sample);
		await verify_hotfix_blobs(conn, db, sample);
		await verify_datetime_boundaries(conn, db);
		verify_structure(db);
	} finally {
		await conn.end();
		db.close();
	}

	console.log(`\n${fail_count === 0 ? 'VERIFICATION PASSED' : 'VERIFICATION FAILED'}: ${pass_count} passed, ${warn_count} warned, ${fail_count} failed`);

	if (drift_allowance > 0)
		console.log('warnings are the live source moving during the export; run the cutover with intake stopped and no --drift, where everything must match exactly');

	process.exit(fail_count === 0 ? 0 : 1);
}

await main();
