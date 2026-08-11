/*!
	acceptance check for the migrated sqlite file: serves a scratch copy through the
	real archavon read API (php -S + ARCHAVON_ENV) and compares responses against mysql.

	usage:
	  bun run wow.export/archavon_read_check.ts --db ./archavon.sqlite
	  ARCHAVON_PHP_ROOT=F:/archavon bun run wow.export/archavon_read_check.ts --db ...
 */

import mysql from 'mysql2/promise';
import path from 'node:path';
import fs from 'node:fs';
import os from 'node:os';

const DEFAULT_DB = './archavon.sqlite';
const DEFAULT_PHP_ROOT = 'F:/archavon';
const PHP_PORT = 8781;

// generous: the first request has to open a multi-gigabyte database
const BOOT_TIMEOUT_MS = 60000;

// mirrors the migration: mysql reports the raw u64, sqlite stores the signed wrap
const U64_SIGN_BIT = 1n << 63n;
const U64_MODULUS = 1n << 64n;

// datetimes are deliberately shifted to utc by the migration (compared in
// archavon_verify.ts); the rest are rewritten continuously by the live write path
const SKIP_COLUMNS = new Set(['first_seen', 'consensus_at', 'attestation_count', 'is_consensus']);

const ENTITY_ROUTES: Record<string, string> = {
	cache_creatures: 'creatures',
	cache_quests: 'quests',
	cache_gameobjects: 'gameobjects',
	cache_pagetext: 'pagetext'
};

// the snapshot is compared against a still-running source, so the api being slightly
// behind mysql is expected; being ahead, or far behind, is not
const DRIFT_ALLOWANCE = 0.005;

let pass_count = 0;
let warn_count = 0;
let fail_count = 0;

function check(ok: boolean, label: string, detail = ''): void {
	if (ok)
		pass_count++;
	else
		fail_count++;

	console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${label}${detail ? '  ' + detail : ''}`);
}

function check_behind(my: number, api: number, label: string, detail = ''): void {
	if (my === api) {
		pass_count++;
		console.log(`  PASS  ${label}${detail ? '  ' + detail : ''}`);

		return;
	}

	if (api < my && (my - api) / Math.max(my, 1) <= DRIFT_ALLOWANCE) {
		warn_count++;
		console.log(`  WARN  ${label}  ${detail}  (snapshot behind live source by ${(my - api).toLocaleString()})`);

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
		bigNumberStrings: true
	});
}

async function spawn_php(php_root: string, db_path: string) {
	const tmp_root = fs.mkdtempSync(path.join(os.tmpdir(), 'archavon-read-'));
	const served = path.join(tmp_root, 'archavon.db');

	fs.copyFileSync(db_path, served);

	const env_path = path.join(tmp_root, 'env.php');
	fs.writeFileSync(env_path, [
		'<?php',
		`\tdefine('DB_BACKEND', 'sqlite');`,
		`\tdefine('SQLITE_PATH', '${served.replaceAll('\\', '/')}');`,
		`\tdefine('SQLITE_JOURNAL_MODE', 'WAL');`,
		`\tdefine('SQLITE_BUSY_TIMEOUT', 5000);`,
		`\tdefine('GITHUB_SECRET', 'unused');`,
		`\tdefine('ARCHAVON_WRITE_SECRET', 'unused');`,
		`\tdefine('DELTA_TMP_DIR', '${tmp_root.replaceAll('\\', '/')}');`,
		`\tdefine('DELTA_MAX_BYTES', 1);`,
		''
	].join('\n'));

	const log_path = path.join(tmp_root, 'php.log');
	const log = fs.openSync(log_path, 'w');

	const proc = Bun.spawn(['php', '-S', `127.0.0.1:${PHP_PORT}`, 'index.php'], {
		cwd: php_root,
		env: { ...process.env, ARCHAVON_ENV: env_path },
		stdout: log,
		stderr: log
	});

	const url = `http://127.0.0.1:${PHP_PORT}`;
	const deadline = Date.now() + BOOT_TIMEOUT_MS;

	const stop = () => {
		proc.kill();

		try {
			fs.closeSync(log);
		} catch {
			// already closed
		}

		try {
			fs.rmSync(tmp_root, { recursive: true, force: true });
		} catch {
			// windows may still hold the sqlite handle; scratch dir, harmless
		}
	};

	for (;;) {
		try {
			const res = await fetch(url + '/test');
			if (res.ok)
				break;
		} catch {
			// not listening yet
		}

		if (proc.exitCode !== null) {
			const output = fs.readFileSync(log_path, 'utf8');
			stop();

			throw new Error(`php -S exited with code ${proc.exitCode}:\n${output}`);
		}

		if (Date.now() > deadline) {
			const output = fs.readFileSync(log_path, 'utf8');
			stop();

			throw new Error(`php -S did not answer /test within ${BOOT_TIMEOUT_MS}ms:\n${output}`);
		}

		await Bun.sleep(250);
	}

	return { url, served, log_path, stop };
}

async function get_json(base: string, route: string): Promise<any> {
	const res = await fetch(base + route);

	if (!res.ok)
		throw new Error(`${route} -> HTTP ${res.status}`);

	return await res.json();
}

async function main(): Promise<void> {
	const argv = process.argv.slice(2);
	let db_path = DEFAULT_DB;

	for (let i = 0; i < argv.length; i++) {
		if (argv[i] === '--db')
			db_path = argv[++i];
		else
			throw new Error('unknown argument: ' + argv[i]);
	}

	const php_root = process.env.ARCHAVON_PHP_ROOT ?? DEFAULT_PHP_ROOT;

	if (!fs.existsSync(path.join(php_root, 'index.php')))
		throw new Error('archavon php root not found: ' + php_root + ' (set ARCHAVON_PHP_ROOT)');

	if (!fs.existsSync(db_path))
		throw new Error('sqlite db not found: ' + db_path);

	console.log(`serving ${path.resolve(db_path)} through ${php_root} on :${PHP_PORT}`);

	const conn = await connect_mysql();
	const server = await spawn_php(php_root, db_path);

	try {
		console.log('\n== stats ==');
		const stats = await get_json(server.url, '/api/v1/stats');

		for (const [table, key] of [
			['cache_creatures', 'creatures'],
			['cache_quests', 'quests'],
			['cache_gameobjects', 'gameobjects'],
			['cache_pagetext', 'pagetext'],
			['hotfix_entries', 'hotfixes'],
			['cache_submissions', 'submissions']
		] as [string, string][]) {
			const [rows]: any = await conn.query(`SELECT COUNT(*) AS c FROM \`${table}\``);
			const my = Number(rows[0].c);
			const api = Number(stats.counts[key]);

			check_behind(my, api, ('counts.' + key).padEnd(28), `mysql ${my.toLocaleString()}  api ${api.toLocaleString()}`);
		}

		const [products]: any = await conn.query(`
			SELECT DISTINCT product FROM cache_creatures
			UNION SELECT DISTINCT product FROM cache_quests
			UNION SELECT DISTINCT product FROM cache_gameobjects
			UNION SELECT DISTINCT product FROM cache_pagetext
			UNION SELECT DISTINCT product FROM hotfix_entries`);

		const my_products = products.map((p: any) => p.product).sort();
		check(JSON.stringify(my_products) === JSON.stringify(stats.products), 'products'.padEnd(28), stats.products.join(', '));

		console.log('\n== entity detail (consensus rows, field-by-field) ==');

		for (const [table, route] of Object.entries(ENTITY_ROUTES)) {
			const [pick]: any = await conn.query(
				`SELECT record_id FROM \`${table}\` WHERE is_consensus = 1 ORDER BY entry_id DESC LIMIT 1`);

			if (pick.length === 0) {
				check(true, route.padEnd(28), 'no consensus rows');
				continue;
			}

			const record_id = Number(pick[0].record_id);
			const body = await get_json(server.url, `/api/v1/${route}/${record_id}`);

			const [my_rows]: any = await conn.query(
				`SELECT * FROM \`${table}\` WHERE record_id = ? ORDER BY game_build DESC, locale ASC`, [record_id]);

			check_behind(my_rows.length, body.versions.length, (route + ' version count').padEnd(28),
				`record ${record_id}: mysql ${my_rows.length}  api ${body.versions.length}`);

			const my_row = my_rows[0];
			const api_row = body.versions.find((v: any) => Number(v.entry_id) === Number(my_row.entry_id)) ?? body.versions[0];

			const [meta]: any = await conn.query(`
				SELECT COLUMN_NAME AS name, COLUMN_TYPE AS column_type
				FROM information_schema.COLUMNS
				WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`, [table]);

			const unsigned_64 = new Set(meta
				.filter((c: any) => String(c.column_type).toLowerCase() === 'bigint unsigned')
				.map((c: any) => c.name));

			let mismatches: string[] = [];

			for (const key of Object.keys(my_row)) {
				if (!(key in api_row) || SKIP_COLUMNS.has(key))
					continue;

				let left = my_row[key];
				const right = api_row[key];

				if (left === null && right === null)
					continue;

				if (unsigned_64.has(key) && left !== null) {
					const raw = BigInt(left);
					left = (raw >= U64_SIGN_BIT ? raw - U64_MODULUS : raw).toString();
				}

				if (String(left) !== String(right) && Number(left) !== Number(right))
					mismatches.push(`${key}: mysql ${JSON.stringify(String(left))} api ${JSON.stringify(String(right))}`);
			}

			check(mismatches.length === 0, (route + ' field equality').padEnd(28),
				`record ${record_id}, ${Object.keys(my_row).length} cols, ${mismatches.length} mismatch`);

			for (const m of mismatches.slice(0, 5))
				console.log('        ' + m);
		}

		console.log('\n== quest junction data ==');
		const [quest]: any = await conn.query(`
			SELECT q.record_id, q.entry_id, COUNT(o.objective_index) AS objectives
			FROM cache_quests q JOIN cache_quest_objectives o ON o.quest_entry_id = q.entry_id
			WHERE q.is_consensus = 1
			GROUP BY q.record_id, q.entry_id ORDER BY objectives DESC LIMIT 1`);

		if (quest.length > 0) {
			const body = await get_json(server.url, `/api/v1/quests/${Number(quest[0].record_id)}`);
			const version = body.versions.find((v: any) => Number(v.entry_id) === Number(quest[0].entry_id));
			const objectives = version?.objectives?.length ?? 0;

			check_behind(Number(quest[0].objectives), objectives, 'objectives attached'.padEnd(28),
				`record ${quest[0].record_id}: mysql ${quest[0].objectives}  api ${objectives}`);
		}

		console.log('\n== list, export and other endpoints ==');

		for (const route of Object.values(ENTITY_ROUTES)) {
			const list = await get_json(server.url, `/api/v1/${route}?per_page=5`);
			check(Array.isArray(list.data) && list.data.length > 0 && list.pagination.total > 0,
				(route + ' list').padEnd(28), `${list.data.length} of ${Number(list.pagination.total).toLocaleString()}`);

			const exported = await get_json(server.url, `/api/v1/${route}/export?per_page=5`);
			check(Array.isArray(exported.data), (route + ' export').padEnd(28), `${exported.data.length} row(s)`);
		}

		const hotfixes = await get_json(server.url, '/api/v1/hotfixes?per_page=5');
		check(Array.isArray(hotfixes.data) && hotfixes.data.length > 0, 'hotfixes'.padEnd(28), `${Number(hotfixes.pagination.total).toLocaleString()} total`);

		const tables = await get_json(server.url, '/api/v1/tables');
		const [table_count]: any = await conn.query('SELECT COUNT(*) AS c FROM db2_table_hashes');
		const table_rows = Array.isArray(tables) ? tables.length : (tables.data?.length ?? 0);
		check(table_rows > 0, 'tables'.padEnd(28), `api ${table_rows}, mysql ${table_count[0].c}`);

		const submissions = await get_json(server.url, '/api/v1/submissions?per_page=5');
		check(Array.isArray(submissions.data) && submissions.data.length > 0, 'submissions list'.padEnd(28),
			`${Number(submissions.pagination.total).toLocaleString()} total`);

		const detail = await get_json(server.url, `/api/v1/submissions/${submissions.data[0].submission_id}`);
		check(detail.submission_id === submissions.data[0].submission_id || detail.submission?.submission_id === submissions.data[0].submission_id,
			'submission detail'.padEnd(28), submissions.data[0].submission_id);

		const binary = await get_json(server.url, '/api/v1/binaryhashes?per_page=5');
		check(Array.isArray(binary.data), 'binaryhashes'.padEnd(28), `${binary.data.length} row(s)`);

		const sync = await get_json(server.url, '/api/v1/sync');
		check(sync !== null, 'sync'.padEnd(28), JSON.stringify(sync).slice(0, 80));
	} catch (e) {
		// php -S writes fatals to its log, not the response body, so a bare HTTP 500 is
		// useless without it
		try {
			const output = fs.readFileSync(server.log_path, 'utf8');
			const errors = output.split('\n').filter(l => !/Accepted$|Closing$|Development Server/.test(l)).join('\n').trim();

			if (errors.length > 0)
				console.log('\nphp log:\n' + errors.slice(-4000));
		} catch {
			// log already gone
		}

		throw e;
	} finally {
		server.stop();
		await conn.end();
	}

	console.log(`\n${fail_count === 0 ? 'READ API CHECK PASSED' : 'READ API CHECK FAILED'}: ${pass_count} passed, ${warn_count} warned, ${fail_count} failed`);
	process.exit(fail_count === 0 ? 0 : 1);
}

await main();
