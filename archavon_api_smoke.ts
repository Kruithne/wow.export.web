/*!
	archavon write API smoke script

	Exercises every call in archavon_api.ts against a local archavon PHP instance.

	Self-hosted (default): stands up `php -S` against a throwaway sqlite db built from the
	app's db/schema.sql, exactly as tools/test_write_intake.php does. Requires `php` on PATH
	and a checkout of the archavon app.

		bun run wow.export/archavon_api_smoke.ts
		ARCHAVON_PHP_ROOT=F:/archavon bun run wow.export/archavon_api_smoke.ts

	Against an already-running instance, set both and nothing is spawned:

		ARCHAVON_WRITE_URL=http://127.0.0.1:8080 ARCHAVON_WRITE_SECRET=... bun run wow.export/archavon_api_smoke.ts

	The instance must be configured with DB_BACKEND=sqlite and a matching
	ARCHAVON_WRITE_SECRET in its env.php. This script writes rows; never point it at
	production.
 */

import { Database } from 'bun:sqlite';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { archavon_api, ArchavonApiError } from './archavon_api';

const PHP_ROOT = process.env.ARCHAVON_PHP_ROOT ?? 'F:/archavon';
const PHP_PORT = 8793;
const SECRET = 'smoke-write-secret';

const MACHINE_ID = '11111111-2222-3333-4444-555555555555';
const BUILD_KEY = 'aabbccddeeff00112233445566778899';
const SUBMISSION_ID = 'sub-smoke-1';

const FILE_KEEP = { file_name: 'creaturecache.wdb', locale: 'enUS' };
const FILE_DROP = { file_name: 'questcache.wdb', locale: 'deDE' };

let checks = 0;
let failures = 0;

function check(condition: boolean, label: string, detail = '') {
	checks++;

	if (condition) {
		console.log(`  ok   ${label}`);
	} else {
		failures++;
		console.log(`  FAIL ${label}${detail ? `: ${detail}` : ''}`);
	}
}

function section(title: string) {
	console.log(`\n${title}`);
}

function build_delta(submission_id = SUBMISSION_ID): Uint8Array {
	const schema = fs.readFileSync(path.join(PHP_ROOT, 'db/delta_schema.sql'), 'utf8');
	const db = new Database(':memory:');
	db.exec(schema);

	db.run('INSERT INTO delta_meta (key, value) VALUES (?, ?)', ['delta_version', '1']);
	db.run('INSERT INTO delta_meta (key, value) VALUES (?, ?)', ['submission_id', submission_id]);
	db.run('INSERT INTO delta_meta (key, value) VALUES (?, ?)', ['machine_id', MACHINE_ID]);

	db.run(
		'INSERT INTO delta_submission_files (file_name, locale, status, records_added) VALUES (?, ?, ?, ?)',
		[FILE_KEEP.file_name, FILE_KEEP.locale, 'completed', 1]
	);

	db.run(
		'INSERT INTO cache_creatures (entry_id, record_id, locale, content_hash, game_build, title) VALUES (?, ?, ?, ?, ?, ?)',
		[1, 4242, 'enUS', 'a'.repeat(64), 60000, 'Smoke Test Creature']
	);

	const data = db.serialize();
	db.close();

	return data;
}

async function wait_for_port(port: number, timeout_ms: number): Promise<boolean> {
	const deadline = Date.now() + timeout_ms;

	while (Date.now() < deadline) {
		try {
			await fetch(`http://127.0.0.1:${port}/api/v1/intake/machine`, { method: 'POST' });
			return true;
		} catch {
			await new Promise(resolve => setTimeout(resolve, 100));
		}
	}

	return false;
}

async function spawn_php(): Promise<{ url: string; db_path: string; stop: () => void }> {
	const tmp_root = fs.mkdtempSync(path.join(os.tmpdir(), 'archavon_smoke_'));
	const main_path = path.join(tmp_root, 'main.db').replace(/\\/g, '/');

	console.log(`temp: ${tmp_root}`);

	const db = new Database(main_path);
	db.exec('PRAGMA journal_mode = DELETE');
	db.exec(fs.readFileSync(path.join(PHP_ROOT, 'db/schema.sql'), 'utf8'));
	db.close();

	const env_path = path.join(tmp_root, 'env.php');
	fs.writeFileSync(env_path, [
		'<?php',
		"\tdefine('GITHUB_SECRET', 'unused');",
		"\tdefine('DB_BACKEND', 'sqlite');",
		`\tdefine('SQLITE_PATH', '${main_path}');`,
		"\tdefine('SQLITE_JOURNAL_MODE', 'DELETE');",
		`\tdefine('ARCHAVON_WRITE_SECRET', '${SECRET}');`,
		`\tdefine('DELTA_TMP_DIR', '${path.join(tmp_root, 'tmp').replace(/\\/g, '/')}');`,
		''
	].join('\n'));

	fs.mkdirSync(path.join(tmp_root, 'tmp'), { recursive: true });

	const log = path.join(tmp_root, 'server.log');
	const proc = Bun.spawn(['php', '-S', `127.0.0.1:${PHP_PORT}`, 'index.php'], {
		cwd: PHP_ROOT,
		env: { ...process.env, ARCHAVON_ENV: env_path },
		stdout: Bun.file(log),
		stderr: Bun.file(log)
	});

	if (!await wait_for_port(PHP_PORT, 10000)) {
		proc.kill();
		throw new Error(`php -S did not come up; see ${log}`);
	}

	return {
		url: `http://127.0.0.1:${PHP_PORT}`,
		db_path: main_path,
		stop: () => proc.kill()
	};
}

async function run(base_url: string, secret: string, db_path?: string) {
	const api = archavon_api({ base_url, secret });

	section('intake/machine');
	const machine = await api.check_machine(MACHINE_ID, 'hw-hash-1');
	check(machine.machine_id === MACHINE_ID, 'machine upserted');
	check(machine.blocked === false, 'machine not blocked');

	section('intake/hashes');
	const stored = await api.store_binary_hashes(BUILD_KEY, [
		{ file_name: 'Wow.exe', content_hash: 'b'.repeat(64), file_size: 100 },
		{ file_name: 'Wow.exe', content_hash: 'c'.repeat(64), file_size: 101 }
	]);
	check(stored.inserted === 2, 'two hashes inserted', JSON.stringify(stored));

	const restored = await api.store_binary_hashes(BUILD_KEY, [
		{ file_name: 'Wow.exe', content_hash: 'b'.repeat(64), file_size: 100 }
	]);
	check(restored.inserted === 0, 're-store is a no-op', JSON.stringify(restored));

	const lookup = await api.get_binary_hashes(BUILD_KEY, ['Wow.exe']);
	check(lookup.known === true, 'build known');
	check(lookup.hashes['Wow.exe']?.length === 2, 'both hashes returned for Wow.exe');

	const unknown = await api.get_binary_hashes('f'.repeat(32));
	check(unknown.known === false, 'unknown build reports known: false');

	section('intake/submission');
	const created = await api.create_submission({
		submission_id: SUBMISSION_ID,
		machine_id: MACHINE_ID,
		product: 'wow',
		patch: '11.0.0',
		build_number: 60000,
		build_key: BUILD_KEY,
		cdn_key: 'ccdd',
		binary_hash: { 'Wow.exe': 'b'.repeat(64) },
		client_ip: 'd'.repeat(64),
		files: [
			{ ...FILE_KEEP, file_size: 2048, object_id: 'obj-keep', modified_at: '2026-08-10T12:00:00Z' },
			{ ...FILE_DROP, file_size: 1024, object_id: 'obj-drop' }
		]
	});
	check(created.status === 'pending', 'submission created as pending');
	check(created.files === 2, 'two file rows created');

	let duplicate_conflict = false;
	try {
		await api.create_submission({
			submission_id: SUBMISSION_ID,
			machine_id: MACHINE_ID,
			product: 'wow',
			patch: '11.0.0',
			build_number: 60000,
			build_key: BUILD_KEY,
			cdn_key: 'ccdd',
			binary_hash: { 'Wow.exe': 'b'.repeat(64) },
			client_ip: 'd'.repeat(64),
			files: [{ ...FILE_KEEP, file_size: 2048, object_id: 'obj-keep' }]
		});
	} catch (error) {
		duplicate_conflict = error instanceof ArchavonApiError && error.status === 409;
	}
	check(duplicate_conflict, 'duplicate submission is 409');

	section('intake/submission/files');
	const detail = await api.get_submission(SUBMISSION_ID);
	check(detail.files.length === 2, 'lookup returns both files');
	check(detail.build_key === BUILD_KEY, 'lookup returns build_key');

	let unknown_submission = false;
	try {
		await api.get_submission('sub-does-not-exist');
	} catch (error) {
		unknown_submission = error instanceof ArchavonApiError && error.status === 404;
	}
	check(unknown_submission, 'unknown submission is 404');

	section('intake/submission/finalize');
	const finalized = await api.finalize_submission(SUBMISSION_ID, [FILE_KEEP]);
	check(finalized.status === 'finalized', 'submission finalized');
	check(finalized.kept.length === 1 && finalized.kept[0].object_id === 'obj-keep', 'kept file returned');
	check(finalized.removed.length === 1 && finalized.removed[0].object_id === 'obj-drop', 'removed file returned');

	section('intake/submission/status');
	const processing = await api.update_submission_status({ submission_id: SUBMISSION_ID, status: 'processing' });
	check(processing.status === 'processing', 'flipped to processing');

	const per_file = await api.update_submission_status({
		submission_id: SUBMISSION_ID,
		files: [{ object_id: 'obj-keep', status: 'completed', records_added: 1 }]
	});
	check(per_file.files_updated === 1, 'per-file status written by object_id');

	const stale_file = await api.update_submission_status({
		submission_id: SUBMISSION_ID,
		files: [{ file_name: 'gone.wdb', locale: 'enUS', status: 'completed' }]
	});
	check(stale_file.unknown_files === 1, 'unknown file counted, not fatal');

	section('delta');
	const delta = build_delta();
	console.log(`  delta: ${delta.byteLength} bytes`);

	const applied = await api.upload_delta(SUBMISSION_ID, delta);
	check(applied.already_applied === false, 'delta applied');

	if (applied.already_applied === false) {
		check(applied.entities.cache_creatures === 1, 'one creature row written', JSON.stringify(applied.entities));
		check(applied.attestations === 1, 'one attestation written');
		check(applied.files_updated === 1, 'file outcome written');
	}

	const reapplied = await api.upload_delta(SUBMISSION_ID, delta);
	check(reapplied.already_applied === true, 're-upload short-circuits via the ledger');

	let delta_unknown = false;
	try {
		await api.upload_delta('sub-does-not-exist', build_delta('sub-does-not-exist'), { retry_count: 0 });
	} catch (error) {
		delta_unknown = error instanceof ArchavonApiError && error.status === 404;
	}
	check(delta_unknown, 'delta for unknown submission is 404');

	let delta_mismatch = false;
	try {
		await api.upload_delta('sub-smoke-mismatch', delta, { retry_count: 0 });
	} catch (error) {
		delta_mismatch = error instanceof ArchavonApiError && error.status === 400;
	}
	check(delta_mismatch, 'delta_meta submission_id mismatch is 400');

	section('intake/cleanup/stale');
	const stale_id = 'sub-smoke-stale';
	await api.create_submission({
		submission_id: stale_id,
		machine_id: MACHINE_ID,
		product: 'wow',
		patch: '11.0.0',
		build_number: 60000,
		build_key: BUILD_KEY,
		cdn_key: 'ccdd',
		binary_hash: { 'Wow.exe': 'b'.repeat(64) },
		client_ip: 'd'.repeat(64),
		files: [{ file_name: 'stale.wdb', locale: 'enUS', file_size: 16, object_id: 'obj-stale' }]
	});

	// min age is 1 hour server-side, so the row has to be backdated to show up
	if (db_path) {
		const db = new Database(db_path);
		db.run("UPDATE cache_submissions SET submitted_at = datetime('now', '-2 hours') WHERE submission_id = ?", [stale_id]);
		db.close();
	}

	const stale = await api.list_stale_submissions(1, 10);
	check(stale.max_age_hours === 1, 'stale listing echoes max_age_hours');
	check(stale.submissions.every(s => s.submission_id !== SUBMISSION_ID), 'finalized submission not listed');

	if (db_path) {
		const listed = stale.submissions.find(s => s.submission_id === stale_id);
		check(listed !== undefined, 'backdated stale submission listed');
		check(listed?.files[0]?.object_id === 'obj-stale', 'stale listing carries object_ids');
	} else {
		console.log('  skip backdated stale listing (external target)');
	}

	section('intake/submission/delete');
	const guarded = await api.delete_submissions([SUBMISSION_ID], true);
	check(guarded.deleted === 0, 'unfinalized_only spares a finalized submission', JSON.stringify(guarded));

	const deleted = await api.delete_submissions([stale_id, 'sub-does-not-exist']);
	check(deleted.requested === 2 && deleted.deleted === 1, 'delete reports requested vs deleted', JSON.stringify(deleted));

	section('auth');
	const bad_api = archavon_api({ base_url, secret: secret + 'x', retry_count: 0 });
	let rejected = false;
	try {
		await bad_api.check_machine(MACHINE_ID);
	} catch (error) {
		rejected = error instanceof ArchavonApiError && error.status === 403;
	}
	check(rejected, 'bad secret is 403');
}

const external_url = process.env.ARCHAVON_WRITE_URL;
let server: { url: string; db_path: string; stop: () => void } | null = null;

try {
	let base_url: string;
	let secret: string;

	if (external_url) {
		base_url = external_url;
		secret = process.env.ARCHAVON_WRITE_SECRET ?? '';

		if (secret === '')
			throw new Error('ARCHAVON_WRITE_SECRET not set');

		console.log(`target: ${base_url} (external)`);
	} else {
		server = await spawn_php();
		base_url = server.url;
		secret = SECRET;
		console.log(`target: ${base_url} (spawned)`);
	}

	await run(base_url, secret, server?.db_path);
} finally {
	server?.stop();
}

console.log(`\n${checks - failures}/${checks} checks passed`);
process.exit(failures === 0 ? 0 : 1);
