import { bucket } from './obj_rds';
import { db_archavon } from './db_archavon';

// deletes CDN objects belonging to failed submissions in a date range and
// marks the file rows purged. submission rows are retained as history.
// dry run unless --delete is passed.
//
// usage: bun run wow.export/cache_purge_failed.ts --from=2026-04-01 --to=2026-07-01 [--delete]

const DELETE_MODE = process.argv.includes('--delete');
const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const BATCH_SIZE = 500;

if (process.env.CACHE_CDN_SECRET === undefined)
	throw new Error('process.env.CACHE_CDN_SECRET not configured');

const cache_bucket = bucket('wow.export.cache', process.env.CACHE_CDN_SECRET);

function arg(name: string): string {
	const raw = process.argv.find(a => a.startsWith(`--${name}=`));
	if (raw === undefined)
		throw new Error(`missing --${name}=YYYY-MM-DD`);

	const value = raw.slice(name.length + 3);
	if (!DATE_PATTERN.test(value))
		throw new Error(`--${name} must be YYYY-MM-DD, got "${value}"`);

	return value;
}

const from = arg('from');
const to = arg('to');

const files = await db_archavon`
	SELECT f.object_id, f.file_size
	FROM cache_submissions s
	JOIN cache_submission_files f ON f.submission_id = s.submission_id
	WHERE s.status = 'failed' AND f.status = 'pending'
	AND s.submitted_at >= ${from} AND s.submitted_at < ${to}
`;

const total = files.reduce((sum: number, f: { file_size: number }) => sum + Number(f.file_size), 0);

console.log(`${from} .. ${to}`);
console.log(`${files.length.toLocaleString()} objects, ${(total / 1073741824).toFixed(2)} GB`);

if (files.length === 0)
	process.exit(0);

if (!DELETE_MODE) {
	console.log('\ndry run, pass --delete to remove these objects');
	process.exit(0);
}

let deleted = 0;
let failed = 0;
const purged: string[] = [];

for (const file of files) {
	let ok = false;
	try {
		ok = await cache_bucket.delete(file.object_id);
	} catch (e) {
		console.log(`error deleting ${file.object_id}: ${(e as Error).message}`);
	}

	if (ok) {
		deleted++;
		purged.push(file.object_id);
	} else {
		failed++;
	}

	if (purged.length >= BATCH_SIZE) {
		await mark_purged(purged.splice(0, purged.length));
		console.log(`deleted ${deleted.toLocaleString()}, failed ${failed.toLocaleString()}`);
	}
}

if (purged.length > 0)
	await mark_purged(purged);

console.log(`\ndeleted ${deleted.toLocaleString()} objects (${failed.toLocaleString()} failed)`);

async function mark_purged(object_ids: string[]) {
	await db_archavon`
		UPDATE cache_submission_files
		SET status = 'rejected', failure_reason = 'purged'
		WHERE object_id IN ${db_archavon(object_ids)}
	`;
}
