import { bucket } from './obj_rds';
import { archavon_api, MAX_OBJECT_IDS_PER_REQUEST } from './archavon_api';

// deletes CDN objects still held by long-failed submissions and marks the file rows
// purged. Submission rows are retained as history. Dry run unless --delete is passed.
//
// this is the manual bulk form of cache_reap_failed in module.ts, which does the same
// thing hourly with a small limit.
//
// usage: bun run wow.export/cache_purge_failed.ts [--min-age-days=30] [--limit=1000] [--delete]

const DELETE_MODE = process.argv.includes('--delete');
const DEFAULT_MIN_AGE_DAYS = 30;
const DEFAULT_LIMIT = 1000;

if (process.env.CACHE_CDN_SECRET === undefined)
	throw new Error('process.env.CACHE_CDN_SECRET not configured');

const cache_bucket = bucket('wow.export.cache', process.env.CACHE_CDN_SECRET);
const archavon = archavon_api();

function num_arg(name: string, fallback: number): number {
	const raw = process.argv.find(a => a.startsWith(`--${name}=`));
	if (raw === undefined)
		return fallback;

	const value = Number(raw.slice(name.length + 3));
	if (!Number.isInteger(value) || value < 1)
		throw new Error(`--${name} must be a positive integer`);

	return value;
}

const min_age_days = num_arg('min-age-days', DEFAULT_MIN_AGE_DAYS);
const limit = num_arg('limit', DEFAULT_LIMIT);

const { files } = await archavon.list_reapable_files(min_age_days, limit);

console.log(`failed submissions older than ${min_age_days} days (limit ${limit})`);
console.log(`${files.length.toLocaleString()} objects`);

if (files.length === 0)
	process.exit(0);

if (!DELETE_MODE) {
	for (const file of files.slice(0, 20))
		console.log(`  ${file.object_id}  ${file.locale}/${file.file_name}`);

	if (files.length > 20)
		console.log(`  ... and ${(files.length - 20).toLocaleString()} more`);

	console.log('\ndry run, pass --delete to remove these objects');
	process.exit(0);
}

let deleted = 0;
let failed = 0;
let purged: string[] = [];

async function mark_purged() {
	if (purged.length === 0)
		return;

	const batch = purged;
	purged = [];

	const result = await archavon.purge_objects(batch);
	console.log(`deleted ${deleted.toLocaleString()}, failed ${failed.toLocaleString()}, marked ${result.purged}/${result.requested}`);
}

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

	if (purged.length >= MAX_OBJECT_IDS_PER_REQUEST)
		await mark_purged();
}

await mark_purged();

console.log(`\ndeleted ${deleted.toLocaleString()} objects (${failed.toLocaleString()} failed)`);
