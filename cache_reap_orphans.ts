import { bucket } from './obj_rds';
import { db_archavon } from './db_archavon';

// reaps cdn objects in the wow.export.cache bucket that have no corresponding
// cache_submission_files row. dry run unless --delete is passed.

const PAGE_SIZE = 1000;
const DELETE_MODE = process.argv.includes('--delete');

if (process.env.CACHE_CDN_SECRET === undefined)
	throw new Error('process.env.CACHE_CDN_SECRET not configured');

const cache_bucket = bucket('wow.export.cache', process.env.CACHE_CDN_SECRET);

const known = new Set<string>();
for (const row of await db_archavon`SELECT object_id FROM cache_submission_files WHERE object_id IS NOT NULL`)
	known.add(row.object_id);

console.log(`db references ${known.size.toLocaleString()} objects`);

const orphans: Array<{ object_id: string, filename: string, size: number }> = [];
let scanned = 0;
let offset = 0;

while (true) {
	const page = await cache_bucket.list(offset, PAGE_SIZE);
	if (page === null)
		throw new Error(`failed to list bucket objects at offset ${offset}`);

	if (page.objects.length === 0)
		break;

	for (const obj of page.objects) {
		scanned++;

		if (!known.has(obj.object_id))
			orphans.push({ object_id: obj.object_id, filename: obj.filename, size: obj.size });
	}

	offset += page.objects.length;
	console.log(`scanned ${scanned.toLocaleString()} objects, ${orphans.length.toLocaleString()} unreferenced`);
}

const total = orphans.reduce((sum, o) => sum + o.size, 0);
console.log(`\n${orphans.length.toLocaleString()} unreferenced objects, ${(total / 1073741824).toFixed(2)} GB`);

for (const o of orphans.slice(0, 20))
	console.log(`  ${o.object_id}  ${o.filename}  ${(o.size / 1048576).toFixed(1)}MB`);

if (orphans.length > 20)
	console.log(`  ... and ${(orphans.length - 20).toLocaleString()} more`);

if (!DELETE_MODE) {
	console.log('\ndry run, pass --delete to remove these objects');
	process.exit(0);
}

let deleted = 0;
let failed = 0;

for (const o of orphans) {
	if (await cache_bucket.delete(o.object_id))
		deleted++;
	else
		failed++;

	if ((deleted + failed) % 100 === 0)
		console.log(`deleted ${deleted.toLocaleString()}, failed ${failed.toLocaleString()}`);
}

console.log(`\ndeleted ${deleted.toLocaleString()} objects (${failed.toLocaleString()} failed)`);
