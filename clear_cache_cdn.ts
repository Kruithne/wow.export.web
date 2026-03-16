import { bucket } from './obj_rds';

if (process.env.CACHE_CDN_SECRET === undefined)
	throw new Error('process.env.CACHE_CDN_SECRET not configured');

const cache_bucket = bucket('wow.export.cache', process.env.CACHE_CDN_SECRET);

const stats = await cache_bucket.stat();
if (stats === null)
	throw new Error('failed to fetch bucket stats');

const { files } = stats as { size: number, files: number };
console.log(`bucket has ${files} objects`);

if (files === 0) {
	console.log('nothing to clear');
	process.exit(0);
}

let deleted = 0;
let failed = 0;
let offset = 0;

while (true) {
	const page = await cache_bucket.list(offset);
	if (page === null)
		throw new Error('failed to list bucket objects');

	if (page.objects.length === 0)
		break;

	for (const obj of page.objects) {
		console.log(`deleting ${obj.object_id} (${obj.filename})...`);

		const success = await cache_bucket.delete(obj.object_id);
		if (success) {
			deleted++;
			console.log(`  deleted ${obj.object_id}`);
		} else {
			failed++;
			console.log(`  failed to delete ${obj.object_id}`);
		}
	}

	// don't advance offset since deleted objects shift the list
	if (failed > 0)
		offset += failed;
}

console.log(`deleted ${deleted} objects from CDN (${failed} failed)`);
console.log('done');
