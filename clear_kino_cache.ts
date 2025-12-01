import { SQL } from 'bun';
import { bucket } from './obj_rds';

if (process.env.DB_URI === undefined)
	throw new Error('process.env.DB_URI not configured');

if (process.env.KINO_CDN_SECRET === undefined)
	throw new Error('process.env.KINO_CDN_SECRET not configured');

const db = new SQL(process.env.DB_URI);
const kino_bucket = bucket('wow.export.kino', process.env.KINO_CDN_SECRET);

console.log('fetching cached entries from database...');
const entries = await db`SELECT enc FROM kino_cached`;
console.log(`found ${entries.length} cached entries`);

if (entries.length === 0) {
	console.log('nothing to clear');
	process.exit(0);
}

let deleted = 0;
let failed = 0;

for (const entry of entries) {
	const enc = entry.enc as string;
	console.log(`deleting ${enc}...`);

	const success = await kino_bucket.delete(enc);
	if (success) {
		deleted++;
		console.log(`  deleted ${enc}`);
	} else {
		failed++;
		console.log(`  failed to delete ${enc}`);
	}
}

console.log(`deleted ${deleted}/${entries.length} objects from CDN (${failed} failed)`);

console.log('clearing database...');
await db`DELETE FROM kino_cached`;
console.log('database cleared');

console.log('done');
