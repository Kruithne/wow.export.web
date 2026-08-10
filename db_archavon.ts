import { SQL } from 'bun';
import * as spooder from 'spooder';

if (process.env.DB_URI_ARCHAVON === undefined)
	spooder.panic('process.env.DB_URI_ARCHAVON not configured');

export const db_archavon = new SQL(process.env.DB_URI_ARCHAVON as string);

// migrations run once from the main thread; cache_worker.ts imports this module
// on every spawn and must not re-apply revisions per submission
export async function db_archavon_migrate() {
	await spooder.db_schema(db_archavon, './wow.export/db_archavon/revisions', { recursive: false });
}
