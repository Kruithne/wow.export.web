import { SQL } from 'bun';
import { isMainThread } from 'node:worker_threads';
import * as spooder from 'spooder';

// db_archavon is capped at max_user_connections 100, shared with the public
// read api on shared hosting. bun defaults to a pool of 10 per SQL instance,
// which is far too greedy for a per-submission worker.
const POOL_MAX_MAIN = 5;
const POOL_MAX_WORKER = 2;

if (process.env.DB_URI_ARCHAVON === undefined)
	spooder.panic('process.env.DB_URI_ARCHAVON not configured');

export const db_archavon = new SQL(process.env.DB_URI_ARCHAVON as string, {
	max: isMainThread ? POOL_MAX_MAIN : POOL_MAX_WORKER
});

// migrations run once from the main thread; cache_worker.ts imports this module
// on every spawn and must not re-apply revisions per submission
export async function db_archavon_migrate() {
	await spooder.db_schema(db_archavon, './wow.export/db_archavon/revisions', { recursive: false });
}
