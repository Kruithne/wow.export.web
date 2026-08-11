import { SQL } from 'bun';
import * as spooder from 'spooder';

// the last MySQL consumer on the VPS: trigger_dbd_update in module.ts syncs the
// WoWDBDefs manifest into db2_table_hashes. db2_table_hashes only travels inside a
// delta, and a delta needs a finalized submission, so a periodic manifest sync has
// no path through the archavon write API yet. Everything else -- intake, the cache
// worker, cleanup -- runs on archavon_api.ts.
const POOL_MAX = 2;

if (process.env.DB_URI_ARCHAVON === undefined)
	spooder.panic('process.env.DB_URI_ARCHAVON not configured');

export const db_archavon = new SQL(process.env.DB_URI_ARCHAVON as string, { max: POOL_MAX });
