import { SQL } from 'bun';
import * as spooder from 'spooder';

if (process.env.DB_URI_ARCHAVON === undefined)
	spooder.panic('process.env.DB_URI_ARCHAVON not configured');

export const db_archavon = new SQL(process.env.DB_URI_ARCHAVON as string);
await spooder.db_schema(db_archavon, './wow.export/db_archavon/revisions', { recursive: false });
