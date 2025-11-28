import { SQL } from 'bun';
import * as spooder from 'spooder';

if (process.env.DB_URI === undefined)
	spooder.panic('process.env.DB_URI not configured');

export const db = new SQL(process.env.DB_URI as string);
await spooder.db_schema(db, './wow.export/db/revisions', { recursive: false });