import { caution } from 'spooder';
import { db_archavon } from './db_archavon';
import { bucket } from './obj_rds';
import { parse_wdb } from './wdb';
import { parse_dbcache } from './dbcache';
import { upsert_machine, store_creatures, store_quests, store_gameobjects, store_pagetext } from './wdb_store';

const cache_bucket = bucket('wow.export.cache', process.env.CACHE_CDN_SECRET!);

const WDB_STORE_MAP: Record<string, typeof store_creatures> = {
	'WMOB': store_creatures,
	'WQST': store_quests,
	'WGOB': store_gameobjects,
	'WPTX': store_pagetext
};

const WDB_MAGIC_KEYS = new Set(Object.keys(WDB_STORE_MAP));
const XFTH_MAGIC = 0x48544658;

const queue: string[] = [];
let processing = false;

async function reject_file(file: { object_id: string; submission_id?: string }) {
	try {
		await cache_bucket.delete(file.object_id);
	} catch (e) {
		log(`failed to delete rejected CDN object {${file.object_id}}: ${(e as Error).message}`);
	}

	try {
		await db_archavon`DELETE FROM cache_submission_files WHERE object_id = ${file.object_id}`;
	} catch (e) {
		log(`failed to delete rejected file row {${file.object_id}}: ${(e as Error).message}`);
	}
}

declare var self: Worker;

function log(text: string) {
	self.postMessage({ type: 'log', text });
}

self.onmessage = (event: MessageEvent) => {
	const { submission_id } = event.data;
	queue.push(submission_id);

	if (!processing)
		process_queue();
};

async function process_queue() {
	processing = true;

	while (queue.length > 0) {
		const submission_id = queue.shift()!;
		log(`processing {${submission_id}} (${queue.length} remaining)`);

		try {
			await process_submission(submission_id);
		} catch (e) {
			caution('cache: failed to process submission', { submission_id, error: e });
		}
	}

	processing = false;
}

async function process_submission(submission_id: string) {
	const [submission] = await db_archavon`
		SELECT build_number, machine_id, patch, product
		FROM cache_submissions
		WHERE submission_id = ${submission_id}
	`;

	if (!submission) {
		log(`submission {${submission_id}} not found, skipping`);
		return;
	}

	const build_number = submission.build_number as number;
	const machine_id = submission.machine_id as string;
	const patch = submission.patch as string;
	const product = submission.product as string;

	log(`submission {${submission_id}} ${product} ${patch}.${build_number} (machine: ${machine_id})`);

	await upsert_machine(db_archavon, machine_id);

	const files = await db_archavon`
		SELECT file_name, locale, object_id
		FROM cache_submission_files
		WHERE submission_id = ${submission_id}
	`;

	let processed = 0;
	let failed = 0;

	for (const file of files) {
		try {
			const res = await cache_bucket.download(file.object_id);
			const data = await res.arrayBuffer();

			// magic validation before parsing
			if (data.byteLength < 4) {
				log(`file {${file.object_id}}: too small (${data.byteLength} bytes), rejecting`);
				await reject_file(file);
				continue;
			}

			const magic_view = new DataView(data);

			if (file.file_name.endsWith('.wdb')) {
				// wdb magic: 4 bytes reversed as ASCII, must match WDB_STORE_MAP keys
				const magic_bytes = new Uint8Array(data, 0, 4);
				const wdb_sig = String.fromCharCode(magic_bytes[3], magic_bytes[2], magic_bytes[1], magic_bytes[0]);

				if (!WDB_MAGIC_KEYS.has(wdb_sig)) {
					log(`wdb {${file.locale}/${file.file_name}}: invalid magic "${wdb_sig}", rejecting`);
					await reject_file(file);
					continue;
				}

				const result = parse_wdb(data, patch);
				if (result) {
					const valid_records = result.records.filter(r => !('parse_error' in r.data));
					const parse_errors = result.records.length - valid_records.length;
					const sig = result.header.signature;
					const store_fn = WDB_STORE_MAP[sig];
					if (store_fn) {
						const stored = await store_fn(db_archavon, valid_records, file.locale, product, build_number, machine_id, submission_id);
						log(`wdb {${file.locale}/${file.file_name}}: ${result.records.length} records, stored ${stored}, ${parse_errors} parse errors (${sig})`);
					} else {
						log(`wdb {${file.locale}/${file.file_name}}: unknown signature ${sig}, ${result.records.length} records skipped`);
					}
				} else {
					log(`wdb {${file.locale}/${file.file_name}}: failed to parse (${data.byteLength} bytes), retaining file`);
				}
			} else if (file.file_name.toLowerCase() === 'dbcache.bin') {
				const dbcache_magic = magic_view.getUint32(0, true);

				if (dbcache_magic !== XFTH_MAGIC) {
					log(`dbcache {${file.locale}/${file.file_name}}: invalid magic 0x${(dbcache_magic >>> 0).toString(16)}, rejecting`);
					await reject_file(file);
					continue;
				}

				const result = parse_dbcache(data);
				if (result) {
					log(`dbcache {${file.locale}/${file.file_name}}: ${result.entries.length} entries, build=${result.header.build}, version=${result.header.version}`);

					const BATCH_SIZE = 500;
					let inserted = 0;

					for (let i = 0; i < result.entries.length; i += BATCH_SIZE) {
						const batch = result.entries.slice(i, i + BATCH_SIZE);
						const placeholders: string[] = [];
						const params: any[] = [];

						for (const entry of batch) {
							placeholders.push('(?, ?, ?, ?, ?, ?, ?, ?, ?)');
							params.push(
								entry.table_hash >>> 0,
								entry.record_id >>> 0,
								entry.push_id >>> 0,
								entry.unique_id >>> 0,
								entry.region_id >>> 0,
								entry.status,
								build_number,
								entry.record_data ? Buffer.from(entry.record_data) : null,
								product
							);
						}

						await db_archavon.unsafe(
							`INSERT INTO hotfix_entries (table_hash, record_id, push_id, unique_id, region_id, status, game_build, data_blob, product) VALUES ${placeholders.join(',')} ON DUPLICATE KEY UPDATE unique_id = IF(VALUES(data_blob) IS NOT NULL AND data_blob IS NULL, VALUES(unique_id), unique_id), region_id = IF(VALUES(data_blob) IS NOT NULL AND data_blob IS NULL, VALUES(region_id), region_id), status = IF(VALUES(data_blob) IS NOT NULL AND data_blob IS NULL, VALUES(status), status), game_build = IF(VALUES(data_blob) IS NOT NULL AND data_blob IS NULL, VALUES(game_build), game_build), data_blob = IF(VALUES(data_blob) IS NOT NULL AND data_blob IS NULL, VALUES(data_blob), data_blob)`,
							params
						);

						inserted += batch.length;
					}

					log(`dbcache {${file.locale}/${file.file_name}}: stored {${inserted}} hotfix entries`);
				} else {
					log(`dbcache {${file.locale}/${file.file_name}}: failed to parse (${data.byteLength} bytes), retaining file`);
				}
			}

			processed++;
		} catch (e) {
			log(`failed to download {${file.object_id}}: ${(e as Error).message}`);
			failed++;
		}
	}

	await db_archavon`UPDATE cache_submissions SET processed_at = NOW() WHERE submission_id = ${submission_id}`;

	log(`submission {${submission_id}} done: ${processed} processed, ${failed} failed`);
}
