import { caution } from 'spooder';
import { db_archavon } from './db_archavon';
import { bucket } from './obj_rds';
import { parse_wdb } from './wdb';
import { parse_dbcache } from './dbcache';
import { upsert_machine, store_creatures, store_quests, store_gameobjects, store_pagetext } from './wdb_store';

const cache_bucket = bucket('wow.export.cache', process.env.CACHE_CDN_SECRET!);

function measure_rss(label: string): number {
	Bun.gc(true);
	const rss_mb = process.memoryUsage.rss() / 1024 / 1024;
	log(`[mem] ${label}: ${rss_mb.toFixed(2)} MB`);
	return rss_mb;
}

const WDB_STORE_MAP: Record<string, typeof store_creatures> = {
	'WMOB': store_creatures,
	'WQST': store_quests,
	'WGOB': store_gameobjects,
	'WPTX': store_pagetext
};

const WDB_MAGIC_KEYS = new Set(Object.keys(WDB_STORE_MAP));
const XFTH_MAGIC = 0x48544658;

interface QueueNode {
	value: string;
	next: QueueNode | null;
}

let queue_head: QueueNode | null = null;
let queue_tail: QueueNode | null = null;
let queue_size = 0;
let processing = false;

async function reject_file(file: { object_id: string }, reason: string) {
	try {
		await cache_bucket.delete(file.object_id);
	} catch (e) {
		log(`failed to delete rejected CDN object {${file.object_id}}: ${(e as Error).message}`);
	}

	try {
		await db_archavon`UPDATE cache_submission_files SET status = 'rejected', failure_reason = ${reason} WHERE object_id = ${file.object_id}`;
	} catch (e) {
		log(`failed to update rejected file row {${file.object_id}}: ${(e as Error).message}`);
	}
}

declare var self: Worker;

function log(text: string) {
	self.postMessage({ type: 'log', text });
}

self.onmessage = (event: MessageEvent) => {
	if (event.data.type === 'memory') {
		self.postMessage({ type: 'memory', data: process.memoryUsage() });
		return;
	}

	const { submission_id } = event.data;
	const node: QueueNode = { value: submission_id, next: null };

	if (queue_tail !== null)
		queue_tail.next = node;
	else
		queue_head = node;

	queue_tail = node;
	queue_size++;

	if (!processing)
		process_queue();
};

async function process_queue() {
	processing = true;

	while (queue_head !== null) {
		const submission_id = queue_head.value;
		queue_head = queue_head.next;
		queue_size--;

		if (queue_head === null)
			queue_tail = null;

		log(`processing {${submission_id}} (${queue_size} remaining)`);

		try {
			await process_submission(submission_id);
		} catch (e) {
			caution('cache: failed to process submission', { submission_id, error: e });
		}
	}

	processing = false;
}

async function update_file_status(object_id: string, status: string, failure_reason: string | null, records_added: number) {
	await db_archavon`
		UPDATE cache_submission_files
		SET status = ${status}, failure_reason = ${failure_reason}, records_added = ${records_added}
		WHERE object_id = ${object_id}
	`;
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

	await db_archavon`UPDATE cache_submissions SET status = 'processing' WHERE submission_id = ${submission_id}`;
	await upsert_machine(db_archavon, machine_id);

	const files = await db_archavon`
		SELECT file_name, locale, object_id
		FROM cache_submission_files
		WHERE submission_id = ${submission_id}
	`;

	let completed = 0;
	let rejected = 0;
	const rejection_reasons: string[] = [];

	const rss_before = measure_rss('submission start');

	for (const file of files) {
		try {
			const res = await cache_bucket.download(file.object_id);
			measure_rss('after download');

			if (!res.ok) {
				await res.body?.cancel();
				log(`file {${file.object_id}}: download failed (${res.status}), rejecting`);
				await reject_file(file, 'download_failed');
				rejected++;
				rejection_reasons.push('download_failed');
				continue;
			}

			const data = await res.arrayBuffer();
			measure_rss('after arrayBuffer');

			if (data.byteLength < 4) {
				log(`file {${file.object_id}}: too small (${data.byteLength} bytes), rejecting`);
				await reject_file(file, 'parse_error');
				rejected++;
				rejection_reasons.push('parse_error');
				continue;
			}

			const magic_view = new DataView(data);

			if (file.file_name.endsWith('.wdb')) {
				const magic_bytes = new Uint8Array(data, 0, 4);
				const wdb_sig = String.fromCharCode(magic_bytes[3], magic_bytes[2], magic_bytes[1], magic_bytes[0]);

				if (!WDB_MAGIC_KEYS.has(wdb_sig)) {
					log(`wdb {${file.locale}/${file.file_name}}: invalid magic "${wdb_sig}", rejecting`);
					await reject_file(file, 'invalid_magic');
					rejected++;
					rejection_reasons.push('invalid_magic');
					continue;
				}

				const result = parse_wdb(data, patch);
				measure_rss('after parse_wdb');

				if (result) {
					const valid_records = result.records.filter(r => !('parse_error' in r.data));
					const parse_errors = result.records.length - valid_records.length;
					const sig = result.header.signature;
					const store_fn = WDB_STORE_MAP[sig];
					if (store_fn) {
						const stored = await store_fn(db_archavon, valid_records, file.locale, product, build_number, machine_id, submission_id);
						measure_rss('after wdb store');
						log(`wdb {${file.locale}/${file.file_name}}: ${result.records.length} records, stored ${stored}, ${parse_errors} parse errors (${sig})`);
						await update_file_status(file.object_id, 'completed', null, stored);
						completed++;
					} else {
						log(`wdb {${file.locale}/${file.file_name}}: unknown signature ${sig}, ${result.records.length} records skipped`);
						await reject_file(file, 'unknown_signature');
						rejected++;
						rejection_reasons.push('unknown_signature');
					}
				} else {
					log(`wdb {${file.locale}/${file.file_name}}: failed to parse (${data.byteLength} bytes)`);
					await reject_file(file, 'parse_error');
					rejected++;
					rejection_reasons.push('parse_error');
				}
			} else if (file.file_name.toLowerCase() === 'dbcache.bin') {
				const dbcache_magic = magic_view.getUint32(0, true);

				if (dbcache_magic !== XFTH_MAGIC) {
					log(`dbcache {${file.locale}/${file.file_name}}: invalid magic 0x${(dbcache_magic >>> 0).toString(16)}, rejecting`);
					await reject_file(file, 'invalid_magic');
					rejected++;
					rejection_reasons.push('invalid_magic');
					continue;
				}

				const result = parse_dbcache(data);
				measure_rss('after parse_dbcache');

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
								entry.record_data ? Buffer.from(new Uint8Array(entry.record_data)) : null,
								product
							);
						}

						await db_archavon.unsafe(
							`INSERT INTO hotfix_entries (table_hash, record_id, push_id, unique_id, region_id, status, game_build, data_blob, product) VALUES ${placeholders.join(',')} ON DUPLICATE KEY UPDATE unique_id = IF(VALUES(data_blob) IS NOT NULL AND data_blob IS NULL, VALUES(unique_id), unique_id), region_id = IF(VALUES(data_blob) IS NOT NULL AND data_blob IS NULL, VALUES(region_id), region_id), status = IF(VALUES(data_blob) IS NOT NULL AND data_blob IS NULL, VALUES(status), status), game_build = IF(VALUES(data_blob) IS NOT NULL AND data_blob IS NULL, VALUES(game_build), game_build), data_blob = IF(VALUES(data_blob) IS NOT NULL AND data_blob IS NULL, VALUES(data_blob), data_blob)`,
							params
						);

						inserted += batch.length;
					}

					measure_rss('after dbcache store');
					log(`dbcache {${file.locale}/${file.file_name}}: stored {${inserted}} hotfix entries`);
					await update_file_status(file.object_id, 'completed', null, inserted);
					completed++;
				} else {
					log(`dbcache {${file.locale}/${file.file_name}}: failed to parse (${data.byteLength} bytes)`);
					await reject_file(file, 'parse_error');
					rejected++;
					rejection_reasons.push('parse_error');
				}
			}
		} catch (e) {
			log(`failed to download {${file.object_id}}: ${(e as Error).message}`);
			await update_file_status(file.object_id, 'rejected', 'download_failed', 0);
			rejected++;
			rejection_reasons.push('download_failed');
		}
	}

	const rss_after = measure_rss('submission end');
	log(`[mem] delta: ${(rss_after - rss_before).toFixed(2)} MB`);

	const total = completed + rejected;
	let status: string;
	let status_reason: string | null = null;

	if (rejected === 0)
		status = 'completed';
	else if (completed === 0)
		status = 'failed';
	else
		status = 'partial';

	if (rejected > 0) {
		const unique_reasons = [...new Set(rejection_reasons)].join(', ');
		status_reason = `${completed}/${total} files processed, ${rejected} rejected (${unique_reasons})`;
	}

	await db_archavon`
		UPDATE cache_submissions
		SET processed_at = NOW(), status = ${status}, status_reason = ${status_reason}
		WHERE submission_id = ${submission_id}
	`;

	log(`submission {${submission_id}} done: ${completed} completed, ${rejected} rejected [${status}]`);
}
