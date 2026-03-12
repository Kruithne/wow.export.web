import { caution } from 'spooder';
import { db_archavon } from './db_archavon';
import { bucket } from './obj_rds';
import { parse_wdb } from './wdb';
import { parse_dbcache } from './dbcache';

const cache_bucket = bucket('wow.export.cache', process.env.CACHE_CDN_SECRET!);

const queue: string[] = [];
let processing = false;

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
		SELECT build_number
		FROM cache_submissions
		WHERE submission_id = ${submission_id}
	`;

	if (!submission) {
		log(`submission {${submission_id}} not found, skipping`);
		return;
	}

	const build_number = submission.build_number as number;

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

			if (file.file_name.endsWith('.wdb')) {
				const result = parse_wdb(data);
				if (result) {
					log(`wdb {${file.locale}/${file.file_name}}: ${result.records.length} records, build=${result.header.build}`);
					for (const record of result.records.slice(0, 5))
						log(`  [${record.id}] ${JSON.stringify(record.data)}`);
				} else {
					log(`wdb {${file.locale}/${file.file_name}}: failed to parse (${data.byteLength} bytes)`);
				}
			} else if (file.file_name === 'dbcache.bin') {
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
							placeholders.push('(?, ?, ?, ?, ?, ?, ?, ?)');
							params.push(
								entry.table_hash >>> 0,
								entry.record_id >>> 0,
								entry.push_id >>> 0,
								entry.unique_id >>> 0,
								entry.region_id >>> 0,
								entry.status,
								build_number,
								entry.record_data ? Buffer.from(entry.record_data) : null
							);
						}

						await db_archavon.unsafe(
							`INSERT IGNORE INTO hotfix_entries (table_hash, record_id, push_id, unique_id, region_id, status, game_build, data_blob) VALUES ${placeholders.join(',')}`,
							params
						);

						inserted += batch.length;
					}

					log(`dbcache {${file.locale}/${file.file_name}}: stored {${inserted}} hotfix entries`);
				} else {
					log(`dbcache {${file.locale}/${file.file_name}}: failed to parse (${data.byteLength} bytes)`);
				}
			}

			processed++;
		} catch (e) {
			log(`failed to download {${file.object_id}}: ${(e as Error).message}`);
			failed++;
		}

		try {
			await cache_bucket.delete(file.object_id);
		} catch (e) {
			log(`failed to delete CDN object {${file.object_id}}: ${(e as Error).message}`);
		}
	}

	await db_archavon`DELETE FROM cache_submission_files WHERE submission_id = ${submission_id}`;
	await db_archavon`DELETE FROM cache_submissions WHERE submission_id = ${submission_id}`;

	log(`submission {${submission_id}} done: ${processed} processed, ${failed} failed`);
}
