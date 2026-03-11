import { caution } from 'spooder';
import { db_archavon } from './db_archavon';
import { bucket } from './obj_rds';

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

			// TODO
			const view = new DataView(data);
			const magic = data.byteLength >= 4 ? String.fromCharCode(view.getUint8(0), view.getUint8(1), view.getUint8(2), view.getUint8(3)) : '????';
			log(`wdb {${file.locale}/${file.file_name}}: ${data.byteLength} bytes, magic={${magic}}`);

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
