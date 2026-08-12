import { caution } from 'spooder';
import { bucket } from './obj_rds';
import { parse_wdb, type WdbRecord } from './wdb';
import { parse_dbcache } from './dbcache';
import { archavon_api, type FailureReason, type SubmissionFile } from './archavon_api';
import { WdbDelta } from './wdb_delta';

const cache_bucket = bucket('wow.export.cache', process.env.CACHE_CDN_SECRET!);
const archavon = archavon_api();

type DeltaAdd = (delta: WdbDelta, records: WdbRecord[], locale: string, product: string, game_build: number) => number;

const WDB_DELTA_MAP: Record<string, DeltaAdd> = {
	'WMOB': (delta, records, locale, product, build) => delta.add_creatures(records, locale, product, build),
	'WQST': (delta, records, locale, product, build) => delta.add_quests(records, locale, product, build),
	'WGOB': (delta, records, locale, product, build) => delta.add_gameobjects(records, locale, product, build),
	'WPTX': (delta, records, locale, product, build) => delta.add_pagetext(records, locale, product, build)
};

const WDB_MAGIC_KEYS = new Set(Object.keys(WDB_DELTA_MAP));
const XFTH_MAGIC = 0x48544658;

// a game build changing a record layout shows up as a spike in per-record parse failures
const PARSE_ERROR_ALERT_RATIO = 0.05;

declare var self: Worker;

function log(text: string) {
	self.postMessage({ type: 'log', text });
}

self.onmessage = async (event: MessageEvent) => {
	if (event.data.type === 'memory') {
		self.postMessage({ type: 'memory', data: process.memoryUsage() });
		return;
	}

	const { submission_id } = event.data;

	try {
		await process_submission(submission_id);
	} catch (e) {
		caution('cache: failed to process submission', { submission_id, error: e });

		// closing without posting 'done' routes the submission through the retry path;
		// delta apply is idempotent server-side, so a re-run is safe
		self.close();
		return;
	}

	self.postMessage({ type: 'done' });
};

// drops the cdn object and records the file as rejected in the delta
async function reject_file(delta: WdbDelta, file: SubmissionFile, reason: FailureReason) {
	try {
		await cache_bucket.delete(file.object_id);
	} catch (e) {
		log(`failed to delete rejected CDN object {${file.object_id}}: ${(e as Error).message}`);
	}

	delta.set_file_result(file.file_name, file.locale, 'rejected', reason, 0);
}

async function process_submission(submission_id: string) {
	const submission = await archavon.get_submission(submission_id).catch(e => {
		if (e?.status === 404)
			return null;

		throw e;
	});

	if (submission === null) {
		log(`submission {${submission_id}} not found, skipping`);
		return;
	}

	const { build_number, machine_id, patch, product } = submission;

	log(`submission {${submission_id}} ${product} ${patch}.${build_number} (machine: ${machine_id})`);

	await archavon.update_submission_status({ submission_id, status: 'processing' });
	await archavon.check_machine(machine_id);

	const delta = new WdbDelta(submission_id, machine_id);

	try {
		let completed = 0;
		let rejected = 0;

		for (const file of submission.files) {
			try {
				const res = await cache_bucket.download(file.object_id);
				if (!res.ok) {
					await res.body?.cancel();
					log(`file {${file.object_id}}: download failed (${res.status}), rejecting`);
					await reject_file(delta, file, 'download_failed');
					rejected++;
					continue;
				}

				const data = await res.arrayBuffer();
				if (data.byteLength < 4) {
					log(`file {${file.object_id}}: too small (${data.byteLength} bytes), rejecting`);
					await reject_file(delta, file, 'parse_error');
					rejected++;
					continue;
				}

				const magic_view = new DataView(data);

				if (file.file_name.endsWith('.wdb')) {
					const magic_bytes = new Uint8Array(data, 0, 4);
					const wdb_sig = String.fromCharCode(magic_bytes[3], magic_bytes[2], magic_bytes[1], magic_bytes[0]);

					if (!WDB_MAGIC_KEYS.has(wdb_sig)) {
						log(`wdb {${file.locale}/${file.file_name}}: invalid magic "${wdb_sig}", rejecting`);
						await reject_file(delta, file, 'invalid_magic');
						rejected++;
						continue;
					}

					const result = parse_wdb(data, patch, product);
					if (result === null) {
						log(`wdb {${file.locale}/${file.file_name}}: failed to parse (${data.byteLength} bytes)`);
						await reject_file(delta, file, 'parse_error');
						rejected++;
						continue;
					}

					const sig = result.header.signature;
					const add_fn = WDB_DELTA_MAP[sig];

					if (add_fn === undefined) {
						log(`wdb {${file.locale}/${file.file_name}}: unknown signature ${sig}, ${result.records.length} records skipped`);
						await reject_file(delta, file, 'unknown_signature');
						rejected++;
						continue;
					}

					const valid_records = result.records.filter(r => !('parse_error' in r.data));
					const parse_errors = result.records.length - valid_records.length;
					const stored = add_fn(delta, valid_records, file.locale, product, build_number);

					log(`wdb {${file.locale}/${file.file_name}}: ${result.records.length} records, stored ${stored}, ${parse_errors} parse errors (${sig})`);

					// dropped records are otherwise invisible; the submission still reports completed
					if (parse_errors > 0 && parse_errors / result.records.length >= PARSE_ERROR_ALERT_RATIO) {
						caution('cache: wdb parse errors above threshold', {
							submission_id,
							file_name: file.file_name,
							locale: file.locale,
							signature: sig,
							product,
							patch,
							cache_build: result.header.build,
							records: result.records.length,
							parse_errors
						});
					}

					delta.set_file_result(file.file_name, file.locale, 'completed', null, stored);
					completed++;
				} else if (file.file_name.toLowerCase() === 'dbcache.bin') {
					const dbcache_magic = magic_view.getUint32(0, true);

					if (dbcache_magic !== XFTH_MAGIC) {
						log(`dbcache {${file.locale}/${file.file_name}}: invalid magic 0x${(dbcache_magic >>> 0).toString(16)}, rejecting`);
						await reject_file(delta, file, 'invalid_magic');
						rejected++;
						continue;
					}

					const result = parse_dbcache(data);
					if (result === null) {
						log(`dbcache {${file.locale}/${file.file_name}}: failed to parse (${data.byteLength} bytes)`);
						await reject_file(delta, file, 'parse_error');
						rejected++;
						continue;
					}

					log(`dbcache {${file.locale}/${file.file_name}}: ${result.entries.length} entries, build=${result.header.build}, version=${result.header.version}`);

					const inserted = delta.add_hotfixes(result.entries, product, build_number);

					log(`dbcache {${file.locale}/${file.file_name}}: stored {${inserted}} hotfix entries`);
					delta.set_file_result(file.file_name, file.locale, 'completed', null, inserted);
					completed++;
				}
			} catch (e) {
				log(`failed to process {${file.object_id}}: ${(e as Error).message}`);
				delta.set_file_result(file.file_name, file.locale, 'rejected', 'download_failed', 0);
				rejected++;
			}
		}

		const payload = delta.serialize();
		log(`submission {${submission_id}} delta built: ${payload.byteLength} bytes, ${completed} completed, ${rejected} rejected`);

		// no terminal status call; the apply endpoint owns the per-file writes and the
		// roll-up, and a failed dispatch throws into the retry path
		const result = await archavon.upload_delta(submission_id, payload);

		if (result.already_applied) {
			log(`submission {${submission_id}} delta already applied at ${result.applied_at}`);
			return;
		}

		log(`submission {${submission_id}} done: ${result.files_updated} files updated, ${result.attestations} attestations, ${result.hotfixes} hotfixes, consensus +${result.consensus.promoted}/-${result.consensus.demoted} [${result.status}]`);
	} finally {
		delta.close();
	}
}
