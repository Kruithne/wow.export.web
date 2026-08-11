/*!
	archavon write API client

	Server-to-server client for the SQLite-backed archavon write API.
	Contract: F:\archavon\docs\write_api.md

	Signing mirrors obj_rds.ts but is pinned to sha256; the algorithm is never negotiated.
		json     signature over the raw request body; body carries `created` (ms)
		upload   signature over `<content-hash>:<created>:<submission-id>`

	No pipeline wiring; call sites are migrated separately.
 */

import crypto from 'node:crypto';

const HMAC_ALG = 'sha256';
const HMAC_MSG_EXPIRY_MS = 5 * 60 * 1000;

const DEFAULT_RETRY_COUNT = 3;
const DEFAULT_RETRY_DELAY_MS = 250;
const DEFAULT_RETRY_DELAY_MAX_MS = 4000;
const DEFAULT_TIMEOUT_MS = 30000;
const DELTA_TIMEOUT_MS = 120000;

const RETRY_STATUS = new Set([408, 425, 429, 500, 502, 503, 504]);

// server-side caps, mirrored so oversized batches fail client-side with a useful message
export const MAX_FILES_PER_REQUEST = 256;
export const MAX_HASHES_PER_REQUEST = 1024;
export const MAX_SUBMISSION_IDS_PER_REQUEST = 500;

export const FILE_STATUS = ['pending', 'completed', 'rejected'] as const;
export const FAILURE_REASONS = ['download_failed', 'checksum_mismatch', 'invalid_magic', 'parse_error', 'no_records', 'unknown_signature', 'purged'] as const;
export const SUBMISSION_STATUS = ['pending', 'finalized', 'processing', 'completed', 'partial', 'failed'] as const;

export type FileStatus = typeof FILE_STATUS[number];
export type FailureReason = typeof FAILURE_REASONS[number];
export type SubmissionStatus = typeof SUBMISSION_STATUS[number];

// `pending` and `finalized` are owned by submission / submission/finalize
export type SettableSubmissionStatus = Exclude<SubmissionStatus, 'pending' | 'finalized'>;

export type MachineState = {
	machine_id: string;
	blocked: boolean;
	block_reason: string | null;
	trust_score: number;
	first_seen: string;
	last_seen: string;
};

export type BinaryHash = {
	content_hash: string;
	file_size: number;
};

export type BinaryHashLookup = {
	build_key: string;
	known: boolean;
	build_hash_count: number;
	hashes: Record<string, BinaryHash[]>;
};

export type BinaryHashEntry = {
	file_name: string;
	content_hash: string;
	file_size: number;
};

export type BinaryHashStoreResult = {
	build_key: string;
	submitted: number;
	inserted: number;
};

export type SubmissionFileInput = {
	file_name: string;
	locale: string;
	file_size: number;
	object_id: string;
	modified_at?: string | null;
};

export type CreateSubmissionRequest = {
	submission_id: string;
	machine_id: string;
	product: string;
	patch: string;
	build_number: number;
	build_key: string;
	cdn_key: string;
	binary_hash: Record<string, string> | string;
	client_ip?: string;
	files: SubmissionFileInput[];
};

export type CreateSubmissionResult = {
	submission_id: string;
	submitted_at: string;
	status: SubmissionStatus;
	files: number;
};

export type SubmissionFile = {
	file_name: string;
	locale: string;
	file_size: number;
	modified_at: string | null;
	object_id: string;
	status: FileStatus;
	failure_reason: FailureReason | null;
	records_added: number;
};

export type SubmissionDetail = {
	submission_id: string;
	machine_id: string;
	product: string;
	patch: string;
	build_number: number;
	build_key: string;
	cdn_key: string | null;
	submitted_at: string;
	finalized_at: string | null;
	processed_at: string | null;
	status: SubmissionStatus;
	status_reason: string | null;
	files: SubmissionFile[];
};

export type FileRef = {
	file_name: string;
	locale: string;
};

export type FinalizeResult = {
	submission_id: string;
	status: SubmissionStatus;
	finalized_at: string;
	kept: Array<FileRef & { object_id: string }>;
	removed: Array<FileRef & { object_id: string }>;
};

// addressed by object_id, or by file_name + locale
export type FileStatusUpdate = {
	object_id?: string;
	file_name?: string;
	locale?: string;
	status: FileStatus;
	failure_reason?: FailureReason | null;
	records_added?: number;
};

export type UpdateStatusRequest = {
	submission_id: string;
	status?: SettableSubmissionStatus;
	status_reason?: string | null;
	files?: FileStatusUpdate[];
};

export type UpdateStatusResult = {
	submission_id: string;
	status: SubmissionStatus;
	status_reason: string | null;
	processed_at: string | null;
	files_updated: number;
	unknown_files: number;
};

export type DeleteSubmissionsResult = {
	requested: number;
	deleted: number;
};

export type StaleSubmission = {
	submission_id: string;
	submitted_at: string;
	files: Array<FileRef & { object_id: string }>;
};

export type StaleSubmissionsResult = {
	max_age_hours: number;
	count: number;
	submissions: StaleSubmission[];
};

export type DeltaEntityCounts = Record<string, number>;

export type DeltaAppliedResult = {
	submission_id: string;
	already_applied: false;
	status: SubmissionStatus;
	status_reason: string | null;
	files_updated: number;
	unknown_files: number;
	entities: DeltaEntityCounts;
	junctions: DeltaEntityCounts;
	attestations: number;
	hotfixes: number;
	table_hashes: number;
	consensus: { promoted: number; demoted: number };
};

export type DeltaAlreadyAppliedResult = {
	submission_id: string;
	already_applied: true;
	applied_at: string;
};

export type DeltaResult = DeltaAppliedResult | DeltaAlreadyAppliedResult;

export type ClientOptions = {
	base_url?: string;
	secret?: string;
	retry_count?: number;
	retry_delay_ms?: number;
	timeout_ms?: number;
};

type RequestOptions = {
	retry_count?: number;
	timeout_ms?: number;
};

export class ArchavonApiError extends Error {
	readonly status: number;
	readonly endpoint: string;
	readonly body: string;

	constructor(endpoint: string, status: number, message: string, body: string) {
		super(`archavon_api: ${endpoint} failed (${status}): ${message}`);
		this.name = 'ArchavonApiError';
		this.status = status;
		this.endpoint = endpoint;
		this.body = body;
	}
}

/** HMAC-sha256 of `message`, hex encoded. */
export function sign_message(message: string, secret: string): string {
	return crypto.createHmac(HMAC_ALG, secret).update(message, 'utf8').digest('hex');
}

/** `X-Signature` value for an already-serialised JSON body. */
export function sign_json_body(body: string, secret: string): string {
	return `${HMAC_ALG}=${sign_message(body, secret)}`;
}

/** `X-Signature` value for a delta upload; signs the claimed body hash, not the body. */
export function sign_upload(content_hash: string, created: number, submission_id: string, secret: string): string {
	return `${HMAC_ALG}=${sign_message(`${content_hash}:${created}:${submission_id}`, secret)}`;
}

export function hash_body(data: Uint8Array): string {
	return new Bun.CryptoHasher('sha256').update(data).digest('hex');
}

function delay(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms));
}

function backoff_delay(base_ms: number, attempt: number): number {
	return Math.min(base_ms * (2 ** attempt), DEFAULT_RETRY_DELAY_MAX_MS);
}

function assert_limit(label: string, length: number, max: number) {
	if (length > max)
		throw new RangeError(`archavon_api: ${label} exceeds server limit (${length} > ${max})`);
}

async function error_from_response(endpoint: string, res: Response): Promise<ArchavonApiError> {
	const body = await res.text().catch(() => '');
	let message = body;

	try {
		const parsed = JSON.parse(body);
		if (parsed && typeof parsed.error === 'string')
			message = parsed.error;
	} catch {
		// non-json error body; the raw text is the message
	}

	return new ArchavonApiError(endpoint, res.status, message || res.statusText, body);
}

export function archavon_api(options: ClientOptions = {}) {
	const base_url = (options.base_url ?? process.env.ARCHAVON_WRITE_URL ?? '').replace(/\/+$/, '');
	const secret = options.secret ?? process.env.ARCHAVON_WRITE_SECRET ?? '';

	const retry_count = options.retry_count ?? DEFAULT_RETRY_COUNT;
	const retry_delay_ms = options.retry_delay_ms ?? DEFAULT_RETRY_DELAY_MS;
	const timeout_ms = options.timeout_ms ?? DEFAULT_TIMEOUT_MS;

	// each attempt re-signs, so a backoff never pushes `created` outside the 5m window
	async function send(endpoint: string, build: () => { body: string | Uint8Array; headers: Record<string, string> }, opts: RequestOptions = {}): Promise<Response> {
		if (base_url === '')
			throw new Error('archavon_api: ARCHAVON_WRITE_URL not configured');

		if (secret === '')
			throw new Error('archavon_api: ARCHAVON_WRITE_SECRET not configured');

		const max_attempts = (opts.retry_count ?? retry_count) + 1;
		const url = `${base_url}/api/v1/${endpoint}`;

		let attempt = 0;
		for (;;) {
			const { body, headers } = build();

			try {
				const res = await fetch(url, {
					method: 'POST',
					headers,
					body: body as any,
					signal: AbortSignal.timeout(opts.timeout_ms ?? timeout_ms)
				});

				if (!res.ok && RETRY_STATUS.has(res.status) && attempt < max_attempts - 1) {
					await res.body?.cancel();
					await delay(backoff_delay(retry_delay_ms, attempt++));
					continue;
				}

				if (!res.ok)
					throw await error_from_response(endpoint, res);

				return res;
			} catch (error) {
				if (error instanceof ArchavonApiError)
					throw error;

				if (attempt >= max_attempts - 1)
					throw error;

				await delay(backoff_delay(retry_delay_ms, attempt++));
			}
		}
	}

	async function post_json<T>(endpoint: string, payload: Record<string, unknown>, opts: RequestOptions = {}): Promise<T> {
		const res = await send(endpoint, () => {
			const body = JSON.stringify({ ...payload, created: Date.now() });

			return {
				body,
				headers: {
					'Content-Type': 'application/json',
					'X-Signature': sign_json_body(body, secret)
				}
			};
		}, opts);

		return await res.json() as T;
	}

	return {
		/** Upserts the machine and reports block state; reject the submission when `blocked`. */
		check_machine: (machine_id: string, hardware_hash?: string): Promise<MachineState> => {
			const payload: Record<string, unknown> = { machine_id };
			if (hardware_hash !== undefined)
				payload.hardware_hash = hardware_hash;

			return post_json<MachineState>('intake/machine', payload);
		},

		/** Known binary hashes for a build; omit `file_names` for every file of the build. */
		get_binary_hashes: (build_key: string, file_names?: string[]): Promise<BinaryHashLookup> => {
			const payload: Record<string, unknown> = { build_key };

			if (file_names !== undefined) {
				assert_limit('file_names', file_names.length, MAX_FILES_PER_REQUEST);
				payload.file_names = file_names;
			}

			return post_json<BinaryHashLookup>('intake/hashes', payload);
		},

		/** Stores hashes parsed out of an install manifest; re-storing is a no-op. */
		store_binary_hashes: (build_key: string, hashes: BinaryHashEntry[]): Promise<BinaryHashStoreResult> => {
			assert_limit('hashes', hashes.length, MAX_HASHES_PER_REQUEST);

			return post_json<BinaryHashStoreResult>('intake/hashes/store', { build_key, hashes });
		},

		/**
		 * Creates the submission and its file rows; `client_ip` must already be hashed.
		 *
		 * Not retried by default: a lost response followed by a retry answers 409, which
		 * would surface as a failure for a submission that actually landed.
		 */
		create_submission: (req: CreateSubmissionRequest, opts: RequestOptions = { retry_count: 0 }): Promise<CreateSubmissionResult> => {
			assert_limit('files', req.files.length, MAX_FILES_PER_REQUEST);

			return post_json<CreateSubmissionResult>('intake/submission', req as unknown as Record<string, unknown>, opts);
		},

		/** Submission + file rows for the stateless finalize/worker path. */
		get_submission: (submission_id: string): Promise<SubmissionDetail> => {
			return post_json<SubmissionDetail>('intake/submission/files', { submission_id });
		},

		/** Closes the submission off; drops files absent from `keep` and returns their object_ids. */
		finalize_submission: (submission_id: string, keep: FileRef[]): Promise<FinalizeResult> => {
			assert_limit('keep', keep.length, MAX_FILES_PER_REQUEST);

			return post_json<FinalizeResult>('intake/submission/finalize', { submission_id, keep });
		},

		/** Submission-level and per-file status writes; every field but the id is optional. */
		update_submission_status: (req: UpdateStatusRequest): Promise<UpdateStatusResult> => {
			if (req.files !== undefined)
				assert_limit('files', req.files.length, MAX_FILES_PER_REQUEST);

			return post_json<UpdateStatusResult>('intake/submission/status', req as unknown as Record<string, unknown>);
		},

		/** Deletes submissions and (cascade) their file rows. */
		delete_submissions: (submission_ids: string[], unfinalized_only = false): Promise<DeleteSubmissionsResult> => {
			assert_limit('submission_ids', submission_ids.length, MAX_SUBMISSION_IDS_PER_REQUEST);

			return post_json<DeleteSubmissionsResult>('intake/submission/delete', { submission_ids, unfinalized_only });
		},

		/** Read-only listing of submissions that were never finalized; `max_age_hours` minimum is 1. */
		list_stale_submissions: (max_age_hours?: number, limit?: number): Promise<StaleSubmissionsResult> => {
			const payload: Record<string, unknown> = {};

			if (max_age_hours !== undefined)
				payload.max_age_hours = max_age_hours;

			if (limit !== undefined)
				payload.limit = limit;

			return post_json<StaleSubmissionsResult>('intake/cleanup/stale', payload);
		},

		/**
		 * Applies one worker-produced delta database (bun:sqlite `db.serialize()`).
		 *
		 * Idempotent server-side via the delta_applications ledger, so retries are safe.
		 */
		upload_delta: async (submission_id: string, data: Uint8Array, opts: RequestOptions = {}): Promise<DeltaResult> => {
			const content_hash = hash_body(data);

			const res = await send('delta', () => {
				const created = Date.now();

				return {
					body: data,
					headers: {
						'Content-Type': 'application/octet-stream',
						'X-Submission-Id': submission_id,
						'X-Content-Hash': content_hash,
						'X-Created': String(created),
						'X-Signature': sign_upload(content_hash, created, submission_id, secret)
					}
				};
			}, { timeout_ms: DELTA_TIMEOUT_MS, ...opts });

			return await res.json() as DeltaResult;
		}
	};
}

export type ArchavonApi = ReturnType<typeof archavon_api>;

export { HMAC_ALG, HMAC_MSG_EXPIRY_MS };
