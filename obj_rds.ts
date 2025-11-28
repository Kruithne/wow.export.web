/*!
	spooderverse/obj_rds (https://github.com/Kruithne/spooderverse)
	Authors: Kruithne <kruithne@gmail.com>
	License: ISC
 */

import crypto from 'node:crypto';
import path from 'node:path';
import { caution } from 'spooder';

const CDN_URL = 'https://cdn.rubberducksolutions.dev';
const PRESIGN_EXPIRY_DEFAULT = 24 * 60 * 60 * 1000; // 24 hours

let HMAC_ALG = 'sha256';

type ObjectID = string;
type BunFile = ReturnType<typeof Bun.file>;

type UploadInput = BunFile | string | Buffer | ArrayBuffer | Uint8Array;

type UploadOptions = {
	chunk_size?: number;
	retry_count?: number;
	queue_size?: number;
	content_type?: string;
	filename?: string;
	object_id?: string;
};

type BucketStats = {
	size: number;
	files: number;
};

type ObjectStats = {
	filename: string;
	size: number;
	content_type: string;
	created: number;
};

type ListResult = {
	objects: Array<{
		object_id: string;
		filename: string;
		size: number;
		content_type: string;
		created: number;
	}>;
};

async function checksum_file(file: Blob): Promise<string> {
	const stream = file.stream();
	const hasher = new Bun.CryptoHasher('sha256');

	for await (const chunk of stream)
		hasher.update(chunk);

	return hasher.digest('hex');
}

function is_bun_file(obj: any): obj is BunFile {
	return obj.constructor === Blob;
}

/**
 * Sets the algorithm for HMAC signing.
 * 
 * Selected algorithm must be supported by API and installed OpenSSL version.
 * 
 * See: `openssl list -digest-algorithms`
 */
export function set_hmac_algorithm(alg: string) {
	HMAC_ALG = alg;
}

export function bucket(bucket_id: string, bucket_secret: string) {
	return {
		action: async function(action: string, params = {}): Promise<Response> {
			const payload = {
				action,
				created: Date.now(),
				...params
			};

			const payload_str = JSON.stringify(payload);
			const hmac = crypto.createHmac(HMAC_ALG, bucket_secret);
			hmac.update(payload_str);

			const res = await fetch(`${CDN_URL}/bucket/${bucket_id}`, {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json',
					'X-Signature': `${HMAC_ALG}=${hmac.digest('hex')}`
				},
				body: payload_str
			});

			if (!res.ok) {
				caution('obj_rds: bucket action failed', {
					status_text: await res.text(),
					status_code: res.status,
					bucket_id,
					action,
					params
				});
			}

			return res;
		},

		/**
		 * Provision an object in a bucket and returns the assigned object ID.
		 *
		 * If object_id is provided, the CDN will use that ID instead of generating one.
		 * Returns 409 status if the provided object_id already exists in this bucket.
		 *
		 * Provisioned object is subject to CDN expiry rules if not finalized.
		 *
		 * Returns NULL and raises a caution in the event of failure.
		 */
		provision: async function(filename: string, content_type: string, size: number, object_id?: string): Promise<ObjectID|null> {
			const params: Record<string, any> = { filename, content_type, size };
			if (object_id !== undefined)
				params.object_id = object_id;

			const res = await this.action('provision', params);

			if (res.ok) {
				const json = await res.json();
				return json.object_id;
			}

			return null;
		},

		finalize: async function (object_id: string, checksum?: string): Promise<boolean> {
			const res = await this.action('finalize', { object_id, checksum });
			return res.ok;
		},

		/**
		 * Get bucket statistics or object metadata.
		 *
		 * If called without object_id, returns bucket stats { size, files }.
		 * If called with object_id, returns object metadata { filename, size, content_type, created }.
		 *
		 * Returns NULL in the event of failure.
		 */
		stat: async function (object_id?: string): Promise<BucketStats | ObjectStats | null> {
			const params = object_id ? { object_id } : {};
			const res = await this.action('stat', params);
			return res.ok ? await res.json() : null;
		},

		/**
		 * Delete an object from the bucket.
		 *
		 * Only finalized objects can be deleted.
		 *
		 * Returns true if successful, false otherwise.
		 */
		delete: async function (object_id: string): Promise<boolean> {
			const res = await this.action('delete', { object_id });
			return res.ok;
		},

		/**
		 * List objects in the bucket.
		 *
		 * Returns paginated list of finalized objects ordered by creation date (oldest first).
		 *
		 * Returns NULL in the event of failure.
		 */
		list: async function (offset = 0, page_size?: number): Promise<ListResult | null> {
			const params: any = { offset };
			if (page_size !== undefined)
				params.page_size = page_size;

			const res = await this.action('list', params);
			return res.ok ? await res.json() : null;
		},

		/**
		 * Generates a pre-signed URL for an object.
		 */
		presign: function (object_id: string, expires = PRESIGN_EXPIRY_DEFAULT, action = 'access'): string {
			const header = { typ: 'JWT', alg: HMAC_ALG };
			const payload = { bucket_id, object_id, action, expires: Date.now() + expires };

			const encoded_header = Buffer.from(JSON.stringify(header)).toString('base64url');
			const encoded_payload = Buffer.from(JSON.stringify(payload)).toString('base64url');

			const message = encoded_header + '.' + encoded_payload;
			const hmac = crypto.createHmac(HMAC_ALG, bucket_secret);
			hmac.update(message);

			return `${this.url(object_id)}?token=${message}.${hmac.digest('base64url')}`;
		},

		upload: async function (input: UploadInput, options?: UploadOptions) {
			let file: Blob;
			if (is_bun_file(input)) {
				file = input;
			} else {
				file = new Blob([input as any], {type: options?.content_type ?? 'application/octet-stream'});
			}

			// start checksum early so it can run concurrently with uploads
			const checksum_promise = checksum_file(file);

			// providing an empty string to the API will use the object_id as the filename.
			const filename = is_bun_file(file) && file.name ? path.basename(file.name) : (options?.filename ?? '');
			const content_type = options?.content_type ?? file.type;
			const object_id = await this.provision(filename, content_type, file.size, options?.object_id);

			if (object_id === null)
				return null;

			const upload_url = this.presign(object_id, undefined, 'upload');

			const chunk_size = options?.chunk_size ?? 5 * 1024 * 1024;
			const retry_count = options?.retry_count ?? 3;
			const queue_size = options?.queue_size ?? 10;

			const total_chunks = Math.ceil(file.size / chunk_size);
			const abort_controller = new AbortController();

			let last_failed_status: number | undefined;
			let last_failed_message: string | undefined;

			const upload_chunk = async (chunk_index: number): Promise<boolean> => {
				const offset = chunk_index * chunk_size;
				const end = Math.min(offset + chunk_size, file.size);
				const chunk = file.slice(offset, end);

				let attempts = 0;
				const max_attempts = retry_count + 1;

				while (attempts < max_attempts) {
					try {
						const form_data = new FormData();
						form_data.append('file', chunk, 'chunk.bin');
						form_data.append('offset', offset.toString());

						const res = await fetch(upload_url, {
							method: 'POST',
							body: form_data,
							signal: abort_controller.signal
						});

						if (!res.ok) {
							last_failed_status = res.status;
							last_failed_message = await res.text();
							throw new Error(`Upload failed with status ${res.status}: ${last_failed_message}`);
						}

						return true;
					} catch (error) {
						if ((error as Error).name === 'AbortError')
							return false;

						attempts++;
						if (attempts >= max_attempts)
							return false;
					}
				}

				return false;
			};

			const active_promises: Promise<boolean>[] = [];
			let next_chunk_index = 0;
			let upload_failed = false;

			while (next_chunk_index < total_chunks || active_promises.length > 0) {
				while (active_promises.length < queue_size && next_chunk_index < total_chunks && !upload_failed)
					active_promises.push(upload_chunk(next_chunk_index++));

				if (active_promises.length > 0) {
					const completed = await Promise.race(active_promises.map((p, idx) =>
						p.then(() => idx).catch(() => idx)
					));

					const [completed_promise] = active_promises.splice(completed, 1);
					const success = await completed_promise;

					if (!success) {
						upload_failed = true;
						abort_controller.abort();
						await Promise.allSettled(active_promises);

						caution('obj_rds: upload failure', {
							bucket_id,
							content_type: file.type,
							filename,
							total_size: file.size,
							status: last_failed_status,
							error_message: last_failed_message
						});

						return null;
					}
				}
			}

			const checksum = await checksum_promise;
			const finalized = await this.finalize(object_id, checksum);
			if (!finalized) {
				caution('obj_rds: checksum integrity failure', { object_id, checksum });
				return null;
			}

			return object_id;
		},

		download: function (object_id: string): Promise<Response> {
			return fetch(this.presign(object_id));
		},

		/**
		 * Returns the public URL for a file.
		 *
		 * Use .presign() for a pre-signed URL to access private buckets.
		 */
		url: function (object_id: string): string {
			return `${CDN_URL}/data/${bucket_id}/${object_id}`;
		}
	}
}