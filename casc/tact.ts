import { log_create_logger } from 'spooder';
import { cache_get_file, cache_put_file } from '../cache';
import { db } from '../db';

const log = log_create_logger('tact', '#b36942');

const TACT_CACHE_KEY = 'tact/keys.txt';

type GitHubHead = {
	ref: string;
	node_id: string;
	url: string;
	object: {
		sha: string;
		type: string;
		url: string;
	}
};

const key_ring = new Map<string, string>();
let tact_available = true;

/**
 * Get a decryption key by its name.
 * @param key_name The name/identifier of the key (hex string)
 * @returns The key as a hex string, or undefined if not found
 */
export function tact_get_key(key_name: string): string | undefined {
	if (!tact_available)
		return undefined;

	return key_ring.get(key_name.toUpperCase());
}

/**
 * Load TACT encryption keys from the remote source.
 * Keys are cached locally and only refreshed when the remote version changes.
 */
export async function tact_load_keys(): Promise<void> {
	log`loading tact encryption keys`;
	let invalidate_cache = false;
	let head_rev: string | undefined;

	try {
		const [query, head_res] = await Promise.all([
			db`SELECT kv_value FROM kv_store WHERE kv_key = ${'tact_rev_hash'}`,
			fetch(process.env.TACT_KEYS_HEAD!)
		]);

		const cached_rev = query[0]?.kv_value;

		if (head_res.ok) {
			const head = await head_res.json() as GitHubHead;
			head_rev = head.object.sha;

			if (cached_rev !== head_rev) {
				log`tact out-of-date (${cached_rev ?? 'none'} !== ${head_rev}), invalidating cache`;
				invalidate_cache = true;
			}
		}
	} catch (e) {
		log`failed to check tact version (${e}), using cached version if available`;
	}

	let data: string | null = null;
	const cached = await cache_get_file(TACT_CACHE_KEY);

	if (!invalidate_cache && cached)
		data = await cached.text();

	if (!data) {
		try {
			const res = await fetch(process.env.TACT_KEYS_URL!);
			if (!res.ok) {
				if (cached) {
					log`failed to fetch tact keys (status: ${res.status}), using cached version`;
					data = await cached.text();
				} else {
					log`failed to fetch tact keys (status: ${res.status}), no cached version available - tact support disabled`;
					tact_available = false;
					return;
				}
			} else {
				data = await res.text();
				cache_put_file(TACT_CACHE_KEY, data);

				if (head_rev)
					await db`INSERT INTO kv_store (kv_key, kv_value) VALUES(${'tact_rev_hash'}, ${head_rev}) ON DUPLICATE KEY UPDATE kv_value = ${head_rev}`;
			}
		} catch (e) {
			if (cached) {
				log`failed to fetch tact keys (${e}), using cached version`;
				data = await cached.text();
			} else {
				log`failed to fetch tact keys (${e}), no cached version available - tact support disabled`;
				tact_available = false;
				return;
			}
		}
	}

	const lines = data.split(/\r?\n/);
	for (const line of lines) {
		const key_name = line.substring(0, 16);
		const key = line.substring(16, 32 + 16 + 1);
		key_ring.set(key_name, key);
	}

	log`loaded ${key_ring.size} tact encryption keys`;
}
