import path from 'node:path';

const CACHE_DIR = './cache';

type CacheData = Blob | NodeJS.TypedArray | ArrayBufferLike | string | Bun.BlobPart[];

export function cache_get_path(key: string): string {
	return path.join(CACHE_DIR, key);
}

export async function cache_get_file(key: string) {
	const cache_path = cache_get_path(key);
	const file = Bun.file(cache_path);

	if (await file.exists())
		return file;

	return null;
}

export async function cache_put_file(key: string, data: CacheData) {
	const cache_path = cache_get_path(key);
	await Bun.write(cache_path, data);
}
