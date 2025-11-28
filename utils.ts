export function url_join(...parts: string[]): string {
	return parts.join('/').replace(/(?<!:)\/\/+/g, '/');
}

export async function promise_queue<T, R>(items: T[], fn: (item: T) => Promise<R>, concurrency: number): Promise<R[]> {
	const results: R[] = [];
	const executing: Promise<void>[] = [];

	for (const [index, item] of items.entries()) {
		const promise = fn(item).then(result => {
			results[index] = result;
		});

		const wrapped = promise.then(() => {
			executing.splice(executing.indexOf(wrapped), 1);
		});

		executing.push(wrapped);

		if (executing.length >= concurrency)
			await Promise.race(executing);
	}

	await Promise.all(executing);
	return results;
}

export function timespan(ms: number): string {
	const totalSeconds = ms / 1000;
	const seconds = Math.floor(totalSeconds);
	const minutes = Math.floor(seconds / 60);
	const hours = Math.floor(minutes / 60);

	const parts: string[] = [];

	if (hours > 0)
		parts.push(`${hours}h`);

	if (minutes % 60 > 0)
		parts.push(`${minutes % 60}m`);

	// If we have hours or minutes, just show whole seconds
	// Otherwise, show decimal precision
	if (hours > 0 || minutes > 0) {
		if (seconds % 60 > 0)
			parts.push(`${seconds % 60}s`);
	} else {
		parts.push(`${totalSeconds.toFixed(2)}s`);
	}

	return parts.length > 0 ? parts.join(' ') : '0.00s';
}