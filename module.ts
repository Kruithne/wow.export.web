import { serve } from 'spooder';
import crypto from 'node:crypto';

type SpooderServer = ReturnType<typeof serve>;

let index: string|null = null;
let index_hash: string|null = null;

export function init(server: SpooderServer) {
	server.route('/wow.export', async (req) => {
		if (index === null) {
			index = await Bun.file('./wow.export/index.html').text();
			index_hash = crypto.createHash('sha256').update(index).digest('hex');
		}
		
		const headers = {
			'Content-Type': 'text/html',
			'Access-Control-Allow-Origin':  '*',
			'ETag': index_hash as string
		} as Record<string, string>;
		
		if (req.headers.get('If-None-Match') === index_hash)
			return new Response(null, { status: 304, headers }); // Not Modified
		
		return new Response(index, { status: 200, headers });
	});

	server.dir('/wow.export/static', './wow.export/static');
}