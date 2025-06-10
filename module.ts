import { serve } from 'spooder';
import path from 'node:path';

type SpooderServer = ReturnType<typeof serve>;

export function init(server: SpooderServer) {
	server.dir('/wow.export', './wow.export/public', async (file_path, file, stat, _request) => {
		// ignore hidden files
		if (path.basename(file_path).startsWith('.'))
			return 404; // Not Found
		
		// ignore directories
		if (stat.isDirectory())
			return 401; // Unauthorized
		
		return file;
	});
}