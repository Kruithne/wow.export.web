import { http_serve, caution } from 'spooder';
import crypto from 'node:crypto';
import os from 'node:os';
import fs from 'node:fs/promises';
import path from 'node:path';
import { ColorInput } from 'bun';
import { extract_zip } from './zip.ts';

const UPDATE_TIMER = 24 * 60 * 60 * 1000; // 24 hours
const MAX_UPDATE_SIZE = 300 * 1024 * 1024; // 300MB

type SpooderServer = ReturnType<typeof http_serve>;

const ANSI_RESET = '\x1b[0m';
function log(message: string, color: ColorInput = 'orange'): void {
	const ansi = Bun.color(color, 'ansi-256');
	process.stdout.write(`[{wow.export}] > ${message}\n`.replace(/\{([^}]+)\}/g, `${ansi}$1${ANSI_RESET}`));
}

async function download_and_store(url: string, target_dir: string, target_filename: string, name: string) {
	const target_file = path.join(target_dir, target_filename);
	let temp_file: string | null = null;

	log(`downloading {${name}} from {${url}} to {${target_file}}`);
	
	try {
		const response = await fetch(url);
		if (!response.ok)
			throw new Error(`Failed to fetch ${name}: ${response.status} ${response.statusText}`);
		
		temp_file = path.join(os.tmpdir(), `${name}_${crypto.randomUUID()}.tmp`);
		
		const content_length = response.headers.get('content-length');
		const total_size = content_length ? parseInt(content_length, 10) : null;
		
		if (total_size && response.body) {
			const reader = response.body.getReader();
			const file_handle = Bun.file(temp_file);
			const writer = file_handle.writer();
			
			let downloaded_bytes = 0;
			let last_logged_percent = -1;
			
			try {
				while (true) {
					const { done, value } = await reader.read();
					if (done)
						break;
					
					writer.write(value);
					downloaded_bytes += value.length;
					
					const percent = Math.floor((downloaded_bytes / total_size) * 100);
					if (percent >= last_logged_percent + 10 && percent <= 100) {
						const downloaded_mb = (downloaded_bytes / (1024 * 1024)).toFixed(1);
						const total_mb = (total_size / (1024 * 1024)).toFixed(1);
						log(`downloading {${name}}: ${percent}% (${downloaded_mb} MB / ${total_mb} MB)`);
						last_logged_percent = percent;
					}
				}
			} finally {
				await writer.end();
				reader.releaseLock();
			}
		} else {
			await Bun.write(temp_file, response);
		}
		
		await fs.mkdir(target_dir, { recursive: true });
		await fs.rename(temp_file, target_file);

		log(`successfully updated {${name}} to {${target_file}}`);

		temp_file = null;
	} catch (error) {
		caution(`Failed to update ${name}`, [error instanceof Error ? error.message : String(error)]);
	} finally {
		if (temp_file)
			await fs.unlink(temp_file);
	}
}

async function update_data_files() {
	await update_listfile();
	await update_tact();

	schedule_update();
}

function schedule_update() {
	setTimeout(update_data_files, UPDATE_TIMER);
}

async function update_listfile() {
	const url = 'https://github.com/wowdev/wow-listfile/releases/latest/download/community-listfile.csv';
	const target_dir = './wow.export/data/listfile';
	
	await download_and_store(url, target_dir, 'master', 'listfile');
}

async function update_tact() {
	const url = 'https://raw.githubusercontent.com/wowdev/TACTKeys/master/WoW.txt';
	const target_dir = './wow.export/data/tact';
	
	await download_and_store(url, target_dir, 'wow', 'tact');
}

let index: string|null = null;
let index_hash: string|null = null;

interface UpdateRequest {
	manifest: any[];
	update_package: string;
	build_id: string;
}

let update_queue: UpdateRequest[] = [];
let is_processing_update = false;

interface UploadSession {
	build: string;
	m_size: number;
	c_size: number;
	t_size: number;
	temp_file: string;
	bytes_written: number;
	file_handle: any;
}

let is_uploading = false;
let current_upload: UploadSession | null = null;

async function cleanup_upload_session() {
	if (current_upload) {
		try {
			if (current_upload.file_handle)
				await current_upload.file_handle.end();

			await fs.unlink(current_upload.temp_file);
		} catch (error) {
			// ignore
		}

		current_upload = null;
	}
	is_uploading = false;
}

async function process_websocket_upload(ws: any) {
	if (!current_upload) {
		ws.close(1011, 'No upload session');
		return;
	}

	const { build, c_size, temp_file } = current_upload;

	try {
		const update_path = path.join('./wow.export/update', build);
		const bundle = Bun.file(temp_file);

		const manifest_data = bundle.slice(c_size);
		const manifest = await manifest_data.json(); // validates JSON parses
		
		const tmp_path_content = path.join(os.tmpdir(), 'wow_export_content_tmp');
		await Bun.write(tmp_path_content, bundle.slice(0, c_size));

		await fs.mkdir(update_path, { recursive: true });

		await Bun.write(path.join(update_path, 'update.json'), manifest_data);

		const proc = Bun.spawn(['mv', tmp_path_content, path.join(update_path, 'update')]);
		await proc.exited;

		log(`websocket upload completed successfully for build {${build}}`);
		ws.close(1000, 'Upload complete');
		await cleanup_upload_session();
	} catch (error) {
		const errorMsg = error instanceof Error ? error.message : String(error);
		log(`websocket upload processing failed: ${errorMsg}`);
		ws.close(1011, 'Processing failed');
		await cleanup_upload_session();
	}
}

async function process_update_request(request: UpdateRequest) {
	const { manifest, update_package, build_id } = request;
	
	try {
		const temp_dir = './temp';
		const temp_archive = path.join(temp_dir, 'build_archive.zip');
		const temp_files = path.join(temp_dir, 'files');
		const manifest_path = `./wow.export/v2/update/manifest/${build_id}.json`;
		const dist_path = `./wow.export/v2/update/dist/${build_id}`;

		await fs.mkdir(temp_dir, { recursive: true });

		log(`downloading update package from {${update_package}}`);
		const response = await fetch(update_package);
		if (!response.ok)
			throw new Error(`Failed to download update package: ${response.status} ${response.statusText}`);

		await Bun.write(temp_archive, response);

		log(`blanking existing manifest at {${manifest_path}}`);
		await fs.mkdir(path.dirname(manifest_path), { recursive: true });
		await fs.writeFile(manifest_path, '[]');

		log(`extracting archive to {${temp_files}}`);
		await extract_zip(temp_archive, temp_files);

		log(`removing existing distribution folder {${dist_path}}`);
		await fs.rm(dist_path, { recursive: true, force: true });

		log(`moving extracted files to {${dist_path}}`);
		await fs.mkdir(path.dirname(dist_path), { recursive: true });
		await fs.rename(temp_files, dist_path);

		log(`writing new manifest to {${manifest_path}}`);
		await fs.writeFile(manifest_path, JSON.stringify(manifest, null, 2));

		log(`cleaning up temporary files`);
		await fs.rm(temp_dir, { recursive: true, force: true });

		log(`successfully updated build {${build_id}}`);
	} catch (error) {
		caution('wow_export build update failed', { error: error instanceof Error ? error.message : String(error) });
		throw error;
	}
}

async function process_queue() {
	if (is_processing_update || update_queue.length === 0)
		return;

	is_processing_update = true;
	log(`processing update queue ({${update_queue.length}} requests pending)`);

	while (update_queue.length > 0) {
		const request = update_queue.shift()!;
		log(`processing update for build {${request.build_id}}`);
		
		try {
			await process_update_request(request);
		} catch (error) {
			log(`failed to process update for build {${request.build_id}}: ${error instanceof Error ? error.message : String(error)}`);
		}
	}

	is_processing_update = false;
	log(`update queue processing complete`);
}

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

	server.route('/wow.export/data/dbd', async (req, url) => {
		const def = url.searchParams.get('def');
		if (def === null)
			return 404;

		const dbd = await fetch(`https://raw.githubusercontent.com/wowdev/WoWDBDefs/master/definitions/${def}.dbd`);
		return new Response(dbd.body, {
			status: dbd.status
		});
	});

	server.dir('/wow.export/data', './wow.export/data');
	server.dir('/wow.export/static', './wow.export/static');
	server.dir('/wow.export/update', './wow.export/update');
	server.dir('/wow.export/download', './wow.export/download');

	schedule_update();

	server.websocket('/wow.export/v2/trigger_update/:build/:msize/:csize', {
		accept(req, url) {
			if (is_uploading)
				return false;

			const key = req.headers.get('authorization');
			const expected_key = process.env.WOW_EXPORT_V2_UPDATE_KEY;
			if (!expected_key || key !== expected_key)
				return false;

			const build = url.searchParams.get('build');
			const m_size = Number(url.searchParams.get('msize'));
			const c_size = Number(url.searchParams.get('csize'));

			if (!build || isNaN(m_size) || m_size <= 0 || isNaN(c_size) || c_size <= 0)
				return false;

			const t_size = m_size + c_size;
			console.log({ t_size, MAX_UPDATE_SIZE });
			if (t_size >= MAX_UPDATE_SIZE)
				return false;

			return { build, m_size, c_size, t_size };
		},

		async open(ws) {
			try {
				 // @ts-ignore
				const { build, m_size, c_size, t_size } = ws.data;

				is_uploading = true;

				const temp_file = path.join(os.tmpdir(), `wow_export_upload_${crypto.randomUUID()}.tmp`);
				const file_handle = Bun.file(temp_file).writer();

				current_upload = {
					build,
					m_size,
					c_size,
					t_size,
					temp_file,
					bytes_written: 0,
					file_handle
				};

				log(`websocket upload started for build {${build}} (${t_size} bytes expected)`);
			} catch (error) {
				log(`failed to initialize upload session: ${error instanceof Error ? error.message : String(error)}`);
				await cleanup_upload_session();
				ws.close(1011, 'Failed to initialize upload');
			}
		},

		async message(ws, message) {
			if (!current_upload) {
				ws.close(1002, 'No upload session active');
				return;
			}

			try {
				const chunk = new Uint8Array(message as unknown as ArrayBuffer);
				const chunk_size = chunk.length;

				if (current_upload.bytes_written + chunk_size > current_upload.t_size) {
					ws.close(1002, 'Upload exceeds expected size');
					await cleanup_upload_session();
					return;
				}

				current_upload.file_handle.write(chunk);
				current_upload.bytes_written += chunk_size;

				const mb_written = Math.floor(current_upload.bytes_written / (1024 * 1024));
				const mb_total = Math.floor(current_upload.t_size / (1024 * 1024));
				if (mb_written > 0 && current_upload.bytes_written % (10 * 1024 * 1024) < chunk_size)
					log(`websocket upload progress: ${mb_written} MB / ${mb_total} MB`);

				if (current_upload.bytes_written === current_upload.t_size) {
					await current_upload.file_handle.end();
					await process_websocket_upload(ws);
				}
			} catch (error) {
				log(`websocket upload error: ${error instanceof Error ? error.message : String(error)}`);
				ws.close(1011, 'Upload processing failed');
				await cleanup_upload_session();
			}
		},

		async close(ws, code, reason) {
			if (current_upload) {
				log(`websocket connection closed during upload (code: ${code}, reason: ${reason})`);
				await cleanup_upload_session();
			}
		}
	});

}