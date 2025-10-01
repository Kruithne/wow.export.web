import { http_serve, caution, HTTP_STATUS_CODE } from 'spooder';
import crypto from 'node:crypto';
import os from 'node:os';
import fs from 'node:fs/promises';
import path from 'node:path';
import { ColorInput } from 'bun';

const UPDATE_TIMER = 24 * 60 * 60 * 1000; // 24 hours

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

interface TriggerUpdateRequest {
	build_tag: string;
	json: any;
}

let trigger_update_queue: TriggerUpdateRequest[] = [];
let is_processing_trigger_update = false;

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
		if (def === null) {
			const api_response = await fetch('https://raw.githubusercontent.com/wowdev/WoWDBDefs/refs/heads/master/manifest.json');
			return new Response(api_response.body, {
				status: api_response.status,
				headers: {
					'Content-Type': 'application/json'
				}
			});
		}

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

	async function stream_to_file(url: string, file_path: string, name: string): Promise<void> {
		const response = await fetch(url);
		if (!response.ok)
			throw new Error(`Failed to fetch ${name}: ${response.status} ${response.statusText}`);

		const content_length = response.headers.get('content-length');
		const total_size = content_length ? parseInt(content_length, 10) : null;

		if (!total_size || !response.body)
			throw new Error(`failed to read download stream`);

		const reader = response.body.getReader();
		const file_handle = Bun.file(file_path);
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

				if (total_size > 50 * 1024 * 1024) { // log in 50MB chunks
					const percent = Math.floor((downloaded_bytes / total_size) * 100);
					if (percent >= last_logged_percent + 10 && percent <= 100) {
						const downloaded_mb = (downloaded_bytes / (1024 * 1024)).toFixed(1);
						const total_mb = (total_size / (1024 * 1024)).toFixed(1);

						log(`downloading ${name}: ${percent}% (${downloaded_mb} MB / ${total_mb} MB)`);
						last_logged_percent = percent;
					}
				}
			}
		} finally {
			await writer.end();
			reader.releaseLock();
		}
	}

	async function process_trigger_update(build_tag: string, json: any) {
		try {
			log(`accepting update for ${build_tag}`);

			const update_out_path = `./wow.export/update/${build_tag}/`;
			const package_out_path = `./wow.export/download/${build_tag}/`;
			
			await fs.mkdir(update_out_path, { recursive: true });
			await fs.mkdir(package_out_path, { recursive: true });

			// update file
			log(`downloading update file from ${json.update_url}`);
			const tmp_path = update_out_path + 'update.tmp';
			await stream_to_file(json.update_url, tmp_path, 'update file');

			// manifest file
			log(`downloading manifest from ${json.manifest_url}`);
			const tmp_manifest_path = update_out_path + 'update.json.tmp';
			await stream_to_file(json.manifest_url, tmp_manifest_path, 'manifest');

			// move new update into place
			await fs.rename(tmp_path, update_out_path + 'update');
			await fs.rename(tmp_manifest_path, update_out_path + 'update.json');
			
			// package archive
			log(`downloading archive file from ${json.package_url}`);
			const package_basename = path.basename(json.package_url);
			const package_file_path = path.join(package_out_path, package_basename);
			await stream_to_file(json.package_url, package_file_path, 'archive');
			
			log(`successfully updated ${build_tag}`);
		} catch (e) {
			caution('wow.export update failed', { e, build_tag, json });
		}
	}

	async function process_trigger_update_queue() {
		if (is_processing_trigger_update || trigger_update_queue.length === 0)
			return;

		is_processing_trigger_update = true;
		log(`processing trigger update queue (${trigger_update_queue.length} requests pending)`);

		while (trigger_update_queue.length > 0) {
			const request = trigger_update_queue.shift()!;
			log(`processing trigger update for build ${request.build_tag}`);
			
			await process_trigger_update(request.build_tag, request.json);
		}

		is_processing_trigger_update = false;
		log(`trigger update queue processing complete`);
	}

	function trigger_update(build_tag: string, json: any) {
		trigger_update_queue.push({ build_tag, json });
		log(`queued trigger update for ${build_tag} (${trigger_update_queue.length} in queue)`);
		process_trigger_update_queue();
	}

	server.json('/wow.export/v2/trigger_update/:build', async (req, url, json) => {
		const key = req.headers.get('authorization');
		const expected_key = process.env.WOW_EXPORT_V2_UPDATE_KEY;

		if (!expected_key || key !== expected_key)
			return HTTP_STATUS_CODE.Unauthorized_401;

		const build_tag = url.searchParams.get('build');
		if (build_tag === null)
			return HTTP_STATUS_CODE.BadRequest_400;

		if (typeof json.update_url !== 'string' || typeof json.package_url !== 'string' || typeof json.manifest_url !== 'string')
			return HTTP_STATUS_CODE.BadRequest_400;

		trigger_update(build_tag, json);
		return HTTP_STATUS_CODE.Accepted_202;
	});

	server.json('/wow.export/v2/test', (req, url, json) => {
		return { 'foo': 42 };
	});
}