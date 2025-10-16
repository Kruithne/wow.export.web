import { http_serve, caution, parse_template, HTTP_STATUS_CODE } from 'spooder';
import crypto from 'node:crypto';
import os from 'node:os';
import fs from 'node:fs/promises';
import path from 'node:path';
import { ColorInput } from 'bun';

const UPDATE_TIMER = 24 * 60 * 60 * 1000; // 24 hours
const LISTFILE_HASH_THRESHOLD = 100;

const LISTFILE_FLAGS = {
	TEXTURE: { flag: 0x01, extensions: ['.blp'] },
	SOUND: { flag: 0x02, extensions: ['.ogg', '.mp3', '.unk_sound'] },
	MODEL: { flag: 0x04, extensions: ['.m2', '.m3', '.wmo'] },
	VIDEO: { flag: 0x08, extensions: ['.avi'] },
	TEXT: { flag: 0x10, extensions: ['.txt', '.lua', '.xml', '.sbt', '.wtf', '.htm', '.toc', '.xsd'] }
} as const;

const LISTFILE_MODEL_FILTER = /_[0-9]{3}\.wmo$/;

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
	update_listfile(); // todo: remove me.
	setTimeout(update_data_files, UPDATE_TIMER);
}

interface ListfileEntry {
	id: number;
	filename: string;
	name_bytes: Uint8Array;
	string_offset: number;
	flags: number;
}

interface TreeNode {
	children: Map<string, TreeNode>;
	files: Array<{ filename: string; fileId: number }>;
}

async function b_listfile_build(target_dir: string): Promise<void> {
	log('build_binary_listfiles :: start');

	const master_file = path.join(target_dir, 'master');
	const csv_content = await Bun.file(master_file).text();

	const entries = b_listfile_parse_entries(csv_content);
	log(`b_listfile_parse_entries :: parsed {${entries.length}} entries`);

	const tree = b_listfile_build_tree(entries);
	log('b_listfile_build_tree :: done');

	await b_listfile_write_files(target_dir, entries, tree);
	log('b_listfile_write_files :: done');
}

function b_listfile_parse_entries(csv_content: string): ListfileEntry[] {
	const entries: ListfileEntry[] = [];
	let string_offset = 0;

	const lines = csv_content.split(/\r?\n/);
	for (const line of lines) {
		if (line.length === 0)
			continue;

		const tokens = line.split(';');
		if (tokens.length !== 2)
			continue;

		const file_data_id = Number(tokens[0]);
		if (isNaN(file_data_id))
			continue;

		const filename = tokens[1].toLowerCase();
		const name_bytes = new TextEncoder().encode(filename);

		let flags = 0;
		for (const category of Object.values(LISTFILE_FLAGS)) {
			for (const ext of category.extensions) {
				if (filename.endsWith(ext)) {
					// omit WMO group files from the model group
					if (category === LISTFILE_FLAGS.MODEL && ext === '.wmo' && LISTFILE_MODEL_FILTER.test(filename))
						continue;

					flags |= category.flag;
					break;
				}
			}
		}

		entries.push({
			id: file_data_id,
			filename,
			name_bytes,
			string_offset,
			flags
		});

		string_offset += name_bytes.length + 1;
	}

	return entries;
}

function b_listfile_build_tree(entries: ListfileEntry[]): Map<string, TreeNode> {
	const tree = new Map<string, TreeNode>();

	for (const entry of entries)
		b_listfile_add_node(tree, entry.filename, entry.id);

	return tree;
}

function b_listfile_add_node(tree: Map<string, TreeNode>, file_path: string, file_id: number): void {
	const components = file_path.split('/');
	let current_level = tree;

	for (let i = 0; i < components.length - 1; i++) {
		const component = components[i];

		if (!current_level.has(component)) {
			current_level.set(component, {
				children: new Map(),
				files: []
			});
		}

		current_level = current_level.get(component)!.children;
	}

	const filename = components[components.length - 1];
	if (!current_level.has('<files>')) {
		current_level.set('<files>', {
			children: new Map(),
			files: []
		});
	}

	current_level.get('<files>')!.files.push({ filename, fileId: file_id });
}

async function b_listfile_write_files(target_dir: string, entries: ListfileEntry[], tree: Map<string, TreeNode>): Promise<void> {
	await b_listfile_write_strings(target_dir, entries);
	await b_listfile_write_index(target_dir, entries);
	await b_listfile_write_tree(target_dir, tree);
}

async function b_listfile_write_strings(target_dir: string, entries: ListfileEntry[]): Promise<void> {
	const total_size = entries.reduce((sum, e) => sum + e.name_bytes.length + 1, 0);
	const buffer = new ArrayBuffer(total_size);
	const view = new Uint8Array(buffer);
	let write_pos = 0;

	for (const entry of entries) {
		view.set(entry.name_bytes, write_pos);
		write_pos += entry.name_bytes.length;
		view[write_pos++] = 0;
	}

	const file_path = path.join(target_dir, 'listfile-strings.dat');
	await Bun.write(file_path, buffer);
}

async function b_listfile_write_index(target_dir: string, entries: ListfileEntry[]): Promise<void> {
	const sorted_entries = [...entries].sort((a, b) => a.id - b.id);

	// format: [id:4][stringOffset:4][flags:1] = 9 bytes per entry
	const buffer = new ArrayBuffer(sorted_entries.length * 9);
	const view = new DataView(buffer);
	let index_pos = 0;

	for (const entry of sorted_entries) {
		view.setUint32(index_pos, entry.id, false);
		view.setUint32(index_pos + 4, entry.string_offset, false);
		view.setUint8(index_pos + 8, entry.flags);
		index_pos += 9;
	}

	const file_path = path.join(target_dir, 'listfile-id-index.dat');
	await Bun.write(file_path, buffer);
}

async function b_listfile_write_tree(target_dir: string, tree: Map<string, TreeNode>): Promise<void> {
	const node_data: ArrayBuffer[] = [];
	const component_idx = new Map<string, number>();

	b_listfile_serialize_tree_node(tree, node_data, component_idx, '<root>');

	const total_node_size = node_data.reduce((sum, buf) => sum + buf.byteLength, 0);
	const node_buffer = new ArrayBuffer(total_node_size);
	const node_view = new Uint8Array(node_buffer);
	let copy_pos = 0;

	for (const buffer of node_data) {
		node_view.set(new Uint8Array(buffer), copy_pos);
		copy_pos += buffer.byteLength;
	}

	const nodes_path = path.join(target_dir, 'listfile-tree-nodes.dat');
	await Bun.write(nodes_path, node_buffer);

	// format: [componentHash:8][nodeOffset:4] = 12 bytes per entry
	const idx_entries = Array.from(component_idx.entries());
	const idx_buffer = new ArrayBuffer(idx_entries.length * 12);
	const idx_view = new DataView(idx_buffer);
	let index_pos = 0;

	for (const [component, offset] of idx_entries) {
		const component_hash = Bun.hash.xxHash64(component);
		idx_view.setBigUint64(index_pos, component_hash, false);
		idx_view.setUint32(index_pos + 8, offset, false);
		index_pos += 12;
	}

	const idx_path = path.join(target_dir, 'listfile-tree-index.dat');
	await Bun.write(idx_path, idx_buffer);
}

function b_listfile_serialize_tree_node(node_map: Map<string, TreeNode>, node_data: ArrayBuffer[], component_idx: Map<string, number>, component_name: string): number {
	const current_ofs = node_data.reduce((sum, buf) => sum + buf.byteLength, 0);
	if (component_name !== '<root>')
		component_idx.set(component_name, current_ofs);

	const children = Array.from(node_map.entries());
	const files = children.find(([name]) => name === '<files>')?.[1]?.files || [];
	const actual_children = children.filter(([name]) => name !== '<files>');

	const is_large_dir = files.length > LISTFILE_HASH_THRESHOLD;

	const header_size = 9 + (actual_children.length * 12); // header + child entries
	let files_size = 0;

	if (is_large_dir) {
		files_size = files.length * 12; // hash + id per file
	} else {
		files_size = files.reduce((sum, f) => sum + 2 + new TextEncoder().encode(f.filename).length + 4, 0);
	}

	const node_buffer = new ArrayBuffer(header_size + files_size);
	const view = new DataView(node_buffer);
	let pos = 0;

	view.setUint32(pos, actual_children.length, false); pos += 4;
	view.setUint32(pos, files.length, false); pos += 4;
	view.setUint8(pos, is_large_dir ? 1 : 0); pos += 1;

	const child_entries_start = pos;
	pos += actual_children.length * 12;

	if (is_large_dir) {
		for (const file of files) {
			const filename_hash = Bun.hash.xxHash64(file.filename);
			view.setBigUint64(pos, filename_hash, false); pos += 8;
			view.setUint32(pos, file.fileId, false); pos += 4;
		}
	} else {
		for (const file of files) {
			const filename_bytes = new TextEncoder().encode(file.filename);
			view.setUint16(pos, filename_bytes.length, false); pos += 2;
			new Uint8Array(node_buffer, pos, filename_bytes.length).set(filename_bytes);
			pos += filename_bytes.length;
			view.setUint32(pos, file.fileId, false); pos += 4;
		}
	}

	node_data.push(node_buffer);

	let child_idx = 0;
	for (const [child_name, child_node] of actual_children) {
		const child_ofs = b_listfile_serialize_tree_node(child_node.children, node_data, component_idx, child_name);

		const child_entry_pos = child_entries_start + (child_idx * 12);
		const child_hash = Bun.hash.xxHash64(child_name);
		view.setBigUint64(child_entry_pos, child_hash, false);
		view.setUint32(child_entry_pos + 8, child_ofs, false);

		child_idx++;
	}

	return current_ofs;
}

async function update_listfile() {
	const url = 'https://github.com/wowdev/wow-listfile/releases/latest/download/community-listfile.csv';
	const target_dir = './wow.export/data/listfile';

	await download_and_store(url, target_dir, 'master', 'listfile');

	try {
		await b_listfile_build(target_dir);
	} catch (error) {
		caution('Failed to build binary listfiles', [error instanceof Error ? error.message : String(error)]);
	}
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

const RELEASE_BUILD_FILE = './wow.export/data/release_builds.json';

let trigger_update_queue: TriggerUpdateRequest[] = [];
let is_processing_trigger_update = false;

let release_builds: Record<string, string> = {}; // automatically populated

export async function init(server: SpooderServer) {
	try {
		release_builds = await Bun.file(RELEASE_BUILD_FILE).json();
	} catch (e) {
		log(`failed to load RELEASE_BUILD_FILE ${RELEASE_BUILD_FILE}: ${(e as Error).message}`);
	}

	server.route('/wow.export', async (req) => {
		if (index === null) {
			index = await Bun.file('./wow.export/index.html').text();
			index = await parse_template(index, release_builds, true);
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

			// update release builds
			log(`updating release build of {${build_tag}} to {${package_basename}}`);
			release_builds[build_tag] = package_basename;
			index = null; // force index re-render

			await Bun.write(RELEASE_BUILD_FILE, JSON.stringify(release_builds));
			
			log(`successfully updated {${build_tag}}`);
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
}