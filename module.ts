import { http_serve, caution, parse_template, HTTP_STATUS_CODE, ErrorWithMetadata } from 'spooder';
import crypto from 'node:crypto';
import os from 'node:os';
import fs from 'node:fs/promises';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { ColorInput } from 'bun';
import { db } from './db';
import { blte_unpack } from './casc/blte';
import { tact_load_keys } from './casc/tact';
import { bucket } from './obj_rds';

const LISTFILE_HASH_THRESHOLD = 100;

const LISTFILE_TYPES = [
	{ name: 'main', extensions: [] },
	{ name: 'models', extensions: ['.m2', '.m3', '.wmo'] },
	{ name: 'textures', extensions: ['.blp'] },
	{ name: 'sounds', extensions: ['.ogg', '.mp3', '.unk_sound'] },
	{ name: 'videos', extensions: ['.avi'] },
	{ name: 'text', extensions: ['.txt', '.lua', '.xml', '.sbt', '.wtf', '.htm', '.toc', '.xsd', '.srt'] },
	{ name: 'fonts', extensions: ['.ttf'] }
] as const;

const LISTFILE_EXT: Record<string, number> = {};
for (let i = 0; i < LISTFILE_TYPES.length; i++) {
	for (const ext of LISTFILE_TYPES[i].extensions)
		LISTFILE_EXT[ext] = i;
}

const LISTFILE_MODEL_FILTER = /(_\d\d\d_)|(_\d\d\d\.wmo$)|(lod\d\.wmo$)/;

type SpooderServer = ReturnType<typeof http_serve>;

const ANSI_RESET = '\x1b[0m';
function log(message: string, color: ColorInput = 'orange'): void {
	const ansi = Bun.color(color, 'ansi-256');
	process.stdout.write(`[{wow.export}] > ${message}\n`.replace(/\{([^}]+)\}/g, `${ansi}$1${ANSI_RESET}`));
}

// region kino
const kino_bucket = bucket('wow.export.kino', process.env.KINO_CDN_SECRET!);

type Hash = string; // 32-char hex

type ArchiveRef = { key: Hash; ofs: number; len: number };
type Media = { enc: Hash; arc?: ArchiveRef };
type Subtitle = Media & { type: number };

type KinoQueueEntry = {
	vid: Media;
	aud?: Media;
	srt?: Subtitle;
};

const kino_queue = new Map<Hash, KinoQueueEntry>();
let kino_processing: Hash | null = null;

function kino_is_valid_hash(str: string): boolean {
	return /^[a-f0-9]{32}$/.test(str);
}

function kino_cache_key(vid: Media, aud?: Media, srt?: Subtitle): string {
	const composite = vid.enc + (aud?.enc ?? '') + (srt?.enc ?? '');
	return crypto.createHash('md5').update(composite).digest('hex');
}

function kino_is_valid_archive_ref(obj: any): obj is ArchiveRef {
	return (
		typeof obj === 'object' &&
		obj !== null &&
		'key' in obj &&
		'ofs' in obj &&
		'len' in obj &&
		typeof obj.key === 'string' &&
		kino_is_valid_hash(obj.key) &&
		Number.isInteger(obj.ofs) &&
		Number.isInteger(obj.len) &&
		obj.ofs >= 0 &&
		obj.len >= 0 &&
		Object.keys(obj).length === 3
	);
}

function kino_is_valid_entry(obj: any): obj is Media {
	if (typeof obj !== 'object' || obj === null)
		return false;

	if (!('enc' in obj) || typeof obj.enc !== 'string' || !kino_is_valid_hash(obj.enc))
		return false;

	if ('arc' in obj) {
		if (!kino_is_valid_archive_ref(obj.arc))
			return false;
		return Object.keys(obj).length === 2;
	}

	return Object.keys(obj).length === 1;
}

function kino_is_valid_subtitle(obj: any): obj is Subtitle {
	if (typeof obj !== 'object' || obj === null)
		return false;

	if (!('enc' in obj) || typeof obj.enc !== 'string' || !kino_is_valid_hash(obj.enc))
		return false;

	if (!('type' in obj) || !Number.isInteger(obj.type))
		return false;

	if ('arc' in obj) {
		if (!kino_is_valid_archive_ref(obj.arc))
			return false;
		return Object.keys(obj).length === 3;
	}

	return Object.keys(obj).length === 2;
}

// casc cdn
const CASC_VERSION_URL = (region: string) => process.env.VERSION_URL!.replace('%s', region);
const CASC_CDN_REGIONS = process.env.CDN_REGIONS!.split(',');
const CASC_PRODUCT = 'wow';

type CASCCDNServerEntry = {
	Name: string;
	Path: string;
	Hosts: string;
};

type CASCVersionEntry = {
	Region: string;
};

let casc_cdn_hosts: string[] = [];
let casc_cdn_path: string = '';
let casc_ready: Promise<void> | null = null;

function casc_url_join(...parts: string[]): string {
	return parts.join('/').replace(/(?<!:)\/\/+/g, '/');
}

function casc_format_key(key: string): string {
	return `${key.substring(0, 2)}/${key.substring(2, 4)}/${key}`;
}

function casc_parse_version_config<T>(data: string): T[] {
	const entries: T[] = [];
	const lines = data.split(/\r?\n/);

	const headerLine = lines.shift();
	if (headerLine === undefined)
		return entries;

	const headers = headerLine.split('|');
	const fields = new Array(headers.length);

	for (let i = 0, n = headers.length; i < n; i++)
		fields[i] = headers[i]!.split('!')[0]!.replace(' ', '');

	for (const entry of lines) {
		if (entry.trim().length === 0 || entry.startsWith('#'))
			continue;

		const node: Record<string, string> = {};
		const entryFields = entry.split('|');
		for (let i = 0, n = entryFields.length; i < n; i++)
			node[fields[i]!] = entryFields[i]!;

		entries.push(node as T);
	}

	return entries;
}

async function casc_detect_version_server(): Promise<{ tag: string; url: string }> {
	const promises = CASC_CDN_REGIONS.map(tag => {
		const url = CASC_VERSION_URL(tag);
		return fetch(url).then(() => ({ tag, url }));
	});

	return await Promise.any(promises);
}

async function casc_init(): Promise<void> {
	log('initializing {casc} CDN client');

	const tact_promise = tact_load_keys();

	const version_server = await casc_detect_version_server();
	log(`casc using {${version_server.tag}} version server`);

	const product_url = casc_url_join(version_server.url, CASC_PRODUCT);

	const [cdns_res, versions_res] = await Promise.all([
		fetch(casc_url_join(product_url, 'cdns')),
		fetch(casc_url_join(product_url, 'versions'))
	]);

	if (!cdns_res.ok)
		throw new ErrorWithMetadata('casc: failed to fetch CDN servers', { status: cdns_res.status });

	if (!versions_res.ok)
		throw new ErrorWithMetadata('casc: failed to fetch versions', { status: versions_res.status });

	const cdn_servers = casc_parse_version_config<CASCCDNServerEntry>(await cdns_res.text());
	const versions = casc_parse_version_config<CASCVersionEntry>(await versions_res.text());

	const cdn_config = cdn_servers.find(e => e.Name === version_server.tag);
	if (!cdn_config)
		throw new ErrorWithMetadata('casc: CDN config missing for region', { region: version_server.tag });

	const version = versions.find(e => e.Region === version_server.tag);
	if (!version)
		throw new ErrorWithMetadata('casc: version missing for region', { region: version_server.tag });

	casc_cdn_path = cdn_config.Path;

	// ping hosts to find fastest
	const hosts = cdn_config.Hosts.split(' ');
	log(`casc pinging {${hosts.length}} CDN hosts`);

	const ping_results = await Promise.all(hosts.map(async host => {
		const url = `https://${host}`;
		const start = performance.now();

		try {
			await fetch(url, { method: 'HEAD' });
			return { host, time: performance.now() - start };
		} catch {
			return null;
		}
	}));

	const valid_hosts = ping_results.filter((r): r is { host: string; time: number } => r !== null);
	if (valid_hosts.length === 0)
		throw new ErrorWithMetadata('casc: all CDN hosts failed ping', { hosts });

	valid_hosts.sort((a, b) => a.time - b.time);
	casc_cdn_hosts = valid_hosts.map(r => r.host);

	log(`casc CDN host priority: {${casc_cdn_hosts.join(', ')}}`);

	await tact_promise;
	log('casc CDN client {ready}');
}

async function casc_download(path: string, offset?: number, length?: number): Promise<Response> {
	const failed_hosts: string[] = [];

	for (const host of casc_cdn_hosts) {
		const url = casc_url_join(`https://${host}`, path);

		try {
			const headers: Record<string, string> = {};
			if (offset !== undefined && length !== undefined)
				headers.Range = `bytes=${offset}-${offset + length - 1}`;

			const res = await fetch(url, { headers });

			if (res.ok) {
				// re-order to deprioritize failed hosts
				if (failed_hosts.length > 0) {
					casc_cdn_hosts = casc_cdn_hosts.filter(h => !failed_hosts.includes(h));
					casc_cdn_hosts.push(...failed_hosts);
				}
				return res;
			}

			failed_hosts.push(host);
		} catch {
			failed_hosts.push(host);
		}
	}

	throw new ErrorWithMetadata('casc: all CDN hosts failed', { path, failed_hosts });
}

async function casc_download_data(key: string, offset?: number, length?: number): Promise<Response> {
	return casc_download(casc_url_join(casc_cdn_path, 'data', casc_format_key(key)), offset, length);
}

async function casc_get_file(media: Media): Promise<ArrayBuffer> {
	const res = media.arc
		? await casc_download_data(media.arc.key, media.arc.ofs, media.arc.len)
		: await casc_download_data(media.enc);

	const data = await res.arrayBuffer();
	return blte_unpack(data, media.enc, false);
}

async function kino_process_video(entry: KinoQueueEntry, cache_key: string): Promise<void> {
	const { vid, aud, srt } = entry;
	const temp_dir = path.join(os.tmpdir(), `kino_${cache_key}`);

	try {
		await casc_ready;
		await fs.mkdir(temp_dir, { recursive: true });

		const video_path = path.join(temp_dir, vid.enc);
		const video_data = await casc_get_file(vid);
		await Bun.write(video_path, video_data);

		let audio_path: string | undefined;
		if (aud !== undefined) {
			audio_path = path.join(temp_dir, aud.enc);
			const audio_data = await casc_get_file(aud);
			await Bun.write(audio_path, audio_data);
		}

		let srt_path: string | undefined;
		if (srt !== undefined && srt.type === 118) {
			srt_path = path.join(temp_dir, srt.enc);
			const srt_data = await casc_get_file(srt);
			await Bun.write(srt_path, srt_data);
		}

		const output_path = path.join(temp_dir, `${cache_key}.mp4`);
		const ffmpeg_args: string[] = ['-i', video_path];

		if (audio_path !== undefined)
			ffmpeg_args.push('-i', audio_path);

		if (srt_path !== undefined)
			ffmpeg_args.push('-i', srt_path);

		ffmpeg_args.push('-c:v', 'copy');

		if (audio_path !== undefined)
			ffmpeg_args.push('-c:a', 'libopus', '-b:a', '128k');

		if (srt_path !== undefined)
			ffmpeg_args.push('-c:s', 'mov_text');

		if (audio_path !== undefined)
			ffmpeg_args.push('-shortest');

		ffmpeg_args.push(output_path);

		await new Promise<void>((resolve, reject) => {
			const ffmpeg = spawn('ffmpeg', ffmpeg_args);

			ffmpeg.on('close', (code) => {
				if (code === 0)
					resolve();
				else
					reject(new Error(`ffmpeg exited with code ${code}`));
			});

			ffmpeg.on('error', reject);
		});

		const upload_result = await kino_bucket.upload(Bun.file(output_path), {
			object_id: cache_key,
			content_type: 'video/mp4'
		});

		if (upload_result === null)
			throw new Error('failed to upload to CDN');

		await db`INSERT INTO kino_cached (enc) VALUES (${cache_key})`;
	} catch (e) {
		caution('kino: failed to process video', { error: e, vid, aud, srt });
	} finally {
		await fs.rm(temp_dir, { recursive: true, force: true });
	}
}

async function kino_process_queue(): Promise<void> {
	if (kino_processing !== null || kino_queue.size === 0)
		return;

	while (kino_queue.size > 0) {
		const [cache_key, entry] = kino_queue.entries().next().value!;
		kino_queue.delete(cache_key);
		kino_processing = cache_key;

		log(`kino processing {${cache_key}} (${kino_queue.size} remaining in queue)`);
		await kino_process_video(entry, cache_key);
	}

	kino_processing = null;
}
// endregion

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

interface ListfileEntry {
	id: number;
	filename: string;
	name_bytes: Uint8Array;
	string_offset: number;
	pf_index: number;
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

function b_listfile_categorize_entries(entries: ListfileEntry[]): ListfileEntry[][] {
	const categorized: ListfileEntry[][] = Array.from({ length: LISTFILE_TYPES.length }, () => []);

	for (const entry of entries) {
		const ext = path.extname(entry.filename);
		let pf_index = LISTFILE_EXT[ext] ?? 0;

		// omit WMO group files from model category
		if (pf_index === 1 && ext === '.wmo' && LISTFILE_MODEL_FILTER.test(entry.filename))
			pf_index = 0;

		entry.pf_index = pf_index;
		categorized[pf_index].push(entry);
	}

	return categorized;
}

function b_listfile_parse_entries(csv_content: string): ListfileEntry[] {
	const entries: ListfileEntry[] = [];

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

		const filename = tokens[1].trim().toLowerCase();
		const name_bytes = new TextEncoder().encode(filename);

		entries.push({
			id: file_data_id,
			filename,
			name_bytes,
			string_offset: 0,
			pf_index: 0
		});
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
	await b_listfile_write_categorized_files(target_dir, entries);
	await b_listfile_write_index(target_dir, entries);
	await b_listfile_write_tree(target_dir, tree);
	await b_listfile_write_fat_files(target_dir, entries);
}

async function b_listfile_write_categorized_files(target_dir: string, entries: ListfileEntry[]): Promise<void> {
	const categorized = b_listfile_categorize_entries(entries);

	for (let pf_index = 0; pf_index < categorized.length; pf_index++) {
		const category_entries = categorized[pf_index];
		const sorted_entries = [...category_entries].sort((a, b) => a.id - b.id);

		// calculate size: entry_count (4 bytes) + sum of (fileDataID (4 bytes) + filename + null terminator)
		let total_size = 4; // entry_count
		for (const entry of sorted_entries)
			total_size += 4 + entry.name_bytes.length + 1;

		const buffer = new ArrayBuffer(total_size);
		const view = new DataView(buffer);
		const bytes_view = new Uint8Array(buffer);

		view.setUint32(0, sorted_entries.length, false);
		let write_pos = 4;

		for (const entry of sorted_entries) {
			view.setUint32(write_pos, entry.id, false);
			write_pos += 4;
			entry.string_offset = write_pos;

			bytes_view.set(entry.name_bytes, write_pos);
			write_pos += entry.name_bytes.length;
			bytes_view[write_pos++] = 0;
		}

		const type_name = LISTFILE_TYPES[pf_index].name;
		const file_name = pf_index === 0 ? 'listfile-strings.dat' : `listfile-pf-${type_name}.dat`;
		const file_path = path.join(target_dir, file_name);
		await Bun.write(file_path, buffer);

		log(`wrote {${file_name}} with {${sorted_entries.length}} entries`);
	}
}

async function b_listfile_write_index(target_dir: string, entries: ListfileEntry[]): Promise<void> {
	const sorted_entries = [...entries].sort((a, b) => a.id - b.id);

	// format: [id:4][stringOffset:4][pf_index:1] = 9 bytes per entry
	const buffer = new ArrayBuffer(sorted_entries.length * 9);
	const view = new DataView(buffer);
	let index_pos = 0;

	for (const entry of sorted_entries) {
		view.setUint32(index_pos, entry.id, false);
		view.setUint32(index_pos + 4, entry.string_offset, false);
		view.setUint8(index_pos + 8, entry.pf_index);
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

	actual_children.sort((a, b) => {
		const hash_a = Bun.hash.xxHash64(a[0]);
		const hash_b = Bun.hash.xxHash64(b[0]);
		return hash_a < hash_b ? -1 : hash_a > hash_b ? 1 : 0;
	});

	const is_large_dir = files.length > LISTFILE_HASH_THRESHOLD;
	if (is_large_dir) {
		files.sort((a, b) => {
			const hash_a = Bun.hash.xxHash64(a.filename);
			const hash_b = Bun.hash.xxHash64(b.filename);
			return hash_a < hash_b ? -1 : hash_a > hash_b ? 1 : 0;
		});
	}

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
		// large directory: use hashes
		for (const file of files) {
			const filename_hash = Bun.hash.xxHash64(file.filename);
			view.setBigUint64(pos, filename_hash, false); pos += 8;
			view.setUint32(pos, file.fileId, false); pos += 4;
		}
	} else {
		// small directory: store filenames directly
		for (const file of files) {
			const filename_bytes = new TextEncoder().encode(file.filename);
			view.setUint16(pos, filename_bytes.length, false); pos += 2;
			new Uint8Array(node_buffer, pos, filename_bytes.length).set(filename_bytes);
			pos += filename_bytes.length;
			view.setUint32(pos, file.fileId, false); pos += 4;
		}
	}

	node_data.push(node_buffer);

	const child_offset_map: Array<{ hash: bigint; offset: number }> = [];
	for (const [child_name, child_node] of actual_children) {
		const child_ofs = b_listfile_serialize_tree_node(child_node.children, node_data, component_idx, child_name);
		const child_hash = Bun.hash.xxHash64(child_name);
		child_offset_map.push({ hash: child_hash, offset: child_ofs });
	}

	let child_entry_pos = child_entries_start;
	for (const { hash, offset } of child_offset_map) {
		view.setBigUint64(child_entry_pos, hash, false);
		view.setUint32(child_entry_pos + 8, offset, false);
		child_entry_pos += 12;
	}

	return current_ofs;
}

async function b_listfile_write_fat_files(target_dir: string, entries: ListfileEntry[]): Promise<void> {
	const sorted_entries = [...entries].sort((a, b) => a.id - b.id);

	// fat strings file (no IDs, just concatenated null-terminated strings)
	let total_size = 0;
	for (const entry of sorted_entries)
		total_size += entry.name_bytes.length + 1;

	const strings_buffer = new ArrayBuffer(total_size);
	const strings_view = new Uint8Array(strings_buffer);
	let write_pos = 0;

	for (const entry of sorted_entries) {
		entry.string_offset = write_pos; // store offset for index
		strings_view.set(entry.name_bytes, write_pos);
		write_pos += entry.name_bytes.length;
		strings_view[write_pos++] = 0;
	}

	const strings_path = path.join(target_dir, 'listfile-strings-fat.dat');
	await Bun.write(strings_path, strings_buffer);
	log(`wrote {listfile-strings-fat.dat} with {${sorted_entries.length}} entries`);

	// fat index (no pf_index, just [id:4][stringOffset:4] = 8 bytes per entry)
	const index_buffer = new ArrayBuffer(sorted_entries.length * 8);
	const index_view = new DataView(index_buffer);
	let index_pos = 0;

	for (const entry of sorted_entries) {
		index_view.setUint32(index_pos, entry.id, false);
		index_view.setUint32(index_pos + 4, entry.string_offset, false);
		index_pos += 8;
	}

	const index_path = path.join(target_dir, 'listfile-id-index-fat.dat');
	await Bun.write(index_path, index_buffer);
	log(`wrote {listfile-id-index-fat.dat} with {${sorted_entries.length}} entries`);
}

async function update_listfile() {
	const target_dir = './wow.export/data/listfile';
	const version_file = path.join(target_dir, 'version.json');

	let should_update = false;
	let remote_head: any = null;

	try {
		const version_res = await fetch('https://api.github.com/repos/wowdev/wow-listfile/git/refs/heads/master');
		if (!version_res.ok) {
			caution('Failed to check listfile version', { status: version_res.status });
			return;
		}

		remote_head = await version_res.json();
		const remote_sha = remote_head?.object?.sha;

		if (!remote_sha) {
			caution('Failed to parse listfile version', { remote_head });
			return;
		}

		try {
			const local_version = await Bun.file(version_file).json();
			const local_sha = local_version?.object?.sha;

			if (local_sha === remote_sha) {
				log(`listfile is up to date ({${remote_sha.substring(0, 8)}})`);
				return;
			}

			log(`listfile update available: {${local_sha?.substring(0, 8)}} -> {${remote_sha.substring(0, 8)}}`);
			should_update = true;
		} catch (e) {
			// version.json doesn't exist or is invalid, proceed with update
			log('no local listfile version found, proceeding with update');
			should_update = true;
		}
	} catch (error) {
		caution('Failed to check listfile version', [error instanceof Error ? error.message : String(error)]);
		return;
	}

	if (!should_update)
		return;

	const url = 'https://github.com/wowdev/wow-listfile/releases/latest/download/community-listfile.csv';
	await download_and_store(url, target_dir, 'master', 'listfile');

	try {
		await b_listfile_build(target_dir);

		await Bun.write(version_file, JSON.stringify(remote_head, null, 2));
		log(`saved listfile version to {${version_file}}`);
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

	casc_ready = casc_init();

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

			const tmp_package_path = package_out_path + package_basename + '.tmp';
			await stream_to_file(json.package_url, tmp_package_path, 'archive');
			await fs.rename(tmp_package_path, package_file_path);

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

	let is_updating_listfile = false;
	let is_updating_tact = false;

	async function trigger_listfile_update() {
		if (is_updating_listfile)
			return;

		is_updating_listfile = true;
		await update_listfile();
		is_updating_listfile = false;
	}

	trigger_listfile_update(); // update listfile on server start

	server.webhook(process.env.LISTFILE_WEBHOOK_SECRET!, '/wow.export/v2/trigger_listfile_rebuild', (payload) => {
		setImmediate(trigger_listfile_update);
		return 200;
	});

	server.webhook(process.env.LISTFILE_WEBHOOK_SECRET!, '/wow.export/v2/trigger_tact_rebuild', (payload) => {
		setImmediate(async () => {
			if (is_updating_tact)
				return;

			is_updating_tact = true;
			await update_tact();
			is_updating_tact = false;
		});
		return 200;
	});

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

	// kino endpoint
	server.json('/wow.export/v2/get_video', async (req, url, json) => {
		if (!req.headers.get('user-agent')?.startsWith('wow.export '))
			return HTTP_STATUS_CODE.Forbidden_403;

		const vid = json.vid;
		if (!kino_is_valid_entry(vid))
			return HTTP_STATUS_CODE.BadRequest_400;

		const aud = json.aud;
		if (aud !== undefined && !kino_is_valid_entry(aud))
			return HTTP_STATUS_CODE.BadRequest_400;

		const srt = json.srt;
		if (srt !== undefined && !kino_is_valid_subtitle(srt))
			return HTTP_STATUS_CODE.BadRequest_400;

		const cache_key = kino_cache_key(vid, aud, srt);

		// check if already cached
		const cached = await db`SELECT 1 AS cached FROM kino_cached WHERE enc = ${cache_key}`;
		if (cached.length) {
			return {
				url: kino_bucket.presign(cache_key)
			};
		}

		// check if already queued or processing
		if (kino_queue.has(cache_key) || kino_processing === cache_key)
			return HTTP_STATUS_CODE.Accepted_202;

		// add to queue and trigger processing
		kino_queue.set(cache_key, { vid, aud, srt });
		setImmediate(kino_process_queue);

		return HTTP_STATUS_CODE.Accepted_202;
	});
}