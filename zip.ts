import { mkdir } from 'node:fs/promises';
import { join, dirname, posix } from 'node:path';

const ENDSIG = 0x06054b50; // "PK\005\006"
const CENSIG = 0x02014b50; // "PK\001\002"
const LOCSIG = 0x04034b50; // "PK\003\004"

const STORED = 0; // no compression
const DEFLATED = 8; // deflate compression

const crc_table = new Uint32Array(256);
for (let i = 0; i < 256; i++) {
	let crc = i;
	for (let j = 0; j < 8; j++)
		crc = (crc & 1) ? (0xedb88320 ^ (crc >>> 1)) : (crc >>> 1);
	crc_table[i] = crc;
}

interface ZipEntry {
	entry_name: string;
	is_directory: boolean;
	compression_method: number;
	compressed_size: number;
	uncompressed_size: number;
	crc32: number;
	local_header_offset: number;
	file_data_offset: number;
}

interface EndOfCentralDirectory {
	central_directory_offset: number;
	central_directory_size: number;
	total_entries: number;
}

function read_uint32_le(buffer: Uint8Array, offset: number): number {
	return (buffer[offset] | (buffer[offset + 1] << 8) | (buffer[offset + 2] << 16) | (buffer[offset + 3] << 24)) >>> 0;
}

function read_uint16_le(buffer: Uint8Array, offset: number): number {
	return buffer[offset] | (buffer[offset + 1] << 8);
}

function find_end_of_central_directory(buffer: Uint8Array): EndOfCentralDirectory | null {
	const max_comment_size = 0xffff;
	const start_pos = Math.max(0, buffer.length - max_comment_size - 22);
	
	for (let i = buffer.length - 22; i >= start_pos; i--) {
		if (read_uint32_le(buffer, i) === ENDSIG) {
			const total_entries = read_uint16_le(buffer, i + 10);
			const central_directory_size = read_uint32_le(buffer, i + 12);
			const central_directory_offset = read_uint32_le(buffer, i + 16);
			
			return {
				central_directory_offset,
				central_directory_size,
				total_entries
			};
		}
	}
	
	return null;
}

function parse_central_directory(buffer: Uint8Array, eocd: EndOfCentralDirectory): ZipEntry[] {
	const entries: ZipEntry[] = [];
	let offset = eocd.central_directory_offset;
	
	for (let i = 0; i < eocd.total_entries; i++) {
		if (read_uint32_le(buffer, offset) !== CENSIG)
			throw new Error('Invalid central directory entry signature');
		
		const compression_method = read_uint16_le(buffer, offset + 10);
		const crc32 = read_uint32_le(buffer, offset + 16);
		const compressed_size = read_uint32_le(buffer, offset + 20);
		const uncompressed_size = read_uint32_le(buffer, offset + 24);
		const filename_length = read_uint16_le(buffer, offset + 28);
		const extra_field_length = read_uint16_le(buffer, offset + 30);
		const comment_length = read_uint16_le(buffer, offset + 32);
		const local_header_offset = read_uint32_le(buffer, offset + 42);
		
		const filename_start = offset + 46;
		const filename_bytes = buffer.slice(filename_start, filename_start + filename_length);
		const entry_name = new TextDecoder('utf-8').decode(filename_bytes);
		
		const is_directory = entry_name.endsWith('/');
		
		entries.push({
			entry_name,
			is_directory,
			compression_method,
			compressed_size,
			uncompressed_size,
			crc32,
			local_header_offset,
			file_data_offset: 0
		});
		
		offset += 46 + filename_length + extra_field_length + comment_length;
	}
	
	return entries;
}

function calculate_file_data_offset(buffer: Uint8Array, entry: ZipEntry): number {
	const local_header_offset = entry.local_header_offset;
	
	if (read_uint32_le(buffer, local_header_offset) !== LOCSIG)
		throw new Error('Invalid local header signature');
	
	const filename_length = read_uint16_le(buffer, local_header_offset + 26);
	const extra_field_length = read_uint16_le(buffer, local_header_offset + 28);
	
	return local_header_offset + 30 + filename_length + extra_field_length;
}

function crc32(data: Uint8Array): number {
	let crc = ~0;
	for (let i = 0; i < data.length; i++)
		crc = crc_table[(crc ^ data[i]) & 0xff] ^ (crc >>> 8);
	return ~crc >>> 0;
}

function decompress_data(compressed_data: Uint8Array, method: number, expected_size: number): Uint8Array {
	switch (method) {
		case STORED:
			return compressed_data;
		case DEFLATED:
			return new Uint8Array(Bun.inflateSync(compressed_data));
		default:
			throw new Error(`Unsupported compression method: ${method}`);
	}
}

function sanitize_path(base_path: string, entry_path: string): string {
	const normalized = posix.normalize(entry_path).replace(/\\/g, '/');
	const clean_path = normalized.startsWith('/') ? normalized.slice(1) : normalized;
	
	if (clean_path.includes('..'))
		throw new Error(`Path traversal attempt detected: ${entry_path}`);
	
	return join(base_path, clean_path);
}

async function extract_file(entry: ZipEntry, out_path: string, zip_data: Uint8Array): Promise<void> {
	const file_data_offset = calculate_file_data_offset(zip_data, entry);
	const compressed_data = zip_data.slice(file_data_offset, file_data_offset + entry.compressed_size);
	
	const decompressed_data = decompress_data(compressed_data, entry.compression_method, entry.uncompressed_size);
	
	const calculated_crc = crc32(decompressed_data);
	if (calculated_crc !== entry.crc32)
		throw new Error(`CRC32 mismatch for ${entry.entry_name}: expected ${entry.crc32}, got ${calculated_crc}`);
	
	const file_path = sanitize_path(out_path, entry.entry_name);
	const dir_path = dirname(file_path);
	
	await mkdir(dir_path, { recursive: true });
	await Bun.write(file_path, decompressed_data);
}

async function create_directory(entry: ZipEntry, out_path: string): Promise<void> {
	const dir_path = sanitize_path(out_path, entry.entry_name);
	await mkdir(dir_path, { recursive: true });
}

export async function extract_zip(zip_file: string, out_path: string): Promise<void> {
	const zip_data = new Uint8Array(await Bun.file(zip_file).arrayBuffer());
	
	const eocd = find_end_of_central_directory(zip_data);
	if (!eocd)
		throw new Error('Invalid ZIP file: End of Central Directory not found');
	
	const entries = parse_central_directory(zip_data, eocd);
	await mkdir(out_path, { recursive: true });
	
	for (const entry of entries) {
		if (entry.is_directory) {
			await create_directory(entry, out_path);
		} else {
			await extract_file(entry, out_path, zip_data);
		}
	}
}