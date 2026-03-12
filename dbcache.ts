import BufferReader from './buffer';

const XFTH_MAGIC = 0x48544658;
const HASH_SIZE = 32; // SHA-256

interface DbCacheHeader {
	version: number;
	build: number;
	hash: string | null;
}

interface DbCacheEntry {
	table_hash: number;
	record_id: number;
	status: number;
	push_id: number;
	data_size: number;
	region_id: number;
	unique_id: number;
}

interface DbCacheResult {
	header: DbCacheHeader;
	entries: DbCacheEntry[];
}

function read_header(buf: BufferReader): DbCacheHeader | null {
	const magic = buf.readUInt32LE();
	if (magic !== XFTH_MAGIC)
		return null;

	const version = buf.readUInt32LE();
	const build = buf.readUInt32LE();

	let hash: string | null = null;
	if (version >= 5)
		hash = buf.readHexString(HASH_SIZE);

	return { version, build, hash };
}

function read_entry(buf: BufferReader, version: number): DbCacheEntry | null {
	if (buf.remainingBytes < 4)
		return null;

	const magic = buf.readUInt32LE();
	if (magic !== XFTH_MAGIC)
		return null;

	let region_id = 0;
	let unique_id = 0;

	if (version >= 9)
		region_id = buf.readUInt32LE();

	const push_id = buf.readUInt32LE();

	if (version >= 8)
		unique_id = buf.readUInt32LE();

	const table_hash = buf.readUInt32LE();
	const record_id = buf.readUInt32LE();
	const data_size = buf.readUInt32LE();
	const status = buf.readUInt8();

	// pad[3]
	buf.move(3);

	// skip raw record data
	if (data_size > 0)
		buf.move(data_size);

	return { table_hash, record_id, status, push_id, data_size, region_id, unique_id };
}

export function parse_dbcache(data: ArrayBuffer): DbCacheResult | null {
	if (data.byteLength < 12)
		return null;

	const buf = new BufferReader(data);
	const header = read_header(buf);

	if (header === null)
		return null;

	if (header.version < 7 || header.version > 9)
		return null;

	const entries: DbCacheEntry[] = [];

	while (buf.remainingBytes > 0) {
		const entry = read_entry(buf, header.version);
		if (entry === null)
			break;

		entries.push(entry);
	}

	return { header, entries };
}

export type { DbCacheResult, DbCacheHeader, DbCacheEntry };
