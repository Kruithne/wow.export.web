import BufferReader from '../buffer';
import Salsa20 from './salsa20';
import { tact_get_key } from './tact';
import { ErrorWithMetadata } from 'spooder';

const BLTE_MAGIC = 0x45544c42;
const ENC_TYPE_SALSA20 = 0x53;
const EMPTY_HASH = '00000000000000000000000000000000';

class EncryptionError extends Error {
	key: string;

	constructor(key: string, ...params: any[]) {
		super('blte: missing decryption key', ...params);
		this.key = key;

		// Maintain stack trace (V8).
		Error.captureStackTrace?.(this, EncryptionError);
	}
}

interface BLTEBlock {
	CompSize: number;
	DecompSize: number;
	Hash: string;
}

/**
 * Unpacks BLTE-encoded data into a raw ArrayBuffer.
 * @param data Input ArrayBuffer containing BLTE data
 * @param hash Expected MD5 hash for validation
 * @param partial_decrypt Allow partial decryption (skip blocks with missing keys)
 * @returns Decompressed/decrypted ArrayBuffer
 */
function blte_unpack(data: ArrayBuffer, hash: string, partial_decrypt: boolean = false): ArrayBuffer {
	const input = new BufferReader(data);

	const { blocks, header_size } = parse_header(input, hash);
	const alloc_size = calculate_alloc_size(input, blocks, header_size, data.byteLength);

	const output = BufferReader.alloc(alloc_size);
	input.seek(header_size > 0 ? header_size : 8);

	for (let blockIndex = 0; blockIndex < blocks.length; blockIndex++) {
		const block = blocks[blockIndex]!;
		const blte_pos = input.offset;

		if (block.Hash !== EMPTY_HASH) {
			const block_data = input.readBuffer(block.CompSize) as BufferReader;
			const block_hash = block_data.calculateHash();

			input.seek(blte_pos);

			if (block_hash !== block.Hash)
				throw new ErrorWithMetadata('blte: invalid block hash', { expected: block.Hash, actual: block_hash });
		}

		try {
			process_block(input, output, blocks, blockIndex, partial_decrypt);
		} catch (e) {
			if (e instanceof EncryptionError && partial_decrypt) {
				// Skip this block by filling with zeros
				output.move(block.DecompSize);
			} else {
				throw e;
			}
		}

		input.seek(blte_pos + block.CompSize);
	}

	output.seek(0);
	return output.raw;
}

function calculate_alloc_size(buf: BufferReader, blocks: BLTEBlock[], header_size: number, total_size: number): number {
	let alloc_size = 0;
	for (const block of blocks)
		alloc_size += block.DecompSize;

	return alloc_size;
}

function parse_header(buf: BufferReader, hash: string): { blocks: BLTEBlock[]; header_size: number } {
	const size = buf.byteLength;

	if (size < 8)
		throw new ErrorWithMetadata('blte: not enough data', { size, required: 8 });

	const magic = buf.readUInt32LE();
	if (magic !== BLTE_MAGIC)
		throw new ErrorWithMetadata(`blte: invalid magic`, { magic });

	const header_size = buf.readInt32BE();

	buf.seek(0);
	let hash_check = header_size > 0 ? (buf.readBuffer(header_size) as BufferReader).calculateHash() : buf.calculateHash();
	if (hash_check !== hash)
		throw new ErrorWithMetadata('blte: invalid hash', { expected: hash, actual: hash_check });

	buf.seek(8); // Reset to after magic and headerSize
	let num_blocks = 1;

	if (header_size > 0) {
		if (size < 12)
			throw new ErrorWithMetadata('blte: not enough data', { size, required: 12 });

		const fc = buf.readBytes(4);
		num_blocks = fc[1]! << 16 | fc[2]! << 8 | fc[3]! << 0;

		if (fc[0] !== 0x0F || num_blocks === 0)
			throw new ErrorWithMetadata('blte: invalid table format', { fc0: fc[0], num_blocks });

		const frame_header_size = 24 * num_blocks + 12;
		if (header_size !== frame_header_size)
			throw new ErrorWithMetadata('blte: invalid header size', { expected: frame_header_size, actual: header_size });

		if (size < frame_header_size)
			throw new ErrorWithMetadata('blte: not enough data for frame header', { size, required: frame_header_size });
	}

	const blocks: BLTEBlock[] = new Array(num_blocks);

	for (let i = 0; i < num_blocks; i++) {
		const block: BLTEBlock = {
			CompSize: 0,
			DecompSize: 0,
			Hash: EMPTY_HASH
		};

		if (header_size !== 0) {
			block.CompSize = buf.readInt32BE();
			block.DecompSize = buf.readInt32BE();
			block.Hash = buf.readHexString(16);
		} else {
			block.CompSize = size - 8;
			block.DecompSize = size - 9;
			block.Hash = EMPTY_HASH;
		}

		blocks[i] = block;
	}

	return { blocks, header_size };
}

function process_block(input: BufferReader, output: BufferReader, blocks: BLTEBlock[], blockIndex: number, partial_decrypt: boolean): void {
	const flag = input.readUInt8();

	switch (flag) {
		case 0x45: // Encrypted
			const decrypted = decrypt_block(input, input.offset + blocks[blockIndex]!.CompSize - 1, blockIndex);
			process_block(decrypted, output, blocks, blockIndex, partial_decrypt);
			break;

		case 0x46: // Frame (Recursive)
			throw new Error('blte: frame decoder not implemented');

		case 0x4E: // Frame (Normal)
			write_buffer(input, output, input.offset + blocks[blockIndex]!.CompSize - 1);
			break;

		case 0x5A: // Compressed
			decomp_block(input, output, blocks, blockIndex);
			break;

		default:
			throw new ErrorWithMetadata('blte: unknown block type', { flag });
	}
}

function decomp_block(input: BufferReader, output: BufferReader, blocks: BLTEBlock[], blockIndex: number): void {
	const block_end = input.offset + blocks[blockIndex]!.CompSize - 1;
	const decomp = input.readBuffer(block_end - input.offset, true, true) as BufferReader;
	const expected_size = blocks[blockIndex]!.DecompSize;

	// Reallocate buffer if needed
	if (decomp.byteLength > expected_size) {
		output.setCapacity(output.byteLength + (decomp.byteLength - expected_size));
	}

	write_buffer(decomp, output, decomp.byteLength);
}

function decrypt_block(data: BufferReader, block_end: number, blockIndex: number): BufferReader {
	const key_name_size = data.readUInt8();
	if (key_name_size === 0 || key_name_size !== 8)
		throw new ErrorWithMetadata('blte: unexpected key name size', { key_name_size });

	const key_name_bytes = new Array(key_name_size);
	for (let i = 0; i < key_name_size; i++)
		key_name_bytes[i] = data.readHexString(1);

	const key_name = key_name_bytes.reverse().join('');
	const iv_size = data.readUInt8();

	if ((iv_size !== 4 && iv_size !== 8) || iv_size > 8)
		throw new ErrorWithMetadata('blte: unexpected iv size', { iv_size });

	const iv_short = data.readBytes(iv_size);
	if (data.remainingBytes === 0)
		throw new Error('blte: unexpected end of data before encryption flag');

	const encrypt_type = data.readUInt8();
	if (encrypt_type !== ENC_TYPE_SALSA20)
		throw new ErrorWithMetadata('blte: unexpected encryption type', { encrypt_type, expected: ENC_TYPE_SALSA20 });

	for (let shift = 0, i = 0; i < 4; shift += 8, i++)
		iv_short[i] = (iv_short[i]! ^ ((blockIndex >> shift) & 0xFF)) & 0xFF;

	const key = tact_get_key(key_name);
	if (typeof key !== 'string')
		throw new EncryptionError(key_name);

	const nonce: number[] = [];
	for (let i = 0; i < 8; i++)
		nonce[i] = (i < iv_short.length ? iv_short[i]! : 0x0);

	const instance = new Salsa20(nonce, key);
	return instance.process(data.readBuffer(block_end - data.offset) as BufferReader);
}

function write_buffer(source: BufferReader, dest: BufferReader, block_end: number): void {
	const source_view = new Uint8Array(source.raw);
	const dest_view = new Uint8Array(dest.raw);

	const start_offset = source.offset;
	const length = block_end - start_offset;

	dest_view.set(source_view.subarray(start_offset, block_end), dest.offset);
	dest.move(length);
}

export { blte_unpack, EncryptionError };
