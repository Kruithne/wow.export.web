import crypto from 'node:crypto';
import { inflateSync, deflateSync } from 'node:zlib';

export default class BufferReader {
	private buffer: ArrayBuffer;
	private view: DataView;
	private _offset: number;

	constructor(buffer: ArrayBuffer) {
		this.buffer = buffer;
		this.view = new DataView(buffer);
		this._offset = 0;
	}

	/**
	 * Allocate a new BufferReader with the specified length.
	 * @param length The size of the buffer in bytes
	 * @returns A new BufferReader instance
	 */
	static alloc(length: number): BufferReader {
		return new BufferReader(new ArrayBuffer(length));
	}

	get length(): number {
		return this.buffer.byteLength;
	}

	get byteLength(): number {
		return this.buffer.byteLength;
	}

	get raw(): ArrayBuffer {
		return this.buffer;
	}

	get offset(): number {
		return this._offset;
	}

	get remainingBytes(): number {
		return this.byteLength - this._offset;
	}

	seek(offset: number): void {
		if (offset < 0) {
			this._offset = this.length + offset;
		} else {
			this._offset = offset;
		}
	}

	move(offset: number): void {
		this._offset += offset;
	}

	readInt32LE(): number {
		const value = this.view.getInt32(this._offset, true);
		this._offset += 4;
		return value;
	}

	readInt32BE(): number {
		const value = this.view.getInt32(this._offset, false);
		this._offset += 4;
		return value;
	}

	readUInt32LE(): number {
		const value = this.view.getUint32(this._offset, true);
		this._offset += 4;
		return value;
	}

	readUInt32Array(count: number): number[] {
		const values = new Array(count);
		for (let i = 0; i < count; i++) {
			values[i] = this.view.getUint32(this._offset, true);
			this._offset += 4;
		}
		return values;
	}

	readUInt32BE(): number {
		const value = this.view.getUint32(this._offset, false);
		this._offset += 4;
		return value;
	}

	readUInt16LE(): number {
		const value = this.view.getUint16(this._offset, true);
		this._offset += 2;
		return value;
	}

	readInt16BE(): number {
		const value = this.view.getInt16(this._offset, false);
		this._offset += 2;
		return value;
	}

	readInt40BE(): number {
		// Read 5 bytes (40 bits) as big-endian signed integer
		const byte0 = this.view.getUint8(this._offset);
		const byte1 = this.view.getUint8(this._offset + 1);
		const byte2 = this.view.getUint8(this._offset + 2);
		const byte3 = this.view.getUint8(this._offset + 3);
		const byte4 = this.view.getUint8(this._offset + 4);
		this._offset += 5;

		// Combine bytes into a 40-bit value
		// JavaScript uses 53-bit precision for numbers, so 40 bits fits safely
		let value = (byte0 * 0x100000000) | (byte1 << 24) | (byte2 << 16) | (byte3 << 8) | byte4;

		// Check if the sign bit (bit 39) is set
		if (byte0 & 0x80) {
			// Sign extend: set bits 40-63 to 1
			value = value - 0x10000000000; // 2^40
		}

		return value;
	}

	readUInt8(): number {
		const value = this.view.getUint8(this._offset);
		this._offset += 1;
		return value;
	}

	readBytes(count: number): Uint8Array {
		const bytes = new Uint8Array(this.buffer, this._offset, count);
		this._offset += count;
		return bytes;
	}

	readUInt8Array(count: number): number[] {
		const values = new Array(count);
		for (let i = 0; i < count; i++) {
			values[i] = this.view.getUint8(this._offset);
			this._offset += 1;
		}
		return values;
	}

	writeUInt8(value: number | number[]): void {
		if (Array.isArray(value)) {
			for (const byte of value) {
				this.view.setUint8(this._offset, byte);
				this._offset += 1;
			}
		} else {
			this.view.setUint8(this._offset, value);
			this._offset += 1;
		}
	}

	writeUInt32LE(value: number): void {
		this.view.setUint32(this._offset, value, true);
		this._offset += 4;
	}

	writeUInt32BE(value: number): void {
		this.view.setUint32(this._offset, value, false);
		this._offset += 4;
	}

	writeInt32BE(value: number): void {
		this.view.setInt32(this._offset, value, false);
		this._offset += 4;
	}

	readHexString(length: number): string {
		const bytes = new Uint8Array(this.buffer, this._offset, length);
		this._offset += length;
		return Buffer.from(bytes).toString('hex');
	}

	readBuffer(length: number, wrap: boolean = true, inflate: boolean = false): BufferReader | ArrayBuffer {
		if (length > this.remainingBytes)
			throw new Error(`Buffer operation out-of-bounds: ${length} > ${this.remainingBytes}`);

		let buffer: ArrayBuffer = this.buffer.slice(this._offset, this._offset + length);
		this._offset += length;

		if (inflate) {
			const node_buffer = Buffer.from(buffer);
			const inflated = inflateSync(node_buffer);
			
			const array_buffer = new ArrayBuffer(inflated.byteLength);
			const view = new Uint8Array(array_buffer);
			view.set(inflated);
			buffer = array_buffer;
		}

		return wrap ? new BufferReader(buffer) : buffer;
	}

	calculateHash(hash: string = 'md5', encoding: 'hex' | 'base64' = 'hex'): string {
		const node_buffer = Buffer.from(this.buffer);
		return crypto.createHash(hash).update(node_buffer).digest(encoding);
	}

	setCapacity(capacity: number): void {
		if (capacity === this.byteLength)
			return;

		const new_buffer = new ArrayBuffer(capacity);
		const new_view = new Uint8Array(new_buffer);
		const old_view = new Uint8Array(this.buffer);

		new_view.set(old_view.subarray(0, Math.min(capacity, this.byteLength)));

		this.buffer = new_buffer;
		this.view = new DataView(new_buffer);
	}

	writeBuffer(source: BufferReader | ArrayBuffer, source_start: number = 0, source_end?: number): void {
		const source_buffer = source instanceof BufferReader ? source.raw : source;
		const source_view = new Uint8Array(source_buffer);
		const dest_view = new Uint8Array(this.buffer);

		const start = source instanceof BufferReader ? source.offset : source_start;
		const end = source_end ?? source_view.length;
		const length = end - start;

		if (this._offset + length > this.byteLength)
			throw new Error(`Buffer write out-of-bounds: ${this._offset + length} > ${this.byteLength}`);

		dest_view.set(source_view.subarray(start, end), this._offset);
		this._offset += length;

		if (source instanceof BufferReader)
			source.move(length);
	}

	/**
	 * Deflate (compress) the buffer data and return as ArrayBuffer.
	 * @returns Compressed data as ArrayBuffer
	 */
	deflate(): ArrayBuffer {
		const node_buffer = Buffer.from(this.buffer);
		const deflated = deflateSync(node_buffer);

		const array_buffer = new ArrayBuffer(deflated.byteLength);
		const view = new Uint8Array(array_buffer);
		view.set(deflated);

		return array_buffer;
	}

	/**
	 * Calculate CRC32 checksum of the buffer data.
	 * @returns CRC32 checksum as a signed 32-bit integer
	 */
	getCRC32(): number {
		const crc_table = new Int32Array(256);
		for (let n = 0; n < 256; n++) {
			let c = n;
			for (let k = 0; k < 8; k++) {
				c = ((c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1));
			}
			crc_table[n] = c;
		}

		let crc = -1;
		const uint8_view = new Uint8Array(this.buffer);
		for (let i = 0; i < uint8_view.length; i++) {
			crc = (crc >>> 8) ^ crc_table[(crc ^ uint8_view[i]!) & 0xFF]!;
		}

		return crc ^ -1;
	}
}
