const HASH_TABLE = [
	0x486E26EE, 0xDCAA16B3, 0xE1918EEF, 0x202DAFDB,
	0x341C7DC7, 0x1C365303, 0x40EF2D37, 0x65FD5E49,
	0xD6057177, 0x904ECE93, 0x1C38024F, 0x98FD323B,
	0xE3061AE7, 0xA39B0FA1, 0x9797F25F, 0xE4444563,
];

export function sstrhash(input: string): number {
	let seed = 0x7FED7FED;
	let shift = 0xEEEEEEEE;

	for (let i = 0; i < input.length; i++) {
		const c = input.charCodeAt(i);

		// uppercase conversion
		const uc = (c >= 0x61 && c <= 0x7A) ? c - 0x20 : c;

		const sub = ((HASH_TABLE[uc >> 4]! - HASH_TABLE[uc & 0xF]!) >>> 0);
		seed = (sub ^ ((shift + seed) >>> 0)) >>> 0;
		shift = (uc + seed + Math.imul(33, shift) + 3) >>> 0;
	}

	return seed === 0 ? 1 : seed;
}
