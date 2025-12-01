// sbt uses frames (24fps pulldown, 23.98 wrathgate), srt uses milliseconds
// 3 minutes would be 0.4s drift, negligible, subtitles have 100ms +- tolerance
const FRAMES_PER_SECOND = 24;

function parse_sbt_timestamp(timestamp: string): number {
	const parts = timestamp.split(':');
	if (parts.length !== 4)
		return 0;

	const hours = parseInt(parts[0], 10);
	const minutes = parseInt(parts[1], 10);
	const seconds = parseInt(parts[2], 10);
	const frames = parseInt(parts[3], 10);

	const total_ms = (hours * 3600 + minutes * 60 + seconds) * 1000;
	const frame_ms = Math.round((frames / FRAMES_PER_SECOND) * 1000);

	return total_ms + frame_ms;
}

function format_srt_timestamp(ms: number): string {
	const hours = Math.floor(ms / 3600000);
	ms %= 3600000;

	const minutes = Math.floor(ms / 60000);
	ms %= 60000;

	const seconds = Math.floor(ms / 1000);
	const millis = ms % 1000;

	const pad2 = (n: number) => n.toString().padStart(2, '0');
	const pad3 = (n: number) => n.toString().padStart(3, '0');

	return `${pad2(hours)}:${pad2(minutes)}:${pad2(seconds)},${pad3(millis)}`;
}

export function sbt_to_srt(sbt: string): string {
	const lines = sbt.split(/\r?\n/);
	const entries: { start: number; end: number; text: string[] }[] = [];

	let current_entry: { start: number; end: number; text: string[] } | null = null;

	for (const line of lines) {
		const trimmed = line.trim();

		if (trimmed.length === 0) {
			if (current_entry !== null && current_entry.text.length > 0) {
				entries.push(current_entry);
				current_entry = null;
			}
			continue;
		}

		// timestamp line: 00:00:14:12 - 00:00:17:08
		const timestamp_match = trimmed.match(/^(\d{2}:\d{2}:\d{2}:\d{2})\s*-\s*(\d{2}:\d{2}:\d{2}:\d{2})/);
		if (timestamp_match) {
			if (current_entry !== null && current_entry.text.length > 0)
				entries.push(current_entry);

			current_entry = {
				start: parse_sbt_timestamp(timestamp_match[1]),
				end: parse_sbt_timestamp(timestamp_match[2]),
				text: []
			};
			continue;
		}

		if (current_entry !== null)
			current_entry.text.push(trimmed);
	}

	// handle final entry without trailing newline
	if (current_entry !== null && current_entry.text.length > 0)
		entries.push(current_entry);

	const srt_lines: string[] = [];
	for (let i = 0; i < entries.length; i++) {
		const entry = entries[i];
		srt_lines.push((i + 1).toString());
		srt_lines.push(`${format_srt_timestamp(entry.start)} --> ${format_srt_timestamp(entry.end)}`);
		srt_lines.push(...entry.text);
		srt_lines.push('');
	}

	return srt_lines.join('\n');
}
