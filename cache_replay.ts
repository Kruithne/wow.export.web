import { db_archavon } from './db_archavon';

// marks previously failed submissions as unprocessed so the backlog drain in
// module.ts replays them one at a time. dry run unless --apply is passed.
//
// usage: bun run wow.export/cache_replay.ts --from=2026-07-01 --to=2026-09-01 [--apply]

const APPLY = process.argv.includes('--apply');
const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

function arg(name: string): string {
	const raw = process.argv.find(a => a.startsWith(`--${name}=`));
	if (raw === undefined)
		throw new Error(`missing --${name}=YYYY-MM-DD`);

	const value = raw.slice(name.length + 3);
	if (!DATE_PATTERN.test(value))
		throw new Error(`--${name} must be YYYY-MM-DD, got "${value}"`);

	return value;
}

const from = arg('from');
const to = arg('to');

const [summary] = await db_archavon`
	SELECT COUNT(DISTINCT s.submission_id) subs, COALESCE(SUM(f.file_size), 0) bytes
	FROM cache_submissions s
	JOIN cache_submission_files f ON f.submission_id = s.submission_id
	WHERE s.status = 'failed' AND f.status = 'pending'
	AND s.submitted_at >= ${from} AND s.submitted_at < ${to}
`;

console.log(`${from} .. ${to}`);
console.log(`${Number(summary.subs).toLocaleString()} submissions, ${(Number(summary.bytes) / 1073741824).toFixed(2)} GB`);

if (Number(summary.subs) === 0) {
	console.log('nothing to replay');
	process.exit(0);
}

if (!APPLY) {
	console.log('\ndry run, pass --apply to queue these for replay');
	process.exit(0);
}

// clearing processed_at makes these eligible for cache_drain_backlog, which
// picks up one at a time only while the pipeline is idle
const result = await db_archavon`
	UPDATE cache_submissions
	SET processed_at = NULL, status = 'finalized', status_reason = NULL
	WHERE status = 'failed'
	AND submitted_at >= ${from} AND submitted_at < ${to}
`;

console.log(`queued ${Number(summary.subs).toLocaleString()} submissions for replay`, result);
console.log('the running server will drain these while idle');
