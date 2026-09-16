import { parse_wdb, classify_product } from './wdb';

// pulls completed submissions from the public archavon read api, parses their wdb files with the
// current parser and reports the per-file and aggregate parse error ratio. exits 1 if the aggregate
// ratio for any signature is at or above the cache_worker alert threshold.
//
// usage: bun run wow.export/wdb_check.ts --product=wow_classic --build=69585 [--sig=WQST] [--limit=10]

const ARCHAVON_READ_URL = 'https://archavon.kruithne.net/api/v1';
const PARSE_ERROR_ALERT_RATIO = 0.05;
const DEFAULT_LIMIT = 10;
const FETCH_HEADERS = { 'User-Agent': 'wow.export/wdb_check' };

function str_arg(name: string): string | undefined {
	const raw = process.argv.find(a => a.startsWith(`--${name}=`));
	return raw?.slice(name.length + 3);
}

const product = str_arg('product');
const build = str_arg('build');
const sig_filter = str_arg('sig');
const limit = Number(str_arg('limit') ?? DEFAULT_LIMIT);

if (product === undefined || build === undefined)
	throw new Error('--product and --build are required');

if (classify_product(product) === null)
	throw new Error(`unknown product family: ${product}`);

async function get_json(url: string): Promise<any> {
	const res = await fetch(url, { headers: FETCH_HEADERS });
	if (!res.ok)
		throw new Error(`${url} failed (${res.status})`);

	return res.json();
}

const listing = await get_json(`${ARCHAVON_READ_URL}/submissions?product=${product}&build=${build}&status=completed`);
const submissions = listing.data.slice(0, limit);
console.log(`${product} build ${build}: ${listing.data.length} completed submissions, checking ${submissions.length}`);

const totals = new Map<string, { records: number; errors: number }>();

for (const sub of submissions) {
	const detail = await get_json(`${ARCHAVON_READ_URL}/submissions/${sub.submission_id}`);

	for (const file of detail.files) {
		if (!file.file_name.toLowerCase().endsWith('.wdb') || file.status !== 'completed')
			continue;

		const res = await fetch(file.cdn_url, { headers: FETCH_HEADERS });
		if (!res.ok) {
			console.log(`  ${file.locale}/${file.file_name}: download failed (${res.status})`);
			continue;
		}

		const result = parse_wdb(await res.arrayBuffer(), sub.patch, product);
		if (result === null) {
			console.log(`  ${file.locale}/${file.file_name}: failed to parse`);
			continue;
		}

		const sig = result.header.signature;
		if (sig_filter !== undefined && sig !== sig_filter)
			continue;

		const records = result.records.length;
		const errors = result.records.filter(r => (r.data as { parse_error?: boolean }).parse_error === true).length;
		const ratio = records > 0 ? errors / records : 0;

		const total = totals.get(sig) ?? { records: 0, errors: 0 };
		total.records += records;
		total.errors += errors;
		totals.set(sig, total);

		console.log(`  ${sub.submission_id.slice(0, 8)} ${file.locale}/${file.file_name} hdr=${result.header.build} ${sig} ${records} records, ${errors} errors (${(ratio * 100).toFixed(1)}%)`);
	}
}

let failed = false;
for (const [sig, total] of totals) {
	const ratio = total.records > 0 ? total.errors / total.records : 0;
	const status = ratio >= PARSE_ERROR_ALERT_RATIO ? 'FAIL' : 'ok';
	if (ratio >= PARSE_ERROR_ALERT_RATIO)
		failed = true;

	console.log(`${sig}: ${total.records} records, ${total.errors} errors (${(ratio * 100).toFixed(1)}%) ${status}`);
}

process.exit(failed ? 1 : 0);
