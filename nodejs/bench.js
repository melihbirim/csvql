'use strict';
/**
 * csvql vs csv-parse vs papaparse — 1 000 000 rows
 * Run:  node --expose-gc bench.js
 */

const csvql    = require('./index.js');
const { parse } = require('csv-parse/sync');
const Papa     = require('papaparse');
const fs       = require('fs');

// ── Generate test data ────────────────────────────────────────────────────────

const CSV    = '/tmp/_csvql_bench.csv';
const ROWS   = 1_000_000;
const CITIES = ['Austin', 'Boston', 'Chicago', 'Denver', 'Eugene'];
const DEPTS  = ['Engineering', 'Marketing', 'Finance', 'HR', 'Sales'];

process.stdout.write(`Generating ${ROWS.toLocaleString()} row test CSV... `);
{
    const lines = ['id,name,age,city,salary,department'];
    for (let i = 1; i <= ROWS; i++) {
        const city   = CITIES[i % CITIES.length];
        const dept   = DEPTS[i % DEPTS.length];
        const salary = 40000 + (i * 97) % 120000;
        lines.push(`${i},Person${i},${20 + (i % 50)},${city},${salary},${dept}`);
    }
    fs.writeFileSync(CSV, lines.join('\n') + '\n');
}
console.log(`done  (${(fs.statSync(CSV).size / 1024 / 1024).toFixed(1)} MB on disk)\n`);

// ── Helpers ───────────────────────────────────────────────────────────────────

function measure(fn) {
    if (global.gc) global.gc();
    const before = process.memoryUsage().heapUsed;
    const t      = process.hrtime.bigint();
    const result = fn();
    const ms     = Number(process.hrtime.bigint() - t) / 1e6;
    const mb     = (process.memoryUsage().heapUsed - before) / 1024 / 1024;
    return { ms, mb: Math.max(0, mb), result };
}

function fmtMem(mb) {
    if (mb < 1)   return '~0 MB';
    return `+${mb.toFixed(0)} MB`;
}

function resultsTable(entries, refMs) {
    // entries: [{label, ms, mb, rows}]
    console.log(`  ${'Library'.padEnd(14)} ${'Time'.padStart(8)}   ${'Heap'.padStart(8)}   ${'Rows'.padStart(9)}   Note`);
    console.log(`  ${'-'.repeat(14)} ${'-'.repeat(8)}   ${'-'.repeat(8)}   ${'-'.repeat(9)}   ----`);
    for (const e of entries) {
        const speedup = refMs / e.ms;
        const note = speedup >= 1.05
            ? `${speedup.toFixed(1)}x faster`
            : speedup <= 0.95
                ? `${(1/speedup).toFixed(1)}x slower`
                : 'baseline';
        console.log(
            `  ${e.label.padEnd(14)} ${String(e.ms.toFixed(0) + 'ms').padStart(8)}   ${fmtMem(e.mb).padStart(8)}   ${String(e.rows).padStart(9)}   ${note}`
        );
    }
}

// ── BENCHMARK ─────────────────────────────────────────────────────────────────

// ── Q1: LIMIT 5 ───────────────────────────────────────────────────────────────
console.log('══════════════════════════════════════════════════════════════════════');
console.log('Q1  SELECT * LIMIT 5   (peek — stop after 5 rows)');
console.log('══════════════════════════════════════════════════════════════════════');
console.log(`
  csvql       (1 line)
    query("SELECT * FROM 'data.csv' LIMIT 5")

  csv-parse   (1 line — stops at row 5 thanks to {to:5})
    parse(fs.readFileSync('data.csv'), { columns: true, to: 5 })

  papaparse   (1 line — BUT reads the whole file into a JS string first)
    Papa.parse(fs.readFileSync('data.csv','utf8'), { header:true, preview:5 }).data
`);

const q1_csvql = measure(() => csvql.query(`SELECT * FROM '${CSV}' LIMIT 5`));
const q1_csv   = measure(() => parse(fs.readFileSync(CSV), { columns: true, to: 5 }));
const q1_papa  = measure(() => Papa.parse(fs.readFileSync(CSV, 'utf8'), { header: true, preview: 5 }).data);

resultsTable([
    { label: 'csvql',     ...q1_csvql, rows: q1_csvql.result.length },
    { label: 'csv-parse', ...q1_csv,   rows: q1_csv.result.length   },
    { label: 'papaparse', ...q1_papa,  rows: q1_papa.result.length  },
], q1_csv.ms);

// ── Q2: GROUP BY ──────────────────────────────────────────────────────────────
console.log(`\n${'══'.repeat(34)}`);
console.log('Q2  GROUP BY city COUNT(*)   (aggregate — full scan)');
console.log('══════════════════════════════════════════════════════════════════════');
console.log(`
  csvql       (1 line)
    query("SELECT city, COUNT(*) as n FROM 'data.csv' GROUP BY city")

  csv-parse   (6 lines — materialises ALL 1M rows as JS objects in heap)
    const records = parse(fs.readFileSync('data.csv'), { columns: true })
    const counts  = records.reduce((acc, r) => {
        acc[r.city] = (acc[r.city] || 0) + 1
        return acc
    }, {})
    const rows = Object.entries(counts).map(([city, n]) => ({ city, n }))

  papaparse   (6 lines — same pattern, same heap cost)
    const { data: records } = Papa.parse(fs.readFileSync('data.csv','utf8'), { header:true })
    const counts  = records.reduce((acc, r) => {
        acc[r.city] = (acc[r.city] || 0) + 1
        return acc
    }, {})
    const rows = Object.entries(counts).map(([city, n]) => ({ city, n }))
`);

const q2_csvql = measure(() => csvql.query(`SELECT city, COUNT(*) as n FROM '${CSV}' GROUP BY city`));
const q2_csv   = measure(() => {
    const records = parse(fs.readFileSync(CSV), { columns: true });
    const counts = records.reduce((acc, r) => { acc[r.city] = (acc[r.city] || 0) + 1; return acc; }, {});
    return Object.entries(counts).map(([city, n]) => ({ city, n }));
});
const q2_papa  = measure(() => {
    const records = Papa.parse(fs.readFileSync(CSV, 'utf8'), { header: true }).data;
    const counts = records.reduce((acc, r) => { acc[r.city] = (acc[r.city] || 0) + 1; return acc; }, {});
    return Object.entries(counts).map(([city, n]) => ({ city, n }));
});

resultsTable([
    { label: 'csvql',     ...q2_csvql, rows: q2_csvql.result.length },
    { label: 'csv-parse', ...q2_csv,   rows: q2_csv.result.length   },
    { label: 'papaparse', ...q2_papa,  rows: q2_papa.result.length  },
], q2_csv.ms);

// ── Q3: WHERE filter ──────────────────────────────────────────────────────────
console.log(`\n${'══'.repeat(34)}`);
console.log('Q3  WHERE salary > 100000   (filter — full scan, large result set)');
console.log('══════════════════════════════════════════════════════════════════════');
console.log(`
  csvql       (1 line)
    query("SELECT name, salary FROM 'data.csv' WHERE salary > 100000")

  csv-parse   (2 lines — loads all rows, then JS filters a copy)
    const records = parse(fs.readFileSync('data.csv'), { columns: true })
    const rows    = records.filter(r => Number(r.salary) > 100000)

  papaparse   (2 lines — same)
    const { data: records } = Papa.parse(fs.readFileSync('data.csv','utf8'), { header:true })
    const rows = records.filter(r => Number(r.salary) > 100000)
`);

const q3_csvql = measure(() => csvql.query(`SELECT name, salary FROM '${CSV}' WHERE salary > 100000`));
const q3_csv   = measure(() => {
    const records = parse(fs.readFileSync(CSV), { columns: true });
    return records.filter(r => Number(r.salary) > 100000);
});
const q3_papa  = measure(() => {
    const records = Papa.parse(fs.readFileSync(CSV, 'utf8'), { header: true }).data;
    return records.filter(r => Number(r.salary) > 100000);
});

resultsTable([
    { label: 'csvql',     ...q3_csvql, rows: q3_csvql.result.length },
    { label: 'csv-parse', ...q3_csv,   rows: q3_csv.result.length   },
    { label: 'papaparse', ...q3_papa,  rows: q3_papa.result.length  },
], q3_csv.ms);

// ── Q4: ORDER BY LIMIT ────────────────────────────────────────────────────────
console.log(`\n${'══'.repeat(34)}`);
console.log('Q4  ORDER BY salary DESC LIMIT 10   (top-N)');
console.log('══════════════════════════════════════════════════════════════════════');
console.log(`
  csvql       (1 line)
    query("SELECT name, salary FROM 'data.csv' ORDER BY salary DESC LIMIT 10")

  csv-parse   (3 lines — loads all, sorts all 1M rows in JS, takes top 10)
    const records = parse(fs.readFileSync('data.csv'), { columns: true })
    const rows    = records
        .sort((a, b) => Number(b.salary) - Number(a.salary))
        .slice(0, 10)

  papaparse   (3 lines — same)
    const { data: records } = Papa.parse(fs.readFileSync('data.csv','utf8'), { header:true })
    const rows = records
        .sort((a, b) => Number(b.salary) - Number(a.salary))
        .slice(0, 10)
`);

const q4_csvql = measure(() => csvql.query(`SELECT name, salary FROM '${CSV}' ORDER BY salary DESC LIMIT 10`));
const q4_csv   = measure(() => {
    const records = parse(fs.readFileSync(CSV), { columns: true });
    return records.sort((a, b) => Number(b.salary) - Number(a.salary)).slice(0, 10);
});
const q4_papa  = measure(() => {
    const records = Papa.parse(fs.readFileSync(CSV, 'utf8'), { header: true }).data;
    return records.sort((a, b) => Number(b.salary) - Number(a.salary)).slice(0, 10);
});

resultsTable([
    { label: 'csvql',     ...q4_csvql, rows: q4_csvql.result.length },
    { label: 'csv-parse', ...q4_csv,   rows: q4_csv.result.length   },
    { label: 'papaparse', ...q4_papa,  rows: q4_papa.result.length  },
], q4_csv.ms);

// ── Feature comparison ────────────────────────────────────────────────────────

console.log(`\n${'══'.repeat(34)}`);
console.log('FEATURE COMPARISON');
console.log('══════════════════════════════════════════════════════════════════════');

const features = [
    ['SQL WHERE / filter',        '✓',              '✗ manual .filter()', '✗ manual .filter()'],
    ['SQL GROUP BY / aggregate',  '✓',              '✗ manual reduce',    '✗ manual reduce'],
    ['SQL ORDER BY / sort',       '✓',              '✗ manual .sort()',   '✗ manual .sort()'],
    ['SQL JOIN across files',     '✓',              '✗',                  '✗'],
    ['SIMD parsing',              '✓',              '✗',                  '✗'],
    ['Sync API — low memory',     '✓ always ~5 MB', '✗ loads all rows',   '✗ loads all rows'],
    ['Streaming (low memory)',     '✓ always',       '✓ async API only',   '✓ async API only'],
    ['Custom delimiter (TSV…)',   '✗',              '✓',                  '✓'],
    ['CSV writing (unparse)',      '✗',              '✗',                  '✓'],
    ['Browser support',           '✗',              '✗',                  '✓'],
    ['TypeScript types',          '✓',              '✓',                  '✓'],
];

console.log(`\n  ${'Feature'.padEnd(30)} ${'csvql'.padEnd(18)} ${'csv-parse'.padEnd(20)} papaparse`);
console.log(`  ${'-'.repeat(30)} ${'-'.repeat(18)} ${'-'.repeat(20)} ---------`);
for (const [feat, a, b, c] of features) {
    console.log(`  ${feat.padEnd(30)} ${String(a).padEnd(18)} ${String(b).padEnd(20)} ${c}`);
}

console.log(`\n  When to use:`);
console.log(`  csvql     → analytics queries on any-size CSV; always low memory`);
console.log(`  csv-parse → custom delimiters, streaming pipelines, raw parse control`);
console.log(`  papaparse → browser, CSV writing, simple small-file parsing`);

fs.unlinkSync(CSV);
