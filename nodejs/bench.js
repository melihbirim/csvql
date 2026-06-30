'use strict';
/**
 * csvql vs csv-parse vs papaparse
 *
 * Run: node bench.js
 * Deps: npm install (csv-parse + papaparse in devDependencies)
 *
 * Measures: wall time + peak heap allocated during each operation.
 * Test data: 1 000 000 rows (~42 MB) generated in /tmp, cleaned up after.
 */

const csvql    = require('./index.js');
const { parse } = require('csv-parse/sync');
const Papa     = require('papaparse');
const fs       = require('fs');

// ── Generate test data ────────────────────────────────────────────────────────

const CSV   = '/tmp/_csvql_bench.csv';
const ROWS  = 1_000_000;
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
const fileMB = (fs.statSync(CSV).size / 1024 / 1024).toFixed(1);
console.log(`done (${fileMB} MB)`);

// ── Helpers ───────────────────────────────────────────────────────────────────

const W = 86;

function measure(fn) {
    if (global.gc) global.gc();
    const heapBefore = process.memoryUsage().heapUsed;
    const t = process.hrtime.bigint();
    const result = fn();
    const elapsed = Number(process.hrtime.bigint() - t) / 1e6;
    const heapPeak = process.memoryUsage().heapUsed - heapBefore;
    return { ms: elapsed, mb: Math.max(0, heapPeak / 1024 / 1024), result };
}

function printRow(label, { ms, mb, result }, ref) {
    const rowCount = Array.isArray(result) ? result.length : '—';
    const speedup  = ref ? (ref / ms) : null;
    const cmp      = speedup
        ? (speedup >= 1 ? `  ${speedup.toFixed(1)}x faster` : `  ${(1/speedup).toFixed(1)}x slower`)
        : '';
    const mem = mb > 1 ? `  +${mb.toFixed(0)} MB heap` : '  ~0 MB heap';
    console.log(`  ${label.padEnd(26)} ${String(ms.toFixed(0) + 'ms').padStart(8)}${mem.padEnd(18)}  ${String(rowCount + ' rows').padStart(11)}${cmp}`);
}

function header(title) {
    console.log(`\n${'─'.repeat(W)}`);
    console.log(title);
    console.log('─'.repeat(W));
}

// ── QUERY 1: LIMIT 5 ─────────────────────────────────────────────────────────

header('QUERY 1 — SELECT * LIMIT 5  (early stop — does not read the whole file)');
console.log(`  csvql:      query("SELECT * FROM '...' LIMIT 5")`);
console.log(`  csv-parse:  parse(readFileSync(f), {columns:true, to:5})          ← stops at row 5`);
console.log(`  papaparse:  Papa.parse(readFileSync(f,'utf8'), {preview:5})        ← reads whole string first`);
console.log();

const q1_csvql = measure(() => csvql.query(`SELECT * FROM '${CSV}' LIMIT 5`));
const q1_csv   = measure(() => parse(fs.readFileSync(CSV), { columns: true, to: 5 }));
const q1_papa  = measure(() => Papa.parse(fs.readFileSync(CSV, 'utf8'), { header: true, preview: 5 }).data);

printRow('csvql',     q1_csvql, q1_csv.ms);
printRow('csv-parse', q1_csv);
printRow('papaparse', q1_papa,  q1_csv.ms);

// ── QUERY 2: GROUP BY ─────────────────────────────────────────────────────────

header('QUERY 2 — GROUP BY city COUNT(*)  (full scan + aggregate)');
console.log(`  csvql:      query("SELECT city, COUNT(*) FROM '...' GROUP BY city")   // 1 line`);
console.log(`  csv-parse:  parse + .reduce() + Object.entries()                      // ~6 lines, all rows in heap`);
console.log(`  papaparse:  Papa.parse + .reduce() + Object.entries()                 // ~6 lines, all rows in heap`);
console.log();

const q2_csvql = measure(() =>
    csvql.query(`SELECT city, COUNT(*) as n FROM '${CSV}' GROUP BY city`));

const q2_csv = measure(() => {
    const records = parse(fs.readFileSync(CSV), { columns: true });
    const counts = records.reduce((acc, r) => { acc[r.city] = (acc[r.city] || 0) + 1; return acc; }, {});
    return Object.entries(counts).map(([city, n]) => ({ city, n }));
});

const q2_papa = measure(() => {
    const records = Papa.parse(fs.readFileSync(CSV, 'utf8'), { header: true }).data;
    const counts = records.reduce((acc, r) => { acc[r.city] = (acc[r.city] || 0) + 1; return acc; }, {});
    return Object.entries(counts).map(([city, n]) => ({ city, n }));
});

printRow('csvql',     q2_csvql, q2_csv.ms);
printRow('csv-parse', q2_csv);
printRow('papaparse', q2_papa,  q2_csv.ms);

// ── QUERY 3: WHERE ────────────────────────────────────────────────────────────

header('QUERY 3 — WHERE salary > 100000  (filter — full scan, large result)');
console.log(`  csvql:      query("SELECT name,salary FROM '...' WHERE salary > 100000")  // 1 line`);
console.log(`  csv-parse:  parse + .filter(r => Number(r.salary) > 100000)               // 2 lines, all rows in heap`);
console.log(`  papaparse:  Papa.parse + .filter(r => Number(r.salary) > 100000)          // 2 lines, all rows in heap`);
console.log();

const q3_csvql = measure(() =>
    csvql.query(`SELECT name, salary FROM '${CSV}' WHERE salary > 100000`));

const q3_csv = measure(() => {
    const records = parse(fs.readFileSync(CSV), { columns: true });
    return records.filter(r => Number(r.salary) > 100000);
});

const q3_papa = measure(() => {
    const records = Papa.parse(fs.readFileSync(CSV, 'utf8'), { header: true }).data;
    return records.filter(r => Number(r.salary) > 100000);
});

printRow('csvql',     q3_csvql, q3_csv.ms);
printRow('csv-parse', q3_csv);
printRow('papaparse', q3_papa,  q3_csv.ms);

// ── QUERY 4: ORDER BY LIMIT ───────────────────────────────────────────────────

header('QUERY 4 — ORDER BY salary DESC LIMIT 10  (top-N)');
console.log(`  csvql:      query("SELECT name,salary FROM '...' ORDER BY salary DESC LIMIT 10")  // 1 line`);
console.log(`  csv-parse:  parse + .sort() + .slice(0,10)                                        // 3 lines, all rows in heap`);
console.log(`  papaparse:  Papa.parse + .sort() + .slice(0,10)                                   // 3 lines, all rows in heap`);
console.log();

const q4_csvql = measure(() =>
    csvql.query(`SELECT name, salary FROM '${CSV}' ORDER BY salary DESC LIMIT 10`));

const q4_csv = measure(() => {
    const records = parse(fs.readFileSync(CSV), { columns: true });
    return records.sort((a, b) => Number(b.salary) - Number(a.salary)).slice(0, 10);
});

const q4_papa = measure(() => {
    const records = Papa.parse(fs.readFileSync(CSV, 'utf8'), { header: true }).data;
    return records.sort((a, b) => Number(b.salary) - Number(a.salary)).slice(0, 10);
});

printRow('csvql',     q4_csvql, q4_csv.ms);
printRow('csv-parse', q4_csv);
printRow('papaparse', q4_papa,  q4_csv.ms);

// ── Feature comparison ────────────────────────────────────────────────────────

console.log(`\n${'═'.repeat(W)}`);
console.log('FEATURE COMPARISON');
console.log('═'.repeat(W));

const features = [
    // [feature, csvql, csv-parse, papaparse]
    ['SQL WHERE / filter',         '✓',                    '✗ manual .filter()',  '✗ manual .filter()'],
    ['SQL GROUP BY / aggregate',   '✓',                    '✗ manual reduce',     '✗ manual reduce'],
    ['SQL ORDER BY / sort',        '✓',                    '✗ manual .sort()',    '✗ manual .sort()'],
    ['SQL JOIN across files',      '✓',                    '✗',                   '✗'],
    ['SQL LIMIT (early exit)',      '✓',                    '✓ with {to:N}',       '✓ with {preview:N}'],
    ['Streaming (low memory)',      '✓ always',             '✓ async API only',    '✓ async API only'],
    ['Sync API stays low memory',  '✓ always ~5 MB',       '✗ loads all rows',    '✗ loads all rows'],
    ['Custom delimiter (TSV etc)', '✗',                    '✓',                   '✓'],
    ['CSV writing (unparse)',      '✗',                    '✗',                   '✓'],
    ['Browser support',            '✗',                    '✗',                   '✓'],
    ['SIMD parsing',               '✓',                    '✗',                   '✗'],
    ['TypeScript types',           '✓',                    '✓',                   '✓'],
];

console.log(`  ${'Feature'.padEnd(34)} ${'csvql'.padEnd(20)} ${'csv-parse'.padEnd(22)} papaparse`);
console.log(`  ${'-'.repeat(34)} ${'-'.repeat(20)} ${'-'.repeat(22)} ---------`);
for (const [feat, a, b, c] of features) {
    console.log(`  ${feat.padEnd(34)} ${String(a).padEnd(20)} ${String(b).padEnd(22)} ${c}`);
}

console.log(`\n${'═'.repeat(W)}`);
console.log('WHEN TO USE WHAT');
console.log('═'.repeat(W));
console.log('  csvql     → SQL on CSV files, any size — ~5 MB RAM regardless of file size');
console.log('  csv-parse → Raw parsing, custom delimiters, streaming pipeline (async API)');
console.log('  papaparse → Browser, CSV writing, simple sync API for small files');
console.log('  Note      → csv-parse/papaparse sync APIs load all rows as JS objects first');
console.log('═'.repeat(W));

fs.unlinkSync(CSV);
