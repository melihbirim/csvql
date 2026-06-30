'use strict';
/**
 * csvql vs csv-parse vs papaparse
 *
 * Run: node bench.js
 * Deps: npm install csv-parse papaparse  (already in package.json devDependencies)
 *
 * What this measures:
 *   - Time from "I have a CSV file path" to "I have results" (total wall time)
 *   - Same 4 real-world queries in each library
 *   - Lines of code needed to express each query
 *
 * Test data: 200 000 rows generated in /tmp (auto-cleaned up)
 */

const csvql   = require('./index.js');
const { parse } = require('csv-parse/sync');
const Papa    = require('papaparse');
const fs      = require('fs');
const path    = require('path');

// ── Generate test data ────────────────────────────────────────────────────────

const CSV = '/tmp/_csvql_bench.csv';
const ROWS = 200_000;
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
console.log(`done (${(fs.statSync(CSV).size / 1024 / 1024).toFixed(1)} MB)`);

// ── Benchmark helper ──────────────────────────────────────────────────────────

const W = 82;

function ms(fn) {
    const t = process.hrtime.bigint();
    const result = fn();
    return { ms: Number(process.hrtime.bigint() - t) / 1e6, result };
}

function row(label, t, rows, ref) {
    const speedup = ref ? (ref / t) : null;
    const speed   = speedup ? (speedup > 1 ? `  ${speedup.toFixed(1)}x faster` : `  ${(1/speedup).toFixed(1)}x slower`) : '';
    console.log(`  ${label.padEnd(28)} ${String(t.toFixed(1) + 'ms').padStart(9)}  ${String(rows + ' rows').padStart(9)}${speed}`);
}

function header(title) {
    console.log(`\n${'─'.repeat(W)}`);
    console.log(title);
    console.log('─'.repeat(W));
}

// ── QUERY 1: First 5 rows ─────────────────────────────────────────────────────

header('QUERY 1 — SELECT * LIMIT 5  (cold read, return first rows)');
console.log('  Code:');
console.log(`    csvql:      query("SELECT * FROM '...' LIMIT 5")                             // 1 line`);
console.log(`    csv-parse:  parse(fs.readFileSync(f), {columns:true, to:5})                  // 1 line`);
console.log(`    papaparse:  Papa.parse(fs.readFileSync(f,'utf8'), {header:true}).data.slice(0,5) // 1 line`);
console.log();

const q1_csvql = ms(() => csvql.query(`SELECT * FROM '${CSV}' LIMIT 5`));
const q1_csv   = ms(() => parse(fs.readFileSync(CSV), { columns: true, to: 5 }));
const q1_papa  = ms(() => Papa.parse(fs.readFileSync(CSV, 'utf8'), { header: true, preview: 5 }).data);

row('csvql',     q1_csvql.ms, q1_csvql.result.length, q1_csv.ms);
row('csv-parse', q1_csv.ms,   q1_csv.result.length);
row('papaparse', q1_papa.ms,  q1_papa.result.length,  q1_csv.ms);

// ── QUERY 2: GROUP BY (aggregation) ──────────────────────────────────────────

header('QUERY 2 — GROUP BY city COUNT(*)  (aggregation — scan full file)');
console.log('  Code:');
console.log(`    csvql:      query("SELECT city, COUNT(*) FROM '...' GROUP BY city")          // 1 line`);
console.log(`    csv-parse:  parse + reduce loop + Object.entries map                         // ~8 lines`);
console.log(`    papaparse:  Papa.parse + reduce loop + Object.entries map                    // ~8 lines`);
console.log();

const q2_csvql = ms(() => csvql.query(`SELECT city, COUNT(*) as n FROM '${CSV}' GROUP BY city`));

const q2_csv = ms(() => {
    const records = parse(fs.readFileSync(CSV), { columns: true });
    const counts = records.reduce((acc, r) => { acc[r.city] = (acc[r.city] || 0) + 1; return acc; }, {});
    return Object.entries(counts).map(([city, n]) => ({ city, n }));
});

const q2_papa = ms(() => {
    const records = Papa.parse(fs.readFileSync(CSV, 'utf8'), { header: true }).data;
    const counts = records.reduce((acc, r) => { acc[r.city] = (acc[r.city] || 0) + 1; return acc; }, {});
    return Object.entries(counts).map(([city, n]) => ({ city, n }));
});

row('csvql',     q2_csvql.ms, q2_csvql.result.length, q2_csv.ms);
row('csv-parse', q2_csv.ms,   q2_csv.result.length);
row('papaparse', q2_papa.ms,  q2_papa.result.length,  q2_csv.ms);

// ── QUERY 3: WHERE filter ─────────────────────────────────────────────────────

header("QUERY 3 — WHERE salary > 100000  (filter — early exit possible)");
console.log('  Code:');
console.log(`    csvql:      query("SELECT name, salary FROM '...' WHERE salary > 100000")    // 1 line`);
console.log(`    csv-parse:  parse full file + .filter(r => Number(r.salary) > 100000)        // 2 lines`);
console.log(`    papaparse:  Papa.parse + .filter(r => Number(r.salary) > 100000)             // 2 lines`);
console.log();

const q3_csvql = ms(() => csvql.query(`SELECT name, salary FROM '${CSV}' WHERE salary > 100000`));

const q3_csv = ms(() => {
    const records = parse(fs.readFileSync(CSV), { columns: true });
    return records.filter(r => Number(r.salary) > 100000);
});

const q3_papa = ms(() => {
    const records = Papa.parse(fs.readFileSync(CSV, 'utf8'), { header: true }).data;
    return records.filter(r => Number(r.salary) > 100000);
});

row('csvql',     q3_csvql.ms, q3_csvql.result.length, q3_csv.ms);
row('csv-parse', q3_csv.ms,   q3_csv.result.length);
row('papaparse', q3_papa.ms,  q3_papa.result.length,  q3_csv.ms);

// ── QUERY 4: ORDER BY LIMIT ───────────────────────────────────────────────────

header('QUERY 4 — ORDER BY salary DESC LIMIT 10  (top-N)');
console.log('  Code:');
console.log(`    csvql:      query("SELECT name, salary FROM '...' ORDER BY salary DESC LIMIT 10")  // 1 line`);
console.log(`    csv-parse:  parse + sort((a,b) => b.salary - a.salary) + slice(0, 10)             // 3 lines`);
console.log(`    papaparse:  Papa.parse + sort((a,b) => b.salary - a.salary) + slice(0, 10)        // 3 lines`);
console.log();

const q4_csvql = ms(() => csvql.query(`SELECT name, salary FROM '${CSV}' ORDER BY salary DESC LIMIT 10`));

const q4_csv = ms(() => {
    const records = parse(fs.readFileSync(CSV), { columns: true });
    return records.sort((a, b) => Number(b.salary) - Number(a.salary)).slice(0, 10);
});

const q4_papa = ms(() => {
    const records = Papa.parse(fs.readFileSync(CSV, 'utf8'), { header: true }).data;
    return records.sort((a, b) => Number(b.salary) - Number(a.salary)).slice(0, 10);
});

row('csvql',     q4_csvql.ms, q4_csvql.result.length, q4_csv.ms);
row('csv-parse', q4_csv.ms,   q4_csv.result.length);
row('papaparse', q4_papa.ms,  q4_papa.result.length,  q4_csv.ms);

// ── Feature comparison ────────────────────────────────────────────────────────

console.log(`\n${'═'.repeat(W)}`);
console.log('FEATURE COMPARISON');
console.log('═'.repeat(W));

const features = [
    ['SQL WHERE / filter',        '✓', '✗ manual .filter()', '✗ manual .filter()'],
    ['SQL GROUP BY / aggregate',  '✓', '✗ manual reduce',    '✗ manual reduce'],
    ['SQL ORDER BY / sort',       '✓', '✗ manual .sort()',   '✗ manual .sort()'],
    ['SQL JOIN across files',     '✓', '✗',                  '✗'],
    ['SQL LIMIT (early exit)',     '✓', '✗ loads full file',  '✗ loads full file'],
    ['Custom delimiter (TSV etc)', '✗', '✓',                  '✓'],
    ['CSV writing (unparse)',      '✗', '✗',                  '✓'],
    ['Streaming API',              '✗', '✓',                  '✓'],
    ['Browser support',            '✗', '✗',                  '✓'],
    ['SIMD parsing',               '✓', '✗',                  '✗'],
    ['Low memory (no full load)',  '✓', '✗',                  '✗'],
    ['TypeScript types',           '✓', '✓',                  '✓'],
];

console.log(`  ${'Feature'.padEnd(32)} ${'csvql'.padEnd(18)} ${'csv-parse'.padEnd(22)} papaparse`);
console.log(`  ${'-'.repeat(32)} ${'-'.repeat(18)} ${'-'.repeat(22)} ---------`);
for (const [feat, a, b, c] of features) {
    console.log(`  ${feat.padEnd(32)} ${String(a).padEnd(18)} ${String(b).padEnd(22)} ${c}`);
}

console.log(`\n${'═'.repeat(W)}`);
console.log('WHEN TO USE WHAT');
console.log('═'.repeat(W));
console.log('  csvql     → SQL queries on CSV (WHERE, GROUP BY, ORDER BY, JOIN) — fastest for analytics');
console.log('  csv-parse → Raw CSV parsing with full control, streaming, custom delimiters');
console.log('  papaparse → Browser + Node, CSV writing, streaming, simple API');
console.log('  Hybrid    → csvql to filter/aggregate large files, then csv-parse for custom processing');
console.log('═'.repeat(W));

// Cleanup
fs.unlinkSync(CSV);
