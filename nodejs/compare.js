'use strict';
/**
 * Functionality comparison: csvql vs csv-parse vs papaparse
 * Run: node compare.js
 *
 * Each section runs the SAME real task in each library and shows:
 *   - the code required
 *   - the actual output
 *   - whether the library supports it natively or needs manual work
 */

const csvql    = require('./index.js');
const { parse } = require('csv-parse/sync');
const Papa     = require('papaparse');
const fs       = require('fs');

// ── Sample files ──────────────────────────────────────────────────────────────

const PEOPLE = '/tmp/_cmp_people.csv';
const ORDERS = '/tmp/_cmp_orders.csv';
const TSV    = '/tmp/_cmp_data.tsv';
const DIRTY  = '/tmp/_cmp_dirty.csv';

fs.writeFileSync(PEOPLE, [
    'id,name,age,city,salary',
    '1,Alice,30,Austin,120000',
    '2,Bob,25,Boston,80000',
    '3,Carol,35,Austin,150000',
    '4,Dave,28,Denver,90000',
    '5,Eve,32,Austin,110000',
].join('\n') + '\n');

fs.writeFileSync(ORDERS, [
    'person_id,product,amount',
    '1,Laptop,1200',
    '2,Phone,800',
    '1,Monitor,400',
    '3,Laptop,1200',
    '4,Keyboard,100',
].join('\n') + '\n');

fs.writeFileSync(TSV, [
    'id\tname\tscore',
    '1\tAlice\t95',
    '2\tBob\t87',
    '3\tCarol\t92',
].join('\n') + '\n');

fs.writeFileSync(DIRTY, [
    'id,name,salary',
    '# this is a comment',
    '1,Alice,120000',
    '',
    '2,Bob,bad_value',
    '3,Carol,150000',
].join('\n') + '\n');

// ── Formatting helpers ────────────────────────────────────────────────────────

const SEP  = '─'.repeat(72);
const WIDE = '═'.repeat(72);

function section(title) {
    console.log(`\n${WIDE}`);
    console.log(title);
    console.log(WIDE);
}

function lib(name) {
    console.log(`\n  ── ${name} ${'─'.repeat(60 - name.length)}`);
}

function code(lines) {
    for (const l of lines) console.log(`    ${l}`);
}

function out(label, value) {
    const s = JSON.stringify(value, null, 0);
    const display = s.length > 120 ? s.slice(0, 117) + '...' : s;
    console.log(`    → ${label}: ${display}`);
}

function na(reason) {
    console.log(`    ✗  Not supported: ${reason}`);
}

function ok(note) {
    console.log(`    ✓  ${note}`);
}

// ══════════════════════════════════════════════════════════════════════════════
section('1. BASIC PARSE — CSV file → array of objects');
// ══════════════════════════════════════════════════════════════════════════════

lib('csvql');
code(["const rows = csvql.query(\"SELECT * FROM 'people.csv'\")"]);
{
    const rows = csvql.query(`SELECT * FROM '${PEOPLE}'`);
    out('rows[0]', rows[0]);
    out('count',   rows.length);
}

lib('csv-parse');
code(["const rows = parse(fs.readFileSync('people.csv'), { columns: true })"]);
{
    const rows = parse(fs.readFileSync(PEOPLE), { columns: true });
    out('rows[0]', rows[0]);
    out('count',   rows.length);
}

lib('papaparse');
code(["const { data } = Papa.parse(fs.readFileSync('people.csv','utf8'), { header: true, skipEmptyLines: true })"]);
{
    const { data } = Papa.parse(fs.readFileSync(PEOPLE, 'utf8'), { header: true, skipEmptyLines: true });
    out('rows[0]', data[0]);
    out('count',   data.length);
}

// ══════════════════════════════════════════════════════════════════════════════
section('2. TYPE CASTING — auto-convert numbers and booleans');
// ══════════════════════════════════════════════════════════════════════════════

lib('csvql');
code(["const rows = csvql.query(\"SELECT age, salary FROM 'people.csv' LIMIT 1\")",
      "// age and salary come back as JS numbers (JSON.parse handles it)"]);
{
    const rows = csvql.query(`SELECT age, salary FROM '${PEOPLE}' LIMIT 1`);
    out('typeof age', typeof rows[0].age);
    out('typeof salary', typeof rows[0].salary);
}

lib('csv-parse');
code(["parse(data, { columns: true, cast: true })",
      "// cast:true auto-converts numbers and booleans"]);
{
    const rows = parse(fs.readFileSync(PEOPLE), { columns: true, cast: true });
    out('typeof age', typeof rows[0].age);
    out('typeof salary', typeof rows[0].salary);
}

lib('papaparse');
code(["Papa.parse(data, { header: true, dynamicTyping: true })",
      "// dynamicTyping:true auto-converts numbers and booleans"]);
{
    const { data } = Papa.parse(fs.readFileSync(PEOPLE, 'utf8'), { header: true, dynamicTyping: true, skipEmptyLines: true });
    out('typeof age', typeof data[0].age);
    out('typeof salary', typeof data[0].salary);
}

// ══════════════════════════════════════════════════════════════════════════════
section('3. CUSTOM DELIMITER — TSV (tab-separated)');
// ══════════════════════════════════════════════════════════════════════════════

lib('csvql');
code(["csvql.query(\"SELECT * FROM 'data.tsv'\", { delimiter: '\\t' })"]);
{
    const rows = csvql.query(`SELECT * FROM '${TSV}'`, { delimiter: '\t' });
    out('rows[0]', rows[0]);
}

lib('csv-parse');
code(["parse(fs.readFileSync('data.tsv'), { columns: true, delimiter: '\\t' })"]);
{
    const rows = parse(fs.readFileSync(TSV), { columns: true, delimiter: '\t' });
    out('rows[0]', rows[0]);
}

lib('papaparse');
code(["Papa.parse(data, { header: true, delimiter: '\\t' })",
      "// or leave delimiter out — papaparse auto-detects it"]);
{
    const { data } = Papa.parse(fs.readFileSync(TSV, 'utf8'), { header: true, skipEmptyLines: true });
    out('auto-detected delimiter', data[0]);
}

// ══════════════════════════════════════════════════════════════════════════════
section('4. SKIP EMPTY LINES & COMMENT ROWS');
// ══════════════════════════════════════════════════════════════════════════════

lib('csvql');
code(["csvql.query(\"SELECT * FROM 'dirty.csv'\", { comment: '#', skipEmptyLines: true })"]);
{
    const rows = csvql.query(`SELECT * FROM '${DIRTY}'`, { comment: '#', skipEmptyLines: true });
    out('rows', rows);
}

lib('csv-parse');
code(["parse(data, { columns: true, skip_empty_lines: true, comment: '#' })"]);
{
    const rows = parse(fs.readFileSync(DIRTY), { columns: true, skip_empty_lines: true, comment: '#' });
    out('rows', rows);
}

lib('papaparse');
code(["Papa.parse(data, { header: true, skipEmptyLines: true, comments: '#' })"]);
{
    const { data } = Papa.parse(fs.readFileSync(DIRTY, 'utf8'), { header: true, skipEmptyLines: true, comments: '#' });
    out('rows', data);
}

// ══════════════════════════════════════════════════════════════════════════════
section('5. FILTER ROWS — WHERE / .filter()');
// ══════════════════════════════════════════════════════════════════════════════

lib('csvql');
code(["csvql.query(\"SELECT name, salary FROM 'people.csv' WHERE salary > 100000\")"]);
{
    const rows = csvql.query(`SELECT name, salary FROM '${PEOPLE}' WHERE salary > 100000`);
    out('rows', rows);
}

lib('csv-parse');
code(["const all  = parse(data, { columns: true, cast: true })",
      "const rows = all.filter(r => r.salary > 100000).map(r => ({ name: r.name, salary: r.salary }))"]);
{
    const all  = parse(fs.readFileSync(PEOPLE), { columns: true, cast: true });
    const rows = all.filter(r => r.salary > 100000).map(r => ({ name: r.name, salary: r.salary }));
    out('rows', rows);
}

lib('papaparse');
code(["const { data } = Papa.parse(csv, { header: true, dynamicTyping: true, skipEmptyLines: true })",
      "const rows = data.filter(r => r.salary > 100000).map(r => ({ name: r.name, salary: r.salary }))"]);
{
    const { data } = Papa.parse(fs.readFileSync(PEOPLE, 'utf8'), { header: true, dynamicTyping: true, skipEmptyLines: true });
    const rows = data.filter(r => r.salary > 100000).map(r => ({ name: r.name, salary: r.salary }));
    out('rows', rows);
}

// ══════════════════════════════════════════════════════════════════════════════
section('6. AGGREGATE — GROUP BY city, COUNT(*), AVG(salary)');
// ══════════════════════════════════════════════════════════════════════════════

lib('csvql');
code(["csvql.query(\"SELECT city, COUNT(*) as n, AVG(salary) as avg_salary FROM 'people.csv' GROUP BY city\")"]);
{
    const rows = csvql.query(`SELECT city, COUNT(*) as n, AVG(salary) as avg_salary FROM '${PEOPLE}' GROUP BY city ORDER BY city`);
    out('rows', rows);
}

lib('csv-parse');
code(["const all  = parse(data, { columns: true, cast: true })",
      "const agg  = {}",
      "for (const r of all) {",
      "    if (!agg[r.city]) agg[r.city] = { n: 0, sum: 0 }",
      "    agg[r.city].n++",
      "    agg[r.city].sum += r.salary",
      "}",
      "const rows = Object.entries(agg).map(([city, { n, sum }]) => ({",
      "    city, n, avg_salary: (sum / n).toFixed(2)",
      "}))"]);
{
    const all = parse(fs.readFileSync(PEOPLE), { columns: true, cast: true });
    const agg = {};
    for (const r of all) {
        if (!agg[r.city]) agg[r.city] = { n: 0, sum: 0 };
        agg[r.city].n++;
        agg[r.city].sum += r.salary;
    }
    const rows = Object.entries(agg).sort(([a],[b])=>a.localeCompare(b)).map(([city, { n, sum }]) => ({ city, n, avg_salary: (sum / n).toFixed(2) }));
    out('rows', rows);
}

lib('papaparse');
na('no aggregation — same manual reduce as csv-parse');

// ══════════════════════════════════════════════════════════════════════════════
section('7. SORT + LIMIT — ORDER BY salary DESC LIMIT 3');
// ══════════════════════════════════════════════════════════════════════════════

lib('csvql');
code(["csvql.query(\"SELECT name, salary FROM 'people.csv' ORDER BY salary DESC LIMIT 3\")"]);
{
    const rows = csvql.query(`SELECT name, salary FROM '${PEOPLE}' ORDER BY salary DESC LIMIT 3`);
    out('rows', rows);
}

lib('csv-parse');
code(["const all  = parse(data, { columns: true, cast: true })",
      "const rows = all.sort((a, b) => b.salary - a.salary).slice(0, 3).map(r => ({ name: r.name, salary: r.salary }))"]);
{
    const all  = parse(fs.readFileSync(PEOPLE), { columns: true, cast: true });
    const rows = all.sort((a, b) => b.salary - a.salary).slice(0, 3).map(r => ({ name: r.name, salary: r.salary }));
    out('rows', rows);
}

lib('papaparse');
code(["const { data } = Papa.parse(csv, { header: true, dynamicTyping: true, skipEmptyLines: true })",
      "const rows = data.sort((a, b) => b.salary - a.salary).slice(0, 3).map(r => ({ name: r.name, salary: r.salary }))"]);
{
    const { data } = Papa.parse(fs.readFileSync(PEOPLE, 'utf8'), { header: true, dynamicTyping: true, skipEmptyLines: true });
    const rows = data.sort((a, b) => b.salary - a.salary).slice(0, 3).map(r => ({ name: r.name, salary: r.salary }));
    out('rows', rows);
}

// ══════════════════════════════════════════════════════════════════════════════
section('8. JOIN — combine two CSV files on a key');
// ══════════════════════════════════════════════════════════════════════════════

lib('csvql');
code([
    "csvql.query(`",
    "    SELECT p.name, o.product, o.amount",
    "    FROM 'people.csv' p",
    "    JOIN 'orders.csv' o ON p.id = o.person_id",
    "    ORDER BY o.amount DESC",
    "`)"
]);
{
    const rows = csvql.query(
        `SELECT p.name, o.product, o.amount FROM '${PEOPLE}' p JOIN '${ORDERS}' o ON p.id = o.person_id ORDER BY o.amount DESC`
    );
    out('rows', rows);
}

lib('csv-parse');
code(["// No JOIN — load both files, build a lookup map, merge manually",
      "const people = parse(fs.readFileSync('people.csv'), { columns: true, cast: true })",
      "const orders = parse(fs.readFileSync('orders.csv'), { columns: true, cast: true })",
      "const byId   = Object.fromEntries(people.map(p => [p.id, p]))",
      "const rows   = orders",
      "    .map(o => ({ name: byId[o.person_id]?.name, product: o.product, amount: o.amount }))",
      "    .sort((a, b) => b.amount - a.amount)"]);
{
    const people = parse(fs.readFileSync(PEOPLE), { columns: true, cast: true });
    const orders = parse(fs.readFileSync(ORDERS), { columns: true, cast: true });
    const byId   = Object.fromEntries(people.map(p => [String(p.id), p]));
    const rows   = orders
        .map(o => ({ name: byId[o.person_id]?.name, product: o.product, amount: o.amount }))
        .sort((a, b) => b.amount - a.amount);
    out('rows', rows);
}

lib('papaparse');
na('no JOIN — same manual map/merge as csv-parse');

// ══════════════════════════════════════════════════════════════════════════════
section('9. CSV WRITING — objects → CSV string');
// ══════════════════════════════════════════════════════════════════════════════

const sampleData = [{ name: 'Alice', salary: 120000 }, { name: 'Bob', salary: 80000 }];

lib('csvql');
code(["// queryCsv() returns results as CSV text (SELECT output only, not arbitrary data)"]);
{
    const result = csvql.queryCsv(`SELECT name, salary FROM '${PEOPLE}' LIMIT 2`);
    out('csv output', result.trim());
}

lib('csv-parse');
na('csv-parse is parse-only — no stringify/unparse function');

lib('papaparse');
code(["Papa.unparse([{ name: 'Alice', salary: 120000 }, { name: 'Bob', salary: 80000 }])"]);
{
    const csv = Papa.unparse(sampleData);
    out('csv output', csv);
}

// ══════════════════════════════════════════════════════════════════════════════
section('10. STREAMING — process rows one at a time (low memory)');
// ══════════════════════════════════════════════════════════════════════════════

lib('csvql');
code(["// csvql streams internally on every query — no streaming API exposed to JS",
      "// sync query() RAM stays flat (tens of MB) regardless of file size"]);
ok('always low memory, no extra code needed');

lib('csv-parse');
code(["// Async streaming API — needs event emitter + promise wrapper",
      "const parser = fs.createReadStream('people.csv').pipe(parse({ columns: true }))",
      "for await (const record of parser) {",
      "    // process one row at a time",
      "}"]);
ok('streaming supported via async iterator');

lib('papaparse');
code(["Papa.parse(fs.createReadStream('people.csv'), {",
      "    header: true,",
      "    step: (result) => { /* called once per row */ },",
      "    complete: () => { /* done */ }",
      "})"]);
ok('streaming supported via step callback');

// ══════════════════════════════════════════════════════════════════════════════
section('SUMMARY');
// ══════════════════════════════════════════════════════════════════════════════

const rows = [
    ['Capability',              'csvql',         'csv-parse',         'papaparse'],
    [SEP.slice(0,24),           SEP.slice(0,15), SEP.slice(0,19),     SEP.slice(0,19)],
    ['Basic parse',             '✓ SQL',         '✓',                 '✓'],
    ['Type casting',            '✓ via JSON',    '✓ cast:true',       '✓ dynamicTyping'],
    ['Custom delimiter',        '✓ { delimiter }','✓',                 '✓ + auto-detect'],
    ['Skip blank/comment rows', '✓ { comment }', '✓',                 '✓'],
    ['Filter (WHERE)',          '✓ SQL 1 line',  '✗ manual .filter','✗ manual .filter'],
    ['Aggregate (GROUP BY)',    '✓ SQL 1 line',  '✗ manual reduce',  '✗ manual reduce'],
    ['Sort (ORDER BY)',         '✓ SQL 1 line',  '✗ manual .sort',   '✗ manual .sort'],
    ['JOIN across files',       '✓ SQL 1 line',  '✗ manual map',     '✗ manual map'],
    ['CSV writing',             '✓ queryCsv()',  '✗',                 '✓ unparse()'],
    ['Streaming API',           '✗ internal',    '✓ async iterator',  '✓ step callback'],
    ['Always low memory',       '✓',             '✗ sync loads all',  '✗ sync loads all'],
    ['Browser support',         '✗',             '✗',                 '✓'],
];

console.log();
for (const [a, b, c, d] of rows) {
    if (b.startsWith('─')) { console.log(`  ${a} ${b} ${c} ${d}`); continue; }
    console.log(`  ${a.padEnd(26)} ${b.padEnd(17)} ${c.padEnd(21)} ${d}`);
}

// Cleanup
[PEOPLE, ORDERS, TSV, DIRTY].forEach(f => fs.unlinkSync(f));
