'use strict';
/**
 * ETL pattern: CSV → filter → insert into SQLite database
 *
 * Shows the difference between:
 *   csv-parse: loads ALL rows into memory first, then you filter in JS
 *   csvql:     you write the filter in SQL — only matching rows ever leave the engine
 *
 * Run: node --experimental-sqlite example_etl.js
 */

const { DatabaseSync } = require('node:sqlite');
const { parse }        = require('csv-parse/sync');
const csvql            = require('./index.js');
const fs               = require('fs');

// ── Generate 100k row CSV ─────────────────────────────────────────────────────

const CSV    = '/tmp/_etl_employees.csv';
const ROWS   = 100_000;
const CITIES = ['Austin', 'Boston', 'Chicago', 'Denver', 'Eugene'];
const DEPTS  = ['Engineering', 'Marketing', 'Finance', 'HR', 'Sales'];

process.stdout.write(`Generating ${ROWS.toLocaleString()} row CSV... `);
{
    const lines = ['id,name,age,city,salary,department,active'];
    for (let i = 1; i <= ROWS; i++) {
        lines.push([
            i,
            `Person${i}`,
            20 + (i % 45),
            CITIES[i % CITIES.length],
            40000 + (i * 97) % 120000,
            DEPTS[i % DEPTS.length],
            i % 4 !== 0 ? 'true' : 'false',   // ~75% active
        ].join(','));
    }
    fs.writeFileSync(CSV, lines.join('\n') + '\n');
}
console.log('done\n');

// ── Shared: create SQLite tables ──────────────────────────────────────────────

function makeDb(path) {
    const db = new DatabaseSync(path);
    db.exec(`CREATE TABLE IF NOT EXISTS employees (
        id INTEGER PRIMARY KEY,
        name TEXT, age INTEGER, city TEXT, salary INTEGER, department TEXT
    )`);
    return db;
}

function batchInsert(db, rows) {
    const stmt = db.prepare(
        'INSERT OR REPLACE INTO employees (id, name, age, city, salary, department) VALUES (?,?,?,?,?,?)'
    );
    db.exec('BEGIN');
    for (const r of rows)
        stmt.run(Number(r.id), r.name, Number(r.age), r.city, Number(r.salary), r.department);
    db.exec('COMMIT');
}

// ── Approach A: csv-parse ─────────────────────────────────────────────────────
// csv-parse has no query language. To insert only active Engineering employees
// earning > $80k you must: load ALL rows → materialise ALL as JS objects → filter in JS → insert.

console.log('══════════════════════════════════════════════════════════════');
console.log('Approach A — csv-parse');
console.log('══════════════════════════════════════════════════════════════');
console.log('Code:');
console.log("  const all  = parse(fs.readFileSync('employees.csv'), { columns: true, cast: true })");
console.log("  const rows = all.filter(r => r.department === 'Engineering'");
console.log("                           && r.active === true");
console.log("                           && r.salary > 80000)");
console.log("  batchInsert(db, rows)");
console.log();

if (global.gc) global.gc();
const heapBefore_csv = process.memoryUsage().heapUsed;
const t_csv = process.hrtime.bigint();

const DB_CSV = '/tmp/_etl_csvparse.db';
if (fs.existsSync(DB_CSV)) fs.unlinkSync(DB_CSV);
const db_csv = makeDb(DB_CSV);

const all  = parse(fs.readFileSync(CSV), { columns: true, cast: true });
// Gotcha: cast:true converts numbers but NOT boolean strings.
// 'true' stays as the string 'true', so === true is always false.
// You must compare against the string, or write a custom cast function.
const rows = all.filter(r =>
    r.department === 'Engineering' && r.active === 'true' && r.salary > 80000
);
batchInsert(db_csv, rows);

const ms_csv   = Number(process.hrtime.bigint() - t_csv) / 1e6;
const heap_csv = (process.memoryUsage().heapUsed - heapBefore_csv) / 1024 / 1024;

console.log(`  Rows loaded into memory : ${all.length.toLocaleString()}  (entire file)`);
console.log(`  Rows inserted into DB   : ${rows.length.toLocaleString()}  (after JS filter)`);
console.log(`  Note: cast:true converts numbers but NOT 'true'/'false' strings.`);
console.log(`        r.active === true always fails — must use r.active === 'true'`);
console.log(`  Time                    : ${ms_csv.toFixed(0)}ms`);
console.log(`  Heap allocated          : +${heap_csv.toFixed(0)} MB`);

// ── Approach B: csvql ─────────────────────────────────────────────────────────
// csvql runs the filter inside the Zig engine. Only matching rows ever come
// back to JS — you never hold the full dataset in memory.

console.log('\n══════════════════════════════════════════════════════════════');
console.log('Approach B — csvql');
console.log('══════════════════════════════════════════════════════════════');
console.log('Code:');
console.log("  const rows = csvql.query(`");
console.log("      SELECT id, name, age, city, salary, department");
console.log("      FROM 'employees.csv'");
console.log("      WHERE department = 'Engineering'");
console.log("        AND active = 'true'");
console.log("        AND salary > 80000");
console.log("  `)");
console.log("  batchInsert(db, rows)");
console.log();

if (global.gc) global.gc();
const heapBefore_q = process.memoryUsage().heapUsed;
const t_q = process.hrtime.bigint();

const DB_Q = '/tmp/_etl_csvql.db';
if (fs.existsSync(DB_Q)) fs.unlinkSync(DB_Q);
const db_q = makeDb(DB_Q);

const filtered = csvql.query(`
    SELECT id, name, age, city, salary, department
    FROM '${CSV}'
    WHERE department = 'Engineering'
      AND active = 'true'
      AND salary > 80000
`);
batchInsert(db_q, filtered);

const ms_q   = Number(process.hrtime.bigint() - t_q) / 1e6;
const heap_q = (process.memoryUsage().heapUsed - heapBefore_q) / 1024 / 1024;

console.log(`  Rows loaded into memory : ${filtered.length.toLocaleString()}  (only matching rows)`);
console.log(`  Rows inserted into DB   : ${filtered.length.toLocaleString()}`);
console.log(`  Time                    : ${ms_q.toFixed(0)}ms`);
console.log(`  Heap allocated          : +${Math.max(0, heap_q).toFixed(0)} MB`);

// ── Verify both DBs have identical data ───────────────────────────────────────

const count_csv = db_csv.prepare('SELECT COUNT(*) as n FROM employees').get().n;
const count_q   = db_q.prepare('SELECT COUNT(*) as n FROM employees').get().n;

console.log('\n══════════════════════════════════════════════════════════════');
console.log('Comparison');
console.log('══════════════════════════════════════════════════════════════');
console.log(`  ${''.padEnd(26)} ${'csv-parse'.padEnd(14)} csvql`);
console.log(`  ${'-'.repeat(26)} ${'-'.repeat(14)} -----`);
console.log(`  ${'Rows read into JS'.padEnd(26)} ${String(all.length.toLocaleString()).padEnd(14)} ${filtered.length.toLocaleString()} (filtered by engine)`);
console.log(`  ${'Rows inserted'.padEnd(26)} ${String(count_csv).padEnd(14)} ${count_q}`);
console.log(`  ${'Time'.padEnd(26)} ${String(ms_csv.toFixed(0) + 'ms').padEnd(14)} ${ms_q.toFixed(0)}ms`);
console.log(`  ${'Heap'.padEnd(26)} ${String('+' + heap_csv.toFixed(0) + ' MB').padEnd(14)} +${Math.max(0, heap_q).toFixed(0)} MB`);
console.log(`  ${'Results match'.padEnd(26)} ${count_csv === count_q ? '✓ identical row count' : '✗ mismatch — see note above'}`);

console.log(`
Key difference:
  csv-parse has no query language — it is a parser only. To filter you
  must first materialise the entire file as JS objects, then run .filter().
  For 100k rows that means ${all.length.toLocaleString()} objects in the heap before a single
  row goes into the database.

  csvql pushes the WHERE clause into the engine. Only the ${filtered.length.toLocaleString()} matching
  rows are returned to JS, so the heap cost scales with the result set,
  not the source file size. On a 10 GB CSV, heap stays the same.
`);

// Cleanup
[CSV, DB_CSV, DB_Q].forEach(f => fs.unlinkSync(f));
