'use strict';
/**
 * csvql Node.js — usage examples
 * Run: node example.js
 */

const csvql = require('./index.js');
const fs    = require('fs');

// ── Sample data ───────────────────────────────────────────────────────────────

const CSV = '/tmp/_csvql_example.csv';
const TSV = '/tmp/_csvql_example.tsv';
const DIRTY = '/tmp/_csvql_example_dirty.csv';
const ORDERS = '/tmp/_csvql_example_orders.csv';

fs.writeFileSync(CSV, [
    'id,name,age,city,salary,department',
    '1,Alice,30,Austin,120000,Engineering',
    '2,Bob,25,Boston,80000,Marketing',
    '3,Carol,35,Austin,150000,Engineering',
    '4,Dave,28,Denver,90000,Marketing',
    '5,Eve,32,Austin,110000,Engineering',
    '6,Frank,40,Boston,130000,Finance',
    '7,Grace,27,Denver,75000,Marketing',
].join('\n') + '\n');

fs.writeFileSync(TSV, [
    'id\tname\tscore\tgrade',
    '1\tAlice\t95\tA',
    '2\tBob\t87\tB',
    '3\tCarol\t92\tA',
    '4\tDave\t78\tC',
].join('\n') + '\n');

fs.writeFileSync(DIRTY, [
    '# exported from HR system 2024-01-15',
    '# columns: id, name, salary',
    'id,name,salary',
    '',
    '1,Alice,120000',
    '2,Bob,80000',
    '',
    '# end of file',
    '3,Carol,150000',
].join('\n') + '\n');

fs.writeFileSync(ORDERS, [
    'person_id,product,amount',
    '1,Laptop,1200',
    '2,Phone,800',
    '1,Monitor,400',
    '3,Laptop,1200',
    '6,Keyboard,100',
].join('\n') + '\n');

function log(title, result) {
    console.log(`\n── ${title}`);
    if (typeof result === 'string') {
        console.log(result);
    } else {
        console.log(JSON.stringify(result, null, 2));
    }
}

// ── 1. Basic SELECT ───────────────────────────────────────────────────────────

log(
    'SELECT * LIMIT 3',
    csvql.query(`SELECT * FROM '${CSV}' LIMIT 3`)
);

// ── 2. WHERE filter ───────────────────────────────────────────────────────────

log(
    'WHERE salary > 100000',
    csvql.query(`SELECT name, city, salary FROM '${CSV}' WHERE salary > 100000 ORDER BY salary DESC`)
);

// ── 3. GROUP BY + aggregate ───────────────────────────────────────────────────

log(
    'GROUP BY city — COUNT + AVG salary',
    csvql.query(`
        SELECT city, COUNT(*) as headcount, AVG(salary) as avg_salary
        FROM '${CSV}'
        GROUP BY city
        ORDER BY avg_salary DESC
    `)
);

// ── 4. ORDER BY LIMIT — top-N ─────────────────────────────────────────────────

log(
    'Top 3 earners',
    csvql.query(`SELECT name, department, salary FROM '${CSV}' ORDER BY salary DESC LIMIT 3`)
);

// ── 5. JOIN two files ─────────────────────────────────────────────────────────

log(
    'JOIN employees + orders',
    csvql.query(`
        SELECT e.name, o.product, o.amount
        FROM '${CSV}' e
        JOIN '${ORDERS}' o ON e.id = o.person_id
        ORDER BY o.amount DESC
    `)
);

// ── 6. TSV file (custom delimiter) ────────────────────────────────────────────

log(
    'TSV — query with { delimiter: "\\t" }',
    csvql.query(`SELECT name, score, grade FROM '${TSV}' WHERE score >= 90 ORDER BY score DESC`, {
        delimiter: '\t',
    })
);

// ── 7. Skip comment rows and blank lines ──────────────────────────────────────

log(
    'Skip # comments and blank lines',
    csvql.query(`SELECT * FROM '${DIRTY}'`, {
        comment: '#',
        skipEmptyLines: true,
    })
);

// ── 8. queryCsv — return CSV string ──────────────────────────────────────────

log(
    'queryCsv — city summary as CSV text',
    csvql.queryCsv(`SELECT city, COUNT(*) as n FROM '${CSV}' GROUP BY city ORDER BY n DESC`)
);

// ── 9. queryCsv — write result to file ───────────────────────────────────────

const OUT = '/tmp/_csvql_out.csv';
const csv = csvql.queryCsv(`SELECT name, salary FROM '${CSV}' WHERE salary > 100000 ORDER BY salary DESC`);
fs.writeFileSync(OUT, csv);
console.log(`\n── Written to ${OUT}`);
console.log(fs.readFileSync(OUT, 'utf8'));

// Cleanup
[CSV, TSV, DIRTY, ORDERS, OUT].forEach(f => fs.unlinkSync(f));
