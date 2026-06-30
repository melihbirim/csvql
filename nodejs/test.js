'use strict';

const csvql = require('./index.js');
const path = require('path');
const fs = require('fs');

// Generate a tiny test CSV in /tmp
const csv = '/tmp/csvql_test.csv';
fs.writeFileSync(csv, [
    'id,name,age,city,salary,department',
    '1,Alice,30,Austin,120000,Engineering',
    '2,Bob,25,Boston,80000,Marketing',
    '3,Carol,35,Austin,150000,Engineering',
    '4,Dave,28,Denver,90000,Marketing',
    '5,Eve,32,Austin,110000,Engineering',
].join('\n') + '\n');

let passed = 0;

function check(label, actual, expected) {
    const ok = JSON.stringify(actual) === JSON.stringify(expected);
    console.log(`${ok ? '✓' : '✗'} ${label}`);
    if (!ok) {
        console.log('  expected:', JSON.stringify(expected));
        console.log('  actual:  ', JSON.stringify(actual));
        process.exitCode = 1;
    } else {
        passed++;
    }
}

// LIMIT
const rows = csvql.query(`SELECT * FROM '${csv}' LIMIT 2`);
check('LIMIT 2 returns 2 rows', rows.length, 2);
check('first row name', rows[0].name, 'Alice');

// WHERE
const filtered = csvql.query(`SELECT name, salary FROM '${csv}' WHERE salary > 100000`);
check('WHERE salary > 100000 → 3 rows', filtered.length, 3);

// GROUP BY
const groups = csvql.query(`SELECT city, COUNT(*) as n FROM '${csv}' GROUP BY city`);
check('GROUP BY city → 3 groups', groups.length, 3);
const austin = groups.find(r => r.city === 'Austin');
check('Austin has 3 rows', austin && Number(austin.n), 3);

// ORDER BY
const sorted = csvql.query(`SELECT name, salary FROM '${csv}' ORDER BY salary DESC LIMIT 1`);
check('top earner is Carol', sorted[0].name, 'Carol');

// queryCsv
const csv_out = csvql.queryCsv(`SELECT name FROM '${csv}' LIMIT 1`);
check('queryCsv returns string with header', csv_out.startsWith('name'), true);

// Error handling
try {
    csvql.query('SELECT * FROM \'/tmp/no_such_file.csv\'');
    check('missing file throws', false, true);
} catch (e) {
    check('missing file throws', true, true);
}

// Custom delimiter (TSV)
const tsv = '/tmp/csvql_test.tsv';
fs.writeFileSync(tsv, 'id\tname\tscore\n1\tAlice\t95\n2\tBob\t87\n3\tCarol\t92\n');
const tsv_rows = csvql.query(`SELECT * FROM '${tsv}'`, { delimiter: '\t' });
check('TSV: row count', tsv_rows.length, 3);
check('TSV: first name', tsv_rows[0].name, 'Alice');
check('TSV: WHERE works after conversion', csvql.query(`SELECT name FROM '${tsv}' WHERE score > 90`, { delimiter: '\t' }).length, 2);
fs.unlinkSync(tsv);

// Comment + skipEmptyLines
const dirty = '/tmp/csvql_test_dirty.csv';
fs.writeFileSync(dirty, '# header comment\nid,name\n\n1,Alice\n# mid comment\n2,Bob\n');
const clean_rows = csvql.query(`SELECT * FROM '${dirty}'`, { comment: '#', skipEmptyLines: true });
check('comment: # rows stripped', clean_rows.length, 2);
check('comment: first id', clean_rows[0].id, 1);
fs.unlinkSync(dirty);

// find() — no SQL
const find_all = csvql.find(csv);
check('find: all rows', find_all.length, 5);

const find_cols = csvql.find(csv, { columns: ['name', 'salary'], where: 'salary>100000' });
check('find: columns + where', find_cols.length, 3);
check('find: only requested columns', Object.keys(find_cols[0]).join(','), 'name,salary');

const find_and = csvql.find(csv, { where: 'department=Engineering AND salary>100000', orderBy: 'salary:desc', limit: 1 });
check('find: AND + orderBy + limit', find_and[0].name, 'Carol');

const find_or = csvql.find(csv, { columns: 'name,city', where: 'city=Austin OR city=Boston' });
check('find: OR condition', find_or.length, 4);

console.log(`\n${passed}/15 tests passed`);

fs.unlinkSync(csv);
