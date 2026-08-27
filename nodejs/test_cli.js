'use strict';

// CLI tests — the `npx csvql-query` entry point.
//
// The exit codes matter as much as the output here: they are the contract a
// shell script or an AI agent branches on, and they mirror the standalone
// binary's (0 success, 1 usage, 2 query error, 3 file/IO error). A silent
// change from 3 to 2 would be invisible in the output but would break every
// caller that distinguishes "your file is missing" from "your SQL is wrong".
//
// The first two checks are the regression test for #149, where the N-API
// binding aborted the whole process on
//   SELECT <one column> FROM 'f.csv' WHERE <a different column> <op> <val>
// producing the correct bytes and then dying while freeing them. Two
// independent faults were behind it — a Zig GPA that does not work inside a
// dlopen()ed library, and the engine's multi-megabyte stack frames on a host
// thread too small for them. Both are fixed in node_binding.zig; these
// assertions keep them fixed.

const { execFileSync, spawnSync } = require('child_process');
const path = require('path');
const fs = require('fs');
const os = require('os');

// See the note in test.js: forward slashes so the path survives being
// embedded in a SQL string on Windows.
const tmp = (name) => path.join(os.tmpdir(), name).replace(/\\/g, '/');

const CLI = path.join(__dirname, 'cli.js');
const csv = tmp('csvql_cli_test.csv');

fs.writeFileSync(csv, [
    'id,name,city,salary',
    '1,Alice,Austin,120000',
    '2,Bob,Boston,80000',
    '3,Carol,Austin,150000',
].join('\n') + '\n');

const tsv = tmp('csvql_cli_test.tsv');
fs.writeFileSync(tsv, 'a\tb\n1\t2\n');

let passed = 0;
let total = 0;

function check(label, actual, expected) {
    total++;
    const ok = JSON.stringify(actual) === JSON.stringify(expected);
    console.log(`${ok ? '✓' : '✗'} ${label}`);
    if (!ok) {
        console.log('  expected:', JSON.stringify(expected));
        console.log('  actual:  ', JSON.stringify(actual));
    } else {
        passed++;
    }
}

function run(...args) {
    const r = spawnSync(process.execPath, [CLI, ...args], { encoding: 'utf8' });
    // Strip CR so the line-splitting comparisons below behave identically on
    // Windows. csvql writes LF only, but the shell and Node layers in between
    // are not worth trusting on this.
    const clean = (v) => (v || '').replace(/\r/g, '');
    return { code: r.status, out: clean(r.stdout), err: clean(r.stderr) };
}

// -- output --------------------------------------------------------------
const csvOut = run(`SELECT name FROM '${csv}' WHERE salary > 100000`);
check('csv output', csvOut.out.trim().split('\n'), ['name', 'Alice', 'Carol']);
check('csv output exits 0', csvOut.code, 0);

const grouped = run(`SELECT city, COUNT(*) AS n FROM '${csv}' GROUP BY city ORDER BY n DESC`);
check('group by', grouped.out.trim().split('\n')[1], 'Austin,2');

// query() returns typed values, so COUNT comes back as a number, not a string.
const jsonOut = run('--json', `SELECT COUNT(*) AS n FROM '${csv}'`);
check('--json is valid JSON', JSON.parse(jsonOut.out), [{ n: 3 }]);

check('-j short flag works', run('-j', `SELECT COUNT(*) AS n FROM '${csv}'`).code, 0);

// -d '\t' takes a literal backslash-t so the caller does not need $'\t'.
const tsvOut = run('-d', '\\t', `SELECT * FROM '${tsv}'`);
check('--delimiter tab', tsvOut.out.trim().split('\n'), ['a,b', '1,2']);

// -- exit codes (the agent-facing contract) ------------------------------
check('missing file exits 3', run(`SELECT * FROM '${tmp('nope_does_not_exist.csv')}'`).code, 3);
check('bad SQL exits 2', run(`SELEKT * FROM '${csv}'`).code, 2);
check('unknown column exits 2', run(`SELECT nocol FROM '${csv}'`).code, 2);
check('unknown flag exits 1', run('--bogus', 'SELECT 1').code, 1);
check('no arguments exits 1', run().code, 1);
check('two SQL statements exits 1', run('SELECT 1', 'SELECT 2').code, 1);
check('option missing its value exits 1', run('-d').code, 1);

// -- meta ----------------------------------------------------------------
check('--version prints version', run('--version').out.trim(), require('./package.json').version);
check('--version exits 0', run('--version').code, 0);
check('--help exits 0', run('--help').code, 0);
check('errors go to stderr, not stdout', run(`SELECT nocol FROM '${csv}'`).out, '');

console.log(`\n${passed}/${total} CLI tests passed`);

fs.unlinkSync(csv);
fs.unlinkSync(tsv);

if (passed !== total) process.exit(1);
