'use strict';

const path = require('path');
const fs = require('fs');

function findNative() {
    // 1. Bundled inside the npm package (pre-built binary shipped with the package)
    const bundled = path.join(__dirname, 'csvql.node');
    if (fs.existsSync(bundled)) return bundled;

    // 2. Dev build output (zig build node from repo root)
    const dev = path.join(__dirname, '..', 'zig-out', 'lib', 'csvql.node');
    if (fs.existsSync(dev)) return dev;

    throw new Error(
        'csvql native module not found.\n' +
        'Run: zig build node -Doptimize=ReleaseFast\n' +
        'Or install the npm package: npm install csvql-query'
    );
}

const native = require(findNative());

/**
 * Run a SQL query against a CSV file and return rows as an array of objects.
 *
 * @param {string} sql - SQL query, e.g. "SELECT * FROM 'data.csv' WHERE age > 30"
 * @returns {Array<Record<string, string>>} Array of row objects (all values are strings)
 * @throws {Error} on invalid SQL or file not found
 *
 * @example
 * const rows = csvql.query("SELECT city, COUNT(*) as n FROM 'data.csv' GROUP BY city");
 * // [{ city: 'Austin', n: '124956' }, ...]
 */
function query(sql) {
    return JSON.parse(native.queryJson(sql));
}

/**
 * Run a SQL query and return results as a CSV string (header row + data rows).
 *
 * @param {string} sql - SQL query
 * @returns {string} CSV text
 * @throws {Error} on invalid SQL or file not found
 */
function queryCsv(sql) {
    return native.queryCsv(sql);
}

module.exports = { query, queryCsv };
