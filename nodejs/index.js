'use strict';

const path   = require('path');
const fs     = require('fs');
const os     = require('os');
const crypto = require('crypto');

function findNative() {
    const bundled = path.join(__dirname, 'csvql.node');
    if (fs.existsSync(bundled)) return bundled;

    const dev = path.join(__dirname, '..', 'zig-out', 'lib', 'csvql.node');
    if (fs.existsSync(dev)) return dev;

    throw new Error(
        'csvql native module not found.\n' +
        'Run: zig build node -Doptimize=ReleaseFast\n' +
        'Or install the npm package: npm install csvql-query'
    );
}

const native = require(findNative());

// ── Preprocessing (delimiter conversion + comment/blank stripping) ────────────

/**
 * Quote-aware delimiter converter for a single CSV/TSV line.
 * Splits on `from` (respecting double-quoted fields), re-joins with `to`,
 * and re-quotes any fields whose value contains `to` or a double-quote.
 */
function convertDelimiter(line, from, to) {
    const fields = [];
    let cur = '', inQuotes = false;
    for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (ch === '"') { inQuotes = !inQuotes; cur += ch; }
        else if (!inQuotes && line.startsWith(from, i)) { fields.push(cur); cur = ''; i += from.length - 1; }
        else { cur += ch; }
    }
    fields.push(cur);
    return fields.map(f => {
        if (f.includes(to) || f.includes('"') || f.includes('\n'))
            return '"' + f.replace(/"/g, '""') + '"';
        return f;
    }).join(to);
}

/**
 * Write a preprocessed copy of `filePath` to a temp file, applying:
 *   opts.delimiter      — source delimiter to convert to comma  (e.g. '\t')
 *   opts.comment        — prefix string to skip (e.g. '#')
 *   opts.skipEmptyLines — skip blank lines
 *
 * Returns the temp file path, or null if no preprocessing is needed.
 * Caller must delete the temp file when done.
 */
function preprocess(filePath, opts) {
    const needsDelim   = opts.delimiter && opts.delimiter !== ',';
    const needsComment = typeof opts.comment === 'string';
    const needsSkip    = opts.skipEmptyLines === true;

    if (!needsDelim && !needsComment && !needsSkip) return null;

    const tmp  = path.join(os.tmpdir(), `_csvql_${crypto.randomBytes(6).toString('hex')}.csv`);
    const src  = fs.readFileSync(filePath, 'utf8');
    const out  = [];

    for (const line of src.split('\n')) {
        if (needsComment && line.startsWith(opts.comment)) continue;
        if (needsSkip    && line.trim() === '')            continue;
        out.push(needsDelim ? convertDelimiter(line, opts.delimiter, ',') : line);
    }

    fs.writeFileSync(tmp, out.join('\n'));
    return tmp;
}

/**
 * Find all file paths referenced in a SQL string (FROM / JOIN clauses).
 * Returns an array of { match, filePath } objects for paths that exist on disk.
 */
function extractPaths(sql) {
    const found = [];
    const re = /['"]([^'"]+)['"]/g;
    let m;
    while ((m = re.exec(sql)) !== null) {
        if (fs.existsSync(m[1])) found.push({ match: m[0], filePath: m[1] });
    }
    return found;
}

/**
 * If opts requires preprocessing, rewrite every file path in sql to a
 * preprocessed temp file. Returns { sql: newSql, temps: [paths to delete] }.
 */
function maybePreprocess(sql, opts) {
    if (!opts || !Object.keys(opts).length) return { sql, temps: [] };

    const temps = [];
    let   out   = sql;

    for (const { match, filePath } of extractPaths(sql)) {
        const tmp = preprocess(filePath, opts);
        if (tmp) {
            out = out.replace(match, `'${tmp}'`);
            temps.push(tmp);
        }
    }

    return { sql: out, temps };
}

function cleanUp(temps) {
    for (const f of temps) try { fs.unlinkSync(f); } catch (_) {}
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Run a SQL query and return rows as an array of objects.
 *
 * @param {string} sql  — e.g. "SELECT * FROM 'data.csv' WHERE age > 30"
 * @param {object} [opts]
 * @param {string}  [opts.delimiter]      — source delimiter if not comma (e.g. '\t' for TSV)
 * @param {string}  [opts.comment]        — skip lines starting with this string (e.g. '#')
 * @param {boolean} [opts.skipEmptyLines] — skip blank lines (default false)
 * @returns {Array<Record<string, unknown>>}
 *
 * @example
 * // TSV file
 * csvql.query("SELECT * FROM 'data.tsv'", { delimiter: '\t' })
 *
 * // CSV with comment rows
 * csvql.query("SELECT * FROM 'data.csv'", { comment: '#', skipEmptyLines: true })
 */
function query(sql, opts) {
    const { sql: rewritten, temps } = maybePreprocess(sql, opts);
    try {
        return JSON.parse(native.queryJson(rewritten));
    } finally {
        cleanUp(temps);
    }
}

/**
 * Run a SQL query and return results as a CSV string.
 *
 * @param {string} sql
 * @param {object} [opts] — same options as query()
 * @returns {string}
 */
function queryCsv(sql, opts) {
    const { sql: rewritten, temps } = maybePreprocess(sql, opts);
    try {
        return native.queryCsv(rewritten);
    } finally {
        cleanUp(temps);
    }
}

module.exports = { query, queryCsv };
