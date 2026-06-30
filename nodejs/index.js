'use strict';

/**
 * csvql — fast SQL queries on CSV files from Node.js
 *
 * Uses the csvql Zig/SIMD engine via N-API. The engine streams through the
 * file without loading it into memory, so RAM usage stays ~5 MB regardless
 * of file size.
 *
 * @example
 * const csvql = require('csvql-query');
 *
 * // Basic query
 * const rows = csvql.query("SELECT * FROM 'data.csv' WHERE salary > 100000");
 *
 * // TSV file
 * const rows = csvql.query("SELECT * FROM 'data.tsv'", { delimiter: '\t' });
 *
 * // CSV with comment/blank rows
 * const rows = csvql.query("SELECT * FROM 'data.csv'", { comment: '#', skipEmptyLines: true });
 *
 * // Return as CSV string
 * const csv = csvql.queryCsv("SELECT city, COUNT(*) FROM 'data.csv' GROUP BY city");
 */

const path   = require('path');
const fs     = require('fs');
const os     = require('os');
const crypto = require('crypto');

function findNative() {
    // 1. Bundled in same dir (npm run build copies it here, or local publish)
    const bundled = path.join(__dirname, 'csvql.node');
    if (fs.existsSync(bundled)) return bundled;

    // 2. Platform-specific npm package (installed via optionalDependencies)
    const pkg = `csvql-query-${process.platform}-${process.arch}`;
    try {
        const pkgDir = path.dirname(require.resolve(`${pkg}/package.json`));
        const bin = path.join(pkgDir, 'csvql.node');
        if (fs.existsSync(bin)) return bin;
    } catch (_) {}

    // 3. Dev build output (zig build node from repo root)
    const dev = path.join(__dirname, '..', 'zig-out', 'lib', 'csvql.node');
    if (fs.existsSync(dev)) return dev;

    throw new Error(
        `csvql native module not found for ${process.platform}-${process.arch}.\n` +
        'Install via npm:  npm install csvql-query\n' +
        'Build from source: zig build node -Doptimize=ReleaseFast'
    );
}

const native = require(findNative());

// ── Preprocessing (delimiter conversion + comment/blank stripping) ────────────

/**
 * Quote-aware delimiter converter for a single line.
 * Splits on `from` respecting double-quoted fields, re-joins with `to`,
 * and re-quotes any output field that contains `to` or a double-quote.
 *
 * @param {string} line
 * @param {string} from  source delimiter (e.g. '\t')
 * @param {string} to    target delimiter (always ',')
 * @returns {string}
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
 * Write a preprocessed copy of `filePath` to a temp file applying:
 *   opts.delimiter      — convert this separator to comma
 *   opts.comment        — skip lines starting with this string
 *   opts.skipEmptyLines — skip blank lines
 *
 * Returns the temp file path, or null when no preprocessing is needed.
 * Caller is responsible for deleting the temp file.
 *
 * @param {string} filePath
 * @param {QueryOptions} opts
 * @returns {string|null}
 */
function preprocess(filePath, opts) {
    const needsDelim   = opts.delimiter && opts.delimiter !== ',';
    const needsComment = typeof opts.comment === 'string';
    const needsSkip    = opts.skipEmptyLines === true;

    if (!needsDelim && !needsComment && !needsSkip) return null;

    const tmp = path.join(os.tmpdir(), `_csvql_${crypto.randomBytes(6).toString('hex')}.csv`);
    const out = [];

    for (const line of fs.readFileSync(filePath, 'utf8').split('\n')) {
        if (needsComment && line.startsWith(opts.comment)) continue;
        if (needsSkip    && line.trim() === '')            continue;
        out.push(needsDelim ? convertDelimiter(line, opts.delimiter, ',') : line);
    }

    fs.writeFileSync(tmp, out.join('\n'));
    return tmp;
}

/**
 * Find all file paths referenced in a SQL string (FROM / JOIN clauses)
 * that exist on disk.
 *
 * @param {string} sql
 * @returns {{ match: string, filePath: string }[]}
 */
function extractPaths(sql) {
    const found = [];
    const re = /['"]([^'"]+)['"]/g;
    let m;
    while ((m = re.exec(sql)) !== null)
        if (fs.existsSync(m[1])) found.push({ match: m[0], filePath: m[1] });
    return found;
}

/**
 * Rewrite every file path in `sql` to a preprocessed temp file when needed.
 * Handles JOIN queries with multiple file paths.
 *
 * @param {string} sql
 * @param {QueryOptions} [opts]
 * @returns {{ sql: string, temps: string[] }}
 */
function maybePreprocess(sql, opts) {
    if (!opts || !Object.keys(opts).length) return { sql, temps: [] };

    const temps = [];
    let out = sql;

    for (const { match, filePath } of extractPaths(sql)) {
        const tmp = preprocess(filePath, opts);
        if (tmp) { out = out.replace(match, `'${tmp}'`); temps.push(tmp); }
    }

    return { sql: out, temps };
}

function cleanUp(temps) {
    for (const f of temps) try { fs.unlinkSync(f); } catch (_) {}
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * @typedef {Object} QueryOptions
 * @property {string}  [delimiter]      - Source field delimiter when not a comma.
 *                                        Use '\t' for TSV, '|' for pipe-delimited, etc.
 * @property {string}  [comment]        - Skip lines whose first character(s) match this string.
 *                                        E.g. '#' to strip shell-style comments.
 * @property {boolean} [skipEmptyLines] - Skip blank lines before querying. Default: false.
 *                                        The engine already ignores truly empty rows, but
 *                                        this also removes whitespace-only lines.
 */

/**
 * Run a SQL query against one or more CSV/TSV files and return results
 * as an array of objects.
 *
 * The engine streams through the file without loading all rows into memory,
 * so RAM stays ~5 MB for any file size. Values are typed where possible
 * (numbers come back as JS numbers via JSON.parse).
 *
 * @param {string}       sql   SQL query. File paths go in FROM/JOIN clauses.
 *                             E.g. "SELECT * FROM 'data.csv' WHERE age > 30"
 * @param {QueryOptions} [opts]
 * @returns {Record<string, unknown>[]}
 * @throws {Error} on invalid SQL, file not found, or engine error
 *
 * @example
 * // Basic select
 * csvql.query("SELECT name, city FROM 'employees.csv' LIMIT 10")
 *
 * @example
 * // WHERE filter
 * csvql.query("SELECT name, salary FROM 'employees.csv' WHERE salary > 100000")
 *
 * @example
 * // GROUP BY aggregate
 * csvql.query("SELECT city, COUNT(*) as n, AVG(salary) as avg FROM 'employees.csv' GROUP BY city ORDER BY avg DESC")
 *
 * @example
 * // JOIN two files
 * csvql.query("SELECT e.name, d.name as dept FROM 'employees.csv' e JOIN 'departments.csv' d ON e.dept_id = d.id")
 *
 * @example
 * // TSV file
 * csvql.query("SELECT * FROM 'data.tsv' WHERE score > 90", { delimiter: '\t' })
 *
 * @example
 * // Skip comment and blank lines
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
 * Run a SQL query and return results as a CSV string (header row + data rows).
 *
 * Useful for piping results into another CSV consumer or writing to a file
 * without the overhead of JSON serialisation.
 *
 * @param {string}       sql
 * @param {QueryOptions} [opts]  Same options as {@link query}.
 * @returns {string}  CSV text, e.g. "name,salary\nAlice,120000\nBob,80000\n"
 * @throws {Error}    on invalid SQL, file not found, or engine error
 *
 * @example
 * const csv = csvql.queryCsv("SELECT city, COUNT(*) FROM 'data.csv' GROUP BY city")
 * fs.writeFileSync('output.csv', csv)
 *
 * @example
 * // TSV input → CSV output
 * const csv = csvql.queryCsv("SELECT name, score FROM 'results.tsv' ORDER BY score DESC", { delimiter: '\t' })
 */
function queryCsv(sql, opts) {
    const { sql: rewritten, temps } = maybePreprocess(sql, opts);
    try {
        return native.queryCsv(rewritten);
    } finally {
        cleanUp(temps);
    }
}

// ── Simple (no-SQL) API ───────────────────────────────────────────────────────

/**
 * Translate a simple condition like "salary>100000" or "name=Alice" into SQL.
 * Numbers stay unquoted; anything that isn't a valid number gets single-quoted.
 *
 * @param {string} cond  e.g. "salary>100000" or "city=Austin"
 * @returns {string}     e.g. "salary > 100000" or "city = 'Austin'"
 */
function translateCondition(cond) {
    // Match: column  operator  value   (operators longest-first to avoid = matching >=)
    const m = cond.trim().match(/^(\w+)(>=|<=|!=|>|<|=)(.+)$/);
    if (!m) throw new Error(`Invalid where condition: "${cond}"`);
    const [, col, op, raw] = m;
    const val = raw.trim();
    const quoted = isNaN(Number(val)) ? `'${val}'` : val;
    return `${col} ${op} ${quoted}`;
}

/**
 * Translate a simple where string (may contain AND / OR) into a SQL WHERE clause.
 * e.g. "salary>100000 AND department=Engineering" → "salary > 100000 AND department = 'Engineering'"
 *
 * @param {string} where
 * @returns {string}
 */
function translateWhere(where) {
    // Split on AND / OR keeping the keyword, translate each leaf condition.
    return where
        .split(/\b(AND|OR)\b/)
        .map(part => {
            const t = part.trim();
            if (t === 'AND' || t === 'OR') return t;
            return translateCondition(t);
        })
        .join(' ');
}

/**
 * Translate a simple orderBy string into a SQL ORDER BY clause.
 * e.g. "salary:desc" → "salary DESC"   |   "name" → "name ASC"
 *
 * @param {string} orderBy
 * @returns {string}
 */
function translateOrderBy(orderBy) {
    const [col, dir = 'asc'] = orderBy.split(':');
    return `${col.trim()} ${dir.trim().toUpperCase()}`;
}

/**
 * Build a SQL string from simple find() options.
 *
 * @param {string}      file
 * @param {FindOptions} opts
 * @returns {string}
 */
function buildSql(file, opts) {
    const cols = opts.columns
        ? (Array.isArray(opts.columns) ? opts.columns.join(', ') : opts.columns)
        : '*';

    let sql = `SELECT ${cols} FROM '${file}'`;
    if (opts.where)   sql += ` WHERE ${translateWhere(opts.where)}`;
    if (opts.orderBy) sql += ` ORDER BY ${translateOrderBy(opts.orderBy)}`;
    if (opts.limit)   sql += ` LIMIT ${opts.limit}`;
    return sql;
}

/**
 * @typedef {Object} FindOptions
 * @property {string|string[]} [columns]      - Columns to return. Comma-separated string or
 *                                              array. Default: all columns.
 * @property {string}          [where]        - Filter condition in simple syntax — no SQL needed.
 *                                              Operators: = != > >= < <=
 *                                              Combine with AND / OR.
 *                                              Values are auto-quoted (numbers stay unquoted).
 *                                              E.g. 'salary>100000 AND department=Engineering'
 * @property {number}          [limit]        - Maximum rows to return. Default: all rows.
 * @property {string}          [orderBy]      - Sort column and direction: 'column:asc' or
 *                                              'column:desc'. Direction defaults to asc.
 *                                              E.g. 'salary:desc'
 * @property {string}          [delimiter]    - Source delimiter when not comma. E.g. '\t' for TSV.
 * @property {string}          [comment]      - Skip lines starting with this string. E.g. '#'.
 * @property {boolean}         [skipEmptyLines] - Skip blank lines. Default false.
 */

/**
 * Query a CSV file without writing SQL.
 *
 * Uses the same SIMD engine as `query()` — the simple options are translated
 * into a SQL string internally. No SQL knowledge required.
 *
 * For aggregates (COUNT, SUM, AVG, GROUP BY) use `query()` instead.
 *
 * @param {string}      file  Path to the CSV/TSV file.
 * @param {FindOptions} [opts]
 * @returns {Record<string, unknown>[]}
 * @throws {Error} on invalid condition, file not found, or engine error
 *
 * @example
 * // All rows
 * csvql.find('employees.csv')
 *
 * @example
 * // Pick columns + filter
 * csvql.find('employees.csv', {
 *     columns: ['name', 'city', 'salary'],
 *     where:   'salary>100000',
 * })
 *
 * @example
 * // Filter + sort + limit
 * csvql.find('employees.csv', {
 *     where:   'department=Engineering AND salary>80000',
 *     orderBy: 'salary:desc',
 *     limit:   10,
 * })
 *
 * @example
 * // TSV file, no SQL needed
 * csvql.find('scores.tsv', {
 *     columns: ['name', 'score'],
 *     where:   'score>=90',
 *     orderBy: 'score:desc',
 *     delimiter: '\t',
 * })
 */
function find(file, opts = {}) {
    const sql = buildSql(file, opts);
    return query(sql, opts);
}

module.exports = { query, queryCsv, find };
