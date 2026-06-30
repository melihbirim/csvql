/**
 * Options accepted by {@link query} and {@link queryCsv}.
 */
export interface QueryOptions {
    /**
     * Source field delimiter when the file is not comma-separated.
     * Use `'\t'` for TSV, `'|'` for pipe-delimited, `';'` for semicolon, etc.
     * The conversion is quote-aware — quoted fields containing the delimiter
     * are handled correctly.
     *
     * @example
     * query("SELECT * FROM 'data.tsv'", { delimiter: '\t' })
     */
    delimiter?: string;

    /**
     * Skip lines whose content starts with this string before running the query.
     * Useful for files that include shell-style comments or metadata headers.
     *
     * @example
     * query("SELECT * FROM 'data.csv'", { comment: '#' })
     */
    comment?: string;

    /**
     * When `true`, blank and whitespace-only lines are stripped before querying.
     * The engine already ignores truly empty lines; this option also removes
     * lines that contain only spaces or tabs.
     *
     * @default false
     */
    skipEmptyLines?: boolean;
}

/**
 * Run a SQL query against one or more CSV/TSV files and return results
 * as an array of objects.
 *
 * The Zig/SIMD engine streams through the file without loading all rows into
 * memory — RAM stays ~5 MB regardless of file size. Numeric values come back
 * as JS numbers (via JSON.parse); everything else is a string.
 *
 * @param sql  SQL query. File paths go inside FROM/JOIN clauses, e.g.
 *             `"SELECT * FROM 'data.csv' WHERE age > 30"`
 * @param opts Optional pre-processing options (delimiter, comment, skipEmptyLines).
 * @throws {Error} on invalid SQL, file not found, or engine error.
 *
 * @example
 * // Basic query
 * const rows = query("SELECT name, city FROM 'employees.csv' LIMIT 10")
 *
 * @example
 * // Filter + aggregate
 * const rows = query(
 *   "SELECT city, COUNT(*) as n, AVG(salary) as avg FROM 'employees.csv' GROUP BY city ORDER BY avg DESC"
 * )
 *
 * @example
 * // JOIN two files
 * const rows = query(
 *   "SELECT e.name, d.name as dept FROM 'employees.csv' e JOIN 'departments.csv' d ON e.dept_id = d.id"
 * )
 *
 * @example
 * // TSV input
 * const rows = query("SELECT * FROM 'scores.tsv' WHERE score > 90", { delimiter: '\t' })
 *
 * @example
 * // Strip comment and blank lines
 * const rows = query("SELECT * FROM 'data.csv'", { comment: '#', skipEmptyLines: true })
 */
export function query(sql: string, opts?: QueryOptions): Record<string, unknown>[];

/**
 * Run a SQL query and return results as a CSV string (header row + data rows).
 *
 * Useful for piping results into another CSV consumer or writing to a file
 * without the overhead of JSON serialisation.
 *
 * @param sql  SQL query.
 * @param opts Optional pre-processing options — same as {@link query}.
 * @returns CSV text, e.g. `"name,salary\nAlice,120000\nBob,80000\n"`
 * @throws {Error} on invalid SQL, file not found, or engine error.
 *
 * @example
 * const csv = queryCsv("SELECT city, COUNT(*) FROM 'data.csv' GROUP BY city")
 * fs.writeFileSync('summary.csv', csv)
 *
 * @example
 * // TSV → CSV
 * const csv = queryCsv("SELECT name, score FROM 'results.tsv' ORDER BY score DESC", { delimiter: '\t' })
 */
export function queryCsv(sql: string, opts?: QueryOptions): string;
