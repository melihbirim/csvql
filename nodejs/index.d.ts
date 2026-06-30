/**
 * Run a SQL query against a CSV file and return rows as an array of objects.
 * All values are strings (CSV has no type information).
 *
 * @example
 * const rows = query("SELECT city, COUNT(*) as n FROM 'data.csv' GROUP BY city");
 * // [{ city: 'Austin', n: '124956' }, ...]
 */
export function query(sql: string): Record<string, string>[];

/**
 * Run a SQL query and return results as a CSV string (header row + data rows).
 */
export function queryCsv(sql: string): string;
