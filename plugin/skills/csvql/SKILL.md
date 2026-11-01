---
name: csvql
description: Query, inspect, filter, aggregate or join CSV, TSV and Excel (.xlsx) files with SQL using the csvql CLI. Use whenever the user asks a question about the contents of a .csv, .tsv or .xlsx file, wants counts, totals, top N, group-bys or a schema of one, or the file is too large to read into context.
---

# csvql

csvql runs SQL directly on CSV files in place. Read-only, no import, no database. Use it instead of reading the file into context or writing a pandas script.

## Check it is installed

```bash
command -v csvql || echo missing
```

If missing, ask the user before installing, then use one of:

```bash
brew install melihbirim/csvql/csvql   # macOS, Linux
pip install csvql-query               # Python
npm install -g csvql-query            # Node, CLI is csvql-query
```

Prebuilt binaries: https://github.com/melihbirim/csvql/releases

## Workflow

1. Look at the file first: `csvql --schema data.csv` (columns, inferred types, row count, size).
2. Query it. File paths are single-quoted inside the SQL:

```bash
csvql "SELECT region, SUM(revenue) AS total FROM 'sales.csv' GROUP BY region ORDER BY total DESC LIMIT 10"
csvql "SELECT COUNT(*) FROM 'events.csv' WHERE status = 'error'"
csvql "SELECT o.id, c.name FROM 'orders.csv' o JOIN 'customers.csv' c ON o.customer_id = c.id"
cat data.csv | csvql "SELECT * FROM '-' WHERE age > 30"
```

3. Always aggregate or `LIMIT` so the output stays small. Add `--json` when you need to parse results, `--markdown` to show the user a table.

## Options

- `-d '\t'` for TSV or another delimiter
- `--no-input-header` when the file has no header row (columns become c1..cN)
- `-o out.csv` to write results to a file
- `--strict` to error instead of skipping rows whose WHERE value is not numeric

## SQL support

SELECT, WHERE (AND/OR/NOT, LIKE/ILIKE, IN, BETWEEN, IS NULL), GROUP BY, HAVING, ORDER BY, LIMIT, DISTINCT, JOIN and LEFT JOIN (chained), `col IN (SELECT ...)`. Aggregates: COUNT, SUM, AVG, MIN, MAX, MEDIAN, QUANTILE, MODE, STDDEV, VARIANCE, GROUP_CONCAT, COUNT(DISTINCT). Date and string functions such as STRFTIME, DATEDIFF, LOWER, UPPER, SUBSTR, ROUND. Arithmetic (`+ - * /`, parentheses) over columns and numbers in SELECT, WHERE and inside SUM, AVG, MIN, MAX, STDDEV and VARIANCE, and over aggregate results: `SUM(price * qty)`, `WHERE price * qty > 100`, `SUM(a) / COUNT(*)`. Column names with spaces go in double quotes: `SELECT "First Name" FROM 'people.csv'`.

Not supported: arithmetic around function calls (`ROUND(a) * 2`), RIGHT/FULL/CROSS JOIN, window functions, UNION, subqueries in FROM, any writes. DATEDIFF and DATEADD take columns, not literals. For those, fall back to another tool.

## Exit codes

- 0 success
- 1 IO or internal error, for example file not found
- 2 the query is wrong: fix the SQL (the error names the problem) and retry
- 3 the data defeated the query under `--strict`

Run `csvql --help` for the full reference.
