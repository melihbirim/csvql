# csvql-query

Fast SQL queries on CSV files from Node.js — powered by a Zig/SIMD engine via N-API.

The engine streams through the file instead of loading it into memory, so RAM stays flat — tens of MB — regardless of file size. Prebuilt native binaries ship for macOS (arm64/x64), Linux (x64/arm64), and Windows (x64) — no compiler needed.

## Install

```bash
npm install csvql-query
```

## Usage

```js
const csvql = require('csvql-query');

// SQL query → array of objects
const rows = csvql.query("SELECT name, salary FROM 'employees.csv' WHERE salary > 100000");

// Aggregates + GROUP BY
const byCity = csvql.query(
  "SELECT city, COUNT(*) AS n, AVG(salary) AS avg FROM 'employees.csv' GROUP BY city ORDER BY avg DESC"
);

// JOIN two files
const joined = csvql.query(
  "SELECT e.name, d.name AS dept FROM 'employees.csv' e JOIN 'departments.csv' d ON e.dept_id = d.id"
);

// Return CSV text instead of objects
const csv = csvql.queryCsv("SELECT city, COUNT(*) FROM 'data.csv' GROUP BY city");
```

### TSV and other delimiters

```js
csvql.query("SELECT * FROM 'data.tsv' WHERE score > 90", { delimiter: '\t' });
```

### Skip comments / blank lines

```js
csvql.query("SELECT * FROM 'data.csv'", { comment: '#', skipEmptyLines: true });
```

## No-SQL API

`find()` builds the SQL for you — no SQL knowledge needed:

```js
csvql.find('employees.csv', {
  columns: ['name', 'city', 'salary'],
  where:   'salary>100000 AND department=Engineering',
  orderBy: 'salary:desc',
  limit:   10,
});
```

Operators in `where`: `= != > >= < <=`, combined with `AND` / `OR`. Values are auto-quoted (numbers stay unquoted). For aggregates (COUNT, SUM, AVG, GROUP BY) use `query()`.

## API

| Function | Returns | Notes |
|----------|---------|-------|
| `query(sql, opts?)` | `Object[]` | Full SQL. File paths are single-quoted in FROM/JOIN. |
| `queryCsv(sql, opts?)` | `string` | Same as `query` but returns CSV text. |
| `find(file, opts?)` | `Object[]` | Simple filter/sort/limit without writing SQL. |

`opts`: `{ delimiter, comment, skipEmptyLines }`. TypeScript declarations are bundled.

## Supported SQL

`SELECT`, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`, `JOIN`, `DISTINCT`, `LIKE`, and `COUNT` / `SUM` / `AVG` / `MIN` / `MAX`.

## License

MIT · [github.com/melihbirim/csvql](https://github.com/melihbirim/csvql)
