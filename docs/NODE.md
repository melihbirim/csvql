# Node.js

csvql ships as a native Node.js addon — same SIMD engine, same performance, no subprocess.

```bash
# 1. Build the native addon
zig build node -Doptimize=ReleaseFast
# → zig-out/lib/csvql.node

# 2. Run
node nodejs/bench.js
```

```js
const csvql = require('csvql-query');
```

## `find()` — no SQL required

For users who don't want to write SQL. Pass a file path and a plain options object:

```js
// All rows
csvql.find('employees.csv')

// Pick columns + filter
csvql.find('employees.csv', {
    columns: ['name', 'city', 'salary'],
    where:   'salary>100000',
})

// AND / OR conditions, sort, limit
csvql.find('employees.csv', {
    where:   'department=Engineering AND salary>80000',
    orderBy: 'salary:desc',
    limit:   10,
})

// OR condition
csvql.find('employees.csv', {
    columns: 'name,city',
    where:   'city=Austin OR city=Boston',
})
```

`where` operators: `=` `!=` `>` `>=` `<` `<=` — string values are quoted automatically, numbers stay numeric. Combine with `AND` / `OR`. For aggregates (`COUNT`, `SUM`, `AVG`, `GROUP BY`) use `query()`.

## `query()` — full SQL

```js
// Returns an array of objects (numbers are typed, not strings)
const rows = csvql.query("SELECT city, COUNT(*) as n, AVG(salary) as avg FROM 'employees.csv' GROUP BY city ORDER BY avg DESC");
// [{ city: 'Austin', n: 3, avg: 126666.67 }, ...]

// Return raw CSV text instead
const csv = csvql.queryCsv("SELECT name, salary FROM 'employees.csv' WHERE salary > 100000");
```

## Options

Both `query()` and `queryCsv()` accept an optional second argument:

| Option           | Type      | Description                                                         |
| ---------------- | --------- | ------------------------------------------------------------------- |
| `delimiter`      | `string`  | Source field separator when not a comma — `'\t'` for TSV, `'\|'` for pipe-delimited, etc. |
| `comment`        | `string`  | Skip lines that start with this string — e.g. `'#'` for shell-style comments. |
| `skipEmptyLines` | `boolean` | Strip blank / whitespace-only lines before querying. Default `false`. |

```js
// TSV file
csvql.query("SELECT * FROM 'scores.tsv' WHERE score > 90", { delimiter: '\t' })

// File with comment rows and blank lines
csvql.query("SELECT * FROM 'data.csv'", { comment: '#', skipEmptyLines: true })

// JOIN two TSV files
csvql.query(
  "SELECT e.name, d.name as dept FROM 'employees.tsv' e JOIN 'departments.tsv' d ON e.dept_id = d.id",
  { delimiter: '\t' }
)
```

**Memory:** the engine streams through the file internally — RAM stays flat (tens of MB) regardless of file size. Compare with `csv-parse` or `papaparse` sync APIs, which materialise all rows as JS objects (200–650 MB for a 42 MB / 1M row file).

## ETL — CSV → filter → database

The most common real-world pattern is: read a CSV, keep only the rows you want, insert them into a database. How each library handles this reveals a fundamental difference.

**csv-parse** has no query language — it is a parser only. To filter you must load the entire file into memory as JS objects first, then call `.filter()`:

```js
// csv-parse: loads ALL 100k rows into heap, then filters in JS
const all  = parse(fs.readFileSync('employees.csv'), { columns: true, cast: true });
const rows = all.filter(r =>
    r.department === 'Engineering' && r.active === 'true' && r.salary > 80000
);
// ⚠ cast:true converts numbers but NOT boolean strings — 'true' stays a string,
//   so r.active === true silently returns 0 rows. You must compare against 'true'.
db.insert(rows);
```

**csvql** pushes the WHERE clause into the Zig engine. Only matching rows are ever returned to JS — the heap cost scales with the result set, not the source file:

```js
// csvql: engine filters first, only 9,991 rows reach JS
const rows = csvql.query(`
    SELECT id, name, city, salary, department
    FROM 'employees.csv'
    WHERE department = 'Engineering'
      AND active     = 'true'
      AND salary     > 80000
`);
db.insert(rows);
```

Results on 100k rows (42 MB CSV, filter matches ~10%):

|                       | csv-parse        | csvql                        |
| --------------------- | ---------------- | ----------------------------- |
| Rows read into JS     | 100,000 (all)    | 9,991 (matching only)        |
| Time                  | ~200 ms          | ~22 ms                       |
| Heap allocated        | +31 MB           | +3 MB                        |
| On a 10 GB CSV        | OOM or very slow | same ~22 ms, same ~3 MB heap |

On a 10 GB file csv-parse loads the entire dataset into memory before a single row reaches the database. csvql's heap cost stays constant because the engine never materialises rows that don't match the filter.

Full runnable example: `node --experimental-sqlite nodejs/example_etl.js`

**Benchmarks:** `node --expose-gc nodejs/bench.js` — comparison against `csv-parse` and `papaparse` on 1M rows.
**Feature comparison:** `node nodejs/compare.js` — side-by-side code and output for 10 common tasks.
