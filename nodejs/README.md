# csvql-query

**SQL on CSV files, in place.** No database, no import, no ingest — point a query at the file where it already lives. A Zig/SIMD engine via N-API, with prebuilt binaries for macOS (arm64/x64), Linux (x64/arm64), and Windows (x64). No compiler needed.

The engine streams the file instead of loading it, so RAM stays flat — tens of MB — regardless of file size.

## Try it without installing anything

```bash
npx csvql-query "SELECT city, COUNT(*) AS n FROM 'data.csv' GROUP BY city ORDER BY n DESC"
```

File paths go in single quotes inside `FROM`. `npx csvql-query --help` for the rest.

```bash
npx csvql-query --json "SELECT * FROM 'sales.csv' WHERE amount > 1000 LIMIT 10"
npx csvql-query -d '\t'  "SELECT * FROM 'data.tsv' LIMIT 3"
```

Exit codes are meant to be branched on: `0` success, `1` usage error, `2` query error, `3` file/IO error.

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

## For AI agents (MCP)

Pasting a 417 MB CSV into an LLM costs ~230 million tokens — it fits in no context window. Over [MCP](https://modelcontextprotocol.io/), an agent queries the file in place and gets back only the answer, for a few hundred tokens.

The MCP server ships in the standalone binary rather than this package:

```bash
brew install melihbirim/csvql/csvql
csvql install          # registers csvql with Claude Code + Claude Desktop
```

There's also a one-click `.mcpb` extension for Claude Desktop on the [releases page](https://github.com/melihbirim/csvql/releases). Full setup — VS Code Copilot, Claude Desktop, manual config — in the [main README](https://github.com/melihbirim/csvql#mcp).

**Read-only by design.** csvql only runs `SELECT`. It has no `INSERT`/`UPDATE`/`DELETE`/`DROP` and physically cannot modify your data. It makes zero network calls and runs fully air-gapped, so you can put it next to the data instead of shipping files out to an LLM.

## Correctness

csvql treats [DuckDB](https://duckdb.org) as the reference implementation and diffs its own output against it — because an agent can't eyeball a wrong answer the way a human scanning a spreadsheet can.

- **97 differential checks** against DuckDB on every PR, across Linux x86_64 and macOS ARM
- **553 unit tests**
- **A differential query generator** runs ~50,000 generated queries against DuckDB nightly; every run appends a row to a public [verification log](https://github.com/melihbirim/csvql/blob/main/VERIFICATION-LOG.md) — wins and losses both
- Where behavior intentionally differs from DuckDB, it's [documented and audited](https://github.com/melihbirim/csvql/blob/main/CORRECTNESS.md), not hand-waved

Unsupported SQL **errors clearly** rather than silently returning wrong data. That distinction is the entire point.

## Supported SQL

`SELECT`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `JOIN`, `DISTINCT`, `LIKE`, `BETWEEN`, `IN` (including `IN (SELECT ...)` subqueries), `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`, and scalar functions (`UPPER`, `LOWER`, `TRIM`, `LENGTH`, `SUBSTR`, `REPLACE`, `COALESCE`, `CAST`, `ROUND`, …).

## License

MIT · [github.com/melihbirim/csvql](https://github.com/melihbirim/csvql)
