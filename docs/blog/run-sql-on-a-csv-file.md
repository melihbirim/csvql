---
layout: post
title: "How to Run SQL on a CSV File From the Command Line: csvql, DuckDB, sqlite3 and csvkit"
description: "Four ways to query a CSV file with SQL without setting up a database: the exact command for each, and how long each took on the same 1 million row file."
date: 2026-11-12
---

You have a CSV file and a question that is one `GROUP BY` away. You do not want to create a database, define a schema or write a script. Four free tools run SQL on the file directly. Here is the command for each, and what each one cost on the same file.

The file: 1,000,000 rows, 36 MB, columns `id,name,age,city,salary,department`. It is the fixture csvql's own correctness tests use, generated with `./bench/gen_fixture.sh people.csv 1000000`. The question:

```sql
SELECT department, COUNT(*) AS n, AVG(salary) AS avg_salary
FROM people WHERE age > 30
GROUP BY department ORDER BY department
```

## csvql

```bash
csvql "SELECT department, COUNT(*) AS n, AVG(salary) AS avg_salary FROM 'people.csv' WHERE age > 30 GROUP BY department ORDER BY department"
```

The file path goes in the `FROM` clause in quotes. No import step: csvql memory maps the file and scans it on every core. Output is a table in a terminal and CSV when piped; `--json`, `--jsonl` and `--markdown` change that. Install with `brew install melihbirim/csvql/csvql`, or `npm i -g csvql-query`, which installs the same engine as a `csvql-query` command.

## DuckDB

```bash
duckdb -c "SELECT department, COUNT(*) AS n, AVG(salary) AS avg_salary FROM 'people.csv' WHERE age > 30 GROUP BY department ORDER BY department"
```

Same shape: the file name works as a table. DuckDB has the widest SQL of the four, with window functions, `UNION`, CTEs and Parquet. If you need any of those, use it.

## sqlite3

```bash
sqlite3 :memory: -cmd ".mode csv" -cmd ".import people.csv people" "SELECT department, COUNT(*) AS n, AVG(salary) AS avg_salary FROM people WHERE age > 30 GROUP BY department ORDER BY department"
```

Already on macOS and most Linux systems. `.import` copies every row into an in memory table first, with the header row as column names, then runs the query. Every column is imported as text, so numeric comparisons rely on SQLite's type affinity. The import is most of the time below.

## csvkit

```bash
csvsql --query "SELECT department, COUNT(*) AS n, AVG(salary) AS avg_salary FROM people WHERE age > 30 GROUP BY department ORDER BY department" people.csv
```

The table name is the file name without `.csv`. csvkit is a Python suite; `csvsql` infers a type for every column, loads all rows into SQLite through SQLAlchemy, then queries. It is the easiest to install with `pip`, and its sibling tools (`csvcut`, `csvstat`, `csvlook`) are handy for small files.

## What each one cost

Best of three runs, wall clock and peak memory from `/usr/bin/time -l`, Apple M2 Pro, 12 cores, 16 GB. Versions: csvql 3.0.3, DuckDB 1.5.5, sqlite3 3.51.0, csvkit 2.2.0.

| Tool | Time | Peak memory |
|---|---|---|
| csvql | 0.01 s | 40 MB |
| DuckDB | 0.15 s | 68 MB |
| sqlite3 | 2.34 s | 57 MB |
| csvkit (`csvsql`) | 20.46 s | 1,376 MB |

All four returned the same counts and averages. Differences were formatting only: csvql prints `85000` where csvkit prints `85000.0`, and sqlite3 rounds the last digits.

One file and one query on one machine is a single data point, not a benchmark suite. The point is the gap in kind: tools that scan the file in place (csvql, DuckDB) against tools that import it first (sqlite3, csvkit). The import cost grows with the file, so on a 1 GB CSV csvkit needs gigabytes of memory before the query starts.

## Which one to use

- **One question on a big file, from a shell or a script**: csvql or DuckDB. Both read the file in place.
- **SQL csvql does not have** (window functions, `UNION`, CTEs) or Parquet: DuckDB.
- **Nothing to install**: sqlite3, if the file fits comfortably in memory.
- **Small files and the rest of the csvkit toolbox**: csvkit.
- **An AI agent asking the questions**: csvql has a read-only MCP server built in, confined to one folder, with a column deny list. See [Let Claude Desktop query a CSV file with SQL](claude-desktop-query-csv-with-sql.html).

To reproduce: build csvql, run `./bench/gen_fixture.sh people.csv 1000000`, then run the four commands above under `/usr/bin/time -l` (`/usr/bin/time -v` on Linux).
