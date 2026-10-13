<p align="center">
  <img src="logo.svg" alt="csvql" width="420"/>
</p>

[![CI](https://github.com/melihbirim/csvql/actions/workflows/ci.yml/badge.svg)](https://github.com/melihbirim/csvql/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE.md)
[![Release](https://img.shields.io/github/v/release/melihbirim/csvql)](https://github.com/melihbirim/csvql/releases)

**The analytical CSV query engine for AI agents.**

Run SQL analytics — `GROUP BY`, aggregates, joins, time-series — on CSV files **in place**: no database, no import, no ingest. csvql ships as an [MCP](https://modelcontextprotocol.io/) server, so an LLM can query a gigabyte file for a few hundred tokens instead of pasting it (impossible) into context. A single static binary written in Zig. Your data never leaves your machine.

> A database is something you load your data *into*. csvql is a query you run on the data where it already lives.

**Read-only and on-prem by design.** csvql only runs `SELECT` — it has no `INSERT`/`UPDATE`/`DELETE`/`DROP` and physically cannot modify your data. It makes zero network calls, needs no cloud, and runs fully air-gapped. **Our next north star:** the safe way to give AI agents query access to corporate data — run csvql *next to the data* on your own servers (read-only, nothing leaves the box) instead of shipping files out to an LLM.

### Token economics: query files instead of pasting them

Pasting a 417 MB CSV into an LLM costs **230 million tokens** — it fits no context window. Over MCP, the agent queries the file in place and gets back only the answer:

| Question an agent asks | Tokens used |
| ---------------------- | ----------- |
| *"How many trips per cab type?"* | **43** |
| *"Which year was busiest?"* | **49** |
| *"Average fare by passenger count?"* | **123** |

Same answers, **~1,000–500,000× fewer tokens** — flat, regardless of file size. One command wires it into Claude: [`csvql install`](#setup). Measure it yourself: [`bench/bench_tokens.py`](bench/bench_tokens.py).

```bash
$ csvql "SELECT cab_type, COUNT(*) FROM 'trips.csv' GROUP BY cab_type"
cab_type,COUNT(*)
green,32447
yellow,967553
  0.05s — no import, queried straight off the file
```

[Website](https://melihbirim.github.io/csvql/) · [Quick Start](#quick-start) · [Installation](#installation) · [Performance](#performance) · [SQL Reference](#sql-reference) · [Docs](#documentation)

---

## Quick Start

csvql auto-detects SQL or simple mode from your input:

```bash
# SQL mode
csvql "SELECT name, salary FROM 'data.csv' WHERE age > 30 ORDER BY salary DESC LIMIT 10"

# Simple mode — same query, shorter syntax
csvql data.csv "name,salary" "age>30" 10 "salary:desc"

# Just browse a file
csvql data.csv
```

### Unix Pipes

```bash
cat data.csv | csvql "SELECT name, age FROM '-' WHERE age > 25"
csvql "SELECT * FROM 'data.csv' WHERE status = 'active'" > output.csv
csvql "SELECT email FROM 'users.csv'" | wc -l
```

### Flags

| Flag                 | Short | Description                                         |
| -------------------- | ----- | --------------------------------------------------- |
| `--no-header`        |       | Suppress header row in output                       |
| `--no-input-header`  |       | Treat the first row as data; auto-name columns `c1`..`cN` |
| `-o`, `--output <file>` |    | Write results to a file instead of stdout           |
| `--delimiter <char>` | `-d`  | Field delimiter (default `,`). Use `\t` for TSV     |
| `--json`             |       | Output as a JSON array (`[{...}, ...]`)             |
| `--jsonl`            |       | Output as JSONL / NDJSON (one JSON object per line) |
| `--markdown`         |       | Output as a GitHub-flavored Markdown table (pipes in cells are escaped, newlines become `<br>`); not combinable with `--json`, `--jsonl` or `--no-header` |
| `--threads <N>`      |       | Worker threads for parallel execution; `0` uses automatic detection |
| `--strict`           |       | Error on a WHERE numeric comparison against a non-numeric value instead of silently skipping that row (see [CORRECTNESS.md](CORRECTNESS.md#strict-and-exit-codes)) |
| `--version`          | `-v`  | Show version                                        |
| `--help`             | `-h`  | Show help                                           |
| `--mcp`              |       | Start as an MCP server (stdio JSON-RPC transport)   |
| `--root <dir>`       |       | Confine file access to a directory (repeatable via commas) |
| `--audit <file>`     |       | Append a JSONL audit record per query (timestamp, SQL)     |

```bash
# TSV file
csvql "SELECT name, salary FROM 'data.tsv'" -d $'\t'

# Pipe into another tool that expects no header
csvql "SELECT name, age FROM 'data.csv'" --no-header | awk -F, '{print $2}'

# TSV input, no header in output
cat data.tsv | csvql "SELECT * FROM '-'" -d $'\t' --no-header

# Markdown table for pasting into an issue or PR (add -o table.md to write a file)
csvql "SELECT name, age FROM 'data.csv' LIMIT 3" --markdown
```

## Installation

### Homebrew (macOS / Linux)

```bash
brew install melihbirim/csvql/csvql
```

Or in two steps if you plan to install multiple tools from this tap:

```bash
brew tap melihbirim/csvql
brew install csvql
```

> `melihbirim/csvql` is the tap (the formula repository), and the trailing `/csvql` is the formula name inside it.

### Prebuilt Binaries

Download from [GitHub Releases](https://github.com/melihbirim/csvql/releases):

```bash
# macOS (Apple Silicon)
curl -L https://github.com/melihbirim/csvql/releases/latest/download/csvql-macos-aarch64.tar.gz | tar xz
sudo mv csvql-macos-aarch64 /usr/local/bin/csvql

# macOS (Intel)
curl -L https://github.com/melihbirim/csvql/releases/latest/download/csvql-macos-x86_64.tar.gz | tar xz
sudo mv csvql-macos-x86_64 /usr/local/bin/csvql

# Linux (x86_64)
curl -L https://github.com/melihbirim/csvql/releases/latest/download/csvql-linux-x86_64.tar.gz | tar xz
sudo mv csvql-linux-x86_64 /usr/local/bin/csvql
```

### Build from Source

Requires [Zig](https://ziglang.org/) 0.13.0+ (tested with 0.15.2):

```bash
git clone https://github.com/melihbirim/csvql.git
cd csvql
zig build -Doptimize=ReleaseFast
sudo cp zig-out/bin/csvql /usr/local/bin/
```

## Performance

**Two fixture sizes, unquoted CSV, Apple M2 Pro**, both engines reading the raw file. Small files are timed as the mean of 30 runs because a single run finishes faster than a shell timer can resolve.

**2M rows, 69 MB:**

| Query | csvql | DuckDB | Speedup |
| --- | --- | --- | --- |
| `COUNT(*)` | **0.0139s** | 0.1185s | **8.5x** |
| `COUNT(*) WHERE age > 30` | **0.0134s** | 0.1170s | **8.7x** |
| `SUM(salary), AVG(salary)` | **0.0180s** | 0.1178s | **6.5x** |

At this size process startup is a visible share of both totals: 0.022s for DuckDB, 0.005s for csvql. Subtracting it, `COUNT(*)` is 10.5x rather than 8.5x, so these figures understate rather than flatter.

**20M rows, 711 MB** (best-of-5):

| Query                                   | csvql      | DuckDB | Speedup   |
| --------------------------------------- | ---------- | ------ | --------- |
| `COUNT(*) GROUP BY department`          | **0.098s** | 0.484s | **4.9x**  |
| `COUNT(*) WHERE age > 30 GROUP BY`      | **0.102s** | 0.506s | **5.0x**  |
| `SELECT DISTINCT city`                  | **0.090s** | 0.494s | **5.5x**  |
| `SUM(salary), AVG(salary) GROUP BY`     | **0.132s** | 0.540s | **4.1x**  |
| `MEDIAN(salary) GROUP BY department`    | 2.832s     | **0.580s** | **0.2x** |

The last row is a real loss, not a rounding artefact: `MEDIAN` buffers every value in a group, so its memory and time are O(rows) rather than O(groups) ([#205](https://github.com/melihbirim/csvql/issues/205)).

**Quoting changes the picture; column count does not.** csvql skips 16 bytes at a time whenever no delimiter, newline or quote is present, and a quoted field disables that. Same files, ~200-byte rows, differing only in whether one column is quoted, `COUNT(*)` with a `WHERE`, 12 threads:

| Columns | Quoting | csvql | DuckDB | Speedup |
| --- | --- | --- | --- | --- |
| 2 | unquoted | **0.026s** | 0.170s | **6.5x** |
| 2 | quoted | 0.084s | 0.136s | 1.6x |
| 8 | unquoted | **0.040s** | 0.240s | **6.0x** |
| 8 | quoted | 0.116s | 0.236s | 2.0x |
| 32 | unquoted | **0.080s** | 0.458s | **5.7x** |
| 32 | quoted | 0.196s | 0.384s | 2.0x |

The speedup is steady across a sixteen-fold range of column counts. Quoting is the variable that matters, costing roughly 3x.

Faster on every shape measured here, and dramatically faster on machine-generated CSV, which mostly does not quote. Reproduce with [`bench/bench_scaling.sh`](bench/bench_scaling.sh). Why, and the work in progress: [The Journey to 10x DuckDB](https://melihbirim.github.io/csvql/blog/journey-to-10x-duckdb-csv.html).

**As a library, against polars 2.0 and DuckDB.** One query per process so nothing is cached between calls, engines interleaved round by round, first round discarded because it pays for page cache. polars is driven through `scan_csv` (lazy), which is its recommended path for a file on disk and the faster of its two APIs on every shape here. 20M rows / 711 MB, median of 5:

| Query shape | csvql | polars 2.0 | DuckDB | vs polars | vs DuckDB |
| --- | --- | --- | --- | --- | --- |
| `SUM(salary)` | **0.0807** | 0.2268 | 0.3716 | **2.8x** | **4.6x** |
| `COUNT(*) GROUP BY department` | **0.1241** | 0.3173 | 0.4978 | **2.6x** | **4.0x** |
| `COUNT(*) WHERE city = '...'` | **0.0804** | 0.2818 | 0.4351 | **3.5x** | **5.4x** |
| `LIMIT 10`, no sort | **0.0011** | 0.0029 | 0.0662 | **2.7x** | **60.9x** |
| `COUNT(*)` | 0.0256 | **0.0220** | 0.4005 | 0.9x | **15.6x** |
| `ORDER BY salary LIMIT 10` | 0.3440 | **0.3047** | 0.4197 | 0.9x | **1.2x** |

pandas is not in this table because it has no lazy API to compare against on equal terms; measured separately on the same `GROUP BY` and files, it takes 0.543426s at 2M rows and 6.720655s at 20M, so 39x and 60x. That margin says more about `read_csv` loading the whole file into a DataFrame first than about either engine.

The last two rows are losses to polars, stated because leaving them out would make the table a sales sheet. `COUNT(*)` is bandwidth-bound for both engines at this size: counting records means reading every byte and CSV carries no row count to shortcut to, so at ~0.026s for 711 MB there is nothing left to win. `ORDER BY ... LIMIT` is a draw after [#224](https://github.com/melihbirim/csvql/pull/224) pushed the limit into the sort workers; before that it was ~6x worse.

**Where the gap is widest: wide files.** csvql finds the fields a query names and skips the rest of each row, while a reader that materialises columns pays for all of them. 757 MB, 60 columns, `SUM` of one column at position 58:

| Engine | Seconds | vs csvql |
| --- | --- | --- |
| csvql | **0.0563** | 1.00x |
| polars 2.0 | 0.2187 | 3.9x |
| DuckDB | 0.5767 | 10.2x |

**How it scales.** Same `GROUP BY department`, four engines, one process each, across a 12x range of file size on a 16 GB machine. Peak RSS and wall time:

| file | csvql | DuckDB | polars 2.0 | pandas |
| --- | --- | --- | --- | --- |
| 0.69 GB | **74 MB / 0.2s** | 254 MB / 0.8s | 1151 MB / 0.6s | 1445 MB / 6.5s |
| 1.89 GB | **74 MB / 0.4s** | 253 MB / 1.2s | 2107 MB / 1.1s | 2093 MB / 17.5s |
| 3.80 GB | **75 MB / 0.7s** | 275 MB / 2.1s | 2666 MB / 9.8s | 2355 MB / 36.4s |
| 8.14 GB | **74 MB / 2.4s** | 367 MB / 5.5s | 1754 MB / 26.7s | 4024 MB / 90.8s |

csvql's memory does not move: 74 MB on a 0.69 GB file and 74 MB on an 8.14 GB one, because an aggregate holds a running accumulator rather than a dataset. The time advantage over polars *grows* with the file, from 3x to 11x, and over pandas from 33x to 38x.

Two things worth saying plainly. **DuckDB is the memory-efficient competitor, not polars**: 254 to 367 MB across the same range, nearly as flat as csvql, so against DuckDB the memory difference is 3.7x rather than the 15x polars suggests. And every engine here finished the 8 GB file on 16 GB of RAM, so there is no point at which the others fall over and csvql keeps going. pandas peaks at 4 GB on an 8 GB file because it stores parsed typed columns rather than text, and polars' peak is not even monotonic (2666 MB at 3.8 GB, 1754 MB at 8.1 GB), so do not extrapolate either curve.

Flat memory is a property of aggregate queries, not of every query: `ORDER BY` has to hold rows, and the 711 MB file peaks at 715 MB on a top-N.

All four rows are measured the same way, in-process from Python via `ru_maxrss`, so each includes the interpreter's own footprint of roughly 20 MB. That is why csvql reads 74 MB here and 54 MB when the CLI is measured directly; the table is internally comparable, which matters more than csvql's number looking smaller.

Reproduce with [`bench/bench_vs_polars.sh`](bench/bench_vs_polars.sh). Full working and the corrections to an earlier version of these numbers: [csvql vs polars](https://melihbirim.github.io/csvql/blog/csvql-vs-polars.html).

**NYC Taxi, 20M rows, 8 GB CSV** — raw CSV, no ingest, both engines: **~3.2x** faster, **~6x** less memory, and **0 bytes** of extra storage (DuckDB's fast path needs a 2.1 GB native store first).

Full breakdown (LIKE, multi-table JOIN, subqueries, memory/storage, methodology): **[BENCHMARKS.md](BENCHMARKS.md)**. Reproduce any number yourself: [`bench/bench_all.sh`](bench/bench_all.sh).

## SQL Reference

`SELECT`/`FROM`/`WHERE`/`GROUP BY`/`HAVING`/`ORDER BY`/`LIMIT`/`OFFSET`, `JOIN`, subquery `IN`/`NOT IN`, `LIKE`/`ILIKE`/`BETWEEN`/`IS NULL`/`AND`/`OR`/`NOT`, aggregates (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`/`VARIANCE`/`STDDEV`/`MEDIAN`/`GROUP_CONCAT`), `CASE WHEN`, and scalar functions (`UPPER`/`LOWER`/`TRIM`/`LTRIM`/`RTRIM`/`LEFT`/`RIGHT`/`CONCAT`/`SUBSTR`/`REPLACE`/`SPLIT_PART`/`LPAD`/`RPAD`/`ROUND`/`SQRT`/`POWER`/`LN`/`LOG`/`EXP`/`TRUNC`/`CAST`/`COALESCE`/`NULLIF`/`IFNULL`/`STRFTIME`/`DATEDIFF`/`DATEADD`/and more).

```bash
csvql "SELECT department, COUNT(*), AVG(salary) FROM 'data.csv' WHERE salary > 50000 GROUP BY department HAVING COUNT(*) > 10 ORDER BY department"
csvql "SELECT e.name, d.dept_name FROM 'employees.csv' e JOIN 'departments.csv' d ON e.dept_id = d.id WHERE d.dept_name = 'Engineering'"
csvql "SELECT id FROM 'orders.csv' WHERE customer_id IN (SELECT id FROM 'customers.csv' WHERE region = 'EU')"
```

Full syntax table, runnable examples for every feature, known differences from DuckDB, and current limitations: **[SQL_REFERENCE.md](SQL_REFERENCE.md)**.

Positional "simple mode" is also available for quick one-off filters without writing SQL: `csvql data.csv "name,salary" "age>30" 10 "salary:desc"` — see [SIMPLE_QUERY_LANGUAGE.md](SIMPLE_QUERY_LANGUAGE.md).

## MCP Server

csvql ships as a [Model Context Protocol](https://modelcontextprotocol.io/) server, letting AI assistants (Claude, Copilot, etc.) query your CSV files directly.

```bash
csvql --mcp
```

### Why query instead of paste?

A 1 MB CSV costs **~560,000 tokens** to paste into an LLM — it doesn't even fit a 200K-token context window. Pasting a real dataset is impossible past a few hundred KB, and expensive long before that. With `csvql --mcp` the agent *queries* the file instead and gets back only the rows it asked for:

| CSV size | Paste into context | Query via `csvql --mcp` | Savings |
| -------- | ------------------ | ----------------------- | ------- |
| 1 MB     | 559K tokens ❌ *(overflows)* | ~540 tokens | **1,000x** |
| 10 MB    | 5.6M tokens ❌      | ~550 tokens | **10,000x** |
| 100 MB   | 55M tokens ❌       | ~565 tokens | **98,000x** |
| 417 MB   | 230M tokens ❌      | ~560 tokens | **~410,000x** |

The query cost is **flat** — it's the SQL plus a few result rows, independent of file size — so a 417 MB file costs the same ~560 tokens as a 1 MB one. Five real questions, answered against DuckDB's NYC-taxi data; token counts via `tiktoken` (exact `cl100k`). Reproduce: [`bench/bench_tokens.py`](bench/bench_tokens.py). Your data never leaves your machine.

### Exposed Tools

| Tool | Description |
|------|-------------|
| `csv_query(sql)` | Execute any supported SQL query, returns results as JSON |
| `csv_schema(file)` | Column names and sample rows for a CSV file |
| `csv_list(directory?)` | List CSV files in a directory |

### Supported Queries via MCP

`csv_query` accepts the full SQL dialect supported by csvql. You can ask your AI assistant things like:

| Natural language prompt | SQL sent to `csv_query` |
|---|---|
| "Show me the top 10 customers by revenue" | `SELECT customer, SUM(revenue) AS total FROM 'sales.csv' GROUP BY customer ORDER BY total DESC LIMIT 10` |
| "How many orders per month in 2025?" | `SELECT STRFTIME('%Y-%m', order_date) AS month, COUNT(*) AS orders FROM 'orders.csv' WHERE order_date BETWEEN '2025-01-01' AND '2025-12-31' GROUP BY month ORDER BY 1` |
| "How long does delivery take on average?" | `SELECT AVG(DATEDIFF('hour', shipped_at, delivered_at)) AS avg_hours FROM 'orders.csv' WHERE delivered_at != ''` |
| "Flag orders where picking exceeded SLA" | `SELECT order_id, DATEDIFF('minute', ordered_at, picked_at) AS mins FROM 'orders.csv' WHERE picked_at != ''` (scalar functions in WHERE not yet supported — filter by `mins > 90` in your shell) |
| "Add 2-day estimated delivery to shipments" | `SELECT order_id, DATEADD('day', 2, shipped_at) AS est_delivery FROM 'orders.csv' WHERE shipped_at != ''` |
| "Which employees have no department?" | `SELECT name FROM 'employees.csv' WHERE department IS NULL` |
| "List all cities, deduplicated, sorted" | `SELECT DISTINCT city FROM 'data.csv' ORDER BY city` |
| "Average salary by department, only > 80k avg" | `SELECT department, AVG(salary) AS avg_sal FROM 'data.csv' GROUP BY department HAVING AVG(salary) > 80000 ORDER BY avg_sal DESC` |
| "Join orders with customers, filter by region" | `SELECT o.id, c.name FROM 'orders.csv' o JOIN 'customers.csv' c ON o.customer_id = c.id WHERE c.region = 'West'` |
| "Salaries in range 50k–70k" | `SELECT name, salary FROM 'data.csv' WHERE salary BETWEEN 50000 AND 70000 ORDER BY salary` |
| "Employees not in London or Paris" | `SELECT name, city FROM 'data.csv' WHERE NOT city IN ('London', 'Paris')` |

**Full WHERE clause support:** `=`, `!=`, `>`, `>=`, `<`, `<=`, `LIKE`, `BETWEEN`, `IN`, `IS NULL`, `IS NOT NULL`, `NOT`, `AND`, `OR`

**Full SELECT support:** column projections, `AS` aliases, `DISTINCT`, `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`/`VARIANCE`/`STDDEV`/`MEDIAN`/`GROUP_CONCAT`, `GROUP BY`, `HAVING`, `ORDER BY` (by name, alias, or position), `LIMIT`, `STRFTIME()`, `DATE_PART()`, `JOIN`, `UPPER`/`LOWER`/`TRIM`/`LTRIM`/`RTRIM`/`REVERSE`/`LENGTH`/`SUBSTR`/`LEFT`/`RIGHT`/`REPLACE`/`SPLIT_PART`/`GREATEST`/`LEAST`, `ABS`/`SIGN`/`CEIL`/`FLOOR`/`MOD`/`ROUND`/`TRUNC`/`SQRT`/`POWER`/`LN`/`LOG`/`EXP`, `COALESCE`, `NULLIF`/`IFNULL`, `CAST`, `DATEDIFF`, `DATEADD`, `EXTRACT`

### Setup

**One command (recommended)** — registers csvql in Claude Code and Claude Desktop, no manual config:

```bash
csvql install          # add --print to dry-run first
```

It runs `claude mcp add` for Claude Code (if the CLI is present) and merges an `mcpServers.csvql` entry into the Claude Desktop config, preserving your other servers. Restart Claude afterward.

**Claude Desktop (one-click)** — grab the `csvql-<platform>.mcpb` for your OS from [Releases](https://github.com/melihbirim/csvql/releases) and open it in Claude Desktop (Settings → Extensions). No terminal. Build it yourself with [`scripts/build-mcpb.sh`](scripts/build-mcpb.sh).

<details>
<summary>Manual config (if you prefer)</summary>

**VS Code (Copilot)** — create `.vscode/mcp.json` in your workspace:

```json
{
  "servers": {
    "csvql": {
      "type": "stdio",
      "command": "/usr/local/bin/csvql",
      "args": ["--mcp"]
    }
  }
}
```

**Claude Desktop** — add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "csvql": {
      "command": "/usr/local/bin/csvql",
      "args": ["--mcp"]
    }
  }
}
```

</details>

Once connected, you can ask your AI assistant to query CSV files directly:
> *"What are the top 5 product categories by revenue this year?"*

### Remote & on-prem: query data where it lives

Big files are hard to download — so run csvql **on the server next to the data** and connect over SSH. Only the SQL query and the small result cross the wire; the data never leaves the box. No open port, no reverse proxy — it rides your existing SSH keys and audit trail:

```jsonc
// client MCP config — csvql runs on the remote server
{ "command": "ssh", "args": ["analyst@dataserver", "csvql", "--mcp", "--root", "/data"] }
```

**`--root` sandboxes file access.** With `--root /data`, queries can only read files under `/data` — `SELECT * FROM '/etc/passwd'` and `../` traversal are rejected. Always set `--root` when exposing csvql to an agent or another user. Pair it with a restricted OS user and a read-only mount for defense in depth.

**Read-only by construction:** csvql only runs `SELECT` — it has no `INSERT`/`UPDATE`/`DELETE`/`DROP` and cannot modify your data. It makes zero outbound network calls and runs fully air-gapped. Full posture and hardening guidance in [SECURITY.md](SECURITY.md).

## Language Libraries

csvql ships as a native library for Python and Node.js — same SIMD engine, same performance, no subprocess.

```bash
pip install csvql-query          # Python: csvql.query(), .query_df(), .query_csv()
zig build node -Doptimize=ReleaseFast   # Node.js: require('csvql-query')
```

Full API, options (delimiter/comment/skip-empty-lines), memory comparisons against `csv-parse`/`papaparse`, and a runnable ETL example: **[docs/NODE.md](docs/NODE.md)** · **[docs/PYTHON.md](docs/PYTHON.md)**.

## Documentation

| Document                                             | Description                                         |
| ---------------------------------------------------- | --------------------------------------------------- |
| [SQL_REFERENCE.md](SQL_REFERENCE.md)                 | Full SQL syntax, runnable examples, DuckDB differences, limitations |
| [BENCHMARKS.md](BENCHMARKS.md)                       | Detailed performance analysis vs DuckDB, ClickHouse |
| [CORRECTNESS.md](CORRECTNESS.md)                     | What's tested against DuckDB, how, known gaps, error behaviour |
| [ARCHITECTURE.md](ARCHITECTURE.md)                   | Engine design, optimization techniques              |
| [SECURITY.md](SECURITY.md)                           | Security posture, network/disk-write guarantees, hardening |
| [SIMPLE_QUERY_LANGUAGE.md](SIMPLE_QUERY_LANGUAGE.md) | Simple mode syntax reference                        |
| [docs/NODE.md](docs/NODE.md)                         | Node.js library: full API, options, ETL example     |
| [docs/PYTHON.md](docs/PYTHON.md)                     | Python library: full API                            |
| [docs/LIBRARY.md](docs/LIBRARY.md)                   | Using the CSV parser as a Zig library               |
| [CONTRIBUTING.md](CONTRIBUTING.md)                   | Contribution guidelines                             |

## Roadmap

| Feature                             | Issue                                                | Status              |
| ----------------------------------- | ---------------------------------------------------- | ------------------- |
| `--no-header` / `--delimiter` flags | [#12](https://github.com/melihbirim/csvql/issues/12) | ✅ shipped (v0.5.0) |
| `LIKE` operator in WHERE            | [#13](https://github.com/melihbirim/csvql/issues/13) | ✅ shipped          |
| `--json` / `--jsonl` output format  | [#14](https://github.com/melihbirim/csvql/issues/14) | ✅ shipped          |
| `HAVING` clause                     |                                                      | ✅ shipped          |
| `STRFTIME()` date bucketing         |                                                      | ✅ shipped          |
| MCP server (`--mcp`)                |                                                      | ✅ shipped          |
| `AS` alias in SELECT & ORDER BY     |                                                      | ✅ shipped          |
| `BETWEEN low AND high`              |                                                      | ✅ shipped          |
| `IS NULL` / `IS NOT NULL`           |                                                      | ✅ shipped          |
| `NOT` prefix for conditions         |                                                      | ✅ shipped          |
| `ORDER BY` positional (`ORDER BY 1`)|                                                      | ✅ shipped          |
| `GROUP BY` alias (`GROUP BY month`) |                                                      | ✅ shipped          |
| `CASE WHEN` inside aggregates       |                                                      | ✅ shipped          |
| `ILIKE` in WHERE                    |                                                      | ✅ shipped          |
| `UPPER`, `LOWER`, `TRIM`, `REVERSE`, `LENGTH`, `SUBSTR` in SELECT |                  | ✅ shipped          |
| `ABS`, `SIGN`, `CEIL`, `FLOOR`, `MOD` in SELECT |                                         | ✅ shipped          |
| `SQRT`, `POWER`, `LN`, `LOG`, `EXP`, `TRUNC` in SELECT | [#157](https://github.com/melihbirim/csvql/issues/157) | ✅ shipped |
| `ROUND(col)` / `ROUND(col, n)` in SELECT |                                               | ✅ shipped          |
| `COALESCE` in SELECT                |                                                      | ✅ shipped          |
| `NULLIF`, `IFNULL` in SELECT | [#204](https://github.com/melihbirim/csvql/issues/204) | ✅ shipped |
| `CAST` in SELECT                    |                                                      | ✅ shipped          |
| `DATE_PART()`, `DATEDIFF`, `DATEADD`, `EXTRACT` |                                          | ✅ shipped          |
| `JOIN` (inner, hash join)           |                                                      | ✅ shipped          |
| `--threads` parallelism control     | [#51](https://github.com/melihbirim/csvql/issues/51) | ✅ shipped (v1.7.0) |
| `--no-input-header` (headerless CSVs, `c1..cN`) | [#49](https://github.com/melihbirim/csvql/issues/49) | ✅ shipped (v1.7.0) |
| MCP token guardrails + `csvql install` + `.mcpb` bundle | [#54](https://github.com/melihbirim/csvql/issues/54) | ✅ shipped (v1.7.0) |
| `--root` file-access sandbox        | [#58](https://github.com/melihbirim/csvql/issues/58) | ✅ shipped (v1.7.0) |
| `--audit` query log                 | [#62](https://github.com/melihbirim/csvql/issues/62) | ✅ shipped (v1.7.0) |
| `-o`/`--output <file>`               | [#71](https://github.com/melihbirim/csvql/issues/71) | ✅ shipped (v1.8.0) |
| `REPLACE`, `SPLIT_PART`, `GREATEST`, `LEAST` | [#67](https://github.com/melihbirim/csvql/issues/67) | ✅ shipped (v1.8.0) |
| `VARIANCE`, `STDDEV`, `MEDIAN`, `GROUP_CONCAT` | [#50](https://github.com/melihbirim/csvql/issues/50) | ✅ shipped (v1.9.0) |
| HTTP/SSE MCP transport (shared service) | [#60](https://github.com/melihbirim/csvql/issues/60) | planned             |
| `OFFSET` clause | [#70](https://github.com/melihbirim/csvql/issues/70) | ✅ shipped |
| `--markdown` output | [#72](https://github.com/melihbirim/csvql/issues/72) | ✅ shipped          |
| Shell completions (bash/zsh) | [#73](https://github.com/melihbirim/csvql/issues/73) | help wanted |
| `LPAD`, `RPAD` in SELECT | [#160](https://github.com/melihbirim/csvql/issues/160) | ✅ shipped |
| `LTRIM`, `RTRIM`, `LEFT`, `RIGHT` in SELECT | [#203](https://github.com/melihbirim/csvql/issues/203) | ✅ shipped (`REPEAT`, `POSITION`, `INSTR` still open) |

## Contributing

Contributions welcome — bug reports, performance improvements, features, docs. See [CONTRIBUTING.md](CONTRIBUTING.md).

New here? The [**good first issues**](https://github.com/melihbirim/csvql/labels/good%20first%20issue) are scoped with file pointers and clear done-when criteria — a great place to start (new SQL functions, output formats, and more).

## License

MIT — see [LICENSE.md](LICENSE.md).

---

**Built with Zig** · **4-9x faster than DuckDB on raw CSV** · **MCP Server** · [GitHub](https://github.com/melihbirim/csvql)
