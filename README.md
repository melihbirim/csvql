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

**2M rows, 56 MB CSV, Apple M2 Pro** — aggregates on the raw CSV (best-of-5):

| Query                        | csvql      | DuckDB | Speedup   |
| ----------------------------- | ---------- | ------ | --------- |
| `SELECT COUNT(*)` scalar      | **0.012s** | 0.136s | **11.3x** |
| `COUNT(*) GROUP BY`           | **0.020s** | 0.146s | **7.3x**  |
| `JOIN SELECT *` (2M × 6)      | **0.088s** | 7.832s | **89x**   |

**NYC Taxi, 20M rows, 8 GB CSV** — raw CSV, no ingest, both engines: **~3.2x** faster, **~6x** less memory, and **0 bytes** of extra storage (DuckDB's fast path needs a 2.1 GB native store first). At this scale csvql reads raw CSV about as fast as `cat` — the read itself is the bound, not parsing.

Full breakdown (LIKE, multi-table JOIN, subqueries, memory/storage, methodology): **[BENCHMARKS.md](BENCHMARKS.md)**. Reproduce any number yourself: [`bench/bench_all.sh`](bench/bench_all.sh).

## SQL Reference

`SELECT`/`FROM`/`WHERE`/`GROUP BY`/`HAVING`/`ORDER BY`/`LIMIT`/`OFFSET`, `JOIN`, subquery `IN`/`NOT IN`, `LIKE`/`ILIKE`/`BETWEEN`/`IS NULL`/`AND`/`OR`/`NOT`, aggregates (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`/`VARIANCE`/`STDDEV`/`MEDIAN`/`GROUP_CONCAT`), `CASE WHEN`, and scalar functions (`UPPER`/`LOWER`/`TRIM`/`CONCAT`/`SUBSTR`/`REPLACE`/`SPLIT_PART`/`ROUND`/`CAST`/`COALESCE`/`STRFTIME`/`DATEDIFF`/`DATEADD`/and more).

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

**Full SELECT support:** column projections, `AS` aliases, `DISTINCT`, `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`/`VARIANCE`/`STDDEV`/`MEDIAN`/`GROUP_CONCAT`, `GROUP BY`, `HAVING`, `ORDER BY` (by name, alias, or position), `LIMIT`, `STRFTIME()`, `DATE_PART()`, `JOIN`, `UPPER`/`LOWER`/`TRIM`/`LENGTH`/`SUBSTR`/`REPLACE`/`SPLIT_PART`/`GREATEST`/`LEAST`, `ABS`/`CEIL`/`FLOOR`/`MOD`/`ROUND`, `COALESCE`, `CAST`, `DATEDIFF`, `DATEADD`, `EXTRACT`

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
| `UPPER`, `LOWER`, `TRIM`, `LENGTH`, `SUBSTR` in SELECT |                             | ✅ shipped          |
| `ABS`, `CEIL`, `FLOOR`, `MOD` in SELECT |                                                 | ✅ shipped          |
| `ROUND(col)` / `ROUND(col, n)` in SELECT |                                               | ✅ shipped          |
| `COALESCE` in SELECT                |                                                      | ✅ shipped          |
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
| `--markdown` output | [#72](https://github.com/melihbirim/csvql/issues/72) | help wanted |
| Shell completions (bash/zsh) | [#73](https://github.com/melihbirim/csvql/issues/73) | help wanted |

## Contributing

Contributions welcome — bug reports, performance improvements, features, docs. See [CONTRIBUTING.md](CONTRIBUTING.md).

New here? The [**good first issues**](https://github.com/melihbirim/csvql/labels/good%20first%20issue) are scoped with file pointers and clear done-when criteria — a great place to start (new SQL functions, output formats, and more).

## License

MIT — see [LICENSE.md](LICENSE.md).

---

**Built with Zig** · **9x faster than DuckDB** · **MCP Server** · [GitHub](https://github.com/melihbirim/csvql)
