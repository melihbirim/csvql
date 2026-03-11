<p align="center">
  <img src="logo.svg" alt="csvql" width="420"/>
</p>

[![CI](https://github.com/melihbirim/csvql/actions/workflows/ci.yml/badge.svg)](https://github.com/melihbirim/csvql/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE.md)
[![Release](https://img.shields.io/github/v/release/melihbirim/csvql)](https://github.com/melihbirim/csvql/releases)

**Query CSV files with SQL. Faster than DuckDB.**

```bash
$ csvql "SELECT name, city, salary FROM 'employees.csv' WHERE salary > 100000 ORDER BY salary DESC LIMIT 5"

name,city,salary
Alice,San Francisco,185000
Bob,New York,172000
Carol,Seattle,168000
Dave,Austin,155000
Eve,Boston,142000

  0.020s — 9x faster than DuckDB on 1M rows
```

[Quick Start](#quick-start) · [Installation](#installation) · [Performance](#performance) · [SQL Reference](#sql-reference) · [Docs](#documentation)

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

| Flag | Short | Description |
| ---- | ----- | ----------- |
| `--no-header` | | Suppress header row in output |
| `--delimiter <char>` | `-d` | Field delimiter (default `,`). Use `\t` for TSV |
| `--json` | | Output as a JSON array (`[{...}, ...]`) |
| `--jsonl` | | Output as JSONL / NDJSON (one JSON object per line) |
| `--version` | `-v` | Show version |
| `--help` | `-h` | Show help |

```bash
# TSV file
csvql "SELECT name, salary FROM 'data.tsv'" -d $'\t'

# Pipe into another tool that expects no header
csvql "SELECT name, age FROM 'data.csv'" --no-header | awk -F, '{print $2}'

# TSV input, no header in output
cat data.tsv | csvql "SELECT * FROM '-'" -d $'\t' --no-header
```

## Installation

### Prebuilt Binaries (recommended)

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

**1M rows, 35MB CSV, Apple M2** — all tools forced to output all rows (no display tricks):

| Query                             | csvql      | DuckDB | Speedup  |
| --------------------------------- | ---------- | ------ | -------- |
| WHERE + ORDER BY LIMIT 10         | **0.020s** | 0.179s | **9x**   |
| ORDER BY LIMIT 10                 | **0.041s** | 0.165s | **4x**   |
| ORDER BY (all 1M rows)            | **0.156s** | 1.221s | **7.8x** |
| WHERE (full output)               | **0.141s** | 0.739s | **5.2x** |
| Full scan (all 1M rows)           | **0.196s** | 1.163s | **5.9x** |
| `COUNT(*) GROUP BY` (6 groups)    | **0.060s** | 0.110s | **1.8x** |
| `SUM + AVG GROUP BY` (6 groups)   | **0.070s** | 0.110s | **1.6x** |
| `SELECT DISTINCT city` (8 values) | **0.060s** | 0.110s | **1.8x** |
| `SELECT COUNT(*)` scalar          | **0.050s** | 0.100s | **2x**   |
| `SELECT SUM(salary)` scalar       | **0.050s** | 0.110s | **2.2x** |

**35x less memory** than DuckDB (1.8MB vs 63.5MB).

**5M rows, 173MB CSV, Apple M2** — output-format benchmark (full output, all rows matched, `> /dev/null`):

| Output format | csvql | DuckDB | Speedup |
| ------------- | ----- | ------ | ------- |
| CSV | **0.100s** | 0.354s | **3.5x** |
| JSON array (`--json`) | **0.164s** | 0.434s | **2.6x** |
| JSONL / NDJSON (`--jsonl`) | **0.172s** | 0.422s | **2.5x** |

Outputs are semantically/byte-identical to DuckDB (verified: CSV byte-for-byte diff; JSONL byte-for-byte diff; JSON array Python-parsed row comparison).

Run the benchmark yourself: [`bench/bench_output_formats.sh`](bench/bench_output_formats.sh)

**5M rows, 173MB CSV, Apple M2** — LIKE operator benchmark (CSV output, `> /dev/null`):

| Pattern | Description | csvql | DuckDB | Speedup |
| ------- | ----------- | ----- | ------ | ------- |
| `WHERE name LIKE 'A%'` | Prefix wildcard | **0.06s** | 2.17s | **~36x** |
| `WHERE city LIKE '%on'` | Suffix wildcard | **0.06s** | 1.12s | **~19x** |
| `WHERE department LIKE '%ing'` | Suffix, high selectivity | **0.07s** | 2.54s | **~36x** |

Row counts verified identical to DuckDB.

Run the benchmark yourself: [`bench/bench_like.sh`](bench/bench_like.sh)

<details>
<summary><b>How is csvql so fast?</b></summary>

- **Memory-mapped I/O** — zero-copy reading at 1.4 GB/sec
- **7-core parallel execution** — lock-free architecture, 669% CPU utilization
- **SIMD field parsing** — vectorized comma detection
- **Radix sort** — O(8N) with IEEE 754 f64→u64 bit trick and pass-skipping
- **Top-K heap** — O(N log K) for LIMIT queries, avoids sorting entire dataset
- **Hardware-aware thresholds** — ARM vs x86 tuned for L1 cache
- **Zero per-row allocations** — arena buffers, zero-copy slices

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full optimization story.

</details>

<details>
<summary><b>Benchmark methodology</b></summary>

DuckDB and DataFusion CLIs default to displaying only 40 rows, making them appear faster than they are. Our benchmarks use `-csv` mode (DuckDB) and `FORMAT CSV` (ClickHouse) to force full output materialization. DataFusion CLI caps output at ~8K rows regardless of settings, so full-output numbers are unavailable.

See [BENCHMARKS.md](BENCHMARKS.md) for the complete analysis.

</details>

## SQL Reference

### Supported

| Feature          | Syntax                                                                  |
| ---------------- | ----------------------------------------------------------------------- |
| **SELECT**       | `SELECT col1, col2` or `SELECT *`                                       |
| **DISTINCT**     | `SELECT DISTINCT col1, col2` — deduplicates output rows                 |
| **FROM**         | `FROM 'file.csv'` or `FROM -` (stdin)                                   |
| **WHERE**        | `=`, `!=`, `>`, `>=`, `<`, `<=` with auto numeric coercion              |
| **LIKE**         | `WHERE col LIKE 'pattern'` — `%` any sequence, `_` any single character |
| **GROUP BY**     | `GROUP BY col1` — groups rows for aggregation                           |
| **COUNT**        | `COUNT(*)` or `COUNT(col)` — with or without `GROUP BY`                 |
| **SUM**          | `SUM(col)` — with or without `GROUP BY`                                 |
| **AVG**          | `AVG(col)` — full precision; with or without `GROUP BY`                 |
| **MIN / MAX**    | `MIN(col)`, `MAX(col)` — with or without `GROUP BY`                     |
| **ORDER BY**     | `ORDER BY col ASC/DESC`                                                 |
| **LIMIT**        | `LIMIT n`                                                               |

### Aggregate Examples

```bash
# Scalar aggregates (whole table)
csvql "SELECT COUNT(*), SUM(salary), AVG(salary), MIN(age), MAX(age) FROM 'data.csv'"

# Grouped aggregates
csvql "SELECT department, COUNT(*), AVG(salary) FROM 'data.csv' GROUP BY department ORDER BY department"

# DISTINCT
csvql "SELECT DISTINCT city FROM 'data.csv' ORDER BY city"
csvql "SELECT DISTINCT city, department FROM 'data.csv'"

# DISTINCT with WHERE
csvql "SELECT DISTINCT department FROM 'data.csv' WHERE salary > 100000"
```

### Simple Mode

Positional args: `csvql <file> [columns] [filter] [limit] [sort]`

```bash
csvql data.csv "name,salary" "age>30" 10 "salary:desc"
```

See [SIMPLE_QUERY_LANGUAGE.md](SIMPLE_QUERY_LANGUAGE.md) for the full reference.

## Documentation

| Document                                             | Description                                         |
| ---------------------------------------------------- | --------------------------------------------------- |
| [ARCHITECTURE.md](ARCHITECTURE.md)                   | Engine design, optimization techniques              |
| [BENCHMARKS.md](BENCHMARKS.md)                       | Detailed performance analysis vs DuckDB, ClickHouse |
| [SIMPLE_QUERY_LANGUAGE.md](SIMPLE_QUERY_LANGUAGE.md) | Simple mode syntax reference                        |
| [docs/LIBRARY.md](docs/LIBRARY.md)                   | Using the CSV parser as a Zig library               |
| [CONTRIBUTING.md](CONTRIBUTING.md)                   | Contribution guidelines                             |

## Roadmap

| Feature | Issue | Status |
| ------- | ----- | ------ |
| `--no-header` / `--delimiter` flags | [#12](https://github.com/melihbirim/csvql/issues/12) | ✅ shipped (v0.5.0) |
| `LIKE` operator in WHERE | [#13](https://github.com/melihbirim/csvql/issues/13) | ✅ shipped |
| `--json` / `--jsonl` output format | [#14](https://github.com/melihbirim/csvql/issues/14) | ✅ shipped |

## Contributing

Contributions welcome — bug reports, performance improvements, features, docs. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT — see [LICENSE.md](LICENSE.md).

---

**Built with Zig** · **9x faster than DuckDB** · [GitHub](https://github.com/melihbirim/csvql)
