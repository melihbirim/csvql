# csvql

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

| Query                     | csvql      | DuckDB | ClickHouse | Speedup  |
| ------------------------- | ---------- | ------ | ---------- | -------- |
| WHERE + ORDER BY LIMIT 10 | **0.020s** | 0.179s | 0.750s     | **9x**   |
| ORDER BY LIMIT 10         | **0.041s** | 0.165s | 0.761s     | **4x**   |
| ORDER BY (all 1M rows)    | **0.156s** | 1.221s | 0.451s     | **7.8x** |
| WHERE (full output)       | **0.141s** | 0.739s | 0.796s     | **5.2x** |
| Full scan (all 1M rows)   | **0.196s** | 1.163s | 0.798s     | **5.9x** |

**35x less memory** than DuckDB (1.8MB vs 63.5MB).

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

| Feature      | Syntax                                                     |
| ------------ | ---------------------------------------------------------- |
| **SELECT**   | `SELECT col1, col2` or `SELECT *`                          |
| **FROM**     | `FROM 'file.csv'` or `FROM -` (stdin)                      |
| **WHERE**    | `=`, `!=`, `>`, `>=`, `<`, `<=` with auto numeric coercion |
| **ORDER BY** | `ORDER BY col ASC/DESC`                                    |
| **LIMIT**    | `LIMIT n`                                                  |

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

## Contributing

Contributions welcome — bug reports, performance improvements, features, docs. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT — see [LICENSE.md](LICENSE.md).

---

**Built with Zig** · **9x faster than DuckDB** · [GitHub](https://github.com/melihbirim/csvql)
