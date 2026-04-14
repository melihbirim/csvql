# csvql-query

[![CI](https://github.com/melihbirim/csvql/actions/workflows/ci.yml/badge.svg)](https://github.com/melihbirim/csvql/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/melihbirim/csvql/blob/main/LICENSE.md)
[![PyPI](https://img.shields.io/pypi/v/csvql-query)](https://pypi.org/project/csvql-query/)

**Query CSV files with SQL from Python — powered by a Zig/SIMD engine.**

Zero-copy mmap reads + SIMD parsing happen before Python ever sees the data.
Faster than DuckDB on typical workloads, no dependencies required.

## Installation

```bash
pip install csvql-query
```

## Quick Start

```python
import csvql

# Returns a list of dicts (like csv.DictReader, but with SQL)
rows = csvql.query("SELECT name, salary FROM 'employees.csv' WHERE salary > 100000 ORDER BY salary DESC")
# [{'name': 'Alice', 'salary': '185000'}, ...]

# Raw CSV string
csv_str = csvql.query_csv("SELECT * FROM 'data.csv' LIMIT 10")

# pandas DataFrame (pandas must be installed)
df = csvql.query_df("SELECT category, COUNT(*) as n FROM 'sales.csv' GROUP BY category")

# (headers, rows) tuples — no dependencies
headers, rows = csvql.query_tuples("SELECT name, age FROM 'users.csv' WHERE age > 25")
```

## API

| Function | Returns | Description |
|---|---|---|
| `query(sql)` | `list[dict]` | Execute SQL, get list of dicts |
| `query_csv(sql)` | `str` | Execute SQL, get raw CSV string |
| `query_df(sql)` | `DataFrame` | Execute SQL, get pandas DataFrame |
| `query_tuples(sql)` | `(list[str], list[tuple])` | Execute SQL, get (headers, rows) |

## SQL Support

The SQL path is embedded in the query string (same as the CLI):

```python
# Filtering, ordering, limiting
csvql.query("SELECT name, city FROM 'data.csv' WHERE age > 30 ORDER BY name LIMIT 5")

# Aggregation
csvql.query("SELECT department, AVG(salary) FROM 'emp.csv' GROUP BY department")

# Unix pipes — use '-' as the filename
import subprocess, sys
# or just pass stdin data via the engine directly
```

Full SQL reference: [SIMPLE_QUERY_LANGUAGE.md](https://github.com/melihbirim/csvql/blob/main/SIMPLE_QUERY_LANGUAGE.md)

## Performance

- mmap + SIMD parsing — data is never copied into Python memory
- Parallel chunk processing on multi-core machines
- Typically 5–9x faster than DuckDB on 1M-row CSVs

## Requirements

- Python ≥ 3.10
- macOS (x86_64 / arm64) or Linux (x86_64)
- `pandas` optional — only needed for `query_df()`

## Links

- [GitHub](https://github.com/melihbirim/csvql)
- [CLI Installation](https://github.com/melihbirim/csvql#installation)
- [SQL Reference](https://github.com/melihbirim/csvql/blob/main/SIMPLE_QUERY_LANGUAGE.md)
