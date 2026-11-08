---
layout: post
title: "csvql vs DuckDB MCP Servers: Which Read-Only CSV Server Should an Agent Get?"
description: "There are several MCP servers that give Claude read-only SQL over CSV files. Most wrap DuckDB. csvql does not. Here is what each one lets an agent do, what it refuses, and when DuckDB is the better choice."
date: 2026-11-08
---

If you want Claude Desktop or Claude Code to answer questions about CSV files with SQL, there are now several MCP servers to pick from. Most of them wrap DuckDB. csvql (published as `csvql-query` on npm and PyPI) is its own engine. This page compares them on the things that matter when the caller is an agent: what it can read, what it can do to your machine, how much comes back per call, and how much SQL it understands.

Everything here is from the projects' own READMEs as of November 2026, plus csvql's measured numbers. Where a README does not say, the table says "not stated", and that is a real difference: the agent gets whatever the default is.

## The candidates

- **csvql** `--mcp`: a single Zig binary. Reads CSV, TSV and the first sheet of an `.xlsx`. Tools: `csv_query`, `csv_schema`, `csv_list`.
- **duckdb-mcp** (0xmortuex): Python, runs DuckDB in memory. Reads CSV, TSV, Parquet, JSON, NDJSON and gzipped text. Tools: `list_files`, `describe_file`, `profile_file`, `sample_file`, `query`, `explain_query`.
- **duckcoop**: Node, runs DuckDB. Reads CSV, Parquet and JSON. Tools: `query`, `list_files`.
- **DuckDB CLI through a shell tool**: not an MCP server, but the thing an agent with shell access will reach for. Included because it is the real baseline.

## What the agent can do

| The agent asks the tool to... | csvql | duckdb-mcp | duckcoop | DuckDB CLI |
| --- | --- | --- | --- | --- |
| Write a file (`COPY ... TO`) | No. The parser has no write statement | No. Statement gate rejects `COPY`, `INSERT`, `CREATE`, `ATTACH`, `EXPORT` | Not stated. `CREATE TEMP TABLE` is allowed | Yes |
| Read a file outside its folder (`../`, `/etc/hosts`) | No. `PathOutsideAllowedRoot`; the folder is `--root` or the start directory | No. DuckDB `allowed_directories`, set from `--dir` | No. Sandbox root is the CLI argument | Yes |
| Fetch a URL, install an extension | No. The binary has no network code | No. `enable_external_access=false`, auto-install off, `lock_configuration=true` | Not stated | Yes, by default |
| Return a whole large table in one call | No. About 100 rows / 12 KB per call | Capped at 1,000 rows by default, cells cut at 200 characters | Not stated | Yes |
| Read a column you marked private | No, with `--deny-columns email,ssn` | Only if you pre-build views | Not stated | Only if you pre-build views |
| Run for minutes on a bad query | Scans are linear; no timeout flag | 30 second timeout by default; README notes it cannot cap CPU or memory | Not stated | Yes |
| Keep an audit trail of queries | `--audit file.jsonl` | Not stated | Not stated | No |

The read-only story differs in kind, not just degree. csvql's parser has no `INSERT`, `UPDATE`, `DELETE`, `DROP` or `COPY` to gate; there is nothing to bypass. duckdb-mcp gates statements in front of a DuckDB that can write, and its README says so plainly. duckcoop allows `CREATE TEMP TABLE`, which is scratch space rather than a write to your files, but the README does not state what else is blocked.

## What SQL the agent gets

| | csvql | DuckDB-based servers |
| --- | --- | --- |
| Joins | Inner and left hash joins, chained | All join types |
| Window functions, `UNION`, CTEs | No, they error | Yes |
| Subqueries | `col IN (SELECT ...)` only | Full |
| Parquet, JSON input | No | Yes |
| Excel `.xlsx` | First sheet | No (duckdb-mcp says export to CSV first) |
| Scalar functions in `WHERE` | `DATEDIFF`, `DATEADD`, `EXTRACT` only | Full |
| Correctness check | 128 queries verified against DuckDB on every commit | It is DuckDB |

If the questions are mostly "group this by that", "top N", "join these two exports" and "how many rows match", csvql covers it. If the agent needs window functions, Parquet or the long tail of DuckDB's SQL, use one of the DuckDB servers. csvql errors on what it does not support rather than guessing, so the agent finds out on the first query, not after a wrong answer.

## Memory on the box next to the data

The reason csvql exists at all. Same `GROUP BY department` on raw CSV, peak memory, one process per engine:

| File | csvql | DuckDB | polars |
| --- | --- | --- | --- |
| 0.72 GB | 54 MB | 221 MB | 1,137 MB |
| 8 GB | 74 MB | 367 MB | 1,754 MB |

Speed is in the same band as DuckDB on raw CSV. The DuckDB-based MCP servers inherit DuckDB's memory profile, plus a Python or Node runtime. This matters when the server runs on a small VM next to the files, which is the setup the `--root` sandbox is designed for.

## Setup, side by side

```bash
# csvql: one binary, registers itself in Claude Code and Claude Desktop
brew install melihbirim/csvql/csvql
cd ~/data && csvql install

# duckdb-mcp: needs uv
claude mcp add duckdb -- uvx duckdb-mcp --dir ~/data

# duckcoop: build from source, Node
npm run build && node dist/cli.js ~/data
```

Claude Desktop starts MCP servers in `/`, not in your project folder. csvql refuses to start there without `--root`, and `csvql install` writes the `--root` for you. duckdb-mcp defaults to the current directory when `--dir` is unset; check what that resolves to under Desktop.

## When to pick which

- **Pick csvql** when the data is CSV or Excel exports, the agent is not fully trusted, and you want the sandbox, the output cap and the column deny list without configuring anything. Also when the server runs on a small machine.
- **Pick duckdb-mcp** when you need Parquet, JSON, window functions or profiling tools, and you are comfortable that read-only is enforced by a statement gate in front of a writable engine.
- **Pick duckcoop** when you already run Node and want DuckDB SQL with a sandbox root, and you have read its ADRs for the security model, since the README leaves most of the above unstated.
- **Do not give the agent the DuckDB CLI through a shell** unless you have set `enable_external_access=false` and read-only mode yourself. By default it can write files and fetch URLs.

Check the csvql column yourself: every claim is in the [README](https://github.com/melihbirim/csvql#what-an-agent-can-do-out-of-the-box) with the command that demonstrates it. The DuckDB CLI defaults: `duckdb -c "SELECT current_setting('enable_external_access')"`.
