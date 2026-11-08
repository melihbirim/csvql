---
layout: post
title: "How to Let Claude Desktop Query a CSV File With SQL Without Reading the Whole File"
description: "Three commands to give Claude Desktop a read-only SQL tool over one folder of CSV and Excel files. What the config looks like, why --root is required, what the agent can and cannot do, and how to check it worked."
date: 2026-11-08
---

Claude Desktop cannot read a large CSV: a 1 MB file is already several times a 200K token context. What it can do is call a tool that runs SQL on the file and returns a few rows. This page sets that up with csvql (published as `csvql-query` on npm and PyPI), a single read-only binary that ships as an MCP server.

## 1. Install

```bash
brew install melihbirim/csvql/csvql
```

Or download `csvql-<platform>.tar.gz` from the [releases page](https://github.com/melihbirim/csvql/releases) and put the binary on your `PATH`. Windows: `csvql-windows-x86_64.zip`.

## 2. Register it, from the folder with your data

```bash
cd ~/Documents/exports && csvql install
```

This merges a `csvql` entry into Claude Desktop's config, keeping your other servers, and also registers it in Claude Code if that CLI is installed. Use `csvql install --print` to see what it would write without writing it.

The entry it writes looks like this (macOS path shown; on Windows the config is under `%APPDATA%\Claude`):

```json
{
  "mcpServers": {
    "csvql": {
      "command": "/opt/homebrew/bin/csvql",
      "args": ["--mcp", "--root", "/Users/you/Documents/exports"]
    }
  }
}
```

`--root` is not optional here. Claude Desktop starts every MCP server in `/`, the filesystem root, not in any project folder. Without `--root`, csvql would be confined to `/`, which is no confinement at all, so it refuses to start and tells you to add the flag. `csvql install` sets it to the folder you ran it from. Several folders: `csvql install --root ~/exports --root ~/finance`.

Restart Claude Desktop.

## 3. Ask a question

In a new conversation: "In orders.csv, what is total revenue by region, highest first?"

Claude sees three tools, `csv_list`, `csv_schema` and `csv_query`, and typically runs:

```sql
SELECT region, SUM(revenue) AS total FROM 'orders.csv' GROUP BY region ORDER BY total DESC
```

It gets back the result rows as JSON, a few hundred tokens, and answers. File paths are relative to `--root`, so `'orders.csv'` means `~/Documents/exports/orders.csv`.

## What the agent can and cannot do

- **Read**: CSV, TSV and the first sheet of `.xlsx` files under `--root`. Nothing outside: `../` and absolute paths fail with `PathOutsideAllowedRoot`.
- **Write**: nothing. The SQL has no `INSERT`, `UPDATE`, `DELETE`, `DROP` or `COPY`.
- **Network**: none. The binary has no network code.
- **Per call**: at most about 100 rows / 12 KB. A `SELECT *` on a big file returns the first page; the agent pages with `LIMIT` and `OFFSET` if it needs more.
- **SQL**: `SELECT` with `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, inner and left joins across files, `IN (SELECT ...)`, aggregates, string and date functions. No window functions, no `UNION`. Unsupported SQL errors; it does not return a guess.

Columns the agent must never see, such as `email` or `national_id`, go in `--deny-columns`:

```json
"args": ["--mcp", "--root", "/Users/you/Documents/exports", "--deny-columns", "email,national_id"]
```

Any query that reads those columns is refused, and `SELECT *` leaves them out. Details: [Hide PII columns from an AI agent](hide-pii-columns-from-an-ai-agent.html).

## Check it worked

Ask Claude "which CSV files can you see?" It should call `csv_list` and name the files under your folder, and nothing else. Then ask it to read a file outside the folder. The tool returns `PathOutsideAllowedRoot`, and Claude will tell you it cannot.

If csvql does not appear in the tools list, run `csvql --mcp --root ~/Documents/exports` in a terminal. It should start and wait silently; an error here is the same error Desktop saw. Common ones: the binary path in the config is wrong, or `--root` is missing.

## One-click alternative

The releases page also has `csvql-<platform>.mcpb`, a Claude Desktop extension bundle. Settings, Extensions, drop the file in; it asks for the data folder on install and sets `--root` for you.

## Excel files

`FROM 'sales.xlsx'` queries the first sheet. Dates come out as `YYYY-MM-DD`, formulas as their saved values. The sheet is unpacked in memory first, so a 9 MB workbook costs about 230 MB while the query runs. Other sheets, `.xls` and `.xlsb` are not supported.
