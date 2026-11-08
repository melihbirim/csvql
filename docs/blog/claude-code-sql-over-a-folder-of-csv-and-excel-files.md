---
layout: post
title: "Claude Code: SQL Over a Folder of CSV and Excel Files"
description: "Give Claude Code a read-only SQL tool over the CSV, TSV and .xlsx files in a project folder. One command to register it, the queries it will write, joins across files, and the limits to know before the agent hits them."
date: 2026-11-08
---

Claude Code can already `cat` a CSV. For a file with more than a few hundred rows that fills the context with data and still does not let it count anything. The fix is a tool that runs SQL on the files and returns rows. This page sets up csvql (published as `csvql-query` on npm and PyPI) in Claude Code and shows what the agent does with it.

## Register it

```bash
brew install melihbirim/csvql/csvql     # or a binary from github.com/melihbirim/csvql/releases
cd ~/projects/q3-analysis && csvql install
```

`csvql install` runs `claude mcp add` for you. Claude Code starts MCP servers in the project folder, so csvql is confined to that folder by default: it only opens files under the directory it was started in. If you want a different or wider folder, say so:

```bash
claude mcp add csvql -- csvql --mcp --root ~/data/exports
```

Without MCP, there is also a Claude Code skill that teaches the agent to use the `csvql` CLI whenever a CSV comes up in a shell session:

```bash
claude plugin marketplace add melihbirim/csvql
claude plugin install csvql@csvql
```

The MCP route is the one with the sandbox and the output cap. The skill route is for when the agent already has a shell and you want it to stop writing pandas scripts for one-off questions.

## What the agent does with it

Ask: "Which customers in customers.csv placed no orders in orders.csv?"

Claude calls `csv_schema` on both files to see the columns, then:

```sql
SELECT c.id, c.name
FROM 'customers.csv' c
LEFT JOIN 'orders.csv' o ON c.id = o.customer_id
WHERE o.id IS NULL
```

Ask: "Monthly order count from the Excel export."

```sql
SELECT STRFTIME('%Y-%m', order_date) AS month, COUNT(*)
FROM 'orders.xlsx'
GROUP BY month
ORDER BY 1
```

`.xlsx` reads the first sheet; dates come out as `YYYY-MM-DD`. Ask: "Top five regions by revenue, excluding returns."

```sql
SELECT region, SUM(amount) AS revenue
FROM 'sales.csv'
WHERE status != 'returned'
GROUP BY region
ORDER BY revenue DESC
LIMIT 5
```

Each `csv_query` returns at most about 100 rows / 12 KB as JSON. The agent pages with `LIMIT` and `OFFSET` when it needs more, which it rarely does: the point is aggregates, not dumps.

## Limits the agent will hit, and what it sees

- **No window functions, no `UNION`, no `RIGHT`/`FULL`/`CROSS JOIN`.** These error with a clear message. The agent rewrites or tells you. It does not get a wrong answer.
- **Only `col IN (SELECT ...)` subqueries.** No correlated subqueries, no subquery in `FROM`.
- **Scalar functions in `WHERE`** are limited to `DATEDIFF`, `DATEADD` and `EXTRACT`. `WHERE UPPER(name) = 'X'` fails; `WHERE name = 'X'` or `ILIKE` works.
- **Empty fields are NULL.** `WHERE age != 30` and `NOT age > 30` both exclude rows with an empty `age`, the same as DuckDB. `IS NULL` finds them.
- **Excel is unpacked in memory.** A 9 MB, 300k row workbook costs about 230 MB during the query. Large sheets are better exported to CSV once.
- **Column names with spaces** work with double quotes: `SELECT "First Name" FROM 'people.csv'`.

Everything supported is in the [SQL reference](https://github.com/melihbirim/csvql/blob/main/SQL_REFERENCE.md). Results are checked against DuckDB on every commit.

## Keeping the agent in its lane

The defaults already give you: read-only (there is no write statement in the SQL), no network (no network code in the binary), confined to the folder. Two more flags for shared or sensitive data:

```bash
claude mcp add csvql -- csvql --mcp --root ~/data --deny-columns email,phone --audit ~/csvql-audit.jsonl
```

`--deny-columns` refuses any query that reads those columns and drops them from `SELECT *`. `--audit` appends one JSON line per query with a timestamp, for later review.

## Checking it

In Claude Code, `/mcp` lists the connected servers; csvql should show three tools. Ask it to read a file outside the project folder: the tool answers `PathOutsideAllowedRoot`. Ask it to delete a row: there is no statement for that, and it will say so.
