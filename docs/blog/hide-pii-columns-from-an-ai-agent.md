---
layout: post
title: "Hide PII Columns From an AI Agent With --deny-columns"
description: "An HR or customer export has columns the agent needs (department, salary, region) and columns it must never see (name, email, national id). csvql's --deny-columns refuses any query that reads the second kind, and keeps the first kind queryable. How it works, what it blocks, and what it does not."
date: 2026-11-08
---

The usual way to let an agent analyse a customer or HR export is to make a copy with the sensitive columns removed. That copy drifts, lives somewhere, and someone has to remember to make it. csvql (published as `csvql-query` on npm and PyPI) does it at query time instead: name the columns the agent must never read, and every query that touches them is refused.

## One flag

```bash
csvql --mcp --root ~/hr-exports --deny-columns name,email,national_id,phone
```

In a Claude Desktop or Claude Code MCP config:

```json
{ "mcpServers": { "csvql": {
  "command": "/opt/homebrew/bin/csvql",
  "args": ["--mcp", "--root", "/Users/you/hr-exports", "--deny-columns", "name,email,national_id,phone"]
} } }
```

Column names match the CSV header, case-insensitive. The same list applies to every file under `--root`; a file without those columns is unaffected.

## What the agent can still do

```sql
SELECT department, COUNT(*), AVG(salary), MAX(hire_date)
FROM 'employees.csv'
GROUP BY department
ORDER BY department
```

Works. The denied columns are not involved. The agent gets the aggregates it was asked for.

```sql
SELECT * FROM 'employees.csv' LIMIT 5
```

Works, and the result has no `name`, `email`, `national_id` or `phone` column. `SELECT *` means "every column you are allowed to see".

## What is refused

Any query that reads a denied column, wherever it appears:

```sql
SELECT email FROM 'employees.csv'                              -- in the SELECT list
SELECT department FROM 'employees.csv' WHERE email LIKE '%@x'  -- in WHERE
SELECT department FROM 'employees.csv' ORDER BY name           -- in ORDER BY
SELECT id FROM 'orders.csv' WHERE id IN (SELECT id FROM 'employees.csv' WHERE phone != '')  -- in a subquery
SELECT e.department FROM 'employees.csv' e JOIN 'contacts.csv' c ON e.email = c.email      -- as a join key
```

Each one fails before any row is read. The agent gets an error, not a partial result, so it cannot work around the list with `LIKE` patterns, `GROUP BY email`, or a join through the column. There is no `SELECT *` leak either, since the denied columns are dropped from the projection.

## What it does not do

Be clear about the boundary, because an agent will probe it:

- **It hides columns, not values.** If `notes` contains an email address in free text, `--deny-columns email` does not touch it. Deny `notes` too, or do not expose that file.
- **It does not detect PII.** You name the columns. A `--deny-pii` flag that guesses from header names and sample values is planned ([#277](https://github.com/melihbirim/csvql/issues/277)), and it will still be a guess.
- **Aggregates over small groups can identify people.** `AVG(salary) GROUP BY department` with one person in a department is that person's salary. A `--min-group-size` flag that blanks such groups is planned ([#275](https://github.com/melihbirim/csvql/issues/275)). Until then, think about group sizes in what you expose.
- **It is per server, not per file.** One list for everything under `--root`. Different files with different rules means two server entries with two roots.
- **It assumes the agent reaches the data only through csvql.** If the same agent also has a file tool or a shell on that folder, it can read the raw CSV. The deny list protects the MCP path. Keep the folder out of other tools' reach.

## Put the three flags together

```bash
csvql --mcp --root ~/hr-exports --deny-columns name,email,national_id,phone --audit ~/csvql-audit.jsonl
```

- `--root`: which folder exists, as far as the agent is concerned.
- `--deny-columns`: which columns inside it do not exist.
- `--audit`: one JSON line per query, with a timestamp, so you can read later what was asked.

Add the fact that the SQL has no write statement and the binary has no network code, and the exposure is: aggregate questions over the allowed columns of the files in one folder, at most about 100 rows per answer, all logged. That is a sentence you can put in a data processing record.

## Check it

Start the server by hand and watch it refuse:

```bash
csvql --root ~/hr-exports --deny-columns email "SELECT email FROM 'employees.csv' LIMIT 1"
# execution error: error.DeniedColumn
csvql --root ~/hr-exports --deny-columns email "SELECT department, COUNT(*) FROM 'employees.csv' GROUP BY department"
# department,COUNT(*)
# ...
```

The same flags work on the CLI, so you can test a deny list before you hand it to an agent.
