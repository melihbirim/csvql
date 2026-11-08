---
layout: post
title: "csvql vs Letting the Agent Read the CSV File Itself"
description: "An agent with a file tool can open a CSV and answer questions about it. For small files that works. Here is where it stops working, measured in tokens, memory and what the agent can see, and what changes when the agent runs SQL on the file instead."
date: 2026-11-08
---

The simplest way to let Claude answer a question about a CSV is to let it read the file. Claude Code has a file tool, Claude Desktop can take an upload, and for a 200 row export that is fine. This page is about what happens after that, and what changes when the agent runs a query against the file instead of reading it. csvql (published as `csvql-query` on npm and PyPI) is the query engine used for the numbers.

## It stops fitting before it gets slow

Tokens for the NYC taxi dataset, measured with `tiktoken`, for five questions:

| File | Rows | Pasting the file | Five questions via csvql | Fits a 200K context? |
| --- | --- | --- | --- | --- |
| 1 MB | 2,400 | 559,282 | 536 | No |
| 10 MB | 24,000 | 5,576,783 | 549 | No |
| 100 MB | 240,000 | 55,118,633 | 565 | No |
| 417 MB | 1,000,000 | 230,396,519 | 562 | No |

A 1 MB CSV is already nearly three times a 200K context window. The query column does not move with file size because the agent never sees the file: it sends SQL, gets back a handful of rows. Full write-up with the method: [The Token Economics of Querying a CSV](token-economics-of-querying-a-csv.html).

Agents cope with this by reading the head of the file. That gives the right column names and a wrong idea of the data: `SELECT COUNT(*)` on the first 50 lines is 50.

## What the agent can see

With a file tool, the agent reads whatever the process can read. The CSV with the `email` column, the one next to it, the `.env` two folders up. Nothing in "read the file" distinguishes the data you meant from the data you did not.

With csvql as an MCP server:

- It only opens files under `--root` (or the folder it started in). `../` and absolute paths outside fail with `PathOutsideAllowedRoot`.
- `--deny-columns email,ssn` refuses any query that reads those columns and drops them from `SELECT *`. The agent can still compute `AVG(salary) GROUP BY dept`.
- Each `csv_query` call returns at most about 100 rows / 12 KB. A `SELECT *` on a million rows comes back as the first page, not the file.
- `--audit queries.jsonl` records every query with a timestamp.

None of that exists for a file read. You cannot deny a column of a file.

## What the agent can do to the file

A file tool usually comes with a write tool. An agent that reads `sales.csv` can also overwrite it, and the model's own correction ("let me fix that row") is the common failure, not malice.

csvql's SQL has no `INSERT`, `UPDATE`, `DELETE`, `DROP` or `COPY`. The binary has no network code. There is nothing to configure off because there is nothing there.

## Correctness

The agent reading a file does arithmetic in its head. On a few hundred rows that is often right. Summing a column of 240,000 numbers by reading them is not something a language model does, and it will produce a plausible figure anyway.

csvql's answers are checked against DuckDB on every commit, 128 queries covering aggregates, joins, NULL handling, ordering and LIKE. The agent gets the same number DuckDB would.

## Memory and speed

Reading a file into context costs nothing on the machine and everything in the model. Running a query costs the machine something:

| `GROUP BY` on raw CSV | csvql | DuckDB | polars |
| --- | --- | --- | --- |
| 0.72 GB, peak memory | 54 MB | 221 MB | 1,137 MB |
| 8 GB, peak memory | 74 MB | 367 MB | 1,754 MB |

Wall time is in the same band as DuckDB. A laptop or a small VM next to the data handles files that no context window will.

## When reading the file is still right

- The file is small, under a few hundred rows, and you want the agent to look at the actual values rather than aggregates.
- The question is about the shape of the file, not its contents: is this really CSV, what is the delimiter, is the header on line 3. (`csvql --schema file.csv` covers most of this too.)
- You have no way to run a binary where the data lives.

For anything else, give the agent a query tool and keep the file where it is.

## Setup

```bash
brew install melihbirim/csvql/csvql     # or a binary from the releases page
cd ~/data && csvql install              # registers in Claude Code and Claude Desktop
```

Then ask: "How many trips per cab type in trips.csv?" The agent runs `SELECT cab_type, COUNT(*) FROM 'trips.csv' GROUP BY cab_type` and gets two rows back.
