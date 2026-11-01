---
layout: post
title: "csvql 3.0: Safe by Default for Agents, Private Columns, and Arithmetic Everywhere"
description: "csvql 3.0 confines an agent to its folder without any flag, adds --deny-columns so names and emails never reach the model, and supports arithmetic in SELECT, WHERE and over aggregates. It also fixes four queries that used to return a wrong answer without an error. Here is what changed and how to upgrade."
date: 2026-11-01
---

The idea behind csvql has not changed: give an AI agent SQL over your CSV files, not your computer. Version 3.0 makes that true without configuration, adds a way to keep personal data out of the model entirely, and fills the SQL gaps agents hit most often. It is a major version because one default changed. That change comes first.

## The breaking change: agents stay in their folder

Until now, `csvql --mcp` could read any file the process user could read unless you passed `--root`. In 3.0, `--mcp` without `--root` only opens files under the directory it was started in. Paths outside it, `../` tricks and symlinks that point outside all fail with `PathOutsideAllowedRoot`.

To choose the folders yourself, pass `--root /data` as before. To get the old behaviour back, pass `--root /`, which also had a bug in 2.x: it rejected every path. That is fixed.

Set `--root` explicitly in your MCP config anyway. The directory a server starts in depends on how the client launches it, and an explicit root does not.

## Private columns stay private

`--deny-columns email,ssn` refuses any query that reads those columns, wherever they appear: the select list, WHERE, ORDER BY, GROUP BY, a function, a subquery, a join condition, any capitalisation, any quoting style. `SELECT *` and the schema tool simply leave them out.

The point is what the agent can still do. With names and emails denied on an HR export, it can answer "what is the average salary per department" and never see who earns what. The values never enter its context, so they never reach the model provider.

One honest limit: a denied column cannot leak through a query, but a small group can. If a department has one person, its average salary is that person's salary. A `--min-group-size` option to blank out small groups is the next piece of this ([#275](https://github.com/melihbirim/csvql/issues/275)).

## Arithmetic everywhere

Agents write arithmetic constantly, and csvql used to refuse it. Now it works in three places:

```sql
SELECT region, SUM(price * qty) AS revenue FROM 'orders.csv' GROUP BY region
SELECT order_id FROM 'orders.csv' WHERE price * qty > 100
SELECT dept, SUM(salary) / COUNT(*), 100.0 * COUNT(*) / 1250 FROM 'staff.csv' GROUP BY dept
```

An empty or non numeric value, or a division by zero, gives an empty result rather than a made up number, and SUM and AVG skip that row. The new arithmetic matched DuckDB on a million row file.

## Column names with spaces

Spreadsheet exports are full of headers like `First Name` and `Total Sales`, and until now csvql could not refer to them at all. In 3.0 they work as `"First Name"`, backticks or `[First Name]`. A double quoted string after a comparison is still a value, so `WHERE city = "New York"` keeps working.

## Four queries that used to be wrong without saying so

The fixes I care most about in this release are the ones where csvql returned an answer, exit code 0, and the answer was wrong. An agent has no reason to doubt that.

1. `SELECT name FROM 'f.csv' WHRE city = 'Cork'` treated the misspelled clause as a table alias and returned every row. It is now an error.
2. `WHERE price > qty * 5` compared price against the text `qty * 5` and matched nothing. It is now arithmetic.
3. `WHERE id = 1;` compared against the value `1;` and matched nothing. A trailing semicolon now ends the statement.
4. `col IN (SELECT ...)` was never resolved over MCP or in the Python and Node bindings, so it matched nothing. It now runs, inside the same sandbox as the outer query.

Most of these came out of a security and leak test I ran against the agent sandbox before this release: about fifty attempts to read outside the folder, get at denied columns, write a file, or hang the server. Nothing was read or leaked. Two things could hang it: reading stdin over MCP, which is the protocol stream itself, and opening a named pipe. Both are refused now. The test also showed that one of our own claims was wrong. The README said the result cap stops an agent from pulling a table out one call at a time. It does not: the cap is per call, and an agent can page with OFFSET. The text now says so and points at `--deny-columns` for data an agent must never see.

## Smaller changes

`csv_list` now searches subfolders, after a real Claude session listed the working directory, saw nothing because the data was in `data/`, and told the user the file did not exist. Write statements such as INSERT, DELETE and COPY now say csvql is read only instead of failing with FileNotFound.

## Upgrading

```bash
brew upgrade csvql            # or
pip install -U csvql-query    # or
npm install -g csvql-query
```

If you run csvql as an MCP server, check that your config passes `--root` with the folders you want the agent to see. If it does, nothing else changes. The Claude Code plugin is at 1.2.0 and its skill knows about the new SQL. The full list of changes is in the [release notes](https://github.com/melihbirim/csvql/releases/tag/v3.0.0).
