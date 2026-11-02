---
layout: post
title: "Claude Desktop Starts MCP Servers in /, So the Working Directory Is Not a Sandbox"
description: "csvql 3.0 confined agents to the working directory by default. Then we checked where Claude Desktop actually starts MCP servers: the filesystem root. Here is how we found it, what csvql 3.0.1 does about it, and what it means for anyone writing an MCP server. Plus LEFT JOIN and Excel files."
date: 2026-11-02
---

csvql 3.0 shipped yesterday with a simple promise: `csvql --mcp` without `--root` only opens files under the directory it was started in. An agent stays in its folder without any flag.

That promise depends on one thing we had not checked: which directory the client starts the server in. For Claude Desktop, the answer is `/`.

## How we found it

The 3.0 notes already said to set `--root` explicitly, "since the starting directory depends on how the client launches the server." That sentence was a hedge, not a fact. So we looked at a live process instead of guessing.

With Claude Desktop running and csvql configured the way `csvql install` sets it up (`"args": ["--mcp"]`), we asked the operating system for the working directory of the server process:

```bash
for p in $(pgrep -f "csvql --mcp"); do lsof -a -p $p -d cwd -Fn | grep ^n; done
```

Every csvql process started by Claude Desktop printed `n/`. Desktop launches stdio MCP servers with the working directory set to the filesystem root. A csvql process started by another client, in a project folder, printed that folder.

So for Claude Desktop users, csvql 3.0's default root was the whole disk. An agent could read `~/.aws/credentials` through `csv_query`. Output is capped per call, and nothing could be written or sent anywhere by csvql, but the model could see the file. It is not a regression, since 2.x had no default root at all, but the claim did not hold where it mattered most.

## What 3.0.1 does

Three changes, all shipped today in [csvql 3.0.1](https://github.com/melihbirim/csvql/releases/tag/v3.0.1):

- **`csvql --mcp` refuses to start in the filesystem root without `--root`.** It exits with an error telling you to pass `--root <folder>`. A server that does not start is better than one that silently allows everything.
- **`csvql install` pins the folder.** The Claude Desktop entry it writes now includes `--root` set to the folder you run it from, or the one you pass with `--root`. Run it from your data folder.
- **The one click `.mcpb` bundle asks for a data folder** when you install it in Claude Desktop, and passes it as `--root`. Its manifest also had the server command in a place the MCPB spec does not define, which is now fixed.

If you configured Claude Desktop with a bare `--mcp`, csvql 3.0.1 will refuse to start until you add a folder:

```json
{ "mcpServers": { "csvql": { "command": "/usr/local/bin/csvql", "args": ["--mcp", "--root", "/Users/you/data"] } } }
```

## If you write an MCP server

Do not treat the working directory as a boundary. Claude Desktop starts stdio servers in `/`, other clients start them in a project folder, and an SSH launch starts them in a home directory. A default that means "this folder" means something different in each. Ask for the folder explicitly, refuse to run without one in the filesystem root, and check your assumption with `lsof` (or `/proc/<pid>/cwd` on Linux) against the real client.

## Also in 3.0.1

**LEFT JOIN.** `LEFT JOIN` and `LEFT OUTER JOIN` work, chained and mixed with inner joins. Rows with no match keep empty right side columns, so the classic "which customers have no orders" query works:

```sql
SELECT c.name FROM 'customers.csv' c LEFT JOIN 'orders.csv' o ON c.id = o.customer_id WHERE o.id IS NULL
```

The output is checked against DuckDB on a large file in CI.

**Excel files.** `FROM 'sales.xlsx'` queries the first sheet, anywhere a CSV works: `WHERE`, `GROUP BY`, `JOIN`, `--schema`, the MCP tools and `--deny-columns`. Dates come out as `YYYY-MM-DD`, formulas give their saved value. The sheet is unpacked in memory first, so a 9 MB workbook with 300k rows takes about 230 MB and 0.6 seconds. Other sheets, `.xls` and `.xlsb` are not supported yet.

## Upgrade

```bash
brew upgrade csvql        # or download a binary from the release page
cd ~/data && csvql install
```

Then restart Claude. The Python and Node.js libraries are `csvql-query` 3.0.1 on PyPI and npm. The full list is in the [3.0.1 release notes](https://github.com/melihbirim/csvql/releases/tag/v3.0.1).
