---
layout: post
title: "Read-Only SQL Access to CSV Files for AI Agents, Without Moving the Data"
description: "Agents are getting access to company data, and the usual two paths both move it: a database connection or an upload. The third path is to run a query engine next to the files and let the agent ask questions there. Here is what read-only means for csvql in checkable terms, how to set it up locally and over SSH, and what it does not protect you from."
date: 2026-10-19
---

An agent that can answer questions about your data needs to read it. Today that happens one of two ways. You hand it a database connection, which usually means a user with more rights than the question needs. Or you upload the file to the model, which means the file now lives somewhere else, and a 400 MB CSV does not fit in a context window anyway.

csvql is a third option. It runs `SELECT` on CSV files where they already sit, on your own box, and it ships as an [MCP](https://modelcontextprotocol.io/) server, so an agent can ask a question of a multi gigabyte file and get back a few rows. The file never leaves the machine. Below is what that guarantee is made of, in terms you can check against the source, and the two places where it is weaker than I would like.

## What read-only means here

Marketing says read-only about a lot of things. These are the specific properties, and how to verify each one yourself.

**The parser has no write statements.** There is no `INSERT`, `UPDATE`, `DELETE` or `DROP` in the grammar. `grep -i "insert\|update\|delete\|drop" src/parser.zig` hits two code comments and nothing else; the only place those words appear as SQL is the help text that lists them as unsupported. An agent cannot be talked into writing to a file through SQL because the engine has no code path for it.

**No network.** There is no `std.net`, `std.http` or socket call in the codebase. No telemetry, no update check, no API key. On Linux, `strace -f -e trace=network csvql "SELECT COUNT(*) FROM 'data.csv'"` shows an empty trace. On an air gapped server it runs the same as anywhere else.

**No unrequested disk writes.** csvql writes to disk in exactly two cases, `-o <file>` and `--audit <file>`, and both need the flag. Everything else goes to stdout. The one caveat is below under gaps.

**A directory boundary.** `--root /data` confines every file the process can open, for the CLI and for MCP. Paths are resolved with `realpath`, so `../../etc/passwd` and a symlink that points outside the tree both fail with `PathOutsideAllowedRoot`. Without `--root`, csvql reads whatever the OS user can read, which is the right default for a CLI and the wrong one for an agent. Always set it when something other than you is typing the SQL.

**A ceiling on what comes back.** MCP results are capped at about 100 rows or 12 KB per call. A careless `SELECT *` on a 20 million row file returns 100 rows and a note, not the file. This is a context window protection first, since one bad query would otherwise blow the conversation, but it also means an agent cannot pull a table out one call at a time without it being obvious in the audit log.

**An audit log.** `--audit queries.jsonl` appends one record per query with the timestamp and the SQL. Over SSH you also inherit the server's auth log, so you know who connected and what they asked.

**Memory stays flat.** An aggregate holds a running accumulator, not the dataset. `GROUP BY` on a 0.7 GB file peaks at 74 MB, and on an 8 GB file it is still 74 MB. This matters for the on prem story because it decides which box csvql can run on. A 2 GB VM next to the data is enough.

## Local setup

```bash
brew install melihbirim/csvql/csvql     # or a binary from the releases page
csvql install                            # registers csvql in Claude Code and Claude Desktop
```

`csvql install` runs `claude mcp add` if the Claude Code CLI is present and merges an `mcpServers.csvql` entry into the Claude Desktop config, keeping your other servers. Restart Claude afterwards. The manual form, which is also what VS Code wants under `servers` in `.vscode/mcp.json`:

```json
{ "mcpServers": { "csvql": { "command": "/usr/local/bin/csvql", "args": ["--mcp", "--root", "/data"] } } }
```

The agent gets three tools. `csv_list` to see what files exist, `csv_schema` for column names and a few sample rows, and `csv_query` for SQL against single quoted paths:

```sql
SELECT customer, SUM(revenue) AS total FROM 'sales.csv' GROUP BY customer ORDER BY total DESC LIMIT 10
```

## Remote and on prem, over SSH

The interesting case is data that is not on your laptop. Install csvql on the server, next to the files, and point the client's MCP config at SSH:

```jsonc
// client side MCP config; csvql runs on the remote server
{ "command": "ssh", "args": ["analyst@dataserver", "csvql", "--mcp", "--root", "/data"] }
```

MCP speaks JSON over stdio, and SSH carries stdio. That is the whole integration. No open port, no reverse proxy, no new credentials, no daemon to keep alive. You reuse the SSH keys, the bastion and the login records you already have. Only the SQL goes up and only the small result comes down.

Hardening checklist for that server, all of it ordinary Unix:

* Run csvql as a dedicated, unprivileged user.
* Mount the data read only for that user. `--root` keeps csvql inside the tree, the mount keeps everything else out even if csvql had a bug.
* Set `--root` to the data directory and nothing wider.
* Turn on `--audit` and ship the file to wherever your logs go.

## What it costs the agent

Five questions about a 417 MB taxi dataset cost about 640 tokens over MCP, tool schema included, and that number does not change with file size. Pasting the file would cost about 230 million tokens, which is a feasibility argument rather than a savings figure, since it does not fit in any context window. The honest comparison is an agent that already has a shell and runs `duckdb -csv`. That uses slightly fewer tokens than csvql over MCP, because MCP returns JSON. The case for csvql with agents is the boundary, the read-only execution, the output cap and the flat memory, not the token count. Full measurement in [the token economics post](token-economics-of-querying-a-csv.html) and `bench/bench_tokens.py`.

## What this does not protect you from

**An agent can read everything under `--root`.** The boundary is a directory, not a row or column policy. If the sales folder also holds the salaries file, the agent can query the salaries file. Put only what the agent should see under the root, or give it its own directory with hard links or copies.

**Prompt injection in the data.** A CSV cell can contain text like "ignore previous instructions". csvql returns cells verbatim because that is what a query engine does. The output cap limits how much of it reaches the model per call, which helps, but this is the agent's problem to solve and csvql does not claim to solve it.

**Two known gaps in the no disk writes claim.** Four internal operations need a scratch buffer while a query runs: `IN (SELECT ...)` subqueries, a `JOIN` followed by `GROUP BY`, and `--table` rendering. On Linux that buffer is `memfd_create()`, anonymous RAM that never touches a disk block. On macOS and Windows it is still a real, immediately deleted temp file in `$TMPDIR`. That is tracked in [#164](https://github.com/melihbirim/csvql/issues/164), not hidden. If your threat model includes forensic recovery of temp files on a Mac or Windows server, know this before you deploy there. Linux servers do not have the gap.

**It is not a database.** No `UNION`, no window functions, no outer joins. Scalar functions in `WHERE` are limited to `DATEDIFF`, `DATEADD` and `EXTRACT`. These error rather than return wrong data, and the full list with tracking issues is in [SQL_REFERENCE.md](https://github.com/melihbirim/csvql/blob/main/SQL_REFERENCE.md#known-limitations). If the agent needs full SQL, run DuckDB behind the same SSH boundary and accept that you now have to do the read-only work yourself.

## Why I think this is the right shape

The alternative designs I looked at all add a component that has to be secured. An HTTP MCP transport needs TLS, auth and a running process. A database needs a user, a grant and a network path. A file upload needs a place to put the file and a policy for deleting it. The SSH plus static binary design adds nothing that was not already there. The whole security review is the list above, and every line of it can be checked by reading about 50 lines of Zig or running one `strace`.

An HTTP transport is still planned, for the case where one csvql should serve a team as a shared service ([#60](https://github.com/melihbirim/csvql/issues/60)). When it ships it will sit behind a reverse proxy that does the TLS and the auth, and the properties above will not change.

Everything referenced here is in [SECURITY.md](https://github.com/melihbirim/csvql/blob/main/SECURITY.md) and the README. If you find a hole in any of the claims, the Security Advisories page on the repository is the place, and I will say so publicly once it is fixed, as I have before.
