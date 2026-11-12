---
layout: post
title: "csvql vs Polars Nanook: An Agent at the Edge or an Agent on a Platform"
description: "Polars Cloud is now Polars Nanook, with a built-in MCP server on every plan. What that changes for agent access to data, what it does not, and where a single fenced binary next to the files still fits."
date: 2026-11-12
---

Polars renamed Polars Cloud to Polars Nanook on 5 November 2026. It is a managed compute platform for cloud, on-premise and, on the Enterprise plan, air-gapped deployment. SQL is a first-class API next to DataFrames, and queries can run distributed and out of core.

Nanook already ships an MCP server alongside query profiling, on every plan including the free one. Polars plans to unify writing queries, starting compute and finding data behind it "in the following month(s)". That puts Polars directly in the space csvql works in: giving an AI agent access to data.

## Where the agent runs, and what it needs first

Nanook is a platform: an account, a cluster, and, on AWS, compute that takes about 45 seconds to boot. Polars says that can feel slow for agentic work and plans warm instances that start in about a second.

csvql is one binary next to the files. It needs no account, no cluster and no Python, and it has no network code. It starts instantly.

## Free Polars and DuckDB are local too

"No account, no cluster" also describes `pip install polars` and DuckDB. The difference is what the agent gets. They give an agent a full engine. csvql gives it a fenced one:

- **Reads only.** The SQL has no `INSERT`, `UPDATE`, `DELETE`, `DROP` or `COPY`.
- **Files under `--root` only.** `csvql --mcp` is confined to its working directory by default and refuses to start in `/`. `../` and symlink escapes fail with `PathOutsideAllowedRoot`.
- **Private fields stay out.** `--deny-columns email,ssn` refuses any query that reads those columns, and `SELECT *` leaves them out. This one needs the flag.
- **Bounded output.** Results are capped at about 100 rows / 12 KB per call. The cap is per call, so an agent can page through a table with `OFFSET`; data it must never see belongs in `--deny-columns`.

## The line

So the line is not local against distributed. It is an agent reading raw files at the edge, with nothing to provision and confined by default, against an agent working through a managed platform.

Teams already on Polars will likely use Nanook's MCP. csvql's case is the machine with the files and no platform: a laptop, an SSH box (`ssh host csvql --mcp --root /data`), or an offline server where nobody will buy an Enterprise contract or run a cluster.

The boot time gap is temporary. The setup gap and the fence are not.

## What we have not checked

Polars' pricing page lists "query profiling and built-in MCP server" without saying what that MCP lets an agent do, whether it is read only, or whether it can hide columns. We will update this post when Polars documents it. Polars has published no benchmarks for the distributed engine or out-of-core mode, so this post makes no speed claims either way.

Sources: [Polars Cloud becomes Polars Nanook](https://pola.rs/posts/polars-cloud-becomes-polars-nanook/), [Polars pricing](https://pola.rs/pricing), [csvql README](https://github.com/melihbirim/csvql#what-an-agent-can-do-out-of-the-box), [csvql SECURITY.md](https://github.com/melihbirim/csvql/blob/main/SECURITY.md).
