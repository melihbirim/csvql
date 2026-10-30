---
layout: post
title: "Privacy"
description: "csvql runs locally, reads only the files you name, sends nothing anywhere and keeps no data."
date: 2026-10-30
permalink: /privacy/
---

csvql, its Python and Node packages, its MCP server and its Claude plugin run entirely on your machine.

- **What it reads.** Only the CSV or TSV files you or your agent name in a query, or list with `csv_list`. With `--root`, only files inside the directories you allow. Those files may contain personal data; csvql reads them and nothing else.
- **What it writes.** Nothing to your data. csvql is read-only. It writes query results to standard output, to a file only when you pass `-o`, and to an audit log only when you pass `--audit`.
- **What it sends.** Nothing. csvql makes no network connections, has no telemetry, no analytics and no accounts. Query results go only to the program or agent that ran the query.
- **What is retained.** Nothing. There is no csvql service, so no data is received or stored by the author.

Installing csvql through Homebrew, pip, npm or GitHub Releases is handled by those services under their own privacy policies.

Questions: open an issue at [github.com/melihbirim/csvql](https://github.com/melihbirim/csvql/issues).
