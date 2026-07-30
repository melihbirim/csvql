---
layout: post
title: "The Token Economics of Querying a CSV Instead of Pasting It"
description: "Pasting an 8 GB CSV into an LLM costs 230 million tokens and still doesn't fit. Querying it costs about 560 tokens, flat, regardless of file size. The actual numbers, measured with tiktoken."
date: 2026-07-30
---

An AI agent that needs to answer a question about a CSV file has exactly two ways to see the data: paste the file into its context window, or ask a tool to run a query and hand back only the answer. Those two paths do not scale the same way, and the gap between them is bigger than it sounds.

## The measurement

I used [bench/bench_tokens.py](https://github.com/melihbirim/csvql/blob/main/bench/bench_tokens.py) against the NYC taxi dataset DuckDB publishes for its own benchmarks. Five realistic questions, tokenized with `tiktoken` (`cl100k_base`, the real GPT tokenizer, not a character estimate), at four file sizes:

| Size | Rows | Paste (tokens) | Query via csvql (tokens) | Ratio | Fits 200K context? |
| ---- | ---- | --------------- | -------------------------- | ----- | ------------------- |
| 1 MB | 2,400 | 559,282 | 536 | 1,043x | no |
| 10 MB | 24,000 | 5,576,783 | 549 | 10,158x | no |
| 100 MB | 240,000 | 55,118,633 | 565 | 97,555x | no |
| full (417 MB) | 1,000,000 | 230,396,519 | 562 | 409,958x | no |

"Paste" is the raw file content tokenized as-is. "Query" is five separate questions, each sent as SQL plus the question text, counting both what the model sends and what it gets back, using [csvql](https://github.com/melihbirim/csvql)'s MCP interface. The one-time MCP tool schema (159 tokens, paid once per conversation, not per query) is included but barely moves the number.

## The part that matters: the paste column doesn't even fit

Every row in that "fits 200K context?" column says no. Not "expensive", not "slow": it does not fit. A 1 MB slice of this dataset already blows past a 200K token context window by nearly 3x. The 417 MB file is 230 million tokens, over a thousand times the budget. There's no discount, no summarization trick, no compression that gets a spreadsheet like this into a conversation. Pasting isn't a worse option here. It's not an option.

## Why the query cost is flat

The query column barely moves between 1 MB and 417 MB: 536 tokens at the small end, 562 at the full file. That's not a rounding coincidence, it's structural. The agent never sees the file. It sends a question and a SQL query, and gets back a handful of result rows: a `GROUP BY cab_type` returns four rows no matter whether the table underneath has 2,400 rows or a billion. File size determines how long csvql takes to scan the file on disk. It has nothing to do with how many tokens the model has to read.

This is the actual shape of the tradeoff: paste cost grows linearly with the file and hits a wall almost immediately. Query cost is flat and never hits anything.

## Why this matters more than raw query speed

Most of the attention on csvql's numbers goes to how fast it scans a file compared to DuckDB. That's a real number, but it is not the number that determines whether an agent workflow works at all. An agent that's 3x slower at reading a file is annoying. An agent that can't read the file because it's 230 million tokens over budget is broken. Token economics is a threshold problem before it's a speed problem: you have to get under the wall before "how fast" is even a question worth asking.

This is also why csvql treats "query in place" as the whole architecture, not an optimization. There's no ingest step, no native store to build, no copy of the file to manage. The agent asks a question, csvql reads the raw CSV off disk, and only the answer travels back through the token budget.

## Reproduce it

```bash
git clone https://github.com/melihbirim/csvql
cd csvql && zig build -Doptimize=ReleaseFast
pip install tiktoken
./bench/bench_taxi.sh --sample   # gets the sample dataset
./bench/bench_tokens.py
```

Swap in your own CSV and your own questions. The shape of the result won't change: paste scales with the file, query doesn't.
