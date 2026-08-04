---
layout: post
title: "When Does Converting a CSV to Parquet Actually Pay Off?"
description: "Converting to Parquet makes every later query faster, but the conversion itself isn't free. Measured the real breakeven point: it takes 20 to 50 queries against the same file before that upfront cost pays for itself."
date: 2026-08-06
---

Most advice about querying big CSVs eventually says the same thing: convert to Parquet first, it's smaller and faster. That's true. It's also incomplete, because the conversion step is never free, and nobody puts a number on it.

## The setup

Same NYC taxi dataset as the [DuckDB benchmark](csvql-vs-duckdb-nyc-taxi.html), two sizes: the 425 MB / 1M-row sample, and a real 8 GB / 20M-row file (one of DuckDB's own dataset chunks, not synthetic data). Three things measured per file: csvql querying the raw CSV directly (its only mode, no conversion step exists), DuckDB querying an already-converted Parquet file ("warm"), and the one-time CSV→Parquet conversion cost via `DuckDB COPY ... TO ... FORMAT PARQUET`.

Four queries, same ones from the earlier benchmarks: `COUNT(*) GROUP BY cab_type`, `AVG(total_amount) GROUP BY passenger_count`, `COUNT(*) WHERE trip_distance > 5`, and a top-3 `GROUP BY` with `ORDER BY`.

## The numbers

**425 MB file:**

| | csvql | Parquet (warm) | Conversion cost |
| --- | --- | --- | --- |
| per query | ~0.07-0.10s | ~0.03s | **2.2s**, → 113 MB Parquet (3.6x smaller) |

**8 GB file:**

| | csvql | Parquet (warm) | Conversion cost |
| --- | --- | --- | --- |
| per query | ~1.6-1.9s | ~0.06-0.08s | **34.2s**, → 2.0 GB Parquet (4x smaller) |

Parquet-warm is genuinely fast, faster than csvql once it's converted, at both sizes. Columnar storage plus compression is a real advantage and it's not close. But "once it's converted" is doing a lot of work in that sentence.

## The number nobody puts in the pitch

Divide conversion cost by the per-query time you save, and you get how many times you need to query the *same* Parquet file before converting was worth it:

- 425 MB: 2.2s ÷ ~0.05s saved per query ≈ **~50 queries** to break even
- 8 GB: 34.2s ÷ ~1.6s saved per query ≈ **~20 queries** to break even

Below that many queries, you'd have been faster overall just querying the CSV directly and skipping conversion entirely. Above it, converting once and reusing the Parquet file wins.

This isn't a knock on Parquet, it's a warehouse format, it's supposed to be queried thousands of times by many people over its lifetime, and at that scale the conversion cost rounds to zero. The problem is applying that same instinct to a one-off question about a file you just received. "Just convert it to Parquet" is good advice for a table that lives in a data warehouse. It's a bad trade for a CSV someone hands you and a couple of questions you want answered right now.

## What about pandas?

pandas can read Parquet too (via `pyarrow`, itself another dependency beyond pandas and numpy). Same four queries, same two files, now with pandas reading CSV directly versus pandas reading the already-converted Parquet file:

**425 MB file, per query:**

| | csvql (CSV) | pandas (CSV, tuned) | pandas (Parquet) | DuckDB (Parquet) |
| --- | --- | --- | --- | --- |
| range | ~0.07-0.10s | ~1.69-1.75s | ~0.01-0.03s | ~0.03s |

**8 GB file, per query:**

| | csvql (CSV) | pandas (CSV, tuned) | pandas (Parquet) | DuckDB (Parquet) |
| --- | --- | --- | --- | --- |
| range | ~1.6-1.9s | ~35.4-36.3s | ~0.27-0.92s | ~0.06-0.08s |

Two things stand out. First, switching pandas from CSV to Parquet is a bigger win than anything else in this post, at 8 GB it goes from 35+ seconds a query to under a second, because pandas' CSV parser is the actual bottleneck, not pandas itself. Second, even reading from the same Parquet file, DuckDB still beats pandas by roughly 5 to 10x at the larger size. Parquet's columnar layout helps both, but pandas still has to materialize a full DataFrame from it, DuckDB's query engine doesn't.

None of that changes the conversion math above. Whichever tool ends up reading the Parquet file, something still has to produce it first, and that 2.2s or 34.2s conversion cost is paid once regardless of whether pandas or DuckDB is on the other end.

Worth noting for completeness: getting the naive `pd.read_csv(file)` number (no `usecols`, all 51 columns) at 8 GB wasn't practical to measure cleanly, it pushed this machine into heavy swapping (28+ GB of 30 GB swap in use) rather than finishing in reasonable time. That's itself a real data point: reading a wide CSV's every column into pandas at multi-gigabyte scale can exhaust available memory outright, not just run slow. The tuned (`usecols`) numbers above are the ones that actually completed.

## Where this actually matters: agents, not dashboards

csvql's stated purpose is letting an AI agent query a CSV it just encountered, not building a warehouse. That workload looks nothing like a dashboard hitting the same table for months. It looks like: here's a file, ask 3 to 10 questions about it, move on to the next file. That's comfortably below the breakeven point at every size measured here. Paying a 2 to 34 second (or far more, on a real multi-gigabyte file) tax before the first question can even be asked, when there may only be five questions total, is exactly the failure mode zero-ingest querying is built to avoid.

This is the same argument as the [token economics post](token-economics-of-querying-a-csv.html), from a different angle. That post was about the cost of *reading* a file into an LLM's context, this one is about the cost of *preparing* a file before you can query it at all. Both say the same thing: for low-repeat, ad-hoc access, upfront cost is the enemy, not per-query speed. A format that's faster per query but demands payment up front loses to a slower-per-query approach that demands nothing up front, right up until you cross the point where you're asking it questions all day.

## Reproduce it

```bash
git clone https://github.com/melihbirim/csvql
cd csvql && zig build -Doptimize=ReleaseFast
./bench/bench_taxi.sh --sample     # gets the 425 MB sample
./bench/bench_parquet.sh
```

Point it at your own CSV to get your own breakeven number: `./bench/bench_parquet.sh /path/to/your.csv`. The conversion cost scales with your file size and column count, the query savings scale with how selective your queries are, so the crossover point will move. The question to ask isn't "which is faster," it's "how many times am I actually going to query this file" — and now there's a number to compare it against instead of a guess.
