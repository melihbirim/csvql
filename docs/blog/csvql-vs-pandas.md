---
layout: post
title: "csvql vs pandas: Querying a CSV Without Loading It First"
description: "pandas needs to load the whole file into a DataFrame before you can ask it anything. csvql doesn't. On the same NYC taxi queries, that difference is 15 to 19x on speed and about 4x on memory, even before counting the load step pandas can't skip."
date: 2026-08-05
---

pandas is the default tool most people reach for to look at a CSV in Python. It's also built around an assumption that doesn't hold once a file gets big: that you can afford to load the whole thing into memory before asking it anything.

## The setup

Same dataset as the [DuckDB benchmark](csvql-vs-duckdb-nyc-taxi.html): a 425 MB, 1 million row slice of the NYC taxi dataset. Same rule too: both sides do the same work, cold, every run. pandas has no "query the file in place" mode, so its side is `pd.read_csv()` followed by the pandas-equivalent operation. That's not a handicap, that's how you use pandas.

To keep it fair to pandas, I passed `usecols` with exactly the columns each query needs, the way anyone optimizing pandas code actually would. Four queries, best of 3 runs each:

| Query | csvql | pandas | Speedup |
| ----- | ----- | ------ | ------- |
| COUNT(*) GROUP BY cab_type | **0.079s** | 1.186s | **15.0x** |
| AVG(total_amount) GROUP BY passenger_count | **0.062s** | 1.178s | **19.1x** |
| COUNT(*) WHERE trip_distance > 5 | **0.059s** | 1.138s | **19.4x** |
| Top 3 passenger_count by AVG(tip_amount) | **0.063s** | 1.175s | **18.6x** |

Both sides agree on the actual numbers, this isn't a case of one engine cutting corners: `cab_type` groups came back `green: 32447, yellow: 967553` identically on both.

## Where the time actually goes

pandas' per-query time barely moves between these four queries, 1.14 to 1.19 seconds each. That's not coincidence, it's `pd.read_csv()` itself dominating. The actual `groupby` or filter afterward is fast, DataFrames are good at that part. The cost is parsing 425 MB of CSV text into a DataFrame before any of that can start, and pandas pays that cost fresh on every single call because there's no persistent structure sitting between your query and the file.

csvql pays a version of that same parsing cost, but it's an order of magnitude cheaper, because it never builds a DataFrame. It scans the raw bytes, applies the query while scanning, and emits only the result rows. No intermediate object graph, no dtype inference across 51 columns you're not using, no Python-object overhead per cell.

## Memory tells the same story, worse

Peak memory on the `cab_type` query, with `usecols` narrowing pandas to just what it needs:

```
csvql    29 MB
pandas  125 MB
```

About 4.3x less. But that's the *generous* case. Drop `usecols` and just call `pd.read_csv(file)`, which is what most pandas code actually looks like day to day, and loading this file alone takes 3.5 seconds and peaks at roughly 970 MB, before you've asked a single question about the data. That's not a query cost, that's the price of admission.

## Why this is the whole design, not an optimization

csvql doesn't have an "efficient mode" you opt into. There's no DataFrame construction step to skip, because there's no DataFrame. Every query reads the CSV directly off disk, decides what it needs while reading, and discards the rest. For a one-off question, this is the difference between an answer in under a tenth of a second and a multi-second load you pay before you can even start.

pandas earns its dominance for a different job: once the data is loaded, transforming it, joining it against other in-memory structures, feeding it into a model, is exactly what DataFrames are for. If that's the job, load it once and reuse it. But "I have a CSV and one question about it" is a much more common task than that framing gives it credit for, and paying a multi-second, near-gigabyte tax for a single `groupby` is the wrong trade for it.

## Before you even get to run a query

There's a cost before any of the numbers above: getting pandas installed in the first place. `pip install pandas` pulls in numpy and python-dateutil as required dependencies, not optional ones. A clean install in a fresh virtualenv took about 18 seconds on a normal connection, and the installed footprint is around 106 MB (pandas plus numpy alone). That's before you've written a single line of your own code.

csvql is a single static binary with zero runtime dependencies, about 1.2 MB. `brew install melihbirim/csvql/csvql` and it's done, no interpreter, no package resolver, nothing else to pull in.

None of this matters if pandas is already sitting in your environment for other reasons, it usually is. But for a throwaway script, a CI job, or a container image where every dependency is something else to build, patch, and audit, that gap compounds. A 1.2 MB binary with nothing behind it is a fundamentally smaller thing to trust than 106 MB of C-extension-backed Python packages.

## Reproduce it

```bash
git clone https://github.com/melihbirim/csvql
cd csvql && zig build -Doptimize=ReleaseFast
pip install pandas
./bench/bench_taxi.sh --sample   # gets the sample dataset
./bench/bench_pandas.py
```

Swap in your own CSV and your own `usecols`. The shape of the result won't change much: pandas' cost is dominated by the load, csvql's isn't, because it doesn't have one.
