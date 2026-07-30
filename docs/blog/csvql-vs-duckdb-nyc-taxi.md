---
layout: post
title: "I Benchmarked My CSV Engine Against DuckDB on Its Own Dataset. It Found Two Bugs in Mine."
description: "csvql queries raw CSV about 2.8x faster than DuckDB on an 8 GB file, using about 6x less memory and zero extra disk. The benchmark that proved it also surfaced two real bugs."
date: 2026-07-13
---

*csvql queries raw CSV ~2.8x faster than DuckDB on an 8 GB file, using ~6x less memory and zero extra disk — and the benchmark that proved it also surfaced two real bugs I'd never have found otherwise.*

---

## The setup: query the same raw CSV, on the same machine

DuckDB publishes [its own NYC-taxi CSV benchmark](https://duckdb.org/2024/10/16/driving-csv-performance-benchmarking-duckdb-with-the-nyc-taxi-dataset). That makes it the perfect yardstick: I can't be accused of misconfiguring the competitor when I'm running the competitor's own dataset with the competitor's own queries.

The dataset lives on DuckDB's own blob storage (`blobs.duckdb.org/data/nyc-taxi-dataset`). Each file is 20 million rows, 51 columns, ~8 GB uncompressed. The four queries are the canonical ["Billion Taxi Rides"](https://github.com/pdet/taxi-benchmark) aggregations — `GROUP BY` counts and averages over cab type, passenger count, year, and rounded trip distance.

**The one rule that makes or breaks a benchmark like this:** both engines must do the *same work*. DuckDB's headline numbers come in two flavors — "with storage" (after loading the CSV into DuckDB's native columnar format) and "without storage" (querying the raw CSV each time). [csvql](https://github.com/melihbirim/csvql) always queries raw CSV directly; it has no native store. So the fair fight is **both engines reading the raw CSV, cold, every run**:

```
csvql:  csvql "SELECT ... FROM 'trips.csv' ..."
duckdb: SELECT ... FROM read_csv_auto('trips.csv') ...   -- NOT a preloaded table
```

Comparing csvql-on-raw-CSV against DuckDB-on-preloaded-store would be dishonest, and anyone who opened the script would say so. Direct vs direct is the only comparison that survives scrutiny.

**Machine:** Apple Silicon, macOS. **DuckDB:** v1.4.2. **Method:** best-of-5 runs per query, warm OS page cache (both engines equally).

---

## Speed: ~2.8x on 8 GB

| Query | csvql | DuckDB | Speedup |
| ----- | ----- | ------ | ------- |
| Q01 `COUNT(*) GROUP BY cab_type` | **1.29 s** | 3.55 s | **2.8x** |
| Q02 `AVG(total_amount) GROUP BY passenger_count` | **1.41 s** | 3.81 s | **2.7x** |
| Q03 `COUNT(*) GROUP BY passenger_count, year` | **1.36 s** | 4.01 s | **2.9x** |
| Q04 `GROUP BY passenger_count, year, ROUND(distance)` | **1.38 s** | 3.99 s | **2.9x** |

Query latency, lower is better:

```
        0s        1s        2s        3s        4s
Q01 csvql  ██████▌ 1.29
    duck   ██████████████████ 3.55
Q02 csvql  ███████ 1.41
    duck   ███████████████████ 3.81
Q03 csvql  ██████▊ 1.36
    duck   ████████████████████ 4.01
Q04 csvql  ██████▉ 1.38
    duck   ████████████████████ 3.99
```

Here's the counterintuitive part: **the gap shrinks as files get bigger.** On a 417 MB / 1M-row sample, csvql is ~10x faster. On 8 GB, ~2.8x. Why? On small files DuckDB's process startup and CSV-reader initialization dominate the wall clock, and csvql's lean startup wins big. On large files the actual parse-and-aggregate throughput dominates, DuckDB's parallel CSV reader catches up, and csvql settles into a steady ~2.8x. Both numbers are true; the honest thing is to publish the whole curve with file sizes attached, not cherry-pick the 10x.

---

## Memory: ~6x less

Same 8 GB file, peak memory footprint per query:

| Query | csvql | DuckDB |
| ----- | ----- | ------ |
| Q01 | **29 MB** | 178 MB |
| Q02 | **30 MB** | 210 MB |
| Q03 | **34 MB** | 208 MB |
| Q04 | **38 MB** | 219 MB |

```
Peak memory (MB), 8 GB file
csvql   █▍                          ~30 MB
duckdb  ██████████████████████████  ~200 MB
```

csvql streams the file through a memory-mapped scan and keeps only the group-by hash table in memory — a few dozen groups. It never holds more than a sliver of the 8 GB at once. That's why a 30 MB footprint can chew through an 8 GB file.

---

## Storage: the number that actually matters

This is where "raw CSV, no store" stops being a fairness caveat and becomes the whole point.

```
Extra disk required to answer these queries on the 8 GB file:

csvql   0 bytes        — queries the CSV in place, no ingest
duckdb  2.1 GB + 21.7s — must build a native store for its fast path
```

DuckDB *can* be faster than these raw-CSV numbers — but only via its "with storage" path, which first ingests the CSV into a 2.1 GB native store, a one-time cost of ~22 seconds for a single 8 GB file (and minutes for the full multi-file dataset). csvql needs **zero extra disk and zero ingest** to hit its numbers. For an ad-hoc query against a CSV that landed on your disk five minutes ago, "no ingest" is the difference between an answer now and an answer after a coffee break.

**The honest summary:** same answers, ~2.8x faster, ~6x less memory, zero extra disk, zero ingest.

---

## The test I *couldn't* run — and why that's the honest part

You'll notice I didn't put csvql's numbers next to DuckDB's *published* figures (their blog reports Q01–Q04 at 2.45 / 3.89 / 5.21 / 11.2 s without storage). It would have looked great — csvql's ~1.3 s next to their 11.2 s on Q04 is a juicy 8x. It would also have been junk.

Two reasons:

1. **Different dataset size.** DuckDB's published query numbers are on the *full* ~1.1-billion-row dataset (all 65 files, ~500 GB of uncompressed CSV). Mine are on a single 20-million-row file. That's a 55x difference in rows. Putting them in the same table would be comparing csvql-on-20M against DuckDB-on-1.1B — the benchmarking equivalent of a fixed fight. (You can see the size gap in their own Q04: 11.2 s on 1.1B rows vs the ~4 s my local DuckDB takes on 20M.)

2. **Different hardware.** Their numbers are from an M1 Pro; mine are from a different Apple Silicon box. Borrowing a competitor's number measured on other silicon and dividing it by yours isn't a benchmark, it's a wish.

To *legitimately* cite DuckDB's published figures, csvql would have to run the identical ~1.1-billion-row dataset — which means ~500 GB of uncompressed CSV on disk. My machine has 275 GB free. It physically won't fit. So instead of borrowing numbers across hardware and data sizes, I ran **both engines on the same 20 M rows on the same machine with the same OS cache state.** That controls for everything the cross-hardware comparison doesn't. It's a smaller, less flashy result — and a far more trustworthy one.

If someone with a 1 TB disk wants to run the full 500 GB and report csvql against DuckDB's exact published table, the script is one flag away (`bench_taxi.sh 65`). I'd love to see it.

---

## The plot twist: the benchmark found two bugs in *my* engine

I built the benchmark to measure speed. It ended up as the best test suite csvql ever had, because running real queries on real data immediately broke two of them:

**Bug #1 — `GROUP BY ROUND(trip_distance)` crashed with `ColumnNotFound`.** Grouping by a *string*-producing function (`STRFTIME`) worked; grouping by a *numeric*-producing one (`ROUND`) didn't — the group-key resolver simply had no case for it. Q04 wouldn't run at all until I added one.

**Bug #2 — `ORDER BY COUNT(*) DESC` silently returned unsorted rows.** No error, no crash — just wrong output. The `GROUP BY` code path sorted results by the internal hash key and ignored the `ORDER BY` clause entirely. This is the scariest class of bug: the kind that returns a confident, wrong answer. A benchmark that only checked timings would have sailed right past it; comparing *output* against DuckDB caught it in one diff.

Both are now fixed, each with a regression test, and the four canonical queries produce byte-identical results to DuckDB (modulo `1` vs `1.0` float formatting). The moral: **a benchmark you check against a trusted oracle is a correctness test wearing a stopwatch.** If I'd only ever run csvql against itself, both bugs would still be there.

---

## Reproduce it yourself

Everything is in the repo. No 500 GB required — the sample mode pulls ~417 MB and runs in seconds:

```bash
git clone https://github.com/melihbirim/csvql
cd csvql && zig build -Doptimize=ReleaseFast

# quick: 1M-row sample (~417 MB)
./bench/bench_taxi.sh --sample

# full: 20M rows (~8 GB download from DuckDB's blobs)
./bench/bench_taxi.sh 1

# memory / CPU / storage instead of speed
./bench/bench_taxi.sh --resources 1
```

The script downloads DuckDB's own dataset, runs both engines on the raw CSV, and prints the tables above. It's ~130 lines of bash — read it before you trust it.

---

## Takeaways

- **Benchmark direct-vs-direct or don't bother.** The single most important line in the whole exercise was refusing to compare raw-CSV csvql against preloaded DuckDB.
- **Publish the whole curve.** ~10x on 400 MB and ~2.8x on 8 GB are both true. Hiding one to headline the other is how benchmarks earn their bad reputation.
- **The interesting number isn't always speed.** ~6x less memory and *zero ingest* is a bigger deal for ad-hoc CSV work than shaving two seconds.
- **Point your benchmark at a trusted oracle.** It doubles as the correctness test you were too lazy to write. It found two bugs I'd have shipped.

csvql is open source (Zig, single static binary, [github.com/melihbirim/csvql](https://github.com/melihbirim/csvql)). If you run the full dataset, tell me what you get.

*Numbers: Apple Silicon, macOS, DuckDB v1.4.2, best-of-5, warm cache. Dataset and queries: [DuckDB's NYC-taxi benchmark](https://github.com/pdet/taxi-benchmark).*
