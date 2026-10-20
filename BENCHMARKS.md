# Benchmarks

Detailed performance analysis vs DuckDB (and ClickHouse where noted). For the headline numbers, see the [README](README.md#performance).

DuckDB and DataFusion CLIs default to displaying only 40 rows, making them appear faster than they are. These benchmarks use `-csv` mode (DuckDB) and `FORMAT CSV` (ClickHouse) to force full output materialization. DataFusion CLI caps output at ~8K rows regardless of settings, so full-output numbers are unavailable for it.

## Aggregates, GROUP BY, DISTINCT

**2M rows, 56 MB CSV, Apple M2 Pro** — both engines on the raw CSV (best-of-5 via [`bench/bench_all.sh --section queries`](bench/bench_all.sh)):

| Query                             | csvql      | DuckDB | Speedup  |
| --------------------------------- | ---------- | ------ | -------- |
| `SELECT COUNT(*)` scalar          | **0.012s** | 0.136s | **11.3x** |
| `MIN(age), MAX(age)`              | **0.016s** | 0.132s | **8.2x** |
| `SELECT AVG(salary)` scalar       | **0.016s** | 0.130s | **8.1x** |
| `SELECT SUM(salary)` scalar       | **0.018s** | 0.138s | **7.7x** |
| `VARIANCE + STDDEV GROUP BY`      | **0.020s** | 0.152s | **7.6x** |
| `COUNT(*) GROUP BY`               | **0.020s** | 0.146s | **7.3x** |
| `SELECT DISTINCT city`            | **0.020s** | 0.142s | **7.1x** |
| `GROUP_CONCAT`                    | **0.020s** | 0.140s | **7.0x** |
| `SUM + AVG GROUP BY`              | **0.022s** | 0.150s | **6.8x** |
| `MEDIAN GROUP BY`                 | **0.056s** | 0.160s | **2.9x** |

**35x less memory** than DuckDB (1.8MB vs 63.5MB).

## GROUP BY by cardinality (csvql only)

**20M rows, 324 to 424 MB CSV, Apple M4 Max (14 cores)**, `> /dev/null`, median of 5 runs, via [`bench/bench_groupby_cardinality.sh`](bench/bench_groupby_cardinality.sh). The script generates its own data and is not run by CI. "high" draws 10M keys with replacement, which gives 5.7M distinct groups.

| Query                           | Keys (distinct) | 1 thread | 14 threads |
| ------------------------------- | --------------- | -------- | ---------- |
| `COUNT(*)`                      | 100             | 0.49s    | 0.05s      |
| `COUNT(*), SUM, AVG`            | 100             | 0.61s    | 0.07s      |
| `COUNT(*)`                      | 100,000         | 0.69s    | 0.31s      |
| `COUNT(*), SUM, AVG`            | 100,000         | 0.96s    | 0.43s      |
| `COUNT(*)`                      | 5.7M            | 4.12s    | 2.18s      |
| `COUNT(*), SUM, AVG`            | 5.7M            | 4.55s    | 2.81s      |

At 5.7M groups, most of the time was outside the scan. Phase timings at 14 threads for `COUNT(*), SUM, AVG`: scan 2.0s, sorting the group keys 3.1s, writing the rows 1.7s. Sorting compared pointers into scattered key storage; it now sorts an array that carries the first eight key bytes and the accumulator pointer (0.45s and 0.75s). The shared map now uses at least 64 shards instead of one per thread, which cut the scan phase from about 2.0s to 1.6s. The same query took 6.93s at 14 threads and 8.31s at 1 thread before those two changes.

## Output-format throughput

**2M rows, 56 MB CSV, Apple M2 Pro** — full output, all rows, via [`bench/bench_all.sh --section formats`](bench/bench_all.sh):

| Output format              | csvql      | DuckDB | Speedup  |
| -------------------------- | ---------- | ------ | -------- |
| CSV                        | **0.030s** | 0.258s | **8.6x** |
| JSON array (`--json`)      | **0.054s** | 0.328s | **6.1x** |
| JSONL / NDJSON (`--jsonl`) | **0.058s** | 0.328s | **5.7x** |

Outputs are semantically/byte-identical to DuckDB (verified via [`bench/verify_correctness.sh`](bench/verify_correctness.sh)). Reproduce any of these with [`bench/bench_all.sh`](bench/bench_all.sh).

## LIKE operator

**5M rows, 173MB CSV, Apple M2** — CSV output, `> /dev/null`:

| Pattern                        | Description              | csvql     | DuckDB | Speedup  |
| ------------------------------ | ------------------------ | --------- | ------ | -------- |
| `WHERE name LIKE 'A%'`         | Prefix wildcard          | **0.06s** | 2.17s  | **~36x** |
| `WHERE city LIKE '%on'`        | Suffix wildcard          | **0.06s** | 1.12s  | **~19x** |
| `WHERE department LIKE '%ing'` | Suffix, high selectivity | **0.07s** | 2.54s  | **~36x** |

Row counts verified identical to DuckDB. Run the benchmark yourself: [`bench/bench_all.sh --section like`](bench/bench_all.sh)

## JOIN (hash join)

**2M rows, 56 MB CSV, Apple M2 Pro** — via [`bench/bench_all.sh --section join`](bench/bench_all.sh):

| Query                                          | csvql      | DuckDB  | Speedup   |
| ---------------------------------------------- | ---------- | ------- | --------- |
| `JOIN departments` (2M × 6)                    | 0.044s     | 2.650s  | **60x**   |
| `JOIN + WHERE` (2M × 6)                        | 0.034s     | 0.988s  | **29x**   |
| `JOIN SELECT *` (2M × 6, all cols)             | 0.088s     | 7.832s  | **89x**   |
| `JOIN cities` (2M × 8)                         | 0.046s     | 2.508s  | **55x**   |
| `JOIN bonus` (2M × 50K, numeric key)           | 0.032s     | 0.286s  | **9x**    |
| `3-table JOIN` (2M × 6 × 3)                    | 0.058s     | 3.294s  | **57x**   |
| `4-table JOIN` (2M × 6 × 3 × 6)               | 0.076s     | 3.948s  | **52x**   |

The base table is memory-mapped, split into line-aligned chunks, and probed against the right-side hash maps across all cores in parallel — output order in a join is undefined, so each worker streams its matches straight out with no merge step. Combined with cat-speed CSV reading (DuckDB spends most of a join parsing the CSV), joins that used to be roughly on par are now a clear win. Byte-for-byte verified against DuckDB in [`bench/verify_correctness.sh`](bench/verify_correctness.sh).

## WHERE col IN (subquery)

**2M-row orders table, cloud VM (Intel Xeon, 4 cores) — different hardware from the tables above, not directly comparable across sections** — `WHERE col IN (SELECT ...)` (#124), which is really a semi-join: build a hash table from the subquery's result, probe it per outer row. Resolved-list size varied by how selective the lookup table's own filter is:

| Resolved subquery result size      | csvql      | DuckDB | Speedup   |
| ----------------------------------- | ---------- | ------ | --------- |
| 6 (a typical lookup-table filter)  | **0.034s** | 0.17s  | **~5x**   |
| 10,000                             | **0.045s** | 0.17s  | **~3.8x** |
| 100,000                            | **0.088s** | 0.21s  | **~2.3x** |
| 1,000,000                          | 0.358s     | 0.357s | even      |

Row counts verified identical to DuckDB at every size. This was originally a ~20x *loss* at 10,000 resolved values (a linear-scan membership check, fine for a hand-typed literal list, not for a subquery-sized one) — found via differential testing, root-caused, and fixed with a hash-set semi-join. Full story, including the crossover point and why it flattens rather than staying ahead forever: [`CORRECTNESS.md#performance-vs-duckdb`](CORRECTNESS.md#performance-vs-duckdb).

Reproduce: [`bench/bench_insubquery.sh`](bench/bench_insubquery.sh)

## NYC Taxi benchmark

**20M rows, 8 GB CSV, Apple M-series** — the canonical [Billion-Taxi-Rides](https://github.com/pdet/taxi-benchmark) queries on [DuckDB's own dataset](https://duckdb.org/2024/10/16/driving-csv-performance-benchmarking-duckdb-with-the-nyc-taxi-dataset). Both engines query the raw uncompressed CSV **directly** (no preload into a native store), cold per run, best-of-3, warm OS cache:

Engine versions: csvql **2.6.0**, DuckDB **1.5.5**.

| Query                                                       | csvql     | DuckDB | Speedup  |
| ----------------------------------------------------------- | --------- | ------ | -------- |
| Q01 `COUNT(*) GROUP BY cab_type`                            | **1.42s** | 4.70s  | **3.3x** |
| Q02 `AVG(total_amount) GROUP BY passenger_count`            | **1.39s** | 4.61s  | **3.3x** |
| Q03 `COUNT(*) GROUP BY passenger_count, year`               | **1.39s** | 4.71s  | **3.4x** |
| Q04 `GROUP BY passenger_count, year, ROUND(distance) ...`   | **1.52s** | 4.70s  | **3.1x** |

Results verified identical to DuckDB. The gap **widens on smaller files** — ~10x on the 417 MB / 1M-row sample, where DuckDB's process and CSV-reader startup dominate; on 8 GB the actual parse+aggregate work dominates and csvql holds a clean ~3.2x.

Reproduce: [`bench/bench_taxi.sh`](bench/bench_taxi.sh) — `./bench/bench_taxi.sh --sample` (417 MB, quick) or `./bench/bench_taxi.sh 1` (full 20M rows, ~8 GB download).

### Memory & storage

Same 8 GB / 20M-row file, peak memory footprint, single cold run:

| Query | csvql peak | DuckDB peak |
| ----- | ---------- | ----------- |
| Q01   | **29 MB**  | 177 MB      |
| Q02   | **30 MB**  | 209 MB      |
| Q03   | **34 MB**  | 213 MB      |
| Q04   | **38 MB**  | 217 MB      |

**~6x less memory** — and csvql needs **0 bytes of extra storage**: it queries the CSV in place via mmap, no ingest. DuckDB's fast "with storage" path first materializes a **2.1 GB native store (25.8 s one-time ingest)** before it can reach comparable query times; querying the raw CSV directly (as csvql does), it uses ~6x the memory and stays ~3x slower.

Reproduce: `./bench/bench_taxi.sh --resources 1` (or `--resources --sample`).

### Read ceiling

**At scale, csvql reads raw CSV faster than a plain `cat` of the same file.** On the 8 GB file (best-of-3, warm cache), `cat file > /dev/null` takes **1.92s**; csvql's `SELECT COUNT(*)` takes **1.05s**, and `GROUP BY cab_type` / `GROUP BY + AVG` both land at **~1.32s** — parallel mmap reads across cores beat a single-threaded sequential `cat`. The parsing, grouping, and aggregation add next to nothing on top of the read itself, which is why the raw-CSV gap over DuckDB (which does more work per byte, single-process) holds at ~3.2x.

Run the full suite (all sections): [`bench/bench_all.sh`](bench/bench_all.sh)

## Apache DataFusion (Apache Arrow's own SQL engine)

Raw `pyarrow` has no SQL/`GROUP BY` layer of its own to compare against — [DataFusion](https://arrow.apache.org/datafusion/) is the fair "Apache Arrow" comparison: an Arrow subproject, SQL executed natively over Arrow's own columnar in-memory format, comparable in scope to csvql's own feature set (unlike DuckDB, which isn't part of the Arrow project).

**1M rows, 405 MB CSV (the same NYC Taxi sample used above), Apple M-series** — same raw-CSV-direct, cold-per-run, best-of-5 discipline as the DuckDB comparison:

Engine versions: csvql **2.6.0**, DataFusion **52.1.0**.

| Query                                             | csvql      | DataFusion | Speedup   |
| -------------------------------------------------- | ---------- | ---------- | --------- |
| Q01 `COUNT(*) GROUP BY cab_type`                  | **0.052s** | 0.100s     | **1.9x**  |
| Q02 `AVG(total_amount) GROUP BY passenger_count`  | **0.054s** | 0.103s     | **1.9x**  |

Peak memory, single cold run: **28.6-29.2MB** (csvql) vs **161.3-162.8MB** (DataFusion), ~5.6x less.

**Only 2 of the 4 canonical queries are published here — Q03/Q04 (`GROUP BY ... year`, parsed from `pickup_datetime`) are deliberately excluded, not forgotten.** This fixture has genuinely mixed datetime formats in one column (`2012-08-31 22:00:00` and `2011-04-01T05:47:46`) — real, messy source data, not a benchmark artifact. csvql and DuckDB are both schema-less/string-based for this column and handle both formats identically (verified byte-for-byte). DataFusion's CSV reader infers `pickup_datetime` as a native `Timestamp(s)` column from a sample (`DESCRIBE` confirms this), and rows in whichever format didn't match that inferred schema silently vanish from `GROUP BY` output — confirmed via `diff` against csvql's output: real missing year groups, not a formatting difference. This is a genuine DataFusion behavior on messy real-world data, not a csvql bug — and not yet root-caused deeply enough to call DataFusion "wrong" either, just different from DuckDB's more forgiving (VARCHAR-until-cast) inference. Publishing a speed number for a query where the two engines don't agree on the answer would be comparing a wrong result's speed to a right one's, which this project's own [`CORRECTNESS.md`](CORRECTNESS.md) discipline rules out.

Only ~1.9x on speed here (vs ~3.3-10x against DuckDB) is the expected, honest result — DataFusion is a mature, well-optimized vectorized engine, a much closer fight than DuckDB's own CSV path on the same queries. The bigger, more consistent gap is memory, matching the same fixed-per-process-overhead pattern found when comparing libscanio (a sibling project) against `pyarrow`/`pyarrow.dataset`: Arrow-ecosystem engines carry buffer-pool/schema-inference/vectorized-operator scaffolding that a schema-less, mmap-based scanner never needs to pay for.

Reproduce: [`bench/bench_datafusion.sh --sample`](bench/bench_datafusion.sh) (`--resources` for memory, `N=5` for more runs). Needs `datafusion-cli` (`cargo install datafusion-cli`) and the fixture already cached by `bench_taxi.sh --sample`.

## Polars 2.0.0 (stable), NYC Taxi sample

Polars 2.0.0 shipped on 6 October 2026, defaulting SQL execution to its streaming engine. Measured here with `polars==2.0.0` from PyPI in an isolated venv (`bench/.venv-polars`). An earlier version of this section used `2.0.0rc1` and read 0.100s / 0.099s for Polars, 2.5x / 2.2x; the stable release is within noise of that.

Same 2 canonical queries, same fixture (425 MB NYC Taxi sample, 1M rows), same cold-per-run best-of-5 discipline, three repeats of the whole script agreed within 0.008s. Polars queries the raw CSV directly via `pl.scan_csv` + `SQLContext`, streaming engine, no separate load step, same rule as the DuckDB/DataFusion comparisons.

Engine versions: csvql **2.9.0** (main at 683b9d0), Polars **2.0.0**, Python 3.14.4, Apple M2 Pro.

| Query                                             | csvql      | Polars     | Speedup   |
| -------------------------------------------------- | ---------- | ---------- | --------- |
| Q01 `COUNT(*) GROUP BY cab_type`                  | **0.041s** | 0.117s     | **2.8x**  |
| Q02 `AVG(total_amount) GROUP BY passenger_count`  | **0.043s** | 0.111s     | **2.6x**  |

Reproduce: `bench/.venv-polars/bin/python3 bench/bench_polars.py` (needs the fixture already cached by `bench_taxi.sh --sample`).

### Polars 2.0.0, library comparison on a 20M row fixture

Different fixture and harness from the two rows above, so not a like-for-like comparison with them.

20M rows / 711 MB, one query per process, engines interleaved, first round discarded, median of 5. Engine versions: csvql **2.8.1** plus the `COUNT(*)` change, Polars **2.0.0**, DuckDB **1.5.6**.

| Query shape | csvql | Polars 2.0 | DuckDB | vs Polars | vs DuckDB |
| --- | --- | --- | --- | --- | --- |
| `SUM(salary)` | **0.0807** | 0.2268 | 0.3716 | **2.8x** | **4.6x** |
| `COUNT(*) GROUP BY department` | **0.1241** | 0.3173 | 0.4978 | **2.6x** | **4.0x** |
| `COUNT(*) WHERE city = '...'` | **0.0804** | 0.2818 | 0.4351 | **3.5x** | **5.4x** |
| `LIMIT 10`, no sort | **0.0011** | 0.0029 | 0.0662 | **2.7x** | **60.9x** |
| `COUNT(*)` | 0.0256 | **0.0220** | 0.4005 | 0.9x | **15.6x** |
| `ORDER BY salary LIMIT 10` | 0.3440 | **0.3047** | 0.4197 | 0.9x | **1.2x** |

Wide file, 757 MB / 60 columns, `SUM` of one column at position 58: csvql **0.0563s**, Polars 0.2187s (**3.9x**), DuckDB 0.5767s (**10.2x**).

Peak RSS on `GROUP BY department`, 711 MB file: csvql **54 MB**, Polars 1110 MB. On `ORDER BY salary DESC LIMIT 10` the same file peaks at 715 MB in csvql, because sorts hold rows while aggregates hold an accumulator.

Note on the Polars API: `pl.read_csv` (eager) got slower in 2.0 while `pl.scan_csv` (lazy) got much faster, by 3.8x on top-N. These figures use `scan_csv`. Benchmarking the eager path overstates csvql by up to 7x on sorts, which is a mistake we made and corrected in [csvql vs polars](https://melihbirim.github.io/csvql/blog/csvql-vs-polars.html).

Reproduce: `./bench/bench_vs_polars.sh fixture.csv 6`.

## Small files, startup included

**2M rows, 69 MB, unquoted CSV, Apple M2 Pro**, both engines reading the raw file. Mean of 30 runs, because a single run finishes faster than a shell timer can resolve.

| Query | csvql | DuckDB | Speedup |
| --- | --- | --- | --- |
| `COUNT(*)` | **0.0139s** | 0.1185s | **8.5x** |
| `COUNT(*) WHERE age > 30` | **0.0134s** | 0.1170s | **8.7x** |
| `SUM(salary), AVG(salary)` | **0.0180s** | 0.1178s | **6.5x** |

At this size process startup is a visible share of both totals: 0.022s for DuckDB, 0.005s for csvql. Subtracting it, `COUNT(*)` is 10.5x rather than 8.5x, so these figures understate rather than flatter.

**20M rows, 711 MB** (best-of-5):

| Query                                   | csvql      | DuckDB | Speedup   |
| --------------------------------------- | ---------- | ------ | --------- |
| `COUNT(*) GROUP BY department`          | **0.098s** | 0.484s | **4.9x**  |
| `COUNT(*) WHERE age > 30 GROUP BY`      | **0.102s** | 0.506s | **5.0x**  |
| `SELECT DISTINCT city`                  | **0.090s** | 0.494s | **5.5x**  |
| `SUM(salary), AVG(salary) GROUP BY`     | **0.132s** | 0.540s | **4.1x**  |
| `MEDIAN(salary) GROUP BY department`    | 2.832s     | **0.580s** | **0.2x** |

The last row is a real loss, not a rounding artefact: `MEDIAN` buffers every value in a group, so its memory and time are O(rows) rather than O(groups) ([#205](https://github.com/melihbirim/csvql/issues/205)).

## Quoting and column count

Quoting changes the picture; column count does not. csvql skips 16 bytes at a time whenever no delimiter, newline or quote is present, and a quoted field disables that. Same files, ~200-byte rows, differing only in whether one column is quoted, `COUNT(*)` with a `WHERE`, 12 threads:

| Columns | Quoting | csvql | DuckDB | Speedup |
| --- | --- | --- | --- | --- |
| 2 | unquoted | **0.026s** | 0.170s | **6.5x** |
| 2 | quoted | 0.084s | 0.136s | 1.6x |
| 8 | unquoted | **0.040s** | 0.240s | **6.0x** |
| 8 | quoted | 0.116s | 0.236s | 2.0x |
| 32 | unquoted | **0.080s** | 0.458s | **5.7x** |
| 32 | quoted | 0.196s | 0.384s | 2.0x |

The speedup is steady across a sixteen-fold range of column counts. Quoting is the variable that matters, costing roughly 3x. Reproduce with [`bench/bench_scaling.sh`](bench/bench_scaling.sh). Why, and the work in progress: [The Journey to 10x DuckDB](https://melihbirim.github.io/csvql/blog/journey-to-10x-duckdb-csv.html).

## Memory and time across file sizes

Same `GROUP BY department`, four engines, one process each, across a 12x range of file size on a 16 GB machine. Peak RSS and wall time:

| file | csvql | DuckDB | polars 2.0 | pandas |
| --- | --- | --- | --- | --- |
| 0.69 GB | **74 MB / 0.2s** | 254 MB / 0.8s | 1151 MB / 0.6s | 1445 MB / 6.5s |
| 1.89 GB | **74 MB / 0.4s** | 253 MB / 1.2s | 2107 MB / 1.1s | 2093 MB / 17.5s |
| 3.80 GB | **75 MB / 0.7s** | 275 MB / 2.1s | 2666 MB / 9.8s | 2355 MB / 36.4s |
| 8.14 GB | **74 MB / 2.4s** | 367 MB / 5.5s | 1754 MB / 26.7s | 4024 MB / 90.8s |

csvql's memory does not move with file size because an aggregate holds a running accumulator rather than a dataset. The time advantage over polars grows with the file, from 3x to 11x, and over pandas from 33x to 38x.

Two things worth saying plainly. **DuckDB is the memory-efficient competitor, not polars**: 254 to 367 MB across the same range, nearly as flat as csvql, so against DuckDB the memory difference is 3.7x rather than the 15x polars suggests. And every engine here finished the 8 GB file on 16 GB of RAM, so there is no point at which the others fall over and csvql keeps going. pandas peaks at 4 GB on an 8 GB file because it stores parsed typed columns rather than text, and polars' peak is not even monotonic (2666 MB at 3.8 GB, 1754 MB at 8.1 GB), so do not extrapolate either curve.

Flat memory is a property of aggregate queries, not of every query: `ORDER BY` has to hold rows, and the 711 MB file peaks at 715 MB on a top-N.

All four columns are measured the same way, in-process from Python via `ru_maxrss`, so each includes the interpreter's own footprint of roughly 20 MB. That is why csvql reads 74 MB here and 54 MB when the CLI is measured directly. pandas, measured separately on the same `GROUP BY`, takes 0.54s at 2M rows and 6.72s at 20M; that margin says more about `read_csv` loading the whole file into a DataFrame than about either engine.

Reproduce with [`bench/bench_vs_polars.sh`](bench/bench_vs_polars.sh). Full working and the corrections to an earlier version of these numbers: [csvql vs polars](https://melihbirim.github.io/csvql/blog/csvql-vs-polars.html).

## How is csvql so fast?

- **Memory-mapped I/O** — zero-copy reading at 1.4 GB/sec
- **7-core parallel execution** — lock-free architecture, 669% CPU utilization
- **SIMD field parsing** — vectorized comma detection
- **Radix sort** — O(8N) with IEEE 754 f64→u64 bit trick and pass-skipping
- **Top-K heap** — O(N log K) for LIMIT queries, avoids sorting entire dataset
- **Hardware-aware thresholds** — ARM vs x86 tuned for L1 cache
- **Zero per-row allocations** — arena buffers, zero-copy slices
- **Adaptive GROUP BY pre-sizing** — hash table capacity tuned to chunk size, eliminates rehash cycles
- **Zero-copy worker scans** — each thread iterates a direct mmap slice, no `pread` syscalls or seam buffers

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full optimization story.
