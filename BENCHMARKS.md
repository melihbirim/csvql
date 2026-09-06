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

## Polars 2.0.0-rc.1 (pre-release, pinned — not a stable-release comparison)

Polars 2.0 reached release-candidate stage on 2 September 2026, defaulting SQL execution to its new streaming engine. This is a **pre-release** result, pinned to `polars==2.0.0rc1` in an isolated venv (`bench/.venv-polars`) — kept separate from the stable-competitor numbers above because rc builds can change before final release. Re-run before citing against a different Polars version.

Same 2 canonical queries, same fixture (405 MB NYC Taxi sample), same cold-per-run best-of-5 discipline. Polars queries the raw CSV directly via `pl.scan_csv` + `SQLContext`, streaming engine, no separate load step — same rule as the DuckDB/DataFusion comparisons.

Engine versions: csvql **2.6.2**, Polars **2.0.0-rc.1**.

| Query                                             | csvql      | Polars     | Speedup   |
| -------------------------------------------------- | ---------- | ---------- | --------- |
| Q01 `COUNT(*) GROUP BY cab_type`                  | **0.040s** | 0.100s     | **2.5x**  |
| Q02 `AVG(total_amount) GROUP BY passenger_count`  | **0.044s** | 0.099s     | **2.2x**  |

Reproduce: `bench/.venv-polars/bin/python3 bench/bench_polars.py` (needs the fixture already cached by `bench_taxi.sh --sample`).

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
