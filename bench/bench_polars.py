#!/usr/bin/env python3
"""
bench_polars.py — csvql vs Polars SQL on the same 2 canonical NYC-taxi
queries used in the DataFusion comparison (see BENCHMARKS.md).

Polars runs SQL via pl.SQLContext, reading the raw CSV directly (scan_csv,
lazy) so both sides query the file in place with no separate load step,
same rule as the DuckDB/DataFusion benchmarks. Cold-per-run, best-of-N.

Usage:
    ./bench/bench_polars.py [CSV]     # default: bench/.taxi-data/sample.csv
    N=5 ./bench/bench_polars.py       # best-of-N runs per query (default: 5)

Needs: csvql built (zig build -Doptimize=ReleaseFast), polars installed
(this repo pins bench/.venv-polars to a specific pre-release — see
BENCHMARKS.md before trusting these numbers against a different version).
"""
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
CSVQL = os.path.join(ROOT, "zig-out", "bin", "csvql")
if not os.path.exists(CSVQL):
    CSVQL = subprocess.run(["bash", "-lc", "command -v csvql"], capture_output=True, text=True).stdout.strip() or CSVQL

CSV = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, ".taxi-data", "sample.csv")
RUNS = int(os.environ.get("N", 5))

if not os.path.exists(CSV):
    sys.exit(f"CSV not found: {CSV}\nGet the sample first:  ./bench/bench_taxi.sh --sample")
if not os.path.exists(CSVQL):
    sys.exit("csvql not found (build: zig build -Doptimize=ReleaseFast)")

try:
    import polars as pl
except ImportError:
    sys.exit("polars not installed — this repo pins a version, see bench/.venv-polars (BENCHMARKS.md)")

QUERIES = [
    ("Q01  COUNT(*) GROUP BY cab_type",
     f"SELECT cab_type, COUNT(*) FROM '{CSV}' GROUP BY cab_type",
     f"SELECT cab_type, COUNT(*) FROM taxi GROUP BY cab_type"),
    ("Q02  AVG(total_amount) GROUP BY passenger_count",
     f"SELECT passenger_count, AVG(total_amount) FROM '{CSV}' GROUP BY passenger_count",
     f"SELECT passenger_count, AVG(total_amount) FROM taxi GROUP BY passenger_count"),
]


def best_of(fn, n):
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return min(times)


def csvql_run(sql):
    subprocess.run([CSVQL, sql], capture_output=True, text=True, check=True)


def polars_run(sql):
    ctx = pl.SQLContext(taxi=pl.scan_csv(CSV))
    ctx.execute(sql).collect(engine="streaming")


print(f"\n  csvql vs Polars {pl.__version__} (pre-release) — {CSV}  ({os.path.getsize(CSV) / 1e6:.0f} MB, best-of-{RUNS})\n")
print(f"  {'query':<45} {'csvql':>10} {'polars':>10} {'speedup':>9}")

for label, csvql_sql, polars_sql in QUERIES:
    csvql_t = best_of(lambda: csvql_run(csvql_sql), RUNS)
    polars_t = best_of(lambda: polars_run(polars_sql), RUNS)
    speedup = polars_t / csvql_t
    print(f"  {label:<45} {csvql_t:>9.3f}s {polars_t:>9.3f}s {speedup:>8.1f}x")

print()
