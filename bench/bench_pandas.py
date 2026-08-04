#!/usr/bin/env python3
"""
bench_pandas.py — csvql vs pandas on the same NYC-taxi queries used in bench_taxi.sh.

Same rule as the DuckDB benchmark: both sides do the same work, cold, every run.
pandas has no "query the file in place" mode, so its side is pd.read_csv() +
the pandas-equivalent operation — that IS how you use pandas for this, not a
handicap. csvql queries the raw CSV directly, no separate load step.

Usage:
    ./bench/bench_pandas.py [CSV]     # default: bench/.taxi-data/sample.csv
    N=5 ./bench/bench_pandas.py       # best-of-N runs per query (default: 3)

Needs: csvql built (zig build -Doptimize=ReleaseFast), pandas installed.
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
RUNS = int(os.environ.get("N", 3))

if not os.path.exists(CSV):
    sys.exit(f"CSV not found: {CSV}\nGet the sample first:  ./bench/bench_taxi.sh --sample")
if not os.path.exists(CSVQL):
    sys.exit("csvql not found (build: zig build -Doptimize=ReleaseFast)")

try:
    import pandas as pd
except ImportError:
    sys.exit("pandas not installed (pip install pandas)")

QUERIES = [
    (
        "Q01  COUNT(*) GROUP BY cab_type",
        f"SELECT cab_type, COUNT(*) FROM '{CSV}' GROUP BY cab_type",
        lambda df: df.groupby("cab_type").size(),
    ),
    (
        "Q02  AVG(total_amount) GROUP BY passenger_count",
        f"SELECT passenger_count, AVG(total_amount) FROM '{CSV}' GROUP BY passenger_count",
        lambda df: df.groupby("passenger_count")["total_amount"].mean(),
    ),
    (
        "Q03  COUNT(*) WHERE trip_distance > 5",
        f"SELECT COUNT(*) FROM '{CSV}' WHERE trip_distance > 5",
        lambda df: (df["trip_distance"] > 5).sum(),
    ),
    (
        "Q04  Top 3 passenger_count by AVG(tip_amount)",
        f"SELECT passenger_count, AVG(tip_amount) AS a FROM '{CSV}' GROUP BY passenger_count ORDER BY a DESC LIMIT 3",
        lambda df: df.groupby("passenger_count")["tip_amount"].mean().sort_values(ascending=False).head(3),
    ),
]

USE_COLS = ["cab_type", "passenger_count", "total_amount", "trip_distance", "tip_amount"]


def best_of(fn, n):
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return min(times)


def csvql_run(sql):
    subprocess.run([CSVQL, sql], capture_output=True, text=True, check=True)


print(f"\n  csvql vs pandas — {CSV}  ({os.path.getsize(CSV) / 1e6:.0f} MB, best-of-{RUNS})\n")
print(f"  {'query':<45} {'csvql':>10} {'pandas':>10} {'ratio':>8}")

for label, sql, pandas_op in QUERIES:
    csvql_t = best_of(lambda: csvql_run(sql), RUNS)

    def pandas_full():
        df = pd.read_csv(CSV, usecols=USE_COLS)
        pandas_op(df)

    pandas_t = best_of(pandas_full, RUNS)
    ratio = pandas_t / csvql_t
    print(f"  {label:<45} {csvql_t:>9.3f}s {pandas_t:>9.3f}s {ratio:>7.1f}x")

print()
