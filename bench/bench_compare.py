#!/usr/bin/env python3
"""
bench_compare.py — csvql vs DuckDB: speed AND output correctness across query types.

Both engines query the same raw CSV directly (no preload). Each query is timed
best-of-N and its output is compared to DuckDB's, numeric cells with tolerance so
float formatting differences don't count as mismatches.

Usage:
    ./bench/bench_compare.py [rows]     # default 5_000_000 (~87 MB)

Needs: csvql (built or on PATH), duckdb, python3. Dataset is generated once and
cached under bench/.compare-data/ (delete to regenerate).
"""
import csv
import io
import os
import random
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
BIN = os.path.join(ROOT, "zig-out", "bin", "csvql")
if not os.path.exists(BIN):
    BIN = subprocess.run(["bash", "-lc", "command -v csvql"], capture_output=True, text=True).stdout.strip() or BIN

ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 5_000_000
DATA_DIR = os.path.join(HERE, ".compare-data")
F = os.path.join(DATA_DIR, "emp.csv")
DK = f"read_csv_auto('{F}')"


def generate():
    os.makedirs(DATA_DIR, exist_ok=True)
    random.seed(7)
    depts = ["eng", "sales", "ops", "hr", "mkt"]
    cities = ["NYC", "LA", "SF", "ATX", "SEA"]
    with open(F, "w") as fh:
        fh.write("dept,city,age,salary\n")
        for _ in range(ROWS):
            fh.write(f"{random.choice(depts)},{random.choice(cities)},{random.randint(21, 65)},{random.randint(40000, 200000)}\n")


def run(cmd):
    return subprocess.run(cmd, capture_output=True, text=True).stdout


def best(cmd, n=5):
    b = float("inf")
    for _ in range(n):
        t = time.perf_counter()
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        b = min(b, time.perf_counter() - t)
    return b


def rows_of(txt):
    r = list(csv.reader(io.StringIO(txt.strip())))
    return r[1:] if r else []  # drop header


def norm(rows):
    def cell(c):
        try:
            return round(float(c), 4)
        except ValueError:
            return c
    return sorted(tuple(cell(c) for c in r) for r in rows)


def same(a, b, tol=1e-6):
    na, nb = norm(a), norm(b)
    if len(na) != len(nb):
        return False
    for ra, rb in zip(na, nb):
        if len(ra) != len(rb):
            return False
        for x, y in zip(ra, rb):
            if isinstance(x, float) and isinstance(y, float):
                if abs(x - y) > tol * max(1, abs(y)):
                    return False
            elif x != y:
                return False
    return True


# (label, csvql_sql, duckdb_sql[, kind]). kind="concat" compares GROUP_CONCAT
# output as an order-insensitive multiset (neither engine orders the values).
# ORDER BY LIMIT compares the sort key only, since ties on non-key columns are an
# undefined (but valid) ordering in both.
QUERIES = [
    ("COUNT(*)", f"SELECT COUNT(*) FROM '{F}'", f"SELECT COUNT(*) FROM {DK}"),
    ("SUM + AVG GROUP BY", f"SELECT dept, SUM(salary), AVG(salary) FROM '{F}' GROUP BY dept",
     f"SELECT dept, SUM(salary), AVG(salary) FROM {DK} GROUP BY dept"),
    ("VARIANCE + STDDEV GROUP BY", f"SELECT dept, VARIANCE(salary), STDDEV(salary) FROM '{F}' GROUP BY dept",
     f"SELECT dept, VAR_POP(salary), STDDEV_POP(salary) FROM {DK} GROUP BY dept"),
    ("MEDIAN GROUP BY", f"SELECT dept, MEDIAN(salary) FROM '{F}' GROUP BY dept",
     f"SELECT dept, median(salary) FROM {DK} GROUP BY dept"),
    ("MIN + MAX GROUP BY", f"SELECT city, MIN(salary), MAX(salary) FROM '{F}' GROUP BY city",
     f"SELECT city, MIN(salary), MAX(salary) FROM {DK} GROUP BY city"),
    ("WHERE filter count", f"SELECT COUNT(*) FROM '{F}' WHERE salary > 150000",
     f"SELECT COUNT(*) FROM {DK} WHERE salary > 150000"),
    ("2-key GROUP BY", f"SELECT dept, city, COUNT(*) FROM '{F}' GROUP BY dept, city",
     f"SELECT dept, city, COUNT(*) FROM {DK} GROUP BY dept, city"),
    ("DISTINCT", f"SELECT DISTINCT city FROM '{F}'", f"SELECT DISTINCT city FROM {DK}"),
    ("ORDER BY DESC LIMIT 10", f"SELECT salary FROM '{F}' ORDER BY salary DESC LIMIT 10",
     f"SELECT salary FROM {DK} ORDER BY salary DESC LIMIT 10"),
    ("GROUP_CONCAT (city per dept)", f"SELECT GROUP_CONCAT(city) FROM '{F}' WHERE age = 21",
     f"SELECT string_agg(city, ',') FROM {DK} WHERE age = 21", "concat"),
]


def main():
    if subprocess.run(["bash", "-lc", "command -v duckdb"], capture_output=True).returncode != 0:
        sys.exit("duckdb not found on PATH")
    if not os.path.exists(BIN):
        sys.exit("csvql not found (build: zig build -Doptimize=ReleaseFast)")
    if not os.path.exists(F):
        print(f"generating {ROWS:,}-row dataset ...")
        generate()
    mb = os.path.getsize(F) / 1e6
    print(f"\ncsvql vs DuckDB — {F} ({mb:.0f} MB, {ROWS:,} rows), both querying raw CSV\n")
    print(f"  {'query':30} {'csvql':>8} {'duckdb':>8} {'speedup':>8}  correct")
    print("  " + "-" * 66)
    all_ok = True
    for q in QUERIES:
        label, cq, dq = q[0], q[1], q[2]
        kind = q[3] if len(q) > 3 else None
        ct, dt = best([BIN, cq]), best(["duckdb", "-csv", "-c", dq])
        c_out, d_out = run([BIN, cq]), run(["duckdb", "-csv", "-c", dq])
        if kind == "concat":
            # single-cell concatenated list — compare as an order-insensitive multiset
            def multiset(txt):
                cell = rows_of(txt)[0][0] if rows_of(txt) else ""
                return sorted(cell.strip('"').split(","))
            ok = multiset(c_out) == multiset(d_out)
        else:
            ok = same(rows_of(c_out), rows_of(d_out))
        all_ok = all_ok and ok
        print(f"  {label:30} {ct:7.3f}s {dt:7.3f}s {dt / ct:7.2f}x  {'MATCH' if ok else 'DIFF'}")
    print(f"\n  correctness: {'all queries match DuckDB' if all_ok else 'MISMATCH — investigate'}")


if __name__ == "__main__":
    main()
