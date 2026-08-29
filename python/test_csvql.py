"""Tests for the csvql Python binding.

These exist for the same reason nodejs/test_cli.js does: #149 shipped a
binding that aborted the host process on an ordinary query, and nothing
caught it because nothing ever *executed* the built library before it was
published. release.yml builds the wheels; a build cannot catch a runtime
fault. This suite is what CI runs so that class of bug fails here instead
of on PyPI.

src/lib.zig carried both #149 faults — a Zig GeneralPurposeAllocator that
does not work inside a dlopen()ed library, and the engine's multi-megabyte
stack frames on a host thread too small for them — and was fixed the same
way as the N-API addon. The crash-shape test below is the regression guard
for that.

No pytest dependency: plain asserts and a __main__ runner, so CI needs
nothing but a Python interpreter and a built libcsvql.
"""

import os
import sys
import tempfile
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import csvql  # noqa: E402

TMP = Path(tempfile.gettempdir())
PEOPLE = TMP / "csvql_py_test.csv"
BLANKS = TMP / "csvql_py_blanks.csv"

PEOPLE.write_text(
    "id,name,city,salary\n"
    "1,Alice,Austin,120000\n"
    "2,Bob,Boston,80000\n"
    "3,Carol,Austin,150000\n"
)
# Empty field in a middle column — exercises the NULL handling #147 touched.
BLANKS.write_text("id,name\n1,Alice\n2,\n3,Carol\n")

# Forward slashes: the path is embedded in a SQL string, and on Windows
# tempfile returns backslashes.
P = str(PEOPLE).replace("\\", "/")
B = str(BLANKS).replace("\\", "/")

passed = 0
total = 0


def check(label, actual, expected):
    global passed, total
    total += 1
    if actual == expected:
        print(f"PASS  {label}")
        passed += 1
    else:
        print(f"FAIL  {label}\n    expected: {expected!r}\n    actual:   {actual!r}")


def check_raises(label, fn, exc=RuntimeError):
    global passed, total
    total += 1
    try:
        fn()
    except exc:
        print(f"PASS  {label}")
        passed += 1
    except Exception as e:  # noqa: BLE001
        print(f"FAIL  {label}\n    wrong exception: {type(e).__name__}: {e}")
    else:
        print(f"FAIL  {label}\n    no exception raised")


# ── #149 regression ──────────────────────────────────────────────────────
# This exact shape aborted the host process before the fix: correct bytes
# produced, then a crash. If the binding regresses, the interpreter dies
# here rather than returning a wrong answer.
check(
    "#149: SELECT one column WHERE a different column filters rows",
    csvql.query(f"SELECT name FROM '{P}' WHERE salary > 100000"),
    [{"name": "Alice"}, {"name": "Carol"}],
)
check(
    "#149: same shape, zero rows match",
    csvql.query(f"SELECT name FROM '{P}' WHERE salary > 999999"),
    [],
)
check(
    "#149: same shape via query_csv",
    csvql.query_csv(f"SELECT name FROM '{P}' WHERE city = 'Austin'"),
    "name\nAlice\nCarol\n",
)

# ── API surface ──────────────────────────────────────────────────────────
check("query: all columns", len(csvql.query(f"SELECT * FROM '{P}'")), 3)
check(
    "query: GROUP BY + aggregate",
    csvql.query(f"SELECT city, COUNT(*) AS n FROM '{P}' GROUP BY city ORDER BY n DESC"),
    [{"city": "Austin", "n": 2}, {"city": "Boston", "n": 1}],
)
check(
    "query_csv: returns CSV text",
    csvql.query_csv(f"SELECT COUNT(*) AS n FROM '{P}'"),
    "n\n3\n",
)
check(
    "query_tuples: headers and rows",
    csvql.query_tuples(f"SELECT id FROM '{P}' ORDER BY id LIMIT 2"),
    (["id"], [("1",), ("2",)]),
)
check(
    "OFFSET (#143), both clause orders agree",
    (
        csvql.query_csv(f"SELECT id FROM '{P}' ORDER BY id LIMIT 1 OFFSET 1"),
        csvql.query_csv(f"SELECT id FROM '{P}' ORDER BY id OFFSET 1 LIMIT 1"),
    ),
    ("id\n2\n", "id\n2\n"),
)

# ── NULL handling (#147) ─────────────────────────────────────────────────
check(
    "#147: empty field is NULL for IS NULL",
    csvql.query(f"SELECT id FROM '{B}' WHERE name IS NULL"),
    [{"id": 2}],
)
check(
    "#147: LENGTH propagates NULL rather than returning 0",
    csvql.query_csv(f"SELECT id, LENGTH(name) FROM '{B}' WHERE name IS NULL"),
    "id,LENGTH(name)\n2,\n",
)

# ── Errors raise, never crash or silently return wrong data ──────────────
check_raises("bad SQL raises", lambda: csvql.query(f"SELEKT * FROM '{P}'"))
check_raises(
    "missing file raises",
    lambda: csvql.query(f"SELECT * FROM '{TMP / 'csvql_py_nope.csv'}'".replace("\\", "/")),
)
check_raises("unknown column raises", lambda: csvql.query(f"SELECT nocol FROM '{P}'"))

# ── Repeated calls: the #149 allocator fault only appeared once the
# allocator needed a fresh page, so a single call was not enough to see it.
try:
    for i in range(200):
        csvql.query(f"SELECT name FROM '{P}' WHERE salary > {100000 + i}")
    total += 1
    passed += 1
    print("PASS  200 consecutive queries (crosses allocator page boundaries)")
except Exception:  # noqa: BLE001
    total += 1
    print("FAIL  200 consecutive queries - failed partway")
    traceback.print_exc()

print(f"\n{passed}/{total} Python binding tests passed")

for f in (PEOPLE, BLANKS):
    try:
        os.unlink(f)
    except OSError:
        pass

sys.exit(0 if passed == total else 1)
