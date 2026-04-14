"""
Basic csvql usage examples.

Run from the repo root:
    python3 python/examples/basic.py
"""

import sys
import os

# Allow running from repo root without installing the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import csvql

# ── 1. SELECT * with LIMIT ────────────────────────────────────────────────────
print("=== SELECT * LIMIT 3 ===")
rows = csvql.query("SELECT * FROM 'test.csv' LIMIT 3")
for row in rows:
    print(row)
# {'name': 'Alice', 'age': 30, 'city': 'NYC'}
# {'name': 'Bob',   'age': 25, 'city': 'SF'}
# ...

# ── 2. WHERE filter ───────────────────────────────────────────────────────────
print("\n=== WHERE age > 28 ===")
rows = csvql.query("SELECT name, age FROM 'test.csv' WHERE age > 28")
for row in rows:
    print(row)

# ── 3. GROUP BY + aggregate ───────────────────────────────────────────────────
print("\n=== GROUP BY city ===")
rows = csvql.query("SELECT city, COUNT(*) FROM 'test.csv' GROUP BY city")
for row in rows:
    print(f"{row['city']}: {row['COUNT(*)']} people")

# ── 4. ORDER BY ───────────────────────────────────────────────────────────────
print("\n=== ORDER BY age DESC ===")
rows = csvql.query("SELECT name, age FROM 'test.csv' ORDER BY age DESC")
for row in rows:
    print(row)

# ── 5. Raw CSV output ─────────────────────────────────────────────────────────
print("\n=== query_csv() ===")
csv_text = csvql.query_csv("SELECT name, city FROM 'test.csv' LIMIT 2")
print(repr(csv_text))

# ── 6. Accessing values ───────────────────────────────────────────────────────
print("\n=== dict access ===")
rows = csvql.query("SELECT * FROM 'test.csv'")
names = [row["name"] for row in rows]
print("names:", names)

# ── 7. Error handling ─────────────────────────────────────────────────────────
print("\n=== error handling ===")
try:
    csvql.query("SELECT * FROM 'nonexistent.csv'")
except RuntimeError as e:
    print(f"caught error: {e}")
