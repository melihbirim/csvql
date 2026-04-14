"""
Pandas integration example.

Install pandas first: pip install pandas

Run from the repo root:
    python3 python/examples/pandas_example.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import csvql

# ── query_df() returns a pandas DataFrame ────────────────────────────────────
df = csvql.query_df("SELECT city, COUNT(*) AS total FROM 'test.csv' GROUP BY city")
print(df)
print(df.dtypes)

# ── Pre-filter a large CSV before loading into pandas ─────────────────────────
# Instead of:
#   df = pd.read_csv("large_test.csv")   # loads EVERYTHING into RAM
#   df = df[df["quantity"] > 5]
#
# Do this — csvql reads only matching rows via mmap, pandas never sees the rest:
df_filtered = csvql.query_df(
    "SELECT * FROM 'large_test.csv' WHERE quantity > 5 LIMIT 100"
)
print(f"\nFiltered rows: {len(df_filtered)}")
print(df_filtered.head())

# ── Aggregate first, then plot ────────────────────────────────────────────────
summary = csvql.query_df(
    "SELECT category, SUM(price) AS revenue FROM 'large_test.csv' GROUP BY category"
)
print("\nRevenue by category:")
print(summary)

# Cast numeric columns after querying (CSV has no type info)
summary["revenue"] = summary["revenue"].astype(float)
print(summary.sort_values("revenue", ascending=False))
