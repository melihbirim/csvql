# Python

csvql ships as a native Python library — same SIMD engine, same performance, no subprocess.

```bash
pip install csvql-query
```

```python
import csvql

# List of dicts
rows = csvql.query("SELECT city, COUNT(*) as n FROM 'employees.csv' GROUP BY city")
# [{'city': 'Austin', 'n': '3'}, ...]

# Raw CSV string
csv_text = csvql.query_csv("SELECT * FROM 'employees.csv' WHERE salary > 100000")

# pandas DataFrame (requires pandas)
df = csvql.query_df("SELECT region, SUM(revenue) FROM 'sales.csv' GROUP BY region")

# Plain tuples — lowest overhead
headers, rows = csvql.query_tuples("SELECT name, age FROM 'employees.csv'")
```
