# SQL Reference

Full syntax reference and runnable examples for every SQL feature csvql supports. For a quick-glance summary, see the [README](README.md#sql-reference).

## Supported

| Feature       | Syntax                                                                  |
| ------------- | ----------------------------------------------------------------------- |
| **SELECT**    | `SELECT col1, col2` or `SELECT *`                                       |
| **AS alias**  | `SELECT expr AS alias` — rename any column or expression in output; `AS` is optional (`SELECT expr alias` also works) |
| **DISTINCT**  | `SELECT DISTINCT col1, col2` — deduplicates output rows                 |
| **FROM**      | `FROM 'file.csv'` or `FROM -` (stdin); optional table alias — `FROM 'file.csv' AS t` or bare `FROM 'file.csv' t`, reference columns as `t.col` |
| **WHERE**     | `=`, `!=`, `>`, `>=`, `<`, `<=` with auto numeric coercion              |
| **Modulo**    | `WHERE col % n = val` — integer remainder test in WHERE (also `MOD(col, n)` as a SELECT expression) |
| **LIKE**      | `WHERE col LIKE 'pattern'` — `%` any sequence, `_` any single char      |
| **ILIKE**     | `WHERE col ILIKE 'pattern'` — same as LIKE but case-insensitive         |
| **BETWEEN**   | `WHERE col BETWEEN low AND high` — inclusive numeric or string range    |
| **IN / NOT IN** | `WHERE col IN ('a', 'b', 'c')`, `WHERE col NOT IN ('a', 'b')` — membership test and its negation |
| **IN (subquery)** | `WHERE col IN (SELECT col FROM 'other.csv' WHERE ...)` — non-correlated, subquery must select exactly one column (also `NOT IN`); see [CORRECTNESS.md](CORRECTNESS.md#subqueries) |
| **IS NULL**   | `WHERE col IS NULL` / `WHERE col IS NOT NULL` — empty-field test; also usable as a SELECT expression (`SELECT col IS NOT NULL AS has_val`), returns `true`/`false` |
| **NOT**       | `WHERE NOT expr` — logical negation of any condition                    |
| **AND / OR**  | `WHERE cond1 AND cond2` / `WHERE cond1 OR cond2` — compound conditions  |
| **JOIN**      | `FROM 'a.csv' a [INNER] JOIN 'b.csv' b ON a.key = b.key`               |
| **GROUP BY**  | `GROUP BY col1` or `GROUP BY alias` — groups rows; accepts SELECT aliases |
| **COUNT**     | `COUNT(*)` or `COUNT(col)` — with or without `GROUP BY`                 |
| **SUM**       | `SUM(col)` or `SUM(CASE WHEN cond THEN n ELSE m END)` — conditional sum |
| **AVG**       | `AVG(col)` — full precision; with or without `GROUP BY`                 |
| **VARIANCE / STDDEV** | `VARIANCE(col)`, `STDDEV(col)` — sample variance / std deviation, N-1 denominator (aliases `VAR`, `VAR_SAMP`, `STDDEV_SAMP`). Population variants: `VAR_POP(col)`, `STDDEV_POP(col)` |
| **MEDIAN** | `MEDIAN(col)` — median of numeric values (mean of the two middles for an even count) |
| **GROUP_CONCAT** | `GROUP_CONCAT(col [, 'sep'])` — concatenate group values (default separator `,`; alias `STRING_AGG`) |
| **CASE WHEN** | `CASE WHEN col OP val THEN n ELSE m END` inside any aggregate function, or wrapping an aggregate in its own condition — `CASE WHEN AVG(col) > n THEN ... END` |
| **MIN / MAX** | `MIN(col)`, `MAX(col)` — with or without `GROUP BY`                     |
| **HAVING**    | `HAVING expr` — filter groups after aggregation (e.g. `HAVING COUNT(*) > 5`); `AND`/`OR` of multiple aggregates not in `SELECT` also works (e.g. `HAVING COUNT(*) > 5 AND MAX(col) > 100`) |
| **STRFTIME**  | `STRFTIME('%Y-%m', col)` — date bucketing in `SELECT` and `GROUP BY`    |
| **DATE_PART** | `DATE_PART('year', col)` — extract `year`/`month`/`day`/`hour`/`minute`/`second`; alias for `STRFTIME` in `SELECT` and `GROUP BY` |
| **UPPER / LOWER** | `SELECT UPPER(col), LOWER(col)` — case conversion; one level of nesting supported, e.g. `LOWER(TRIM(col))` |
| **TRIM**      | `SELECT TRIM(col)` — strip leading and trailing whitespace; nestable, e.g. `TRIM(UPPER(col))` |
| **REVERSE**   | `SELECT REVERSE(col)` — reverse a string by Unicode code point; nestable, e.g. `REVERSE(TRIM(col))` |
| **LENGTH**    | `SELECT LENGTH(col)` — byte length of the value; nestable, e.g. `LENGTH(TRIM(col))` |
| **CONCAT**    | `SELECT CONCAT(col1, '-', col2, ...)` — concatenate columns and/or string literals |
| **SUBSTR**    | `SELECT SUBSTR(col, start, len)` — substring (1-based, `len` optional)  |
| **REPLACE**   | `SELECT REPLACE(col, 'from', 'to')` — replace all occurrences of a substring |
| **SPLIT_PART**| `SELECT SPLIT_PART(col, 'delim', n)` — n-th field (1-based) after splitting on delim |
| **GREATEST / LEAST** | `SELECT GREATEST(a, b, ...)`, `LEAST(a, b, ...)` — row-wise max/min (numeric or lexicographic) |
| **ABS / SIGN / CEIL / FLOOR** | `SELECT ABS(col), SIGN(col), CEIL(col), FLOOR(col)` — numeric functions; `SIGN` returns `-1`, `0`, or `1` |
| **MOD**       | `SELECT MOD(col, n)` — modulo by a numeric literal                      |
| **ROUND**     | `SELECT ROUND(col)` — round to integer; `ROUND(col, n)` — round to `n` decimal places |
| **COALESCE**  | `SELECT COALESCE(col, 'default')` — replace empty/null with fallback    |
| **CAST**      | `SELECT CAST(col AS INTEGER/FLOAT/TEXT)` — type conversion              |
| **DATEDIFF**  | `DATEDIFF('unit', start_col, end_col)` — duration between two datetime columns. Units: `second`, `minute`, `hour`, `day`, `week`, `month` (≈30 days), `year` (≈365 days). Auto-detects ISO-8601, US (MM/DD/YYYY), EU (DD.MM.YYYY) and mixed formats in the same file |
| **DATEADD**   | `DATEADD('unit', amount, date_col)` — add/subtract interval from a datetime column. `amount` may be negative. Units: `second`, `minute`, `hour`, `day`, `week`, `month` (≈30 days), `year` (≈365 days). Returns `YYYY-MM-DD HH:MM:SS` |
| **ORDER BY**  | `ORDER BY col [ASC\|DESC]`, multi-column `ORDER BY col1 ASC, col2 DESC`, alias, positional (`ORDER BY 1`), or a column not in the `SELECT` list (sorts by the raw source/grouping column) |
| **LIMIT / OFFSET** | `LIMIT n [OFFSET m]` — return up to `n` rows after skipping the first `m` result rows |

## Known differences from DuckDB

Two intentional differences, found via differential testing against DuckDB (see the [blog post](https://melihbirim.github.io/csvql/blog/what-one-comment-found.html)) and kept as-is rather than "fixed", documented here so they don't surprise anyone migrating queries:

| Behavior | csvql | DuckDB |
| -------- | ----- | ------ |
| `LENGTH(col)` on a unicode string | Byte length (UTF-8 bytes) | Character count (codepoints) |
| Empty CSV field | Stays an empty string | Inferred as `NULL` |
| `DATEDIFF('hour'/'minute'/etc, a, b)` on a non-exact interval | Fractional (e.g. `8.5` hours) | Truncated to whole units (e.g. `8`) |

If you need character count instead of byte length, NULL instead of empty-string semantics, or truncated instead of fractional interval math, be aware the two engines diverge here rather than assume identical output.

## Known limitations

Not yet supported — these error clearly rather than silently returning wrong data, and are tracked as open issues:

| Limitation | Tracking |
| ---------- | -------- |
| Subqueries other than `col IN (SELECT ...)` / `col NOT IN (SELECT ...)` (non-correlated, single-column — see [CORRECTNESS.md](CORRECTNESS.md#subqueries)) | [#124](https://github.com/melihbirim/csvql/issues/124) |
| `UNION` / `INTERSECT` / `EXCEPT` | [#122](https://github.com/melihbirim/csvql/issues/122), [#127](https://github.com/melihbirim/csvql/issues/127) |
| Window functions (`RANK() OVER (...)`, etc.) | [#126](https://github.com/melihbirim/csvql/issues/126) |

## Aggregate Examples

```bash
# Scalar aggregates (whole table)
csvql "SELECT COUNT(*), SUM(salary), AVG(salary), MIN(age), MAX(age) FROM 'data.csv'"

# Grouped aggregates
csvql "SELECT department, COUNT(*), AVG(salary) FROM 'data.csv' GROUP BY department ORDER BY department"

# HAVING — filter groups after aggregation
csvql "SELECT department, SUM(salary) FROM 'data.csv' GROUP BY department HAVING SUM(salary) > 500000"
csvql "SELECT category, COUNT(*) FROM 'orders.csv' GROUP BY category HAVING COUNT(*) > 1000"

# CASE WHEN inside aggregates — conditional counting and summing
csvql "SELECT department, COUNT(*) AS total, SUM(CASE WHEN city = 'London' THEN 1 ELSE 0 END) AS london_count FROM 'data.csv' GROUP BY department"
csvql "SELECT SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active, SUM(CASE WHEN status = 'inactive' THEN 1 ELSE 0 END) AS inactive FROM 'data.csv'"
csvql "SELECT product, COUNT(*) AS total, SUM(CASE WHEN status = 'returned' THEN 1 ELSE 0 END) AS returns FROM 'orders.csv' GROUP BY product ORDER BY returns DESC"

# DISTINCT
csvql "SELECT DISTINCT city FROM 'data.csv' ORDER BY city"
csvql "SELECT DISTINCT city, department FROM 'data.csv'"

# DISTINCT with WHERE
csvql "SELECT DISTINCT department FROM 'data.csv' WHERE salary > 100000"
```

## Scalar Function Examples

Scalar functions transform column values row-by-row in `SELECT`. They can also be used in `GROUP BY` projections.

```bash
# String functions
csvql "SELECT UPPER(name), LOWER(city), TRIM(notes) FROM 'data.csv'"
csvql "SELECT name, LENGTH(name), SUBSTR(name, 1, 3) FROM 'data.csv'"

# Numeric functions
csvql "SELECT name, ABS(balance), SIGN(balance), CEIL(score), FLOOR(score) FROM 'data.csv'"
csvql "SELECT name, MOD(age, 10) AS age_decade FROM 'data.csv'"
csvql "SELECT name, ROUND(price) AS rounded, ROUND(price, 2) AS price_2dp FROM 'data.csv'"

# COALESCE — replace empty values with a fallback
csvql "SELECT name, COALESCE(email, 'unknown') FROM 'data.csv'"
csvql "SELECT COALESCE(phone, COALESCE(email, 'no contact')) FROM 'contacts.csv'"

# CAST — explicit type conversion
csvql "SELECT name, CAST(price AS INTEGER), CAST(id AS TEXT) FROM 'products.csv'"
csvql "SELECT CAST(year AS INTEGER) AS yr, SUM(revenue) FROM 'data.csv' GROUP BY yr"

# ILIKE — case-insensitive LIKE
csvql "SELECT * FROM 'data.csv' WHERE name ILIKE '%smith%'"
csvql "SELECT * FROM 'data.csv' WHERE email ILIKE '%@gmail.com'"

# Scalar functions work with GROUP BY
csvql "SELECT UPPER(city), COUNT(*) FROM 'data.csv' GROUP BY city"
csvql "SELECT LOWER(department) AS dept, AVG(salary) FROM 'data.csv' GROUP BY department"

# Mix scalars with AS aliases
csvql "SELECT UPPER(name) AS Name, CAST(salary AS INTEGER) AS Salary FROM 'data.csv' ORDER BY Salary DESC"

# CONCAT — combine columns and string literals
csvql "SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM 'data.csv'"
csvql "SELECT CONCAT(name, ' (', department, ')') FROM 'data.csv'"

# Nested functions — one level of composition
csvql "SELECT name, LOWER(TRIM(department)) FROM 'data.csv'"
csvql "SELECT department, LENGTH(TRIM(department)) FROM 'data.csv' GROUP BY department"

# IS NULL / IS NOT NULL as a SELECT expression
csvql "SELECT name, email IS NOT NULL AS has_email FROM 'data.csv'"

# Table alias outside JOIN
csvql "SELECT t.name, t.salary FROM 'data.csv' AS t WHERE t.salary > 100000"
```

## WHERE Filter Examples

```bash
# Comparison operators
csvql "SELECT name, salary FROM 'data.csv' WHERE salary > 80000"

# BETWEEN — inclusive range (numeric or string)
csvql "SELECT name, salary FROM 'data.csv' WHERE salary BETWEEN 50000 AND 80000"
csvql "SELECT * FROM 'orders.csv' WHERE order_date BETWEEN '2025-01-01' AND '2025-12-31'"

# IN — membership test
csvql "SELECT name FROM 'data.csv' WHERE city IN ('London', 'Paris', 'Berlin')"

# IS NULL / IS NOT NULL — test for missing (empty) fields
csvql "SELECT * FROM 'data.csv' WHERE email IS NULL"
csvql "SELECT * FROM 'data.csv' WHERE email IS NOT NULL"

# NOT — negate any condition
csvql "SELECT * FROM 'data.csv' WHERE NOT city IN ('London', 'Paris')"
csvql "SELECT * FROM 'data.csv' WHERE NOT salary BETWEEN 40000 AND 60000"

# AND / OR — compound conditions
csvql "SELECT * FROM 'data.csv' WHERE age > 30 AND department = 'Engineering'"
csvql "SELECT * FROM 'data.csv' WHERE city = 'London' OR city = 'Berlin'"
csvql "SELECT * FROM 'data.csv' WHERE status LIKE 'active%' AND salary > 50000"

# AS alias + ORDER BY alias or positional
csvql "SELECT name AS employee, salary AS pay FROM 'data.csv' ORDER BY pay DESC LIMIT 10"
csvql "SELECT city, COUNT(*) AS cnt FROM 'data.csv' GROUP BY city ORDER BY cnt DESC"
csvql "SELECT name, salary FROM 'data.csv' ORDER BY 2 DESC LIMIT 5"  # ORDER BY positional
```

## ORDER BY Examples

```bash
# Single-column ORDER BY
csvql "SELECT name, salary FROM 'data.csv' ORDER BY salary DESC LIMIT 10"
csvql "SELECT * FROM 'data.csv' ORDER BY name ASC"

# Multi-column ORDER BY — sort by primary key, then break ties with secondary key(s)
csvql "SELECT name, department, salary FROM 'data.csv' ORDER BY department ASC, salary DESC"
csvql "SELECT * FROM 'data.csv' ORDER BY city, age, name"
csvql "SELECT name, city, salary FROM 'employees.csv' WHERE salary > 80000 ORDER BY city ASC, salary DESC"

# Multi-column ORDER BY with GROUP BY results
csvql "SELECT department, AVG(salary) AS avg_sal FROM 'data.csv' GROUP BY department ORDER BY avg_sal DESC, department ASC"

# ORDER BY alias (resolved from SELECT clause)
csvql "SELECT name, salary AS pay FROM 'data.csv' ORDER BY pay DESC LIMIT 5"

# ORDER BY positional (1-based column index)
csvql "SELECT name, city, salary FROM 'data.csv' ORDER BY 3 DESC LIMIT 10"
```

## Time-Series and Date Bucketing

`STRFTIME('%fmt', column)` extracts or truncates date components for time-series aggregation.

Supported format specifiers: `%Y` (year), `%m` (month), `%d` (day), `%H` (hour), `%M` (minute), `%S` (second).

Input dates can be ISO-8601 date (`YYYY-MM-DD`) or datetime (`YYYY-MM-DD HH:MM:SS`).

```bash
# Monthly revenue trend — GROUP BY the full STRFTIME expression
csvql "SELECT STRFTIME('%Y-%m', order_date), COUNT(*), SUM(price) FROM 'orders.csv' GROUP BY STRFTIME('%Y-%m', order_date)"

# Same query using AS alias — GROUP BY the alias name
csvql "SELECT STRFTIME('%Y-%m', order_date) AS month, COUNT(*) AS orders, SUM(price) AS revenue FROM 'orders.csv' GROUP BY month ORDER BY month"

# Year-over-year breakdown by category
csvql "SELECT category, STRFTIME('%Y', order_date) AS yr, SUM(price) FROM 'orders.csv' GROUP BY category, yr"

# Date range filter + monthly bucketing + HAVING
csvql "SELECT STRFTIME('%Y-%m', order_date) AS month, COUNT(*), SUM(price) FROM 'orders.csv' WHERE order_date >= '2026-01-01' GROUP BY month HAVING COUNT(*) > 1000000"

# Daily active users
csvql "SELECT STRFTIME('%Y-%m-%d', event_date) AS day, COUNT(DISTINCT user_id) FROM 'events.csv' GROUP BY day ORDER BY day"
```

## DateTime and Duration Functions

`DATEDIFF` and `DATEADD` work with **four datetime formats** in the same CSV — no pre-processing needed:

| Format | Example |
|--------|---------|
| ISO-8601 (space) | `2026-01-15 09:30:00` |
| ISO-8601 (T) | `2026-01-16T10:00:00` |
| US (MM/DD/YYYY) | `01/15/2026 08:00:00` |
| EU (DD.MM.YYYY) | `15.01.2026 07:30:00` |

```bash
# Order workflow: time from order to pick (in minutes)
csvql "SELECT order_id, DATEDIFF('minute', ordered_at, picked_at) AS pick_min FROM 'orders.csv' WHERE picked_at != ''"

# Delivery time in days
csvql "SELECT order_id, DATEDIFF('day', shipped_at, delivered_at) AS ship_days FROM 'orders.csv' WHERE shipped_at != '' AND delivered_at != '' ORDER BY ship_days DESC"

# SLA check — select orders with pick time, then filter in your shell (DATEDIFF in WHERE not yet supported)
csvql "SELECT order_id, customer_name, DATEDIFF('hour', ordered_at, picked_at) AS hrs FROM 'orders.csv' WHERE picked_at != ''"

# Average processing time by status
csvql "SELECT status, AVG(DATEDIFF('minute', ordered_at, packaged_at)) AS avg_proc_min FROM 'orders.csv' WHERE packaged_at != '' GROUP BY status ORDER BY avg_proc_min"

# DATEADD — compute SLA deadlines
csvql "SELECT order_id, ordered_at, DATEADD('hour', 2, ordered_at) AS pick_deadline FROM 'orders.csv'"

# Estimated delivery date (ship date + 2 days)
csvql "SELECT order_id, shipped_at, DATEADD('day', 2, shipped_at) AS est_delivery FROM 'orders.csv' WHERE shipped_at != ''"

# Supported units for both functions: second, minute, hour, day, week, month (approx 30 days), year (approx 365 days)
csvql "SELECT order_id, DATEDIFF('second', ordered_at, picked_at) AS pick_secs FROM 'orders.csv'"
csvql "SELECT order_id, DATEADD('week', -1, delivered_at) AS sent_reminder FROM 'orders.csv'"
```

**Mixed formats work automatically** — a single CSV can have some dates as `2026-01-15 09:30:00`, others as `01/15/2026 08:00:00`, and `DATEDIFF` handles them all.

## JOIN Examples

```bash
# Basic INNER JOIN — select columns from both tables using aliases
csvql "SELECT e.name, d.dept_name FROM 'employees.csv' e INNER JOIN 'departments.csv' d ON e.dept_id = d.id"

# Bare JOIN (INNER is optional)
csvql "SELECT e.name, d.dept_name FROM 'employees.csv' e JOIN 'departments.csv' d ON e.dept_id = d.id"

# JOIN with WHERE — filter on joined columns
csvql "SELECT e.name, d.dept_name FROM 'employees.csv' e JOIN 'departments.csv' d ON e.dept_id = d.id WHERE d.dept_name = 'Engineering'"

# SELECT * on join returns all columns from both tables
csvql "SELECT * FROM 'orders.csv' o JOIN 'customers.csv' c ON o.customer_id = c.id"

# JOIN with LIMIT
csvql "SELECT e.name, d.dept_name FROM 'employees.csv' e JOIN 'departments.csv' d ON e.dept_id = d.id LIMIT 10"
```

**Notes:**
- Table aliases are required when using qualified column references (`alias.col`)
- Unqualified column names are resolved from the left table first, then the right
- The right table is fully loaded into memory (build side); the left table is streamed (probe side)

## Simple Mode

Positional args: `csvql <file> [columns] [filter] [limit] [sort]`

```bash
csvql data.csv "name,salary" "age>30" 10 "salary:desc"
```

See [SIMPLE_QUERY_LANGUAGE.md](SIMPLE_QUERY_LANGUAGE.md) for the full reference.
