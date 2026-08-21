#!/usr/bin/env bash
# verify_correctness.sh — Compare csvql output against DuckDB for all major query shapes.
#
# Treats DuckDB as ground truth. Diffs every query result and reports PASS / FAIL.
# Results are sorted before diff so row-order ties (unstable sort) do not cause
# false failures — the set of result rows must match exactly.
#
# A check FAILs (never silently passes) when: either engine exits non-zero,
# or both sides return zero rows (an empty-vs-empty "match" is almost always
# two broken queries, not a real one).
#
# Usage:
#   ./bench/verify_correctness.sh [csv_file]
#
# Environment overrides:
#   DUCKDB_BIN   — path to duckdb binary  (default: duckdb in PATH)
#
# Exit code: 0 = all pass,  1 = one or more failures

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CSVQL="${SCRIPT_DIR}/zig-out/bin/csvql"
DUCKDB="${DUCKDB_BIN:-duckdb}"

CSV_ARG="${1:-}"
if [[ -n "$CSV_ARG" ]]; then
  CSV="$CSV_ARG"
else
  CSV="${SCRIPT_DIR}/large_test.csv"
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

RED='\033[0;31m'
GREEN='\033[0;32m'
BOLD='\033[1m'
DIM='\033[2m'
RESET='\033[0m'

PASS=0
FAIL=0
TOTAL=0

# ── dependency check ─────────────────────────────────────────────
if [[ ! -x "$CSVQL" ]]; then
  echo "csvql not found at $CSVQL — run: zig build -Doptimize=ReleaseFast"
  exit 1
fi
if ! command -v "$DUCKDB" >/dev/null 2>&1; then
  echo "duckdb not found in PATH (set DUCKDB_BIN=/path/to/duckdb)"
  exit 1
fi
if [[ ! -f "$CSV" ]]; then
  echo "CSV file not found: $CSV"
  exit 1
fi

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║            csvql vs DuckDB — Correctness Verification       ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo "  CSV:    $CSV"
echo "  csvql:  $("$CSVQL" --version 2>/dev/null || echo "unknown")"
echo "  DuckDB: $("$DUCKDB" --version 2>/dev/null | head -1 || echo "unknown")"
echo ""

# ── helper ───────────────────────────────────────────────────────
# check <label> <csvql_sql> <duckdb_sql>
# Runs both queries, strips csvql header, sorts both outputs, then diffs.
# For float results sets NUMERIC_COMPARE=1 to allow rounding tolerance.
check() {
  local label="$1"
  local csvql_sql="$2"
  local duck_sql="$3"
  local sort_key="${4:--k1}"    # default: sort on first field
  TOTAL=$((TOTAL + 1))

  local out_csvql="$TMP/csvql_${TOTAL}.txt"
  local out_duck="$TMP/duck_${TOTAL}.txt"
  local err_csvql="$TMP/csvql_err_${TOTAL}.txt"
  local err_duck="$TMP/duck_err_${TOTAL}.txt"

  # Run queries. Exit codes come from PIPESTATUS[0] (the query engine, not
  # `tail`/`sort`) so a crash or execution error is never masked into an
  # empty-vs-empty false pass.
  "$CSVQL" "$csvql_sql" 2>"$err_csvql" | tail -n +2 | sort $sort_key > "$out_csvql"
  local csvql_rc=${PIPESTATUS[0]}
  "$DUCKDB" -csv -noheader -c "$duck_sql" 2>"$err_duck" | sort $sort_key > "$out_duck"
  local duck_rc=${PIPESTATUS[0]}

  if [[ $csvql_rc -ne 0 ]]; then
    printf "  ${RED}FAIL${RESET}  %s (csvql exited %d)\n" "$label" "$csvql_rc"
    FAIL=$((FAIL + 1))
    sed 's/^/        /' "$err_csvql"
    echo ""
    return
  fi
  if [[ $duck_rc -ne 0 ]]; then
    printf "  ${RED}FAIL${RESET}  %s (duckdb exited %d — check the reference query, not csvql)\n" "$label" "$duck_rc"
    FAIL=$((FAIL + 1))
    sed 's/^/        /' "$err_duck"
    echo ""
    return
  fi
  if [[ ! -s "$out_duck" && ! -s "$out_csvql" ]]; then
    printf "  ${RED}FAIL${RESET}  %s (both sides returned zero rows — likely a broken query, not a real match)\n" "$label"
    FAIL=$((FAIL + 1))
    echo ""
    return
  fi

  # Compare
  if diff -q "$out_duck" "$out_csvql" > /dev/null 2>&1; then
    printf "  ${GREEN}PASS${RESET}  %s\n" "$label"
    PASS=$((PASS + 1))
  else
    printf "  ${RED}FAIL${RESET}  %s\n" "$label"
    FAIL=$((FAIL + 1))
    # Show first 10 differing lines
    diff "$out_duck" "$out_csvql" | head -25 | sed 's/^/        /' | \
      sed "s/^        </        ${DIM}duck  <${RESET}/" | \
      sed "s/^        >/        ${BOLD}csvql >${RESET}/"
    echo ""
  fi
}

# check_approx: for float aggregates — rounds both sides to N decimal places before diff
check_approx() {
  local label="$1"
  local csvql_sql="$2"
  local duck_sql="$3"
  local decimals="${4:-4}"
  TOTAL=$((TOTAL + 1))

  local out_csvql="$TMP/csvql_${TOTAL}.txt"
  local out_duck="$TMP/duck_${TOTAL}.txt"
  local err_csvql="$TMP/csvql_err_${TOTAL}.txt"
  local err_duck="$TMP/duck_err_${TOTAL}.txt"

  "$CSVQL" "$csvql_sql" 2>"$err_csvql" | tail -n +2 | \
    awk -v d="$decimals" '{
      for(i=1;i<=NF;i++) {
        if ($i ~ /^-?[0-9]+(\.[0-9]+)?$/){
          printf "%.*f", d, $i+0
        } else printf "%s", $i
        printf (i<NF?",":"\n")
      }
    }' FS=',' OFS=',' | sort > "$out_csvql"
  local csvql_rc=${PIPESTATUS[0]}

  "$DUCKDB" -csv -noheader -c "$duck_sql" 2>"$err_duck" | \
    awk -v d="$decimals" '{
      for(i=1;i<=NF;i++) {
        if ($i ~ /^-?[0-9]+(\.[0-9]+)?$/){
          printf "%.*f", d, $i+0
        } else printf "%s", $i
        printf (i<NF?",":"\n")
      }
    }' FS=',' OFS=',' | sort > "$out_duck"
  local duck_rc=${PIPESTATUS[0]}

  if [[ $csvql_rc -ne 0 ]]; then
    printf "  ${RED}FAIL${RESET}  %s (csvql exited %d)\n" "$label" "$csvql_rc"
    FAIL=$((FAIL + 1))
    sed 's/^/        /' "$err_csvql"
    echo ""
    return
  fi
  if [[ $duck_rc -ne 0 ]]; then
    printf "  ${RED}FAIL${RESET}  %s (duckdb exited %d — check the reference query, not csvql)\n" "$label" "$duck_rc"
    FAIL=$((FAIL + 1))
    sed 's/^/        /' "$err_duck"
    echo ""
    return
  fi
  if [[ ! -s "$out_duck" && ! -s "$out_csvql" ]]; then
    printf "  ${RED}FAIL${RESET}  %s (both sides returned zero rows — likely a broken query, not a real match)\n" "$label"
    FAIL=$((FAIL + 1))
    echo ""
    return
  fi

  if diff -q "$out_duck" "$out_csvql" > /dev/null 2>&1; then
    printf "  ${GREEN}PASS${RESET}  %s\n" "$label"
    PASS=$((PASS + 1))
  else
    printf "  ${RED}FAIL${RESET}  %s\n" "$label"
    FAIL=$((FAIL + 1))
    diff "$out_duck" "$out_csvql" | head -25 | sed 's/^/        /'
    echo ""
  fi
}

# ════════════════════════════════════════════════════════════════
echo "── Scalar Aggregates ───────────────────────────────────────"

check_approx \
  "COUNT(*)" \
  "SELECT COUNT(*) FROM '$CSV'" \
  "SELECT COUNT(*) FROM read_csv_auto('$CSV')"

check_approx \
  "SUM(salary)" \
  "SELECT SUM(salary) FROM '$CSV'" \
  "SELECT SUM(salary) FROM read_csv_auto('$CSV')"

check_approx \
  "AVG(salary)" \
  "SELECT AVG(salary) FROM '$CSV'" \
  "SELECT AVG(salary) FROM read_csv_auto('$CSV')"

check_approx \
  "MIN(age), MAX(age)" \
  "SELECT MIN(age), MAX(age) FROM '$CSV'" \
  "SELECT MIN(age), MAX(age) FROM read_csv_auto('$CSV')"

check_approx \
  "COUNT(*) WHERE age > 30" \
  "SELECT COUNT(*) FROM '$CSV' WHERE age > 30" \
  "SELECT COUNT(*) FROM read_csv_auto('$CSV') WHERE age > 30"

check_approx \
  "SUM(salary) WHERE salary > 100000" \
  "SELECT SUM(salary) FROM '$CSV' WHERE salary > 100000" \
  "SELECT SUM(salary) FROM read_csv_auto('$CSV') WHERE salary > 100000"

check_approx \
  "VARIANCE(salary) / STDDEV(salary)" \
  "SELECT VARIANCE(salary), STDDEV(salary) FROM '$CSV'" \
  "SELECT VAR_SAMP(salary), STDDEV_SAMP(salary) FROM read_csv_auto('$CSV')" \
  "1"

check_approx \
  "MEDIAN(salary)" \
  "SELECT MEDIAN(salary) FROM '$CSV'" \
  "SELECT MEDIAN(salary) FROM read_csv_auto('$CSV')"

GC="$TMP/groupconcat.csv"
cat > "$GC" <<'EOF'
dept,city
HR,Austin
Eng,Boston
HR,Denver
EOF
check \
  "GROUP_CONCAT by department (small fixture — insertion-order dependent, avoids large-file parallel-scan reordering)" \
  "SELECT dept, GROUP_CONCAT(city) FROM '$GC' GROUP BY dept ORDER BY dept" \
  "SELECT dept, string_agg(city, ',') FROM read_csv_auto('$GC') GROUP BY dept ORDER BY dept"

# ════════════════════════════════════════════════════════════════
echo ""
echo "── GROUP BY ────────────────────────────────────────────────"

check \
  "GROUP BY department" \
  "SELECT department FROM '$CSV' GROUP BY department ORDER BY department" \
  "SELECT department FROM read_csv_auto('$CSV') GROUP BY department ORDER BY department"

check \
  "GROUP BY city" \
  "SELECT city FROM '$CSV' GROUP BY city ORDER BY city" \
  "SELECT city FROM read_csv_auto('$CSV') GROUP BY city ORDER BY city"

check \
  "GROUP BY department + COUNT(*)" \
  "SELECT department, COUNT(*) FROM '$CSV' GROUP BY department ORDER BY department" \
  "SELECT department, COUNT(*) FROM read_csv_auto('$CSV') GROUP BY department ORDER BY department"

check \
  "GROUP BY city + COUNT(*) + SUM(salary)" \
  "SELECT city, COUNT(*), SUM(salary) FROM '$CSV' GROUP BY city ORDER BY city" \
  "SELECT city, COUNT(*), SUM(salary) FROM read_csv_auto('$CSV') GROUP BY city ORDER BY city"

check_approx \
  "GROUP BY department + AVG(salary)" \
  "SELECT department, AVG(salary) FROM '$CSV' GROUP BY department ORDER BY department" \
  "SELECT department, ROUND(AVG(salary),4) FROM read_csv_auto('$CSV') GROUP BY department ORDER BY department"

check \
  "GROUP BY department + MIN/MAX(age)" \
  "SELECT department, MIN(age), MAX(age) FROM '$CSV' GROUP BY department ORDER BY department" \
  "SELECT department, MIN(age), MAX(age) FROM read_csv_auto('$CSV') GROUP BY department ORDER BY department"

check \
  "GROUP BY name,department (~48 groups)" \
  "SELECT name, department, COUNT(*) FROM '$CSV' GROUP BY name, department ORDER BY name, department" \
  "SELECT name, department, COUNT(*) FROM read_csv_auto('$CSV') GROUP BY name, department ORDER BY name, department"

check \
  "WHERE salary>100000 + GROUP BY department" \
  "SELECT department, COUNT(*) FROM '$CSV' WHERE salary > 100000 GROUP BY department ORDER BY department" \
  "SELECT department, COUNT(*) FROM read_csv_auto('$CSV') WHERE salary > 100000 GROUP BY department ORDER BY department"

check \
  "CASE WHEN with aggregate in condition (#113)" \
  "SELECT department, CASE WHEN AVG(salary) > 80000 THEN 'high' ELSE 'low' END AS tier FROM '$CSV' GROUP BY department ORDER BY department" \
  "SELECT department, CASE WHEN AVG(salary) > 80000 THEN 'high' ELSE 'low' END AS tier FROM read_csv_auto('$CSV') GROUP BY department ORDER BY department"

check \
  "HAVING AND of two aggregates not in SELECT (#118)" \
  "SELECT department, COUNT(*) FROM '$CSV' GROUP BY department HAVING COUNT(*) > 5 AND MAX(salary) > 80000 ORDER BY department" \
  "SELECT department, COUNT(*) FROM read_csv_auto('$CSV') GROUP BY department HAVING COUNT(*) > 5 AND MAX(salary) > 80000 ORDER BY department"

check \
  "Implicit alias, no AS keyword (#116)" \
  "SELECT department d, COUNT(*) c FROM '$CSV' GROUP BY department ORDER BY department" \
  "SELECT department AS d, COUNT(*) AS c FROM read_csv_auto('$CSV') GROUP BY department ORDER BY department"

# ════════════════════════════════════════════════════════════════
echo ""
echo "── DISTINCT ────────────────────────────────────────────────"

check \
  "DISTINCT name" \
  "SELECT DISTINCT name FROM '$CSV' ORDER BY name" \
  "SELECT DISTINCT name FROM read_csv_auto('$CSV') ORDER BY name"

check \
  "DISTINCT city" \
  "SELECT DISTINCT city FROM '$CSV' ORDER BY city" \
  "SELECT DISTINCT city FROM read_csv_auto('$CSV') ORDER BY city"

check \
  "DISTINCT department" \
  "SELECT DISTINCT department FROM '$CSV' ORDER BY department" \
  "SELECT DISTINCT department FROM read_csv_auto('$CSV') ORDER BY department"

check \
  "DISTINCT city,department" \
  "SELECT DISTINCT city, department FROM '$CSV' ORDER BY city, department" \
  "SELECT DISTINCT city, department FROM read_csv_auto('$CSV') ORDER BY city, department"

check \
  "DISTINCT department WHERE salary > 100000" \
  "SELECT DISTINCT department FROM '$CSV' WHERE salary > 100000 ORDER BY department" \
  "SELECT DISTINCT department FROM read_csv_auto('$CSV') WHERE salary > 100000 ORDER BY department"

# ════════════════════════════════════════════════════════════════
echo ""
echo "── WHERE Filters ───────────────────────────────────────────"

check \
  "WHERE age = 30 (exact numeric)" \
  "SELECT name, age FROM '$CSV' WHERE age = 30 ORDER BY name, age" \
  "SELECT name, age FROM read_csv_auto('$CSV') WHERE age = 30 ORDER BY name, age"

check \
  "WHERE salary >= 100000 AND salary <= 110000" \
  "SELECT name, salary FROM '$CSV' WHERE salary >= 100000 AND salary <= 110000 ORDER BY salary, name" \
  "SELECT name, salary FROM read_csv_auto('$CSV') WHERE salary >= 100000 AND salary <= 110000 ORDER BY salary, name"

check \
  "WHERE city = 'NYC'" \
  "SELECT name, city FROM '$CSV' WHERE city = 'NYC' ORDER BY name" \
  "SELECT name, city FROM read_csv_auto('$CSV') WHERE city = 'NYC' ORDER BY name"

check \
  "WHERE id % 2 = 0 (modulo, #119)" \
  "SELECT name, id FROM '$CSV' WHERE id % 2 = 0 ORDER BY id, name" \
  "SELECT name, id FROM read_csv_auto('$CSV') WHERE id % 2 = 0 ORDER BY id, name"

check \
  "WHERE department NOT IN ('Sales') (#115)" \
  "SELECT name FROM '$CSV' WHERE department NOT IN ('Sales') ORDER BY name" \
  "SELECT name FROM read_csv_auto('$CSV') WHERE department NOT IN ('Sales') ORDER BY name"

# ════════════════════════════════════════════════════════════════
echo ""
echo "── LIKE Pattern Matching ───────────────────────────────────"

check \
  "WHERE name LIKE 'A%' (prefix)" \
  "SELECT name FROM '$CSV' WHERE name LIKE 'A%' ORDER BY name" \
  "SELECT name FROM read_csv_auto('$CSV') WHERE name LIKE 'A%' ORDER BY name"

check \
  "WHERE name LIKE '%e' (suffix)" \
  "SELECT name FROM '$CSV' WHERE name LIKE '%e' ORDER BY name" \
  "SELECT name FROM read_csv_auto('$CSV') WHERE name LIKE '%e' ORDER BY name"

check \
  "WHERE city LIKE '%o%' (contains)" \
  "SELECT DISTINCT city FROM '$CSV' WHERE city LIKE '%o%' ORDER BY city" \
  "SELECT DISTINCT city FROM read_csv_auto('$CSV') WHERE city LIKE '%o%' ORDER BY city"

check \
  "WHERE city ILIKE 'nyc' (case-insensitive)" \
  "SELECT DISTINCT city FROM '$CSV' WHERE city ILIKE 'nyc'" \
  "SELECT DISTINCT city FROM read_csv_auto('$CSV') WHERE city ILIKE 'nyc'"

check \
  "WHERE age BETWEEN 25 AND 30" \
  "SELECT name, age FROM '$CSV' WHERE age BETWEEN 25 AND 30 ORDER BY name, age" \
  "SELECT name, age FROM read_csv_auto('$CSV') WHERE age BETWEEN 25 AND 30 ORDER BY name, age"

# ════════════════════════════════════════════════════════════════
echo ""
echo "── ORDER BY ────────────────────────────────────────────────"

check \
  "ORDER BY name (stable unique)" \
  "SELECT DISTINCT name FROM '$CSV' ORDER BY name" \
  "SELECT DISTINCT name FROM read_csv_auto('$CSV') ORDER BY name"

check \
  "ORDER BY salary DESC LIMIT 5 (unique keys)" \
  "SELECT city, MAX(salary) as ms FROM '$CSV' GROUP BY city ORDER BY ms DESC" \
  "SELECT city, MAX(salary) as ms FROM read_csv_auto('$CSV') GROUP BY city ORDER BY ms DESC"

check \
  "GROUP BY dept ORDER BY COUNT DESC" \
  "SELECT department, COUNT(*) FROM '$CSV' GROUP BY department ORDER BY COUNT(*) DESC" \
  "SELECT department, COUNT(*) FROM read_csv_auto('$CSV') GROUP BY department ORDER BY count(*) DESC"

check \
  "ORDER BY positional (ORDER BY 1)" \
  "SELECT department, COUNT(*) FROM '$CSV' GROUP BY department ORDER BY 1" \
  "SELECT department, COUNT(*) FROM read_csv_auto('$CSV') GROUP BY department ORDER BY 1"

# ════════════════════════════════════════════════════════════════
echo ""
echo "── SELECT * / Projection ───────────────────────────────────"

check \
  "SELECT * LIMIT 20 (by sorted key)" \
  "SELECT id, name, age, city, salary, department FROM '$CSV' ORDER BY id LIMIT 20" \
  "SELECT id, name, age, city, salary, department FROM read_csv_auto('$CSV') ORDER BY id LIMIT 20"

check \
  "Column projection + WHERE" \
  "SELECT name, department FROM '$CSV' WHERE age < 25 ORDER BY name, department" \
  "SELECT name, department FROM read_csv_auto('$CSV') WHERE age < 25 ORDER BY name, department"

check \
  "IS NULL / IS NOT NULL in SELECT (#114)" \
  "SELECT id, name, department IS NOT NULL AS has_dept FROM '$CSV' WHERE id <= 20 ORDER BY id" \
  "SELECT id, name, department IS NOT NULL AS has_dept FROM read_csv_auto('$CSV') WHERE id <= 20 ORDER BY id"

check \
  "CONCAT with column and literal args (#125)" \
  "SELECT id, name, CONCAT(name, '-', department) AS tag FROM '$CSV' WHERE id <= 20 ORDER BY id" \
  "SELECT id, name, CONCAT(name, '-', department) AS tag FROM read_csv_auto('$CSV') WHERE id <= 20 ORDER BY id"

check \
  "Nested scalar functions: LOWER(TRIM(col)) (#120)" \
  "SELECT id, name, LOWER(TRIM(department)) AS d FROM '$CSV' WHERE id <= 20 ORDER BY id" \
  "SELECT id, name, LOWER(TRIM(department)) AS d FROM read_csv_auto('$CSV') WHERE id <= 20 ORDER BY id"

check \
  "Nested scalar functions in GROUP BY key (#120)" \
  "SELECT department, LOWER(TRIM(department)) AS d FROM '$CSV' GROUP BY department ORDER BY department" \
  "SELECT department, LOWER(TRIM(department)) AS d FROM read_csv_auto('$CSV') GROUP BY department ORDER BY department"

check \
  "Table alias outside JOIN, AS keyword (#121)" \
  "SELECT t.name, t.department FROM '$CSV' AS t WHERE t.salary > 100000 ORDER BY t.name, t.department" \
  "SELECT t.name, t.department FROM read_csv_auto('$CSV') AS t WHERE t.salary > 100000 ORDER BY t.name, t.department"

check \
  "Table alias outside JOIN, bare (no AS) (#121)" \
  "SELECT t.name FROM '$CSV' t WHERE t.department = 'Sales' ORDER BY t.name" \
  "SELECT t.name FROM read_csv_auto('$CSV') t WHERE t.department = 'Sales' ORDER BY t.name"

# ════════════════════════════════════════════════════════════════
echo ""
echo "── Scalar Functions ──────────────────────────────────────────"

check \
  "REPLACE(city, 'o', '0')" \
  "SELECT DISTINCT REPLACE(city, 'o', '0') FROM '$CSV' ORDER BY 1" \
  "SELECT DISTINCT REPLACE(city, 'o', '0') FROM read_csv_auto('$CSV') ORDER BY 1"

check \
  "SPLIT_PART(department, 'a', 1)" \
  "SELECT DISTINCT SPLIT_PART(department, 'a', 1) FROM '$CSV' ORDER BY 1" \
  "SELECT DISTINCT SPLIT_PART(department, 'a', 1) FROM read_csv_auto('$CSV') ORDER BY 1"

check \
  "GREATEST/LEAST with column and numeric literal args (#134)" \
  "SELECT id, name, GREATEST(age, salary), LEAST(age, 30) FROM '$CSV' WHERE id <= 20 ORDER BY id" \
  "SELECT id, name, GREATEST(age, salary), LEAST(age, 30) FROM read_csv_auto('$CSV') WHERE id <= 20 ORDER BY id"

check \
  "ABS/CEIL/FLOOR — bare column args only" \
  "SELECT id, ABS(age), CEIL(salary), FLOOR(salary) FROM '$CSV' WHERE id <= 20 ORDER BY id" \
  "SELECT id, ABS(age), CEIL(salary), FLOOR(salary) FROM read_csv_auto('$CSV') WHERE id <= 20 ORDER BY id"

check \
  "MOD(age, 3) as a SELECT expression" \
  "SELECT id, MOD(age, 3) FROM '$CSV' WHERE id <= 20 ORDER BY id" \
  "SELECT id, MOD(age, 3) FROM read_csv_auto('$CSV') WHERE id <= 20 ORDER BY id"

check \
  "COALESCE(department, 'Unknown')" \
  "SELECT id, COALESCE(department, 'Unknown') FROM '$CSV' WHERE id <= 20 ORDER BY id" \
  "SELECT id, COALESCE(department, 'Unknown') FROM read_csv_auto('$CSV') WHERE id <= 20 ORDER BY id"

check \
  "CAST(salary AS INTEGER/TEXT)" \
  "SELECT id, CAST(salary AS INTEGER), CAST(salary AS TEXT) FROM '$CSV' WHERE id <= 20 ORDER BY id" \
  "SELECT id, CAST(salary AS INTEGER), CAST(salary AS VARCHAR) FROM read_csv_auto('$CSV') WHERE id <= 20 ORDER BY id"

check_approx \
  "CAST(salary AS FLOAT)" \
  "SELECT id, CAST(salary AS FLOAT) FROM '$CSV' WHERE id <= 20 ORDER BY id" \
  "SELECT id, CAST(salary AS FLOAT) FROM read_csv_auto('$CSV') WHERE id <= 20 ORDER BY id" \
  "1"

# ════════════════════════════════════════════════════════════════
echo ""
echo "── Date/Time Functions ─────────────────────────────────────"

DATES="$TMP/dates.csv"
cat > "$DATES" <<'EOF'
id,event,started_at,ended_at
1,launch,2024-01-15 09:00:00,2024-01-15 17:30:00
2,migration,2024-02-01 00:00:00,2024-02-03 12:00:00
3,rollout,2024-03-10 08:15:00,2024-03-10 08:45:00
4,incident,2024-06-20 22:00:00,2024-06-21 02:00:00
5,review,2024-07-04 10:00:00,2024-07-04 11:30:00
6,maintenance,2024-08-01 00:00:00,2024-08-03 00:00:00
EOF

check \
  "DATEDIFF('day', started_at, ended_at) — exact-day data avoids the fractional-vs-truncated difference (see README Known differences)" \
  "SELECT id, DATEDIFF('day', started_at, ended_at) FROM '$DATES' WHERE id = 6 ORDER BY id" \
  "SELECT id, DATEDIFF('day', started_at, ended_at) FROM read_csv_auto('$DATES') WHERE id = 6 ORDER BY id"

check \
  "DATEADD('day', 7, started_at)" \
  "SELECT id, DATEADD('day', 7, started_at) FROM '$DATES' ORDER BY id" \
  "SELECT id, started_at + INTERVAL 7 DAY FROM read_csv_auto('$DATES') ORDER BY id"

check \
  "STRFTIME('%Y-%m', started_at) date bucketing (GROUP BY path — plain SELECT tracked in #133)" \
  "SELECT STRFTIME('%Y-%m', started_at), COUNT(*) FROM '$DATES' GROUP BY STRFTIME('%Y-%m', started_at) ORDER BY 1" \
  "SELECT strftime(started_at, '%Y-%m'), COUNT(*) FROM read_csv_auto('$DATES') GROUP BY strftime(started_at, '%Y-%m') ORDER BY 1"

# ════════════════════════════════════════════════════════════════
echo ""
echo "── JOIN (hash-join, exercises parallel probe on large base) ─"

# Lookup tables (small right side)
DEPTS="$TMP/depts.csv"
cat > "$DEPTS" <<'EOF'
dept_name,region,budget_code
Engineering,West,ENG-001
Finance,East,FIN-002
HR,Central,HR-003
Marketing,East,MKT-004
Operations,West,OPS-005
Sales,Central,SAL-006
EOF

CITIES="$TMP/cities.csv"
cat > "$CITIES" <<'EOF'
city,state,timezone
Austin,TX,CDT
Boston,MA,EDT
Chicago,IL,CDT
Denver,CO,MDT
LA,CA,PDT
NYC,NY,EDT
SF,CA,PDT
Seattle,WA,PDT
EOF

BONUS="$TMP/bonus_50k.csv"
awk -F, 'NR==1{print "emp_id,bonus_pct"} NR>1&&NR<=50001{printf "%s,%.2f\n",$1,($5*0.1)}' "$CSV" > "$BONUS"

REGIONS="$TMP/regions.csv"
cat > "$REGIONS" <<'EOF'
region_name,continent
West,North America
East,North America
Central,North America
EOF

# Join output order is undefined; pass "" to sort whole lines before diff.
check \
  "JOIN departments" \
  "SELECT e.name, e.salary, d.region FROM '$CSV' e INNER JOIN '$DEPTS' d ON e.department = d.dept_name" \
  "SELECT e.name, e.salary, d.region FROM read_csv_auto('$CSV') AS e JOIN read_csv_auto('$DEPTS') AS d ON e.department = d.dept_name" \
  ""

check \
  "JOIN departments + WHERE region" \
  "SELECT e.name, e.salary, d.region FROM '$CSV' e INNER JOIN '$DEPTS' d ON e.department = d.dept_name WHERE d.region = 'West'" \
  "SELECT e.name, e.salary, d.region FROM read_csv_auto('$CSV') AS e JOIN read_csv_auto('$DEPTS') AS d ON e.department = d.dept_name WHERE d.region = 'West'" \
  ""

check \
  "JOIN SELECT *" \
  "SELECT * FROM '$CSV' e INNER JOIN '$DEPTS' d ON e.department = d.dept_name" \
  "SELECT * FROM read_csv_auto('$CSV') AS e JOIN read_csv_auto('$DEPTS') AS d ON e.department = d.dept_name" \
  ""

check \
  "JOIN cities" \
  "SELECT e.name, e.city, c.state, c.timezone FROM '$CSV' e INNER JOIN '$CITIES' c ON e.city = c.city" \
  "SELECT e.name, e.city, c.state, c.timezone FROM read_csv_auto('$CSV') AS e JOIN read_csv_auto('$CITIES') AS c ON e.city = c.city" \
  ""

check_approx \
  "JOIN 50K right on numeric id" \
  "SELECT e.name, e.salary, b.bonus_pct FROM '$CSV' e INNER JOIN '$BONUS' b ON e.id = b.emp_id" \
  "SELECT e.name, e.salary, b.bonus_pct FROM read_csv_auto('$CSV') AS e JOIN read_csv_auto('$BONUS') AS b ON e.id::TEXT = b.emp_id::TEXT"

check \
  "3-table JOIN departments then regions" \
  "SELECT e.name, e.salary, d.region, r.continent FROM '$CSV' e INNER JOIN '$DEPTS' d ON e.department = d.dept_name INNER JOIN '$REGIONS' r ON d.region = r.region_name" \
  "SELECT e.name, e.salary, d.region, r.continent FROM read_csv_auto('$CSV') AS e JOIN read_csv_auto('$DEPTS') AS d ON e.department = d.dept_name JOIN read_csv_auto('$REGIONS') AS r ON d.region = r.region_name" \
  ""

# ════════════════════════════════════════════════════════════════
echo ""
echo "── Adversarial CSV fixtures ─────────────────────────────────"

EDGE="$TMP/edge.csv"
cat > "$EDGE" <<'EOF'
id,name,note,amount,tags
1,"Alice, Jr.","line1
line2",100.5,a
2,Bob,plain note,2500.25,b
3,NoComma,tab and space  ok,0,c
4,Zero,ok,-0,d
5,"Ünïcödé","üñîçødé test",42,e
EOF

check \
  "quoted field with embedded comma" \
  "SELECT id, name FROM '$EDGE' WHERE id = 1" \
  "SELECT id, name FROM read_csv_auto('$EDGE') WHERE id = 1"

check \
  "quoted field with embedded newline" \
  "SELECT id, note FROM '$EDGE' WHERE id = 1" \
  "SELECT id, note FROM read_csv_auto('$EDGE') WHERE id = 1"

check \
  "unicode field content + LIKE" \
  "SELECT id FROM '$EDGE' WHERE name LIKE '%cöd%'" \
  "SELECT id FROM read_csv_auto('$EDGE') WHERE name LIKE '%cöd%'"

check_approx \
  "numeric column, no embedded newlines" \
  "SELECT SUM(amount) FROM '$EDGE' WHERE id != 1" \
  "SELECT SUM(amount) FROM read_csv_auto('$EDGE') WHERE id != 1"

# Regression checks for previously-fixed silent-wrong-answer bugs. These stay
# in the running suite (not excluded) so a regression fails CI instead of
# silently reappearing. #91 (LENGTH byte-vs-char count, empty-string-vs-NULL)
# is a deliberate, documented difference from DuckDB, not a bug — it is
# intentionally excluded rather than asserted to match.

ESCQ="$TMP/escq.csv"
cat > "$ESCQ" <<'EOF'
id,note
1,"She said ""hi"" today"
2,plain
EOF
check \
  "escaped double-quote unescaping (\"\" -> \") in a quoted field (#89)" \
  "SELECT id, note FROM '$ESCQ' WHERE id = 1" \
  "SELECT id, note FROM read_csv_auto('$ESCQ') WHERE id = 1"

SCINOT="$TMP/scinot.csv"
cat > "$SCINOT" <<'EOF'
id,amount
1,2.5e3
2,100
EOF
check_approx \
  "scientific notation (2.5e3) recognized as numeric (#90)" \
  "SELECT SUM(amount) FROM '$SCINOT'" \
  "SELECT SUM(amount) FROM read_csv_auto('$SCINOT')"

check_approx \
  "SUM() across a row with an embedded-newline quoted field before it (#92)" \
  "SELECT SUM(amount) FROM '$EDGE'" \
  "SELECT SUM(amount) FROM read_csv_auto('$EDGE')"

# ════════════════════════════════════════════════════════════════
# Large-file, quote-heavy regression check (#139): the parallel scalar-agg
# path's per-2MB-IO-buffer scan lost a record's quote state across reads,
# so a numeric WHERE filter on a large quoted CSV could silently undercount
# even though a bare COUNT(*) still came out right. Only ever reproduced
# with a real multi-GB quoted file, not a synthetic one — gated on the
# gitignored taxi fixture (bench/bench_taxi.sh downloads it) so CI stays
# green without that multi-GB download.
TAXI_CSV="${SCRIPT_DIR}/bench/.taxi-data/trips.csv"
if [[ -f "$TAXI_CSV" ]]; then
  echo ""
  echo "── Large quoted-field file, parallel WHERE (#139) ───────────"
  check \
    "COUNT(*) WHERE trip_distance > 5 (taxi dataset, parallel scan)" \
    "SELECT COUNT(*) FROM '$TAXI_CSV' WHERE trip_distance > 5" \
    "SELECT COUNT(*) FROM read_csv_auto('$TAXI_CSV') WHERE trip_distance > 5"
fi

# ════════════════════════════════════════════════════════════════
echo ""
echo "── Summary ─────────────────────────────────────────────────"
echo "  Total:  $TOTAL"
printf "  Pass:   ${GREEN}%d${RESET}\n" $PASS
if [[ $FAIL -gt 0 ]]; then
  printf "  Fail:   ${RED}%d${RESET}\n" $FAIL
else
  printf "  Fail:   ${GREEN}%d${RESET}\n" $FAIL
fi
echo ""

if [[ $FAIL -eq 0 ]]; then
  printf "  ${GREEN}${BOLD}All $TOTAL checks passed — csvql output matches DuckDB.${RESET}\n\n"
  exit 0
else
  printf "  ${RED}${BOLD}$FAIL of $TOTAL checks failed.${RESET}\n\n"
  exit 1
fi
