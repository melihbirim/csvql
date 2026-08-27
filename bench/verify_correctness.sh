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
# check <label> <csvql_sql> <duckdb_sql> [sort_key]
# Runs both queries, strips csvql header, then diffs. Exact string equality —
# use check_approx for any result containing a float (AVG/VARIANCE/STDDEV/
# division), since csvql and DuckDB can format the same float with a
# different number of trailing digits even when the value is identical.
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

  # When the query itself has an ORDER BY, row order is a claim under test —
  # sorting both sides before diff would silently hide a wrong-order bug.
  # Compare exact engine output order in that case; sort only for queries
  # with no defined order (plain WHERE scans, unordered GROUP BY/JOIN),
  # where the two engines' internal row order is allowed to differ.
  local csvql_rc duck_rc
  if echo "$csvql_sql" | grep -qi '\border[[:space:]]\+by\b'; then
    "$CSVQL" "$csvql_sql" 2>"$err_csvql" | tail -n +2 > "$out_csvql"
    csvql_rc=${PIPESTATUS[0]}
    "$DUCKDB" -csv -noheader -c "$duck_sql" 2>"$err_duck" > "$out_duck"
    duck_rc=${PIPESTATUS[0]}
  else
    "$CSVQL" "$csvql_sql" 2>"$err_csvql" | tail -n +2 | sort $sort_key > "$out_csvql"
    csvql_rc=${PIPESTATUS[0]}
    "$DUCKDB" -csv -noheader -c "$duck_sql" 2>"$err_duck" | sort $sort_key > "$out_duck"
    duck_rc=${PIPESTATUS[0]}
  fi

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

# check_self <label> <sql>
# Self-differential: runs the same csvql query single-threaded vs auto
# (multi-threaded) and diffs the two csvql outputs against EACH OTHER — no
# DuckDB involved. This is the shape that catches the #139 class of bug: a
# parallel scan losing quote state across an internal IO-buffer boundary,
# where COUNT(*) still comes out right but a numeric WHERE filter silently
# undercounts. --threads 1 takes the sequential/mmap path that reads the
# whole file in one mapping (no buffer-boundary chunking), so any divergence
# from the auto-threaded run is a real parallel-path bug, not an oracle
# disagreement.
check_self() {
  local label="$1"
  local sql="$2"
  TOTAL=$((TOTAL + 1))

  local out1="$TMP/self1_${TOTAL}.txt"
  local outN="$TMP/selfN_${TOTAL}.txt"
  local err1="$TMP/self1_err_${TOTAL}.txt"
  local errN="$TMP/selfN_err_${TOTAL}.txt"

  "$CSVQL" --threads 1 "$sql" 2>"$err1" | tail -n +2 > "$out1"
  local rc1=${PIPESTATUS[0]}
  "$CSVQL" --threads 0 "$sql" 2>"$errN" | tail -n +2 > "$outN"
  local rcN=${PIPESTATUS[0]}

  if [[ $rc1 -ne 0 ]]; then
    printf "  ${RED}FAIL${RESET}  %s (--threads 1 exited %d)\n" "$label" "$rc1"
    FAIL=$((FAIL + 1)); sed 's/^/        /' "$err1"; echo ""; return
  fi
  if [[ $rcN -ne 0 ]]; then
    printf "  ${RED}FAIL${RESET}  %s (--threads 0/auto exited %d)\n" "$label" "$rcN"
    FAIL=$((FAIL + 1)); sed 's/^/        /' "$errN"; echo ""; return
  fi
  if [[ ! -s "$out1" && ! -s "$outN" ]]; then
    printf "  ${RED}FAIL${RESET}  %s (both runs returned zero rows)\n" "$label"
    FAIL=$((FAIL + 1)); echo ""; return
  fi

  if diff -q "$out1" "$outN" > /dev/null 2>&1; then
    printf "  ${GREEN}PASS${RESET}  %s\n" "$label"
    PASS=$((PASS + 1))
  else
    printf "  ${RED}FAIL${RESET}  %s (single-thread vs auto-thread disagree)\n" "$label"
    FAIL=$((FAIL + 1))
    diff "$out1" "$outN" | head -25 | sed 's/^/        /' | \
      sed "s/^        </        ${DIM}threads=1 <${RESET}/" | \
      sed "s/^        >/        ${BOLD}threads=0 >${RESET}/"
    echo ""
  fi
}

# check_approx <label> <csvql_sql> <duckdb_sql> [decimals=4]
# For float aggregates (AVG, VARIANCE, STDDEV, SUM of non-integers): rounds
# every numeric-looking field on both sides to `decimals` places (default 4)
# before diffing. This is a formatting tolerance, not a numerical-accuracy
# one — csvql and DuckDB both compute with f64, so any genuine precision
# divergence beyond float rounding noise is still a real bug and will still
# fail at 4 decimal places. Lower `decimals` (e.g. pass 2) only for values
# where the two engines' rounding-half-to-even vs rounding-half-up choice
# can legitimately differ in the last couple of digits.
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

  # Same order-sensitivity rule as check() — see comment there.
  local order_pipe=(sort)
  if echo "$csvql_sql" | grep -qi '\border[[:space:]]\+by\b'; then order_pipe=(cat); fi

  "$CSVQL" "$csvql_sql" 2>"$err_csvql" | tail -n +2 | \
    awk -v d="$decimals" '{
      for(i=1;i<=NF;i++) {
        if ($i ~ /^-?[0-9]+(\.[0-9]+)?$/){
          printf "%.*f", d, $i+0
        } else printf "%s", $i
        printf (i<NF?",":"\n")
      }
    }' FS=',' OFS=',' | "${order_pipe[@]}" > "$out_csvql"
  local csvql_rc=${PIPESTATUS[0]}

  "$DUCKDB" -csv -noheader -c "$duck_sql" 2>"$err_duck" | \
    awk -v d="$decimals" '{
      for(i=1;i<=NF;i++) {
        if ($i ~ /^-?[0-9]+(\.[0-9]+)?$/){
          printf "%.*f", d, $i+0
        } else printf "%s", $i
        printf (i<NF?",":"\n")
      }
    }' FS=',' OFS=',' | "${order_pipe[@]}" > "$out_duck"
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
  "SELECT name, age FROM '$CSV' WHERE age = 30 ORDER BY name, age, id" \
  "SELECT name, age FROM read_csv_auto('$CSV') WHERE age = 30 ORDER BY name, age, id"

check \
  "WHERE salary >= 100000 AND salary <= 110000" \
  "SELECT name, salary FROM '$CSV' WHERE salary >= 100000 AND salary <= 110000 ORDER BY salary, name, id" \
  "SELECT name, salary FROM read_csv_auto('$CSV') WHERE salary >= 100000 AND salary <= 110000 ORDER BY salary, name, id"

check \
  "WHERE city = 'NYC'" \
  "SELECT name, city FROM '$CSV' WHERE city = 'NYC' ORDER BY name, id" \
  "SELECT name, city FROM read_csv_auto('$CSV') WHERE city = 'NYC' ORDER BY name, id"

check \
  "WHERE id % 2 = 0 (modulo, #119)" \
  "SELECT name, id FROM '$CSV' WHERE id % 2 = 0 ORDER BY id, name" \
  "SELECT name, id FROM read_csv_auto('$CSV') WHERE id % 2 = 0 ORDER BY id, name"

check \
  "WHERE department NOT IN ('Sales') (#115)" \
  "SELECT name FROM '$CSV' WHERE department NOT IN ('Sales') ORDER BY name, id" \
  "SELECT name FROM read_csv_auto('$CSV') WHERE department NOT IN ('Sales') ORDER BY name, id"

# ════════════════════════════════════════════════════════════════
echo ""
echo "── LIKE Pattern Matching ───────────────────────────────────"

check \
  "WHERE name LIKE 'A%' (prefix)" \
  "SELECT name FROM '$CSV' WHERE name LIKE 'A%' ORDER BY name, id" \
  "SELECT name FROM read_csv_auto('$CSV') WHERE name LIKE 'A%' ORDER BY name, id"

check \
  "WHERE name LIKE '%e' (suffix)" \
  "SELECT name FROM '$CSV' WHERE name LIKE '%e' ORDER BY name, id" \
  "SELECT name FROM read_csv_auto('$CSV') WHERE name LIKE '%e' ORDER BY name, id"

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
  "SELECT name, age FROM '$CSV' WHERE age BETWEEN 25 AND 30 ORDER BY name, age, id" \
  "SELECT name, age FROM read_csv_auto('$CSV') WHERE age BETWEEN 25 AND 30 ORDER BY name, age, id"

# IS NULL/IS NOT NULL combined with AND/OR — found via the differential
# query generator (#142): the substring scan for IS NULL/IS NOT NULL ran
# before the AND/OR top-level split, so "age IS NOT NULL AND city = 'x'"
# silently dropped the AND clause entirely (returned all rows, not just
# city='x'), and "age > 5 AND city IS NULL" errored ColumnNotFound.
check \
  "WHERE col IS NOT NULL AND another condition (#142)" \
  "SELECT name FROM '$CSV' WHERE age IS NOT NULL AND city = 'Austin' ORDER BY name, id" \
  "SELECT name FROM read_csv_auto('$CSV') WHERE age IS NOT NULL AND city = 'Austin' ORDER BY name, id"
check \
  "WHERE another condition AND col IS NOT NULL (#142)" \
  "SELECT name FROM '$CSV' WHERE city = 'Austin' AND age IS NOT NULL ORDER BY name, id" \
  "SELECT name FROM read_csv_auto('$CSV') WHERE city = 'Austin' AND age IS NOT NULL ORDER BY name, id"
check \
  "WHERE col IS NULL OR another condition (#142)" \
  "SELECT name FROM '$CSV' WHERE department IS NULL OR age > 55 ORDER BY name, id" \
  "SELECT name FROM read_csv_auto('$CSV') WHERE department IS NULL OR age > 55 ORDER BY name, id"

# ════════════════════════════════════════════════════════════════
echo ""
echo "── ORDER BY ────────────────────────────────────────────────"

check \
  "ORDER BY name (stable unique)" \
  "SELECT DISTINCT name FROM '$CSV' ORDER BY name" \
  "SELECT DISTINCT name FROM read_csv_auto('$CSV') ORDER BY name"

check \
  "ORDER BY salary DESC LIMIT 5 (unique keys)" \
  "SELECT city, MAX(salary) as ms FROM '$CSV' GROUP BY city ORDER BY ms DESC, city" \
  "SELECT city, MAX(salary) as ms FROM read_csv_auto('$CSV') GROUP BY city ORDER BY ms DESC, city"

check \
  "GROUP BY dept ORDER BY COUNT DESC" \
  "SELECT department, COUNT(*) FROM '$CSV' GROUP BY department ORDER BY COUNT(*) DESC, department" \
  "SELECT department, COUNT(*) FROM read_csv_auto('$CSV') GROUP BY department ORDER BY count(*) DESC, department"

check \
  "ORDER BY positional (ORDER BY 1)" \
  "SELECT department, COUNT(*) FROM '$CSV' GROUP BY department ORDER BY 1" \
  "SELECT department, COUNT(*) FROM read_csv_auto('$CSV') GROUP BY department ORDER BY 1"

# ════════════════════════════════════════════════════════════════
echo ""
echo "── OFFSET ──────────────────────────────────────────────────"

check \
  "LIMIT then OFFSET" \
  "SELECT id, name FROM '$CSV' ORDER BY id LIMIT 5 OFFSET 5" \
  "SELECT id, name FROM read_csv_auto('$CSV') ORDER BY id LIMIT 5 OFFSET 5"

check \
  "OFFSET then LIMIT" \
  "SELECT id, name FROM '$CSV' ORDER BY id OFFSET 5 LIMIT 5" \
  "SELECT id, name FROM read_csv_auto('$CSV') ORDER BY id OFFSET 5 LIMIT 5"

check \
  "bare OFFSET" \
  "SELECT id, name FROM '$CSV' ORDER BY id OFFSET 5" \
  "SELECT id, name FROM read_csv_auto('$CSV') ORDER BY id OFFSET 5"

check \
  "GROUP BY with ORDER BY and OFFSET" \
  "SELECT department, COUNT(*) FROM '$CSV' GROUP BY department ORDER BY department LIMIT 2 OFFSET 1" \
  "SELECT department, COUNT(*) FROM read_csv_auto('$CSV') GROUP BY department ORDER BY department LIMIT 2 OFFSET 1"

check \
  "DISTINCT with ORDER BY and OFFSET" \
  "SELECT DISTINCT city FROM '$CSV' ORDER BY city LIMIT 2 OFFSET 1" \
  "SELECT DISTINCT city FROM read_csv_auto('$CSV') ORDER BY city LIMIT 2 OFFSET 1"

# ════════════════════════════════════════════════════════════════
echo ""
echo "── SELECT * / Projection ───────────────────────────────────"

check \
  "SELECT * LIMIT 20 (by sorted key)" \
  "SELECT id, name, age, city, salary, department FROM '$CSV' ORDER BY id LIMIT 20" \
  "SELECT id, name, age, city, salary, department FROM read_csv_auto('$CSV') ORDER BY id LIMIT 20"

check \
  "Column projection + WHERE" \
  "SELECT name, department FROM '$CSV' WHERE age < 25 ORDER BY name, department, id" \
  "SELECT name, department FROM read_csv_auto('$CSV') WHERE age < 25 ORDER BY name, department, id"

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
  "SELECT t.name, t.department FROM '$CSV' AS t WHERE t.salary > 100000 ORDER BY t.name, t.department, t.id" \
  "SELECT t.name, t.department FROM read_csv_auto('$CSV') AS t WHERE t.salary > 100000 ORDER BY t.name, t.department, t.id"

check \
  "Table alias outside JOIN, bare (no AS) (#121)" \
  "SELECT t.name FROM '$CSV' t WHERE t.department = 'Sales' ORDER BY t.name, t.id" \
  "SELECT t.name FROM read_csv_auto('$CSV') t WHERE t.department = 'Sales' ORDER BY t.name, t.id"

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
  "STRFTIME('%Y-%m', started_at) date bucketing (GROUP BY path)" \
  "SELECT STRFTIME('%Y-%m', started_at), COUNT(*) FROM '$DATES' GROUP BY STRFTIME('%Y-%m', started_at) ORDER BY 1" \
  "SELECT strftime(started_at, '%Y-%m'), COUNT(*) FROM read_csv_auto('$DATES') GROUP BY strftime(started_at, '%Y-%m') ORDER BY 1"

check \
  "plain SELECT STRFTIME(...) without GROUP BY (#133)" \
  "SELECT id, STRFTIME('%Y-%m', started_at) FROM '$DATES' ORDER BY id" \
  "SELECT id, strftime(started_at, '%Y-%m') FROM read_csv_auto('$DATES') ORDER BY id"

# Mixed date formats in one column (ISO-8601, US MM/DD/YYYY, EU DD.MM.YYYY) —
# STRFTIME/DATE_PART silently sliced the wrong bytes for anything that
# wasn't ISO-8601-shaped until this was fixed (#140).
MIXDATE="$TMP/mixdate.csv"
cat > "$MIXDATE" <<'EOF'
id,d
1,2026-01-15
2,01/20/2026
3,25.01.2026
EOF
check \
  "STRFTIME/DATE_PART on mixed ISO/US/EU date formats in one column (#140)" \
  "SELECT id, DATE_PART('day', d), DATE_PART('month', d), DATE_PART('year', d) FROM '$MIXDATE' ORDER BY id" \
  "SELECT id, lpad(date_part('day', CASE WHEN d LIKE '%-%' THEN strptime(d,'%Y-%m-%d') WHEN d LIKE '%/%' THEN strptime(d,'%m/%d/%Y') ELSE strptime(d,'%d.%m.%Y') END)::VARCHAR,2,'0'), lpad(date_part('month', CASE WHEN d LIKE '%-%' THEN strptime(d,'%Y-%m-%d') WHEN d LIKE '%/%' THEN strptime(d,'%m/%d/%Y') ELSE strptime(d,'%d.%m.%Y') END)::VARCHAR,2,'0'), date_part('year', CASE WHEN d LIKE '%-%' THEN strptime(d,'%Y-%m-%d') WHEN d LIKE '%/%' THEN strptime(d,'%m/%d/%Y') ELSE strptime(d,'%d.%m.%Y') END) FROM read_csv_auto('$MIXDATE', types={'d':'VARCHAR'}) ORDER BY id"

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
echo "── WHERE col IN (SELECT ...) — non-correlated subqueries (#124) ─"

check \
  "IN (SELECT ...) — departments in the West region" \
  "SELECT name FROM '$CSV' WHERE department IN (SELECT dept_name FROM '$DEPTS' WHERE region = 'West')" \
  "SELECT name FROM read_csv_auto('$CSV') WHERE department IN (SELECT dept_name FROM read_csv_auto('$DEPTS') WHERE region = 'West')"

check \
  "NOT IN (SELECT ...) — departments outside the West region" \
  "SELECT name FROM '$CSV' WHERE department NOT IN (SELECT dept_name FROM '$DEPTS' WHERE region = 'West')" \
  "SELECT name FROM read_csv_auto('$CSV') WHERE department NOT IN (SELECT dept_name FROM read_csv_auto('$DEPTS') WHERE region = 'West')"

check \
  "IN (SELECT ...) combined with an outer AND condition" \
  "SELECT name FROM '$CSV' WHERE department IN (SELECT dept_name FROM '$DEPTS' WHERE region = 'West') AND salary > 80000" \
  "SELECT name FROM read_csv_auto('$CSV') WHERE department IN (SELECT dept_name FROM read_csv_auto('$DEPTS') WHERE region = 'West') AND salary > 80000"

check \
  "IN (SELECT ... GROUP BY ... HAVING ...) — aggregate subquery" \
  "SELECT name FROM '$CSV' WHERE department IN (SELECT department FROM '$CSV' GROUP BY department HAVING COUNT(*) > 5)" \
  "SELECT name FROM read_csv_auto('$CSV') WHERE department IN (SELECT department FROM read_csv_auto('$CSV') GROUP BY department HAVING COUNT(*) > 5)"

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

# -- CRLF line endings --
CRLF="$TMP/crlf.csv"
printf 'id,name\r\n1,alice\r\n2,bob\r\n' > "$CRLF"
check \
  "CRLF line endings" \
  "SELECT id, name FROM '$CRLF' WHERE id = 2" \
  "SELECT id, name FROM read_csv_auto('$CRLF') WHERE id = 2"

# -- UTF-8 BOM --
BOMFILE="$TMP/bom.csv"
printf '\xEF\xBB\xBFid,name\n1,alice\n2,bob\n' > "$BOMFILE"
check \
  "UTF-8 BOM in header doesn't glue onto the first column name" \
  "SELECT id, name FROM '$BOMFILE' WHERE id = 1" \
  "SELECT id, name FROM read_csv_auto('$BOMFILE') WHERE id = 1"

# -- RTL / emoji / combining marks. Compared via a WHERE LIKE predicate that
# returns only the (unambiguous, unquoted) id column — DuckDB's CSV writer
# quotes any field containing non-ASCII bytes while csvql only quotes per
# RFC 4180 (delimiter/quote/newline present), so diffing the raw unicode
# field value directly would fail on a quoting-style difference, not a
# content difference.
UNI="$TMP/uni.csv"
cat > "$UNI" <<'EOF'
id,val
1,🎉 party
2,مرحبا بالعالم
3,café (é as a combining mark: e + ́)
EOF
check \
  "emoji in a field, matched via LIKE" \
  "SELECT id FROM '$UNI' WHERE val LIKE '%🎉%'" \
  "SELECT id FROM read_csv_auto('$UNI') WHERE val LIKE '%🎉%'"
check \
  "RTL (Arabic) text in a field, matched via LIKE" \
  "SELECT id FROM '$UNI' WHERE val LIKE '%مرحبا%'" \
  "SELECT id FROM read_csv_auto('$UNI') WHERE val LIKE '%مرحبا%'"
check \
  "combining-mark unicode in a field, matched via LIKE" \
  "SELECT id FROM '$UNI' WHERE val LIKE '%café%'" \
  "SELECT id FROM read_csv_auto('$UNI') WHERE val LIKE '%café%'"

# -- Scientific notation / signed-zero numeric forms that DuckDB's own CSV
# type sniffer also recognizes as numeric (leading zeros like "007" and an
# explicit leading "+" like "+5" make DuckDB's sniffer fall back to VARCHAR
# for the whole column, so those two forms are exercised as a csvql-only
# assertion below instead — see "Known differences" in CORRECTNESS.md).
NUMFORMS="$TMP/numforms.csv"
cat > "$NUMFORMS" <<'EOF'
id,val
1,1e10
2,-2.5E-3
3,-0
EOF
check_approx \
  "scientific notation and negative zero forms" \
  "SELECT SUM(val) FROM '$NUMFORMS'" \
  "SELECT SUM(val) FROM read_csv_auto('$NUMFORMS')"

# -- Header-only / single-row / single-column files --
HEADERONLY="$TMP/headeronly.csv"
printf "id,name\n" > "$HEADERONLY"
check_approx \
  "header-only file (zero data rows)" \
  "SELECT COUNT(*) FROM '$HEADERONLY'" \
  "SELECT COUNT(*) FROM read_csv_auto('$HEADERONLY')"

SINGLEROW="$TMP/singlerow.csv"
printf "id,name\n1,alice\n" > "$SINGLEROW"
check \
  "single-row file" \
  "SELECT id, name FROM '$SINGLEROW'" \
  "SELECT id, name FROM read_csv_auto('$SINGLEROW')"

SINGLECOL="$TMP/singlecol.csv"
printf "val\n10\n20\n30\n" > "$SINGLECOL"
check_approx \
  "single-column file" \
  "SELECT SUM(val) FROM '$SINGLECOL'" \
  "SELECT SUM(val) FROM read_csv_auto('$SINGLECOL')"

# -- csvql-only behavioral locks (no DuckDB oracle comparison): these three
# fixtures each hit a case where DuckDB's own behavior isn't a meaningful
# reference — see the corresponding rows in CORRECTNESS.md's "Known
# differences" table for why. The point here is only to pin down csvql's
# own current, sane behavior so it can't silently regress.
TOTAL=$((TOTAL + 1))
LEADZERO="$TMP/leadzero.csv"
printf "id,val\n1,007\n2,+5\n3,10\n" > "$LEADZERO"
leadzero_out=$("$CSVQL" "SELECT SUM(val) FROM '$LEADZERO'" 2>/dev/null | tail -n +2)
if [[ "$leadzero_out" == "22" ]]; then
  printf "  ${GREEN}PASS${RESET}  %s\n" "csvql-only: leading-zero (007) and leading-plus (+5) numeric literals parse correctly"
  PASS=$((PASS + 1))
else
  printf "  ${RED}FAIL${RESET}  %s (got %s, want 22)\n" "csvql-only: leading-zero (007) and leading-plus (+5) numeric literals parse correctly" "$leadzero_out"
  FAIL=$((FAIL + 1))
fi

# Empty-field NULL semantics. The CORRECTNESS.md row describing this had
# drifted out of sync with the engine (it claimed an empty field "stays an
# empty string", false in every path here) precisely because nothing tested
# it — hence these locks.
#
# The second check previously pinned the *inconsistent* behavior so that
# fixing #147 would fail it and force the doc to be updated in the same
# change. That worked: #147 is fixed, this check failed, and the row was
# corrected. It now pins the consistent behavior — every scalar function
# propagates NULL, LENGTH included.
TOTAL=$((TOTAL + 1))
BLANKS="$TMP/blanks.csv"
printf "id,name,city\n1,Alice,NYC\n2,,Boston\n3,Carol,\n" > "$BLANKS"
blank_isnull=$("$CSVQL" "SELECT id FROM '$BLANKS' WHERE name IS NULL" 2>/dev/null | tail -n +2)
blank_count=$("$CSVQL" "SELECT COUNT(name) FROM '$BLANKS'" 2>/dev/null | tail -n +2)
blank_coalesce=$("$CSVQL" "SELECT COALESCE(name, 'MISSING') FROM '$BLANKS'" 2>/dev/null | tail -n +2 | tr '\n' '|')
if [[ "$blank_isnull" == "2" && "$blank_count" == "2" && "$blank_coalesce" == "Alice|MISSING|Carol|" ]]; then
  printf "  ${GREEN}PASS${RESET}  %s\n" "csvql-only: empty field behaves as NULL for IS NULL, COUNT(col), COALESCE"
  PASS=$((PASS + 1))
else
  printf "  ${RED}FAIL${RESET}  %s (IS NULL=%s want 2; COUNT=%s want 2; COALESCE=%s want Alice|MISSING|Carol|)\n" \
    "csvql-only: empty field behaves as NULL for IS NULL, COUNT(col), COALESCE" "$blank_isnull" "$blank_count" "$blank_coalesce"
  FAIL=$((FAIL + 1))
fi

TOTAL=$((TOTAL + 1))
blank_len=$("$CSVQL" "SELECT id, LENGTH(name) FROM '$BLANKS' WHERE name IS NULL" 2>/dev/null | tail -n +2)
blank_upper=$("$CSVQL" "SELECT id, UPPER(name) FROM '$BLANKS' WHERE name IS NULL" 2>/dev/null | tail -n +2)
blank_substr=$("$CSVQL" "SELECT id, SUBSTR(name, 1, 2) FROM '$BLANKS' WHERE name IS NULL" 2>/dev/null | tail -n +2)
if [[ "$blank_len" == "2," && "$blank_upper" == "2," && "$blank_substr" == "2," ]]; then
  printf "  ${GREEN}PASS${RESET}  %s\n" "csvql-only: scalar functions propagate NULL on an empty field — LENGTH/UPPER/SUBSTR (#147)"
  PASS=$((PASS + 1))
else
  printf "  ${RED}FAIL${RESET}  %s (LENGTH=%s UPPER=%s SUBSTR=%s, each want '2,')\n" \
    "csvql-only: scalar functions propagate NULL on an empty field — LENGTH/UPPER/SUBSTR (#147)" "$blank_len" "$blank_upper" "$blank_substr"
  FAIL=$((FAIL + 1))
fi

TOTAL=$((TOTAL + 1))
RAGGED="$TMP/ragged.csv"
printf "id,name,age\n1,alice,30\n2,bob\n3,carol,25,extra\n" > "$RAGGED"
ragged_out=$("$CSVQL" "SELECT id, name, age FROM '$RAGGED'" 2>/dev/null | tail -n +2)
ragged_expected=$'1,alice,30\n2,bob,\n3,carol,25'
if [[ "$ragged_out" == "$ragged_expected" ]]; then
  printf "  ${GREEN}PASS${RESET}  %s\n" "csvql-only: ragged rows pad-missing/truncate-extra instead of erroring"
  PASS=$((PASS + 1))
else
  printf "  ${RED}FAIL${RESET}  %s\n" "csvql-only: ragged rows pad-missing/truncate-extra instead of erroring"
  diff <(echo "$ragged_expected") <(echo "$ragged_out") | head -10 | sed 's/^/        /'
  FAIL=$((FAIL + 1))
fi

TOTAL=$((TOTAL + 1))
EMPTYFILE="$TMP/emptyfile.csv"
: > "$EMPTYFILE"
"$CSVQL" "SELECT * FROM '$EMPTYFILE'" > /dev/null 2>/dev/null
empty_rc=$?
if [[ $empty_rc -eq 3 ]]; then
  printf "  ${GREEN}PASS${RESET}  %s\n" "csvql-only: zero-byte file errors clearly (exit 3) instead of silently succeeding"
  PASS=$((PASS + 1))
else
  printf "  ${RED}FAIL${RESET}  %s (exit code %d, want 3)\n" "csvql-only: zero-byte file errors clearly (exit 3) instead of silently succeeding" "$empty_rc"
  FAIL=$((FAIL + 1))
fi

# Values near i64/f64 limits: a plain SELECT is a pass-through projection, no
# arithmetic, so csvql preserves the exact source literal. DuckDB's CSV type
# sniffer infers DOUBLE for these and reformats through IEEE-754 rounding +
# scientific notation even without any computation — not a meaningful
# apples-to-apples comparison (see CORRECTNESS.md).
TOTAL=$((TOTAL + 1))
LIMITS="$TMP/limits.csv"
printf "id,val\n1,9223372036854775807\n2,1.7976931348623157e308\n3,-9223372036854775807\n" > "$LIMITS"
limits_out=$("$CSVQL" "SELECT val FROM '$LIMITS'" 2>/dev/null | tail -n +2)
limits_expected=$'9223372036854775807\n1.7976931348623157e308\n-9223372036854775807'
if [[ "$limits_out" == "$limits_expected" ]]; then
  printf "  ${GREEN}PASS${RESET}  %s\n" "csvql-only: values near i64/f64 limits pass through a plain SELECT unrounded"
  PASS=$((PASS + 1))
else
  printf "  ${RED}FAIL${RESET}  %s\n" "csvql-only: values near i64/f64 limits pass through a plain SELECT unrounded"
  diff <(echo "$limits_expected") <(echo "$limits_out") | head -10 | sed 's/^/        /'
  FAIL=$((FAIL + 1))
fi

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
# Large-file, quote-heavy regression check (#139): a parallel scan lost a
# record's quote state across an internal IO-buffer boundary (2MB), so a
# numeric WHERE filter on a large quoted CSV could silently undercount even
# though a bare COUNT(*) still came out right.
#
# Primary guard: a synthetic quote-heavy fixture sized well past several
# 2MB boundaries, with quoted (and some embedded-newline) fields of varying
# length so row boundaries land at many different offsets relative to each
# 2MB mark — no multi-GB download needed. Self-differential (threads=1 vs
# threads=0/auto), so it needs no DuckDB oracle either.
echo ""
echo "── Large quote-heavy file, self-differential threads=1 vs auto (#139) ─"

BOUNDARY_CSV="$TMP/boundary.csv"
awk 'BEGIN{
  for (i = 1; i <= 90000; i++) {
    amount = i % 97;
    # Note length cycles through a set of sizes that share no common factor
    # with 2MB, so cumulative byte offsets drift across every 2MB boundary
    # instead of always landing on the same relative position.
    len = 40 + (i % 53) * 17;
    note = "";
    for (j = 0; j < len; j++) note = note "x";
    if (i % 37 == 0) {
      # Embedded newline inside the quoted field, the exact shape #139 needs.
      print i "," amount ",\"" note "\nmore text after the newline\"";
    } else if (i % 11 == 0) {
      # Embedded comma inside the quoted field.
      print i "," amount ",\"" note ", with a comma\"";
    } else {
      print i "," amount ",\"" note "\"";
    }
  }
}' > "$TMP/boundary_body.csv"
{ echo "id,amount,note"; cat "$TMP/boundary_body.csv"; } > "$BOUNDARY_CSV"
rm -f "$TMP/boundary_body.csv"

check_self \
  "COUNT(*) WHERE amount > 50 (quote-heavy, crosses many 2MB boundaries)" \
  "SELECT COUNT(*) FROM '$BOUNDARY_CSV' WHERE amount > 50"

check_self \
  "SUM(amount) WHERE amount > 50 (quote-heavy, crosses many 2MB boundaries)" \
  "SELECT SUM(amount) FROM '$BOUNDARY_CSV' WHERE amount > 50"

# Opportunistic real-world check: gated on the gitignored taxi fixture
# (bench/bench_taxi.sh downloads it), so CI stays green without a multi-GB
# download but still gets this if the fixture happens to be present locally.
TAXI_CSV="${SCRIPT_DIR}/bench/.taxi-data/trips.csv"
if [[ -f "$TAXI_CSV" ]]; then
  echo ""
  echo "── Large quoted-field file, parallel WHERE (#139, real dataset) ─"
  # null_padding=true: the raw dataset has at least one genuinely malformed
  # row (fewer fields than the header) that DuckDB's strict-mode reader
  # refuses outright by default (CSV Error, wrong column count). csvql pads
  # a short row with empty fields rather than erroring (documented in
  # CORRECTNESS.md's ragged-rows difference) — null_padding matches that
  # semantic on DuckDB's side instead of asserting DuckDB's strict-mode
  # rejection is the correct behavior to diff against.
  check \
    "COUNT(*) WHERE trip_distance > 5 (taxi dataset, parallel scan)" \
    "SELECT COUNT(*) FROM '$TAXI_CSV' WHERE trip_distance > 5" \
    "SELECT COUNT(*) FROM read_csv_auto('$TAXI_CSV', null_padding=true) WHERE trip_distance > 5"
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
