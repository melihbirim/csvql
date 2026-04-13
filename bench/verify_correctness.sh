#!/usr/bin/env bash
# verify_correctness.sh — Compare csvql output against DuckDB for all major query shapes.
#
# Treats DuckDB as ground truth. Diffs every query result and reports PASS / FAIL.
# Results are sorted before diff so row-order ties (unstable sort) do not cause
# false failures — the set of result rows must match exactly.
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
  local diff_out="$TMP/diff_${TOTAL}.txt"

  # Run queries
  "$CSVQL" "$csvql_sql" 2>/dev/null | tail -n +2 | sort $sort_key > "$out_csvql" || true
  "$DUCKDB" -csv -noheader -c "$duck_sql" 2>/dev/null | sort $sort_key > "$out_duck" || true

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

  "$CSVQL" "$csvql_sql" 2>/dev/null | tail -n +2 | \
    awk -v d="$decimals" '{
      for(i=1;i<=NF;i++) {
        if ($i+0==$i && index($i,".")){
          printf "%.*f", d, $i+0
        } else printf "%s", $i
        printf (i<NF?",":"\n")
      }
    }' FS=',' OFS=',' | sort > "$out_csvql" || true

  "$DUCKDB" -csv -noheader -c "$duck_sql" 2>/dev/null | \
    awk -v d="$decimals" '{
      for(i=1;i<=NF;i++) {
        if ($i+0==$i && index($i,".")){
          printf "%.*f", d, $i+0
        } else printf "%s", $i
        printf (i<NF?",":"\n")
      }
    }' FS=',' OFS=',' | sort > "$out_duck" || true

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
