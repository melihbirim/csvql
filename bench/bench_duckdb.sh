#!/usr/bin/env bash
# bench_duckdb.sh — Compare csvql vs DuckDB on COUNT, DISTINCT, and scalar aggregates
# Usage: ./bench/bench_duckdb.sh [csv_file]
# Requires: duckdb in PATH, csvql built (zig build -Doptimize=ReleaseFast)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CSVQL="${SCRIPT_DIR}/zig-out/bin/csvql"
DUCKDB="${DUCKDB_BIN:-duckdb}"
CSV="${1:-${SCRIPT_DIR}/large_test.csv}"
BENCH_DIR="${SCRIPT_DIR}/.bench_tmp"
mkdir -p "$BENCH_DIR"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

check_deps() {
  [[ -x "$CSVQL" ]] || { echo "csvql not found at $CSVQL — run: zig build -Doptimize=ReleaseFast"; exit 1; }
  command -v "$DUCKDB" >/dev/null || { echo "duckdb not found in PATH"; exit 1; }
  [[ -f "$CSV" ]] || { echo "CSV not found: $CSV"; exit 1; }
}

# Run command N times, return median wall-clock seconds (3 decimal places)
bench_median() {
  local n=3 times=()
  for ((i=0; i<n; i++)); do
    local t
    # /usr/bin/time -p outputs POSIX "real X.XX" to stderr
    t=$( { /usr/bin/time -p "$@" >/dev/null; } 2>&1 | awk '/^real/{printf "%.3f", $2}' ) || true
    [[ -n "$t" ]] || t="0.000"
    times+=("$t")
  done
  # sort and pick middle
  IFS=$'\n' sorted=($(printf '%s\n' "${times[@]}" | sort -n)); unset IFS
  echo "${sorted[1]}"   # median of 3
}

hash_file() { md5 -q "$1" 2>/dev/null || md5sum "$1" | awk '{print $1}'; }

# Hash a CSV ignoring output row ordering (sort data rows, keep header fixed)
hash_sorted() {
  local f="$1"
  { head -1 "$f"; tail -n +2 "$f" | sort; } | md5 -q 2>/dev/null || \
  { head -1 "$f"; tail -n +2 "$f" | sort; } | md5sum | awk '{print $1}'
}

run_query() {
  local label="$1" csvql_sql="$2" duck_sql="$3"
  local cq_out="${BENCH_DIR}/csvql_$(echo "$label" | tr ' /' '__').csv"
  local dk_out="${BENCH_DIR}/duck_$(echo "$label"  | tr ' /' '__').csv"

  # --- csvql ---
  "$CSVQL" "$csvql_sql" > "$cq_out" 2>/dev/null || true
  local cq_hash; cq_hash=$(hash_sorted "$cq_out")
  local cq_rows; cq_rows=$(( $(wc -l < "$cq_out") - 1 ))
  local cq_time; cq_time=$(bench_median "$CSVQL" "$csvql_sql")

  # --- duckdb ---
  "$DUCKDB" -csv -c "$duck_sql" > "$dk_out" 2>/dev/null || true
  local dk_hash; dk_hash=$(hash_sorted "$dk_out")
  local dk_rows; dk_rows=$(( $(wc -l < "$dk_out") - 1 ))
  local dk_time; dk_time=$(bench_median "$DUCKDB" -csv -c "$duck_sql")

  # determine speedup/slowdown (>1 means csvql is faster)
  local ratio
  ratio=$(awk -v cq="$cq_time" -v dk="$dk_time" 'BEGIN{if(cq<=0)cq=0.001; printf "%.2f", dk/cq}')

  local match_icon
  if [[ "$cq_hash" == "$dk_hash" ]]; then
    match_icon="${GREEN}MATCH${RESET}"
  elif [[ "$cq_rows" -eq "$dk_rows" ]]; then
    match_icon="${YELLOW}ROWS-OK / HASH-DIFF${RESET}"
  else
    match_icon="${RED}MISMATCH${RESET} (csvql=$cq_rows duck=$dk_rows)"
  fi

  printf "${BOLD}%-55s${RESET}\n" "$label"
  printf "  %-12s  time: ${CYAN}%ss${RESET}  rows: %-6d  md5(sorted): %s\n" "csvql"  "$cq_time" "$cq_rows" "$cq_hash"
  printf "  %-12s  time: ${CYAN}%ss${RESET}  rows: %-6d  md5(sorted): %s\n" "duckdb" "$dk_time" "$dk_rows" "$dk_hash"
  printf "  result: %b  speedup: ${YELLOW}%sx${RESET} (csvql vs duckdb)\n\n" "$match_icon" "$ratio"

  # Show first 5 output rows from csvql
  echo -e "  ${BOLD}csvql sample (first 5 data rows):${RESET}"
  head -6 "$cq_out" | sed 's/^/    /'
  echo ""
}

# ─────────────────────────────────────────────────────────────────
check_deps
echo -e "\n${BOLD}=== csvql vs DuckDB benchmark ===${RESET}"
echo -e "CSV:     $CSV"
echo -e "Rows:    $(( $(wc -l < "$CSV") - 1 )) data rows"
echo -e "DuckDB:  $($DUCKDB --version 2>&1 | head -1)"
echo -e "csvql:   $($CSVQL --version 2>&1 | head -1 || echo 'dev build')"
echo ""

# ── Query 1: COUNT(*) per department ────────────────────────────
run_query \
  "COUNT(*) GROUP BY department" \
  "SELECT department, COUNT(*) FROM '${CSV}' GROUP BY department ORDER BY department" \
  "SELECT department, COUNT(*) FROM read_csv_auto('${CSV}') GROUP BY department ORDER BY department"

# ── Query 2: COUNT(*) with WHERE ────────────────────────────────
run_query \
  "COUNT(*) WHERE age > 30 GROUP BY department" \
  "SELECT department, COUNT(*) FROM '${CSV}' WHERE age > 30 GROUP BY department ORDER BY department" \
  "SELECT department, COUNT(*) FROM read_csv_auto('${CSV}') WHERE age > 30 GROUP BY department ORDER BY department"

# ── Query 3: DISTINCT city ───────────────────────────────────────
run_query \
  "SELECT DISTINCT city" \
  "SELECT DISTINCT city FROM '${CSV}' ORDER BY city" \
  "SELECT DISTINCT city FROM read_csv_auto('${CSV}') ORDER BY city"

# ── Query 4: DISTINCT department ────────────────────────────────
run_query \
  "SELECT DISTINCT department" \
  "SELECT DISTINCT department FROM '${CSV}' ORDER BY department" \
  "SELECT DISTINCT department FROM read_csv_auto('${CSV}') ORDER BY department"

# ── Query 5: DISTINCT city + department (no ORDER BY in csvql — sort for hash) ──
run_query \
  "SELECT DISTINCT city, department" \
  "SELECT DISTINCT city, department FROM '${CSV}'" \
  "SELECT DISTINCT city, department FROM read_csv_auto('${CSV}')"

# ── Query 6: DISTINCT with WHERE ────────────────────────────────
run_query \
  "SELECT DISTINCT department WHERE salary > 100000" \
  "SELECT DISTINCT department FROM '${CSV}' WHERE salary > 100000 ORDER BY department" \
  "SELECT DISTINCT department FROM read_csv_auto('${CSV}') WHERE salary > 100000 ORDER BY department"

# ── Query 7: SUM + AVG per department ───────────────────────────
run_query \
  "SUM(salary) AVG(salary) GROUP BY department" \
  "SELECT department, SUM(salary), AVG(salary) FROM '${CSV}' GROUP BY department ORDER BY department" \
  "SELECT department, SUM(salary), AVG(salary) FROM read_csv_auto('${CSV}') GROUP BY department ORDER BY department"

# ── Query 8: COUNT(*) GROUP BY city ─────────────────────────────
run_query \
  "COUNT(*) GROUP BY city ORDER BY city" \
  "SELECT city, COUNT(*) FROM '${CSV}' GROUP BY city ORDER BY city" \
  "SELECT city, COUNT(*) FROM read_csv_auto('${CSV}') GROUP BY city ORDER BY city"

# ── Query 9: scalar COUNT(*) ─────────────────────────────────────
run_query \
  "SELECT COUNT(*)" \
  "SELECT COUNT(*) FROM '${CSV}'" \
  "SELECT COUNT(*) FROM read_csv_auto('${CSV}')"

# ── Query 10: scalar SUM ─────────────────────────────────────────
run_query \
  "SELECT SUM(salary)" \
  "SELECT SUM(salary) FROM '${CSV}'" \
  "SELECT SUM(salary) FROM read_csv_auto('${CSV}')"

# ── Query 11: scalar AVG ─────────────────────────────────────────
run_query \
  "SELECT AVG(salary)" \
  "SELECT AVG(salary) FROM '${CSV}'" \
  "SELECT AVG(salary) FROM read_csv_auto('${CSV}')"

# ── Query 12: scalar MIN + MAX ───────────────────────────────────
run_query \
  "SELECT MIN(age), MAX(age)" \
  "SELECT MIN(age), MAX(age) FROM '${CSV}'" \
  "SELECT MIN(age), MAX(age) FROM read_csv_auto('${CSV}')"

# ── Query 13: scalar COUNT(*) with WHERE ─────────────────────────
run_query \
  "SELECT COUNT(*) WHERE age > 30" \
  "SELECT COUNT(*) FROM '${CSV}' WHERE age > 30" \
  "SELECT COUNT(*) FROM read_csv_auto('${CSV}') WHERE age > 30"
echo -e "${BOLD}=== Done ===${RESET}\n"