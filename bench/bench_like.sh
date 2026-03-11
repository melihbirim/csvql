#!/usr/bin/env bash
# bench_like.sh — Compare csvql vs DuckDB on LIKE pattern-matching queries
# Usage: ./bench/bench_like.sh [csv_file]
# Requires: duckdb in PATH, csvql built (zig build -Doptimize=ReleaseFast)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CSVQL="${SCRIPT_DIR}/zig-out/bin/csvql"
DUCKDB="${DUCKDB_BIN:-duckdb}"
CSV="${1:-/tmp/very_large_test.csv}"
BENCH_DIR="${SCRIPT_DIR}/.bench_tmp"
mkdir -p "$BENCH_DIR"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

RUNS=5

check_deps() {
  [[ -x "$CSVQL" ]] || { echo "csvql not found at $CSVQL — run: zig build -Doptimize=ReleaseFast"; exit 1; }
  command -v "$DUCKDB" >/dev/null || { echo "duckdb not found in PATH"; exit 1; }
  [[ -f "$CSV" ]] || {
    echo "CSV not found: $CSV"
    echo "Generate it with: zig run generate_large_csv.zig -- 5000000 > /tmp/very_large_test.csv"
    exit 1
  }
}

# Run command N times, collect real-time via /usr/bin/time -p, return avg seconds
bench_avg() {
  local n="$RUNS" total=0
  # warmup
  "$@" >/dev/null 2>&1 || true
  for ((i=0; i<n; i++)); do
    local t
    t=$( { /usr/bin/time -p "$@" >/dev/null; } 2>&1 | awk '/^real/{printf "%.3f", $2}' ) || true
    [[ -n "$t" ]] || t="0.000"
    total=$(awk -v s="$total" -v v="$t" 'BEGIN{printf "%.3f", s+v}')
  done
  awk -v s="$total" -v n="$n" 'BEGIN{printf "%.3f", s/n}'
}

# Run command N times, return min seconds
bench_min() {
  local n="$RUNS" min=""
  # warmup
  "$@" >/dev/null 2>&1 || true
  for ((i=0; i<n; i++)); do
    local t
    t=$( { /usr/bin/time -p "$@" >/dev/null; } 2>&1 | awk '/^real/{printf "%.3f", $2}' ) || true
    [[ -n "$t" ]] || t="0.000"
    if [[ -z "$min" ]]; then
      min="$t"
    else
      min=$(awk -v a="$min" -v b="$t" 'BEGIN{printf "%.3f", (a<b)?a:b}')
    fi
  done
  echo "$min"
}

run_query() {
  local label="$1" csvql_sql="$2" duck_sql="$3"
  local cq_out="${BENCH_DIR}/csvql_like_$(echo "$label" | tr ' %/' '___').csv"
  local dk_out="${BENCH_DIR}/duck_like_$(echo "$label"  | tr ' %/' '___').csv"

  # --- capture output for row count + correctness ---
  "$CSVQL" "$csvql_sql" > "$cq_out" 2>/dev/null || true
  "$DUCKDB" -csv -c "$duck_sql" > "$dk_out" 2>/dev/null || true

  local cq_rows; cq_rows=$(( $(wc -l < "$cq_out") - 1 ))
  local dk_rows; dk_rows=$(( $(wc -l < "$dk_out") - 1 ))

  # correctness: row counts
  local match_icon
  if [[ "$cq_rows" -eq "$dk_rows" ]]; then
    match_icon="${GREEN}ROWS-OK (${cq_rows})${RESET}"
  else
    match_icon="${RED}MISMATCH${RESET} (csvql=${cq_rows} duckdb=${dk_rows})"
  fi

  # --- timing ---
  local cq_avg; cq_avg=$(bench_avg "$CSVQL" "$csvql_sql")
  local cq_min; cq_min=$(bench_min "$CSVQL" "$csvql_sql")
  local dk_avg; dk_avg=$(bench_avg "$DUCKDB" -csv -c "$duck_sql")
  local dk_min; dk_min=$(bench_min "$DUCKDB" -csv -c "$duck_sql")

  # speedup (avg-over-avg)
  local speedup
  speedup=$(awk -v cq="$cq_avg" -v dk="$dk_avg" 'BEGIN{if(cq<=0)cq=0.001; printf "%.1f", dk/cq}')

  printf "${BOLD}%-60s${RESET}\n" "$label"
  printf "  %-12s  avg: ${CYAN}%ss${RESET}  min: ${CYAN}%ss${RESET}  rows: %-8d\n" "csvql"  "$cq_avg" "$cq_min" "$cq_rows"
  printf "  %-12s  avg: ${CYAN}%ss${RESET}  min: ${CYAN}%ss${RESET}  rows: %-8d\n" "duckdb" "$dk_avg" "$dk_min" "$dk_rows"
  printf "  result: %b  speedup: ${YELLOW}%sx${RESET} (csvql vs duckdb, avg)\n\n" "$match_icon" "$speedup"

  # Show first 4 output rows from csvql
  echo -e "  ${BOLD}csvql sample (first 4 data rows):${RESET}"
  head -5 "$cq_out" | sed 's/^/    /'
  echo ""
}

# ─────────────────────────────────────────────────────────────────
check_deps
echo -e "\n${BOLD}=== csvql vs DuckDB — LIKE operator benchmark ===${RESET}"
echo -e "CSV:     $CSV"
echo -e "Rows:    $(( $(wc -l < "$CSV") - 1 )) data rows"
echo -e "Runs:    $RUNS per tool (+ 1 warmup)"
echo -e "DuckDB:  $($DUCKDB --version 2>&1 | head -1)"
echo -e "csvql:   $($CSVQL --version 2>&1 | head -1 || echo 'dev build')"
echo ""

# ── Query 1: prefix wildcard ─────────────────────────────────────
run_query \
  "WHERE name LIKE 'A%'  (prefix wildcard)" \
  "SELECT * FROM '${CSV}' WHERE name LIKE 'A%'" \
  "SELECT * FROM read_csv_auto('${CSV}') WHERE name LIKE 'A%'"

# ── Query 2: suffix wildcard ─────────────────────────────────────
run_query \
  "WHERE city LIKE '%on'  (suffix wildcard)" \
  "SELECT * FROM '${CSV}' WHERE city LIKE '%on'" \
  "SELECT * FROM read_csv_auto('${CSV}') WHERE city LIKE '%on'"

# ── Query 3: suffix wildcard, high selectivity ───────────────────
run_query \
  "WHERE department LIKE '%ing'  (suffix, high selectivity)" \
  "SELECT * FROM '${CSV}' WHERE department LIKE '%ing'" \
  "SELECT * FROM read_csv_auto('${CSV}') WHERE department LIKE '%ing'"

echo -e "${BOLD}Done.${RESET} Temp files in ${BENCH_DIR}"
