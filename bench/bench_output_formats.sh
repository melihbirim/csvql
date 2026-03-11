#!/usr/bin/env bash
# bench_output_formats.sh — Compare csvql vs DuckDB throughput across output formats
#                            (CSV, JSON array, JSONL) on large files
#
# Usage:
#   ./bench/bench_output_formats.sh [csv_file]
#
# Requires: duckdb in PATH, csvql built (zig build -Doptimize=ReleaseFast)
# Output goes to /dev/null so disk I/O does not skew results.
#
# Results from 5M-row dataset (Apple M2, Zig 0.15.2, DuckDB v1.4.2):
#
#   Format      csvql avg   DuckDB avg   csvql speedup
#   CSV         0.100s      0.354s       3.5x
#   JSON array  0.164s      0.434s       2.6x
#   JSONL       0.172s      0.422s       2.5x
#
# Outputs are semantically/byte-identical to DuckDB (verified via Python + diff).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CSVQL="${SCRIPT_DIR}/zig-out/bin/csvql"
DUCKDB="${DUCKDB_BIN:-duckdb}"
CSV="${1:-/tmp/very_large_test.csv}"
RUNS="${BENCH_RUNS:-5}"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

check_deps() {
  if [[ ! -x "$CSVQL" ]]; then
    echo "csvql not found at $CSVQL — run: zig build -Doptimize=ReleaseFast"
    exit 1
  fi
  if ! command -v "$DUCKDB" >/dev/null 2>&1; then
    echo "duckdb not found in PATH (set DUCKDB_BIN=/path/to/duckdb to override)"
    exit 1
  fi
  if [[ ! -f "$CSV" ]]; then
    echo "CSV file not found: $CSV"
    echo "Pass a path as the first argument, e.g.:"
    echo "  $0 /tmp/very_large_test.csv"
    exit 1
  fi
}

# Run a command RUNS times (plus one warmup), print avg/min/max wall-clock seconds.
# Prints a single summary line to stdout; all command output is discarded.
bench_runs() {
  local label="$1"; shift
  local times_file
  times_file=$(mktemp)

  # Warmup (not counted)
  "$@" > /dev/null 2>&1 || true

  for ((i=0; i<RUNS; i++)); do
    { /usr/bin/time -p "$@" > /dev/null; } 2>/tmp/_bench_t || true
    awk '/^real /{print $2}' /tmp/_bench_t >> "$times_file"
  done

  awk -v lbl="$label" -v runs="$RUNS" \
    '{s+=$1; if(NR==1||$1<mn)mn=$1; if(NR==1||$1>mx)mx=$1}
     END{printf "  %-22s  avg=%s%.3fs%s  min=%.3fs  max=%.3fs  (%d runs)\n",
         lbl, "'"${CYAN}"'", s/NR, "'"${RESET}"'", mn, mx, runs}' "$times_file"

  rm -f "$times_file"
}

# Run both tools for one format, then print the speedup.
bench_format() {
  local format_label="$1"
  local csvql_extra="$2"          # extra csvql flag(s), e.g. "--json"
  local duck_copy_opts="$3"       # DuckDB COPY TO options, e.g. "FORMAT JSON, ARRAY true"
  local csvql_sql="$4"
  local duck_inner_sql="$5"

  printf "\n${BOLD}%s${RESET}\n" "$format_label"

  # Capture avg timing for speedup calculation
  local cq_avg dk_avg

  cq_avg=$(
    times_file=$(mktemp)
    "$CSVQL" $csvql_extra "$csvql_sql" > /dev/null 2>&1 || true   # warmup
    for ((i=0; i<RUNS; i++)); do
      { /usr/bin/time -p $CSVQL $csvql_extra "$csvql_sql" > /dev/null; } 2>/tmp/_bench_t || true
      awk '/^real /{print $2}' /tmp/_bench_t >> "$times_file"
    done
    awk '{s+=$1;if(NR==1||$1<mn)mn=$1;if(NR==1||$1>mx)mx=$1}END{printf "%.3f %.3f %.3f",s/NR,mn,mx}' "$times_file"
    rm -f "$times_file"
  )
  dk_avg=$(
    times_file=$(mktemp)
    "$DUCKDB" -c "COPY ($duck_inner_sql) TO '/dev/null' ($duck_copy_opts)" > /dev/null 2>&1 || true  # warmup
    for ((i=0; i<RUNS; i++)); do
      { /usr/bin/time -p "$DUCKDB" -c "COPY ($duck_inner_sql) TO '/dev/null' ($duck_copy_opts)" > /dev/null; } 2>/tmp/_bench_t || true
      awk '/^real /{print $2}' /tmp/_bench_t >> "$times_file"
    done
    awk '{s+=$1;if(NR==1||$1<mn)mn=$1;if(NR==1||$1>mx)mx=$1}END{printf "%.3f %.3f %.3f",s/NR,mn,mx}' "$times_file"
    rm -f "$times_file"
  )

  read -r cq_a cq_mn cq_mx <<< "$cq_avg"
  read -r dk_a dk_mn dk_mx <<< "$dk_avg"

  local speedup
  speedup=$(awk -v cq="$cq_a" -v dk="$dk_a" 'BEGIN{if(cq<=0)cq=0.001; printf "%.2f", dk/cq}')

  printf "  %-22s  avg=${CYAN}%.3fs${RESET}  min=%.3fs  max=%.3fs  (%d runs)\n" \
    "csvql $csvql_extra" "$cq_a" "$cq_mn" "$cq_mx" "$RUNS"
  printf "  %-22s  avg=${CYAN}%.3fs${RESET}  min=%.3fs  max=%.3fs  (%d runs)\n" \
    "duckdb" "$dk_a" "$dk_mn" "$dk_mx" "$RUNS"
  printf "  speedup: ${GREEN}%sx${RESET} faster (csvql vs duckdb)\n" "$speedup"
}

# ─────────────────────────────────────────────────────────────────
check_deps

ROW_COUNT=$(( $(wc -l < "$CSV") - 1 ))
FILE_SIZE_MB=$(du -m "$CSV" | awk '{print $1}')

echo ""
printf "${BOLD}=== csvql vs DuckDB — output format benchmark ===${RESET}\n"
printf "CSV:     %s\n" "$CSV"
printf "Rows:    %d data rows  (%d MB)\n" "$ROW_COUNT" "$FILE_SIZE_MB"
printf "DuckDB:  %s\n" "$("$DUCKDB" --version 2>&1 | head -1)"
printf "csvql:   %s\n" "$("$CSVQL" --version 2>&1 | head -1 || echo 'dev build')"
printf "Runs:    %d per measurement (+ 1 warmup, output → /dev/null)\n" "$RUNS"

Q_CSVQL="SELECT id, name, age, city, salary, department FROM '${CSV}' WHERE age > 30"
Q_DUCK="SELECT id, name, age, city, salary, department FROM '${CSV}' WHERE age > 30"

bench_format \
  "── CSV ──────────────────────────────────────────" \
  "" \
  "HEADER, DELIMITER ','" \
  "$Q_CSVQL" \
  "$Q_DUCK"

bench_format \
  "── JSON array (--json) ──────────────────────────" \
  "--json" \
  "FORMAT JSON, ARRAY true" \
  "$Q_CSVQL" \
  "$Q_DUCK"

bench_format \
  "── JSONL / NDJSON (--jsonl) ─────────────────────" \
  "--jsonl" \
  "FORMAT JSON" \
  "$Q_CSVQL" \
  "$Q_DUCK"

printf "\n${BOLD}=== Done ===${RESET}\n\n"
