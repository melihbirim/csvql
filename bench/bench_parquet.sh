#!/usr/bin/env bash
# =============================================================================
# bench_parquet.sh — csvql on raw CSV vs "convert to Parquet, then query"
#
# The workflow this measures: most people don't query a CSV directly, they
# convert it to Parquet first for size and speed, then point DuckDB/pandas at
# the Parquet file. That conversion is a real, one-time cost — this script
# counts it instead of hiding it, same principle as bench_taxi.sh counting
# DuckDB's native-store build time rather than only showing its fast path.
#
# Three numbers per query:
#   csvql          — query the raw CSV directly, zero conversion, every run
#   parquet (cold)  — CSV→Parquet conversion + DuckDB query on the Parquet file,
#                      i.e. what "just convert it" actually costs on run #1
#   parquet (warm)  — DuckDB query on an already-converted Parquet file,
#                      i.e. the payoff once the conversion is amortized
#
# Usage:
#   ./bench/bench_parquet.sh              # uses bench/.taxi-data/sample.csv
#   ./bench/bench_parquet.sh /path/to.csv
#   N=5 ./bench/bench_parquet.sh          # best-of-5 runs per query (default 3)
#
# Needs: csvql built (zig build -Doptimize=ReleaseFast), duckdb on PATH.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
CSVQL="$(command -v csvql || echo "$ROOT/zig-out/bin/csvql")"
DUCKDB="${DUCKDB_BIN:-duckdb}"
RUNS="${N:-3}"

CSV="${1:-$SCRIPT_DIR/.taxi-data/sample.csv}"
PARQUET="$SCRIPT_DIR/.bench_tmp/taxi_sample.parquet"
mkdir -p "$SCRIPT_DIR/.bench_tmp"

CYAN='\033[0;36m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'
header() { echo; echo -e "${BOLD}${CYAN}━━━ $* ━━━${NC}"; }

[ -x "$CSVQL" ] || { echo "csvql not found (build: zig build -Doptimize=ReleaseFast)"; exit 1; }
command -v "$DUCKDB" >/dev/null || { echo "duckdb not found on PATH"; exit 1; }
[ -f "$CSV" ] || { echo "CSV not found: $CSV"; echo "Get the sample first: ./bench/bench_taxi.sh --sample"; exit 1; }

CSV_SIZE=$(du -h "$CSV" | cut -f1)

best_of() {
  # best_of <n> <cmd...>  — runs cmd n times, prints the min wall time in seconds
  local n="$1"; shift
  local best=""
  for _ in $(seq 1 "$n"); do
    local t0 t1 dt
    t0=$(date +%s.%N)
    "$@" >/dev/null 2>&1
    t1=$(date +%s.%N)
    dt=$(echo "$t1 - $t0" | bc)
    if [ -z "$best" ] || (( $(echo "$dt < $best" | bc -l) )); then best="$dt"; fi
  done
  echo "$best"
}

header "csvql (raw CSV, zero conversion) vs Parquet (conversion + query)"
echo "  CSV:     $CSV  ($CSV_SIZE)"
echo "  csvql:   $("$CSVQL" --version 2>&1 || echo unknown)"
echo "  DuckDB:  $("$DUCKDB" --version 2>/dev/null | head -1)"
echo "  Runs:    best-of-$RUNS per number"

rm -f "$PARQUET"

# ── One-time conversion cost, measured once, reported separately ──────────
header "CSV → Parquet conversion (one-time cost)"
CONV_T0=$(date +%s.%N)
"$DUCKDB" -c "COPY (SELECT * FROM read_csv_auto('$CSV')) TO '$PARQUET' (FORMAT PARQUET)"
CONV_T1=$(date +%s.%N)
CONV_TIME=$(echo "$CONV_T1 - $CONV_T0" | bc)
PARQUET_SIZE=$(du -h "$PARQUET" | cut -f1)
printf "  Conversion time: ${YELLOW}%.3fs${NC}\n" "$CONV_TIME"
echo "  CSV size:        $CSV_SIZE"
echo "  Parquet size:     $PARQUET_SIZE"

declare -a LABELS=(
  "COUNT(*) GROUP BY cab_type"
  "AVG(total_amount) GROUP BY passenger_count"
  "COUNT(*) WHERE trip_distance > 5"
  "Top 3 passenger_count by AVG(tip_amount)"
)
declare -a CSVQL_SQL=(
  "SELECT cab_type, COUNT(*) FROM '$CSV' GROUP BY cab_type"
  "SELECT passenger_count, AVG(total_amount) FROM '$CSV' GROUP BY passenger_count"
  "SELECT COUNT(*) FROM '$CSV' WHERE trip_distance > 5"
  "SELECT passenger_count, AVG(tip_amount) AS a FROM '$CSV' GROUP BY passenger_count ORDER BY a DESC LIMIT 3"
)
declare -a PARQUET_SQL=(
  "SELECT cab_type, COUNT(*) FROM read_parquet('$PARQUET') GROUP BY cab_type"
  "SELECT passenger_count, AVG(total_amount) FROM read_parquet('$PARQUET') GROUP BY passenger_count"
  "SELECT COUNT(*) FROM read_parquet('$PARQUET') WHERE trip_distance > 5"
  "SELECT passenger_count, AVG(tip_amount) AS a FROM read_parquet('$PARQUET') GROUP BY passenger_count ORDER BY a DESC LIMIT 3"
)

header "Query speed"
printf "  %-45s %10s %12s %12s\n" "query" "csvql" "parquet(warm)" "+conversion"

for i in "${!LABELS[@]}"; do
  CSVQL_T=$(best_of "$RUNS" "$CSVQL" "${CSVQL_SQL[$i]}")
  PARQUET_T=$(best_of "$RUNS" "$DUCKDB" -c "${PARQUET_SQL[$i]}")
  COLD_T=$(echo "$PARQUET_T + $CONV_TIME" | bc)
  printf "  %-45s %9.3fs %11.3fs %11.3fs\n" "${LABELS[$i]}" "$CSVQL_T" "$PARQUET_T" "$COLD_T"
done

echo
echo -e "  ${DIM}\"parquet(warm)\" = query only, conversion already paid and amortized.${NC}"
echo -e "  ${DIM}\"+conversion\"   = what run #1 actually costs: convert once, then query.${NC}"
echo -e "  ${DIM}csvql pays neither — it queries the raw CSV directly, every run.${NC}"
echo
