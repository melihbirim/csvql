#!/usr/bin/env bash
# =============================================================================
# bench_taxi.sh — csvql vs DuckDB on the canonical NYC Taxi benchmark
#
# Same dataset + queries DuckDB uses to benchmark themselves:
#   https://duckdb.org/2024/10/16/driving-csv-performance-benchmarking-duckdb-with-the-nyc-taxi-dataset
#   data:    https://blobs.duckdb.org/data/nyc-taxi-dataset/ (DuckDB's own blobs)
#   queries: https://github.com/pdet/taxi-benchmark  (Billion Taxi Rides)
#
# Fair fight: BOTH engines query the raw uncompressed CSV directly (no preload
# into a native store). Both run cold per invocation, both parallel.
#
# Usage:
#   ./bench/bench_taxi.sh              # 1 file  (20M rows, ~8 GB CSV)
#   ./bench/bench_taxi.sh 3            # 3 files (60M rows, ~24 GB CSV)
#   ./bench/bench_taxi.sh --sample     # 1M-row sample (~417 MB), quick smoke test
#   N=5 ./bench/bench_taxi.sh          # best-of-5 runs per query (default 3)
#
# Needs: csvql on PATH (or built at zig-out/bin), duckdb, curl, gzip. ~8 GB free
# per file. Downloaded data cached under bench/.taxi-data/ (delete to re-fetch).
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
CSVQL="$(command -v csvql || echo "$ROOT/zig-out/bin/csvql")"
DATA_DIR="$SCRIPT_DIR/.taxi-data"
RUNS="${N:-3}"
BLOB="https://blobs.duckdb.org/data/nyc-taxi-dataset"
# 65 files exist: trips_xaa .. trips_xcm. We use the first N.
FILE_IDS=(xaa xab xac xad xae xaf xag xah xai xaj xak xal xam xan xao xap)

CYAN='\033[0;36m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BOLD='\033[1m'; NC='\033[0m'
header() { echo; echo -e "${BOLD}${CYAN}━━━ $* ━━━${NC}"; }
info()   { echo -e "  ${YELLOW}→${NC} $*"; }

# --resources: measure peak memory / CPU / storage instead of query speed (macOS `time -l`).
RESOURCES=0
if [ "${1:-}" = "--resources" ]; then RESOURCES=1; shift; fi

command -v duckdb >/dev/null || { echo "duckdb not found on PATH"; exit 1; }
[ -x "$CSVQL" ] || { echo "csvql not found (build with: zig build -Doptimize=ReleaseFast)"; exit 1; }

# 51-column header (from taxi-benchmark schema.sql) — the CSV files are headerless.
HEADER='trip_id,vendor_id,pickup_datetime,dropoff_datetime,store_and_fwd_flag,rate_code_id,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,ehail_fee,improvement_surcharge,total_amount,payment_type,trip_type,pickup,dropoff,cab_type,precipitation,snow_depth,snowfall,max_temperature,min_temperature,average_wind_speed,pickup_nyct2010_gid,pickup_ctlabel,pickup_borocode,pickup_boroname,pickup_ct2010,pickup_boroct2010,pickup_cdeligibil,pickup_ntacode,pickup_ntaname,pickup_puma,dropoff_nyct2010_gid,dropoff_ctlabel,dropoff_borocode,dropoff_boroname,dropoff_ct2010,dropoff_boroct2010,dropoff_cdeligibil,dropoff_ntacode,dropoff_ntaname,dropoff_puma'

mkdir -p "$DATA_DIR"
CSV="$DATA_DIR/trips.csv"

# ── Build the CSV (cached) ───────────────────────────────────────────────────
if [ "${1:-}" = "--sample" ]; then
    if [ ! -f "$DATA_DIR/sample.csv" ]; then
        header "Fetching 1M-row sample (~417 MB)"
        # head closes the pipe early → SIGPIPE on curl/gzip; expected, don't let pipefail abort.
        set +o pipefail
        { echo "$HEADER"; curl -fsSL "$BLOB/trips_xaa.csv.gz" | gzip -dc | head -1000000; } > "$DATA_DIR/sample.csv"
        set -o pipefail
    fi
    CSV="$DATA_DIR/sample.csv"
else
    NFILES="${1:-1}"
    if [ ! -f "$CSV" ] || [ "${TAXI_FILES_BUILT:-0}" != "$NFILES" ]; then
        header "Building CSV from $NFILES file(s) (~$((NFILES * 8)) GB) — cached in $DATA_DIR"
        : > "$CSV"
        echo "$HEADER" > "$CSV"
        for i in $(seq 0 $((NFILES - 1))); do
            id="${FILE_IDS[$i]}"
            info "trips_$id.csv.gz"
            curl -fsSL "$BLOB/trips_$id.csv.gz" | gzip -dc >> "$CSV"
        done
    fi
fi

ROWS=$(( $(wc -l < "$CSV") - 1 ))
SIZE=$(du -h "$CSV" | cut -f1)
header "csvql vs DuckDB — direct raw-CSV query — $ROWS rows, $SIZE, best-of-$RUNS"

DK="read_csv_auto('$CSV')"
export RUNS
best() {  # best (min) wall-clock over $RUNS runs. python3 timer — portable (macOS date lacks %N).
    python3 - "$@" <<'PY'
import subprocess, sys, os, time
cmd = sys.argv[1:]
b = float("inf")
for _ in range(int(os.environ.get("RUNS", "3"))):
    t = time.perf_counter()
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    b = min(b, time.perf_counter() - t)
print(f"{b:.3f}")
PY
}

# measure CMD... -> "real_s peak_mb cpu_pct" for a single cold run (macOS `time -l`).
measure() {
    local tf; tf=$(mktemp)
    /usr/bin/time -l "$@" >/dev/null 2>"$tf" || true
    local real user sys peak
    real=$(awk '/ real /{print $1; exit}' "$tf")
    user=$(awk '{for (i=1;i<=NF;i++) if ($i=="user") print $(i-1)}' "$tf" | head -1)
    sys=$(awk '{for (i=1;i<=NF;i++) if ($i=="sys") print $(i-1)}' "$tf" | head -1)
    peak=$(awk '/peak memory footprint/{print $1; exit}' "$tf")            # macOS (bytes)
    [ -z "$peak" ] && peak=$(awk '/maximum resident set size/{print $1; exit}' "$tf")
    rm -f "$tf"
    echo "$real $(echo "scale=1; $peak/1048576" | bc) $(echo "scale=0; ($user+$sys)/$real*100" | bc)"
}

# Canonical Billion-Taxi-Rides queries (csvql uses STRFTIME where DuckDB uses DATE_PART).
declare -a NAMES=(Q01 Q02 Q03 Q04)
declare -a CQ=(
  "SELECT cab_type, COUNT(*) FROM '$CSV' GROUP BY cab_type"
  "SELECT passenger_count, AVG(total_amount) FROM '$CSV' GROUP BY passenger_count"
  "SELECT passenger_count, STRFTIME('%Y',pickup_datetime) AS y, COUNT(*) FROM '$CSV' GROUP BY passenger_count, y"
  "SELECT passenger_count, STRFTIME('%Y',pickup_datetime) AS y, ROUND(trip_distance) AS d, COUNT(*) AS c FROM '$CSV' GROUP BY passenger_count, y, d ORDER BY y, c DESC"
)
declare -a DQ=(
  "SELECT cab_type, COUNT(*) FROM $DK GROUP BY cab_type"
  "SELECT passenger_count, AVG(total_amount) FROM $DK GROUP BY passenger_count"
  "SELECT passenger_count, DATE_PART('year',pickup_datetime) AS y, COUNT(*) FROM $DK GROUP BY passenger_count, y"
  "SELECT passenger_count, DATE_PART('year',pickup_datetime) AS y, ROUND(trip_distance) AS d, COUNT(*) AS c FROM $DK GROUP BY passenger_count, y, d ORDER BY y, c DESC"
)

if [ "$RESOURCES" = 1 ]; then
    printf "\n  %-5s %-8s %9s %9s %6s\n" q engine real peakMB CPU
    for i in "${!NAMES[@]}"; do
        read -r cr cm cc < <(measure "$CSVQL" "${CQ[$i]}")
        read -r dr dm dc < <(measure duckdb -csv -c "${DQ[$i]}")
        printf "  ${BOLD}%-5s${NC} %-8s %8ss %8sM %5s%%\n" "${NAMES[$i]}" csvql "$cr" "$cm" "$cc"
        printf "  %-5s %-8s %8ss %8sM %5s%%\n" "" duckdb "$dr" "$dm" "$dc"
    done

    header "Storage — extra disk required to answer these queries"
    info "csvql:  0 bytes — queries the raw CSV in place (mmap), no ingest"
    DB="$DATA_DIR/taxi.duckdb"; rm -f "$DB"
    t0=$(python3 -c 'import time;print(time.time())')
    duckdb "$DB" -c "CREATE TABLE trips AS SELECT * FROM read_csv_auto('$CSV');" >/dev/null 2>&1
    t1=$(python3 -c 'import time;print(time.time())')
    info "DuckDB: $(du -h "$DB" | cut -f1) native store, built in $(printf '%.1f' "$(echo "$t1-$t0" | bc)")s (the one-time cost of its fast 'with storage' path)"
    rm -f "$DB"
    echo
    info "Note: single cold run per query; peak = macOS 'peak memory footprint'. CPU>100% = multi-core."
else
    printf "\n  %-5s %10s %10s %8s\n" q csvql duckdb ratio
    for i in "${!NAMES[@]}"; do
        c=$(best "$CSVQL" "${CQ[$i]}")
        d=$(best duckdb -csv -c "${DQ[$i]}")
        r=$(echo "scale=2; $d/$c" | bc)
        printf "  ${BOLD}%-5s${NC} %9ss %9ss ${GREEN}%6sx${NC}\n" "${NAMES[$i]}" "$c" "$d" "$r"
    done
    echo
    info "Note: OS page-cache is warm on best-of-N (both engines equally). First cold run is slower."
fi
