#!/usr/bin/env bash
# =============================================================================
# bench_datafusion.sh — csvql vs Apache DataFusion on the canonical NYC Taxi
# benchmark
#
# Same queries as bench_taxi.sh (csvql vs DuckDB) — this is the same fixture
# and query set, run against DataFusion instead. DataFusion (not raw pyarrow)
# is the fair "Apache Arrow" comparison for csvql specifically: it's Apache
# Arrow's own SQL engine (GROUP BY, JOIN, etc., all executed over Arrow's
# columnar in-memory format) — pyarrow itself has no SQL layer to compare
# against. Reuses bench_taxi.sh's cached fixture at bench/.taxi-data/ rather
# than re-downloading; run bench_taxi.sh --sample first if that cache is
# empty.
#
# Fair fight: both engines query the raw uncompressed CSV directly (no
# preload into a native store). Both run cold per invocation, both parallel.
#
# Usage:
#   ./bench/bench_datafusion.sh              # 1 file  (20M rows, ~8 GB CSV)
#   ./bench/bench_datafusion.sh --sample     # 1M-row sample (~417 MB)
#   N=5 ./bench/bench_datafusion.sh --sample # best-of-5 runs per query
#
# Needs: csvql on PATH (or built at zig-out/bin), datafusion-cli
# (cargo install datafusion-cli), the bench_taxi.sh fixture already cached.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
CSVQL="$(command -v csvql || echo "$ROOT/zig-out/bin/csvql")"
DATA_DIR="$SCRIPT_DIR/.taxi-data"
RUNS="${N:-3}"

CYAN='\033[0;36m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BOLD='\033[1m'; NC='\033[0m'
header() { echo; echo -e "${BOLD}${CYAN}━━━ $* ━━━${NC}"; }
info()   { echo -e "  ${YELLOW}→${NC} $*"; }

# --resources: measure peak memory / CPU instead of query speed (macOS `time -l`).
RESOURCES=0
if [ "${1:-}" = "--resources" ]; then RESOURCES=1; shift; fi

command -v datafusion-cli >/dev/null || { echo "datafusion-cli not found on PATH (cargo install datafusion-cli)"; exit 1; }
[ -x "$CSVQL" ] || { echo "csvql not found (build with: zig build -Doptimize=ReleaseFast)"; exit 1; }

if [ "${1:-}" = "--sample" ]; then
    CSV="$DATA_DIR/sample.csv"
else
    CSV="$DATA_DIR/trips.csv"
fi
[ -f "$CSV" ] || { echo "Fixture not found: $CSV — run bench_taxi.sh${1:+ $1} first to build/download it."; exit 1; }

ROWS=$(( $(wc -l < "$CSV") - 1 ))
SIZE=$(du -h "$CSV" | cut -f1)
header "csvql vs Apache DataFusion — direct raw-CSV query — $ROWS rows, $SIZE, best-of-$RUNS"

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

# Only Q01/Q02 — Q03/Q04 (STRFTIME/date_part over pickup_datetime) are
# deliberately excluded, not forgotten. This fixture has genuinely mixed
# datetime formats ("2012-08-31 22:00:00" and "2011-04-01T05:47:46").
# DuckDB and csvql are both schema-less/string-based for this column and
# handle both formats uniformly (verified identical output). DataFusion's
# CSV reader infers pickup_datetime as a native Timestamp(s) column from
# a sample, and rows in whichever format DIDN'T match that inferred
# schema silently disappear from GROUP BY results (confirmed via `diff`:
# real, missing year groups, not a formatting nit) — a genuine DataFusion
# behavior difference on messy real-world data, not a csvql bug, and not
# yet root-caused deeply enough to call it "DataFusion is wrong" either.
# Publishing Q03/Q04 numbers before that's understood would be comparing
# a wrong answer's speed to a right one's — see CORRECTNESS.md's own
# discipline on this. Logged as an open finding, not swept under the rug.
declare -a NAMES=(Q01 Q02)
declare -a CQ=(
  "SELECT cab_type, COUNT(*) FROM '$CSV' GROUP BY cab_type"
  "SELECT passenger_count, AVG(total_amount) FROM '$CSV' GROUP BY passenger_count"
)
declare -a DFQ=(
  "SELECT cab_type, COUNT(*) FROM '$CSV' GROUP BY cab_type"
  "SELECT passenger_count, AVG(total_amount) FROM '$CSV' GROUP BY passenger_count"
)

if [ "$RESOURCES" = 1 ]; then
    printf "\n  %-5s %-11s %9s %9s %6s\n" q engine real peakMB CPU
    for i in "${!NAMES[@]}"; do
        read -r cr cm cc < <(measure "$CSVQL" "${CQ[$i]}")
        read -r dr dm dc < <(measure datafusion-cli --format csv -q -c "${DFQ[$i]}")
        printf "  ${BOLD}%-5s${NC} %-11s %8ss %8sM %5s%%\n" "${NAMES[$i]}" csvql "$cr" "$cm" "$cc"
        printf "  %-5s %-11s %8ss %8sM %5s%%\n" "" datafusion "$dr" "$dm" "$dc"
    done
    echo
    info "Note: single cold run per query; peak = macOS 'peak memory footprint'. CPU>100% = multi-core."
else
    printf "\n  %-5s %10s %10s %8s\n" q csvql datafusion ratio
    for i in "${!NAMES[@]}"; do
        c=$(best "$CSVQL" "${CQ[$i]}")
        d=$(best datafusion-cli --format csv -q -c "${DFQ[$i]}")
        r=$(echo "scale=2; $d/$c" | bc)
        printf "  ${BOLD}%-5s${NC} %9ss %9ss ${GREEN}%6sx${NC}\n" "${NAMES[$i]}" "$c" "$d" "$r"
    done
    echo
    info "Note: OS page-cache is warm on best-of-N (both engines equally). First cold run is slower."
fi
info "Q03/Q04 excluded — see script comments above: a real DataFusion mixed-datetime-format correctness gap found while verifying, not yet included pending that being understood."
