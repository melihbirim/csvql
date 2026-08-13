#!/usr/bin/env bash
# =============================================================================
# bench_qsv.sh — Single-thread raw parse throughput: csvql vs qsv
#
# Compares row-counting speed on the same file, same machine, same run.
# qsv (https://github.com/dathere/qsv) is a Rust CSV toolkit built on the
# `csv` crate — one of the fastest CSV parsers in common use, so it's a
# meaningful reference point for "how fast is our parser, really."
#
# Requires qsv on PATH: brew install qsv
#
# Usage:
#   ./bench/bench_qsv.sh [CSV_FILE]
#
# Default CSV: large_test.csv
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
CSVQL="$ROOT/zig-out/bin/csvql"
CSV="${1:-$ROOT/large_test.csv}"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

if ! command -v qsv >/dev/null 2>&1; then
    echo "qsv not found. Install with: brew install qsv"
    exit 1
fi
if [ ! -x "$CSVQL" ]; then
    echo "csvql binary not found at $CSVQL — run: zig build -Doptimize=ReleaseFast"
    exit 1
fi
if [ ! -f "$CSV" ]; then
    echo "File not found: $CSV"
    exit 1
fi

SIZE_MB=$(($(stat -f%z "$CSV" 2>/dev/null || stat -c%s "$CSV") / 1024 / 1024))
echo -e "${BOLD}File: $CSV (${SIZE_MB} MB)${NC}"
echo

run() {
    local label="$1"; shift
    local start end elapsed
    start=$(date +%s.%N)
    "$@" >/dev/null
    end=$(date +%s.%N)
    elapsed=$(echo "$end - $start" | bc)
    local mbps
    mbps=$(echo "scale=0; $SIZE_MB / $elapsed" | bc)
    printf "  %-28s %7.2fs   %5s MB/s\n" "$label" "$elapsed" "$mbps"
}

echo -e "${YELLOW}Row count, single-thread comparison:${NC}"
run "qsv count" qsv count --no-headers "$CSV"
run "csvql COUNT(*)" "$CSVQL" "SELECT COUNT(*) FROM '$CSV'"

echo
echo -e "${GREEN}Note:${NC} csvql parallelizes queries across all cores by default;"
echo "this is a raw single-run wall-clock comparison, not single-thread-pinned."
echo "For an isolated single-thread parser number use: zig build bench -- $CSV"
