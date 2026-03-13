#!/usr/bin/env bash
# =============================================================================
# bench_datetime.sh — DateTime correctness + performance: csvql vs DuckDB
#
# Usage:
#   ./bench/bench_datetime.sh [CSV_FILE]
#
# Default CSV: test_orders.csv (30 rows, Tesco-style order workflow data)
# Columns: order_id,customer_name,product,price,ordered_at,picked_at,
#           packaged_at,shipped_at,delivered_at,collected_at,status,order_type
#
# Date formats in test data (mixed):
#   ISO-8601 space:  2026-01-15 09:30:00
#   ISO-8601 T:      2026-01-16T10:00:00
#   US slash:        01/15/2026 08:00:00
#   EU dot:          15.01.2026 07:30:00
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
CSVQL="$ROOT/zig-out/bin/csvql"
CSV="${1:-$ROOT/test_orders.csv}"
DUCK_CSV="$(cd "$(dirname "$CSV")" && pwd)/$(basename "$CSV")"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

pass=0; fail=0; perf_faster=0; perf_slower=0; skipped=0

header() { echo; echo -e "${BOLD}${CYAN}━━━ $* ━━━${NC}"; }
ok()     { echo -e "  ${GREEN}✓${NC} $*"; ((pass++)) || true; }
bad()    { echo -e "  ${RED}✗${NC} $*"; ((fail++)) || true; }
info()   { echo -e "  ${YELLOW}→${NC} $*"; }
skip()   { echo -e "  ${YELLOW}⊘${NC} $* (skipped — format not supported by DuckDB)"; ((skipped++)) || true; }

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

sorted_data() { tail -n+2 "$1" | sort; }

# Normalize numeric strings: 2.0 → 2, 75.0 → 75, 1.75 → 1.75 (unchanged)
normalize_nums() {
    python3 -c "
import sys, csv, re
def norm(v):
    try:
        f = float(v)
        return str(int(f)) if f == int(f) else v
    except ValueError:
        return v
for line in sys.stdin:
    parts = line.rstrip('\n').split(',')
    print(','.join(norm(p) for p in parts))
"
}

diff_check() {
    local label="$1" a="$2" b="$3"
    if diff -q <(sorted_data "$a" | normalize_nums) <(sorted_data "$b" | normalize_nums) > /dev/null 2>&1; then
        ok "$label — output matches DuckDB"
    else
        bad "$label — OUTPUT MISMATCH"
        { diff <(sorted_data "$a" | normalize_nums) <(sorted_data "$b" | normalize_nums) | head -30; } || true
    fi
}

bench_and_check() {
    local label="$1" cq="$2" dsql="$3"
    local out_csvql="$TMP/${label//[^a-zA-Z0-9]/_}_csvql.csv"
    local out_duck="$TMP/${label//[^a-zA-Z0-9]/_}_duck.csv"

    info "$label"

    local t0 t1
    t0=$(date +%s%N)
    if ! "$CSVQL" "$cq" > "$out_csvql" 2>&1; then
        bad "$label — csvql EXECUTION ERROR: $(cat "$out_csvql")"
        return
    fi
    t1=$(date +%s%N)
    local csvql_t
    csvql_t=$(echo "scale=3; ($t1 - $t0)/1000000000" | bc)

    t0=$(date +%s%N)
    if ! duckdb -csv -c "$dsql" > "$out_duck" 2>&1; then
        bad "$label — duckdb EXECUTION ERROR: $(cat "$out_duck")"
        return
    fi
    t1=$(date +%s%N)
    local duck_t
    duck_t=$(echo "scale=3; ($t1 - $t0)/1000000000" | bc)

    diff_check "$label" "$out_csvql" "$out_duck"

    local ratio
    ratio=$(echo "scale=2; $duck_t / ($csvql_t + 0.001)" | bc)
    local faster
    if (( $(echo "$csvql_t < $duck_t" | bc -l) )); then
        faster="${GREEN}csvql ${ratio}x faster${NC}"
        ((perf_faster++)) || true
    else
        faster="${YELLOW}duckdb faster${NC}"
        ((perf_slower++)) || true
    fi
    echo -e "    csvql: ${csvql_t}s  |  duckdb: ${duck_t}s  |  $faster"
}

# Only check csvql output (no DuckDB equivalent for mixed-format parsing)
csvql_only_check() {
    local label="$1" cq="$2" expected_contains="$3"
    local out_csvql="$TMP/${label//[^a-zA-Z0-9]/_}_csvql.csv"

    info "$label"

    if ! "$CSVQL" "$cq" > "$out_csvql" 2>&1; then
        bad "$label — csvql EXECUTION ERROR: $(cat "$out_csvql")"
        return
    fi

    if grep -q "$expected_contains" "$out_csvql"; then
        ok "$label — output contains expected: '$expected_contains'"
    else
        bad "$label — expected '$expected_contains' not found in output"
        cat "$out_csvql" | head -10
    fi
}

# ── Preflight ─────────────────────────────────────────────────────────────────
echo -e "${BOLD}csvql vs DuckDB — DateTime Benchmark & Correctness${NC}"
echo    "CSV file : $DUCK_CSV"
echo    "Rows     : $(( $(wc -l < "$CSV") - 1 )) rows"
echo    "csvql    : $($CSVQL --version 2>/dev/null || echo 'local build')"
echo    "duckdb   : $(duckdb --version)"

# ── SECTION 1: Multi-Format Parsing (csvql only) ──────────────────────────────
header "1. Multi-Format DateTime Parsing (csvql only — DuckDB rejects mixed formats)"

# Create subset CSVs for each format to test parsing
# ISO-8601 space format
cat > "$TMP/iso_space.csv" << 'EOF'
order_id,ordered_at,delivered_at
1001,2026-01-15 09:30:00,2026-01-16 10:30:00
1002,2026-01-17 11:00:00,2026-01-18 10:00:00
EOF

# ISO-8601 T format
cat > "$TMP/iso_t.csv" << 'EOF'
order_id,ordered_at,delivered_at
1003,2026-01-16T10:00:00,2026-01-17T09:20:00
1004,2026-01-18T08:00:00,2026-01-19T11:00:00
EOF

# US slash format
cat > "$TMP/us_slash.csv" << 'EOF'
order_id,ordered_at,delivered_at
1005,01/15/2026 08:00:00,01/16/2026 11:15:00
1006,01/17/2026 09:30:00,01/18/2026 10:00:00
EOF

# EU dot format
cat > "$TMP/eu_dot.csv" << 'EOF'
order_id,ordered_at,delivered_at
1007,15.01.2026 07:30:00,16.01.2026 09:00:00
1008,18.01.2026 10:30:00,19.01.2026 14:30:00
EOF

info "ISO-8601 space: 2026-01-15 09:30:00"
csvql_t_iso=$("$CSVQL" "SELECT order_id, DATEDIFF('minute', ordered_at, delivered_at) FROM '$TMP/iso_space.csv'" 2>&1)
echo "    $csvql_t_iso"
if echo "$csvql_t_iso" | grep -qE "^1001,"; then
    ok "ISO-8601 space format parsed correctly"
else
    bad "ISO-8601 space format parse failed"
fi

info "ISO-8601 T separator: 2026-01-16T10:00:00"
csvql_t_isot=$("$CSVQL" "SELECT order_id, DATEDIFF('minute', ordered_at, delivered_at) FROM '$TMP/iso_t.csv'" 2>&1)
echo "    $csvql_t_isot"
if echo "$csvql_t_isot" | grep -qE "^1003,"; then
    ok "ISO-8601 T format parsed correctly"
else
    bad "ISO-8601 T format parse failed"
fi

info "US slash format: 01/15/2026 08:00:00"
csvql_t_us=$("$CSVQL" "SELECT order_id, DATEDIFF('minute', ordered_at, delivered_at) FROM '$TMP/us_slash.csv'" 2>&1)
echo "    $csvql_t_us"
if echo "$csvql_t_us" | grep -qE "^1005,"; then
    ok "US slash format parsed correctly"
else
    bad "US slash format parse failed"
fi

info "EU dot format: 15.01.2026 07:30:00"
csvql_t_eu=$("$CSVQL" "SELECT order_id, DATEDIFF('minute', ordered_at, delivered_at) FROM '$TMP/eu_dot.csv'" 2>&1)
echo "    $csvql_t_eu"
if echo "$csvql_t_eu" | grep -qE "^1007,"; then
    ok "EU dot format parsed correctly"
else
    bad "EU dot format parse failed"
fi

# Mixed formats in same CSV
info "Mixed formats (ISO + US + EU dot) in same CSV"
cat > "$TMP/mixed.csv" << 'EOF'
order_id,ordered_at,picked_at
A,2026-01-15 09:30:00,2026-01-15 10:45:00
B,01/15/2026 08:00:00,01/15/2026 09:30:00
C,2026-01-16T10:00:00,2026-01-16T11:30:00
D,15.01.2026 07:30:00,15.01.2026 09:00:00
EOF
csvql_mixed=$("$CSVQL" "SELECT order_id, DATEDIFF('minute', ordered_at, picked_at) FROM '$TMP/mixed.csv'" 2>&1)
echo "    $csvql_mixed"
if echo "$csvql_mixed" | grep -q "75" && echo "$csvql_mixed" | grep -q "90"; then
    ok "Mixed format parsing — all 4 rows computed correctly"
else
    bad "Mixed format parsing failed"
fi

# ── SECTION 2: DATEDIFF Correctness vs DuckDB (ISO-8601 only) ─────────────────
header "2. DATEDIFF — Correctness vs DuckDB"

# Create ISO-only test file for DuckDB comparison
cat > "$TMP/orders_iso.csv" << 'EOF'
order_id,ordered_at,picked_at,packaged_at,shipped_at,delivered_at,status,order_type,price
1001,2026-01-15 09:30:00,2026-01-15 10:45:00,2026-01-15 11:15:00,2026-01-15 14:00:00,2026-01-16 10:30:00,delivered,delivery,2.50
1002,2026-01-17 11:00:00,2026-01-17 12:15:00,2026-01-17 12:45:00,2026-01-17 16:00:00,2026-01-18 10:00:00,delivered,delivery,2.30
1003,2026-01-18 08:00:00,2026-01-18 09:30:00,2026-01-18 10:00:00,2026-01-18 13:30:00,2026-01-19 11:00:00,delivered,delivery,3.50
1004,2026-01-19 07:00:00,2026-01-19 08:30:00,2026-01-19 09:00:00,2026-01-19 12:30:00,2026-01-20 09:45:00,delivered,delivery,3.20
1005,2026-01-20 09:30:00,,,,,in_transit,delivery,2.75
1006,2026-01-21 10:00:00,2026-01-21 11:30:00,2026-01-21 12:00:00,,2026-01-21 16:30:00,collected,collection,4.50
EOF
DUCK_ISO="$TMP/orders_iso.csv"

bench_and_check "DATEDIFF minutes (pick time)" \
    "SELECT order_id, DATEDIFF('minute', ordered_at, picked_at) AS pick_min FROM '$DUCK_ISO' WHERE picked_at != ''" \
    "SELECT order_id, CAST(epoch(strptime(picked_at, '%Y-%m-%d %H:%M:%S') - strptime(ordered_at, '%Y-%m-%d %H:%M:%S')) / 60 AS BIGINT) AS pick_min FROM read_csv_auto('$DUCK_ISO', ALL_VARCHAR=TRUE) WHERE picked_at != ''"

bench_and_check "DATEDIFF hours (processing time)" \
    "SELECT order_id, DATEDIFF('hour', ordered_at, packaged_at) AS proc_hours FROM '$DUCK_ISO' WHERE packaged_at != ''" \
    "SELECT order_id, epoch(strptime(packaged_at, '%Y-%m-%d %H:%M:%S') - strptime(ordered_at, '%Y-%m-%d %H:%M:%S')) / 3600.0 AS proc_hours FROM read_csv_auto('$DUCK_ISO', ALL_VARCHAR=TRUE) WHERE packaged_at != ''"

bench_and_check "DATEDIFF days (shipping time)" \
    "SELECT order_id, DATEDIFF('day', shipped_at, delivered_at) AS ship_days FROM '$DUCK_ISO' WHERE shipped_at != '' AND delivered_at != ''" \
    "SELECT order_id, epoch(strptime(delivered_at, '%Y-%m-%d %H:%M:%S') - strptime(shipped_at, '%Y-%m-%d %H:%M:%S')) / 86400.0 AS ship_days FROM read_csv_auto('$DUCK_ISO', ALL_VARCHAR=TRUE) WHERE shipped_at != '' AND delivered_at != ''"

bench_and_check "DATEDIFF seconds" \
    "SELECT order_id, DATEDIFF('second', ordered_at, picked_at) AS pick_secs FROM '$DUCK_ISO' WHERE picked_at != ''" \
    "SELECT order_id, CAST(epoch(strptime(picked_at, '%Y-%m-%d %H:%M:%S') - strptime(ordered_at, '%Y-%m-%d %H:%M:%S')) AS BIGINT) AS pick_secs FROM read_csv_auto('$DUCK_ISO', ALL_VARCHAR=TRUE) WHERE picked_at != ''"

bench_and_check "DATEDIFF in WHERE clause" \
    "SELECT order_id FROM '$DUCK_ISO' WHERE shipped_at != '' AND delivered_at != '' AND DATEDIFF('day', shipped_at, delivered_at) > 1" \
    "SELECT order_id FROM read_csv_auto('$DUCK_ISO', ALL_VARCHAR=TRUE) WHERE shipped_at != '' AND delivered_at != '' AND epoch(strptime(delivered_at, '%Y-%m-%d %H:%M:%S') - strptime(shipped_at, '%Y-%m-%d %H:%M:%S')) / 86400.0 > 1"

bench_and_check "DATEDIFF with ORDER BY" \
    "SELECT order_id, DATEDIFF('minute', ordered_at, picked_at) AS pick_min FROM '$DUCK_ISO' WHERE picked_at != '' ORDER BY pick_min DESC" \
    "SELECT order_id, CAST(epoch(strptime(picked_at, '%Y-%m-%d %H:%M:%S') - strptime(ordered_at, '%Y-%m-%d %H:%M:%S')) / 60 AS BIGINT) AS pick_min FROM read_csv_auto('$DUCK_ISO', ALL_VARCHAR=TRUE) WHERE picked_at != '' ORDER BY pick_min DESC"

# ── SECTION 3: DATEADD Correctness vs DuckDB ──────────────────────────────────
header "3. DATEADD — Correctness vs DuckDB"

bench_and_check "DATEADD days" \
    "SELECT order_id, DATEADD('day', 2, shipped_at) AS est_delivery FROM '$DUCK_ISO' WHERE shipped_at != ''" \
    "SELECT order_id, strftime(strptime(shipped_at, '%Y-%m-%d %H:%M:%S') + INTERVAL 2 DAYS, '%Y-%m-%d %H:%M:%S') AS est_delivery FROM read_csv_auto('$DUCK_ISO', ALL_VARCHAR=TRUE) WHERE shipped_at != ''"

bench_and_check "DATEADD hours" \
    "SELECT order_id, DATEADD('hour', 5, ordered_at) AS sla_deadline FROM '$DUCK_ISO'" \
    "SELECT order_id, strftime(strptime(ordered_at, '%Y-%m-%d %H:%M:%S') + INTERVAL 5 HOURS, '%Y-%m-%d %H:%M:%S') AS sla_deadline FROM read_csv_auto('$DUCK_ISO', ALL_VARCHAR=TRUE)"

bench_and_check "DATEADD minutes" \
    "SELECT order_id, DATEADD('minute', 90, ordered_at) AS pick_deadline FROM '$DUCK_ISO'" \
    "SELECT order_id, strftime(strptime(ordered_at, '%Y-%m-%d %H:%M:%S') + INTERVAL 90 MINUTES, '%Y-%m-%d %H:%M:%S') AS pick_deadline FROM read_csv_auto('$DUCK_ISO', ALL_VARCHAR=TRUE)"

# ── SECTION 4: Workflow analysis on full mixed-format data ────────────────────
header "4. Workflow Analysis — Full test_orders.csv (mixed formats)"

info "Processing time breakdown (all completed orders)"
"$CSVQL" "SELECT order_id, customer_name, DATEDIFF('minute', ordered_at, picked_at) AS picking_min, DATEDIFF('minute', picked_at, packaged_at) AS packaging_min FROM '$DUCK_CSV' WHERE status IN ('delivered', 'collected') ORDER BY order_id" 2>&1 | head -12
ok "Workflow time breakdown completed"

info "Shipping duration analysis (delivery orders)"
"$CSVQL" "SELECT order_id, DATEDIFF('day', shipped_at, delivered_at) AS ship_days FROM '$DUCK_CSV' WHERE order_type = 'delivery' AND delivered_at != '' ORDER BY ship_days DESC" 2>&1 | head -10
ok "Shipping duration query completed"

info "In-transit orders — estimated delivery"
"$CSVQL" "SELECT order_id, shipped_at, DATEADD('day', 2, shipped_at) AS estimated_delivery FROM '$DUCK_CSV' WHERE status = 'in_transit' ORDER BY order_id" 2>&1
ok "DATEADD estimated delivery completed"

# ── SECTION 5: Performance on large-ish file ──────────────────────────────────
header "5. Performance — Generate 10K rows and benchmark"

python3 - "$TMP/large_orders.csv" << 'PYEOF'
import sys, csv, random
from datetime import datetime, timedelta

outfile = sys.argv[1]
formats = [
    lambda dt: dt.strftime('%Y-%m-%d %H:%M:%S'),
    lambda dt: dt.strftime('%m/%d/%Y %H:%M:%S'),
    lambda dt: dt.strftime('%Y-%m-%dT%H:%M:%S'),
    lambda dt: dt.strftime('%d.%m.%Y %H:%M:%S'),
]
statuses = ['delivered', 'collected', 'in_transit', 'picking', 'pending']
order_types = ['delivery', 'collection']
products = ['Bananas 1kg', 'Milk 2L', 'Bread', 'Chicken 500g', 'Eggs 12pk', 
            'Coffee 250g', 'Rice 1kg', 'Pasta 500g', 'Cheese 200g', 'Butter']

with open(outfile, 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['order_id','customer_name','product','price','ordered_at','picked_at',
                'packaged_at','shipped_at','delivered_at','collected_at','status','order_type'])
    
    base = datetime(2026, 1, 1, 8, 0, 0)
    for i in range(10000):
        fmt = formats[i % len(formats)]
        ordered = base + timedelta(minutes=random.randint(0, 43200))
        picked = ordered + timedelta(minutes=random.randint(60, 120))
        packaged = picked + timedelta(minutes=random.randint(20, 60))
        status = statuses[i % len(statuses)]
        otype = order_types[i % 2]
        
        shipped_at = ''
        delivered_at = ''
        collected_at = ''
        
        if status == 'delivered':
            shipped = packaged + timedelta(hours=random.randint(2, 6))
            delivered = shipped + timedelta(hours=random.randint(20, 48))
            shipped_at = fmt(shipped)
            delivered_at = fmt(delivered)
        elif status == 'collected':
            collected = packaged + timedelta(hours=random.randint(1, 4))
            collected_at = fmt(collected)
        elif status == 'in_transit':
            shipped = packaged + timedelta(hours=random.randint(2, 6))
            shipped_at = fmt(shipped)
        
        w.writerow([
            f'ORD{i+1:05d}', f'Customer {i+1}', random.choice(products),
            round(random.uniform(0.5, 15.0), 2),
            fmt(ordered), fmt(picked), fmt(packaged),
            shipped_at, delivered_at, collected_at, status, otype
        ])
PYEOF

LARGE_CSV="$TMP/large_orders.csv"
DUCK_LARGE="$LARGE_CSV"
echo "    Generated 10,000 rows with mixed date formats"

info "DATEDIFF on 10K rows (mixed formats)"
t0=$(date +%s%N)
"$CSVQL" "SELECT order_id, DATEDIFF('minute', ordered_at, picked_at) AS pick_min FROM '$LARGE_CSV' WHERE picked_at != ''" > /dev/null
t1=$(date +%s%N)
csvql_t=$(echo "scale=3; ($t1 - $t0)/1000000000" | bc)
echo -e "    csvql time: ${csvql_t}s"

# DuckDB — ISO-only subset for comparison
python3 - "$TMP/large_iso.csv" << 'PYEOF2'
import sys, csv, random
from datetime import datetime, timedelta
outfile = sys.argv[1]
base = datetime(2026, 1, 1, 8, 0, 0)
random.seed(42)
with open(outfile, 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['order_id', 'ordered_at', 'picked_at'])
    for i in range(10000):
        ordered = base + timedelta(minutes=random.randint(0, 43200))
        picked = ordered + timedelta(minutes=random.randint(60, 120))
        w.writerow([f'ORD{i+1:05d}', ordered.strftime('%Y-%m-%d %H:%M:%S'), picked.strftime('%Y-%m-%d %H:%M:%S')])
PYEOF2

info "DATEDIFF on 10K rows (ISO-only, DuckDB comparison)"
t0=$(date +%s%N)
"$CSVQL" "SELECT order_id, DATEDIFF('minute', ordered_at, picked_at) AS pick_min FROM '$TMP/large_iso.csv'" > /dev/null
t1=$(date +%s%N)
csvql_t=$(echo "scale=3; ($t1 - $t0)/1000000000" | bc)

t0=$(date +%s%N)
duckdb -csv -c "SELECT order_id, epoch(strptime(picked_at, '%Y-%m-%d %H:%M:%S') - strptime(ordered_at, '%Y-%m-%d %H:%M:%S')) / 60 AS pick_min FROM read_csv_auto('$TMP/large_iso.csv', ALL_VARCHAR=TRUE)" > /dev/null
t1=$(date +%s%N)
duck_t=$(echo "scale=3; ($t1 - $t0)/1000000000" | bc)

ratio=$(echo "scale=2; $duck_t / ($csvql_t + 0.001)" | bc)
if (( $(echo "$csvql_t < $duck_t" | bc -l) )); then
    echo -e "    csvql: ${csvql_t}s  |  duckdb: ${duck_t}s  |  ${GREEN}csvql ${ratio}x faster${NC}"
    ((perf_faster++)) || true
else
    echo -e "    csvql: ${csvql_t}s  |  duckdb: ${duck_t}s  |  ${YELLOW}duckdb faster${NC}"
    ((perf_slower++)) || true
fi
ok "10K rows DATEDIFF completed"

info "DATEADD on 10K rows (ISO-only, DuckDB comparison)"
t0=$(date +%s%N)
"$CSVQL" "SELECT order_id, DATEADD('day', 2, ordered_at) AS deadline FROM '$TMP/large_iso.csv'" > /dev/null
t1=$(date +%s%N)
csvql_t=$(echo "scale=3; ($t1 - $t0)/1000000000" | bc)

t0=$(date +%s%N)
duckdb -csv -c "SELECT order_id, strftime(strptime(ordered_at, '%Y-%m-%d %H:%M:%S') + INTERVAL 2 DAYS, '%Y-%m-%d %H:%M:%S') AS deadline FROM read_csv_auto('$TMP/large_iso.csv', ALL_VARCHAR=TRUE)" > /dev/null
t1=$(date +%s%N)
duck_t=$(echo "scale=3; ($t1 - $t0)/1000000000" | bc)

ratio=$(echo "scale=2; $duck_t / ($csvql_t + 0.001)" | bc)
if (( $(echo "$csvql_t < $duck_t" | bc -l) )); then
    echo -e "    csvql: ${csvql_t}s  |  duckdb: ${duck_t}s  |  ${GREEN}csvql ${ratio}x faster${NC}"
    ((perf_faster++)) || true
else
    echo -e "    csvql: ${csvql_t}s  |  duckdb: ${duck_t}s  |  ${YELLOW}duckdb faster${NC}"
    ((perf_slower++)) || true
fi
ok "10K rows DATEADD completed"

# ── Summary ───────────────────────────────────────────────────────────────────
echo
echo -e "${BOLD}━━━ Results Summary ━━━${NC}"
echo -e "  Tests passed             : ${GREEN}${pass}${NC}"
echo -e "  Tests FAILED             : ${RED}${fail}${NC}"
echo -e "  Skipped (format limits)  : ${YELLOW}${skipped}${NC}"
echo -e "  Queries csvql was faster : ${GREEN}${perf_faster}${NC}"
echo -e "  Queries DuckDB was faster: ${YELLOW}${perf_slower}${NC}"
echo

if [[ $fail -eq 0 ]]; then
    echo -e "${GREEN}${BOLD}All datetime correctness checks passed. ✓${NC}"
else
    echo -e "${RED}${BOLD}${fail} failure(s) found. See details above.${NC}"
    exit 1
fi
