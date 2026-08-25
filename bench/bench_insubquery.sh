#!/usr/bin/env bash
# =============================================================================
# bench_insubquery.sh — WHERE col IN (SELECT ...) semi-join: csvql vs DuckDB
#
# col IN (SELECT ...) (#124) is semantically a semi-join: build a hash table
# from the subquery's result, probe it per outer row. Timed across four
# resolved-list sizes (6 / 10,000 / 100,000 / 1,000,000) since the two
# engines' relative speed depends heavily on how large that hash table is —
# see CORRECTNESS.md's "Performance vs. DuckDB" for why, and the ~20x
# regression this shape originally exposed before the hash-set fix.
#
# Usage:
#   ./bench/bench_insubquery.sh
#
# Environment overrides:
#   DUCKDB_BIN — path to duckdb binary (default: duckdb in PATH)
#   ORDERS     — number of order rows to generate (default: 2000000)
# =============================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CSVQL="${SCRIPT_DIR}/zig-out/bin/csvql"
DUCKDB="${DUCKDB_BIN:-duckdb}"
ORDERS="${ORDERS:-2000000}"
MAXCUST=3000000

if [[ ! -x "$CSVQL" ]]; then
  echo "csvql not found at $CSVQL — run: zig build -Doptimize=ReleaseFast"
  exit 1
fi
if ! command -v "$DUCKDB" >/dev/null 2>&1; then
  echo "duckdb not found in PATH (set DUCKDB_BIN=/path/to/duckdb)"
  exit 1
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
ORDERS_CSV="$TMP/orders.csv"

echo "Generating $ORDERS order rows (customer_id spread over 1..$MAXCUST)..."
awk -v n="$ORDERS" -v maxcust="$MAXCUST" 'BEGIN{
  srand(42);
  print "id,customer_id,amount";
  for (i = 1; i <= n; i++) {
    cust = int(rand() * maxcust) + 1;
    amt = int(rand() * 100000) / 100;
    print i "," cust "," amt;
  }
}' > "$ORDERS_CSV"

# time_cmd <cmd...> — wall time in seconds, printed to var _t
time_cmd() {
  local t0 t1
  t0=$(date +%s%N)
  "$@" > /dev/null
  t1=$(date +%s%N)
  _t=$(echo "scale=3; ($t1 - $t0)/1000000000" | bc)
}

# run_scenario <label> <gold_modulus>
# Customers table always spans the SAME 1..MAXCUST range as orders (so match
# probability is comparable across scenarios, not an artifact of a narrower
# id range) — only the gold_modulus changes, giving ~MAXCUST/gold_modulus
# resolved values.
run_scenario() {
  local label="$1" gold_modulus="$2"
  local cust_csv="$TMP/customers_${label}.csv"
  awk -v n="$MAXCUST" -v m="$gold_modulus" 'BEGIN{
    print "customer_id,tier";
    for (i = 1; i <= n; i++) print i "," ((i % m == 0) ? "gold" : "silver");
  }' > "$cust_csv"

  local q_csvql="SELECT COUNT(*) FROM '${ORDERS_CSV}' WHERE customer_id IN (SELECT customer_id FROM '${cust_csv}' WHERE tier = 'gold')"
  local q_duck="SELECT COUNT(*) FROM read_csv_auto('${ORDERS_CSV}') WHERE customer_id IN (SELECT customer_id FROM read_csv_auto('${cust_csv}') WHERE tier = 'gold')"

  local csvql_out duck_out
  csvql_out="$("$CSVQL" "$q_csvql" | tail -n +2)"
  duck_out="$("$DUCKDB" -csv -noheader -c "$q_duck")"
  if [[ "$csvql_out" != "$duck_out" ]]; then
    echo "  MISMATCH ($label): csvql=$csvql_out duckdb=$duck_out"
    return 1
  fi

  time_cmd "$CSVQL" "$q_csvql"; local t_csvql="$_t"
  time_cmd "$DUCKDB" -csv -c "$q_duck"; local t_duck="$_t"
  printf "  %-12s resolved=~%-10s csvql=%-8s duckdb=%-8s (rows matched: %s)\n" \
    "$label" "$(( MAXCUST / gold_modulus ))" "${t_csvql}s" "${t_duck}s" "$csvql_out"
}

echo ""
echo "Query: SELECT COUNT(*) FROM orders WHERE customer_id IN (SELECT customer_id FROM customers WHERE tier = 'gold')"
echo ""
run_scenario "small" 500000   # ~6 resolved values
run_scenario "10k"   300      # ~10,000 resolved values
run_scenario "100k"  30       # ~100,000 resolved values
run_scenario "1m"    3        # ~1,000,000 resolved values
