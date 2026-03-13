#!/usr/bin/env bash
# =============================================================================
# bench_duckdb.sh — Performance + data consistency comparison: csvql vs DuckDB
#
# Usage:
#   ./bench/bench_duckdb.sh [CSV_FILE]
#
# Default CSV: large_test.csv (1M rows, columns: id,name,age,city,salary,department)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."
CSVQL="$ROOT/zig-out/bin/csvql"
CSV="${1:-$ROOT/large_test.csv}"
DUCK_CSV="$(cd "$(dirname "$CSV")" && pwd)/$(basename "$CSV")"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

pass=0; fail=0; perf_faster=0; perf_slower=0

header() { echo; echo -e "${BOLD}${CYAN}━━━ $* ━━━${NC}"; }
ok()     { echo -e "  ${GREEN}✓${NC} $*"; ((pass++)) || true; }
bad()    { echo -e "  ${RED}✗${NC} $*"; ((fail++)) || true; }
info()   { echo -e "  ${YELLOW}→${NC} $*"; }

# ── Helpers ──────────────────────────────────────────────────────────────────

# time_cmd <label> <cmd...>  — run command, return wall time in seconds (var: _t)
time_cmd() {
    local label="$1"; shift
    local t0 t1
    t0=$(date +%s%N)
    "$@" > /dev/null 2>&1
    t1=$(date +%s%N)
    _t=$(echo "scale=3; ($t1 - $t0)/1000000000" | bc)
    echo -e "    ${label}: ${_t}s"
}

# sorted_csv — sort data rows (skip header) for deterministic diff
sorted_data() { tail -n+2 "$1" | sort; }

# diff_check <label> <a> <b>
# Compares data rows only — header naming differs between csvql and DuckDB
# (csvql: COUNT(*), DuckDB: count_star(); etc.) which is cosmetic not a bug.
diff_check() {
    local label="$1" a="$2" b="$3"
    if diff -q <(sorted_data "$a") <(sorted_data "$b") > /dev/null 2>&1; then
        ok "$label — output matches"
    else
        bad "$label — OUTPUT MISMATCH"
        { diff <(sorted_data "$a") <(sorted_data "$b") | head -30; } || true
    fi
}

# bench_and_check <label> <csvql_query> <duck_sql>
# Runs both, checks consistency, reports timing.
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

bench_and_check() {
    local label="$1" cq="$2" dsql="$3"
    local out_csvql="$TMP/${label//[^a-zA-Z0-9]/_}_csvql.csv"
    local out_duck="$TMP/${label//[^a-zA-Z0-9]/_}_duck.csv"

    info "$label"

    # Run csvql (timed)
    local t0 t1
    t0=$(date +%s%N)
    if ! "$CSVQL" "$cq" > "$out_csvql" 2>&1; then
        bad "$label — csvql EXECUTION ERROR: $(cat "$out_csvql")"
        return
    fi
    t1=$(date +%s%N)
    local csvql_t
    csvql_t=$(echo "scale=3; ($t1 - $t0)/1000000000" | bc)

    # Run DuckDB (timed)
    t0=$(date +%s%N)
    if ! duckdb -csv -c "$dsql" > "$out_duck" 2>&1; then
        bad "$label — duckdb EXECUTION ERROR: $(cat "$out_duck")"
        return
    fi
    t1=$(date +%s%N)
    local duck_t
    duck_t=$(echo "scale=3; ($t1 - $t0)/1000000000" | bc)

    # Consistency check
    diff_check "$label" "$out_csvql" "$out_duck"

    # Perf
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

# ── Preflight checks ──────────────────────────────────────────────────────────
echo -e "${BOLD}csvql vs DuckDB — Performance & Consistency Benchmark${NC}"
echo    "CSV file : $DUCK_CSV"
echo    "Rows     : $(( $(wc -l < "$CSV") - 1 )) rows"
echo    "csvql    : $($CSVQL --version 2>/dev/null || echo 'local build')"
echo    "duckdb   : $(duckdb --version)"

# ── SECTION 1: Basic SELECT / filtering ──────────────────────────────────────
header "1. Basic SELECT and WHERE"

bench_and_check "SELECT *" \
    "SELECT * FROM '$DUCK_CSV'" \
    "SELECT * FROM read_csv_auto('$DUCK_CSV')"

bench_and_check "SELECT columns + WHERE numeric" \
    "SELECT name, salary FROM '$DUCK_CSV' WHERE salary > 80000" \
    "SELECT name, salary FROM read_csv_auto('$DUCK_CSV') WHERE salary > 80000"

bench_and_check "WHERE BETWEEN" \
    "SELECT name, salary FROM '$DUCK_CSV' WHERE salary BETWEEN 60000 AND 70000" \
    "SELECT name, salary FROM read_csv_auto('$DUCK_CSV') WHERE salary BETWEEN 60000 AND 70000"

bench_and_check "WHERE IN" \
    "SELECT name, city FROM '$DUCK_CSV' WHERE city IN ('Denver','Austin','Miami')" \
    "SELECT name, city FROM read_csv_auto('$DUCK_CSV') WHERE city IN ('Denver','Austin','Miami')"

bench_and_check "WHERE IS NULL" \
    "SELECT name FROM '$DUCK_CSV' WHERE name IS NOT NULL" \
    "SELECT name FROM read_csv_auto('$DUCK_CSV') WHERE name IS NOT NULL"

bench_and_check "WHERE LIKE prefix" \
    "SELECT name FROM '$DUCK_CSV' WHERE name LIKE 'A%'" \
    "SELECT name FROM read_csv_auto('$DUCK_CSV') WHERE name LIKE 'A%'"

bench_and_check "WHERE ILIKE" \
    "SELECT name FROM '$DUCK_CSV' WHERE name ILIKE 'a%'" \
    "SELECT name FROM read_csv_auto('$DUCK_CSV') WHERE name ILIKE 'a%'"

bench_and_check "WHERE AND compound" \
    "SELECT name, salary FROM '$DUCK_CSV' WHERE age > 30 AND department = 'Finance'" \
    "SELECT name, salary FROM read_csv_auto('$DUCK_CSV') WHERE age > 30 AND department = 'Finance'"

bench_and_check "WHERE OR compound" \
    "SELECT name, city FROM '$DUCK_CSV' WHERE city = 'Denver' OR city = 'Austin'" \
    "SELECT name, city FROM read_csv_auto('$DUCK_CSV') WHERE city = 'Denver' OR city = 'Austin'"

# ── SECTION 2: Aggregates ─────────────────────────────────────────────────────
header "2. Aggregates"

bench_and_check "COUNT(*)" \
    "SELECT COUNT(*) FROM '$DUCK_CSV'" \
    "SELECT COUNT(*) FROM read_csv_auto('$DUCK_CSV')"

bench_and_check "SUM + AVG + MIN + MAX" \
    "SELECT SUM(salary), AVG(salary), MIN(salary), MAX(salary) FROM '$DUCK_CSV'" \
    "SELECT SUM(salary), AVG(salary), MIN(salary), MAX(salary) FROM read_csv_auto('$DUCK_CSV')"

bench_and_check "GROUP BY COUNT" \
    "SELECT department, COUNT(*) FROM '$DUCK_CSV' GROUP BY department" \
    "SELECT department, COUNT(*) FROM read_csv_auto('$DUCK_CSV') GROUP BY department"

bench_and_check "GROUP BY SUM ORDER BY" \
    "SELECT department, SUM(salary) AS total FROM '$DUCK_CSV' GROUP BY department ORDER BY total DESC" \
    "SELECT department, SUM(salary) AS total FROM read_csv_auto('$DUCK_CSV') GROUP BY department ORDER BY total DESC"

bench_and_check "GROUP BY AVG" \
    "SELECT city, AVG(salary) FROM '$DUCK_CSV' GROUP BY city" \
    "SELECT city, AVG(salary) FROM read_csv_auto('$DUCK_CSV') GROUP BY city"

bench_and_check "GROUP BY + HAVING" \
    "SELECT department, COUNT(*) AS n FROM '$DUCK_CSV' GROUP BY department HAVING COUNT(*) > 50000" \
    "SELECT department, COUNT(*) AS n FROM read_csv_auto('$DUCK_CSV') GROUP BY department HAVING COUNT(*) > 50000"

bench_and_check "GROUP BY two columns" \
    "SELECT department, city, COUNT(*) AS n FROM '$DUCK_CSV' GROUP BY department, city" \
    "SELECT department, city, COUNT(*) AS n FROM read_csv_auto('$DUCK_CSV') GROUP BY department, city"

# ── SECTION 3: ORDER BY / LIMIT ───────────────────────────────────────────────
header "3. ORDER BY and LIMIT"

bench_and_check "ORDER BY ASC" \
    "SELECT name, salary FROM '$DUCK_CSV' ORDER BY salary ASC LIMIT 20" \
    "SELECT name, salary FROM read_csv_auto('$DUCK_CSV') ORDER BY salary ASC LIMIT 20"

bench_and_check "ORDER BY DESC LIMIT" \
    "SELECT name, salary FROM '$DUCK_CSV' ORDER BY salary DESC LIMIT 10" \
    "SELECT name, salary FROM read_csv_auto('$DUCK_CSV') ORDER BY salary DESC LIMIT 10"

bench_and_check "WHERE + ORDER BY + LIMIT" \
    "SELECT name, salary FROM '$DUCK_CSV' WHERE salary > 80000 ORDER BY salary DESC LIMIT 10" \
    "SELECT name, salary FROM read_csv_auto('$DUCK_CSV') WHERE salary > 80000 ORDER BY salary DESC LIMIT 10"

# ── SECTION 4: Scalar functions ───────────────────────────────────────────────
header "4. Scalar Functions"

bench_and_check "UPPER + LOWER" \
    "SELECT UPPER(name), LOWER(city) FROM '$DUCK_CSV' WHERE salary > 90000" \
    "SELECT UPPER(name), LOWER(city) FROM read_csv_auto('$DUCK_CSV') WHERE salary > 90000"

bench_and_check "TRIM + LENGTH" \
    "SELECT TRIM(name), LENGTH(name) FROM '$DUCK_CSV' WHERE age < 25" \
    "SELECT TRIM(name), LENGTH(name) FROM read_csv_auto('$DUCK_CSV') WHERE age < 25"

bench_and_check "SUBSTR" \
    "SELECT SUBSTR(name, 1, 3) AS initials FROM '$DUCK_CSV' LIMIT 1000" \
    "SELECT SUBSTR(name, 1, 3) AS initials FROM read_csv_auto('$DUCK_CSV') LIMIT 1000"

bench_and_check "ABS + CAST" \
    "SELECT name, ABS(age) AS abs_age, CAST(age AS TEXT) AS age_str FROM '$DUCK_CSV' WHERE salary > 95000" \
    "SELECT name, ABS(age) AS abs_age, CAST(age AS VARCHAR) AS age_str FROM read_csv_auto('$DUCK_CSV') WHERE salary > 95000"

bench_and_check "CEIL + FLOOR" \
    "SELECT name, CEIL(salary) AS ceil_sal, FLOOR(salary) AS floor_sal FROM '$DUCK_CSV' WHERE department = 'Engineering' LIMIT 100" \
    "SELECT name, CEIL(salary) AS ceil_sal, FLOOR(salary) AS floor_sal FROM read_csv_auto('$DUCK_CSV') WHERE department = 'Engineering' LIMIT 100"

bench_and_check "GROUP BY + UPPER scalar" \
    "SELECT UPPER(department), COUNT(*) AS n FROM '$DUCK_CSV' GROUP BY department ORDER BY n DESC" \
    "SELECT UPPER(department), COUNT(*) AS n FROM read_csv_auto('$DUCK_CSV') GROUP BY department ORDER BY n DESC"

# ── SECTION 5: DISTINCT ───────────────────────────────────────────────────────
header "5. DISTINCT"

bench_and_check "SELECT DISTINCT single col" \
    "SELECT DISTINCT department FROM '$DUCK_CSV'" \
    "SELECT DISTINCT department FROM read_csv_auto('$DUCK_CSV')"

bench_and_check "SELECT DISTINCT two cols" \
    "SELECT DISTINCT city, department FROM '$DUCK_CSV'" \
    "SELECT DISTINCT city, department FROM read_csv_auto('$DUCK_CSV')"

# ── SECTION 6: STRFTIME date bucketing ───────────────────────────────────────
# Only run this section if the CSV has a date column; skip if not.
# (large_test.csv has no date col — skip gracefully)
header "6. STRFTIME (skipped — large_test.csv has no date column)"
info "Use a CSV with a date/datetime column to benchmark STRFTIME."

# ── Summary ───────────────────────────────────────────────────────────────────
echo
echo -e "${BOLD}━━━ Results Summary ━━━${NC}"
echo -e "  Consistency checks passed : ${GREEN}${pass}${NC}"
echo -e "  Consistency checks FAILED : ${RED}${fail}${NC}"
echo -e "  Queries csvql was faster  : ${GREEN}${perf_faster}${NC}"
echo -e "  Queries DuckDB was faster : ${YELLOW}${perf_slower}${NC}"
echo

if [[ $fail -eq 0 ]]; then
    echo -e "${GREEN}${BOLD}All outputs match DuckDB. ✓${NC}"
else
    echo -e "${RED}${BOLD}${fail} output mismatch(es) found. See diffs above.${NC}"
    exit 1
fi
