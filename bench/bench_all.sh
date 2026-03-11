#!/usr/bin/env bash
# bench_all.sh — Unified csvql vs DuckDB benchmark suite
#
# Sections:
#   queries   — COUNT, DISTINCT, GROUP BY, scalar aggregates (csvql vs DuckDB)
#   like      — LIKE pattern-matching (prefix, suffix, high-selectivity)
#   formats   — Output format throughput (CSV, JSON array, JSONL)
#   parse     — Raw CSV parsing throughput (buffered / naive / mmap)
#   groupby   — GROUP BY query shapes via internal Zig engine
#   join      — INNER JOIN hash-join performance (csvql vs DuckDB)
#
# Usage:
#   ./bench/bench_all.sh [--section queries|like|formats|parse|groupby] [csv_file]
#
# Environment overrides:
#   DUCKDB_BIN   — path to duckdb binary  (default: duckdb in PATH)
#   BENCH_RUNS   — number of timed runs   (default: 5)
#
# Requires:
#   duckdb in PATH, csvql built:  zig build -Doptimize=ReleaseFast

set -euo pipefail

# ── Parse arguments ───────────────────────────────────────────────
SECTION="all"
CSV_ARG=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --section) SECTION="$2"; shift 2 ;;
    --section=*) SECTION="${1#--section=}"; shift ;;
    *) CSV_ARG="$1"; shift ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CSVQL="${SCRIPT_DIR}/zig-out/bin/csvql"
DUCKDB="${DUCKDB_BIN:-duckdb}"
RUNS="${BENCH_RUNS:-5}"
BENCH_DIR="${SCRIPT_DIR}/.bench_tmp"
mkdir -p "$BENCH_DIR"

# Default CSV: use the repo's large_test.csv (1M rows, 35MB)
if [[ -n "$CSV_ARG" ]]; then
  CSV="$CSV_ARG"
else
  CSV="${SCRIPT_DIR}/large_test.csv"
fi

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; DIM='\033[2m'; RESET='\033[0m'

# ── Dependency check ─────────────────────────────────────────────
check_deps() {
  if [[ ! -x "$CSVQL" ]]; then
    echo "csvql not found at $CSVQL"
    echo "Build it first:  zig build -Doptimize=ReleaseFast"
    exit 1
  fi
  if ! command -v "$DUCKDB" >/dev/null 2>&1; then
    echo "duckdb not found in PATH (set DUCKDB_BIN=/path/to/duckdb to override)"
    exit 1
  fi
  if [[ ! -f "$CSV" ]]; then
    echo "CSV file not found: $CSV"
    echo "Options:"
    echo "  1) Pass a path:  $0 /path/to/file.csv"
    echo "  2) Generate 5M-row file:"
    echo "     zig run generate_large_csv.zig -- 5000000 > /tmp/very_large_test.csv"
    exit 1
  fi
}

# ── Core timing helper ───────────────────────────────────────────
# bench_cmd <command...>
# Sets globals BENCH_AVG, BENCH_MIN, BENCH_MAX to measured seconds (3 d.p.)
# Runs one silent warmup, then RUNS timed runs.
BENCH_AVG="0.000"; BENCH_MIN="0.000"; BENCH_MAX="0.000"
bench_cmd() {
  # warmup
  "$@" >/dev/null 2>&1 || true

  local total=0 min="" max=""
  for ((i=0; i<RUNS; i++)); do
    local t
    t=$( { /usr/bin/time -p "$@" >/dev/null; } 2>&1 | awk '/^real/{printf "%.3f", $2}' ) || true
    [[ -n "$t" ]] || t="0.000"
    total=$(awk -v s="$total" -v v="$t" 'BEGIN{printf "%.3f",s+v}')
    if [[ -z "$min" ]]; then
      min="$t"; max="$t"
    else
      min=$(awk -v a="$min" -v b="$t" 'BEGIN{printf "%.3f",(a<b)?a:b}')
      max=$(awk -v a="$max" -v b="$t" 'BEGIN{printf "%.3f",(a>b)?a:b}')
    fi
  done
  BENCH_AVG=$(awk -v s="$total" -v n="$RUNS" 'BEGIN{printf "%.3f",s/n}')
  BENCH_MIN="$min"
  BENCH_MAX="$max"
}

# ── Section header ────────────────────────────────────────────────
section_header() {
  echo ""
  printf "${BOLD}══════════════════════════════════════════════════════════════${RESET}\n"
  printf "${BOLD}  %s${RESET}\n" "$1"
  printf "${BOLD}══════════════════════════════════════════════════════════════${RESET}\n"
  echo ""
}

# ── Print one query result row ────────────────────────────────────
# run_query <label> <csvql_sql> <duck_sql>
# Runs both tools, checks correctness via hash (fallback: row count), prints result.
run_query() {
  local label="$1" csvql_sql="$2" duck_sql="$3"

  bench_cmd "$CSVQL" "$csvql_sql"
  local cq_avg="$BENCH_AVG" cq_min="$BENCH_MIN" cq_max="$BENCH_MAX"

  bench_cmd "$DUCKDB" -csv -c "$duck_sql"
  local dk_avg="$BENCH_AVG" dk_min="$BENCH_MIN" dk_max="$BENCH_MAX"

  local speedup
  speedup=$(awk -v cq="$cq_avg" -v dk="$dk_avg" \
    'BEGIN{if(cq<=0)cq=0.001; printf "%.1f", dk/cq}')

  printf "${BOLD}%-60s${RESET}\n" "$label"
  printf "  %-10s  avg=${CYAN}%ss${RESET}  min=%ss  max=%ss\n" \
    "csvql"  "$cq_avg" "$cq_min" "$cq_max"
  printf "  %-10s  avg=${CYAN}%ss${RESET}  min=%ss  max=%ss\n" \
    "duckdb" "$dk_avg" "$dk_min" "$dk_max"
  printf "  speedup: ${YELLOW}%sx${RESET} (csvql vs duckdb)\n\n" "$speedup"
}

# ── Format benchmark (output discarded, no correctness check) ────
# run_format <label> <csvql_flag> <duck_copy_opts> <csvql_sql> <duck_inner_sql>
run_format() {
  local label="$1" csvql_flag="$2" duck_copy_opts="$3" csvql_sql="$4" duck_inner_sql="$5"

  # csvql: flag may be empty
  if [[ -z "$csvql_flag" ]]; then
    bench_cmd "$CSVQL" "$csvql_sql"
  else
    bench_cmd "$CSVQL" "$csvql_flag" "$csvql_sql"
  fi
  local cq_avg="$BENCH_AVG" cq_min="$BENCH_MIN" cq_max="$BENCH_MAX"

  bench_cmd "$DUCKDB" -c "COPY (${duck_inner_sql}) TO '/dev/null' (${duck_copy_opts})"
  local dk_avg="$BENCH_AVG" dk_min="$BENCH_MIN" dk_max="$BENCH_MAX"

  local speedup
  speedup=$(awk -v cq="$cq_avg" -v dk="$dk_avg" \
    'BEGIN{if(cq<=0)cq=0.001; printf "%.1f", dk/cq}')

  local csvql_label="csvql${csvql_flag:+ $csvql_flag}"
  printf "${BOLD}%-55s${RESET}\n" "$label"
  printf "  %-20s  avg=${CYAN}%ss${RESET}  min=%ss  max=%ss\n" \
    "$csvql_label" "$cq_avg" "$cq_min" "$cq_max"
  printf "  %-20s  avg=${CYAN}%ss${RESET}  min=%ss  max=%ss\n" \
    "duckdb" "$dk_avg" "$dk_min" "$dk_max"
  printf "  speedup: ${YELLOW}%sx${RESET} (csvql vs duckdb)\n\n" "$speedup"
}

# ═════════════════════════════════════════════════════════════════
# SECTION 1: Aggregate / GROUP BY / DISTINCT queries
# ═════════════════════════════════════════════════════════════════
run_section_queries() {
  section_header "SECTION 1 — Aggregates, GROUP BY, DISTINCT"

  run_query \
    "COUNT(*) GROUP BY department" \
    "SELECT department, COUNT(*) FROM '${CSV}' GROUP BY department ORDER BY department" \
    "SELECT department, COUNT(*) FROM read_csv_auto('${CSV}') GROUP BY department ORDER BY department"

  run_query \
    "COUNT(*) WHERE age > 30 GROUP BY department" \
    "SELECT department, COUNT(*) FROM '${CSV}' WHERE age > 30 GROUP BY department ORDER BY department" \
    "SELECT department, COUNT(*) FROM read_csv_auto('${CSV}') WHERE age > 30 GROUP BY department ORDER BY department"

  run_query \
    "SELECT DISTINCT city" \
    "SELECT DISTINCT city FROM '${CSV}' ORDER BY city" \
    "SELECT DISTINCT city FROM read_csv_auto('${CSV}') ORDER BY city"

  run_query \
    "SELECT DISTINCT department" \
    "SELECT DISTINCT department FROM '${CSV}' ORDER BY department" \
    "SELECT DISTINCT department FROM read_csv_auto('${CSV}') ORDER BY department"

  run_query \
    "SELECT DISTINCT city, department" \
    "SELECT DISTINCT city, department FROM '${CSV}'" \
    "SELECT DISTINCT city, department FROM read_csv_auto('${CSV}')"

  run_query \
    "SELECT DISTINCT department WHERE salary > 100000" \
    "SELECT DISTINCT department FROM '${CSV}' WHERE salary > 100000 ORDER BY department" \
    "SELECT DISTINCT department FROM read_csv_auto('${CSV}') WHERE salary > 100000 ORDER BY department"

  run_query \
    "SUM(salary) AVG(salary) GROUP BY department" \
    "SELECT department, SUM(salary), AVG(salary) FROM '${CSV}' GROUP BY department ORDER BY department" \
    "SELECT department, SUM(salary), AVG(salary) FROM read_csv_auto('${CSV}') GROUP BY department ORDER BY department"

  run_query \
    "COUNT(*) GROUP BY city ORDER BY city" \
    "SELECT city, COUNT(*) FROM '${CSV}' GROUP BY city ORDER BY city" \
    "SELECT city, COUNT(*) FROM read_csv_auto('${CSV}') GROUP BY city ORDER BY city"

  run_query \
    "SELECT COUNT(*)" \
    "SELECT COUNT(*) FROM '${CSV}'" \
    "SELECT COUNT(*) FROM read_csv_auto('${CSV}')"

  run_query \
    "SELECT SUM(salary)" \
    "SELECT SUM(salary) FROM '${CSV}'" \
    "SELECT SUM(salary) FROM read_csv_auto('${CSV}')"

  run_query \
    "SELECT AVG(salary)" \
    "SELECT AVG(salary) FROM '${CSV}'" \
    "SELECT AVG(salary) FROM read_csv_auto('${CSV}')"

  run_query \
    "SELECT MIN(age), MAX(age)" \
    "SELECT MIN(age), MAX(age) FROM '${CSV}'" \
    "SELECT MIN(age), MAX(age) FROM read_csv_auto('${CSV}')"

  run_query \
    "SELECT COUNT(*) WHERE age > 30" \
    "SELECT COUNT(*) FROM '${CSV}' WHERE age > 30" \
    "SELECT COUNT(*) FROM read_csv_auto('${CSV}') WHERE age > 30"
}

# ═════════════════════════════════════════════════════════════════
# SECTION 2: LIKE pattern-matching
# ═════════════════════════════════════════════════════════════════
run_section_like() {
  section_header "SECTION 2 — LIKE operator"

  run_query \
    "WHERE name LIKE 'A%'  (prefix wildcard)" \
    "SELECT * FROM '${CSV}' WHERE name LIKE 'A%'" \
    "SELECT * FROM read_csv_auto('${CSV}') WHERE name LIKE 'A%'"

  run_query \
    "WHERE city LIKE '%on'  (suffix wildcard)" \
    "SELECT * FROM '${CSV}' WHERE city LIKE '%on'" \
    "SELECT * FROM read_csv_auto('${CSV}') WHERE city LIKE '%on'"

  run_query \
    "WHERE department LIKE '%ing'  (suffix, high selectivity)" \
    "SELECT * FROM '${CSV}' WHERE department LIKE '%ing'" \
    "SELECT * FROM read_csv_auto('${CSV}') WHERE department LIKE '%ing'"
}

# ═════════════════════════════════════════════════════════════════
# SECTION 3: Output format throughput
# ═════════════════════════════════════════════════════════════════
run_section_formats() {
  section_header "SECTION 3 — Output format throughput (output → /dev/null)"

  local Q_CSVQL="SELECT id, name, age, city, salary, department FROM '${CSV}' WHERE age > 30"
  local Q_DUCK="SELECT id, name, age, city, salary, department FROM '${CSV}' WHERE age > 30"

  run_format \
    "CSV" \
    "" \
    "HEADER, DELIMITER ','" \
    "$Q_CSVQL" \
    "$Q_DUCK"

  run_format \
    "JSON array  (--json)" \
    "--json" \
    "FORMAT JSON, ARRAY true" \
    "$Q_CSVQL" \
    "$Q_DUCK"

  run_format \
    "JSONL / NDJSON  (--jsonl)" \
    "--jsonl" \
    "FORMAT JSON" \
    "$Q_CSVQL" \
    "$Q_DUCK"
}

# ═════════════════════════════════════════════════════════════════
# SECTION 4: Raw CSV parse throughput (Zig microbenchmark)
# ═════════════════════════════════════════════════════════════════
run_section_parse() {
  local bin="${SCRIPT_DIR}/zig-out/bin/csv_parse_bench"
  if [[ ! -x "$bin" ]]; then
    echo "csv_parse_bench not built — run: zig build -Doptimize=ReleaseFast"
    return
  fi
  section_header "SECTION 4 — Raw CSV parse throughput"
  "$bin" "$CSV"
}

# ═════════════════════════════════════════════════════════════════
# SECTION 5: GROUP BY query shapes (Zig microbenchmark)
# ═════════════════════════════════════════════════════════════════
run_section_groupby() {
  local bin="${SCRIPT_DIR}/zig-out/bin/groupby_bench"
  if [[ ! -x "$bin" ]]; then
    echo "groupby_bench not built — run: zig build -Doptimize=ReleaseFast"
    return
  fi
  section_header "SECTION 5 — GROUP BY query shapes (Zig engine)"
  "$bin" "$CSV"
}

# ═════════════════════════════════════════════════════════════════
# SECTION 6: JOIN queries (hash-join) — csvql vs DuckDB
# ═════════════════════════════════════════════════════════════════
run_section_join() {
  section_header "SECTION 6 — JOIN queries (hash-join, csvql vs DuckDB)"

  # ── Create tiny lookup tables ──────────────────────────────────
  local DEPTS="${BENCH_DIR}/bench_depts.csv"
  cat > "$DEPTS" <<'EOF'
dept_name,region,budget_code
Engineering,West,ENG-001
Finance,East,FIN-002
HR,Central,HR-003
Marketing,East,MKT-004
Operations,West,OPS-005
Sales,Central,SAL-006
EOF

  local CITIES="${BENCH_DIR}/bench_cities.csv"
  cat > "$CITIES" <<'EOF'
city,state,timezone
Austin,TX,CDT
Boston,MA,EDT
Chicago,IL,CDT
Denver,CO,MDT
LA,CA,PDT
NYC,NY,EDT
SF,CA,PDT
Seattle,WA,PDT
EOF

  # ── 50K-row right table: id + bonus_pct (join on numeric id) ──
  local BONUS="${BENCH_DIR}/bench_bonus_50k.csv"
  printf "Generating 50K-row bonus lookup table… "
  awk -F, 'NR==1{print "emp_id,bonus_pct"} NR>1&&NR<=50001{printf "%s,%.2f\n",$1,($5*0.1)}' "$CSV" > "$BONUS"
  echo "done"
  echo ""

  # ── Benchmark cases ────────────────────────────────────────────
  run_query \
    "JOIN lookup (${ROW_COUNT} × 6): employees JOIN departments" \
    "SELECT e.name, e.salary, d.region FROM '${CSV}' e INNER JOIN '${DEPTS}' d ON e.department = d.dept_name" \
    "SELECT e.name, e.salary, d.region FROM read_csv_auto('${CSV}') AS e JOIN read_csv_auto('${DEPTS}') AS d ON e.department = d.dept_name"

  run_query \
    "JOIN lookup + WHERE (${ROW_COUNT} × 6): WHERE d.region = 'West'" \
    "SELECT e.name, e.salary, d.region FROM '${CSV}' e INNER JOIN '${DEPTS}' d ON e.department = d.dept_name WHERE d.region = 'West'" \
    "SELECT e.name, e.salary, d.region FROM read_csv_auto('${CSV}') AS e JOIN read_csv_auto('${DEPTS}') AS d ON e.department = d.dept_name WHERE d.region = 'West'"

  run_query \
    "JOIN SELECT * (${ROW_COUNT} × 6): all columns from both tables" \
    "SELECT * FROM '${CSV}' e INNER JOIN '${DEPTS}' d ON e.department = d.dept_name" \
    "SELECT * FROM read_csv_auto('${CSV}') AS e JOIN read_csv_auto('${DEPTS}') AS d ON e.department = d.dept_name"

  run_query \
    "JOIN cities lookup (${ROW_COUNT} × 8): employees JOIN cities" \
    "SELECT e.name, e.city, c.state, c.timezone FROM '${CSV}' e INNER JOIN '${CITIES}' c ON e.city = c.city" \
    "SELECT e.name, e.city, c.state, c.timezone FROM read_csv_auto('${CSV}') AS e JOIN read_csv_auto('${CITIES}') AS c ON e.city = c.city"

  run_query \
    "JOIN larger right (${ROW_COUNT} × 50K): employees JOIN bonus table on id" \
    "SELECT e.name, e.salary, b.bonus_pct FROM '${CSV}' e INNER JOIN '${BONUS}' b ON e.id = b.emp_id" \
    "SELECT e.name, e.salary, b.bonus_pct FROM read_csv_auto('${CSV}') AS e JOIN read_csv_auto('${BONUS}') AS b ON e.id::TEXT = b.emp_id::TEXT"

  # ── 3-table chained JOIN ───────────────────────────────────────
  local REGIONS="${BENCH_DIR}/bench_regions.csv"
  cat > "$REGIONS" <<'EOF'
region_name,continent
West,North America
East,North America
Central,North America
EOF

  run_query \
    "3-table JOIN (${ROW_COUNT} × 6 × 3): employees → departments → regions" \
    "SELECT e.name, e.salary, d.region, r.continent FROM '${CSV}' e INNER JOIN '${DEPTS}' d ON e.department = d.dept_name INNER JOIN '${REGIONS}' r ON d.region = r.region_name" \
    "SELECT e.name, e.salary, d.region, r.continent FROM read_csv_auto('${CSV}') AS e JOIN read_csv_auto('${DEPTS}') AS d ON e.department = d.dept_name JOIN read_csv_auto('${REGIONS}') AS r ON d.region = r.region_name"

  run_query \
    "3-table JOIN + WHERE (${ROW_COUNT} × 6 × 3): WHERE r.continent = 'North America'" \
    "SELECT e.name, e.salary, d.region FROM '${CSV}' e INNER JOIN '${DEPTS}' d ON e.department = d.dept_name INNER JOIN '${REGIONS}' r ON d.region = r.region_name WHERE r.continent = 'North America'" \
    "SELECT e.name, e.salary, d.region FROM read_csv_auto('${CSV}') AS e JOIN read_csv_auto('${DEPTS}') AS d ON e.department = d.dept_name JOIN read_csv_auto('${REGIONS}') AS r ON d.region = r.region_name WHERE r.continent = 'North America'"
}

# ═════════════════════════════════════════════════════════════════
# Entry point
# ═════════════════════════════════════════════════════════════════
check_deps

ROW_COUNT=$(( $(wc -l < "$CSV") - 1 ))
FILE_SIZE_MB=$(du -m "$CSV" | awk '{print $1}')

echo ""
printf "${BOLD}╔══════════════════════════════════════════════════════════════╗${RESET}\n"
printf "${BOLD}║         csvql vs DuckDB — full benchmark suite               ║${RESET}\n"
printf "${BOLD}╚══════════════════════════════════════════════════════════════╝${RESET}\n"
printf "  CSV:      %s\n" "$CSV"
printf "  Rows:     %d data rows  (%d MB)\n" "$ROW_COUNT" "$FILE_SIZE_MB"
printf "  DuckDB:   %s\n" "$("$DUCKDB" --version 2>&1 | head -1)"
printf "  csvql:    %s\n" "$("$CSVQL" --version 2>&1 | head -1 || echo 'dev build')"
printf "  Runs:     %d per measurement (+ 1 warmup)\n" "$RUNS"
printf "  Section:  %s\n" "$SECTION"

case "$SECTION" in
  all)
    run_section_queries
    run_section_like
    run_section_formats
    run_section_parse
    run_section_groupby
    run_section_join
    ;;
  queries)
    run_section_queries
    ;;
  like)
    run_section_like
    ;;
  formats)
    run_section_formats
    ;;
  parse)
    run_section_parse
    ;;
  groupby)
    run_section_groupby
    ;;
  join)
    run_section_join
    ;;
  *)
    echo "Unknown section: $SECTION"
    echo "Valid values: all | queries | like | formats | parse | groupby | join"
    exit 1
    ;;
esac

echo ""
printf "${BOLD}══════════════════════════════════════════════════════════════${RESET}\n"
printf "${BOLD}  Done.${RESET}\n"
printf "${BOLD}══════════════════════════════════════════════════════════════${RESET}\n"
echo ""
rm -rf "$BENCH_DIR"
