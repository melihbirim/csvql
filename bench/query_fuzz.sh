#!/usr/bin/env bash
# query_fuzz.sh — Differential query generator for csvql vs DuckDB (#141).
#
# Deliberately scoped down from a full SQL grammar fuzzer (see issue #141 for
# why): single table, WHERE predicates + aggregates only, no JOINs, no
# shrinking. Template-based generation against the known large_test.csv
# schema, run both engines, diff — reuses the same comparison rules as
# verify_correctness.sh's check_approx (numeric rounding tolerance, empty
# field == NULL).
#
# Deterministic: the awk PRNG is seeded with --seed, so the same seed always
# generates the same query sequence — a mismatch always reproduces with the
# exact command printed alongside it. On a mismatch, the query pair is
# dumped to tests/regressions/ permanently instead of using a shrinker.
#
# Usage:
#   ./bench/query_fuzz.sh --seed 42 --count 2000
#   ./bench/query_fuzz.sh --seed 42 --count 2000 --csv large_test.csv
#
# Environment overrides:
#   DUCKDB_BIN — path to duckdb binary (default: duckdb in PATH)

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CSVQL="${SCRIPT_DIR}/zig-out/bin/csvql"
DUCKDB="${DUCKDB_BIN:-duckdb}"
CSV="${SCRIPT_DIR}/large_test.csv"
SEED=1
COUNT=1000
STOP_ON_FAIL=0
REGRESSIONS_DIR="${SCRIPT_DIR}/tests/regressions"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --seed) SEED="$2"; shift 2 ;;
    --count) COUNT="$2"; shift 2 ;;
    --csv) CSV="$2"; shift 2 ;;
    --csvql-bin) CSVQL="$2"; shift 2 ;;
    --duckdb-bin) DUCKDB="$2"; shift 2 ;;
    --stop-on-fail) STOP_ON_FAIL=1; shift ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ ! -x "$CSVQL" ]]; then
  echo "csvql not found at $CSVQL — run: zig build -Doptimize=ReleaseFast"
  exit 1
fi
if ! command -v "$DUCKDB" >/dev/null 2>&1; then
  echo "duckdb not found in PATH (set DUCKDB_BIN=/path/to/duckdb)"
  exit 1
fi
if [[ ! -f "$CSV" ]]; then
  echo "CSV fixture not found: $CSV"
  exit 1
fi

RED='\033[0;31m'
GREEN='\033[0;32m'
DIM='\033[2m'
RESET='\033[0m'

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
QUERIES_FILE="$TMP/queries.tsv"

# ── Generate the query sequence in one awk process (deterministic, given
# --seed) ─────────────────────────────────────────────────────────────────
# Each output line is: kind \t csvql_sql \t duck_sql
# \t is safe as a field separator here since none of the generated SQL
# contains a literal tab.
awk -v seed="$SEED" -v count="$COUNT" -v csv="$CSV" '
BEGIN {
    srand(seed)
    split("id age salary", numeric_cols, " ")
    split("name city department", string_cols, " ")
    split("id name age city salary department", all_cols_any, " ")
    split("COUNT SUM AVG MIN MAX", aggs, " ")
    split("= != > >= < <=", numeric_ops, " ")
    split("= !=", string_ops, " ")

    # Sample real values per string column from the fixture (first 5000
    # rows) so string predicates have a realistic hit rate.
    FS = ","
    getline header < csv
    n_hdr = split(header, hdr, ",")
    for (i = 1; i <= n_hdr; i++) col_idx[hdr[i]] = i
    row_n = 0
    while ((getline line < csv) > 0 && row_n < 5000) {
        row_n++
        n_fields = split(line, fields, ",")
        for (j = 1; j <= 3; j++) {
            c = string_cols[j]
            idx = col_idx[c]
            if (idx > 0 && idx <= n_fields && fields[idx] != "") {
                v = fields[idx]
                if (!(c SUBSEP v in seen_values)) {
                    seen_values[c SUBSEP v] = 1
                    n_values[c]++
                    values[c, n_values[c]] = v
                }
            }
        }
    }
    close(csv)

    for (q = 0; q < count; q++) {
        kind_roll = rand()
        if (kind_roll < 0.34) {
            emit_scalar_agg()
        } else if (kind_roll < 0.67) {
            emit_groupby_agg()
        } else {
            emit_projection()
        }
    }
}

function pick(arr, n) {
    return arr[int(rand() * n) + 1]
}

# One "col OP val" predicate — operator set depends on column type.
function gen_predicate() {
    r = rand()
    if (r < 0.15) {
        # IS NULL / IS NOT NULL — this fixture never has an empty field, so
        # IS NULL always matches zero rows and IS NOT NULL always matches
        # all of them, but that still exercises the operator itself: both
        # engines have to agree on "zero rows" or "every row", not just on
        # a value comparison.
        col = pick(all_cols_any, 6)
        return col ((rand() < 0.5) ? " IS NULL" : " IS NOT NULL")
    } else if (r < 0.5) {
        col = pick(numeric_cols, 3)
        op = pick(numeric_ops, 6)
        if (col == "id") val = int(rand() * 20000) + 1
        else if (col == "age") val = int(rand() * 50) + 15
        else val = int(rand() * 100000) + 30000
        return col " " op " " val
    } else if (r < 0.7) {
        # LIKE against a substring of a real sampled value — prefix,
        # suffix, or contains wildcard, so it actually matches something
        # instead of testing only the zero-row case.
        col = pick(string_cols, 3)
        nv = n_values[col]
        if (nv == 0) { val = "nobody" } else { val = values[col, int(rand() * nv) + 1] }
        vlen = length(val)
        piece_len = (vlen > 1) ? int(rand() * (vlen - 1)) + 1 : vlen
        shape = rand()
        if (shape < 0.34) pattern = substr(val, 1, piece_len) "%"
        else if (shape < 0.67) pattern = "%" substr(val, vlen - piece_len + 1)
        else pattern = "%" substr(val, 1, piece_len) "%"
        gsub(/'"'"'/, "'"'"''"'"'", pattern)  # escape single quotes for SQL
        return col " LIKE '"'"'" pattern "'"'"'"
    } else {
        col = pick(string_cols, 3)
        op = pick(string_ops, 2)
        nv = n_values[col]
        if (nv == 0) { val = "nobody" } else { val = values[col, int(rand() * nv) + 1] }
        gsub(/'"'"'/, "'"'"''"'"'", val)  # escape single quotes for SQL
        return col " " op " '"'"'" val "'"'"'"
    }
}

# A single predicate, or two joined with AND/OR.
function gen_where() {
    w = gen_predicate()
    if (rand() < 0.4) {
        joiner = (rand() < 0.5) ? "AND" : "OR"
        w = w " " joiner " " gen_predicate()
    }
    return w
}

function emit_scalar_agg() {
    agg = pick(aggs, 5)
    numcol = pick(numeric_cols, 3)
    where = gen_where()
    arg = (agg == "COUNT" && rand() < 0.3) ? "*" : numcol
    csvql_sql = "SELECT " agg "(" arg ") FROM '"'"'" csv "'"'"' WHERE " where
    duck_sql = "SELECT " agg "(" arg ") FROM read_csv_auto('"'"'" csv "'"'"') WHERE " where
    print "scalar_agg\t" csvql_sql "\t" duck_sql
}

function emit_groupby_agg() {
    group_col = pick(string_cols, 3)
    agg = pick(aggs, 5)
    numcol = pick(numeric_cols, 3)
    where = gen_where()
    arg = (agg == "COUNT" && rand() < 0.3) ? "*" : numcol
    csvql_sql = "SELECT " group_col ", " agg "(" arg ") FROM '"'"'" csv "'"'"' WHERE " where " GROUP BY " group_col " ORDER BY " group_col
    duck_sql = "SELECT " group_col ", " agg "(" arg ") FROM read_csv_auto('"'"'" csv "'"'"') WHERE " where " GROUP BY " group_col " ORDER BY " group_col
    print "groupby_agg\t" csvql_sql "\t" duck_sql
}

function emit_projection() {
    all_cols_n = 6
    split("id name age city salary department", all_cols, " ")
    n = int(rand() * 3) + 1
    # sample n distinct columns without replacement
    delete chosen
    cols_str = ""
    got = 0
    while (got < n) {
        c = all_cols[int(rand() * all_cols_n) + 1]
        if (!(c in chosen)) {
            chosen[c] = 1
            cols_str = (cols_str == "") ? c : cols_str ", " c
            got++
        }
    }
    where = gen_where()
    # ORDER BY id makes row order deterministic (id is unique) so output
    # can be diffed exactly instead of needing a sort-before-compare step.
    csvql_sql = "SELECT " cols_str " FROM '"'"'" csv "'"'"' WHERE " where " ORDER BY id"
    duck_sql = "SELECT " cols_str " FROM read_csv_auto('"'"'" csv "'"'"') WHERE " where " ORDER BY id"
    print "projection\t" csvql_sql "\t" duck_sql
}
' > "$QUERIES_FILE"

# ── Normalize + compare one pair of outputs ─────────────────────────────
# Rounds numeric-looking fields to 4dp (float formatting tolerance, same as
# verify_correctness.sh's check_approx) and maps an empty field to NULL,
# matching the documented empty-string-vs-NULL difference (CORRECTNESS.md)
# so it isn't reported as a mismatch — that's a known, intentional
# difference, not something this generator should rediscover every run.
normalize() {
  awk '{
    n = split($0, fields, ",")
    if (n == 0) {
      # awk split("", arr, sep) returns 0 fields, not one empty field — a
      # genuinely empty row (single empty-string column) would otherwise
      # silently vanish here instead of normalizing to NULL like it should.
      print "NULL"
      next
    }
    out = ""
    for (i = 1; i <= n; i++) {
      f = fields[i]
      if (f == "" || f == "NULL") f = "NULL"
      else if (f ~ /^-?[0-9]+(\.[0-9]+)?$/) f = sprintf("%.4f", f)
      out = (i == 1) ? f : out "," f
    }
    print out
  }'
}

total=0
failed=0
declare -a failure_paths=()

while IFS=$'\t' read -r kind csvql_sql duck_sql; do
  total=$((total + 1))

  out_csvql="$TMP/csvql_${total}.txt"
  out_duck="$TMP/duck_${total}.txt"
  err_csvql="$TMP/csvql_err_${total}.txt"
  err_duck="$TMP/duck_err_${total}.txt"

  "$CSVQL" "$csvql_sql" 2>"$err_csvql" | tail -n +2 | normalize > "$out_csvql"
  csvql_rc=${PIPESTATUS[0]}
  "$DUCKDB" -csv -noheader -c "$duck_sql" 2>"$err_duck" | normalize > "$out_duck"
  duck_rc=${PIPESTATUS[0]}

  if [[ $csvql_rc -ne 0 ]]; then
    echo "[seed=$SEED q=$total] csvql exited $csvql_rc: $csvql_sql"
    sed 's/^/  /' "$err_csvql"
    dump_path="$REGRESSIONS_DIR/seed${SEED}_q${total}_${kind}.txt"
    mkdir -p "$REGRESSIONS_DIR"
    {
      echo "# Reproduce: ./bench/query_fuzz.sh --seed $SEED --count $COUNT --stop-on-fail"
      echo "kind: $kind"
      echo "csvql_sql: $csvql_sql"
      echo "duck_sql: $duck_sql"
      echo "csvql_rc: $csvql_rc"
      echo "--- csvql stderr ---"
      cat "$err_csvql"
    } > "$dump_path"
    failure_paths+=("$dump_path")
    failed=$((failed + 1))
    [[ $STOP_ON_FAIL -eq 1 ]] && break
    continue
  fi
  if [[ $duck_rc -ne 0 ]]; then
    # DuckDB itself rejected the query — not a csvql bug, most likely the
    # generator producing something DuckDB's sniffer can't handle for this
    # fixture. Report but don't count as a csvql failure.
    echo "[seed=$SEED q=$total] duckdb exited $duck_rc (not a csvql bug, skipping): $duck_sql"
    continue
  fi

  if ! diff -q "$out_duck" "$out_csvql" > /dev/null 2>&1; then
    echo "[seed=$SEED q=$total] MISMATCH ($kind):"
    echo "  csvql: $csvql_sql"
    echo "  duck:  $duck_sql"
    diff "$out_duck" "$out_csvql" | head -10 | sed 's/^/    /' | \
      sed "s/^    </    ${DIM}duck  <${RESET}/" | \
      sed "s/^    >/    ${RESET}csvql >${RESET}/"
    dump_path="$REGRESSIONS_DIR/seed${SEED}_q${total}_${kind}.txt"
    mkdir -p "$REGRESSIONS_DIR"
    {
      echo "# Reproduce: ./bench/query_fuzz.sh --seed $SEED --count $COUNT --stop-on-fail"
      echo "kind: $kind"
      echo "csvql_sql: $csvql_sql"
      echo "duck_sql: $duck_sql"
      echo "--- csvql output ---"
      cat "$out_csvql"
      echo "--- duckdb output ---"
      cat "$out_duck"
    } > "$dump_path"
    echo "  dumped to $dump_path"
    failure_paths+=("$dump_path")
    failed=$((failed + 1))
    [[ $STOP_ON_FAIL -eq 1 ]] && break
  fi
done < "$QUERIES_FILE"

echo ""
echo "$total queries run, $failed mismatches, seed=$SEED"
if [[ $failed -gt 0 ]]; then
  echo "Regression files:"
  for p in "${failure_paths[@]}"; do echo "  $p"; done
  printf "\n  ${RED}FAIL${RESET}\n"
  exit 1
fi
printf "  ${GREEN}%s generated queries, zero divergence from DuckDB.${RESET}\n" "$total"
