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
    duck_sql = "SELECT " agg "(" arg ") FROM __fuzz_csv WHERE " where
    print "scalar_agg\t" csvql_sql "\t" duck_sql
}

function emit_groupby_agg() {
    group_col = pick(string_cols, 3)
    agg = pick(aggs, 5)
    numcol = pick(numeric_cols, 3)
    where = gen_where()
    arg = (agg == "COUNT" && rand() < 0.3) ? "*" : numcol
    csvql_sql = "SELECT " group_col ", " agg "(" arg ") FROM '"'"'" csv "'"'"' WHERE " where " GROUP BY " group_col " ORDER BY " group_col
    duck_sql = "SELECT " group_col ", " agg "(" arg ") FROM __fuzz_csv WHERE " where " GROUP BY " group_col " ORDER BY " group_col
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
    duck_sql = "SELECT " cols_str " FROM __fuzz_csv WHERE " where " ORDER BY id"
    print "projection\t" csvql_sql "\t" duck_sql
}
' > "$QUERIES_FILE"

# ── Batch the DuckDB side into ONE process ──────────────────────────────
# csvql's own startup is ~5ms; the DuckDB CLI's is ~85ms (measured) — nearly
# all of this script's wall time was DuckDB process spawn, not query
# execution. Running N statements through one `duckdb -f` invocation
# instead of N separate invocations is the difference between a few hundred
# queries and tens of thousands in the same wall time.
#
# `.bail off` makes DuckDB continue past a statement error instead of
# aborting the whole batch (default CLI behavior stops at the first error,
# confirmed by testing — a batch with 300+ generated queries WILL contain
# at least one DuckDB-side quirk eventually). A BEGIN/END marker row wraps
# each query so its output block is unambiguous regardless of row count —
# including zero, which is a legitimate result for some generated queries,
# not just a marker of failure.
#
# CREATE TABLE once instead of read_csv_auto() per statement: the CSV
# sniffing/type-inference read_csv_auto does on every call turned out to be
# the dominant cost once process-spawn was eliminated (measured: 300 calls
# to read_csv_auto() in one duckdb process still took ~17s; against a
# pre-materialized table, the same 300 queries took ~0.15s). Every
# generated duck_sql references the fixed table name __fuzz_csv.
BATCH_SQL="$TMP/batch.sql"
BATCH_OUT="$TMP/batch_out.txt"
BATCH_ERR="$TMP/batch_err.txt"
{
  echo ".bail off"
  echo "CREATE TABLE __fuzz_csv AS SELECT * FROM read_csv_auto('${CSV}');"
  i=0
  while IFS=$'\t' read -r _kind _csvql_sql duck_sql; do
    echo "SELECT '@@BEGIN${i}@@' AS m;"
    echo "$duck_sql;"
    echo "SELECT '@@END${i}@@' AS m;"
    i=$((i + 1))
  done < "$QUERIES_FILE"
} > "$BATCH_SQL"

"$DUCKDB" -csv -noheader -f "$BATCH_SQL" > "$BATCH_OUT" 2> "$BATCH_ERR"

# Split the one combined DuckDB output back into per-query files, keyed by
# the BEGIN<i>/END<i> markers.
awk -v tmp="$TMP" '
  /^@@BEGIN[0-9]+@@$/ { idx = substr($0, 8, length($0) - 9); out = tmp "/duck_" idx ".txt"; next }
  /^@@END[0-9]+@@$/ { close(out); out = ""; next }
  out != "" { print > out }
' "$BATCH_OUT"

# ── Run csvql (the irreducible per-query cost — this is the thing under
# test) once per query, raw output only, no pipe through tail/awk/diff. All
# normalization and comparison happens afterward in ONE awk pass instead of
# ~4 subprocess spawns per query (tail + 2×normalize + diff) — that
# bash-loop overhead, not query execution, was the dominant cost once the
# DuckDB side was batched (measured: ~50ms/query wall time against ~5ms
# actual csvql runtime).
total=0
mkdir -p "$REGRESSIONS_DIR"
: > "$TMP/csvql_failures.tsv"
i=0
while IFS=$'\t' read -r kind csvql_sql duck_sql; do
  i=$((i + 1))
  total=$((total + 1))
  "$CSVQL" "$csvql_sql" > "$TMP/csvql_${i}.txt" 2> "$TMP/csvql_err_${i}.txt"
  csvql_rc=$?
  if [[ $csvql_rc -ne 0 ]]; then
    printf '%s\t%s\t%s\t%s\t%s\n' "$i" "$kind" "$csvql_sql" "$duck_sql" "$csvql_rc" >> "$TMP/csvql_failures.tsv"
    if [[ $STOP_ON_FAIL -eq 1 ]]; then break; fi
  fi
done < "$QUERIES_FILE"

declare -a failure_paths=()
failed=0

# Report csvql-side crashes/errors first (these are always real bugs, never
# a "DuckDB disagreed" case).
if [[ -s "$TMP/csvql_failures.tsv" ]]; then
  while IFS=$'\t' read -r i kind csvql_sql duck_sql csvql_rc; do
    echo "[seed=$SEED q=$i] csvql exited $csvql_rc: $csvql_sql"
    sed 's/^/  /' "$TMP/csvql_err_${i}.txt"
    dump_path="$REGRESSIONS_DIR/seed${SEED}_q${i}_${kind}.txt"
    {
      echo "# Reproduce: ./bench/query_fuzz.sh --seed $SEED --count $COUNT --stop-on-fail"
      echo "kind: $kind"
      echo "csvql_sql: $csvql_sql"
      echo "duck_sql: $duck_sql"
      echo "csvql_rc: $csvql_rc"
      echo "--- csvql stderr ---"
      cat "$TMP/csvql_err_${i}.txt"
    } > "$dump_path"
    failure_paths+=("$dump_path")
    failed=$((failed + 1))
  done < "$TMP/csvql_failures.tsv"
fi

# One awk pass: normalize + diff every (csvql_i, duck_(i-1)) file pair and
# report mismatches — replaces what used to be a diff + 2×normalize
# subprocess spawn per query.
MISMATCH_REPORT="$TMP/mismatches.tsv"
awk -v tmp="$TMP" -v total="$total" -v queries_file="$QUERIES_FILE" -v failtsv="$TMP/csvql_failures.tsv" '
  # macOS one-true-awk quirk: a name used as an array anywhere in the
  # program (even in an unrelated function-local scope) cannot also be used
  # as a scalar anywhere else, including a different functions parameter of
  # the same name. Every identifier below is unique across the whole
  # script on purpose — do not reuse a short name like "f" or "line" again.
  function normalize_field(val) {
    if (val == "" || val == "NULL") return "NULL"
    if (val ~ /^-?[0-9]+(\.[0-9]+)?$/) return sprintf("%.4f", val)
    return val
  }
  function normalize_row(rawrow,    nfields, colidx, colparts, outrow, normed) {
    nfields = split(rawrow, colparts, ",")
    if (nfields == 0) return "NULL"
    outrow = ""
    for (colidx = 1; colidx <= nfields; colidx++) {
      normed = normalize_field(colparts[colidx])
      outrow = (colidx == 1) ? normed : outrow "," normed
    }
    return outrow
  }
  function read_rows(path, dest, skip_first,    rowtext, is_header, rowcount) {
    rowcount = 0
    is_header = skip_first
    while ((getline rowtext < path) > 0) {
      if (is_header) { is_header = 0; continue }
      dest[rowcount++] = normalize_row(rowtext)
    }
    close(path)
    return rowcount
  }
  BEGIN {
    while ((getline tsvline < failtsv) > 0) {
      split(tsvline, failparts, "\t")
      skip_idx[failparts[1]] = 1
    }
    close(failtsv)

    qnum = 0
    while ((getline tsvline < queries_file) > 0) {
      qnum++
      if (qnum in skip_idx) continue
      split(tsvline, qparts, "\t")
      qkind = qparts[1]; qcsvql_sql = qparts[2]; qduck_sql = qparts[3]

      n_csvql_rows = read_rows(tmp "/csvql_" qnum ".txt", csvql_rows, 1)  # csvql always writes a header row
      n_duck_rows = read_rows(tmp "/duck_" (qnum - 1) ".txt", duck_rows, 0)  # -noheader: no header row to skip

      row_mismatch = (n_csvql_rows != n_duck_rows)
      if (!row_mismatch) {
        for (rowidx = 0; rowidx < n_csvql_rows; rowidx++) {
          if (csvql_rows[rowidx] != duck_rows[rowidx]) { row_mismatch = 1; break }
        }
      }
      if (row_mismatch) {
        dumpfile = tmp "/dump_" qnum ".txt"
        print "kind: " qkind > dumpfile
        print "csvql_sql: " qcsvql_sql >> dumpfile
        print "duck_sql: " qduck_sql >> dumpfile
        print "--- csvql output (normalized) ---" >> dumpfile
        for (rowidx = 0; rowidx < n_csvql_rows; rowidx++) print csvql_rows[rowidx] >> dumpfile
        print "--- duckdb output (normalized) ---" >> dumpfile
        for (rowidx = 0; rowidx < n_duck_rows; rowidx++) print duck_rows[rowidx] >> dumpfile
        close(dumpfile)
        print qnum "\t" qkind "\t" qcsvql_sql "\t" qduck_sql
      }
    }
  }
' > "$MISMATCH_REPORT"

if [[ -s "$MISMATCH_REPORT" ]]; then
  while IFS=$'\t' read -r i kind csvql_sql duck_sql; do
    echo "[seed=$SEED q=$i] MISMATCH ($kind):"
    echo "  csvql: $csvql_sql"
    echo "  duck:  $duck_sql"
    head -20 "$TMP/dump_${i}.txt" | tail -n +4 | sed 's/^/    /'
    dump_path="$REGRESSIONS_DIR/seed${SEED}_q${i}_${kind}.txt"
    {
      echo "# Reproduce: ./bench/query_fuzz.sh --seed $SEED --count $COUNT --stop-on-fail"
      cat "$TMP/dump_${i}.txt"
    } > "$dump_path"
    echo "  dumped to $dump_path"
    failure_paths+=("$dump_path")
    failed=$((failed + 1))
  done < "$MISMATCH_REPORT"
fi

echo ""
if [[ -s "$BATCH_ERR" ]]; then
  echo "DuckDB reported $(wc -l < "$BATCH_ERR" | tr -d ' ') stderr line(s) somewhere in the batch"
  echo "(a query the generator produced that DuckDB itself rejected — not"
  echo "necessarily a csvql bug; if a MISMATCH above looks like this, check"
  echo "whether the DuckDB output is simply missing for that query):"
  sed 's/^/  /' "$BATCH_ERR"
fi
echo "$total queries run, $failed mismatches, seed=$SEED"
if [[ $failed -gt 0 ]]; then
  echo "Regression files:"
  for p in "${failure_paths[@]}"; do echo "  $p"; done
  printf "\n  ${RED}FAIL${RESET}\n"
  exit 1
fi
printf "  ${GREEN}%s generated queries, zero divergence from DuckDB.${RESET}\n" "$total"
