#!/usr/bin/env bash
# gen_fixture.sh — Generate the large_test.csv fixture used by
# bench/verify_correctness.sh and bench/query_fuzz.sh.
#
# This used to be duplicated inline in ci.yml and nightly-fuzz.yml —
# identical today, but two copies drift silently the moment one is edited
# and the other isn't. Checking it in as one script makes fixture
# generation itself a reproducible, reviewable artifact instead of a
# YAML side effect: a VERIFICATION-LOG.md row's seed only replays the same
# query stream if it's run against the same fixture, and this is what
# guarantees that.
#
# No randomness or timestamps — same row count always produces the exact
# same bytes.
#
# Usage:
#   ./bench/gen_fixture.sh [output_path] [row_count]
#   ./bench/gen_fixture.sh                 # large_test.csv, 20000 rows (default, matches CI)

set -euo pipefail

OUT="${1:-large_test.csv}"
ROWS="${2:-20000}"

awk -v rows="$ROWS" 'BEGIN{
  split("Alice Bob Carol Dave Eve Frank Grace Heidi", names, " ");
  split("Austin Boston Chicago Denver LA NYC SF Seattle", cities, " ");
  split("Engineering Finance HR Marketing Operations Sales", depts, " ");
  print "id,name,age,city,salary,department";
  for (i = 1; i <= rows; i++) {
    n = names[(i % 8) + 1]; c = cities[(i % 8) + 1]; d = depts[(i % 6) + 1];
    age = 20 + (i % 40);
    salary = 40000 + (i % 80) * 1000;
    print i","n","age","c","salary","d;
  }
}' > "$OUT"
