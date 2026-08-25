# Verification Log

Append-only record of `bench/query_fuzz.sh` (#141) nightly runs. Each line is one run:
a random seed, a query count, and the mismatch count found. Anyone can re-run any line
exactly: `./bench/query_fuzz.sh --seed <seed> --count <queries>`.

Scope, stated honestly: single table, WHERE predicates (numeric + string columns,
compound AND/OR, LIKE, IS NULL/IS NOT NULL) and five aggregates (COUNT/SUM/AVG/MIN/MAX,
scalar and GROUP BY) against a fixed 6-column schema. No JOINs, no subqueries, no window
functions — see [CORRECTNESS.md](CORRECTNESS.md) and issue #141 for what's in and out of
scope, and why.

A mismatch here means a real bug was found — when that happens, the entry stays in this
log with the mismatch count, and the fix is a separate commit/issue referenced in the
line below. Zero mismatches forever would mean the generator stopped finding anything
new, not that csvql is bug-free outside this scope.

| Date | Seed | Queries | Mismatches | csvql | DuckDB | Notes |
| ---- | ---- | ------- | ---------- | ----- | ------ | ----- |
<!-- nightly-fuzz.yml appends new rows below this line -->
| 2026-08-22 | 1897126267 | 50000 | 0 | 2.5.0 | 1.5.5 |  |
| 2026-08-23 | 2459520179 | 50000 | 0 | 2.5.0 | 1.5.5 |  |
| 2026-08-24 | 422312471 | 50000 | 0 | 2.5.0 | 1.5.5 |  |
| 2026-08-25 | 3027931993 | 50000 | 0 | 2.5.0 | 1.5.5 |  |
