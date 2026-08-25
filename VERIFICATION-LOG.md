# Verification Log

Append-only record of `bench/query_fuzz.sh` (#141) nightly runs. Each row is meant to be
self-contained: check out the `csvql` version named, install the `DuckDB` version named,
run `./bench/gen_fixture.sh` to regenerate the exact fixture (deterministic, no
randomness), then run the `Command` cell verbatim. See CORRECTNESS.md's "Determinism
guarantees" section for why all three of those steps matter, not just the seed.

`Distinct` is the number of distinct generated queries (deduped on query text) out of
`Queries` total — it's the signal for when a seed/count combination has stopped finding
anything new about the query space, as opposed to just re-running shapes already covered.

Scope, stated honestly: single table, WHERE predicates (numeric + string columns,
compound AND/OR, LIKE, IS NULL/IS NOT NULL) and five aggregates (COUNT/SUM/AVG/MIN/MAX,
scalar and GROUP BY) against a fixed 6-column schema. No JOINs, no subqueries, no window
functions — see [CORRECTNESS.md](CORRECTNESS.md) and issue #141 for what's in and out of
scope, and why.

A mismatch here means a real bug was found — when that happens, the entry stays in this
log with the mismatch count, and the fix is a separate commit/issue referenced in the
line below. Zero mismatches forever would mean the generator stopped finding anything
new, not that csvql is bug-free outside this scope.

Rows before 2026-08-26 predate the `Distinct` and `Command` columns; `Distinct` is left
blank for them rather than backfilled, since recomputing it would require re-running the
generator and isn't guaranteed to reproduce byte-for-byte if the generator's template set
has changed since. `Command` is backfilled — it's fully determined by the `Seed` and
`Queries` columns already recorded.

| Date | Seed | Queries | Distinct | Mismatches | csvql | DuckDB | Command | Notes |
| ---- | ---- | ------- | -------- | ---------- | ----- | ------ | ------- | ----- |
<!-- nightly-fuzz.yml appends new rows below this line -->
| 2026-08-22 | 1897126267 | 50000 |  | 0 | 2.5.0 | 1.5.5 | `./bench/query_fuzz.sh --seed 1897126267 --count 50000` |  |
| 2026-08-23 | 2459520179 | 50000 |  | 0 | 2.5.0 | 1.5.5 | `./bench/query_fuzz.sh --seed 2459520179 --count 50000` |  |
| 2026-08-24 | 422312471 | 50000 |  | 0 | 2.5.0 | 1.5.5 | `./bench/query_fuzz.sh --seed 422312471 --count 50000` |  |
| 2026-08-25 | 3027931993 | 50000 |  | 0 | 2.5.0 | 1.5.5 | `./bench/query_fuzz.sh --seed 3027931993 --count 50000` |  |
