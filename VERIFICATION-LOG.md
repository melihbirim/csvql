# Verification Log

Append-only record of `bench/query_fuzz.sh` (#141) nightly runs. Each row is meant to be
self-contained: check out the `csvql` version named, install the `DuckDB` version named,
run `./bench/gen_fixture.sh` to regenerate the exact fixture (deterministic, no
randomness), then run the `Command` cell verbatim. See CORRECTNESS.md's "Determinism
guarantees" section for why all three of those steps matter, not just the seed.

`Distinct` and `Templates` are two different coverage numbers, and the difference between
them matters:

- **`Distinct`** — unique query *text* out of `Queries` total. Because query text embeds
  the randomly-drawn literals, this mostly measures literal churn rather than coverage:
  `WHERE salary != 90483` and `WHERE salary != 4122` count as two distinct queries while
  testing one code path. It does **not** saturate — measured on the generator as it
  stands: 81.5% distinct at count=50000, still 66.3% at count=400000, climbing
  near-linearly throughout. It will read ~81% at count=50000 no matter how narrow or wide
  the templates are, so **it is not the saturation signal** (an earlier version of this
  file claimed it was — that was wrong, and reading ~82% as evidence of a rich query
  space was reading the randomness of the literals, not the coverage).
- **`Templates`** — unique query *shape*: the same queries with literals normalized away
  and column names collapsed to their type class, leaving just the SQL surface exercised.
  This one does plateau, so this is the number that answers "has this run stopped finding
  new query shapes?". Measured: **~8,900 templates total**, and it's a property of the
  generator rather than the seed — 8424 / 8434 / 8466 at count=200000 for seeds
  99 / 12345 / 7. count=50000 reaches ~6,300 of them (~71%); count=200000 reaches ~8,400
  (~95%); another 200,000 queries past that buys only ~450 new templates.

So the honest read of a typical nightly row is "~71% of the generator's own template space
covered", not the ~81% the `Distinct` column suggests. Widen the templates when
`Templates` sits near its ceiling while the SQL surface csvql supports has grown past it —
which is already true today: see Scope below.

Scope, stated honestly: single table, WHERE predicates (numeric + string columns,
compound AND/OR, LIKE, IS NULL/IS NOT NULL) and five aggregates (COUNT/SUM/AVG/MIN/MAX,
scalar and GROUP BY) against a fixed 6-column schema. Nine operators in total: `=`, `!=`,
`<`, `<=`, `>`, `>=`, `LIKE`, `IS NULL`, `IS NOT NULL`.

**`IS NULL` / `IS NOT NULL` are emitted but not meaningfully tested, and listing them
above without this caveat overstated what these runs prove.** `gen_fixture.sh` produces no
empty fields, so on this fixture `IS NULL` matches zero rows and `IS NOT NULL` matches
every row, always — the two engines are being asked to agree on a constant, not on NULL
semantics. That is 15% of every generated predicate (`query_fuzz.sh`'s `gen_predicate`
picks them at `r < 0.15`; measured 84,442 of 560,070 predicate occurrences across 400,000
queries). Three-valued logic — `NULL` inside `AND`/`OR` chains, `COUNT(col)` vs
`COUNT(*)`, `GROUP BY` over a nullable column, aggregates skipping nulls — is
**not covered by any of the rows in this log**. It is covered only by hand-written checks
in `bench/verify_correctness.sh`. Fixing this needs empty fields in the fixture, which
needs fixture versioning first: these rows' replay contract is "run `gen_fixture.sh`, then
the `Command` cell", so changing that script's schema silently invalidates every row above
it.

What that leaves untested by *this* generator, counted directly over 400,000 generated
queries — every one of these appeared exactly **zero** times, and every one of them is
SQL csvql supports: `JOIN`, `DISTINCT`, `LIMIT`, `BETWEEN`, `IN` / `NOT IN` (literal or
subquery), `HAVING`, `COUNT(DISTINCT ...)`, every scalar function (`UPPER`/`LOWER`/
`TRIM`/`SUBSTR`/`REPLACE`/`COALESCE`/`CAST`/`ROUND`/...), and every date function
(`DATEDIFF`/`DATEADD`/`STRFTIME`/`DATE_PART`). Those shapes are covered only by the ~95
hand-written differential checks in `bench/verify_correctness.sh`, not by generated
volume. See [CORRECTNESS.md](CORRECTNESS.md) and issue #141 for what's in and out of
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

`Templates` is backfilled for the 2026-08-26 row only. That backfill is exact rather than
estimated: re-running its recorded seed reproduced its `Distinct` value of 40707 on the
nose, which is direct evidence the generator is byte-for-byte unchanged since that run, so
the template count derived from the same replay is the number that run would have printed.
Earlier rows are left blank under the same policy as `Distinct` above.

| Date | Seed | Queries | Distinct | Templates | Mismatches | csvql | DuckDB | Command | Notes |
| ---- | ---- | ------- | -------- | --------- | ---------- | ----- | ------ | ------- | ----- |
<!-- nightly-fuzz.yml appends new rows below this line -->
| 2026-08-22 | 1897126267 | 50000 |  |  | 0 | 2.5.0 | 1.5.5 | `./bench/query_fuzz.sh --seed 1897126267 --count 50000` |  |
| 2026-08-23 | 2459520179 | 50000 |  |  | 0 | 2.5.0 | 1.5.5 | `./bench/query_fuzz.sh --seed 2459520179 --count 50000` |  |
| 2026-08-24 | 422312471 | 50000 |  |  | 0 | 2.5.0 | 1.5.5 | `./bench/query_fuzz.sh --seed 422312471 --count 50000` |  |
| 2026-08-25 | 3027931993 | 50000 |  |  | 0 | 2.5.0 | 1.5.5 | `./bench/query_fuzz.sh --seed 3027931993 --count 50000` |  |
| 2026-08-26 | 880127841 | 50000 | 40707 | 6214 | 0 | 2.5.0 | 1.5.5 | `./bench/query_fuzz.sh --seed 880127841 --count 50000` |  |
| 2026-08-27 | 164236225 | 50000 | 41074 | 6255 | 0 | 2.5.0 | 1.5.5 | `./bench/query_fuzz.sh --seed 164236225 --count 50000` |  |
| 2026-08-28 | 2576729949 | 50000 | 40831 | 6261 | 0 | 2.6.0 | 1.5.5 | `./bench/query_fuzz.sh --seed 2576729949 --count 50000` |  |
| 2026-08-29 | 179854169 | 50000 | 40917 | 6228 | 0 | 2.6.0 | 1.5.5 | `./bench/query_fuzz.sh --seed 179854169 --count 50000` |  |
| 2026-08-30 | 296285844 | 50000 | 40821 | 6231 | 0 | 2.6.0 | 1.5.5 | `./bench/query_fuzz.sh --seed 296285844 --count 50000` |  |
| 2026-08-31 | 1719510575 | 50000 | 40994 | 6286 | 0 | 2.6.0 | 1.5.5 | `./bench/query_fuzz.sh --seed 1719510575 --count 50000` |  |
