# Correctness

csvql treats [DuckDB](https://duckdb.org) as the reference implementation and diffs its own
output against DuckDB's for every query shape it supports. This page documents what is
tested, how, and — just as important — what isn't tested yet or isn't supported at all.

Run it yourself: `zig build verify -Doptimize=ReleaseFast` (or directly:
`./bench/verify_correctness.sh`). Needs a `duckdb` binary in `PATH` (or set
`DUCKDB_BIN=/path/to/duckdb`). Takes well under a minute.

## What is tested

- **95 differential checks** in `bench/verify_correctness.sh` (96 when the optional
  multi-GB taxi fixture is present locally — see below), covering: SELECT/projection,
  every WHERE operator (`=`, comparisons, `LIKE`/`ILIKE`, `BETWEEN`, `IN`/`NOT IN`
  (literal list and `IN (SELECT ...)` subquery, #124), `IS NULL`, modulo, compound
  `AND`/`OR`/`NOT`), GROUP BY + all aggregate functions
  (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `VARIANCE`, `STDDEV`, `MEDIAN`, `GROUP_CONCAT`,
  `COUNT(DISTINCT)`), HAVING, DISTINCT, ORDER BY (single/multi-column, positional, alias),
  LIMIT, every scalar function (`UPPER`/`LOWER`/`TRIM`/`LENGTH`/`SUBSTR`/`REPLACE`/
  `SPLIT_PART`/`GREATEST`/`LEAST`/`ABS`/`CEIL`/`FLOOR`/`MOD`/`ROUND`/`COALESCE`/`CAST`,
  one level of nesting), date/time functions (`DATEDIFF`, `DATEADD`, `STRFTIME`,
  `DATE_PART`, including mixed ISO/US/EU date formats in a single column), JOIN
  (including a 3-table chain and a 50K-row hash probe), table aliases, and a wide
  adversarial CSV fixture set: embedded commas/newlines/quotes, escaped `""`, CRLF line
  endings, a UTF-8 BOM, emoji/RTL/combining-mark unicode (matched via `LIKE`, not
  diffed raw — see below), scientific notation and negative zero, header-only/
  single-row/single-column files, ragged rows, a zero-byte file, and values near
  i64/f64 limits.
- **550 unit tests** (`zig build test`) covering internals differential testing can't
  reach directly: parser edge cases, arena-buffer offset safety across reallocation,
  overflow handling, TDD regression tests for every numbered bug fix referenced below.
- **2 platforms in CI**: `ubuntu-latest` (x86_64) and `macos-14` (Apple Silicon,
  aarch64) — both run the full differential suite against DuckDB, not just the build.
  ARM matters here specifically because csvql's speed comes from SIMD, and ARM is where
  most desktop users actually run it.
- **Self-differential guard for the #139 bug class**: a ~44MB synthetic quote-heavy
  fixture (row boundaries deliberately drift across many 2MB marks) run at
  `--threads 1` vs `--threads 0` (auto) and diffed against *itself* — no DuckDB needed,
  catches the parallel-scan-loses-quote-state-across-a-buffer-boundary shape of bug
  directly.
- **Differential query generator** (`bench/query_fuzz.sh`, #141): template-based, not a
  full grammar fuzzer — single table, WHERE predicates (numeric + string columns,
  compound AND/OR, `LIKE`, `IS NULL`/`IS NOT NULL`) and aggregates
  (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`, scalar and `GROUP BY`) against the existing fixture
  schema. No JOINs, no subqueries, no window functions — out of scope by design, see
  the issue for why. Deterministic seeding (`--seed`) so every mismatch reproduces with
  one command — see [Determinism guarantees](#determinism-guarantees) for what that
  actually rests on and how it's verified, not just asserted; failures dump to
  `tests/regressions/` permanently instead of using a shrinker. DuckDB's side runs as
  one batched process (materialized table + all
  queries in a single `duckdb -f` invocation) rather than one process per query —
  the difference between hundreds and tens of thousands of queries in the same wall
  time.
  - **Per PR**: fixed seed, 300 queries (~15s) — catches a regression before merge.
  - **Nightly**: random seed, higher volume — this is where the cumulative number
    comes from. Every run appends one line to [VERIFICATION-LOG.md](VERIFICATION-LOG.md),
    win or lose; a mismatch stays in the log with its count, not just the zeros.
  - **Per release tag**: not yet wired up — see Known gaps.

## Oracle methodology

- **Reference**: DuckDB CLI, `duckdb --version` printed at the top of every suite run.
  Version is pinned via `bench/DUCKDB_VERSION` (currently `1.5.5`) and bumped
  deliberately, not tracked to `latest` — see [Determinism guarantees](#determinism-guarantees).
- **Comparison rule**: exact string equality on row output, *except* for results
  containing a float (`AVG`, `VARIANCE`, `STDDEV`, non-integer `SUM`), which go through
  `check_approx` — see [Float tolerance](#float-tolerance) below.
- **Row order**: compared **exactly** when the query has an `ORDER BY` — order is a
  claim under test, sorting both sides before diffing would hide a wrong-order bug.
  Sorted before diffing only when the query has no defined order (JOIN, GROUP BY/DISTINCT
  without ORDER BY), where the two engines' internal row order is legitimately allowed to
  differ. Every ORDER BY check in the suite sorts on a column combination that's unique
  per row (adding `id` as a tiebreaker where the natural key isn't unique), so exact-order
  comparison is well-defined rather than coincidentally passing.
- **A check FAILs, never silently passes, when**: either engine exits non-zero (crash,
  parse error, anything) — the captured stderr is printed; or both sides return zero
  rows, since an empty-vs-empty "match" on a suite where no check is designed to expect
  an empty result is almost always two broken queries, not a real one.
- **Unicode fields are compared via a `WHERE ... LIKE` predicate that returns only the
  id column**, not by diffing the raw field value directly. DuckDB's CSV writer quotes
  any field containing non-ASCII bytes; csvql only quotes per RFC 4180 (delimiter/quote/
  newline present). Diffing the quoted-vs-unquoted output directly would fail on that
  formatting difference, not a content difference.
- **A few fixtures compare against csvql alone, not DuckDB**, when DuckDB's own behavior
  isn't a meaningful reference for that specific input (a leading `+`/`0` making DuckDB's
  CSV sniffer bail to `VARCHAR`, DuckDB reformatting a huge integer through `DOUBLE`
  rounding on a plain projection, ragged rows confusing DuckDB's header detection
  entirely, DuckDB silently returning empty for a 0-byte file instead of erroring). These
  pin down csvql's own current, deliberate behavior instead — see the table below.

## Determinism guarantees

`bench/query_fuzz.sh` and its `VERIFICATION-LOG.md` are only evidence if a logged row can
actually be replayed. That rests on three separate guarantees — worth stating explicitly,
because only some of them are automatic and the others silently stop holding if broken:

- **Same seed → same query stream.** `bench/query_fuzz.sh` seeds its *own* PRNG
  (`mysrand`/`myrand`, a small MINSTD LCG implemented directly in the awk script) rather
  than calling awk's built-in `srand()`/`rand()`. This isn't a style preference: it was
  found, while adding this section, that at least one `mawk` build (compiled against
  `arc4random` for its rand-funcs backend — visible via `awk -W version`) silently ignores
  `srand()`'s seed argument and reseeds from OS entropy on every process start, so the
  *same* `--seed` produced a *different* query stream on every single invocation. Rolling
  our own seeded LCG removes the dependency on the host's awk build entirely. This is
  verified continuously, not just asserted: `./bench/query_fuzz.sh --seed N --count M
  --check-determinism` regenerates the stream twice and diffs them, and runs in CI
  (`ci.yml`, before either engine is even invoked) on every PR.
- **Query-space size, honestly.** The template space is bounded (a handful of aggregates,
  three query shapes, predicates over six columns), so a naive guess is that most of a
  50,000-query run is duplicates. Measured directly (dedup the generated SQL text) across
  several real seeds at `--count 50000`: **~82% distinct** (e.g. 41,010 of 50,000 for
  seed 1), consistent run to run — the space is far less saturated than that guess. This
  is what `VERIFICATION-LOG.md`'s `Distinct` column reports per run, and it's the signal
  for when to widen the generator's template set: watch for that percentage trending down
  as more seeds accumulate, not a one-time guess.
- **See it without reading awk.** `bench/sample-queries.sql` is 50 real generated queries
  (`./bench/query_fuzz.sh --seed 1 --count 50 --dump-queries bench/sample-queries.sql`,
  committed) — regenerate it after any change to the generator's template logic so it
  stays representative. `--dump-queries` writes the generated SQL and exits without
  touching either engine.
- **Same seed → same fixture.** The query stream is generated *against* `large_test.csv`,
  so reproducing it also requires the fixture to be byte-identical. `bench/gen_fixture.sh`
  is the single source of truth for that file — it used to be duplicated inline in
  `ci.yml` and `nightly-fuzz.yml`, which worked only as long as both copies stayed in
  sync. It's deterministic (no randomness, no timestamps: pure modulo arithmetic over a
  row index), so `./bench/gen_fixture.sh` always produces the same bytes.
- **Same versions → same result.** A replayed seed against a *different* csvql or DuckDB
  version is a legitimate regression check, not a reproduction of the original logged
  result — the whole point of running new code against an old query set. DuckDB is
  pinned via `bench/DUCKDB_VERSION` (bumped deliberately, not tracked to `latest`) so the
  oracle a log row names is the oracle that actually ran; csvql's version is whatever's
  checked out. Every `VERIFICATION-LOG.md` row records both, plus the exact `Command` to
  run, so a row is self-contained: check out the named csvql version, install the named
  DuckDB version, run `gen_fixture.sh`, then the row's `Command` verbatim.

### Float tolerance

`check_approx` rounds every numeric-looking field on both sides to 4 decimal places
before diffing (configurable per-check). This is a **formatting** tolerance, not a
numerical-accuracy one — both engines compute with `f64`, so any genuine precision
divergence beyond float-rounding noise still fails at 4 decimal places. It exists because
csvql and DuckDB can format the identical `f64` value with a different number of trailing
digits (`2600` vs `2600.0`), which isn't a correctness bug.

## Known differences (intentional)

Found via differential testing, kept as deliberate design choices rather than "fixed" —
documented so they don't surprise anyone migrating queries from DuckDB. Full detail and
examples in the [README](README.md#known-differences-from-duckdb).

| Behavior | csvql | DuckDB |
| -------- | ----- | ------ |
| `LENGTH(col)` on a unicode string | Byte length (UTF-8 bytes) | Character count (codepoints) |
| Empty CSV field | Stays an empty string | Inferred as `NULL` |
| `DATEDIFF('hour'/'minute'/etc, a, b)` on a non-exact interval | Fractional (e.g. `8.5`) | Truncated (e.g. `8`) |
| Numeric literal with a leading `0` (`007`) or `+` (`+5`) | Parses as a number | CSV sniffer infers `VARCHAR` for the whole column, `SUM`/etc then error |
| A huge integer literal (e.g. `9223372036854775807`) in a plain (non-aggregate) `SELECT` | Passed through unchanged, exact text preserved | CSV sniffer infers `DOUBLE`, reformats via IEEE-754 rounding + scientific notation even with no computation |
| Ragged rows (fewer/more fields than the header) | Short rows padded with empty fields, long rows truncated to header width | CSV sniffer can lose confidence in the header entirely and re-infer the first row as data (`column0`, `column1`, ...) |
| A zero-byte input file | `error.EmptyFile` (exit 3) | Returns silently with zero rows, no error |

These are intentionally excluded from the differential suite's pass/fail assertions —
asserting parity here would be asserting the wrong thing. The last four rows are locked
in as csvql-only regression checks in `bench/verify_correctness.sh` instead of a DuckDB
diff, since DuckDB's own behavior in each case isn't the property being tested.

## Subqueries

The one supported shape is `col IN (SELECT ...)` / `col NOT IN (SELECT ...)` (#124) —
non-correlated (the subquery has no access to the outer row) and single-column (the
subquery must project exactly one column). It's resolved once, before the outer query
runs: the inner query executes as a complete, ordinary csvql query in its own right —
its own `WHERE`, `GROUP BY`/`HAVING`, `ORDER BY`, `DISTINCT`, `LIMIT` all work normally —
and its output column becomes the membership list for the outer `IN`/`NOT IN`. A
correlated subquery (referencing the outer table's column) isn't silently wrong: the
inner query simply doesn't have that column, so it fails with a clear `ColumnNotFound`
rather than an incorrect result. Every other subquery shape — `FROM (SELECT ...)`, a
scalar subquery in the `SELECT` list, `HAVING agg > (SELECT ...)` — is still a clear
`error.SubqueriesNotSupported`, not a silent misparse.

Three real bugs were found and fixed while adding this (TDD + differential testing
against DuckDB doing exactly what they're for):
- `bench/verify_correctness.sh` caught a **segfault** on `col IN (SELECT ...) AND
  other_condition` — `Expression`'s `.binary`/`.unary` variants hold heap pointers shared
  across every copy of a `Query`, while `.comparison` is stored inline and copied by
  value. Resolving a subquery through a copy handled those two cases in exactly the
  wrong way (double-free for the shared/compound case, a leak for the bare-comparison
  case). Fixed by resolving on the caller's one addressable `Query`, before it's ever
  copied — see `resolveInSubqueries` in `engine.zig` for the detailed reasoning.
- A subquery with its own `GROUP BY`/`HAVING`/`ORDER BY`/`LIMIT` — e.g. `col IN (SELECT
  ... GROUP BY ... HAVING ...)` — was parsed wrong: `findClauseKeyword` located the
  *outer* query's clause boundaries with a scan that wasn't parenthesis-depth-aware, so
  it matched those same keywords *inside* the subquery's own parens. Not reachable before
  subqueries existed (nothing else could put `GROUP BY`/`HAVING` text inside a WHERE
  clause's parens), so it was a latent bug this feature was the first thing to expose.
- `zig build test`'s leak checker caught a **pre-existing, unrelated** leak in
  `executeGroupBy`: a `HAVING COUNT(*) >= N` referencing an aggregate not also in
  `SELECT` allocates `having_extra_aggs`/`comparisons` bookkeeping that was never freed.
  Reproduces on a plain `GROUP BY ... HAVING COUNT(*) >= N` query with no subquery
  involved at all — simply never exercised by an existing test before one of the new
  subquery tests happened to hit that exact shape.

### Performance vs. DuckDB

`col IN (SELECT ...)` is semantically a semi-join — probe a hash table built from the
right side (the subquery), keep only left rows that match, project only left-side
columns — not "a WHERE clause that happens to run a query first." Measured directly
(2M-row orders table, `WHERE customer_id IN (SELECT customer_id FROM customers WHERE
tier = 'gold')`), same result set both engines:

| Subquery result size | csvql | DuckDB |
| --------------------- | ----- | ------ |
| Small (6 matching customers — a typical lookup-table filter) | **0.034s** | 0.17s (csvql ~5x faster) |
| Large (10,000 matching customers) | **0.045s** | 0.17s (csvql ~3.8x faster) |

The large case was originally **3.5s** (DuckDB ~20x faster) — `Comparison.in_values`
membership testing (`compareValues` in `parser.zig`) was a linear scan per row, fine for a
literal `IN ('a','b','c')` list (always short, someone typed it by hand) but a genuine
bottleneck once a subquery could produce a list with thousands of entries. Fixed by
attaching a `std.StringHashMap(void)` (`Comparison.in_values_set`, built once at
subquery-resolution time, alongside `in_values`) so membership is O(1) instead of O(n) —
the same hash-build-then-probe shape csvql's own JOINs already use, not new infrastructure.
Not built for a literal `IN (...)` list, which is never large enough for the scan to
matter. The 78x improvement (3.5s → 0.045s) is the semi-join reframing paying off, not a
one-off tuning trick: any future genuinely-large IN-list — subquery or not — gets it too.

## Known gaps (open issues)

Not yet supported. All of these **error clearly** rather than silently returning wrong
data — see [Error behaviour](#error-behaviour) for why that distinction is the entire
point.

| Gap | Tracking |
| --- | -------- |
| Subqueries other than `col IN (SELECT ...)` / `col NOT IN (SELECT ...)` — correlated subqueries, subqueries in `FROM`/`SELECT`-list/`HAVING`-comparison position | [#124](https://github.com/melihbirim/csvql/issues/124) |
| `UNION` / `INTERSECT` / `EXCEPT` | [#122](https://github.com/melihbirim/csvql/issues/122), [#127](https://github.com/melihbirim/csvql/issues/127) |
| Window functions (`RANK() OVER (...)`, etc.) | [#126](https://github.com/melihbirim/csvql/issues/126) |
| `OFFSET` clause | [#70](https://github.com/melihbirim/csvql/issues/70) |
| Per-release-tag differential fuzz run (bigger volume than nightly, recorded in the release notes) isn't wired up yet — only per-PR (fixed seed, smoke scale) and nightly (random seed, volume) exist | [#141](https://github.com/melihbirim/csvql/issues/141) |
| Coverage measurement (line coverage per module) not wired up | not yet filed |
| No Windows-specific adversarial subset in CI (CRLF is tested on macOS/Linux; Windows' own line-ending and path-separator handling isn't independently verified) | not yet filed |

## What is not supported

Anything not listed under "Supported" in the [README's SQL Reference](README.md#supported)
is not supported. `INSERT`/`UPDATE`/`DELETE` — csvql is read-only by design, it queries
CSV files, it does not mutate them.

## Error behaviour

The entire premise of this document is that a wrong number with no error is worse than a
crash. Every gap listed above raises a clear error (`error.SubqueriesNotSupported`,
`error.UnsupportedSetOperation`, `error.ColumnNotFound`, etc.) instead of returning an
empty, zero, or partial result. **The goal is for this section to have nothing else to
say** — every previously-found silent-wrong-answer bug (unresolved WHERE columns
silently matching zero rows, aggregate scans dropping a row's value, ORDER BY on an
un-projected column) has been converted to a loud failure, and stays covered by a
regression check in `bench/verify_correctness.sh` or a `zig build test` unit test so it
can't silently reappear.

If you find a case where csvql returns a plausible-looking wrong answer instead of
erroring, that is the highest-priority class of bug this project tracks — please
[file an issue](https://github.com/melihbirim/csvql/issues/new).

### `--strict` and exit codes

`--strict` is scoped to *refuse the unknown*, not *refuse the divergent*: it does not
turn the [documented intentional differences](#known-differences-intentional) from
DuckDB into errors — those are deliberate design choices, not risk. What it does refuse:
a `WHERE` numeric comparison (`age > 30`) against a field value that doesn't parse as a
number. Default behavior (unchanged) silently skips that row, on the theory that CSV
columns have no enforced type and a stray non-numeric value in an otherwise-numeric
column is common and usually meant to be filtered out, not fatal. `--strict` is for
callers who'd rather find out their data has a type mismatch than get a silently-partial
result.

This is deliberately narrow — it does **not** become a place for a known-wrong bug to
hide behind an opt-in flag. Anything `--strict` refuses was already merely *risky* under
the default, never *silently wrong*: if a case is ever found where the default returns
a plausible-looking wrong answer, the fix is to make the default error (or return the
right answer), not to gate correctness behind `--strict`.

Exit codes distinguish who's at fault, since the caller of a CLI tool in a pipeline is
usually a script, not a human:

| Code | Meaning |
| ---- | ------- |
| 0 | success |
| 1 | uncategorized failure (IO error, internal error, file not found) |
| 2 | the query is wrong — bad/unsupported SQL, unresolved column. The user's bug. |
| 3 | the data defeated csvql — currently only `--strict`'s non-numeric WHERE value. A data-quality signal, not a query bug; script callers may want to route this differently (e.g. quarantine the file) rather than treating it identically to a typo'd query. |

These codes are part of the CLI contract; changing what maps to which is a breaking change.
