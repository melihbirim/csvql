# Correctness

csvql treats [DuckDB](https://duckdb.org) as the reference implementation and diffs its own
output against DuckDB's for every query shape it supports. This page documents what is
tested, how, and — just as important — what isn't tested yet or isn't supported at all.

Run it yourself: `zig build verify -Doptimize=ReleaseFast` (or directly:
`./bench/verify_correctness.sh`). Needs a `duckdb` binary in `PATH` (or set
`DUCKDB_BIN=/path/to/duckdb`). Takes well under a minute.

## What is tested

- **88 differential checks** in `bench/verify_correctness.sh` (89 when the optional
  multi-GB taxi fixture is present locally — see below), covering: SELECT/projection,
  every WHERE operator (`=`, comparisons, `LIKE`/`ILIKE`, `BETWEEN`, `IN`/`NOT IN`,
  `IS NULL`, modulo, compound `AND`/`OR`/`NOT`), GROUP BY + all aggregate functions
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
- **529 unit tests** (`zig build test`) covering internals differential testing can't
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

## Oracle methodology

- **Reference**: DuckDB CLI, `duckdb --version` printed at the top of every suite run
  (currently `v1.4.2`). Pinned to whatever the CI runner's `duckdb --version` reports —
  not currently pinned to an exact release, see [Known gaps](#known-gaps-open-issues).
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

## Known gaps (open issues)

Not yet supported. All of these **error clearly** rather than silently returning wrong
data — see [Error behaviour](#error-behaviour) for why that distinction is the entire
point.

| Gap | Tracking |
| --- | -------- |
| Subqueries (`WHERE col IN (SELECT ...)`, `HAVING x > (SELECT ...)`) | [#124](https://github.com/melihbirim/csvql/issues/124) |
| `UNION` / `INTERSECT` / `EXCEPT` | [#122](https://github.com/melihbirim/csvql/issues/122), [#127](https://github.com/melihbirim/csvql/issues/127) |
| Window functions (`RANK() OVER (...)`, etc.) | [#126](https://github.com/melihbirim/csvql/issues/126) |
| `OFFSET` clause | [#70](https://github.com/melihbirim/csvql/issues/70) |
| DuckDB CLI version not pinned in CI (`--version` is whatever `latest` resolves to at run time) | not yet filed |
| No grammar-based differential fuzzer — the 88 checks are hand-written, so they only find bugs already imagined, not the full space of query combinations. This is how #140 below was found: adding one more hand-written adversarial fixture (mixed date formats), not a fuzzer | not yet filed |
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
