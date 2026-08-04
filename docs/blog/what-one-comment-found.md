---
layout: post
title: "One Comment on a Benchmark Post Turned Into 18 Filed Bugs"
description: "A reader suggested adversarial CSV testing against DuckDB instead of trusting the happy path. It found 18 real bugs in csvql, 12 already fixed and shipped."
date: 2026-08-04
---

I published a [benchmark post](csvql-vs-duckdb-nyc-taxi.html) comparing csvql against DuckDB on the NYC taxi dataset. Someone left this comment:

> The most valuable result here may be the two correctness bugs, because differential testing gives you an oracle while the implementation is still small enough to reason about. I'd keep DuckDB in CI as a semantic reference and generate adversarial CSV fixtures: quoted newlines, escaped delimiters, empty versus NULL, Unicode, exponent notation, NaN/Infinity, timestamp offsets, and group cardinalities large enough to force spills. Compare normalized result sets and error classes, not just elapsed time.

I checked what csvql's existing correctness suite actually covered. It diffed csvql against DuckDB, but only on business-style data: names, cities, departments, salaries. No quotes, no embedded newlines, no unicode, nothing adversarial. Every check passed because nothing in the fixtures could fail.

## What five test rows found

I built a small CSV with a quoted field containing an embedded newline, a doubled quote, and a unicode name, and ran it through csvql instead of the usual clean data. Two bugs showed up before I'd even finished writing the fixture.

`LENGTH()` on a field like `"has ""quotes"" inside"` came back wrong. The zero-copy parser found the correct closing quote but never collapsed the doubled `""` down to a single `"`, so the stored value was longer than it should have been.

`SUM()` was worse. A row with an embedded newline in a quoted field caused the aggregate to silently drop that row's value from the total. No error, no crash, just a wrong number. Plain projection of the same column returned the correct value, so the bug was specific to how the aggregation scan split rows, not how it read them.

Both went into a CI job that runs DuckDB as an oracle on every push, so they can't come back unnoticed.

## The second pass found more, and worse

Fixing those two led to a closer look at how CSV quoting was handled everywhere else in the codebase. The same class of bug showed up repeatedly, because quote-handling logic had been implemented separately in several places instead of shared:

- The mmap and bulk scanners toggled quote state on *any* quote character, not just one at the start of a field, so a value like `abc"def` inside an unquoted field could throw off everything after it.
- Header parsing didn't respect quotes at all when splitting column names.
- `IN (...)` list parsing split on raw commas, breaking on any value like `'New York, NY'`.
- A fixed 8KB stack buffer for DISTINCT deduplication silently truncated longer rows and could falsely collapse two different rows into one.
- `parseIntFast` had no overflow check, so a 20+ digit number could silently corrupt under a release build instead of failing.
- An ORDER BY sort key was recorded as a live slice into a growable arena buffer, a genuine use-after-free once the arena reallocated mid-scan.
- A duplicate header column name (two columns both called `id`) silently returned whichever one happened to be inserted into the lookup table last, not the first, with no indication anything was ambiguous.
- A UTF-8 BOM, which Excel writes by default when you export "CSV UTF-8", glued onto the first column name and broke every reference to that column.

None of these needed a large file or a strange query. All of them needed data slightly outside what the original test suite had ever generated.

## Where it stands

18 issues filed since that comment, 12 already fixed and shipped across two point releases. 6 are still open: a documentation-only issue about intentional differences from DuckDB (byte length vs character count, that kind of thing), a known gap where the four parallel worker threads used for files over 10MB still don't have the same quote-aware parsing as the single-threaded paths, and a handful of smaller ones (`SUM`/`AVG` returning 0 instead of NULL on an empty result set, a bare `CASE WHEN` outside an aggregate failing with a misleading error, a negative `LIMIT` silently being treated as unlimited).

The parallel-path gap is the one I'm least comfortable rushing. Fixing it means touching four independent threaded scan functions, each with its own I/O buffer boundary handling, and getting that wrong under load is worse than leaving it as a known, documented limitation for now.

## The actual lesson

None of this was exotic. It was one reader pointing out that a correctness suite built entirely from clean, well-formed business data will pass no matter how many real bugs are sitting in the parser, because it never exercises the code paths those bugs live in. The fix wasn't a smarter test, it was a meaner one.

If you want to see the current adversarial fixtures or run them yourself:

```bash
git clone https://github.com/melihbirim/csvql
cd csvql && zig build -Doptimize=ReleaseFast
./bench/verify_correctness.sh
```

It runs on every push now. If you find a fixture it should have but doesn't, that's exactly the kind of comment that started this.
