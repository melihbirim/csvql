---
layout: post
title: "Auditing Our Own Claims Found a Real Bug"
description: "Checking a 4-month-old blog post's memory claim against real measurement turned up two things: the claim was wrong, and the honest re-measurement exposed a live silent-data-corruption bug in eight places across the query engine."
date: 2026-08-17
---

An old post about csvql said memory usage "stays under 2MB regardless of file size" and the npm package description said "~5 MB." Both specific, both checkable. So we checked them, the same way every other claim on this project gets checked: measure, don't reason from memory of what the code used to do.

## The 2MB number was real, just about the wrong thing

`IO_BUF: usize = 2 * 1024 * 1024` is a real constant, still in the code today, in four different worker functions in `src/engine.zig` and `src/parallel_mmap.zig`. It's the size of each thread's streaming read buffer. Four months ago, on a single-threaded build, that buffer *was* close to the whole memory story, so "under 2MB" was an honest description at the time.

It stopped being an honest description of *total* memory the moment parallel execution shipped. `/usr/bin/time -l` (macOS, reports true peak resident set size for the whole process, not a point-in-time snapshot) on a 70MB file:

```
=== single-thread, 70MB file ===
77938688 bytes  maximum resident set size   (~74 MB)
=== parallel, 7.9GB file ===
55623680 bytes  maximum resident set size   (~53 MB)
```

Tens of MB, not under 2. The npm package's "~5 MB" claim was worse, and the first attempt to check it made the same mistake in a new way.

## A measurement bug while measuring a bug

First pass used `process.memoryUsage()` called right after `csvql.query()` returned from Node.js:

```js
const rows = csvql.query(`SELECT COUNT(*) FROM '${file}' WHERE salary > 100000`);
const mem = process.memoryUsage();
console.log('rss MB:', (mem.rss / 1024 / 1024).toFixed(1));
```

That gave a csvql-attributable delta of about 5.5MB over a bare-Node baseline — deceptively close to the claim being checked. It's also wrong methodology: it's a snapshot taken *after* the native call returns, and the parallel worker threads' buffers and arenas can already be freed by then. It measures what's left over, not the peak.

Same query, same file, measured properly with `/usr/bin/time -l node script.js` — true peak RSS for the whole process lifetime, the same approach already used for the CLI's own numbers:

```
maximum resident set size: 96174080   (~96 MB total, ~56 MB over baseline)
```

Not ~5MB. Essentially the same number as the CLI, because the Node addon spawns real OS threads via Zig's `std.Thread` directly — Node's single-threaded event loop has nothing to do with it. The docs (`nodejs/README.md`, `nodejs/index.js`, `README.md`, `nodejs/bench.js`, `nodejs/compare.js`) all said "~5 MB regardless of file size." All six now say what's actually true: RAM stays flat — tens of MB — regardless of file size, because it scales with thread count (roughly `cores × few MB`), not with the file.

## The bug the wrong test query accidentally found

The corrected memory test was run a second time against the real 7.9GB taxi dataset, reusing the same query text from the 70MB-file test without checking that the column existed:

```
SELECT COUNT(*) FROM 'trips.csv' WHERE salary > 100000
```

`trips.csv` has no `salary` column. Correct behavior: error. Actual behavior:

```
COUNT(*)
0
```

Silent, wrong, and confident about it. This project has hit this exact bug class before — issue #138, fixed a few releases ago for `executeSequential`'s inline WHERE evaluation — with the reasoning already written down at the time it was fixed:

> Erroring is the correct behavior here — silently treating this as "zero rows match" would be indistinguishable from a genuine negative result to the caller.

#138 fixed one function. It turned out the same "resolve WHERE column index, leave it `null` if not found, silently skip every row" pattern had been copy-pasted into the WHERE-column setup of every other scan path in the engine, none of which #138 touched. The first three found and fixed, shipped as v2.4.1:

- `executeSequential`'s DISTINCT/dedup path (a second, separate setup site from the one #138 fixed)
- `executeScalarAgg` (parallel `COUNT`/`SUM`/`AVG` etc.)
- `executeGroupBy` (parallel `GROUP BY`)

Once the pattern was named, checking for it everywhere else found five more, not yet released:

- `executeSequential`'s own earlier setup site (the plain-SELECT/scalar-transform path)
- `executeFromStdin`
- the parallel scalar-transform dispatcher (`UPPER()`, `ABS()`, and friends on big files)
- `parallel_mmap.zig`'s JOIN/scalar worker setup
- `mmap_engine.zig`'s medium-file path

Eight setup sites total, same bug, same fix each time — after resolving the column index by linear scan over the header, check whether it actually resolved:

```zig
var where_col_idx: ?usize = null;
if (query.where_expr) |expr| {
    if (expr == .comparison) {
        for (lower_header, 0..) |lh, i| {
            if (std.mem.eql(u8, lh, expr.comparison.column)) {
                where_col_idx = i;
                break;
            }
        }
        // Unresolved column: error, don't silently treat as zero matches.
        if (where_col_idx == null) return columnLookupError(expr.comparison.column);
    }
}
```

Confirmed against the taxi file, all three previously-silent paths now refuse to guess:

```
$ csvql "SELECT COUNT(*) FROM 'trips.csv' WHERE salary > 100000"
execution error: error.ColumnNotFound

$ csvql "SELECT vendor_id, COUNT(*) FROM 'trips.csv' WHERE salary > 100000 GROUP BY vendor_id"
execution error: error.ColumnNotFound

$ csvql "SELECT DISTINCT vendor_id FROM 'trips.csv' WHERE salary > 100000"
execution error: error.ColumnNotFound
```

69/69 correctness checks against DuckDB still pass throughout. The first three fixes shipped as v2.4.1; the rest were verified the same way but hadn't gone out as a release at the time of writing.

## While in there: three more redundant scans, closed for free

Fixing the WHERE setup meant reading through every hot scan loop in the engine, and a pattern from an earlier optimization ([the fused single-pass scan from issue #139](what-one-comment-found.html)) turned up only half-applied. `scanRecordFused` finds the record boundary, every delimiter position, and whether a quote is present, all in one SIMD sweep. It was wired into `scalarAggWorkerScan` (plain `COUNT`/`SUM`) months ago, but `GROUP BY` and plain filtered `SELECT` were still doing it the old way: one scan to find the record end (`findRecordEnd`), a second scan just to check "does this line contain a quote" (`std.mem.indexOfScalar`), a third scan to find delimiter positions (`findCommasSIMD`). Three passes over identical bytes where one already existed elsewhere in the same file.

Wired the same fused scan into `gbWorkerScan` (`GROUP BY`'s parallel worker) and `scalarProcessChunk` (parallel filtered `SELECT`), falling back to the original quote-aware path only when a quote is actually present — same conservative contract as everywhere else this technique is used. Before/after, same 7.9GB file, same machine, `/usr/bin/time` (Apple M2 Pro, 12 cores):

**GROUP BY** — `SELECT vendor_id, COUNT(*), AVG(fare_amount) FROM trips.csv GROUP BY vendor_id`

| | user (CPU-seconds) | wall |
|---|---|---|
| before | 19.6s | 2.3-3.1s |
| after | 6.8s | 2.2-2.65s |

**Filtered SELECT** — `SELECT vendor_id, UPPER(cab_type) FROM trips.csv WHERE trip_distance > 5`

| | user (CPU-seconds) | wall |
|---|---|---|
| before | 20.1s | 2.3-3.1s |
| after | 7.1s | 1.8-1.9s |

Roughly 2.8-2.9x fewer CPU-seconds both times. Wall clock barely moved on the GROUP BY case — at 12 threads against a 7.9GB file this particular run is disk-I/O-bound, a finding from earlier benchmarking on this same file that held again here. The CPU-time win is real regardless; it shows up as wall-clock improvement on smaller files, cached reads, or machines with fewer cores, and it's less kernel scheduling contention either way.

The JOIN/parallel-scalar worker in `parallel_mmap.zig` got the same fix, smaller payoff (~8s → ~6s CPU-seconds, that path already avoided some of the duplicate work) but still a real, measured improvement, not a neutral change. `mmap_engine.zig`'s medium-file path was already fully fused from the original #139 work — nothing to do there beyond the WHERE-column fix.

69/69 correctness checks still pass with all three changes in.

## The pattern, again

Every fix in this post started from the same instinct: don't trust a number or a behavior because it used to be true, or because the code that produces it looks obviously correct. Measure the memory claim honestly instead of taking the flattering snapshot. Notice the query that returned `0` is suspicious instead of moving on. Check whether a fix that closed one function actually closed the bug class, or just the one instance someone happened to be looking at. None of these findings were found by reasoning about the code from a distance — they came from running it, checking the output against what should be true, and following the thread when something didn't add up.
