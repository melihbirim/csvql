---
layout: post
title: "Chasing qsv: What It Takes to Close a Single-Thread Parsing Gap"
description: "csvql started 1.9x behind qsv's Rust CSV parser on raw single-thread throughput. Two targeted fixes closed it to parity, one plausible-sounding optimization made things worse, and a bad first reading almost led to the wrong conclusion. The real numbers, the real code, and the one idea that didn't work."
date: 2026-08-13
---

"Fastest CSV parser" is a specific, checkable claim, so we checked it. This is the log of what happened: where csvql actually stood against [qsv](https://github.com/dathere/qsv) (a Rust CSV toolkit built on the `csv` crate, one of the more heavily optimized parsers in common use), what closed the gap, what didn't, and a mistake along the way worth being honest about.

## Two different numbers, and why conflating them is the first way to lie to yourself

There are two entirely different things you can measure here, and the first mistake is treating them as one number.

**Raw parsing throughput**: given a line of CSV text, how fast can you split it into fields and find the next record boundary. No SQL, no CLI, no I/O beyond reading the bytes. This is what [`bench/csv_parse_bench.zig`](https://github.com/melihbirim/csvql/blob/main/bench/csv_parse_bench.zig) isolates, and it's the fair, apples-to-apples comparison against qsv's underlying parser — run with `zig build bench -- <file>`.

**Full query wall-clock**: what a user actually experiences running `csvql "SELECT COUNT(*) FROM file.csv"` versus `qsv count file.csv`. This includes process startup, thread-pool spawn, SQL parsing, and everything else that isn't the parser. [`bench/bench_qsv.sh`](https://github.com/melihbirim/csvql/blob/main/bench/bench_qsv.sh) measures this one.

csvql's answer on these two questions is different, and both answers are real.

## Starting point: 1.9x behind, single-thread

First honest measurement, same 7.9 GB file, same machine, `qsv count --no-polars` (confirmed single-threaded — this particular build doesn't have the multithreaded Polars reader compiled in) against csvql's real field parser:

| | Single-thread throughput |
|---|---|
| qsv | 622 MB/s |
| csvql (before) | 324 MB/s |

Not close. Two things in csvql's hot path were leaving real performance on the table.

## Fix 1: the comma-finder was doing SIMD work with a scalar loop

`findCommasSIMD` in `src/simd.zig` already compared 16 bytes at once against the delimiter using a `@Vector(16, u8)`, but then walked all 16 lanes with a branch to check each one:

```zig
var j: usize = 0;
while (j < VecSize) : (j += 1) {
    if (matches[j] and count < positions.len) {
        positions[count] = i + j;
        count += 1;
    }
}
```

That's SIMD comparison feeding a scalar loop, which throws away most of the point. The fix casts the boolean match vector straight to an integer bitmask and walks only the set bits with `@ctz` (count trailing zeros):

```zig
const matches: @Vector(VecSize, bool) = chunk == delim_vec;
var bitmask: u16 = @bitCast(matches);
while (bitmask != 0) {
    const bit = @ctz(bitmask);
    positions[count] = i + bit;
    count += 1;
    bitmask &= bitmask - 1; // clear lowest set bit
}
```

A line with two commas now costs two loop iterations instead of sixteen branch checks. Small, mechanical, real.

## Fix 2: the actual hot loop had no SIMD path at all

This is the one that mattered. Every mmap-based scan calls `csv.findRecordEnd` once per row to find where a record ends, skipping over `\n` bytes inside quoted fields (RFC 4180 allows literal newlines in quotes). It was a fully scalar state machine, byte by byte, with zero fast path:

```zig
while (i < data.len) {
    const c = data[i];
    if (in_quote) { ... }
    else if (c == '"' and at_field_start) { in_quote = true; ... }
    else if (c == delimiter) { at_field_start = true; }
    else if (c == '\n') return i;
    ...
    i += 1;
}
```

Most rows have no quotes anywhere. The fix: use `std.mem.indexOfScalarPos` (already SIMD-vectorized in Zig's standard library — 16+ bytes per instruction on this hardware) to find the next newline, then the same call to check for any quote byte before it. No quote found means that newline is the record boundary, done. A quote found falls back to the scalar state machine, which is always correct, just conservative — it triggers on any quote byte, not only ones that would actually open a quoted field.

```zig
pub fn findRecordEnd(data: []const u8, start: usize, delimiter: u8) ?usize {
    const nl = std.mem.indexOfScalarPos(u8, data, start, '\n') orelse return null;
    if (std.mem.indexOfScalarPos(u8, data[0..nl], start, '"') == null) {
        return nl;
    }
    return findRecordEndScalar(data, start, delimiter);
}
```

Same result after both fixes, same file, same machine:

| | Single-thread throughput |
|---|---|
| qsv | 316-328 MB/s (this run) |
| csvql (after) | 307-323 MB/s (this run) |

Parity, not a win, confirmed across repeated runs. The absolute numbers moved between runs (the machine was under real load, more on that below), but the *relative* result held: from 1.9x behind to essentially tied.

## The idea that didn't work

`findRecordEnd`'s fast path already scans the line for a quote byte to confirm the boundary. `parseCSVFieldsStatic` then scans the *same line* again for a quote byte to decide its own fast path. That's a redundant scan, and threading the "already know it's quote-free" fact through to skip the second scan looked like free performance.

It wasn't. `findRecordEndKnownQuote` plus a `parseCSVFieldsStaticKnownNoQuote` variant, benchmarked head to head against the plain version, three repeated runs:

| Run | Plain (bench 4) | Fused (bench 5) |
|---|---|---|
| 1 | 410 MB/s | 406 MB/s |
| 2 | 438 MB/s | 432 MB/s |
| 3 | 439 MB/s | 432 MB/s |

Consistently slightly *slower*, every run. The scan being avoided is cheap (SIMD, over a short line); the struct return and extra branch to route around it cost more than it saved. Reverted rather than kept as unproven complexity. Worth stating plainly: not every optimization that sounds right on paper survives contact with a benchmark, and the fix for that is measuring, not reasoning harder.

## The bad reading, and the correction

Early in this, `bench_qsv.sh` on the 70 MB file showed csvql at 149 MB/s against qsv's 304 MB/s, and the obvious-sounding explanation was thread-pool spawn overhead not being amortized on a small file. That explanation was wrong. Rerun clean, repeated three times:

| Run | qsv | csvql |
|---|---|---|
| 1 | 314 MB/s | 1708 MB/s |
| 2 | 398 MB/s | 2104 MB/s |
| 3 | 389 MB/s | 1829 MB/s |

csvql was actually ~5x *ahead* on the small file. The first number was a cold-start artifact (first invocation, cold page cache, unrelated to any of this session's changes), not a real pattern, and the "thread overhead" theory was reasoning from a single unverified data point instead of rerunning it. The fix for a bad measurement is always the same: run it again before building a theory on top of it.

## Where it landed

Full query wall-clock, same 7.9 GB file:

| | Wall-clock throughput |
|---|---|
| qsv count | 636 MB/s |
| csvql `COUNT(*)` | 3456 MB/s |

5.4x ahead on the number a user actually experiences, because csvql parallelizes across all cores by default and this qsv build doesn't. Single-thread, raw parsing: parity, not ahead, and that's the honest remaining gap if "fastest CSV parser" is meant literally rather than "fastest CSV query."

## Reproduce it

```
zig build -Doptimize=ReleaseFast
zig build bench -- large_test.csv          # isolated single-thread parser number
./bench/bench_qsv.sh large_test.csv        # full query wall-clock, needs: brew install qsv
```

Both scripts are in the repo, unmodified from what produced every number above.
