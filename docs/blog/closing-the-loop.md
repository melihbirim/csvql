---
layout: post
title: "Closing the Loop: 18 of 19 Bugs Fixed, One Filed Honestly"
description: "The differential-testing bug hunt that started with one blog comment is done for now. 19 issues filed, 18 fixed and shipped across five point releases, one narrow gap filed rather than hidden."
date: 2026-08-10
---

Back in [the post about the comment that started this](what-one-comment-found.html), I'd fixed 12 of 18 filed bugs. The other 6 were still open, including the one I was least comfortable rushing: the four parallel worker threads used for files over 10MB had their own independent, still-naive CSV parsing, separate from the single-threaded paths that had already been fixed.

That's done now. 19 issues filed total since the original comment, 18 fixed and shipped, 1 filed and left open on purpose.

## What was actually wrong in the parallel paths

The single-threaded fixes taught a pattern: row boundaries and field splitting both need to be quote-aware, or an embedded newline or a stray quote character corrupts the scan. The parallel paths hadn't just missed that pattern, they'd reimplemented CSV parsing five separate times across `engine.zig` and `parallel_mmap.zig`, and none of the five copies had it.

Two distinct bugs stacked on top of each other:

**Row boundaries**, in two places per worker. The per-row scan inside each thread used a plain `\n` search, so an embedded newline in a quoted field split one record into two garbage ones, exactly like the original `SUM()` bug from the first round, just in code that hadn't been touched yet. Worse, the *chunk-splitting* that divides a file across threads used the same naive search, which meant a chunk boundary could itself land inside a quoted multi-line field. Fixing only the per-row scan wouldn't have been enough, a thread could still start reading from the middle of a quoted value it had no idea was open.

**Field splitting**, in one specific worker. `scalarWorkerThread` (the one handling `LENGTH()`, `UPPER()`, and friends on big files) split fields with a completely unaware `std.mem.splitScalar`, no quote handling of any kind. A comma or newline inside quotes just broke the row into the wrong number of fields.

## What "fix it properly" turned out to mean

Once the row-boundary function was quote-aware (`csv.findRecordEnd`, the same one written for the single-threaded fix), it could be reused in both places: the per-row scan loop and the chunk-splitting function. That single function, used twice, closed both the corruption bug and the harder boundary-safety problem at once.

The field-splitting fix meant swapping the naive splitter for the same `simd.parseCSVFieldsStatic` already used elsewhere, plus threading through the doubled-quote unescape treatment (the original #89 bug) that the zero-copy parser still doesn't handle on its own. That unescape needed its own small per-row arena in three different worker functions, since none of them had a scratch allocator that reset on the right cadence, mixing it into an arena that lives for the whole chunk would have quietly leaked memory for the length of the scan.

Verification was a single adversarial fixture, one row with an embedded newline and a doubled quote, padded to 33 MB to force the parallel threshold, run through all four query shapes: a scalar function, a scalar aggregate, a `GROUP BY`, and a `JOIN`. All four matched DuckDB exactly, where before the fix each one failed differently.

## The one I filed instead of fixing

While verifying, I found a fifth spot with the same gap: the `ORDER BY` worker's WHERE-filter and sort-key comparison don't unescape doubled quotes either. Unlike the others, this one doesn't corrupt the *output*, the values shown to the user are re-parsed and correctly unescaped in a separate merge step. It only affects filtering or sort order on a column that both contains an escaped quote and gets compared or sorted on, in a file big enough to hit the parallel path.

That's narrow enough that bundling a fifth fix into an already-large change felt like the wrong tradeoff, more surface area to verify, for a bug with a much smaller blast radius than the four I'd just confirmed. So it's [issue #107](https://github.com/melihbirim/csvql/issues/107) instead of a diff. The same rule from the first post applies here too: a benchmark or a fix that quietly expands scope past what you've verified is how bugs get shipped, not how they get caught.

## The numbers, for the record

19 issues filed since the original comment. 18 closed. 1 open, filed the same day it was found, with a repro and an honest note about why it wasn't bundled in. Five point releases (v2.0.1 through v2.1.0) shipped the fixes as they landed, rather than batching everything into one release at the end.

None of this needed a different kind of testing than the first round, just running the same kind of adversarial fixture against the code paths that hadn't been checked yet. The lesson from the original comment held all the way through: a correctness suite only tells you about the paths it actually exercises.

## Try it

```bash
git clone https://github.com/melihbirim/csvql
cd csvql && zig build -Doptimize=ReleaseFast
./bench/verify_correctness.sh
```

Same suite, still growing, still runs on every push.
