---
layout: post
title: "Five Ways to Find Distinct Values in a 10 GB CSV. One of Them Was Mine, and It Was the Worst."
description: "Python, Node, Rust, DuckDB and csvql on the same 11 GB file. csvql took more than 6 minutes and 4 GB of memory to do what DuckDB did in 5.7 seconds. The benchmark found a real bug, and fixing it took the query to 4.8 seconds."
date: 2026-09-23
---

*Counting distinct values in an 11 GB CSV, five ways. The engine I wrote came last by a factor of sixty. The benchmark was right, and the bug it found was real.*

---

## The question

"How many distinct users are in this file?" is the most boring question you can ask of a CSV, and it is the one that breaks things. The file does not fit in memory. The naive answer builds a set of every distinct value, and if the cardinality is high, that set is the problem.

So: 75 million rows, 14 columns, 11.15 GB on disk. One column, `user_id`, holds exactly 5,000,000 distinct values. Count them.

```
id,user_id,session_id,event,country,city,device,browser,referrer,path,ts,amount,currency,status
000000000,u000000000,s000000000000,click,US,Chicago,mobile,chrome,...
```

**Machine:** Apple M2 Pro, 12 cores, 16 GB RAM. The file is comfortably larger than half of RAM, which is the point. **DuckDB:** v1.4.2. Every tool reads the same raw CSV from the same disk. Three runs each.

---

## The five ways

**Python, 8 lines.** The standard library, the way most people write it first.

```python
import csv, sys

seen = set()
with open(sys.argv[1], newline="") as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        seen.add(row[1])
print(len(seen))
```

**Node, 15 lines.** Streaming, because `readFileSync` on 11 GB is not a benchmark, it is a crash.

```js
const rl = readline.createInterface({
  input: fs.createReadStream(process.argv[2], { highWaterMark: 1 << 20 }),
  crlfDelay: Infinity,
});
const seen = new Set();
rl.on("line", (line) => {
  const a = line.indexOf(",");
  const b = line.indexOf(",", a + 1);
  seen.add(line.slice(a + 1, b));
});
rl.on("close", () => console.log(seen.size));
```

**Rust, 21 lines.** No dependencies, `std::collections::HashSet`, reusing one line buffer.

```rust
let mut seen: HashSet<Box<str>> = HashSet::new();
let mut line = String::with_capacity(256);
reader.read_line(&mut line).unwrap(); // header
loop {
    line.clear();
    if reader.read_line(&mut line).unwrap() == 0 { break; }
    let user_id = line.split(',').nth(1).unwrap();
    if !seen.contains(user_id) { seen.insert(user_id.into()); }
}
```

**DuckDB, one query.**

```sql
SELECT count(DISTINCT user_id) FROM read_csv('events.csv', header=true);
```

**csvql, one query.**

```bash
csvql "SELECT COUNT(DISTINCT user_id) FROM 'events.csv'"
```

---

## The first result

All five agreed on the answer: 5,000,000. They did not agree on anything else.

| Method | Time | Peak RAM | Code |
|---|---|---|---|
| Python `set` | 124 s | 445 MB | 8 lines |
| Node `Set` | 39 s | 539 MB | 15 lines |
| Rust `HashSet` | 19 s | 283 MB | 21 lines |
| DuckDB | 5.7 s | 2639 MB | 1 query |
| csvql | **killed at 6 min** | **~4 GB** | 1 query |

I wrote csvql. It lost to Python by a factor of three, and it did not finish.

This is the part where it would be easy to quietly change the benchmark. Pick a lower cardinality, report the query it wins, move on. I want to argue the opposite: this table is the most useful thing that happened all week, because a benchmark that only ever confirms what you hoped is not a benchmark, it is decoration.

---

## Reading the failure correctly

Two things were wrong, and one of them was in my measurement rather than my code.

`/usr/bin/time -l` reported csvql's peak resident memory as **52 MB**. At the same moment, `top` reported **4058 MB**, and the machine had pushed 29.9 GB into swap. macOS had compressed the pages out from under RSS. If I had trusted max RSS, as most benchmark scripts do, I would have concluded csvql was frugal and slow, when it was actually ravenous and slow. On macOS, sample the footprint, not the resident size.

With that cleared up, `sample` pointed at the real problem immediately. Of 2415 stack samples, **2394 were inside a single hash map insertion loop**, on one thread, after all twelve worker threads had finished.

The design was this: each of the 12 workers built its own private `StringHashMap` of the distinct values it saw. At the end, the main thread merged all twelve into one.

That is a perfectly reasonable way to parallelize an aggregate, and it is free when the number of distinct values is small. `GROUP BY country` has 8 groups, so each worker holds 8 entries and the merge is 96 operations that nobody will ever notice.

At 5 million distinct values it is a catastrophe. Every worker sees essentially every key, so all twelve build the *entire* 5 million entry set: about 4 GB held at once, to produce a single number. Then one thread merges 60 million entries while the other eleven cores sit idle and the machine swaps.

The giveaway was that **more threads made it monotonically worse**:

| Threads | Result |
|---|---|
| 1 | 32 s, 1.71 GB |
| 4 | killed at 2:50 |
| 12 | killed at 6:00, 4 GB, 30 GB swap |

Single threaded was the fastest configuration. Whenever adding cores makes a workload slower, the parallelism is not the optimization, it is the bug.

---

## The fix

Stop replicating. Workers share one set, sharded 64 ways by key hash. A key only ever reaches the shard that owns it, so shards never coordinate with each other, each key is stored exactly once, and there is no merge phase at all.

The whole thing is about 40 lines. The table it produces:

| Method | Time | Peak RAM | Code |
|---|---|---|---|
| Python `set` | 124 s | 445 MB | 8 lines |
| Node `Set` | 39 s | 539 MB | 15 lines |
| Rust `HashSet` | 19 s | 283 MB | 21 lines |
| DuckDB | 5.7 s | 2639 MB | 1 query |
| csvql | **4.8 s** | **235 MB** | 1 query |

From over six minutes to 4.8 seconds, and from 4 GB to 235 MB. The interesting column is not the time, where csvql and DuckDB are close enough that machine noise matters. It is the memory: **235 MB against 2639 MB**, for identical work and an identical answer.

---

## The same bug, three more times

Once you can name a bug, you can go looking for it. The pattern was "replicate per worker, merge at the end", and it was not only in `COUNT(DISTINCT)`.

`SELECT DISTINCT user_id` is rewritten internally into a `GROUP BY`, and that path built a full accumulator for every group even though the query has no aggregates to accumulate: nine empty slice headers and two allocations per group, measured at 441 bytes per group for keys about 10 bytes long. With no aggregates, the key *is* the result. It now shares the same sharded set.

`GROUP BY user_id` at 5 million groups had the replication problem in its purest form: twelve workers each constructing all five million accumulators, 60 million constructions to produce five million results. Workers now start local, which keeps low cardinality lock free, and switch to a shared sharded map once their own map passes 50,000 groups.

| Query | Before | After | DuckDB |
|---|---|---|---|
| `COUNT(DISTINCT user_id)` | >360 s | 4.8 s | 5.7 s |
| `SELECT DISTINCT user_id` | >130 s | 9.0 s | 7.0 s |
| `GROUP BY user_id` (5M groups) | >150 s | 16.2 s | 8.3 s |
| `GROUP BY country` (8 groups) | 1.76 s | 1.76 s | 4.84 s |
| `COUNT(*) WHERE country='JP'` | 1.95 s | 1.95 s | 4.34 s |

DuckDB is still ahead on `SELECT DISTINCT` and roughly 2x ahead on high cardinality `GROUP BY`, at about twice the memory. I would rather print that than not.

One wrong turn is worth recording, because it cost a working afternoon. I assumed the serial merge was the bottleneck for `GROUP BY` and built a parallel merge. It changed nothing: still over 150 seconds, before and after. Sampling showed the cost was in the scan, not the merge. The lesson is the one I keep relearning: the profiler is cheap and my intuition is not.

---

## What to actually use

If you are here because you have a large CSV and a distinct-values problem:

**Python's `set` works.** 124 seconds is slow but it is not broken, and the memory is fine because a 5 million entry Python set is only ~445 MB. If this is a thing you do once, write the eight lines and go make coffee. The failure mode to watch is cardinality: at 50 million distinct values, that set is the thing that kills you, not the file size.

**Node is 3x faster than Python** for the same shape of code, and holds slightly more memory.

**Rust with no dependencies gets you to 19 seconds** in 21 lines. It is the best result you will get without a real engine, and `std`'s SipHash is leaving speed on the table if you care to swap it.

**DuckDB is the right default.** It is one query, it is fast, it is correct, and it is a single binary. If you do not already have an opinion about this, use DuckDB. The cost is memory: 2.6 GB to count distinct values in a file, which matters on a small VM and not at all on a laptop.

**csvql** is the same one query at a tenth of the memory, and it queries the CSV in place with no import step. That is the tradeoff it exists for.

Anyone can reproduce this. The fixture is deterministic, and the four competing implementations are printed above in full.

---

## The part worth keeping

The bug had been in csvql for a long time, behind a benchmark suite that was green the whole time, because every test and every benchmark used low cardinality data. Eight groups, twelve groups, a few hundred. The replicate-and-merge design is invisible until the key space gets large, and nothing in the repository ever made it large.

That is what the competitor was for. I did not find this by reading my own code, and I did not find it by running my own benchmarks. I found it by putting csvql next to four other tools on a problem chosen because it is hard, and being willing to publish the result before I knew what it would say.

The fixes are in [#180](https://github.com/melihbirim/csvql/pull/180), [#182](https://github.com/melihbirim/csvql/pull/182), [#183](https://github.com/melihbirim/csvql/pull/183) and [#184](https://github.com/melihbirim/csvql/pull/184). The two things still open are in [#185](https://github.com/melihbirim/csvql/issues/185) and [#186](https://github.com/melihbirim/csvql/issues/186), including one I do not have an answer for yet.
