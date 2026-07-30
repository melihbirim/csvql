---
layout: post
title: "Why csvql joins beat DuckDB by 50x while aggregates only beat it by 8x"
description: "Two query types, two different bottlenecks. What csvql's parallel join probe actually changed, and why it doesn't move the needle on a plain COUNT(*)."
date: 2026-07-30
---

I got asked a fair question this week. If csvql is fast enough to beat DuckDB by 50x on joins, why is it only 8x faster on a plain `COUNT(*)`? Shouldn't the gap be consistent?

The short answer: the two query types are bottlenecked by completely different things.

## Aggregates hit a floor on both sides

Run `SELECT COUNT(*)` on an 81MB, 2 million row CSV and csvql finishes in about 0.02 seconds. DuckDB takes about 0.15 seconds. That looks like a big win on paper (7.6x) but the absolute numbers tell the real story.

DuckDB's 0.15 seconds is mostly fixed cost. Planner startup, catalog setup, execution engine spin up. The actual work of counting rows is nearly free once that machinery is running. csvql doesn't carry that machinery, so its 0.02 seconds is close to the pure cost of reading 81MB off disk and counting rows. csvql on large files sits near the cat/read ceiling. There isn't much room left to shave.

So when both sides are close to their respective floors, the ratio between them stays flat no matter what you do to either engine. You can't out-optimize physics.

## Joins are a different problem

A hash join over 2 million rows is real computational work. You build a hash table from one side, then probe it row by row from the other side. That cost scales with data size, not with a fixed planner overhead. This is where an engine's actual algorithm matters, not its startup tax.

DuckDB's join times climb fast under this load. A plain lookup join takes 2.6 seconds. `SELECT *` across both tables takes 7.6 seconds. A 4 table join takes almost 4 seconds. This is real work, done mostly single threaded relative to what's available.

csvql recently shipped a parallelized join probe, spreading the probe phase across every core instead of one. The same lookup join that took DuckDB 2.6 seconds takes csvql 0.048 seconds. `SELECT *` across both tables drops from 7.6 seconds to 0.1 seconds. A 4 table join goes from 3.9 seconds to 0.07 seconds.

That is not a fixed cost disappearing. That is real, scalable work being split across cores and finishing in a fraction of the time.

## The pattern

Speedup ratio tracks how much real work a query does per row, not how fast an engine can theoretically go.

Aggregates: cheap per row work, dominated by fixed overhead on the DuckDB side and by pure IO on the csvql side. Ratio settles around 7 to 9x and won't move much further.

Joins: expensive per row work, no fixed overhead dominating either side, embarrassingly parallel probe phase. Ratio lands between 30x and 70x depending on shape, and climbs the more tables and columns you throw at it.

If csvql ever adds heavier per row computation somewhere else in the pipeline, expect that gap to open up the same way. The lesson isn't that csvql is inconsistent. It's that benchmark ratios only mean something once you know what each side is actually bottlenecked on.

## Check it yourself

If you want to check these results yourself, clone the repo and run `bench_all.sh` on your own machine and data:

[github.com/melihbirim/csvql](https://github.com/melihbirim/csvql)

If csvql is useful to you, a star on the repo helps others find it. Sharing the project with someone who deals with large CSVs helps even more.
