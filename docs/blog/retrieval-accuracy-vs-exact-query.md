---
layout: post
title: "For Structured Data, Retrieval Accuracy Isn't a Model Problem"
description: "The AI industry is spending real money on reranking and hybrid search to push retrieval accuracy up a few points. For a table, exact SQL already gets you 100%, for free."
date: 2026-08-13
---

MongoDB shipped three retrieval features this week: context-aware embeddings, hybrid search, and a native reranker. The pitch is the same one every vector database is making right now: retrieval accuracy is the thing that determines whether an agent's next step is right or wrong, and getting it wrong is expensive twice over, once for the bad action and again for the retry.

That framing is correct. It's also a framing built entirely around unstructured data, and it quietly assumes retrieval has to be approximate.

## Why retrieval is probabilistic at all

Vector search returns the *k* passages whose embeddings are closest to the query's embedding. "Closest" is a distance in a learned space, not a guarantee of relevance. Two chunks can sit near each other in that space and still not answer the question, which is exactly why reranking exists as a second pass: a bigger model re-scores the same candidates because the first pass wasn't reliable enough on its own. Hybrid search exists because vector similarity alone misses exact terms like a product SKU or an error code. Context-aware chunking exists because splitting a document into pieces throws away the surrounding meaning each piece depended on.

All three of those are real, useful fixes. They're also all patches on the same underlying fact: when your data is unstructured text, there is no query that deterministically identifies "the right answer." You can only get closer to it.

## A table doesn't have that problem

`SELECT dept, AVG(salary) FROM employees WHERE region = 'West' GROUP BY dept` doesn't return the four passages that are probably relevant. It returns the rows that satisfy the predicate, with 100% recall and 100% precision, every time, because the question and the data share the same structure. There's no embedding to be almost-right in the middle of. There's no reranking step, because there's nothing to rerank, the WHERE clause already decided.

This is why csvql's benchmarking has always focused on two things: does the query return the exact same result set DuckDB does (we test that continuously against real bugs, not just happy-path queries — see [the last post](what-one-comment-found.html)), and how many tokens does an agent spend getting that result (see [the token economics post](token-economics-of-querying-a-csv.html)). Accuracy was never the axis we had to optimize, because for tabular data, exact retrieval isn't a hard problem someone solved with a bigger model. It's what a WHERE clause has always done.

## Where the two worlds actually meet

None of this means vector search is wrong for what it's for. A support ticket, a contract, a Slack thread, those are unstructured, and "closest passage" is a genuinely reasonable strategy when there's no schema to query against. The mistake is applying that strategy to data that already has one. A CSV of orders, a Parquet file of transactions, a SQL export of user events, these don't need semantic retrieval at all. They need a query engine an agent can call directly, cheaply, and get back an exact answer.

The token cost of a wrong retrieval compounds the same way in both worlds, an extra round trip, a retry, a chain of tokens that never needed to be spent. The difference is what fixes it. For unstructured data, the fix is a better model. For structured data, the fix already existed. It's SQL.
