---
layout: home
title: "csvql: the analytical CSV query engine for AI agents"
description: "Run SQL on CSV files in place. No database, no import, no ingest. MCP-native so an LLM can query gigabyte files for a few hundred tokens. Single static Zig binary, read-only, air-gappable."
---

<section id="tokens">
  <div class="wrap">
    <h2><span class="accent">Token economics.</span> Query files instead of pasting them.</h2>
    <p>Pasting a 417&nbsp;MB CSV into an LLM costs about <strong>230 million tokens</strong>. It fits no context window. Over MCP, the agent queries the file in place and gets back only the answer.</p>
    <div class="scroll">
    <table>
      <tr><th>Question an agent asks</th><th>Tokens used</th></tr>
      <tr><td>"How many trips per cab type?"</td><td><strong>43</strong></td></tr>
      <tr><td>"Which year was busiest?"</td><td><strong>49</strong></td></tr>
      <tr><td>"Average fare by passenger count?"</td><td><strong>123</strong></td></tr>
    </table>
    </div>
    <p>Same answers, <strong>~1,000&ndash;500,000&times; fewer tokens</strong>. Flat, regardless of file size. One command wires it into Claude.</p>
  </div>
</section>

<section id="why">
  <div class="wrap">
    <h2>Why <span class="accent">csvql</span></h2>
    <p class="quote">A database is something you load your data into. csvql is a query you run on the data where it already lives.</p>
    <div class="grid">
      <div class="card"><h3>MCP-native</h3><p>Ships as an MCP server. Agents query CSV/JSON directly; results are capped so a careless query can't flood the context.</p></div>
      <div class="card"><h3>Read-only</h3><p>SELECT only. No INSERT/UPDATE/DELETE/DROP exists. It cannot modify your data.</p></div>
      <div class="card"><h3>On-prem &amp; air-gapped</h3><p>Zero network calls, no cloud, no telemetry. Run it next to the data with <code>--root</code> sandboxing and <code>--audit</code> logging.</p></div>
      <div class="card"><h3>Zero ingest</h3><p>Queries the raw CSV in place via mmap. No import step, no 2&nbsp;GB native store to build first.</p></div>
      <div class="card"><h3>Fast</h3><p>SIMD parsing, lock-free parallel execution, radix sort. On large files it reads raw CSV about as fast as your OS hands it the bytes.</p></div>
      <div class="card"><h3>Single binary</h3><p>Written in Zig. No runtime, no dependencies. macOS, Linux, Windows. Also on npm, PyPI, and Homebrew.</p></div>
    </div>
  </div>
</section>

<section id="perf">
  <div class="wrap">
    <h2><span class="accent">Faster than DuckDB</span> on raw CSV</h2>
    <p>NYC Taxi benchmark on DuckDB's own dataset, both engines querying the raw uncompressed CSV directly (no preload), best-of-5.</p>
    <div class="scroll">
    <table>
      <tr><th>File</th><th>Query</th><th>csvql</th><th>DuckDB</th><th>Speedup</th></tr>
      <tr><td>417 MB</td><td>GROUP BY</td><td><strong>0.05s</strong></td><td>0.54s</td><td><strong>~10&times;</strong></td></tr>
      <tr><td>8 GB</td><td>GROUP BY</td><td><strong>1.31s</strong></td><td>3.55s</td><td><strong>~2.8&times;</strong></td></tr>
      <tr><td>81 MB</td><td>JOIN</td><td><strong>0.05s</strong></td><td>2.6s</td><td><strong>~54&times;</strong></td></tr>
      <tr><td>81 MB</td><td>JOIN SELECT *</td><td><strong>0.11s</strong></td><td>7.6s</td><td><strong>~70&times;</strong></td></tr>
    </table>
    </div>
    <p>Plus <strong>~6&times; less memory</strong> and <strong>zero extra storage</strong>. DuckDB's comparable speed needs a 2.1&nbsp;GB native store built over ~22&nbsp;seconds first. Reproduce with <code>bench/bench_taxi.sh</code> and <code>bench/bench_all.sh --section join</code>. More on why joins and aggregates scale differently: <a href="blog/join-performance-vs-duckdb.html">the blog post</a>.</p>
  </div>
</section>

<section id="install">
  <div class="wrap">
    <h2>Install</h2>
<pre><span class="c"># Homebrew (macOS / Linux)</span>
brew install melihbirim/csvql/csvql

<span class="c"># Query a CSV</span>
csvql <span class="k">"SELECT cab_type, COUNT(*) FROM 'trips.csv' GROUP BY cab_type"</span>

<span class="c"># Wire it into Claude (Code + Desktop), no manual config</span>
csvql install</span></pre>
    <p>Prebuilt binaries and one-click <code>.mcpb</code> bundles for Claude Desktop on the <a href="https://github.com/melihbirim/csvql/releases">releases page</a>. Also: <code>npm i csvql-query</code>, <code>pip install csvql-query</code>.</p>
  </div>
</section>
