---
layout: home
title: "csvql: read-only SQL on CSV files for AI agents"
description: "Give an AI agent SQL over your CSV files, not your computer. Out of the box it can't write, can't reach the network, can't leave its folder, and can't pour a whole table into the context. About as fast as DuckDB and polars, with 3 to 11x less memory."
---

<section id="why">
  <div class="wrap">
    <h2>Why <span class="accent">csvql</span></h2>
    <p class="quote">A database is something you load your data into. csvql is a query you run on the data where it already lives.</p>
    <div class="grid">
      <div class="card"><h3>Read-only</h3><p>SELECT only. No INSERT/UPDATE/DELETE/DROP exists in the parser. It cannot modify your data.</p></div>
      <div class="card"><h3>Sandboxed by default</h3><p>As an MCP server, csvql only opens files under the directory it started in. <code>--root &lt;dir&gt;</code> picks the folders instead. Paths outside it, including <code>../</code> traversal, fail with <code>PathOutsideAllowedRoot</code>. Give an agent a directory, not a filesystem.</p></div>
      <div class="card"><h3>MCP-native</h3><p>Ships as an MCP server, wired up by <code>csvql install</code>. Results are capped at about 100 rows / 12&nbsp;KB, so a careless query can't flood the context. The cap is per call, so keep data an agent must never see out with <code>--deny-columns</code>.</p></div>
      <div class="card"><h3>3 to 11&times; less memory</h3><p>A <code>GROUP BY</code> on an 8&nbsp;GB file: <strong>74&nbsp;MB</strong> for csvql, 367 for DuckDB, 1,754 for polars. Counting 5 million distinct values in 11&nbsp;GB: <strong>235&nbsp;MB</strong> against 2,639 for DuckDB. Fits on the small VM next to your data.</p></div>
      <div class="card"><h3>Zero ingest, zero network</h3><p>Queries the raw CSV in place via mmap. No import step, no native store to build first. No cloud, no telemetry, air-gappable, with <code>--audit</code> logging.</p></div>
      <div class="card"><h3>One static binary</h3><p>Written in Zig. No runtime, no dependencies. macOS, Linux, Windows. Published as <code>csvql-query</code> on npm and PyPI, <code>csvql</code> on Homebrew.</p></div>
    </div>
  </div>
</section>

<section id="perf">
  <div class="wrap">
    <h2><span class="accent">Same speed band</span>, a fraction of the memory</h2>
    <p>Same <code>GROUP BY department</code> on raw CSV, one process per engine, Apple M2 Pro, 16&nbsp;GB. Peak memory and wall time:</p>
    <div class="scroll">
    <table>
      <tr><th>File</th><th>csvql</th><th>DuckDB</th><th>polars 2.0</th></tr>
      <tr><td>0.69 GB</td><td><strong>74 MB</strong> / 0.2s</td><td>254 MB / 0.8s</td><td>1151 MB / 0.6s</td></tr>
      <tr><td>1.89 GB</td><td><strong>74 MB</strong> / 0.4s</td><td>253 MB / 1.2s</td><td>2107 MB / 1.1s</td></tr>
      <tr><td>3.80 GB</td><td><strong>75 MB</strong> / 0.7s</td><td>275 MB / 2.1s</td><td>2666 MB / 9.8s</td></tr>
      <tr><td>8.14 GB</td><td><strong>74 MB</strong> / 2.4s</td><td>367 MB / 5.5s</td><td>1754 MB / 26.7s</td></tr>
    </table>
    </div>
    <p>Memory is flat because an aggregate holds an accumulator, not a dataset. Speed is in the same band as DuckDB and polars: ahead on scans, filters, small files and joins against raw CSV, level with polars on <code>COUNT(*)</code> and top-N, behind DuckDB where it has to hold a lot of state.</p>
    <p>Where DuckDB wins, on an 11.15&nbsp;GB CSV with 75 million rows and a column of 5 million distinct values (DuckDB 1.4.2, best-of-3):</p>
    <div class="scroll">
    <table>
      <tr><th>Query</th><th>csvql</th><th>DuckDB</th><th>Memory</th></tr>
      <tr><td><code>COUNT(*) WHERE country='JP'</code></td><td><strong>1.95s</strong></td><td>4.34s</td><td><strong>53&nbsp;MB</strong> vs 268&nbsp;MB</td></tr>
      <tr><td><code>COUNT(DISTINCT user_id)</code> (5M)</td><td><strong>4.8s</strong></td><td>5.7s</td><td><strong>235&nbsp;MB</strong> vs 2,639&nbsp;MB</td></tr>
      <tr><td><code>SELECT DISTINCT user_id</code> (5M)</td><td>9.0s</td><td><strong>7.0s</strong></td><td><strong>262&nbsp;MB</strong> vs 2,724&nbsp;MB</td></tr>
      <tr><td><code>GROUP BY user_id</code> (5M groups)</td><td>16.2s</td><td><strong>8.3s</strong></td><td><strong>1,502&nbsp;MB</strong> vs 2,894&nbsp;MB</td></tr>
    </table>
    </div>
    <p>On high-cardinality grouping DuckDB is about 2&times; ahead, at roughly twice the memory. <code>MEDIAN</code> is a 5&times; loss. If memory is free, DuckDB is an excellent default. If you are running next to the data on a small box, or handing a directory-confined tool to an agent, the memory column is the reason csvql exists.</p>
    <p>Every number has a reproduce script. Full tables, including joins, quoted CSV, wide files and the losses: <a href="https://github.com/melihbirim/csvql/blob/main/BENCHMARKS.md">BENCHMARKS.md</a>. The run that produced the second table also found a real bug: <a href="blog/distinct-values-in-a-10gb-csv.html">Five ways to find distinct values in a 10&nbsp;GB CSV</a>.</p>
  </div>
</section>

<section id="agents">
  <div class="wrap">
    <h2>Giving an agent <span class="accent">a directory, not a filesystem</span></h2>
    <p><code>--root</code> confines every file the process can open, so an agent with shell access to csvql still cannot read outside the directory you named.</p>
<pre><span class="c"># allowed</span>
csvql --root /data <span class="k">"SELECT COUNT(*) FROM '/data/events.csv'"</span>
2000000

<span class="c"># blocked, including ../ traversal</span>
csvql --root /data <span class="k">"SELECT COUNT(*) FROM '/etc/passwd'"</span>
execution error: error.PathOutsideAllowedRoot</pre>
    <p>Combined with SELECT-only execution, no network calls and <code>--audit</code> logging, that is the whole trust story: the tool cannot modify your data, cannot phone home, and cannot read outside its root.</p>
    <p>Token cost: five questions about a taxi dataset cost about <strong>640 tokens</strong> over MCP, tool schema included, and that does not change with file size. Pasting the 417&nbsp;MB file would cost about 230 million tokens, so pasting was never an option. An agent that already shells out to <code>duckdb -csv</code> uses slightly fewer tokens than csvql over MCP, because MCP returns JSON. The case for csvql is the paragraph above, not the token count.</p>
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
csvql install</pre>
    <p>Prebuilt binaries for macOS, Linux and Windows, and one-click <code>.mcpb</code> bundles for Claude Desktop, on the <a href="https://github.com/melihbirim/csvql/releases">releases page</a>. Also: <code>npm i csvql-query</code>, <code>pip install csvql-query</code>.</p>
  </div>
</section>
