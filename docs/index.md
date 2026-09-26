---
layout: home
title: "csvql: the analytical CSV query engine for AI agents"
description: "Run SQL on CSV files in place, using about 10x less memory than a database. MCP-native so an agent can query gigabyte files for a few hundred tokens, sandboxed to a directory you choose. Single 1.3 MB Zig binary, read-only, air-gappable."
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
      <div class="card"><h3>About 10&times; less memory</h3><p>Counting 5 million distinct values in an 11&nbsp;GB CSV: <strong>235&nbsp;MB</strong> for csvql, 2,639&nbsp;MB for DuckDB. A plain filter over the same file holds <strong>53&nbsp;MB</strong>. Fits on the small VM sitting next to your data.</p></div>
      <div class="card"><h3>Sandboxed by a flag</h3><p><code>--root &lt;dir&gt;</code> confines every file the process can open. Paths outside it, including <code>../</code> traversal, fail with <code>PathOutsideAllowedRoot</code>. Give an agent a directory, not a filesystem.</p></div>
      <div class="card"><h3>MCP-native</h3><p>Ships as an MCP server, wired up by <code>csvql install</code>. Agents query CSV/JSON directly; results are capped so a careless query can't flood the context.</p></div>
      <div class="card"><h3>Read-only</h3><p>SELECT only. No INSERT/UPDATE/DELETE/DROP exists. It cannot modify your data.</p></div>
      <div class="card"><h3>Zero ingest, zero network</h3><p>Queries the raw CSV in place via mmap. No import step, no native store to build first. No cloud, no telemetry, air-gappable, with <code>--audit</code> logging.</p></div>
      <div class="card"><h3>One 1.3&nbsp;MB binary</h3><p>Written in Zig. No runtime, no dependencies. macOS, Linux, Windows. Also on npm, PyPI, and Homebrew.</p></div>
    </div>
  </div>
</section>

<section id="perf">
  <div class="wrap">
    <h2><span class="accent">Faster than DuckDB</span> on scans and joins</h2>
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
    <p>Plus <strong>zero extra storage</strong>. DuckDB's comparable speed needs a 2.1&nbsp;GB native store built over ~22&nbsp;seconds first. Reproduce with <code>bench/bench_taxi.sh</code> and <code>bench/bench_all.sh --section join</code>. More on why joins and aggregates scale differently: <a href="blog/join-performance-vs-duckdb.html">the blog post</a>.</p>
  </div>
</section>

<section id="memory">
  <div class="wrap">
    <h2><span class="accent">The memory gap</span>, including where DuckDB wins</h2>
    <p>An 11.15&nbsp;GB CSV, 75 million rows, one column with 5 million distinct values. Apple M2 Pro, 12 cores, 16&nbsp;GB RAM, DuckDB 1.4.2, both engines reading the same raw CSV, best-of-3.</p>
    <div class="scroll">
    <table>
      <tr><th>Query</th><th>csvql</th><th>DuckDB</th><th>Memory</th></tr>
      <tr><td><code>COUNT(*) WHERE country='JP'</code></td><td><strong>1.95s</strong></td><td>4.34s</td><td><strong>53&nbsp;MB</strong> vs 268&nbsp;MB</td></tr>
      <tr><td><code>GROUP BY country</code> (8 groups)</td><td><strong>1.76s</strong></td><td>4.84s</td><td><strong>55&nbsp;MB</strong> vs 265&nbsp;MB</td></tr>
      <tr><td><code>COUNT(DISTINCT user_id)</code> (5M)</td><td><strong>4.8s</strong></td><td>5.7s</td><td><strong>235&nbsp;MB</strong> vs 2,639&nbsp;MB</td></tr>
      <tr><td><code>SELECT DISTINCT user_id</code> (5M)</td><td>9.0s</td><td><strong>7.0s</strong></td><td><strong>262&nbsp;MB</strong> vs 2,724&nbsp;MB</td></tr>
      <tr><td><code>GROUP BY user_id</code> (5M groups)</td><td>16.2s</td><td><strong>8.3s</strong></td><td><strong>1,502&nbsp;MB</strong> vs 2,894&nbsp;MB</td></tr>
    </table>
    </div>
    <p>DuckDB is faster on the last two. It is a mature parallel engine and on high-cardinality grouping it is still about 2&times; ahead, at roughly twice the memory. csvql wins the scans and holds a fraction of the RAM on every row. Pick accordingly: if memory is free, DuckDB is an excellent default. If you are running next to the data on a small box, or handing a filesystem-confined tool to an agent, that column is the reason csvql exists.</p>
    <p>The benchmark that produced this table also found a real bug in csvql, which is written up in full: <a href="blog/distinct-values-in-a-10gb-csv.html">Five ways to find distinct values in a 10&nbsp;GB CSV</a>.</p>
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
    <p>Prebuilt binaries and one-click <code>.mcpb</code> bundles for Claude Desktop on the <a href="https://github.com/melihbirim/csvql/releases">releases page</a>. Also: <code>npm i csvql-query</code>, <code>pip install csvql-query</code>.</p>
  </div>
</section>
