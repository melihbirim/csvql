#!/usr/bin/env node
'use strict';

// csvql-query CLI — the zero-install entry point.
//
//   npx csvql-query "SELECT city, COUNT(*) FROM 'data.csv' GROUP BY city"
//
// This exists so someone can try csvql in one line without adding a
// dependency or writing a script. It is a thin wrapper over the same N-API
// module index.js exports — no query logic lives here, so it cannot drift
// from the library.
//
// For the MCP server (what an AI agent connects to), install the standalone
// binary: `brew install melihbirim/csvql/csvql` then `csvql install`. That
// server lives in the Zig binary, not in this package — see --help.

const csvql = require('./index.js');

const VERSION = require('./package.json').version;

const HELP = `csvql-query ${VERSION} — SQL on CSV files, in place

USAGE
  npx csvql-query [options] "<SQL>"

  File paths go in single quotes inside the FROM clause:
    npx csvql-query "SELECT * FROM 'data.csv' LIMIT 5"

OPTIONS
  -j, --json              Output JSON instead of CSV
  -d, --delimiter <c>     Input delimiter (e.g. -d '\\t' for TSV)
  -c, --comment <s>       Skip lines starting with <s>
  -s, --skip-empty        Skip blank lines
  -h, --help              Show this help
  -v, --version           Show version

EXAMPLES
  npx csvql-query "SELECT * FROM 'sales.csv' WHERE amount > 1000 LIMIT 10"
  npx csvql-query "SELECT region, SUM(amount) FROM 'sales.csv' GROUP BY region"
  npx csvql-query --json "SELECT COUNT(*) AS n FROM 'sales.csv'"
  npx csvql-query -d '\\t' "SELECT * FROM 'data.tsv' LIMIT 3"

SUPPORTED SQL
  SELECT, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, JOIN, DISTINCT, LIKE,
  IN (incl. subqueries), BETWEEN, COUNT/SUM/AVG/MIN/MAX, and scalar functions.
  Read-only: there is no INSERT/UPDATE/DELETE/DROP.

FOR AI AGENTS (MCP)
  This package is the Node library + CLI. The MCP server ships in the
  standalone binary so an agent can query a multi-GB CSV for a few hundred
  tokens instead of pasting it into context:

    brew install melihbirim/csvql/csvql   # or grab a release binary
    csvql install                         # registers it with Claude

  https://github.com/melihbirim/csvql#mcp

EXIT CODES
  0 success   1 usage error   2 query error   3 file/IO error
`;

function fail(code, msg) {
  process.stderr.write(`csvql-query: ${msg}\n`);
  process.exit(code);
}

function parseArgs(argv) {
  const opts = {};
  let sql = null;
  let json = false;

  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const needsValue = (name) => {
      const v = argv[++i];
      if (v === undefined) fail(1, `${name} requires a value`);
      return v;
    };

    switch (a) {
      case '-h':
      case '--help':
        process.stdout.write(HELP);
        process.exit(0);
        break;
      case '-v':
      case '--version':
        process.stdout.write(`${VERSION}\n`);
        process.exit(0);
        break;
      case '-j':
      case '--json':
        json = true;
        break;
      case '-d':
      case '--delimiter':
        // Accept a literal backslash-t from the shell as a real tab, so
        // `-d '\t'` works without the caller needing $'\t' quoting.
        opts.delimiter = needsValue(a).replace(/\\t/g, '\t');
        break;
      case '-c':
      case '--comment':
        opts.comment = needsValue(a);
        break;
      case '-s':
      case '--skip-empty':
        opts.skipEmptyLines = true;
        break;
      default:
        if (a.startsWith('-') && a !== '-') fail(1, `unknown option: ${a}`);
        if (sql !== null) fail(1, 'more than one SQL statement given (quote the whole query)');
        sql = a;
    }
  }

  return { sql, opts, json };
}

function main() {
  const { sql, opts, json } = parseArgs(process.argv.slice(2));

  if (sql === null || sql.trim() === '') {
    process.stderr.write(HELP);
    process.exit(1);
  }

  let out;
  try {
    out = json ? JSON.stringify(csvql.query(sql, opts), null, 2) : csvql.queryCsv(sql, opts);
  } catch (err) {
    // Distinguish "your file is missing" from "your SQL is wrong" so a
    // script (or an agent) can branch on it, matching the standalone
    // binary's exit-code contract.
    const msg = String((err && err.message) || err);
    const isIO = /ENOENT|no such file|FileNotFound|cannot open|EmptyFile/i.test(msg);
    fail(isIO ? 3 : 2, msg);
  }

  process.stdout.write(out.endsWith('\n') ? out : out + '\n');
}

main();
