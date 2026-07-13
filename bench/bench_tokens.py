#!/usr/bin/env python3
"""
bench_tokens.py — token cost of pasting a CSV into an LLM vs querying it via csvql's MCP.

Answers 5 realistic questions about the data both ways and counts the tokens the model
would consume: the whole file (paste) vs the SQL + result rows (query). The query cost is
flat — independent of file size — while the paste cost grows linearly and overflows the
context window almost immediately.

Usage:
    ./bench/bench_tokens.py [CSV]     # default: bench/.taxi-data/sample.csv
    # get the sample first with:  ./bench/bench_taxi.sh --sample

Tokenizer: uses `tiktoken` (cl100k) if installed for exact counts; otherwise falls back to
a char-based estimate and says so. `pip install tiktoken` for the accurate numbers.
"""
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
CSVQL = os.path.join(ROOT, "zig-out", "bin", "csvql")
if not os.path.exists(CSVQL):
    CSVQL = subprocess.run(["bash", "-lc", "command -v csvql"], capture_output=True, text=True).stdout.strip() or CSVQL

SRC = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, ".taxi-data", "sample.csv")
CONTEXT_WINDOW = 200_000  # typical LLM context budget, for the "does it even fit" column

# --- tokenizer (exact if tiktoken is present, else honest approximation) --------------
try:
    import tiktoken

    _enc = tiktoken.get_encoding("cl100k_base")
    tok = lambda s: len(_enc.encode(s))
    TOKENIZER = "tiktoken cl100k (exact)"
except Exception:
    # CSV is token-dense; ~0.29 tok/char measured on this dataset vs tiktoken. Approximate.
    tok = lambda s: max(1, round(len(s) * 0.29))
    TOKENIZER = "char-estimate (APPROXIMATE — `pip install tiktoken` for exact)"

# 5 questions a user might ask an agent about this file, and the SQL that answers each.
QA = [
    ("How many trips per cab type?",
     "SELECT cab_type, COUNT(*) FROM '{f}' GROUP BY cab_type"),
    ("Average total fare by passenger count?",
     "SELECT passenger_count, AVG(total_amount) FROM '{f}' GROUP BY passenger_count"),
    ("Which year had the most trips?",
     "SELECT DATE_PART('year',pickup_datetime) AS y, COUNT(*) AS c FROM '{f}' GROUP BY y ORDER BY c DESC LIMIT 1"),
    ("Total number of trips?",
     "SELECT COUNT(*) FROM '{f}'"),
    ("Top 3 passenger counts by average tip?",
     "SELECT passenger_count, AVG(tip_amount) AS a FROM '{f}' GROUP BY passenger_count ORDER BY a DESC LIMIT 3"),
]

# One-time MCP tool-schema overhead the model pays once per conversation.
TOOL_SCHEMA = """{"name":"csv_query","description":"Run a read-only SQL query against a CSV file and return the rows.","input_schema":{"type":"object","properties":{"file":{"type":"string","description":"path to the CSV file"},"sql":{"type":"string","description":"SQL SELECT query; FROM must reference the file path in single quotes"}},"required":["file","sql"]}}
{"name":"csv_schema","description":"Return column names, inferred types, row count and size of a CSV file.","input_schema":{"type":"object","properties":{"file":{"type":"string"}},"required":["file"]}}
{"name":"csv_list","description":"List CSV/JSON files in a directory.","input_schema":{"type":"object","properties":{"dir":{"type":"string"}},"required":["dir"]}}"""


def paste_tokens(path):
    with open(path, "r", errors="replace") as fh:
        return tok(fh.read())


def query_tokens(path):
    total = 0
    for question, sql in QA:
        q = sql.format(f=path)
        out = subprocess.run([CSVQL, q], capture_output=True, text=True).stdout
        total += tok(question) + tok(q) + tok(out)  # sent (question+SQL) + received (rows)
    return total


def main():
    if not os.path.exists(SRC):
        sys.exit(f"CSV not found: {SRC}\nGet the sample first:  ./bench/bench_taxi.sh --sample")
    if not os.path.exists(CSVQL):
        sys.exit("csvql not found (build: zig build -Doptimize=ReleaseFast)")

    lines = open(SRC, errors="replace").read().splitlines()
    header, body = lines[0], lines[1:]
    # ~417 bytes/row on this dataset → row counts approximating each size.
    sizes = [("1 MB", 2_400), ("10 MB", 24_000), ("100 MB", 240_000), ("full", len(body))]

    print(f"\n  Token cost: paste CSV into context  vs  query via csvql --mcp")
    print(f"  source={SRC}  rows={len(body):,}  tokenizer={TOKENIZER}\n")
    print(f"  {'size':>7} {'rows':>10} {'paste':>13} {'query':>9} {'ratio':>9}  fits {CONTEXT_WINDOW//1000}K?")

    schema_t = tok(TOOL_SCHEMA)
    for label, n in sizes:
        n = min(n, len(body))
        slice_path = os.path.join(HERE, ".taxi-data", f"slice_{label.replace(' ', '')}.csv")
        with open(slice_path, "w") as fh:
            fh.write(header + "\n" + "\n".join(body[:n]) + "\n")
        p = paste_tokens(slice_path)
        q = query_tokens(slice_path) + schema_t  # amortized one-time schema counted once
        os.remove(slice_path)
        fits = "yes" if p <= CONTEXT_WINDOW else "NO"
        print(f"  {label:>7} {n:>10,} {p:>13,} {q:>9,} {round(p / q):>8,}x  {fits}")

    print(f"\n  Query cost is flat — SQL + a few result rows, independent of file size.")
    print(f"  MCP tool schema: {schema_t} tokens, paid once per conversation (not per query).\n")


if __name__ == "__main__":
    main()
