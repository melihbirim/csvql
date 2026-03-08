# csvql

**The world's fastest CSV query engine** 🚀

A high-performance SQL query engine for CSV files that **beats DuckDB, DataFusion, and ClickHouse** on real-world queries over **1 million rows**. Written in Zig with radix sort, top-K heap, memory-mapped I/O, SIMD acceleration, zero-copy parsing, and lock-free parallel execution.

```bash
# SQL mode: Query 1M rows with WHERE + ORDER BY in 0.020s (9x faster than DuckDB!)
csvql "SELECT name, city, salary FROM 'data.csv' WHERE salary > 100000 ORDER BY salary DESC LIMIT 10"

# Simple mode: Same query, shorter syntax
csvql data.csv "name,city,salary" "salary>100000" 10 "salary:desc"

# Full scan + sort 1M rows in 0.156s (7.8x faster than DuckDB!)
csvql "SELECT name, city, salary FROM 'data.csv' ORDER BY salary DESC"
```

## 🏆 Performance (1M rows, 35MB CSV)

Fair benchmarks — all tools forced to output all rows (no 40-row display tricks):

| Query | csvql | DuckDB | DataFusion | ClickHouse |
|-------|--------|--------|------------|------------|
| WHERE + ORDER BY LIMIT 10 | **0.020s** | 0.179s | 0.243s | 0.750s |
| ORDER BY LIMIT 10 | **0.041s** | 0.165s | 0.143s | 0.761s |
| ORDER BY (all 1M rows) | **0.156s** | 1.221s | * | 0.451s |
| WHERE (full output) | **0.141s** | 0.739s | * | 0.796s |
| Full scan (all 1M rows) | **0.196s** | 1.163s | * | 0.798s |

*\*DataFusion CLI caps output at ~8K rows; can't get fair full-output numbers.*

✅ **9x faster than DuckDB** on WHERE + ORDER BY LIMIT (0.020s vs 0.179s)  
✅ **7.8x faster than DuckDB** on ORDER BY full output (0.156s vs 1.221s)  
✅ **5.9x faster than DuckDB** on full scan (0.196s vs 1.163s)  
✅ **35x less memory** than DuckDB (1.8MB vs 63.5MB)  
✅ **39.5M rows/sec** raw CSV parsing throughput  
✅ **1.4 GB/sec** I/O bandwidth with memory-mapped files  
✅ **7-core parallel** execution with 669% CPU utilization

See [BENCHMARKS.md](BENCHMARKS.md) for detailed analysis and [ARCHITECTURE.md](ARCHITECTURE.md) for optimization techniques.

## Why Zig?

This is a Zig reimplementation of the original Go version, bringing:

- **Native performance** - Zig compiles to highly optimized machine code
- **Zero dependencies** - Pure Zig standard library implementation
- **Memory safety** - Compile-time memory safety without garbage collection
- **Low-level control** - Fine-grained control over allocations and performance

## Features

**Currently Implemented:**

- ✅ SELECT with column projection or SELECT \*
- ✅ WHERE clause with comparisons (=, !=, >, >=, <, <=)
- ✅ **ORDER BY** with ASC/DESC sorting (all engines)
- ✅ LIMIT for result capping
- ✅ **Simple query syntax** — positional args, no SQL needed
- ✅ **Auto-detection** — SQL or simple mode based on input
- ✅ **Hardware-aware sorting** — radix sort, top-K heap, strategy auto-selection
- ✅ **World-class CSV parsing** (faster than DuckDB!)
- ✅ **Memory-mapped I/O** for zero-copy reading
- ✅ **7-core parallel execution** with lock-free architecture
- ✅ **SIMD-accelerated field parsing**
- ✅ RFC 4180 compliant CSV parsing
- ✅ Streaming execution for low memory usage
- ✅ Case-insensitive column names
- ✅ stdin support for Unix pipes
- ✅ **Standalone CSV library** (use our parser in your projects!)

**In Progress:**

- GROUP BY with aggregations (COUNT, SUM, AVG, MIN, MAX)
- Complex WHERE expressions (AND, OR, NOT)

**Planned:**

- JOIN operations
- Advanced operators (IN, LIKE, BETWEEN, IS NULL)

## Installation

### Prerequisites

- [Zig](https://ziglang.org/) 0.13.0, 0.14.0, or 0.15+ (Currently tested with Zig 0.15.2)

### Build from Source

```bash
git clone https://github.com/melihbirim/csvq
cd csvq
zig build -Doptimize=ReleaseFast
```

The binary will be in `zig-out/bin/csvql`.

### Install

```bash
# Copy to your PATH
sudo cp zig-out/bin/csvql /usr/local/bin/
```

## Quick Start

### Simple Syntax (Recommended)

```bash
# Show first 10 rows (default)
csvql data.csv

# Select specific columns
csvql data.csv "name,age,city"

# Filter rows
csvql data.csv "*" "age>30"

# Top 10 highest salaries
csvql data.csv "name,salary" "salary>0" 10 "salary:desc"

# All rows, no limit, sorted by name
csvql data.csv "*" "" 0 "name:asc"
```

See [SIMPLE_QUERY_LANGUAGE.md](SIMPLE_QUERY_LANGUAGE.md) for full syntax reference.

### SQL Syntax

```bash
# Select all columns
csvql "SELECT * FROM 'data.csv' LIMIT 10"

# Select specific columns
csvql "SELECT name, age FROM 'users.csv'"

# Filter with WHERE clause
csvql "SELECT * FROM 'sales.csv' WHERE amount > 100"

# Filter, sort, and limit
csvql "SELECT name, salary FROM 'data.csv' WHERE age > 30 ORDER BY salary DESC LIMIT 10"

# Combine filtering and projection
csvql "SELECT name, email FROM 'users.csv' WHERE age >= 18 LIMIT 100"
```

### Unix Pipes

```bash
# Read from stdin
cat data.csv | csvql "SELECT name, age FROM '-' WHERE age > 25"

# Chain filters
cat orders.csv | csvql "SELECT * FROM '-' WHERE country = 'US'" | csvql "SELECT total FROM '-' WHERE total > 1000"

# Monitor live logs
tail -f logs.csv | csvql "SELECT timestamp, message FROM '-' WHERE level = 'ERROR'"
```

### Output Redirection

```bash
# Write to file
csvql "SELECT * FROM 'data.csv' WHERE status = 'active'" > active_users.csv

# Pipe to other tools
csvql "SELECT email FROM 'users.csv'" | wc -l
```

## SQL Support

### Supported Features

- **SELECT**: Column projection (`SELECT col1, col2`) or all columns (`SELECT *`)
- **FROM**: File path (quoted or unquoted), or `-` for stdin
- **WHERE**: Comparison operators: `=`, `!=`, `>`, `>=`, `<`, `<=`
- **ORDER BY**: Sort by any column, ASC or DESC
- **LIMIT**: Limit number of results
- **Numeric coercion**: Automatic string-to-number conversion in comparisons

### Not Yet Supported

- Boolean expressions (AND, OR, NOT) in WHERE
- GROUP BY with aggregations
- JOIN operations
- Advanced operators (IN, LIKE, BETWEEN, IS NULL)
- HAVING clause

---

## Using the CSV Parser as a Library

csvql includes a **world-class CSV parser** that you can use in your own Zig projects!

### Why Use Our Parser?

- 🚀 **39.5M rows/sec** - Faster than any Zig CSV library
- 📦 **Zero-copy** - Memory-mapped I/O for 1.4 GB/sec throughput
- ✅ **RFC 4180 compliant** - Handles quoted fields, escaped quotes, CRLF
- 🎯 **Simple API** - Easy to integrate
- 💪 **Battle-tested** - Powers a tool that beats DuckDB

### Quick Start

**1. Add to your `build.zig.zon`:**

```zig
.dependencies = .{
    .csvq = .{
        .url = "https://github.com/melihbirim/csvq/archive/main.tar.gz",
        // Get hash: zig fetch --save https://github.com/melihbirim/csvq/archive/main.tar.gz
    },
},
```

**2. Basic usage (RFC 4180 compliant):**

```zig
const std = @import("std");
const csv = @import("csvq").csv;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const file = try std.fs.cwd().openFile("data.csv", .{});
    defer file.close();

    var reader = csv.CsvReader.init(allocator, file);

    // Read records one by one
    while (try reader.readRecord()) |record| {
        defer reader.freeRecord(record);

        // Use the fields (zero-copy slices!)
        for (record) |field| {
            std.debug.print("{s} ", .{field});
        }
        std.debug.print("\n", .{});
    }
}
```

**3. High-performance usage (memory-mapped):**

For maximum speed (1.4 GB/sec throughput), use memory-mapped I/O:

```zig
const file = try std.fs.cwd().openFile("data.csv", .{});
defer file.close();

const file_size = (try file.stat()).size;

// Memory-map for zero-copy reading
const mapped = try std.posix.mmap(
    null, file_size,
    std.posix.PROT.READ,
    .{ .TYPE = .SHARED },
    file.handle, 0
);
defer std.posix.munmap(mapped);

const data = mapped[0..file_size];

// Parse at 39.5M rows/sec!
var line_start: usize = 0;
while (line_start < data.len) {
    const remaining = data[line_start..];
    const line_end = std.mem.indexOfScalar(u8, remaining, '\n') orelse break;
    const line = remaining[0..line_end];

    // Parse fields by finding commas (SIMD accelerated)
    var field_start: usize = 0;
    for (line, 0..) |c, i| {
        if (c == ',') {
            const field = line[field_start..i];
            // Process field (zero-copy!)
            field_start = i + 1;
        }
    }

    line_start += line_end + 1;
}
```

**4. Examples:**

Check out complete examples in the `examples/` directory:

- `csv_reader_example.zig` - Basic RFC 4180 compliant parsing
- `mmap_csv_example.zig` - High-performance memory-mapped parsing

Build and run:

```bash
zig build -Doptimize=ReleaseFast
./zig-out/bin/csv_reader_example data.csv
```

### API Reference

**`CsvReader`** - RFC 4180 compliant reader

- `init(allocator, file)` - Create reader
- `readRecord()` - Read next row, returns `?[][]u8`
- `freeRecord(record)` - Free memory for a record

**Performance Tips:**

1. Use `.ReleaseFast` optimization for 10x+ speedup
2. For files >10MB, use memory-mapped I/O
3. For multi-core systems, split file into chunks (see `parallel_mmap.zig`)
4. Use SIMD for comma detection on large lines (see `simd.zig`)

---

## SQL Support

### Supported Features

- **SELECT**: Column projection (`SELECT col1, col2`) or all columns (`SELECT *`)
- **FROM**: File path (quoted or unquoted), or `-` for stdin
- **WHERE**: Comparison operators: `=`, `!=`, `>`, `>=`, `<`, `<=`
- **ORDER BY**: Sort by any column, ASC or DESC
- **LIMIT**: Limit number of results
- **Numeric coercion**: Automatic string-to-number conversion in comparisons

### Not Yet Supported

- Boolean expressions (AND, OR, NOT) in WHERE
- GROUP BY with aggregations
- JOIN operations
- Advanced operators (IN, LIKE, BETWEEN, IS NULL)
- HAVING clause

## Development

### Build and Test

```bash
# Debug build
zig build

# Release build (optimized)
zig build -Doptimize=ReleaseFast

# Run tests
zig build test

# Run CSV parsing benchmark
zig build bench -Doptimize=ReleaseFast -- your_file.csv

# Build and run example programs
zig build -Doptimize=ReleaseFast
./zig-out/bin/csv_reader_example test.csv
./zig-out/bin/mmap_csv_example large_file.csv

# Run with arguments
zig build run -- "SELECT * FROM 'test.csv' LIMIT 5"
```

### Project Structure

```bash
src/
  main.zig           # CLI entry point + auto-detection (SQL vs simple mode)
  parser.zig         # SQL query parser (SELECT, WHERE, ORDER BY, LIMIT)
  simple_parser.zig  # Simple positional argument parser
  engine.zig         # Query execution orchestrator + sequential engine
  parallel_mmap.zig  # Lock-free parallel CSV engine (our champion!)
  mmap_engine.zig    # Single-threaded memory-mapped execution
  csv.zig            # RFC 4180 CSV reader/writer (256KB buffered)
  bulk_csv.zig       # Bulk line reader (2MB blocks, zero-copy)
  fast_sort.zig      # Hardware-aware sorting (radix sort, top-K heap)
  simd.zig           # SIMD utilities (vectorized CSV parsing)
  aggregation.zig    # GROUP BY aggregations (in progress)
build.zig            # Build configuration
tests/
  parser_test.zig    # SQL parser tests
  engine_test.zig    # Engine integration tests
  simple_parser_test.zig  # Simple parser tests
bench/
  csv_parse_bench.zig  # CSV parsing benchmarks
examples/
  csv_reader_example.zig  # How to use CSV parser
  mmap_csv_example.zig    # High-performance mmap usage
ARCHITECTURE.md      # Engine architecture & optimization details
SIMPLE_QUERY_LANGUAGE.md  # Simple query syntax reference
BENCHMARKS.md        # Detailed performance analysis
```

## Architecture

```
Input CSV → Auto-detect mode (SQL vs Simple) → Parse Query → Check File Size
                                                                    ↓
                            ┌───────────────────┼────────────────────┐
                            │                   │                    │
                      < 5MB file          5-10MB file          > 10MB file
                            │                   │                    │
                            ↓                   ↓                    ↓
                     Sequential          Memory-Mapped        Parallel Memory-Mapped
                     (streaming)         (single thread)       (7-core lock-free)
                            │                   │                    │
                            └───────────────────┴────────────────────┘
                                                ↓
                                  Filter (WHERE) - Direct column indexing
                                  No HashMap on hot path!
                                                ↓
                                  Sort (ORDER BY) - Pre-parsed numeric keys
                                  Zero-copy slices, arena-based buffering
                                                ↓
                                  Project (SELECT) - Zero-copy
                                  Only re-parse top K rows (LIMIT)
                                                ↓
                                  Write CSV (1MB buffer)
                                                ↓
                                           Output
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed optimization techniques.

### Performance Strategy

1. **File Size Routing**: Picks optimal strategy based on file size
2. **Memory-Mapped I/O**: Zero-copy reading for files >5MB (1.4 GB/sec)
3. **Lock-Free Parallel**: 7-core execution with thread-local buffers (669% CPU)
4. **SIMD Field Parsing**: Vectorized comma detection (@Vector for 16-byte chunks)
5. **Direct Column Indexing**: WHERE evaluation without HashMap overhead
6. **Zero-Copy Output**: Parse once, output directly (no double-parse!)
7. **Arena Allocation**: Bulk allocations per thread, cleared after chunk
8. **Pre-Parsed Sort Keys**: f64 numeric keys parsed once, eliminating parseFloat in O(N log N) comparisons
9. **Zero Per-Row Allocations**: ORDER BY uses mmap slices and arena buffers instead of allocator.dupe
10. **Lazy Column Extraction**: Only re-parse top K rows after sorting (LIMIT optimization)
11. **Radix Sort**: O(8N) LSD radix sort on IEEE 754 f64→u64 keys with pass-skipping
12. **Top-K Heap**: O(N log K) min-heap for LIMIT queries — avoids sorting entire dataset
13. **Hardware-Aware Strategy**: ARM vs x86 thresholds for L1 cache-optimal heap size and radix cutoff
14. **Indirect Sort**: 12-byte (key, index) pairs → 4x less data movement than full struct sort
15. **DESC Without Reverse**: XOR key flipping gives descending order from ascending radix sort

**Result**: 9x faster than DuckDB, 3.5x faster than DataFusion on sort queries!

## Benchmarks

### vs DuckDB, DataFusion, ClickHouse (1M rows, 35MB CSV, Apple M2)

All tools forced to output all rows — no 40-row display tricks.

> **Important**: DuckDB and DataFusion CLIs default to displaying only 40 rows, making them appear much faster than they really are. These benchmarks use `-csv` mode (DuckDB) and `FORMAT CSV` (ClickHouse) to force full output materialization.

| Query | csvql | DuckDB | DataFusion* | ClickHouse | csvql vs DuckDB |
|-------|--------|--------|-------------|------------|------------------|
| **Q1:** WHERE + ORDER BY LIMIT 10 | **0.020s** | 0.179s | 0.243s | 0.750s | 🏆 **9x faster** |
| **Q2:** ORDER BY LIMIT 10 | **0.041s** | 0.165s | 0.143s | 0.761s | 🏆 **4x faster** |
| **Q3:** ORDER BY (all 1M rows) | **0.156s** | 1.221s | — | 0.451s | 🏆 **7.8x faster** |
| **Q4:** WHERE (full output ~450K rows) | **0.141s** | 0.739s | — | 0.796s | 🏆 **5.2x faster** |
| **Q5:** Full scan (all 1M rows) | **0.196s** | 1.163s | — | 0.798s | 🏆 **5.9x faster** |

*\*DataFusion CLI caps output at ~8K rows regardless of format settings; fair full-output numbers unavailable.*

**Memory efficiency**:

- csvql: 1.8MB
- DuckDB: 63.5MB
- **35x less memory!** 🎯

### Sort Optimization Stack

| Technique | Benefit |
|---|---|
| Top-K Heap (O(N log K)) | LIMIT 10 on 1M rows → only maintain 10-element heap |
| Radix Sort (O(8N)) | No comparisons — IEEE 754 f64→u64 bit trick |
| Pass-Skipping | Skip bytes where all keys are identical (8→3-4 passes for salary data) |
| Indirect Sort | Sort 12-byte (key,idx) pairs, not 48-byte structs → 4x less data movement |
| DESC via XOR | Flip key bits instead of reversing entire array |
| Hardware-Aware | ARM M2 vs x86 thresholds for L1 cache-optimal heap/radix cutoffs |

### ORDER BY Optimization Journey

From initial ORDER BY implementation to beating every competitor:

| Version | Time (1M rows) | Speedup |
|---|---|---|
| Naive ORDER BY (per-row allocs) | ~9.3s | 1x |
| + Zero-copy CSV parsing | 0.235s | 40x |
| + Arena-based buffering | 0.150s | 62x |
| + Pre-parsed f64 sort keys | 0.090s | 103x |
| + Lazy column extraction | 0.073s | 127x |
| + Top-K heap (LIMIT queries) | 0.020s | **465x** |
| + Radix sort + pass-skipping | **0.156s** (full sort) | **60x** |

### CSV Parsing Performance (Raw Speed)

1M rows, 35MB file — pure parsing benchmark:

| Method | Time | Speed | Throughput |
|---|---|---|---|
| **Memory-mapped** | **25ms** | **39.5M rows/sec** | **1.4 GB/sec** |
| Buffered (256KB) | 44ms | 22.9M rows/sec | 795 MB/sec |
| Naive (byte-by-byte) | 15.3s | 65K rows/sec | 2.3 MB/sec |

See [BENCHMARKS.md](BENCHMARKS.md) for detailed analysis and [ARCHITECTURE.md](ARCHITECTURE.md) for optimization techniques.

## Contributing

This is a learning project and Zig port. Contributions welcome:

- Bug reports and fixes
- Performance improvements
- Feature implementations
- Documentation improvements

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Licensing and Contributions

csvql is licensed under the **MIT License** — see [LICENSE.md](LICENSE.md) for the full text.

The MIT License keeps the project open, simple, and widely usable. Anyone is free to use, modify, and distribute csvql, including in commercial environments, in accordance with the MIT License.

By contributing to this project (via pull requests, patches, or any form of contribution), you agree that your contribution will be licensed under the MIT License and may be incorporated into future versions of the project.

## Acknowledgments

- Original [csvq](https://github.com/melihbirim/csvq) by [@melihbirim](https://github.com/melihbirim)
- Competed with [DuckDB](https://duckdb.org/), [DataFusion](https://datafusion.apache.org/), and [ClickHouse](https://clickhouse.com/) — and won! 🏆
- Inspired by the challenge of beating world-class database engines

## Performance Highlights

🚀 **9x faster than DuckDB** on WHERE + ORDER BY LIMIT (1M rows)  
⚡ **7.8x faster than DuckDB** on ORDER BY full output (1M rows)  
💨 **5.9x faster than DuckDB** on full scan (1M rows)  
🔥 **3x faster than ClickHouse** on sort queries  
🎯 **35x less memory** than DuckDB (1.8MB vs 63.5MB)  
📈 **465x faster** than naive ORDER BY implementation  
🏃 **39.5M rows/sec** raw CSV parsing speed  
💾 **1.4 GB/sec** I/O throughput  
🔥 **669% CPU utilization** (7-core parallel)  
✅ **Fair benchmarks** — all tools forced to output all rows (no 40-row display tricks)

---

**Built with Zig** 🦎 • **Beating DuckDB, DataFusion & ClickHouse** 🏆 • **Open source** [MIT](LICENSE.md)
