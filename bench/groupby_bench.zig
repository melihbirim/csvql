/// GROUP BY benchmark harness.
///
/// Measures GROUP BY query performance using the csvq engine at three
/// cardinalities (low / medium / high) and with aggregates.  Results are
/// printed to stderr so they are visible even when stdout is redirected.
///
/// Usage:
///   ./zig-out/bin/groupby_bench <file.csv>
///
/// The CSV file must have the schema produced by generate_large_csv.zig:
///   id,name,age,city,salary,department
const std = @import("std");
const engine = @import("engine");
const parser = @import("parser");

const WARM_UP_RUNS: usize = 2;
const TIMED_RUNS: usize = 5;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        std.debug.print("Usage: groupby_bench <file.csv>\n", .{});
        std.process.exit(1);
    }

    const file_path = args[1];

    // Count rows for reporting
    const row_count = countRows(file_path) catch 0;
    const file_size_mb = fileSizeMb(file_path) catch 0.0;

    std.debug.print(
        \\
        \\════════════════════════════════════════════════════════════
        \\ csvq GROUP BY Benchmark
        \\════════════════════════════════════════════════════════════
        \\ File : {s}
        \\ Rows : {d}
        \\ Size : {d:.1} MB
        \\ Runs : {d} warm-up + {d} timed  (median reported)
        \\════════════════════════════════════════════════════════════
        \\
    , .{ file_path, row_count, file_size_mb, WARM_UP_RUNS, TIMED_RUNS });

    const queries = [_]BenchCase{
        // ── low cardinality (6 groups) ─────────────────────────────────
        .{
            .label = "Q1  GROUP BY department  (6 groups, low cardinality)",
            .sql = "SELECT department FROM '{f}' GROUP BY department",
        },
        // ── medium cardinality (8 groups) ─────────────────────────────
        .{
            .label = "Q2  GROUP BY city  (8 groups)",
            .sql = "SELECT city FROM '{f}' GROUP BY city",
        },
        // ── aggregates on low cardinality ──────────────────────────────
        .{
            .label = "Q3  GROUP BY department + COUNT(*)  (6 groups)",
            .sql = "SELECT department, COUNT(*) FROM '{f}' GROUP BY department",
        },
        // ── aggregates: SUM + AVG ──────────────────────────────────────
        .{
            .label = "Q4  GROUP BY city + COUNT(*) + SUM(salary)  (8 groups)",
            .sql = "SELECT city, COUNT(*), SUM(salary) FROM '{f}' GROUP BY city",
        },
        // ── WHERE filter then GROUP BY ─────────────────────────────────
        .{
            .label = "Q5  WHERE salary > 100000  +  GROUP BY department",
            .sql = "SELECT department, COUNT(*) FROM '{f}' WHERE salary > 100000 GROUP BY department",
        },
        // ── high cardinality (8 names × 6 depts = ~48 combos) ─────────
        .{
            .label = "Q6  GROUP BY name,department  (~48 groups, multi-column)",
            .sql = "SELECT name, department, COUNT(*) FROM '{f}' GROUP BY name, department",
        },
    };

    std.debug.print("{s:<65} {s:>10}  {s:>12}  {s:>12}\n", .{
        "Query", "Median ms", "Rows/sec", "MB/sec",
    });
    std.debug.print("{s}\n", .{"-" ** 104});

    for (queries) |q| {
        // Build the real SQL (substitute {f} with the file path)
        const sql = try std.mem.replaceOwned(u8, allocator, q.sql, "{f}", file_path);
        defer allocator.free(sql);

        var times: [WARM_UP_RUNS + TIMED_RUNS]u64 = undefined;

        for (0..WARM_UP_RUNS + TIMED_RUNS) |run| {
            const t = try timeQuery(allocator, sql);
            times[run] = t;
        }

        // Median of TIMED_RUNS
        var timed = times[WARM_UP_RUNS..];
        std.mem.sort(u64, timed, {}, std.sort.asc(u64));
        const median_ns = timed[TIMED_RUNS / 2];
        const median_ms = @as(f64, @floatFromInt(median_ns)) / 1_000_000.0;
        const rows_sec = @as(f64, @floatFromInt(row_count)) / (median_ms / 1000.0);
        const mb_sec = file_size_mb / (median_ms / 1000.0);

        std.debug.print("{s:<65} {d:>9.1}  {d:>12.0}  {d:>11.0}\n", .{
            q.label, median_ms, rows_sec, mb_sec,
        });
    }

    std.debug.print("\n", .{});
}

const BenchCase = struct {
    label: []const u8,
    sql: []const u8,
};

/// Run query once, discard output, return elapsed nanoseconds.
fn timeQuery(allocator: std.mem.Allocator, sql: []const u8) !u64 {
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    // Discard output — open /dev/null
    const sink = try std.fs.cwd().createFile("/dev/null", .{});
    defer sink.close();

    var timer = try std.time.Timer.start();
    try engine.execute(allocator, q, sink);
    return timer.read();
}

fn countRows(path: []const u8) !usize {
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();

    var buf: [65536]u8 = undefined;
    var count: usize = 0;
    while (true) {
        const n = try file.read(&buf);
        if (n == 0) break;
        for (buf[0..n]) |c| if (c == '\n') {
            count += 1;
        };
    }
    return if (count > 0) count - 1 else 0; // subtract header
}

fn fileSizeMb(path: []const u8) !f64 {
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();
    const st = try file.stat();
    return @as(f64, @floatFromInt(st.size)) / (1024.0 * 1024.0);
}
