/// GROUP BY benchmark harness.
///
/// Measures GROUP BY query performance across six query shapes:
///   Q1  low-cardinality GROUP BY (6 groups)
///   Q2  medium-cardinality GROUP BY (8 groups)
///   Q3  GROUP BY + COUNT(*)
///   Q4  GROUP BY + COUNT(*) + SUM(salary)
///   Q5  WHERE + GROUP BY + COUNT(*)
///   Q6  multi-column GROUP BY (~48 groups)
///
/// Usage:
///   zig build bench-groupby -- large_test.csv
///
/// The CSV file must have the schema from generate_large_csv.zig:
///   id,name,age,city,salary,department
const std = @import("std");
const engine = @import("engine");

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
    const row_count = countRows(file_path) catch 0;
    const file_size_mb = fileSizeMb(file_path) catch 0.0;

    std.debug.print(
        \\
        \\=================================================================
        \\ csvq GROUP BY Benchmark
        \\=================================================================
        \\ File : {s}
        \\ Rows : {d}
        \\ Size : {d:.1} MB
        \\ Runs : {d} warm-up + {d} timed  (median reported)
        \\=================================================================
        \\
    , .{ file_path, row_count, file_size_mb, WARM_UP_RUNS, TIMED_RUNS });

    const queries = [_]BenchCase{
        .{
            .label = "Q1  GROUP BY department           (6 groups)",
            .sql = "SELECT department FROM '{f}' GROUP BY department",
        },
        .{
            .label = "Q2  GROUP BY city                 (8 groups)",
            .sql = "SELECT city FROM '{f}' GROUP BY city",
        },
        .{
            .label = "Q3  GROUP BY department + COUNT(*)",
            .sql = "SELECT department, COUNT(*) FROM '{f}' GROUP BY department",
        },
        .{
            .label = "Q4  GROUP BY city + COUNT(*) + SUM(salary)",
            .sql = "SELECT city, COUNT(*), SUM(salary) FROM '{f}' GROUP BY city",
        },
        .{
            .label = "Q5  WHERE salary>100000 + GROUP BY department",
            .sql = "SELECT department, COUNT(*) FROM '{f}' WHERE salary > 100000 GROUP BY department",
        },
        .{
            .label = "Q6  GROUP BY name,department       (~48 groups)",
            .sql = "SELECT name, department, COUNT(*) FROM '{f}' GROUP BY name, department",
        },
    };

    std.debug.print("{s:<55} {s:>10}  {s:>12}  {s:>11}\n", .{
        "Query", "Median ms", "Rows/sec", "MB/sec",
    });
    std.debug.print("{s}\n", .{"-" ** 93});

    for (queries) |q| {
        const sql = try std.mem.replaceOwned(u8, allocator, q.sql, "{f}", file_path);
        defer allocator.free(sql);

        var times: [WARM_UP_RUNS + TIMED_RUNS]u64 = undefined;
        for (0..WARM_UP_RUNS + TIMED_RUNS) |run| {
            times[run] = try timeQuery(allocator, sql);
        }

        const timed = times[WARM_UP_RUNS..];
        std.mem.sort(u64, timed, {}, std.sort.asc(u64));
        const median_ns = timed[TIMED_RUNS / 2];
        const median_ms = @as(f64, @floatFromInt(median_ns)) / 1_000_000.0;
        const rows_sec = @as(f64, @floatFromInt(row_count)) / (median_ms / 1000.0);
        const mb_sec = file_size_mb / (median_ms / 1000.0);

        std.debug.print("{s:<55} {d:>9.1}  {d:>12.0}  {d:>10.0}\n", .{
            q.label, median_ms, rows_sec, mb_sec,
        });
    }
    std.debug.print("\n", .{});
}

const BenchCase = struct {
    label: []const u8,
    sql: []const u8,
};

fn timeQuery(allocator: std.mem.Allocator, sql: []const u8) !u64 {
    var q = try engine.parseQuery(allocator, sql);
    defer q.deinit();

    const sink = try std.fs.cwd().createFile("/dev/null", .{});
    defer sink.close();

    var timer = try std.time.Timer.start();
    try engine.execute(allocator, q, sink, .{});
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
    return if (count > 0) count - 1 else 0;
}

fn fileSizeMb(path: []const u8) !f64 {
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();
    return @as(f64, @floatFromInt((try file.stat()).size)) / (1024.0 * 1024.0);
}
