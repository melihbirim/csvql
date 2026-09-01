/// WHERE-expression parser benchmark: times parser.parse() over a fixed
/// corpus of representative full queries (simple comparisons, AND/OR
/// chains, BETWEEN, LIKE/ILIKE, IN literal + subquery, scalar functions,
/// modulo, NOT, nested parens). Baseline for the tokenizer/recursive-descent
/// rewrite of the WHERE expression grammar — run before and after to catch
/// a perf regression, not just a correctness one.
///
/// Usage:
///   zig build bench-parser
const std = @import("std");
const parser = @import("parser");

const WARM_UP_ITERS: usize = 3;
const TIMED_ITERS: usize = 12;

const QUERIES = [_][]const u8{
    "SELECT id FROM 'f.csv' WHERE id = 5",
    "SELECT id FROM 'f.csv' WHERE age > 30 AND city = 'Austin'",
    "SELECT id FROM 'f.csv' WHERE age > 30 OR city = 'Austin'",
    "SELECT id FROM 'f.csv' WHERE age BETWEEN 20 AND 40",
    "SELECT id FROM 'f.csv' WHERE age BETWEEN 20 AND 40 OR city = 'Austin'",
    "SELECT id FROM 'f.csv' WHERE age BETWEEN 20 AND 40 AND city = 'Austin' AND salary > 50000",
    "SELECT id FROM 'f.csv' WHERE name LIKE 'A%'",
    "SELECT id FROM 'f.csv' WHERE name ILIKE '%smith%'",
    "SELECT id FROM 'f.csv' WHERE department IN ('Sales', 'Engineering', 'HR')",
    "SELECT id FROM 'f.csv' WHERE department NOT IN ('Sales', 'Engineering')",
    "SELECT id FROM 'f.csv' WHERE id IN (SELECT id FROM 'g.csv' WHERE department LIKE 'Op%')",
    "SELECT id FROM 'f.csv' WHERE id NOT IN (SELECT id FROM 'g.csv' WHERE age BETWEEN 20 AND 30)",
    "SELECT id FROM 'f.csv' WHERE NOT (age > 30)",
    "SELECT id FROM 'f.csv' WHERE id % 2 = 0",
    "SELECT id FROM 'f.csv' WHERE DATEDIFF('day', start_col, end_col) > 5",
    "SELECT id FROM 'f.csv' WHERE age IS NULL OR city IS NOT NULL",
    "SELECT id FROM 'f.csv' WHERE (age > 20 AND age < 40) OR (city = 'Austin' AND department = 'Sales')",
    "SELECT id FROM 'f.csv' WHERE age > 20 AND age < 40 AND city != 'Austin' AND department = 'Sales' OR salary > 90000",
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print(
        \\
        \\=================================================================
        \\ WHERE-expression parser benchmark
        \\=================================================================
        \\ Corpus : {d} queries, {d} warm-up + {d} timed passes
        \\=================================================================
        \\
    , .{ QUERIES.len, WARM_UP_ITERS, TIMED_ITERS });

    for (0..WARM_UP_ITERS) |_| try runPass(allocator);

    var times = std.ArrayListUnmanaged(f64){};
    defer times.deinit(allocator);
    for (0..TIMED_ITERS) |_| {
        const t0 = std.time.nanoTimestamp();
        try runPass(allocator);
        const t1 = std.time.nanoTimestamp();
        try times.append(allocator, @as(f64, @floatFromInt(t1 - t0)) / 1_000_000.0);
    }

    std.mem.sort(f64, times.items, {}, std.sort.asc(f64));
    const median = times.items[times.items.len / 2];
    var sum: f64 = 0;
    for (times.items) |t| sum += t;
    const mean = sum / @as(f64, @floatFromInt(times.items.len));

    std.debug.print("median: {d:.4} ms/pass ({d:.2} us/query)\n", .{ median, (median * 1000.0) / @as(f64, @floatFromInt(QUERIES.len)) });
    std.debug.print("mean:   {d:.4} ms/pass\n", .{mean});
    std.debug.print("min:    {d:.4} ms/pass\n", .{times.items[0]});
    std.debug.print("max:    {d:.4} ms/pass\n", .{times.items[times.items.len - 1]});
}

fn runPass(allocator: std.mem.Allocator) !void {
    for (QUERIES) |q| {
        var query = try parser.parse(allocator, q);
        query.deinit();
    }
}
