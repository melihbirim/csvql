const std = @import("std");
const parser = @import("parser.zig");
const simple_parser = @import("simple_parser.zig");
const engine = @import("engine.zig");
const Allocator = std.mem.Allocator;

const version = "0.3.0";

const help_text =
    \\csvql — the world's fastest CSV query engine
    \\
    \\USAGE:
    \\  csvql <file> [columns] [where] [limit] [orderby]
    \\  csvql "SELECT ... FROM 'file.csv' ..."
    \\  cat file.csv | csvql "SELECT ... FROM '-' ..."
    \\
    \\SQL MODE:
    \\  csvql "SELECT name, age FROM 'data.csv'"
    \\  csvql "SELECT * FROM 'data.csv' WHERE age > 30"
    \\  csvql "SELECT name FROM 'data.csv' WHERE age > 30 ORDER BY name ASC"
    \\  csvql "SELECT * FROM 'data.csv' ORDER BY salary DESC LIMIT 10"
    \\  csvql "SELECT * FROM 'data.csv' WHERE age > 25 AND city = 'NYC'"
    \\  csvql "SELECT * FROM 'data.csv' WHERE status = 'active' OR score >= 90"
    \\
    \\SIMPLE MODE:
    \\  csvql data.csv                                    # all columns, default limit 10
    \\  csvql data.csv "name,age,city"                    # select columns
    \\  csvql data.csv "*" "age>30"                       # WHERE filter
    \\  csvql data.csv "name,salary" "salary>0" 10 "salary:desc"
    \\  csvql data.csv "*" "" 0 "name:asc"               # 0 = no limit
    \\
    \\PIPE MODE (use '-' as filename):
    \\  cat data.csv | csvql "SELECT name FROM '-' WHERE age > 25"
    \\  tail -f logs.csv | csvql "SELECT * FROM '-' WHERE level = 'ERROR'"
    \\
    \\SUPPORTED SQL:
    \\  SELECT   column list or *
    \\  FROM     'file.csv' (single-quoted path)
    \\  WHERE    comparisons with =, !=, >, >=, <, <=
    \\           combine with AND, OR, NOT, parentheses
    \\           string values: city = 'NYC'
    \\           numeric values: age > 30
    \\  ORDER BY column ASC|DESC
    \\  LIMIT    number of rows
    \\
    \\NOT SUPPORTED:
    \\  JOIN, GROUP BY, HAVING, DISTINCT, subqueries,
    \\  aggregate functions (COUNT, SUM, AVG, etc.),
    \\  multiple ORDER BY columns, LIKE, IN, BETWEEN,
    \\  aliases (AS), UNION, INSERT/UPDATE/DELETE
    \\
    \\OPTIONS:
    \\  -h, --help       Show this help
    \\  -v, --version    Show version
    \\
    \\EXAMPLES:
    \\  csvql "SELECT * FROM 'users.csv' WHERE age >= 18 LIMIT 100"
    \\  csvql "SELECT * FROM 'data.csv' WHERE status = 'active'" > out.csv
    \\  csvql "SELECT email FROM 'users.csv'" | wc -l
    \\
;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Get command line arguments
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const stdout_file = std.fs.File{ .handle = std.posix.STDOUT_FILENO };
    const stderr_file = std.fs.File{ .handle = std.posix.STDERR_FILENO };

    // Check for flags
    if (args.len >= 2) {
        if (std.mem.eql(u8, args[1], "--version") or std.mem.eql(u8, args[1], "-v")) {
            try stderr_file.writeAll("csvql " ++ version ++ "\n");
            return;
        }
        if (std.mem.eql(u8, args[1], "--help") or std.mem.eql(u8, args[1], "-h")) {
            try stdout_file.writeAll(help_text);
            return;
        }
    }

    // Detect query mode: simple vs SQL
    var query = if (args.len > 1 and !isSQL(args[1])) blk: {
        // Simple mode: csvql file.csv [columns] [where] [limit] [orderby]
        const simple_args = args[1..];
        break :blk simple_parser.parseSimple(allocator, simple_args) catch |err| {
            std.debug.print("error: {}\n", .{err});
            std.debug.print("\nRun 'csvql --help' for usage information.\n", .{});
            std.process.exit(1);
        };
    } else blk: {
        // SQL mode: csvql "SELECT ..."
        const query_text = try getQueryFromArgsOrStdin(allocator, args);
        defer allocator.free(query_text);

        break :blk parser.parse(allocator, query_text) catch |err| {
            std.debug.print("SQL parse error: {}\n", .{err});
            std.debug.print("\nRun 'csvql --help' for usage information.\n", .{});
            std.process.exit(1);
        };
    };
    defer query.deinit();

    // Execute query
    engine.execute(allocator, query, stdout_file) catch |err| {
        std.debug.print("execution error: {}\n", .{err});
        std.process.exit(1);
    };
}

/// Detect if argument is SQL query (starts with SELECT)
fn isSQL(arg: []const u8) bool {
    const trimmed = std.mem.trim(u8, arg, &std.ascii.whitespace);
    if (trimmed.len < 6) return false;

    // Check if starts with SELECT (case-insensitive)
    var upper_buf: [6]u8 = undefined;
    const upper = std.ascii.upperString(&upper_buf, trimmed[0..6]);
    return std.mem.eql(u8, upper, "SELECT");
}

fn getQueryFromArgsOrStdin(allocator: Allocator, args: [][:0]u8) ![]u8 {
    // If query provided as argument
    if (args.len > 1) {
        // Join all args after program name
        var query_parts = try std.ArrayList([]const u8).initCapacity(allocator, args.len - 1);
        defer query_parts.deinit(allocator);

        for (args[1..]) |arg| {
            try query_parts.append(allocator, arg);
        }

        return try std.mem.join(allocator, " ", query_parts.items);
    }

    // Read query from stdin
    const stdin = std.fs.File{ .handle = std.posix.STDIN_FILENO };
    const query = try stdin.readToEndAlloc(allocator, 1024 * 1024); // Max 1MB query
    const trimmed = std.mem.trim(u8, query, &std.ascii.whitespace);

    if (trimmed.len == 0) {
        const stderr = std.fs.File{ .handle = std.posix.STDERR_FILENO };
        try stderr.writeAll("error: no query provided\n\nRun 'csvql --help' for usage information.\n");
        std.process.exit(1);
    }

    // Return a copy of the trimmed string
    const result = try allocator.dupe(u8, trimmed);
    allocator.free(query);
    return result;
}
