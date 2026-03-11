const std = @import("std");
const parser = @import("parser.zig");
const simple_parser = @import("simple_parser.zig");
const engine = @import("engine.zig");
const options_mod = @import("options.zig");
const Allocator = std.mem.Allocator;

const version = "0.4.0";

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
    \\  -h, --help              Show this help
    \\  -v, --version           Show version
    \\  --no-header             Suppress header row in output
    \\  -d, --delimiter <char>  Field delimiter (default: ',')  e.g. -d '\t' for TSV
    \\  --json                  Output results as a JSON array of objects
    \\  --jsonl                 Output results as newline-delimited JSON (NDJSON)
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

    // Check for early-exit flags first (before any filtering)
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

    // Strip --no-header / -d / --delimiter flags; collect remainder for query parsing.
    var opts = options_mod.Options{};
    var clean_args = try std.ArrayList([]const u8).initCapacity(allocator, args.len);
    defer clean_args.deinit(allocator);
    try clean_args.append(allocator, args[0]); // keep program name at [0]

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--no-header")) {
            opts.no_header = true;
        } else if (std.mem.eql(u8, arg, "--json")) {
            opts.format = .json;
        } else if (std.mem.eql(u8, arg, "--jsonl")) {
            opts.format = .jsonl;
        } else if (std.mem.eql(u8, arg, "-d") or std.mem.eql(u8, arg, "--delimiter")) {
            i += 1;
            if (i >= args.len) {
                try stderr_file.writeAll("error: --delimiter requires a character argument\n");
                std.process.exit(1);
            }
            opts.delimiter = parseDelimiter(args[i]);
        } else {
            try clean_args.append(allocator, arg);
        }
    }

    // Detect query mode: simple vs SQL
    var query = if (clean_args.items.len > 1 and !isSQL(clean_args.items[1])) blk: {
        // Simple mode: csvql file.csv [columns] [where] [limit] [orderby]
        break :blk simple_parser.parseSimple(allocator, clean_args.items[1..]) catch |err| {
            std.debug.print("error: {}\n", .{err});
            std.debug.print("\nRun 'csvql --help' for usage information.\n", .{});
            std.process.exit(1);
        };
    } else blk: {
        // SQL mode: csvql "SELECT ..."
        const query_text = try getQueryFromArgs(allocator, clean_args.items);
        defer allocator.free(query_text);

        break :blk parser.parse(allocator, query_text) catch |err| {
            std.debug.print("SQL parse error: {}\n", .{err});
            std.debug.print("\nRun 'csvql --help' for usage information.\n", .{});
            std.process.exit(1);
        };
    };
    defer query.deinit();

    // Execute query
    engine.execute(allocator, query, stdout_file, opts) catch |err| {
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

/// Join clean_args[1..] into a single query string, or read from stdin.
fn getQueryFromArgs(allocator: Allocator, clean_args: []const []const u8) ![]u8 {
    if (clean_args.len > 1) {
        return try std.mem.join(allocator, " ", clean_args[1..]);
    }

    // Read query from stdin
    const stdin = std.fs.File{ .handle = std.posix.STDIN_FILENO };
    const query = try stdin.readToEndAlloc(allocator, 1024 * 1024);
    const trimmed = std.mem.trim(u8, query, &std.ascii.whitespace);

    if (trimmed.len == 0) {
        const stderr = std.fs.File{ .handle = std.posix.STDERR_FILENO };
        try stderr.writeAll("error: no query provided\n\nRun 'csvql --help' for usage information.\n");
        std.process.exit(1);
    }

    const result = try allocator.dupe(u8, trimmed);
    allocator.free(query);
    return result;
}

/// Parse a delimiter argument, supporting \t, \n, \r escapes.
fn parseDelimiter(arg: []const u8) u8 {
    if (arg.len == 1) return arg[0];
    // Handle 2-char escape sequences passed as literal strings (e.g. shell: -d '\t')
    if (arg.len == 2 and arg[0] == '\\') {
        return switch (arg[1]) {
            't' => '\t',
            'n' => '\n',
            'r' => '\r',
            '|' => '|',
            ';' => ';',
            else => arg[1],
        };
    }
    // Fallback: use first byte
    return arg[0];
}
