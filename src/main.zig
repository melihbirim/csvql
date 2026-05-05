const std = @import("std");
const builtin = @import("builtin");
const parser = @import("parser.zig");
const simple_parser = @import("simple_parser.zig");
const engine = @import("engine.zig");
const options_mod = @import("options.zig");
const mcp = @import("mcp.zig");
const zigtable = @import("zigtable");
const Allocator = std.mem.Allocator;

const version = "1.5.0";

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
    \\           LIKE / ILIKE pattern matching: name LIKE 'A%'
    \\           IN list: status IN ('active', 'pending')
    \\           BETWEEN range: age BETWEEN 18 AND 65
    \\           IS NULL / IS NOT NULL
    \\  GROUP BY column grouping with aggregate functions
    \\  HAVING   post-aggregation filter
    \\  ORDER BY column ASC|DESC (positional ORDER BY 1 supported)
    \\  LIMIT    number of rows
    \\  AS       column aliases: SELECT name AS n
    \\  DISTINCT deduplicate output rows
    \\  SELECT   aggregate functions: COUNT, SUM, AVG, MIN, MAX, COUNT(DISTINCT)
    \\  JOIN     inner join: SELECT ... FROM 'a.csv' JOIN 'b.csv' ON a.id = b.id
    \\
    \\NOT SUPPORTED:
    \\  subqueries, multiple ORDER BY columns,
    \\  UNION, INSERT/UPDATE/DELETE
    \\
    \\OPTIONS:
    \\  -h, --help              Show this help
    \\  -v, --version           Show version
    \\  --no-header             Suppress header row in output
    \\  -d, --delimiter <char>  Field delimiter (default: ',')  e.g. -d '\t' for TSV
    \\  --json                  Output results as a JSON array of objects
    \\  --jsonl                 Output results as newline-delimited JSON (NDJSON)
    \\  --mcp                   Start as an MCP (Model Context Protocol) server
    \\                          Exposes csv_query, csv_schema, csv_list tools    \  --schema <file>         Show column names, types, row count, and file size    \\
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
        if (std.mem.eql(u8, args[1], "--mcp")) {
            try mcp.run(allocator);
            return;
        }
        if (std.mem.eql(u8, args[1], "--schema")) {
            if (args.len < 3) {
                try stderr_file.writeAll("error: --schema requires a file path argument\n");
                std.process.exit(1);
            }
            try runSchema(allocator, args[2], stdout_file, stderr_file);
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
        } else if (std.mem.eql(u8, arg, "--table")) {
            opts.table_mode = .on;
        } else if (std.mem.eql(u8, arg, "--wrap")) {
            opts.wrap_cells = true;
        } else if (std.mem.eql(u8, arg, "--no-table")) {
            opts.table_mode = .off;
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

    // Determine whether to render as a table
    const use_table = opts.format == .csv and !opts.no_header and switch (opts.table_mode) {
        .on => true,
        .off => false,
        .auto => std.posix.isatty(std.posix.STDOUT_FILENO),
    };

    if (use_table) {
        renderTableOutput(allocator, query, stdout_file, opts) catch |err| {
            std.debug.print("execution error: {}\n", .{err});
            std.process.exit(1);
        };
    } else {
        engine.execute(allocator, query, stdout_file, opts) catch |err| {
            std.debug.print("execution error: {}\n", .{err});
            std.process.exit(1);
        };
    }
}

/// Run the engine into a temp file, read back, parse CSV, render via zigtable.
fn renderTableOutput(allocator: Allocator, query: parser.Query, stdout_file: std.fs.File, opts: options_mod.Options) !void {
    // Write CSV to a temp file
    var tmp_buf: [128]u8 = undefined;
    const tmp_path = try std.fmt.bufPrint(&tmp_buf, "/tmp/csvql_{d}.tmp", .{std.time.milliTimestamp()});
    {
        const tmp_file = try std.fs.createFileAbsolute(tmp_path, .{ .truncate = true });
        defer tmp_file.close();
        try engine.execute(allocator, query, tmp_file, opts);
    }
    defer std.fs.deleteFileAbsolute(tmp_path) catch {};

    // Read back
    const tmp_read = try std.fs.openFileAbsolute(tmp_path, .{});
    defer tmp_read.close();
    const csv_data = try tmp_read.readToEndAlloc(allocator, 4 * 1024 * 1024 * 1024);
    defer allocator.free(csv_data);

    // Use a quote-aware record iterator so multiline quoted fields are not split prematurely.
    var records = CsvRecordIterator.init(csv_data);

    // Parse header
    const header_record = records.next() orelse return;
    var col_list: std.ArrayList(zigtable.Column) = .{};
    defer col_list.deinit(allocator);
    var hdr_it = CsvRowIterator.init(header_record, opts.delimiter);
    while (hdr_it.next()) |col_name| {
        try col_list.append(allocator, .{ .name = col_name });
    }
    if (col_list.items.len == 0) return;

    var table = zigtable.Table.init(allocator, col_list.items);
    defer table.deinit();
    table.terminal_width = getTerminalWidth();
    table.wrap_cells = opts.wrap_cells;

    // Add data rows; sanitize embedded newlines so the table renders correctly.
    var row_fields: std.ArrayList([]const u8) = .{};
    defer row_fields.deinit(allocator);
    // Track sanitized allocations — must outlive table.render().
    var owned: std.ArrayList([]u8) = .{};
    defer {
        for (owned.items) |s| allocator.free(s);
        owned.deinit(allocator);
    }
    while (records.next()) |raw_record| {
        row_fields.clearRetainingCapacity();
        var row_it = CsvRowIterator.init(raw_record, opts.delimiter);
        while (row_it.next()) |field| {
            if (try sanitizeField(allocator, field)) |sanitized| {
                try owned.append(allocator, sanitized);
                try row_fields.append(allocator, sanitized);
            } else {
                try row_fields.append(allocator, field);
            }
        }
        try table.addRow(row_fields.items);
    }

    // Buffer the table output, then write to stdout in one shot.
    var table_buf: std.ArrayList(u8) = .{};
    defer table_buf.deinit(allocator);

    const BufWriter = struct {
        buf: *std.ArrayList(u8),
        alloc: Allocator,
        const WriteError = error{OutOfMemory};
        pub fn write(self: @This(), data: []const u8) WriteError!usize {
            try self.buf.appendSlice(self.alloc, data);
            return data.len;
        }
    };
    try table.render(BufWriter{ .buf = &table_buf, .alloc = allocator });
    try stdout_file.writeAll(table_buf.items);
}

/// Return the current terminal width in columns.
/// Checks $COLUMNS env var first, then TIOCGWINSZ via libc ioctl, then falls back to 80.
fn getTerminalWidth() usize {
    if (std.process.getEnvVarOwned(std.heap.page_allocator, "COLUMNS") catch null) |s| {
        defer std.heap.page_allocator.free(s);
        if (std.fmt.parseInt(usize, std.mem.trim(u8, s, " \t"), 10) catch null) |n| {
            if (n >= 20) return n;
        }
    }
    // TIOCGWINSZ via libc (works on macOS and Linux; we always link libc).
    const WinSize = extern struct { ws_row: u16, ws_col: u16, ws_xpixel: u16, ws_ypixel: u16 };
    const tiocgwinsz: c_uint = switch (builtin.os.tag) {
        .macos, .ios, .tvos, .watchos => 0x40087468,
        else => 0x5413, // Linux / FreeBSD
    };
    var ws = std.mem.zeroes(WinSize);
    _ = std.c.ioctl(std.posix.STDOUT_FILENO, tiocgwinsz, @intFromPtr(&ws));
    if (ws.ws_col >= 20) return @as(usize, ws.ws_col);
    return 80;
}

/// Analyse a CSV file and print a schema table: column index, name, inferred type,
/// non-empty count, and empty count.  Also prints a summary header line.
fn runSchema(allocator: Allocator, raw_path: []const u8, stdout_file: std.fs.File, stderr_file: std.fs.File) !void {
    // Strip surrounding quotes that users sometimes include in shell invocations.
    const file_path = blk: {
        var p = raw_path;
        if (p.len >= 2 and ((p[0] == '\'' and p[p.len - 1] == '\'') or (p[0] == '"' and p[p.len - 1] == '"'))) {
            p = p[1 .. p.len - 1];
        }
        break :blk p;
    };

    // Stat the file for size.
    const stat = std.fs.cwd().statFile(file_path) catch |err| {
        const msg = try std.fmt.allocPrint(allocator, "error: cannot open '{s}': {}\n", .{ file_path, err });
        defer allocator.free(msg);
        try stderr_file.writeAll(msg);
        std.process.exit(1);
    };
    const file_size = stat.size;

    // Read the whole file.
    const file = try std.fs.cwd().openFile(file_path, .{});
    defer file.close();
    const csv_data = try file.readToEndAlloc(allocator, 4 * 1024 * 1024 * 1024);
    defer allocator.free(csv_data);

    var records = CsvRecordIterator.init(csv_data);

    // --- Parse header ---
    const header_record = records.next() orelse {
        try stderr_file.writeAll("error: file is empty\n");
        std.process.exit(1);
    };
    var col_names = std.ArrayList([]const u8){};
    defer col_names.deinit(allocator);
    var hdr_it = CsvRowIterator.init(header_record, ',');
    while (hdr_it.next()) |name| try col_names.append(allocator, name);

    const n_cols = col_names.items.len;
    if (n_cols == 0) {
        try stderr_file.writeAll("error: no columns found\n");
        std.process.exit(1);
    }

    // Per-column accumulators.
    const ColStats = struct {
        non_empty: u64 = 0,
        empty: u64 = 0,
        all_int: bool = true,
        all_float: bool = true,
    };
    var stats = try allocator.alloc(ColStats, n_cols);
    defer allocator.free(stats);
    for (stats) |*s| s.* = .{};

    var total_rows: u64 = 0;

    while (records.next()) |raw_record| {
        total_rows += 1;
        var ci: usize = 0;
        var row_it = CsvRowIterator.init(raw_record, ',');
        while (row_it.next()) |field| : (ci += 1) {
            if (ci >= n_cols) break;
            const trimmed = std.mem.trim(u8, field, " \t\r");
            if (trimmed.len == 0) {
                stats[ci].empty += 1;
            } else {
                stats[ci].non_empty += 1;
                if (stats[ci].all_int) {
                    _ = std.fmt.parseInt(i64, trimmed, 10) catch {
                        stats[ci].all_int = false;
                    };
                }
                if (stats[ci].all_float) {
                    _ = std.fmt.parseFloat(f64, trimmed) catch {
                        stats[ci].all_float = false;
                    };
                }
            }
        }
        // Fill missing columns as empty.
        while (ci < n_cols) : (ci += 1) {
            stats[ci].empty += 1;
        }
    }

    // --- Format file size ---
    var size_buf: [32]u8 = undefined;
    const size_str: []const u8 = blk: {
        if (file_size >= 1024 * 1024 * 1024) {
            break :blk try std.fmt.bufPrint(&size_buf, "{d:.1} GB", .{@as(f64, @floatFromInt(file_size)) / (1024 * 1024 * 1024)});
        } else if (file_size >= 1024 * 1024) {
            break :blk try std.fmt.bufPrint(&size_buf, "{d:.1} MB", .{@as(f64, @floatFromInt(file_size)) / (1024 * 1024)});
        } else if (file_size >= 1024) {
            break :blk try std.fmt.bufPrint(&size_buf, "{d:.1} KB", .{@as(f64, @floatFromInt(file_size)) / 1024});
        } else {
            break :blk try std.fmt.bufPrint(&size_buf, "{d} B", .{file_size});
        }
    };

    // --- Summary header line ---
    const summary = try std.fmt.allocPrint(allocator, "File: {s}  |  Rows: {d}  |  Columns: {d}  |  Size: {s}\n", .{
        file_path, total_rows, n_cols, size_str,
    });
    defer allocator.free(summary);
    try stdout_file.writeAll(summary);

    // --- Build schema table ---
    const schema_cols = [_]zigtable.Column{
        .{ .name = "#" },
        .{ .name = "Column" },
        .{ .name = "Type" },
        .{ .name = "Non-Empty" },
        .{ .name = "Empty" },
    };
    var table = zigtable.Table.init(allocator, &schema_cols);
    defer table.deinit();

    // Reusable buffers for row fields (all owned, freed after render).
    var owned_bufs = std.ArrayList([]u8){};
    defer {
        for (owned_bufs.items) |b| allocator.free(b);
        owned_bufs.deinit(allocator);
    }

    for (col_names.items, 0..) |col_name, i| {
        const s = &stats[i];
        const type_str: []const u8 = if (s.non_empty == 0)
            "TEXT"
        else if (s.all_int)
            "INTEGER"
        else if (s.all_float)
            "FLOAT"
        else
            "TEXT";

        const idx_s = try std.fmt.allocPrint(allocator, "{d}", .{i + 1});
        try owned_bufs.append(allocator, idx_s);
        const ne_s = try std.fmt.allocPrint(allocator, "{d}", .{s.non_empty});
        try owned_bufs.append(allocator, ne_s);
        const em_s = try std.fmt.allocPrint(allocator, "{d}", .{s.empty});
        try owned_bufs.append(allocator, em_s);

        try table.addRow(&.{ idx_s, col_name, type_str, ne_s, em_s });
    }

    // Render.
    const BufWriter = struct {
        buf: *std.ArrayList(u8),
        alloc: Allocator,
        const WriteError = error{OutOfMemory};
        pub fn write(self: @This(), data: []const u8) WriteError!usize {
            try self.buf.appendSlice(self.alloc, data);
            return data.len;
        }
    };
    var out_buf = std.ArrayList(u8){};
    defer out_buf.deinit(allocator);
    try table.render(BufWriter{ .buf = &out_buf, .alloc = allocator });
    try stdout_file.writeAll(out_buf.items);
}

/// Replace embedded '\n'/'\r' in a field value with the literal two-character string "\n".
/// Returns an owned slice only when replacement is needed; null means use the original.
fn sanitizeField(allocator: Allocator, field: []const u8) !?[]u8 {
    var needs = false;
    for (field) |c| {
        if (c == '\n' or c == '\r') {
            needs = true;
            break;
        }
    }
    if (!needs) return null;
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);
    for (field) |c| {
        switch (c) {
            '\n' => try buf.appendSlice(allocator, "\\n"),
            '\r' => {}, // drop bare \r
            else => try buf.append(allocator, c),
        }
    }
    return try buf.toOwnedSlice(allocator);
}

/// Quote-aware CSV record iterator over an in-memory buffer.
/// Treats '\n' inside a quoted field as part of the record, not a boundary.
/// Empty lines (blank records) are skipped automatically.
const CsvRecordIterator = struct {
    data: []const u8,
    pos: usize,

    fn init(data: []const u8) CsvRecordIterator {
        return .{ .data = data, .pos = 0 };
    }

    fn next(self: *CsvRecordIterator) ?[]const u8 {
        while (self.pos < self.data.len) {
            const start = self.pos;
            var in_quote = false;
            while (self.pos < self.data.len) {
                const c = self.data[self.pos];
                if (c == '"') {
                    if (in_quote) {
                        // "" escaped quote — stay inside
                        if (self.pos + 1 < self.data.len and self.data[self.pos + 1] == '"') {
                            self.pos += 2;
                            continue;
                        }
                        in_quote = false;
                    } else {
                        in_quote = true;
                    }
                    self.pos += 1;
                } else if (c == '\n' and !in_quote) {
                    var end = self.pos;
                    if (end > start and self.data[end - 1] == '\r') end -= 1;
                    self.pos += 1;
                    const record = self.data[start..end];
                    if (record.len > 0) return record;
                    break; // empty line — restart outer loop
                } else {
                    self.pos += 1;
                }
            }
            // End of data without trailing newline
            if (self.pos > start) {
                const record = self.data[start..self.pos];
                self.pos = self.data.len;
                return record;
            }
        }
        return null;
    }
};

/// Minimal CSV field iterator: handles quoted and unquoted fields.
/// Returns slices into the original line (quotes stripped, no unescape).
const CsvRowIterator = struct {
    line: []const u8,
    pos: usize,
    delimiter: u8,
    done: bool = false,

    fn init(line: []const u8, delimiter: u8) CsvRowIterator {
        return .{ .line = line, .pos = 0, .delimiter = delimiter };
    }

    fn next(self: *CsvRowIterator) ?[]const u8 {
        if (self.done) return null;
        if (self.pos > self.line.len) return null;
        // Trailing empty field after a delimiter at end
        if (self.pos == self.line.len) {
            self.done = true;
            return "";
        }
        if (self.line[self.pos] == '"') {
            // Quoted field
            self.pos += 1;
            const start = self.pos;
            while (self.pos < self.line.len) {
                if (self.line[self.pos] == '"') {
                    if (self.pos + 1 < self.line.len and self.line[self.pos + 1] == '"') {
                        self.pos += 2; // skip escaped quote
                    } else {
                        const field = self.line[start..self.pos];
                        self.pos += 1; // skip closing quote
                        if (self.pos < self.line.len and self.line[self.pos] == self.delimiter) {
                            self.pos += 1;
                        } else {
                            self.done = true;
                        }
                        return field;
                    }
                } else {
                    self.pos += 1;
                }
            }
            self.done = true;
            return self.line[start..self.pos];
        } else {
            // Unquoted field
            const start = self.pos;
            while (self.pos < self.line.len and self.line[self.pos] != self.delimiter) {
                self.pos += 1;
            }
            const field = self.line[start..self.pos];
            if (self.pos < self.line.len) {
                self.pos += 1; // skip delimiter
            } else {
                self.done = true;
            }
            return field;
        }
    }
};

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

    // If stdin is a TTY (no pipe), nothing will ever come — show help instead.
    if (std.posix.isatty(std.posix.STDIN_FILENO)) {
        const stdout_file = std.fs.File{ .handle = std.posix.STDOUT_FILENO };
        try stdout_file.writeAll(help_text);
        std.process.exit(0);
    }

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
