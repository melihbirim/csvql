//! MCP (Model Context Protocol) server for csvql.
//! Implements the stdio transport: newline-delimited JSON-RPC 2.0.
//!
//! Exposed tools:
//!   csv_query(sql)           — execute any SQL, return JSON results
//!   csv_schema(file)         — column names + sample rows
//!   csv_list(directory?)     — list CSV files in a directory
//!
//! Usage:  csvql --mcp
//! Config (Claude Desktop example):
//!   ~/.config/claude/claude_desktop_config.json
//!   { "mcpServers": { "csvql": { "command": "/path/to/csvql", "args": ["--mcp"] } } }

const std = @import("std");
const parser = @import("parser.zig");
const engine = @import("engine.zig");
const options_mod = @import("options.zig");
const Allocator = std.mem.Allocator;

// Zig 0.15: std.ArrayList is unmanaged (allocator per-call).
// std.array_list.Managed is the old managed type with stored allocator.
const ManagedList = std.array_list.Managed(u8);

// ---------------------------------------------------------------------------
// Tool definitions
// ---------------------------------------------------------------------------

const INIT_RESULT =
    "{\"protocolVersion\":\"2024-11-05\",\"capabilities\":{\"tools\":{}},\"serverInfo\":{\"name\":\"csvql\",\"version\":\"0.6.0\"}}";

// Multiline for readability; flattened to single line before sending.
const TOOLS_JSON =
    \\{"tools":[
    \\{"name":"csv_query","description":"Execute a SQL query against CSV files and return results as JSON. Supports SELECT, WHERE, GROUP BY, ORDER BY, LIMIT, JOIN, COUNT/SUM/AVG/MIN/MAX, DISTINCT, LIKE. File paths must be single-quoted in FROM. Use LIMIT to keep results compact. Example: SELECT dept, COUNT(*) FROM 'data.csv' GROUP BY dept","inputSchema":{"type":"object","properties":{"sql":{"type":"string","description":"SQL query with single-quoted file paths in FROM clause"}},"required":["sql"]}},
    \\{"name":"csv_schema","description":"Show column names and a few sample rows from a CSV file. Call this before csv_query to understand column names and data types.","inputSchema":{"type":"object","properties":{"file":{"type":"string","description":"Path to the CSV file"}},"required":["file"]}},
    \\{"name":"csv_list","description":"List CSV files in a directory.","inputSchema":{"type":"object","properties":{"directory":{"type":"string","description":"Directory to search (defaults to current working directory)"}}}}
    \\]}
;

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

pub fn run(allocator: Allocator) !void {
    const stdin = std.fs.File{ .handle = std.posix.STDIN_FILENO };
    const stdout = std.fs.File{ .handle = std.posix.STDOUT_FILENO };

    const r = stdin.reader();

    var line_buf = ManagedList.init(allocator);
    defer line_buf.deinit();

    var resp_buf = ManagedList.init(allocator);
    defer resp_buf.deinit();

    while (true) {
        line_buf.clearRetainingCapacity();
        r.streamUntilDelimiter(line_buf.writer(), '\n', 64 * 1024 * 1024) catch |err| {
            if (err == error.EndOfStream) break;
            return err;
        };

        const line = std.mem.trim(u8, line_buf.items, "\r\n\t ");
        if (line.len == 0) continue;

        resp_buf.clearRetainingCapacity();
        processMessage(allocator, line, &resp_buf) catch |err| {
            std.debug.print("[csvql-mcp] error processing message: {s}\n", .{@errorName(err)});
        };
        if (resp_buf.items.len > 0) {
            try stdout.writeAll(resp_buf.items);
        }
    }
}

// ---------------------------------------------------------------------------
// Message dispatch
// ---------------------------------------------------------------------------

fn processMessage(allocator: Allocator, line: []const u8, resp: *ManagedList) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, line, .{});
    defer parsed.deinit();

    if (parsed.value != .object) return;
    const obj = parsed.value.object;

    const method_val = obj.get("method") orelse return;
    if (method_val != .string) return;
    const method = method_val.string;

    // id is absent for notifications; we must not reply to them.
    const id: ?std.json.Value = obj.get("id");
    const is_notification = (id == null);

    if (std.mem.eql(u8, method, "initialize")) {
        if (!is_notification) try sendResult(id, INIT_RESULT, resp);
    } else if (std.mem.eql(u8, method, "ping")) {
        if (!is_notification) try sendResult(id, "{}", resp);
    } else if (std.mem.eql(u8, method, "tools/list")) {
        if (!is_notification) {
            var flat = ManagedList.init(allocator);
            defer flat.deinit();
            for (TOOLS_JSON) |c| {
                if (c != '\n') try flat.append(c);
            }
            try sendResult(id, flat.items, resp);
        }
    } else if (std.mem.eql(u8, method, "tools/call")) {
        if (!is_notification) {
            const params_val = obj.get("params") orelse {
                try sendRpcError(id, -32602, "Missing params", resp);
                return;
            };
            if (params_val != .object) {
                try sendRpcError(id, -32602, "params must be an object", resp);
                return;
            }
            try dispatchToolCall(allocator, id, params_val.object, resp);
        }
    }
    // notifications/initialized and unknown methods: no response needed.
}

fn dispatchToolCall(
    allocator: Allocator,
    id: ?std.json.Value,
    params: std.json.ObjectMap,
    resp: *ManagedList,
) !void {
    const name_val = params.get("name") orelse {
        try sendToolError(allocator, id, "Missing tool name", resp);
        return;
    };
    if (name_val != .string) {
        try sendToolError(allocator, id, "Tool name must be a string", resp);
        return;
    }
    const name = name_val.string;

    const args: ?std.json.ObjectMap = if (params.get("arguments")) |a|
        if (a == .object) a.object else null
    else
        null;

    if (std.mem.eql(u8, name, "csv_query")) {
        try toolCsvQuery(allocator, id, args, resp);
    } else if (std.mem.eql(u8, name, "csv_schema")) {
        try toolCsvSchema(allocator, id, args, resp);
    } else if (std.mem.eql(u8, name, "csv_list")) {
        try toolCsvList(allocator, id, args, resp);
    } else {
        const msg = try std.fmt.allocPrint(allocator, "Unknown tool: {s}", .{name});
        defer allocator.free(msg);
        try sendToolError(allocator, id, msg, resp);
    }
}

// ---------------------------------------------------------------------------
// Tool: csv_query
// ---------------------------------------------------------------------------

fn toolCsvQuery(
    allocator: Allocator,
    id: ?std.json.Value,
    args: ?std.json.ObjectMap,
    resp: *ManagedList,
) !void {
    const sql = getStringArg(args, "sql") orelse {
        try sendToolError(allocator, id, "Missing required argument: sql", resp);
        return;
    };

    const output = runQuery(allocator, sql, .json) catch |err| {
        const msg = try std.fmt.allocPrint(allocator, "Query error: {s}", .{@errorName(err)});
        defer allocator.free(msg);
        try sendToolError(allocator, id, msg, resp);
        return;
    };
    defer allocator.free(output);

    try sendToolResult(allocator, id, std.mem.trim(u8, output, "\r\n "), resp);
}

// ---------------------------------------------------------------------------
// Tool: csv_schema
// ---------------------------------------------------------------------------

fn toolCsvSchema(
    allocator: Allocator,
    id: ?std.json.Value,
    args: ?std.json.ObjectMap,
    resp: *ManagedList,
) !void {
    const file = getStringArg(args, "file") orelse {
        try sendToolError(allocator, id, "Missing required argument: file", resp);
        return;
    };

    const sql = try std.fmt.allocPrint(allocator, "SELECT * FROM '{s}' LIMIT 5", .{file});
    defer allocator.free(sql);

    const output = runQuery(allocator, sql, .json) catch |err| {
        const msg = try std.fmt.allocPrint(
            allocator,
            "Error reading '{s}': {s}",
            .{ file, @errorName(err) },
        );
        defer allocator.free(msg);
        try sendToolError(allocator, id, msg, resp);
        return;
    };
    defer allocator.free(output);

    try sendToolResult(allocator, id, std.mem.trim(u8, output, "\r\n "), resp);
}

// ---------------------------------------------------------------------------
// Tool: csv_list
// ---------------------------------------------------------------------------

fn toolCsvList(
    allocator: Allocator,
    id: ?std.json.Value,
    args: ?std.json.ObjectMap,
    resp: *ManagedList,
) !void {
    const dir_path = getStringArg(args, "directory") orelse ".";

    var dir = std.fs.cwd().openDir(dir_path, .{ .iterate = true }) catch |err| {
        const msg = try std.fmt.allocPrint(
            allocator,
            "Cannot open directory '{s}': {s}",
            .{ dir_path, @errorName(err) },
        );
        defer allocator.free(msg);
        try sendToolError(allocator, id, msg, resp);
        return;
    };
    defer dir.close();

    // Collect file names — use unmanaged ArrayList since we pass allocator per-call.
    var files = std.ArrayList([]const u8){};
    defer {
        for (files.items) |f| allocator.free(f);
        files.deinit(allocator);
    }

    var iter = dir.iterate();
    while (try iter.next()) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".csv")) continue;
        try files.append(allocator, try allocator.dupe(u8, entry.name));
    }

    std.mem.sort([]const u8, files.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);

    var result = ManagedList.init(allocator);
    defer result.deinit();

    if (files.items.len == 0) {
        try result.appendSlice("No CSV files found in '");
        try result.appendSlice(dir_path);
        try result.appendSlice("'.");
    } else {
        for (files.items) |f| {
            try result.appendSlice(dir_path);
            if (dir_path[dir_path.len - 1] != '/') try result.append('/');
            try result.appendSlice(f);
            try result.append('\n');
        }
    }

    const trimmed = std.mem.trim(u8, result.items, "\n");
    try sendToolResult(allocator, id, trimmed, resp);
}

// ---------------------------------------------------------------------------
// Core query execution
// ---------------------------------------------------------------------------

fn runQuery(allocator: Allocator, sql: []const u8, format: options_mod.OutputFormat) ![]u8 {
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    // Capture engine output via a temp file (engine.execute writes to std.fs.File).
    // MCP is sequential over stdio so a single path is safe per process.
    const tmp_path = "/tmp/.csvql_mcp.tmp";
    const tmp_file = try std.fs.createFileAbsolute(tmp_path, .{ .read = true });
    defer {
        tmp_file.close();
        std.fs.deleteFileAbsolute(tmp_path) catch {};
    }

    const opts = options_mod.Options{ .format = format };
    try engine.execute(allocator, q, tmp_file, opts);

    try tmp_file.seekTo(0);
    return tmp_file.readToEndAlloc(allocator, 100 * 1024 * 1024);
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn getStringArg(args: ?std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const a = args orelse return null;
    const val = a.get(key) orelse return null;
    return if (val == .string) val.string else null;
}

/// Write a complete JSON-RPC 2.0 response line into resp.
fn sendResult(id: ?std.json.Value, result_json: []const u8, resp: *ManagedList) !void {
    try resp.appendSlice("{\"jsonrpc\":\"2.0\",\"id\":");
    if (id) |i| {
        try writeJsonValue(i, resp);
    } else {
        try resp.appendSlice("null");
    }
    try resp.appendSlice(",\"result\":");
    try resp.appendSlice(result_json);
    try resp.appendSlice("}\n");
}

/// Send a successful MCP tool result (content envelope, isError: false).
fn sendToolResult(
    allocator: Allocator,
    id: ?std.json.Value,
    text: []const u8,
    resp: *ManagedList,
) !void {
    var buf = ManagedList.init(allocator);
    defer buf.deinit();
    try buf.appendSlice("{\"content\":[{\"type\":\"text\",\"text\":");
    try writeJsonString(text, &buf); // escapes \n, ", etc.
    try buf.appendSlice("}],\"isError\":false}");
    try sendResult(id, buf.items, resp);
}

/// Send a tool error result (isError: true).
fn sendToolError(
    allocator: Allocator,
    id: ?std.json.Value,
    msg: []const u8,
    resp: *ManagedList,
) !void {
    var buf = ManagedList.init(allocator);
    defer buf.deinit();
    try buf.appendSlice("{\"content\":[{\"type\":\"text\",\"text\":");
    try writeJsonString(msg, &buf);
    try buf.appendSlice("}],\"isError\":true}");
    try sendResult(id, buf.items, resp);
}

/// Send a JSON-RPC protocol-level error (for malformed requests).
fn sendRpcError(
    id: ?std.json.Value,
    code: i32,
    message: []const u8,
    resp: *ManagedList,
) !void {
    try resp.appendSlice("{\"jsonrpc\":\"2.0\",\"id\":");
    if (id) |i| {
        try writeJsonValue(i, resp);
    } else {
        try resp.appendSlice("null");
    }
    try resp.appendSlice(",\"error\":{\"code\":");
    var code_buf: [16]u8 = undefined;
    const code_str = std.fmt.bufPrint(&code_buf, "{d}", .{code}) catch "0";
    try resp.appendSlice(code_str);
    try resp.appendSlice(",\"message\":");
    try writeJsonString(message, resp);
    try resp.appendSlice("}}\n");
}

/// Append a JSON-encoded string (with surrounding quotes and proper escaping).
fn writeJsonString(s: []const u8, buf: *ManagedList) !void {
    try buf.append('"');
    for (s) |c| {
        switch (c) {
            '"' => try buf.appendSlice("\\\""),
            '\\' => try buf.appendSlice("\\\\"),
            '\n' => try buf.appendSlice("\\n"),
            '\r' => try buf.appendSlice("\\r"),
            '\t' => try buf.appendSlice("\\t"),
            0x00...0x08, 0x0b...0x0c, 0x0e...0x1f => {
                var hex: [6]u8 = undefined;
                const s2 = std.fmt.bufPrint(&hex, "\\u{x:0>4}", .{c}) catch continue;
                try buf.appendSlice(s2);
            },
            else => try buf.append(c),
        }
    }
    try buf.append('"');
}

/// Append a JSON-encoded Value (id can be integer, string, or null).
fn writeJsonValue(v: std.json.Value, buf: *ManagedList) !void {
    switch (v) {
        .null => try buf.appendSlice("null"),
        .bool => |b| try buf.appendSlice(if (b) "true" else "false"),
        .integer => |n| {
            var int_buf: [32]u8 = undefined;
            const s = std.fmt.bufPrint(&int_buf, "{d}", .{n}) catch "0";
            try buf.appendSlice(s);
        },
        .float => |f| {
            var float_buf: [64]u8 = undefined;
            const s = std.fmt.bufPrint(&float_buf, "{d}", .{f}) catch "0";
            try buf.appendSlice(s);
        },
        .number_string => |s| try buf.appendSlice(s),
        .string => |s| try writeJsonString(s, buf),
        .array, .object => try buf.appendSlice("null"),
    }
}
