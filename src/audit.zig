//! Optional query audit log (`--audit <file>`). Appends one JSON object per query
//! so on-prem operators can prove, after the fact, exactly what was queried and when.
//! Best-effort: an audit failure never fails the query.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Append a JSONL record: {"ts":<unix>,"sql":"...","rows":N,"bytes":N}.
/// `rows`/`bytes` are optional (null when unknown, e.g. streamed CLI output).
/// ponytail: plain append with seekFromEnd. One process/query at a time here
/// (MCP is sequential, CLI is one query per run), so no cross-process locking.
pub fn record(allocator: Allocator, path: []const u8, sql: []const u8, rows: ?usize, bytes: ?usize) void {
    var line = std.ArrayList(u8){};
    defer line.deinit(allocator);

    line.appendSlice(allocator, "{\"ts\":") catch return;
    line.writer(allocator).print("{d}", .{std.time.timestamp()}) catch return;
    line.appendSlice(allocator, ",\"sql\":\"") catch return;
    appendEscaped(&line, allocator, sql) catch return;
    line.append(allocator, '"') catch return;
    if (rows) |r| line.writer(allocator).print(",\"rows\":{d}", .{r}) catch return;
    if (bytes) |b| line.writer(allocator).print(",\"bytes\":{d}", .{b}) catch return;
    line.appendSlice(allocator, "}\n") catch return;

    // Open or create without truncating, then append at the end.
    const f = std.fs.cwd().createFile(path, .{ .truncate = false }) catch return;
    defer f.close();
    f.seekFromEnd(0) catch {};
    f.writeAll(line.items) catch {};
}

fn appendEscaped(out: *std.ArrayList(u8), allocator: Allocator, s: []const u8) !void {
    for (s) |c| switch (c) {
        '"' => try out.appendSlice(allocator, "\\\""),
        '\\' => try out.appendSlice(allocator, "\\\\"),
        '\n' => try out.appendSlice(allocator, "\\n"),
        '\r' => try out.appendSlice(allocator, "\\r"),
        '\t' => try out.appendSlice(allocator, "\\t"),
        else => try out.append(allocator, c),
    };
}

test "audit record appends a valid JSONL line" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    // create the file so realpath resolves, then record into it
    {
        const f = try tmp.dir.createFile("audit.log", .{});
        f.close();
    }
    const path = try tmp.dir.realpath("audit.log", &path_buf);

    record(allocator, path, "SELECT * FROM 'a.csv' WHERE x = \"q\"", 3, 42);
    record(allocator, path, "SELECT COUNT(*) FROM 'b.csv'", null, null);

    const data = try tmp.dir.readFileAlloc(allocator, "audit.log", 64 * 1024);
    defer allocator.free(data);
    try std.testing.expect(std.mem.indexOf(u8, data, "\"rows\":3") != null);
    try std.testing.expect(std.mem.indexOf(u8, data, "\"bytes\":42") != null);
    try std.testing.expect(std.mem.indexOf(u8, data, "\\\"q\\\"") != null); // escaped quote
    // two lines
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, data, "\n"));
}
