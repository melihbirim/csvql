const std = @import("std");

/// Optimized integer parsing used by compareValues in parser.zig and engine.zig.
/// Rejects non-integer strings (e.g. "1117.43") so callers can fall back to
/// parseFloat rather than silently truncating.
pub inline fn parseIntFast(str: []const u8) !i64 {
    var i: usize = 0;
    while (i < str.len and std.ascii.isWhitespace(str[i])) : (i += 1) {}

    if (i >= str.len) return error.InvalidInput;

    var negative = false;
    if (str[i] == '-') {
        negative = true;
        i += 1;
    } else if (str[i] == '+') {
        i += 1;
    }

    if (i >= str.len) return error.InvalidInput;

    var result: i64 = 0;
    while (i < str.len) : (i += 1) {
        const c = str[i];
        if (c < '0' or c > '9') break;
        result = result * 10 + (c - '0');
    }

    // Reject strings with non-whitespace chars after the digits.
    // "1117.43" must NOT silently truncate to 1117.
    while (i < str.len and std.ascii.isWhitespace(str[i])) : (i += 1) {}
    if (i < str.len) return error.InvalidInput;

    return if (negative) -result else result;
}

/// String equality check used by WHERE clause evaluation in parser.zig.
/// Delegates to std.mem.eql (which may use SIMD internally on supported targets).
pub inline fn stringsEqualFast(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    if (a.ptr == b.ptr) return true;
    return std.mem.eql(u8, a, b);
}

/// CSV field splitter used by parallel_mmap.zig.
/// Zero-copy: returned slices point into the original line buffer.
pub fn parseCSVFields(line: []const u8, fields: *std.ArrayList([]const u8), allocator: std.mem.Allocator, delimiter: u8) !void {
    if (line.len == 0) return;

    if (line.len < 32) {
        var start: usize = 0;
        for (line, 0..) |c, i| {
            if (c == delimiter) {
                try fields.append(allocator, line[start..i]);
                start = i + 1;
            }
        }
        try fields.append(allocator, line[start..]);
        return;
    }

    var comma_positions_buf: [64]usize = undefined;
    var comma_count: usize = 0;

    var i: usize = 0;
    while (i < line.len and comma_count < 64) : (i += 1) {
        if (line[i] == delimiter) {
            comma_positions_buf[comma_count] = i;
            comma_count += 1;
        }
    }

    var start: usize = 0;
    for (comma_positions_buf[0..comma_count]) |comma_pos| {
        try fields.append(allocator, line[start..comma_pos]);
        start = comma_pos + 1;
    }
    try fields.append(allocator, line[start..]);
}

// ── Tests ─────────────────────────────────────────────────────────────────

test "parseIntFast: valid positive integer" {
    try std.testing.expectEqual(@as(i64, 42), try parseIntFast("42"));
}

test "parseIntFast: valid negative integer" {
    try std.testing.expectEqual(@as(i64, -7), try parseIntFast("-7"));
}

test "parseIntFast: explicit plus sign" {
    try std.testing.expectEqual(@as(i64, 10), try parseIntFast("+10"));
}

test "parseIntFast: leading and trailing whitespace" {
    try std.testing.expectEqual(@as(i64, 99), try parseIntFast("  99  "));
}

test "parseIntFast: float string returns error" {
    try std.testing.expectError(error.InvalidInput, parseIntFast("1117.43"));
}

test "parseIntFast: empty string returns error" {
    try std.testing.expectError(error.InvalidInput, parseIntFast(""));
}

test "stringsEqualFast: equal strings" {
    try std.testing.expect(stringsEqualFast("hello", "hello"));
}

test "stringsEqualFast: different strings" {
    try std.testing.expect(!stringsEqualFast("hello", "world"));
}

test "stringsEqualFast: different lengths" {
    try std.testing.expect(!stringsEqualFast("hi", "hii"));
}

test "stringsEqualFast: long strings equal (>16 chars)" {
    try std.testing.expect(stringsEqualFast("abcdefghijklmnopq", "abcdefghijklmnopq"));
}

test "stringsEqualFast: long strings not equal" {
    try std.testing.expect(!stringsEqualFast("abcdefghijklmnopq", "abcdefghijklmnopX"));
}

test "parseCSVFields: basic split small line" {
    var fields = std.ArrayList([]const u8){};
    defer fields.deinit(std.testing.allocator);
    try parseCSVFields("a,b,c", &fields, std.testing.allocator, ',');
    try std.testing.expectEqual(@as(usize, 3), fields.items.len);
    try std.testing.expectEqualStrings("a", fields.items[0]);
    try std.testing.expectEqualStrings("b", fields.items[1]);
    try std.testing.expectEqualStrings("c", fields.items[2]);
}

test "parseCSVFields: single field no delimiter" {
    var fields = std.ArrayList([]const u8){};
    defer fields.deinit(std.testing.allocator);
    try parseCSVFields("onlyfield", &fields, std.testing.allocator, ',');
    try std.testing.expectEqual(@as(usize, 1), fields.items.len);
    try std.testing.expectEqualStrings("onlyfield", fields.items[0]);
}

test "parseCSVFields: large line uses position-first path" {
    // Line longer than 32 bytes to exercise the comma_positions_buf path
    var fields = std.ArrayList([]const u8){};
    defer fields.deinit(std.testing.allocator);
    try parseCSVFields("field_one,field_two,field_three,field_four", &fields, std.testing.allocator, ',');
    try std.testing.expectEqual(@as(usize, 4), fields.items.len);
    try std.testing.expectEqualStrings("field_one", fields.items[0]);
    try std.testing.expectEqualStrings("field_four", fields.items[3]);
}

test "parseCSVFields: custom delimiter" {
    var fields = std.ArrayList([]const u8){};
    defer fields.deinit(std.testing.allocator);
    try parseCSVFields("x|y|z", &fields, std.testing.allocator, '|');
    try std.testing.expectEqual(@as(usize, 3), fields.items.len);
    try std.testing.expectEqualStrings("y", fields.items[1]);
}
