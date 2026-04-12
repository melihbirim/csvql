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
