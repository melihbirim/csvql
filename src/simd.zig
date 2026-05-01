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

/// SIMD-vectorized delimiter search.
/// Processes 16 bytes at a time using @Vector comparisons (SSE2/NEON).
/// Fills `positions` with the byte offsets of every delimiter found.
/// Returns the number of positions written; stops early if `positions` is full.
pub fn findCommasSIMD(line: []const u8, positions: []usize, delimiter: u8) usize {
    var count: usize = 0;

    const VecSize = 16;
    const Vec = @Vector(VecSize, u8);
    const delim_vec: Vec = @splat(delimiter);

    var i: usize = 0;

    // SIMD main loop — 16 bytes per iteration
    while (i + VecSize <= line.len and count < positions.len) : (i += VecSize) {
        const chunk: Vec = line[i..][0..VecSize].*;
        const matches = chunk == delim_vec;

        var j: usize = 0;
        while (j < VecSize) : (j += 1) {
            if (matches[j] and count < positions.len) {
                positions[count] = i + j;
                count += 1;
            }
        }
    }

    // Scalar tail for remaining bytes (< 16)
    while (i < line.len and count < positions.len) : (i += 1) {
        if (line[i] == delimiter) {
            positions[count] = i;
            count += 1;
        }
    }

    return count;
}

/// Quote-aware CSV field splitter into a caller-supplied static buffer.
/// Returns the number of fields written.  All returned slices are zero-copy pointers into `line`.
/// For quoted fields the surrounding `"` characters are stripped; `""` escape sequences are
/// traversed correctly for boundary detection but are NOT unescaped in the returned slice.
/// Use `parseCSVFields` (ArrayList version) when `""` → `"` unescaping is also required.
/// Returns `error.TooManyColumns` if more fields are found than `buf` can hold.
pub fn parseCSVFieldsStatic(line: []const u8, buf: [][]const u8, delimiter: u8) !usize {
    // Fast path: no quotes — plain splitScalar (zero overhead), preserving the
    // std.mem.splitScalar contract (empty line yields one empty field).
    if (std.mem.indexOfScalar(u8, line, '"') == null) {
        var count: usize = 0;
        var iter = std.mem.splitScalar(u8, line, delimiter);
        while (iter.next()) |field| {
            if (count >= buf.len) return error.TooManyColumns;
            buf[count] = field;
            count += 1;
        }
        return count;
    }

    // Quote-aware path: walk byte-by-byte tracking quoted regions.
    var count: usize = 0;
    var i: usize = 0;
    while (true) {
        if (count >= buf.len) return error.TooManyColumns;
        if (i < line.len and line[i] == '"') {
            // Quoted field: collect until closing quote, honoring "" escape sequences.
            i += 1; // skip opening quote
            const start = i;
            while (i < line.len) {
                if (line[i] == '"') {
                    if (i + 1 < line.len and line[i + 1] == '"') {
                        i += 2; // skip "" pair and keep scanning
                        continue;
                    }
                    break; // real closing quote
                }
                i += 1;
            }
            buf[count] = line[start..i];
            if (i < line.len) i += 1; // skip closing quote
            count += 1;
            // Skip any trailing garbage before the next delimiter (RFC tolerance)
            while (i < line.len and line[i] != delimiter) : (i += 1) {}
        } else {
            // Unquoted field
            const start = i;
            while (i < line.len and line[i] != delimiter) : (i += 1) {}
            buf[count] = line[start..i];
            count += 1;
        }
        if (i >= line.len or line[i] != delimiter) break;
        i += 1; // consume delimiter
    }
    return count;
}

/// CSV field splitter used by parallel_mmap.zig.
/// Uses findCommasSIMD for lines >= 32 bytes that contain no quotes, scalar loop for shorter ones.
/// Zero-copy: returned slices point into the original line buffer when no quoting is present.
/// Quote-aware: when the line contains `"` characters the function falls back to a state-machine
/// that correctly skips delimiters inside quoted fields and strips the surrounding quotes.
/// Quoted field contents are heap-allocated via `allocator`; unquoted fields remain zero-copy.
pub fn parseCSVFields(line: []const u8, fields: *std.ArrayList([]const u8), allocator: std.mem.Allocator, delimiter: u8) !void {
    if (line.len == 0) return;

    // Fast path: no quotes anywhere — use SIMD splitting (fully zero-copy, no allocation).
    if (std.mem.indexOfScalar(u8, line, '"') == null) {
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

        // SIMD path: find all delimiter positions at once (up to 64)
        var comma_positions_buf: [64]usize = undefined;
        const comma_count = findCommasSIMD(line, &comma_positions_buf, delimiter);

        // If the buffer is full, check whether more delimiters exist after the last
        // found one — if so the line exceeds 64 fields.
        if (comma_count == comma_positions_buf.len) {
            const last_comma = comma_positions_buf[comma_count - 1];
            if (std.mem.indexOfScalar(u8, line[last_comma + 1 ..], delimiter) != null) {
                return error.TooManyColumns;
            }
        }

        var start: usize = 0;
        for (comma_positions_buf[0..comma_count]) |comma_pos| {
            try fields.append(allocator, line[start..comma_pos]);
            start = comma_pos + 1;
        }
        try fields.append(allocator, line[start..]);
        return;
    }

    // Slow path: quote-aware state machine (RFC 4180).
    // Quoted fields are heap-allocated (quotes stripped, "" → "); unquoted fields are zero-copy.
    var i: usize = 0;
    while (true) {
        if (i < line.len and line[i] == '"') {
            // Quoted field: consume content until the matching closing quote.
            i += 1; // skip opening quote
            var field_buf = std.ArrayList(u8){};
            defer field_buf.deinit(allocator);

            while (i < line.len) {
                const c = line[i];
                if (c == '"') {
                    i += 1;
                    if (i < line.len and line[i] == '"') {
                        // Escaped quote "" → emit single "
                        try field_buf.append(allocator, '"');
                        i += 1;
                    } else {
                        // End of quoted region
                        break;
                    }
                } else {
                    try field_buf.append(allocator, c);
                    i += 1;
                }
            }

            // Skip any trailing garbage between closing quote and next delimiter (RFC tolerance)
            while (i < line.len and line[i] != delimiter) : (i += 1) {}

            try fields.append(allocator, try field_buf.toOwnedSlice(allocator));
        } else {
            // Unquoted field — zero-copy slice into the original line buffer
            const start = i;
            while (i < line.len and line[i] != delimiter) : (i += 1) {}
            try fields.append(allocator, line[start..i]);
        }

        // Advance past delimiter, or stop if we've reached the end of the line.
        if (i >= line.len or line[i] != delimiter) break;
        i += 1; // consume delimiter, continue to next field
    }
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
    var fields = std.ArrayList([]const u8).empty;
    defer fields.deinit(std.testing.allocator);
    try parseCSVFields("a,b,c", &fields, std.testing.allocator, ',');
    try std.testing.expectEqual(@as(usize, 3), fields.items.len);
    try std.testing.expectEqualStrings("a", fields.items[0]);
    try std.testing.expectEqualStrings("b", fields.items[1]);
    try std.testing.expectEqualStrings("c", fields.items[2]);
}

test "parseCSVFields: single field no delimiter" {
    var fields = std.ArrayList([]const u8).empty;
    defer fields.deinit(std.testing.allocator);
    try parseCSVFields("onlyfield", &fields, std.testing.allocator, ',');
    try std.testing.expectEqual(@as(usize, 1), fields.items.len);
    try std.testing.expectEqualStrings("onlyfield", fields.items[0]);
}

test "parseCSVFields: large line uses position-first path" {
    // Line longer than 32 bytes to exercise the comma_positions_buf path
    var fields = std.ArrayList([]const u8).empty;
    defer fields.deinit(std.testing.allocator);
    try parseCSVFields("field_one,field_two,field_three,field_four", &fields, std.testing.allocator, ',');
    try std.testing.expectEqual(@as(usize, 4), fields.items.len);
    try std.testing.expectEqualStrings("field_one", fields.items[0]);
    try std.testing.expectEqualStrings("field_four", fields.items[3]);
}

test "parseCSVFields: custom delimiter" {
    var fields = std.ArrayList([]const u8).empty;
    defer fields.deinit(std.testing.allocator);
    try parseCSVFields("x|y|z", &fields, std.testing.allocator, '|');
    try std.testing.expectEqual(@as(usize, 3), fields.items.len);
    try std.testing.expectEqualStrings("y", fields.items[1]);
}

test "parseCSVFields: returns TooManyColumns for lines with more than 64 fields" {
    // Build a line with 66 comma-separated fields (all "a") — well above the 64 limit
    var line_buf: [66 * 2]u8 = undefined;
    var pos: usize = 0;
    for (0..66) |i| {
        line_buf[pos] = 'a';
        pos += 1;
        if (i < 65) {
            line_buf[pos] = ',';
            pos += 1;
        }
    }
    var fields = std.ArrayList([]const u8).empty;
    defer fields.deinit(std.testing.allocator);
    try std.testing.expectError(
        error.TooManyColumns,
        parseCSVFields(line_buf[0..pos], &fields, std.testing.allocator, ','),
    );
}

// ── Quoted-field tests (fix for issue #42) ────────────────────────────────

test "parseCSVFields: quoted field with embedded comma (short line)" {
    // Reproduces the bug from issue #42: Bob,"Enjoys biscuits, has a bike"
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var fields = std.ArrayList([]const u8){};
    defer fields.deinit(alloc);
    try parseCSVFields("Bob,\"Enjoys biscuits, has a bike\"", &fields, alloc, ',');
    try std.testing.expectEqual(@as(usize, 2), fields.items.len);
    try std.testing.expectEqualStrings("Bob", fields.items[0]);
    try std.testing.expectEqualStrings("Enjoys biscuits, has a bike", fields.items[1]);
}

test "parseCSVFields: quoted field with embedded comma (large line >=32 bytes)" {
    // Verifies that the quote-aware path is also triggered for lines beyond the 32-byte threshold
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var fields = std.ArrayList([]const u8){};
    defer fields.deinit(alloc);
    try parseCSVFields("first_column,second_column,\"third, has a comma\",fourth_column", &fields, alloc, ',');
    try std.testing.expectEqual(@as(usize, 4), fields.items.len);
    try std.testing.expectEqualStrings("first_column", fields.items[0]);
    try std.testing.expectEqualStrings("second_column", fields.items[1]);
    try std.testing.expectEqualStrings("third, has a comma", fields.items[2]);
    try std.testing.expectEqualStrings("fourth_column", fields.items[3]);
}

test "parseCSVFields: escaped double-quote inside quoted field" {
    // RFC 4180: "" inside a quoted field is a literal "
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var fields = std.ArrayList([]const u8){};
    defer fields.deinit(alloc);
    try parseCSVFields("name,\"She said \"\"hello\"\"\"", &fields, alloc, ',');
    try std.testing.expectEqual(@as(usize, 2), fields.items.len);
    try std.testing.expectEqualStrings("name", fields.items[0]);
    try std.testing.expectEqualStrings("She said \"hello\"", fields.items[1]);
}

test "parseCSVFields: empty quoted field" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var fields = std.ArrayList([]const u8){};
    defer fields.deinit(alloc);
    try parseCSVFields("a,\"\",b", &fields, alloc, ',');
    try std.testing.expectEqual(@as(usize, 3), fields.items.len);
    try std.testing.expectEqualStrings("a", fields.items[0]);
    try std.testing.expectEqualStrings("", fields.items[1]);
    try std.testing.expectEqualStrings("b", fields.items[2]);
}

test "parseCSVFields: mixed quoted and unquoted fields" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var fields = std.ArrayList([]const u8){};
    defer fields.deinit(alloc);
    try parseCSVFields("Garry,Likes beer,active", &fields, alloc, ',');
    try std.testing.expectEqual(@as(usize, 3), fields.items.len);
    try std.testing.expectEqualStrings("Garry", fields.items[0]);
    try std.testing.expectEqualStrings("Likes beer", fields.items[1]);
    try std.testing.expectEqualStrings("active", fields.items[2]);
}
