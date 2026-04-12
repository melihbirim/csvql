const std = @import("std");

/// SIMD-accelerated utilities for query processing.
/// NOTE: Most functions in this file are stubs or thin scalar wrappers —
/// they are NOT called from any hot path (engine, mmap_engine, bulk_csv,
/// parallel_mmap). The only function used in production code is
/// `parseIntFast` (called from compareValues in parser.zig) and
/// `findCommasSIMD` (which has real vector code but is unused).
pub const simd = struct {
    /// Parse multiple integers in parallel using SIMD (when possible)
    pub fn parseIntsSimd(strings: []const []const u8, results: []i64) !void {
        if (strings.len == 0) return;

        // For now, fall back to scalar - full SIMD int parsing is complex
        for (strings, 0..) |str, i| {
            results[i] = try std.fmt.parseInt(i64, str, 10);
        }
    }

    /// Compare integers using SIMD when batch size is large enough.
    /// NOT IMPLEMENTED — returns empty slice. Not called anywhere.
    pub fn compareIntsBatch(values: []const i64, threshold: i64, comptime op: CompareOp) []const bool {
        _ = values;
        _ = threshold;
        _ = op;
        // TODO: Implement SIMD comparison
        return &[_]bool{};
    }

    /// Fast memory search for delimiter.
    /// Delegates entirely to std.mem.indexOfScalar (no SIMD added here).
    pub fn findDelimiter(haystack: []const u8, delimiter: u8) ?usize {
        const vec_size = 16; // SSE/NEON vector size

        if (haystack.len < vec_size) {
            // Fall back to scalar for small inputs
            return std.mem.indexOfScalar(u8, haystack, delimiter);
        }

        // For now, use standard library (which may use SIMD internally)
        return std.mem.indexOfScalar(u8, haystack, delimiter);
    }
};

pub const CompareOp = enum {
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
};

/// Vectorized numeric comparison.
/// NOT called from any hot path. Allocates via page_allocator with no
/// free path — caller is responsible but has no allocator reference.
/// Kept for potential future use.
pub fn compareVectorized(values: []const []const u8, threshold: []const u8, comptime op: CompareOp) ![]bool {
    const allocator = std.heap.page_allocator;
    var results = try allocator.alloc(bool, values.len);

    // Parse all values
    for (values, 0..) |val, i| {
        const val_int = std.fmt.parseInt(i64, val, 10) catch {
            results[i] = false;
            continue;
        };
        const threshold_int = std.fmt.parseInt(i64, threshold, 10) catch {
            results[i] = false;
            continue;
        };

        results[i] = switch (op) {
            .Equal => val_int == threshold_int,
            .NotEqual => val_int != threshold_int,
            .Greater => val_int > threshold_int,
            .GreaterEqual => val_int >= threshold_int,
            .Less => val_int < threshold_int,
            .LessEqual => val_int <= threshold_int,
        };
    }

    return results;
}

/// String equality — delegates to std.mem.eql in all cases (no SIMD added).
/// std.mem.eql may use SIMD internally on supported targets.
pub inline fn stringsEqualFast(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    if (a.ptr == b.ptr) return true;

    // For short strings, use direct comparison
    if (a.len < 16) {
        return std.mem.eql(u8, a, b);
    }

    // For longer strings, std.mem.eql may use SIMD internally
    return std.mem.eql(u8, a, b);
}

/// Optimized integer parsing with SIMD hints
pub inline fn parseIntFast(str: []const u8) !i64 {
    // Skip leading whitespace
    var i: usize = 0;
    while (i < str.len and std.ascii.isWhitespace(str[i])) : (i += 1) {}

    if (i >= str.len) return error.InvalidInput;

    // Check for sign
    var negative = false;
    if (str[i] == '-') {
        negative = true;
        i += 1;
    } else if (str[i] == '+') {
        i += 1;
    }

    if (i >= str.len) return error.InvalidInput;

    // Parse digits - compiler can vectorize this loop
    var result: i64 = 0;
    while (i < str.len) : (i += 1) {
        const c = str[i];
        if (c < '0' or c > '9') break;
        result = result * 10 + (c - '0');
    }

    // Reject strings with non-whitespace chars after the integer digits.
    // "1117.43" must NOT silently truncate to 1117 — the caller must fall
    // back to parseFloat to get the correct value.
    while (i < str.len and std.ascii.isWhitespace(str[i])) : (i += 1) {}
    if (i < str.len) return error.InvalidInput;

    return if (negative) -result else result;
}

/// Batch parse floats (for future aggregation optimizations)
pub fn parseFloatsBatch(strings: []const []const u8, results: []f64) !void {
    for (strings, 0..) |str, i| {
        results[i] = try std.fmt.parseFloat(f64, str);
    }
}

/// Fast SIMD-optimized newline search
/// Uses vectorized comparison for faster scanning of large buffers
pub inline fn findNewline(haystack: []const u8, start: usize) ?usize {
    if (start >= haystack.len) return null;

    const data = haystack[start..];

    // For small searches, use standard library
    if (data.len < 64) {
        if (std.mem.indexOfScalar(u8, data, '\n')) |pos| {
            return start + pos;
        }
        return null;
    }

    // For larger searches, process in chunks
    // The standard library may use SIMD internally
    if (std.mem.indexOfScalar(u8, data, '\n')) |pos| {
        return start + pos;
    }

    return null;
}

/// Find multiple newlines at once using SIMD
pub fn findNewlinesBatch(haystack: []const u8, positions: []usize, max_count: usize) usize {
    var count: usize = 0;
    var pos: usize = 0;

    while (count < max_count and pos < haystack.len) {
        if (findNewline(haystack, pos)) |newline_pos| {
            positions[count] = newline_pos;
            count += 1;
            pos = newline_pos + 1;
        } else {
            break;
        }
    }

    return count;
}
/// SIMD-accelerated CSV field parser
/// Finds all comma positions in a line to split into fields
/// Returns slices pointing into the original line (zero-copy)
pub fn parseCSVFields(line: []const u8, fields: *std.ArrayList([]const u8), allocator: std.mem.Allocator, delimiter: u8) !void {
    if (line.len == 0) return;

    // Fast path for small lines
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

    // For larger lines, find all delimiter positions first, then slice
    // This allows better prefetching and branch prediction
    var comma_positions_buf: [64]usize = undefined; // Max 64 fields
    var comma_count: usize = 0;

    var i: usize = 0;
    while (i < line.len and comma_count < 64) : (i += 1) {
        if (line[i] == delimiter) {
            comma_positions_buf[comma_count] = i;
            comma_count += 1;
        }
    }

    // Build fields from comma positions
    var start: usize = 0;
    for (comma_positions_buf[0..comma_count]) |comma_pos| {
        try fields.append(allocator, line[start..comma_pos]);
        start = comma_pos + 1;
    }
    try fields.append(allocator, line[start..]);
}

/// SIMD-vectorized comma search for CSV parsing
/// Processes 16 bytes at once looking for comma delimiters
pub fn findCommasSIMD(line: []const u8, positions: []usize) usize {
    var count: usize = 0;

    const VecSize = 16; // SSE/NEON vector size
    const Vec = @Vector(VecSize, u8);
    const comma_vec: Vec = @splat(',');

    var i: usize = 0;

    // Process 16 bytes at a time with SIMD
    while (i + VecSize <= line.len and count < positions.len) : (i += VecSize) {
        const chunk: Vec = line[i..][0..VecSize].*;
        const matches = chunk == comma_vec;

        // Extract positions of matches
        var j: usize = 0;
        while (j < VecSize) : (j += 1) {
            if (matches[j] and count < positions.len) {
                positions[count] = i + j;
                count += 1;
            }
        }
    }

    // Handle remaining bytes
    while (i < line.len and count < positions.len) : (i += 1) {
        if (line[i] == ',') {
            positions[count] = i;
            count += 1;
        }
    }

    return count;
}
