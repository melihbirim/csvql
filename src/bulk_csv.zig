const std = @import("std");
const Allocator = std.mem.Allocator;
const simd = @import("simd.zig");

/// High-performance bulk CSV reader - optimized for speed over RFC 4180 compliance
/// Assumes: no quoted fields with embedded newlines/commas
/// Limitation: maximum 256 columns per row (fixed field_buf size)
pub const BulkCsvReader = struct {
    file: std.fs.File,
    allocator: Allocator,
    delimiter: u8,
    buffer: []u8,
    buffer_pos: usize,
    buffer_len: usize,
    eof: bool,
    line_start: usize,
    // Pre-allocated field buffer for zero-copy reads (avoids per-field allocation).
    // Hard limit: CSVs with more than 256 columns are not supported by this reader.
    // Use csv.CsvReader instead for wide files.
    field_buf: [256][]const u8 = undefined,
    // Scratch arena for the rare field that contains an escaped "" quote pair
    // and needs unescaping (parseCSVFieldsStatic returns those unescaped —
    // see issue #89). Reset every call instead of freed per-field, since the
    // common case allocates nothing at all.
    unescape_arena: std.heap.ArenaAllocator,

    pub fn init(allocator: Allocator, file: std.fs.File) !BulkCsvReader {
        // Allocate 2MB buffer for bulk reading - fewer syscalls
        const buffer = try allocator.alloc(u8, 2 * 1024 * 1024);
        return BulkCsvReader{
            .file = file,
            .allocator = allocator,
            .delimiter = ',',
            .buffer = buffer,
            .buffer_pos = 0,
            .buffer_len = 0,
            .eof = false,
            .line_start = 0,
            .unescape_arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    pub fn deinit(self: *BulkCsvReader) void {
        self.allocator.free(self.buffer);
        self.unescape_arena.deinit();
    }

    /// Collapse `""` escape pairs to `"` within an already quote-stripped field.
    /// Only called when the field is known to contain `""`, so it always
    /// allocates a (shorter-or-equal) replacement from the reader's scratch arena.
    fn unescapeQuotes(self: *BulkCsvReader, field: []const u8) ![]const u8 {
        var out = try self.unescape_arena.allocator().alloc(u8, field.len);
        var w: usize = 0;
        var i: usize = 0;
        while (i < field.len) {
            if (field[i] == '"' and i + 1 < field.len and field[i + 1] == '"') {
                out[w] = '"';
                w += 1;
                i += 2;
            } else {
                out[w] = field[i];
                w += 1;
                i += 1;
            }
        }
        return out[0..w];
    }

    /// Find the absolute buffer index of the '\n' that ends the current CSV record,
    /// skipping '\n' characters that appear inside quoted fields.
    /// Returns null when no record-ending newline exists before `buffer_len`.
    fn findRecordEnd(self: *BulkCsvReader) ?usize {
        var i = self.line_start;
        var in_quote = false;
        while (i < self.buffer_len) {
            const c = self.buffer[i];
            if (c == '"') {
                if (in_quote) {
                    // "" inside a quoted field — escaped quote, stay in quoted mode
                    if (i + 1 < self.buffer_len and self.buffer[i + 1] == '"') {
                        i += 2;
                        continue;
                    }
                    in_quote = false;
                } else {
                    in_quote = true;
                }
            } else if (c == '\n' and !in_quote) {
                return i;
            }
            i += 1;
        }
        return null;
    }

    fn fillBuffer(self: *BulkCsvReader) !void {
        if (self.eof) return;

        // If there's partial data at the end, move it to the start
        if (self.line_start < self.buffer_len) {
            const remaining = self.buffer_len - self.line_start;
            std.mem.copyForwards(u8, self.buffer[0..remaining], self.buffer[self.line_start..self.buffer_len]);
            self.buffer_len = remaining;
            self.buffer_pos = remaining;
            self.line_start = 0;
        } else {
            self.buffer_len = 0;
            self.buffer_pos = 0;
            self.line_start = 0;
        }

        // Fill the rest of the buffer
        const bytes_read = try self.file.read(self.buffer[self.buffer_len..]);
        self.buffer_len += bytes_read;
        if (bytes_read == 0) {
            self.eof = true;
        }
    }

    /// Read next CSV record using bulk operations
    pub fn readRecord(self: *BulkCsvReader) !?[][]u8 {
        while (true) {
            if (self.line_start >= self.buffer_len) {
                try self.fillBuffer();
                if (self.eof and self.buffer_len == 0) return null;
                if (self.line_start >= self.buffer_len) return null;
                continue;
            }

            if (self.findRecordEnd()) |newline_abs| {
                var line_end = newline_abs;
                // Trim \r in \r\n endings
                if (line_end > self.line_start and self.buffer[line_end - 1] == '\r') {
                    line_end -= 1;
                }
                const line = self.buffer[self.line_start..line_end];
                self.line_start = newline_abs + 1;
                return try self.parseLine(line);
            } else {
                if (self.eof) {
                    if (self.line_start < self.buffer_len) {
                        const line = self.buffer[self.line_start..self.buffer_len];
                        self.line_start = self.buffer_len;
                        return try self.parseLine(line);
                    }
                    return null;
                }
                try self.fillBuffer();
                if (self.eof and self.line_start >= self.buffer_len) return null;
            }
        }
    }

    /// Zero-copy read - returns slices directly into the read buffer.
    /// Returned data is only valid until the next readRecordSlices() call.
    /// No allocations are performed (uses pre-allocated field buffer).
    pub fn readRecordSlices(self: *BulkCsvReader) !?[]const []const u8 {
        while (true) {
            if (self.line_start >= self.buffer_len) {
                try self.fillBuffer();
                if (self.eof and self.buffer_len == 0) return null;
                if (self.line_start >= self.buffer_len) return null;
                continue;
            }

            if (self.findRecordEnd()) |newline_abs| {
                var line_end = newline_abs;
                if (line_end > self.line_start and self.buffer[line_end - 1] == '\r') {
                    line_end -= 1;
                }
                const line = self.buffer[self.line_start..line_end];
                self.line_start = newline_abs + 1;
                return try self.parseLineSlices(line);
            } else {
                if (self.eof) {
                    if (self.line_start < self.buffer_len) {
                        const line = self.buffer[self.line_start..self.buffer_len];
                        self.line_start = self.buffer_len;
                        return try self.parseLineSlices(line);
                    }
                    return null;
                }
                try self.fillBuffer();
                if (self.eof and self.line_start >= self.buffer_len) return null;
            }
        }
    }

    /// Parse a line into field_buf. Zero-copy for the common case; a field
    /// containing an escaped `""` pair gets unescaped into the reader's
    /// scratch arena (reset here, so no per-field free is needed).
    /// Quote-aware: quoted fields have their surrounding `"` stripped.
    fn parseLineSlices(self: *BulkCsvReader, line: []const u8) ![]const []const u8 {
        _ = self.unescape_arena.reset(.retain_capacity);
        const count = try simd.parseCSVFieldsStatic(line, &self.field_buf, self.delimiter);
        for (self.field_buf[0..count]) |*field| {
            if (std.mem.indexOf(u8, field.*, "\"\"") != null) {
                field.* = try self.unescapeQuotes(field.*);
            }
        }
        return self.field_buf[0..count];
    }

    fn parseLine(self: *BulkCsvReader, line: []const u8) ![][]u8 {
        var fields = std.ArrayList([]u8){};
        errdefer {
            for (fields.items) |field| self.allocator.free(field);
            fields.deinit(self.allocator);
        }

        if (std.mem.indexOfScalar(u8, line, '"') == null) {
            // Fast path: no quotes — split by delimiter and dupe
            var iter = std.mem.splitScalar(u8, line, self.delimiter);
            while (iter.next()) |field| {
                try fields.append(self.allocator, try self.allocator.dupe(u8, field));
            }
            return try fields.toOwnedSlice(self.allocator);
        }

        // Quote-aware path: strip surrounding quotes and unescape "" → "
        var i: usize = 0;
        while (true) {
            if (i < line.len and line[i] == '"') {
                i += 1; // skip opening quote
                var field_buf = std.ArrayList(u8){};
                defer field_buf.deinit(self.allocator);
                while (i < line.len) {
                    const c = line[i];
                    if (c == '"') {
                        i += 1;
                        if (i < line.len and line[i] == '"') {
                            try field_buf.append(self.allocator, '"');
                            i += 1;
                        } else break; // end of quoted region
                    } else {
                        try field_buf.append(self.allocator, c);
                        i += 1;
                    }
                }
                while (i < line.len and line[i] != self.delimiter) : (i += 1) {}
                try fields.append(self.allocator, try field_buf.toOwnedSlice(self.allocator));
            } else {
                const start = i;
                while (i < line.len and line[i] != self.delimiter) : (i += 1) {}
                try fields.append(self.allocator, try self.allocator.dupe(u8, line[start..i]));
            }
            if (i >= line.len or line[i] != self.delimiter) break;
            i += 1; // consume delimiter
        }
        return try fields.toOwnedSlice(self.allocator);
    }

    /// Free a record returned by readRecord
    pub fn freeRecord(self: *BulkCsvReader, record: [][]u8) void {
        for (record) |field| {
            self.allocator.free(field);
        }
        self.allocator.free(record);
    }
};

test "parseLineSlices returns TooManyColumns when row exceeds 256 fields" {
    // Construct a CSV line with 257 comma-separated fields: "a,a,a,...,a"
    var line_buf: [257 * 2]u8 = undefined;
    var pos: usize = 0;
    for (0..257) |i| {
        line_buf[pos] = 'a';
        pos += 1;
        if (i < 256) {
            line_buf[pos] = ',';
            pos += 1;
        }
    }

    var reader: BulkCsvReader = undefined;
    reader.unescape_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer reader.unescape_arena.deinit();
    reader.delimiter = ',';
    try std.testing.expectError(error.TooManyColumns, reader.parseLineSlices(line_buf[0..pos]));
}

test "parseLineSlices returns exactly 256 fields for a 256-column row" {
    // 256 fields = 255 commas + last field, all within limit
    var line_buf: [256 * 2]u8 = undefined;
    var pos: usize = 0;
    for (0..256) |i| {
        line_buf[pos] = 'a';
        pos += 1;
        if (i < 255) {
            line_buf[pos] = ',';
            pos += 1;
        }
    }

    var reader: BulkCsvReader = undefined;
    reader.unescape_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer reader.unescape_arena.deinit();
    reader.delimiter = ',';
    const fields = try reader.parseLineSlices(line_buf[0..pos]);
    try std.testing.expectEqual(@as(usize, 256), fields.len);
}

test "parseLineSlices: single field with no delimiter" {
    var reader: BulkCsvReader = undefined;
    reader.unescape_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer reader.unescape_arena.deinit();
    reader.delimiter = ',';
    const fields = try reader.parseLineSlices("onlyvalue");
    try std.testing.expectEqual(@as(usize, 1), fields.len);
    try std.testing.expectEqualStrings("onlyvalue", fields[0]);
}

test "parseLineSlices: empty line returns one empty field" {
    var reader: BulkCsvReader = undefined;
    reader.unescape_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer reader.unescape_arena.deinit();
    reader.delimiter = ',';
    const fields = try reader.parseLineSlices("");
    try std.testing.expectEqual(@as(usize, 1), fields.len);
    try std.testing.expectEqualStrings("", fields[0]);
}

test "parseLineSlices: custom tab delimiter" {
    var reader: BulkCsvReader = undefined;
    reader.unescape_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer reader.unescape_arena.deinit();
    reader.delimiter = '\t';
    const fields = try reader.parseLineSlices("col1\tcol2\tcol3");
    try std.testing.expectEqual(@as(usize, 3), fields.len);
    try std.testing.expectEqualStrings("col1", fields[0]);
    try std.testing.expectEqualStrings("col3", fields[2]);
}
