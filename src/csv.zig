const std = @import("std");
const Allocator = std.mem.Allocator;
const options_mod = @import("options.zig");

/// Re-export options so callers that only import csv can access Options/OutputFormat.
pub const options = options_mod;

/// RFC 4180 compliant CSV reader
pub const CsvReader = struct {
    file: std.fs.File,
    allocator: Allocator,
    delimiter: u8,
    buffer: [262144]u8, // Increased to 256KB for fewer syscalls
    buffer_pos: usize,
    buffer_len: usize,
    eof: bool,
    putback_byte: ?u8,

    pub fn init(allocator: Allocator, file: std.fs.File) CsvReader {
        return CsvReader{
            .file = file,
            .allocator = allocator,
            .delimiter = ',',
            .buffer = undefined,
            .buffer_pos = 0,
            .buffer_len = 0,
            .eof = false,
            .putback_byte = null,
        };
    }

    fn readByte(self: *CsvReader) !?u8 {
        // Check if there's a putback byte first
        if (self.putback_byte) |byte| {
            self.putback_byte = null;
            return byte;
        }

        if (self.buffer_pos >= self.buffer_len) {
            if (self.eof) return null;
            self.buffer_len = try self.file.read(&self.buffer);
            self.buffer_pos = 0;
            if (self.buffer_len == 0) {
                self.eof = true;
                return null;
            }
        }
        const byte = self.buffer[self.buffer_pos];
        self.buffer_pos += 1;
        return byte;
    }

    fn putBackByte(self: *CsvReader, byte: u8) void {
        self.putback_byte = byte;
    }

    /// Read the next CSV record
    pub fn readRecord(self: *CsvReader) !?[][]u8 {
        var fields = std.ArrayList([]u8).empty;
        errdefer {
            for (fields.items) |field| {
                self.allocator.free(field);
            }
            fields.deinit(self.allocator);
        }

        var field_buffer = std.ArrayList(u8).empty;
        defer field_buffer.deinit(self.allocator);

        var in_quotes = false;
        var at_start = true;

        while (true) {
            const byte_opt = try self.readByte();
            const byte = byte_opt orelse {
                // EOF - handle last field
                if (field_buffer.items.len > 0 or fields.items.len > 0 or !at_start) {
                    try fields.append(self.allocator, try field_buffer.toOwnedSlice(self.allocator));
                }
                if (fields.items.len == 0) {
                    return null;
                }
                return try fields.toOwnedSlice(self.allocator);
            };

            at_start = false;

            if (in_quotes) {
                if (byte == '"') {
                    // Check for escaped quote
                    const next_opt = try self.readByte();
                    if (next_opt) |next| {
                        if (next == '"') {
                            // Escaped quote
                            try field_buffer.append(self.allocator, '"');
                        } else {
                            // End of quoted field
                            in_quotes = false;
                            // Put back the byte
                            self.putBackByte(next);
                        }
                    } else {
                        // EOF after quote
                        in_quotes = false;
                    }
                } else {
                    try field_buffer.append(self.allocator, byte);
                }
            } else {
                if (byte == '"' and field_buffer.items.len == 0) {
                    // Start of quoted field
                    in_quotes = true;
                } else if (byte == self.delimiter) {
                    // End of field
                    try fields.append(self.allocator, try field_buffer.toOwnedSlice(self.allocator));
                    field_buffer = std.ArrayList(u8).empty;
                } else if (byte == '\r') {
                    // Handle CR - check for LF
                    const next_opt = try self.readByte();
                    if (next_opt) |next| {
                        if (next != '\n') {
                            self.putBackByte(next);
                        }
                    }
                    // End of record
                    try fields.append(self.allocator, try field_buffer.toOwnedSlice(self.allocator));
                    return try fields.toOwnedSlice(self.allocator);
                } else if (byte == '\n') {
                    // End of record
                    try fields.append(self.allocator, try field_buffer.toOwnedSlice(self.allocator));
                    return try fields.toOwnedSlice(self.allocator);
                } else {
                    try field_buffer.append(self.allocator, byte);
                }
            }
        }
    }

    /// Free a record returned by readRecord
    pub fn freeRecord(self: *CsvReader, record: [][]u8) void {
        for (record) |field| {
            self.allocator.free(field);
        }
        self.allocator.free(record);
    }
};

/// Fast CSV reader for simple cases (no quotes)
pub const FastCsvReader = struct {
    file: std.fs.File,
    allocator: Allocator,
    delimiter: u8,
    line_buffer: std.ArrayList(u8),
    buffer: [262144]u8, // Increased to 256KB to match CsvReader
    buffer_pos: usize,
    buffer_len: usize,
    eof: bool,

    pub fn init(allocator: Allocator, file: std.fs.File) FastCsvReader {
        return FastCsvReader{
            .file = file,
            .allocator = allocator,
            .delimiter = ',',
            .line_buffer = std.ArrayList(u8).empty,
            .buffer = undefined,
            .buffer_pos = 0,
            .buffer_len = 0,
            .eof = false,
        };
    }

    fn readByte(self: *FastCsvReader) !?u8 {
        if (self.buffer_pos >= self.buffer_len) {
            if (self.eof) return null;
            self.buffer_len = try self.file.read(&self.buffer);
            self.buffer_pos = 0;
            if (self.buffer_len == 0) {
                self.eof = true;
                return null;
            }
        }
        const byte = self.buffer[self.buffer_pos];
        self.buffer_pos += 1;
        return byte;
    }

    pub fn deinit(self: *FastCsvReader) void {
        self.line_buffer.deinit(self.allocator);
    }

    /// Read the next CSV record (fast path - assumes no escaped quotes)
    pub fn readRecord(self: *FastCsvReader) !?[][]u8 {
        self.line_buffer.clearRetainingCapacity();

        // Read line byte by byte until \n
        while (try self.readByte()) |byte| {
            if (byte == '\n') break;
            try self.line_buffer.append(self.allocator, byte);
        }

        if (self.line_buffer.items.len == 0 and self.eof) {
            return null;
        }

        // Trim trailing \r if present
        if (self.line_buffer.items.len > 0 and self.line_buffer.items[self.line_buffer.items.len - 1] == '\r') {
            _ = self.line_buffer.pop();
        }

        // Split by delimiter
        var fields = std.ArrayList([]u8).empty;
        errdefer {
            for (fields.items) |field| {
                self.allocator.free(field);
            }
            fields.deinit(self.allocator);
        }

        var iter = std.mem.splitScalar(u8, self.line_buffer.items, self.delimiter);
        while (iter.next()) |field| {
            try fields.append(self.allocator, try self.allocator.dupe(u8, field));
        }

        return try fields.toOwnedSlice(self.allocator);
    }

    /// Free a record returned by readRecord
    pub fn freeRecord(self: *FastCsvReader, record: [][]u8) void {
        for (record) |field| {
            self.allocator.free(field);
        }
        self.allocator.free(record);
    }
};

/// CSV writer with buffering
pub const CsvWriter = struct {
    file: std.fs.File,
    delimiter: u8,
    buffer: [1048576]u8, // 1MB buffer for fewer write syscalls
    buffer_pos: usize,

    pub fn init(file: std.fs.File) CsvWriter {
        return CsvWriter{
            .file = file,
            .delimiter = ',',
            .buffer = undefined,
            .buffer_pos = 0,
        };
    }

    pub fn writeToBuffer(self: *CsvWriter, data: []const u8) !void {
        var remaining = data;
        while (remaining.len > 0) {
            const space_left = self.buffer.len - self.buffer_pos;
            if (space_left == 0) {
                try self.flush();
                continue;
            }

            const to_copy = @min(remaining.len, space_left);
            @memcpy(self.buffer[self.buffer_pos..][0..to_copy], remaining[0..to_copy]);
            self.buffer_pos += to_copy;
            remaining = remaining[to_copy..];
        }
    }

    pub fn writeRecord(self: *CsvWriter, fields: []const []const u8) !void {
        for (fields, 0..) |field, i| {
            if (i > 0) {
                try self.writeToBuffer(&[_]u8{self.delimiter});
            }

            // Check if field needs quoting
            const needs_quotes = std.mem.indexOfAny(u8, field, ",\"\r\n") != null;
            if (needs_quotes) {
                try self.writeToBuffer("\"");
                // Escape quotes
                for (field) |c| {
                    if (c == '"') {
                        try self.writeToBuffer("\"\"");
                    } else {
                        try self.writeToBuffer(&[_]u8{c});
                    }
                }
                try self.writeToBuffer("\"");
            } else {
                try self.writeToBuffer(field);
            }
        }
        try self.writeToBuffer("\n");
    }

    pub fn flush(self: *CsvWriter) !void {
        if (self.buffer_pos > 0) {
            try self.file.writeAll(self.buffer[0..self.buffer_pos]);
            self.buffer_pos = 0;
        }
    }
};

/// Returns true if `s` is a valid JSON number literal (integer or float).
/// Used to write numeric CSV values unquoted in JSON output, matching DuckDB behaviour.
pub fn isJsonNumber(s: []const u8) bool {
    if (s.len == 0) return false;
    var i: usize = 0;
    if (s[i] == '-') {
        i += 1;
        if (i >= s.len) return false;
    }
    if (s[i] == '0') {
        i += 1;
    } else {
        if (s[i] < '1' or s[i] > '9') return false;
        i += 1;
        while (i < s.len and s[i] >= '0' and s[i] <= '9') i += 1;
    }
    if (i < s.len and s[i] == '.') {
        i += 1;
        if (i >= s.len or s[i] < '0' or s[i] > '9') return false;
        while (i < s.len and s[i] >= '0' and s[i] <= '9') i += 1;
    }
    if (i < s.len and (s[i] == 'e' or s[i] == 'E')) {
        i += 1;
        if (i < s.len and (s[i] == '+' or s[i] == '-')) i += 1;
        if (i >= s.len or s[i] < '0' or s[i] > '9') return false;
        while (i < s.len and s[i] >= '0' and s[i] <= '9') i += 1;
    }
    return i == s.len;
}

/// Write a JSON-escaped version of `s` into `out` (without surrounding quotes).
pub fn appendJsonEscapedToList(out: *std.ArrayList(u8), allocator: Allocator, s: []const u8) !void {
    var segment_start: usize = 0;
    for (s, 0..) |c, ci| {
        var is_unicode_escape = false;
        var escape_seq: []const u8 = undefined;
        switch (c) {
            '"' => escape_seq = "\\\"",
            '\\' => escape_seq = "\\\\",
            '\n' => escape_seq = "\\n",
            '\r' => escape_seq = "\\r",
            '\t' => escape_seq = "\\t",
            0x00...0x08, 0x0B...0x0C, 0x0E...0x1F => is_unicode_escape = true,
            else => continue,
        }
        if (ci > segment_start) try out.appendSlice(allocator, s[segment_start..ci]);
        if (is_unicode_escape) {
            var buf: [6]u8 = undefined;
            const encoded = std.fmt.bufPrint(&buf, "\\u{X:0>4}", .{c}) catch unreachable;
            try out.appendSlice(allocator, encoded);
        } else {
            try out.appendSlice(allocator, escape_seq);
        }
        segment_start = ci + 1;
    }
    if (segment_start < s.len) try out.appendSlice(allocator, s[segment_start..]);
}

/// Allocate a JSON key fragment like `"colname":` (caller must free with `allocator`).
pub fn allocJsonKeyFragment(allocator: Allocator, key: []const u8) ![]const u8 {
    var out = std.ArrayList(u8).empty;
    defer out.deinit(allocator);
    try out.append(allocator, '"');
    try appendJsonEscapedToList(&out, allocator, key);
    try out.appendSlice(allocator, "\":");
    return try out.toOwnedSlice(allocator);
}

/// JSON output writer.  Produces either a JSON array (format == .json) or
/// newline-delimited JSON (format == .jsonl) from structured record data.
///
/// Usage:
///   1. Call setHeader() with column names before writing any records.
///   2. Call writeRecord() for each row.
///   3. Call finish() to emit the closing bracket for .json mode.
///   4. Call flush() to drain the write buffer.
pub const JsonWriter = struct {
    file: std.fs.File,
    mode: options_mod.OutputFormat, // .json or .jsonl
    /// Column names — a borrowed slice valid for the writer's lifetime.
    header: []const []const u8,
    /// Pre-escaped key fragments like `"col":`, allocated once in setHeader().
    header_key_fragments: []const []const u8,
    /// True until the first record is written (used to emit "[" prefix).
    first_record: bool,
    buffer: [1048576]u8, // 1 MB write buffer
    buffer_pos: usize,

    pub fn init(file: std.fs.File, mode: options_mod.OutputFormat) JsonWriter {
        return JsonWriter{
            .file = file,
            .mode = mode,
            .header = &[_][]const u8{},
            .header_key_fragments = &[_][]const u8{},
            .first_record = true,
            .buffer = undefined,
            .buffer_pos = 0,
        };
    }

    pub fn deinit(self: *JsonWriter) void {
        self.freeHeaderKeyFragments();
    }

    /// Store column names (borrowed — must outlive the writer).
    pub fn setHeader(self: *JsonWriter, fields: []const []const u8) !void {
        self.freeHeaderKeyFragments();
        self.header = fields;
        if (fields.len == 0) return;

        const allocator = std.heap.page_allocator;
        const fragments = try allocator.alloc([]const u8, fields.len);
        errdefer allocator.free(fragments);

        var built: usize = 0;
        errdefer {
            for (fragments[0..built]) |frag| allocator.free(frag);
        }

        for (fields, 0..) |key, i| {
            fragments[i] = try allocJsonKeyFragment(allocator, key);
            built += 1;
        }
        self.header_key_fragments = fragments;
    }

    /// Write one data row as a JSON object.
    pub fn writeRecord(self: *JsonWriter, fields: []const []const u8) !void {
        if (self.mode == .json) {
            if (self.first_record) {
                try self.writeToBuffer("[\n");
            } else {
                try self.writeToBuffer(",\n");
            }
        }
        self.first_record = false;

        try self.writeToBuffer("{");
        for (self.header_key_fragments, 0..) |key_fragment, i| {
            if (i > 0) try self.writeToBuffer(",");
            try self.writeToBuffer(key_fragment);
            if (i < fields.len) {
                try self.writeJsonValue(fields[i]);
            } else {
                try self.writeToBuffer("\"\"");
            }
        }
        try self.writeToBuffer("}");
        if (self.mode == .jsonl) try self.writeToBuffer("\n");
    }

    /// Write a pre-formatted JSON object line (used by ORDER BY sorted output).
    /// `line` must already be a valid JSON object string without trailing newline.
    pub fn writeRawLine(self: *JsonWriter, line: []const u8) !void {
        if (self.mode == .json) {
            if (self.first_record) {
                try self.writeToBuffer("[\n");
            } else {
                try self.writeToBuffer(",\n");
            }
        }
        self.first_record = false;
        try self.writeToBuffer(line);
        if (self.mode == .jsonl) try self.writeToBuffer("\n");
    }

    /// Finalise output: emit the closing `]` for .json mode.
    pub fn finish(self: *JsonWriter) !void {
        if (self.mode == .json) {
            if (!self.first_record) {
                try self.writeToBuffer("\n]");
            } else {
                try self.writeToBuffer("[]");
            }
            try self.writeToBuffer("\n");
        }
    }

    pub fn flush(self: *JsonWriter) !void {
        if (self.buffer_pos > 0) {
            try self.file.writeAll(self.buffer[0..self.buffer_pos]);
            self.buffer_pos = 0;
        }
    }

    pub fn writeToBuffer(self: *JsonWriter, data: []const u8) !void {
        var remaining = data;
        while (remaining.len > 0) {
            const space_left = self.buffer.len - self.buffer_pos;
            if (space_left == 0) {
                try self.flush();
                continue;
            }
            const to_copy = @min(remaining.len, space_left);
            @memcpy(self.buffer[self.buffer_pos..][0..to_copy], remaining[0..to_copy]);
            self.buffer_pos += to_copy;
            remaining = remaining[to_copy..];
        }
    }

    fn writeJsonString(self: *JsonWriter, s: []const u8) !void {
        try self.writeToBuffer("\"");
        var segment_start: usize = 0;
        for (s, 0..) |c, i| {
            var is_unicode_escape = false;
            var escape_seq: []const u8 = undefined;

            switch (c) {
                '"' => escape_seq = "\\\"",
                '\\' => escape_seq = "\\\\",
                '\n' => escape_seq = "\\n",
                '\r' => escape_seq = "\\r",
                '\t' => escape_seq = "\\t",
                0x00...0x08, 0x0B...0x0C, 0x0E...0x1F => {
                    is_unicode_escape = true;
                },
                else => continue,
            }

            if (i > segment_start) {
                try self.writeToBuffer(s[segment_start..i]);
            }
            if (is_unicode_escape) {
                // `\uXXXX` is exactly 6 bytes; bufPrint cannot fail with this buffer size.
                var buf: [6]u8 = undefined;
                const encoded = std.fmt.bufPrint(&buf, "\\u{X:0>4}", .{c}) catch unreachable;
                try self.writeToBuffer(encoded);
            } else {
                try self.writeToBuffer(escape_seq);
            }
            segment_start = i + 1;
        }
        if (segment_start < s.len) {
            try self.writeToBuffer(s[segment_start..]);
        }
        try self.writeToBuffer("\"");
    }

    /// Write `s` as a JSON value: unquoted if it is a number, quoted+escaped otherwise.
    fn writeJsonValue(self: *JsonWriter, s: []const u8) !void {
        if (isJsonNumber(s)) {
            try self.writeToBuffer(s);
        } else {
            try self.writeJsonString(s);
        }
    }

    fn freeHeaderKeyFragments(self: *JsonWriter) void {
        if (self.header_key_fragments.len == 0) return;
        const allocator = std.heap.page_allocator;
        for (self.header_key_fragments) |frag| allocator.free(frag);
        allocator.free(self.header_key_fragments);
        self.header_key_fragments = &[_][]const u8{};
    }
};

/// Format-aware record writer that wraps either a CsvWriter or a JsonWriter.
///
/// This is the single output abstraction used by all engine paths.  Callers:
///   1. var writer = RecordWriter.init(output_file, opts);
///   2. try writer.writeHeader(header, opts.no_header);
///   3. try writer.writeRecord(row);          // for each row
///   4. try writer.finish();                  // finalise (closes JSON array etc.)
///   5. try writer.flush();                   // drain write buffer
pub const RecordWriter = union(enum) {
    csv: CsvWriter,
    json: JsonWriter,

    /// Create a writer appropriate for the output format specified in `opts`.
    pub fn init(file: std.fs.File, opts: options_mod.Options) RecordWriter {
        return switch (opts.format) {
            .csv => blk: {
                var w = CsvWriter.init(file);
                w.delimiter = opts.delimiter;
                break :blk RecordWriter{ .csv = w };
            },
            .json => RecordWriter{ .json = JsonWriter.init(file, .json) },
            .jsonl => RecordWriter{ .json = JsonWriter.init(file, .jsonl) },
        };
    }

    pub fn deinit(self: *RecordWriter) void {
        switch (self.*) {
            .csv => {},
            .json => |*w| w.deinit(),
        }
    }

    /// Write the header row.
    /// For CSV: writes the row unless `no_header` is true.
    /// For JSON/JSONL: stores column names for use as object keys (ignores `no_header`).
    pub fn writeHeader(self: *RecordWriter, fields: []const []const u8, no_header: bool) !void {
        switch (self.*) {
            .csv => |*w| {
                if (!no_header) try w.writeRecord(fields);
            },
            .json => |*w| try w.setHeader(fields),
        }
    }

    /// Write a data row.
    pub fn writeRecord(self: *RecordWriter, fields: []const []const u8) !void {
        switch (self.*) {
            .csv => |*w| try w.writeRecord(fields),
            .json => |*w| try w.writeRecord(fields),
        }
    }

    /// Write a pre-formatted line directly.
    /// For CSV: `line` is written verbatim followed by "\n".
    /// For JSON/JSONL: `line` must be a JSON object string (no trailing newline).
    pub fn writeRawLine(self: *RecordWriter, line: []const u8) !void {
        switch (self.*) {
            .csv => |*w| {
                try w.writeToBuffer(line);
                try w.writeToBuffer("\n");
            },
            .json => |*w| try w.writeRawLine(line),
        }
    }

    /// Finalise output (emits closing `]` for JSON mode).
    pub fn finish(self: *RecordWriter) !void {
        switch (self.*) {
            .csv => {},
            .json => |*w| try w.finish(),
        }
    }

    /// Drain the internal write buffer.
    pub fn flush(self: *RecordWriter) !void {
        switch (self.*) {
            .csv => |*w| try w.flush(),
            .json => |*w| try w.flush(),
        }
    }
};

test "csv reader simple" {
    const allocator = std.testing.allocator;

    // Create a temporary file
    const file_path = "test_csv_simple.csv";
    var file = try std.fs.cwd().createFile(file_path, .{ .read = true });
    defer {
        file.close();
        std.fs.cwd().deleteFile(file_path) catch {};
    }

    // Write CSV data
    try file.writeAll("name,age\nAlice,30\nBob,25\n");
    try file.seekTo(0);

    // Read CSV
    var reader = CsvReader.init(allocator, file);

    // Read header
    const header = (try reader.readRecord()).?;
    defer reader.freeRecord(header);
    try std.testing.expectEqualStrings("name", header[0]);
    try std.testing.expectEqualStrings("age", header[1]);

    // Read first row
    const row1 = (try reader.readRecord()).?;
    defer reader.freeRecord(row1);
    try std.testing.expectEqualStrings("Alice", row1[0]);
    try std.testing.expectEqualStrings("30", row1[1]);

    // Read second row
    const row2 = (try reader.readRecord()).?;
    defer reader.freeRecord(row2);
    try std.testing.expectEqualStrings("Bob", row2[0]);
    try std.testing.expectEqualStrings("25", row2[1]);

    // No more rows
    try std.testing.expect(try reader.readRecord() == null);
}
