const std = @import("std");
const parser = @import("parser.zig");
const csv = @import("csv.zig");
const fast_sort = @import("fast_sort.zig");
const Allocator = std.mem.Allocator;

/// Sort entry for ORDER BY — uses fast_sort SortKey
const MmapSortEntry = fast_sort.SortKey;

/// Arena buffer for building CSV lines
const ArenaBuffer = struct {
    data: []u8,
    pos: usize,
    allocator: Allocator,

    pub fn init(alloc: Allocator, initial_size: usize) !ArenaBuffer {
        return ArenaBuffer{
            .data = try alloc.alloc(u8, initial_size),
            .pos = 0,
            .allocator = alloc,
        };
    }

    pub fn deinit(self: *ArenaBuffer) void {
        self.allocator.free(self.data);
    }

    pub fn append(self: *ArenaBuffer, bytes: []const u8) ![]const u8 {
        if (self.pos + bytes.len > self.data.len) {
            const new_size = @max(self.data.len * 2, self.pos + bytes.len);
            const new_data = try self.allocator.alloc(u8, new_size);
            @memcpy(new_data[0..self.pos], self.data[0..self.pos]);
            self.allocator.free(self.data);
            self.data = new_data;
        }
        const start = self.pos;
        @memcpy(self.data[start .. start + bytes.len], bytes);
        self.pos += bytes.len;
        return self.data[start .. start + bytes.len];
    }
};

/// Memory-mapped parallel CSV processing
pub fn executeMapped(
    allocator: Allocator,
    query: parser.Query,
    input_file: std.fs.File,
    output_file: std.fs.File,
) !void {
    const file_size = (try input_file.stat()).size;

    // Memory-map the entire file
    const mapped = try std.posix.mmap(
        null,
        file_size,
        std.posix.PROT.READ,
        .{ .TYPE = .SHARED },
        input_file.handle,
        0,
    );
    defer std.posix.munmap(mapped);

    const data = mapped[0..file_size];

    // Find end of header line
    const header_end = std.mem.indexOfScalar(u8, data, '\n') orelse return error.NoHeader;
    const header_line = data[0..header_end];

    // Parse header
    var header = std.ArrayList([]const u8){};
    defer header.deinit(allocator);

    var header_iter = std.mem.splitScalar(u8, header_line, ',');
    while (header_iter.next()) |col| {
        try header.append(allocator, col);
    }

    // Build column map
    var column_map = std.StringHashMap(usize).init(allocator);
    defer column_map.deinit();

    var lower_header = try allocator.alloc([]u8, header.items.len);
    defer {
        for (lower_header) |lower_name| {
            allocator.free(lower_name);
        }
        allocator.free(lower_header);
    }

    for (header.items, 0..) |col_name, idx| {
        const lower_name = try allocator.alloc(u8, col_name.len);
        _ = std.ascii.lowerString(lower_name, col_name);
        lower_header[idx] = lower_name;
        try column_map.put(lower_name, idx);
    }

    // Determine output columns
    var output_indices = std.ArrayList(usize){};
    defer output_indices.deinit(allocator);

    if (query.all_columns) {
        for (0..header.items.len) |idx| {
            try output_indices.append(allocator, idx);
        }
    } else {
        for (query.columns) |col| {
            const lower_col = try allocator.alloc(u8, col.len);
            defer allocator.free(lower_col);
            _ = std.ascii.lowerString(lower_col, col);
            const idx = column_map.get(lower_col) orelse return error.ColumnNotFound;
            try output_indices.append(allocator, idx);
        }
    }

    // Write output header
    var writer = csv.CsvWriter.init(output_file);
    var output_header = std.ArrayList([]const u8){};
    defer output_header.deinit(allocator);

    for (output_indices.items) |idx| {
        try output_header.append(allocator, header.items[idx]);
    }
    try writer.writeRecord(output_header.items);

    // OPTIMIZATION: Find WHERE column index for fast lookup
    var where_column_idx: ?usize = null;
    if (query.where_expr) |expr| {
        if (expr == .comparison) {
            const comp = expr.comparison;
            for (lower_header, 0..) |lower_name, idx| {
                if (std.mem.eql(u8, lower_name, comp.column)) {
                    where_column_idx = idx;
                    break;
                }
            }
        }
    }

    // ORDER BY support
    var sort_entries: ?std.ArrayList(MmapSortEntry) = null;
    var arena: ?ArenaBuffer = null;
    var order_by_col_idx: ?usize = null;
    defer {
        if (sort_entries) |*entries| entries.deinit(allocator);
        if (arena) |*a| a.deinit();
    }

    if (query.order_by) |order_by| {
        sort_entries = std.ArrayList(MmapSortEntry){};
        arena = try ArenaBuffer.init(allocator, 4 * 1024 * 1024); // 4MB initial for larger files
        for (output_indices.items, 0..) |out_idx, pos| {
            if (out_idx < lower_header.len) {
                if (std.mem.eql(u8, lower_header[out_idx], order_by.column)) {
                    order_by_col_idx = pos;
                    break;
                }
            }
        }
        if (order_by_col_idx == null) {
            return error.OrderByColumnNotFound;
        }
    }

    // DISTINCT dedup state
    var distinct_arena = std.heap.ArenaAllocator.init(allocator);
    defer distinct_arena.deinit();
    var distinct_seen = std.StringHashMap(void).init(allocator);
    defer distinct_seen.deinit();

    // Pre-allocate output row buffer (reused across all rows)
    var output_row = try allocator.alloc([]const u8, output_indices.items.len);
    defer allocator.free(output_row);

    // Process data starting after header
    const data_start = header_end + 1;
    var rows_written: i32 = 0;

    // Split into lines using bulk operations
    var line_start: usize = data_start;
    while (line_start < data.len) {
        const remaining = data[line_start..];
        const line_end = std.mem.indexOfScalar(u8, remaining, '\n') orelse data.len - line_start;

        var line = remaining[0..line_end];
        // Trim \r if present
        if (line.len > 0 and line[line.len - 1] == '\r') {
            line = line[0 .. line.len - 1];
        }

        if (line.len > 0) {
            // Parse fields as slices into mmap data (zero-copy)
            var field_buf: [256][]const u8 = undefined;
            var field_count: usize = 0;
            var field_iter = std.mem.splitScalar(u8, line, ',');
            while (field_iter.next()) |field| {
                if (field_count >= field_buf.len) break;
                field_buf[field_count] = field;
                field_count += 1;
            }
            const fields = field_buf[0..field_count];

            // Fast WHERE evaluation
            if (query.where_expr) |expr| {
                if (expr == .comparison) {
                    const comp = expr.comparison;
                    if (where_column_idx) |col_idx| {
                        if (col_idx < fields.len) {
                            const field_value = fields[col_idx];
                            var matches = false;
                            if (comp.numeric_value) |threshold| {
                                const val = std.fmt.parseFloat(f64, field_value) catch {
                                    line_start += line_end + 1;
                                    continue;
                                };
                                matches = switch (comp.operator) {
                                    .equal => val == threshold,
                                    .not_equal => val != threshold,
                                    .greater => val > threshold,
                                    .greater_equal => val >= threshold,
                                    .less => val < threshold,
                                    .less_equal => val <= threshold,
                                };
                            } else {
                                matches = switch (comp.operator) {
                                    .equal => std.mem.eql(u8, field_value, comp.value),
                                    .not_equal => !std.mem.eql(u8, field_value, comp.value),
                                    else => false,
                                };
                            }
                            if (!matches) {
                                line_start += line_end + 1;
                                continue;
                            }
                        } else {
                            line_start += line_end + 1;
                            continue;
                        }
                    } else {
                        line_start += line_end + 1;
                        continue;
                    }
                } else {
                    // Complex WHERE — use HashMap fallback
                    var row_map = std.StringHashMap([]const u8).init(allocator);
                    defer row_map.deinit();
                    for (lower_header, 0..) |lower_name, idx| {
                        if (idx < fields.len) {
                            try row_map.put(lower_name, fields[idx]);
                        }
                    }
                    if (!parser.evaluate(expr, row_map)) {
                        line_start += line_end + 1;
                        continue;
                    }
                }
            }

            // Project output columns (reuse pre-allocated output_row)
            for (output_indices.items, 0..) |idx, i| {
                output_row[i] = if (idx < fields.len) fields[idx] else "";
            }

            // DISTINCT: skip duplicate rows
            if (query.distinct) {
                var key_buf: [8192]u8 = undefined;
                var klen: usize = 0;
                for (output_row, 0..) |field, fi| {
                    if (fi > 0 and klen < key_buf.len) {
                        key_buf[klen] = 0;
                        klen += 1;
                    }
                    const n = @min(field.len, key_buf.len - klen);
                    if (n > 0) @memcpy(key_buf[klen..][0..n], field[0..n]);
                    klen += n;
                }
                const row_key = key_buf[0..klen];
                if (distinct_seen.contains(row_key)) {
                    line_start += line_end + 1;
                    continue;
                }
                try distinct_seen.put(try distinct_arena.allocator().dupe(u8, row_key), {});
            }

            if (sort_entries) |*entries| {
                // Buffer for ORDER BY: store sort key + CSV line in arena
                const a = &(arena.?);
                const sort_key = try a.append(output_row[order_by_col_idx.?]);
                const numeric_key = std.fmt.parseFloat(f64, sort_key) catch std.math.nan(f64);
                const line_buf_start = a.pos;
                for (output_row, 0..) |field, i| {
                    if (i > 0) _ = try a.append(",");
                    _ = try a.append(field);
                }
                const csv_line = a.data[line_buf_start..a.pos];
                try entries.append(allocator, fast_sort.makeSortKey(
                    numeric_key,
                    sort_key,
                    csv_line,
                ));
                rows_written += 1;
            } else {
                try writer.writeRecord(output_row);
                rows_written += 1;

                if (query.limit >= 0 and rows_written >= query.limit) {
                    break;
                }
                if (@rem(rows_written, 32768) == 0) {
                    try writer.flush();
                }
            }
        }

        line_start += line_end + 1;
    }

    // Sort and write buffered rows if ORDER BY
    if (sort_entries) |*entries| {
        if (query.order_by) |order_by| {
            const limit: ?usize = if (query.limit >= 0) @intCast(query.limit) else null;
            const sorted = try fast_sort.sortEntries(
                allocator,
                entries.items,
                order_by.order == .desc,
                limit,
            );

            var ob_distinct_arena = std.heap.ArenaAllocator.init(allocator);
            defer ob_distinct_arena.deinit();
            var ob_distinct_seen = std.StringHashMap(void).init(allocator);
            defer ob_distinct_seen.deinit();

            for (sorted) |entry| {
                if (query.distinct) {
                    if (ob_distinct_seen.contains(entry.line)) continue;
                    try ob_distinct_seen.put(try ob_distinct_arena.allocator().dupe(u8, entry.line), {});
                }
                try writer.writeToBuffer(entry.line);
                try writer.writeToBuffer("\n");
            }
        }
    }

    try writer.flush();
}
