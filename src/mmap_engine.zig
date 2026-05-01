const std = @import("std");
const parser = @import("parser.zig");
const csv = @import("csv.zig");
const fast_sort = @import("fast_sort.zig");
const options_mod = @import("options.zig");
const arena_buffer = @import("arena_buffer.zig");
const simd = @import("simd.zig");
const Allocator = std.mem.Allocator;
const ArenaBuffer = arena_buffer.ArenaBuffer;
const appendJsonStringToArena = arena_buffer.appendJsonStringToArena;

/// Sort entry for ORDER BY — uses fast_sort SortKey
const MmapSortEntry = fast_sort.SortKey;

pub fn executeMapped(
    allocator: Allocator,
    query: parser.Query,
    input_file: std.fs.File,
    output_file: std.fs.File,
    opts: options_mod.Options,
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
    std.posix.madvise(mapped.ptr, mapped.len, std.posix.MADV.SEQUENTIAL) catch {};

    const data = mapped[0..file_size];

    // Find end of header line
    const header_end = std.mem.indexOfScalar(u8, data, '\n') orelse return error.NoHeader;
    const header_line_raw = data[0..header_end];
    // Strip trailing \r for CRLF files
    const header_line = if (header_line_raw.len > 0 and header_line_raw[header_line_raw.len - 1] == '\r') header_line_raw[0 .. header_line_raw.len - 1] else header_line_raw;

    // Parse header
    var header = std.ArrayList([]const u8).empty;
    defer header.deinit(allocator);

    var header_iter = std.mem.splitScalar(u8, header_line, opts.delimiter);
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
    var output_indices = std.ArrayList(usize).empty;
    defer output_indices.deinit(allocator);

    if (query.all_columns) {
        for (0..header.items.len) |idx| {
            try output_indices.append(allocator, idx);
        }
    } else {
        for (query.columns) |col| {
            // Strip AS alias: "name AS n" → look up "name", output as "n"
            const expr_part = if (std.ascii.indexOfIgnoreCase(col, " as ")) |as_idx|
                std.mem.trim(u8, col[0..as_idx], &std.ascii.whitespace)
            else
                col;
            const lower_col = try allocator.alloc(u8, expr_part.len);
            defer allocator.free(lower_col);
            _ = std.ascii.lowerString(lower_col, expr_part);
            const idx = column_map.get(lower_col) orelse return error.ColumnNotFound;
            try output_indices.append(allocator, idx);
        }
    }

    // Write output header
    var writer = csv.RecordWriter.init(output_file, opts);
    defer writer.deinit();

    var output_header = std.ArrayList([]const u8).empty;
    defer output_header.deinit(allocator);

    if (query.all_columns) {
        for (output_indices.items) |idx| {
            try output_header.append(allocator, header.items[idx]);
        }
    } else {
        for (query.columns) |col| {
            // Use alias as the output column name when present
            const out_name = if (std.ascii.indexOfIgnoreCase(col, " as ")) |as_idx|
                std.mem.trim(u8, col[as_idx + 4 ..], &std.ascii.whitespace)
            else
                col;
            try output_header.append(allocator, out_name);
        }
    }
    try writer.writeHeader(output_header.items, opts.no_header);

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
        sort_entries = std.ArrayList(MmapSortEntry).empty;
        arena = try ArenaBuffer.init(allocator, 16 * 1024 * 1024); // 16MB initial for large result sets

        // Positional ORDER BY: "ORDER BY 1" → position 0 in output
        const pos_num = std.fmt.parseInt(usize, order_by.column, 10) catch 0;
        if (pos_num >= 1 and pos_num <= output_header.items.len) {
            order_by_col_idx = pos_num - 1;
        }

        if (order_by_col_idx == null) {
            // Match against output header (supports AS aliases)
            for (output_header.items, 0..) |hdr, pos| {
                const lower_hdr = try allocator.alloc(u8, hdr.len);
                defer allocator.free(lower_hdr);
                _ = std.ascii.lowerString(lower_hdr, hdr);
                if (std.mem.eql(u8, lower_hdr, order_by.column)) {
                    order_by_col_idx = pos;
                    break;
                }
            }
        }

        if (order_by_col_idx == null) {
            // Fall back to raw column name match
            for (output_indices.items, 0..) |out_idx, pos| {
                if (out_idx < lower_header.len) {
                    if (std.mem.eql(u8, lower_header[out_idx], order_by.column)) {
                        order_by_col_idx = pos;
                        break;
                    }
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
            // Parse fields as slices into mmap data (quote-aware, zero-copy where possible)
            var field_buf: [256][]const u8 = undefined;
            const field_count = simd.parseCSVFieldsStatic(line, &field_buf, opts.delimiter) catch break;
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
                                    .like => parser.matchLike(field_value, comp.value),
                                    .ilike => parser.matchILike(field_value, comp.value),
                                    .between, .is_null, .is_not_null => parser.compareValues(comp, field_value),
                                };
                            } else {
                                // Delegate to compareValues which handles IN, BETWEEN,
                                // IS NULL/NOT NULL, LIKE, ILIKE, and normal comparisons.
                                matches = parser.compareValues(comp, field_value);
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
                    if (!parser.evaluateDirect(expr, fields, lower_header)) {
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
                // Buffer for ORDER BY: store sort key + output line in arena
                const a = &(arena.?);
                const sort_key = try a.append(output_row[order_by_col_idx.?]);
                const numeric_key = std.fmt.parseFloat(f64, sort_key) catch std.math.nan(f64);
                const line_buf_start = a.pos;
                switch (opts.format) {
                    .csv => {
                        for (output_row, 0..) |field, i| {
                            if (i > 0) _ = try a.append(",");
                            _ = try a.append(field);
                        }
                    },
                    .json, .jsonl => {
                        _ = try a.append("{");
                        for (output_row, 0..) |field, i| {
                            if (i > 0) _ = try a.append(",");
                            try appendJsonStringToArena(a, output_header.items[i]);
                            _ = try a.append(":");
                            try appendJsonStringToArena(a, field);
                        }
                        _ = try a.append("}");
                    },
                }
                const output_line = a.data[line_buf_start..a.pos];
                try entries.append(allocator, fast_sort.makeSortKey(
                    numeric_key,
                    sort_key,
                    output_line,
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
                try writer.writeRawLine(entry.line);
            }
        }
    }

    try writer.finish();
    try writer.flush();
}
