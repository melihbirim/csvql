const std = @import("std");
const builtin = @import("builtin");
const parser = @import("parser.zig");
const csv = @import("csv.zig");
const fast_sort = @import("fast_sort.zig");
const options_mod = @import("options.zig");
const arena_buffer = @import("arena_buffer.zig");
const simd = @import("simd.zig");
const Allocator = std.mem.Allocator;

fn mapFile(allocator: Allocator, file: std.fs.File, size: u64) ![]const u8 {
    if (builtin.os.tag == .windows) {
        return file.readToEndAlloc(allocator, @intCast(size));
    }
    const mapped = try std.posix.mmap(null, @intCast(size), std.posix.PROT.READ, .{ .TYPE = .SHARED }, file.handle, 0);
    std.posix.madvise(mapped.ptr, mapped.len, std.posix.MADV.SEQUENTIAL) catch {};
    return mapped;
}

fn unmapFile(allocator: Allocator, data: []const u8) void {
    if (builtin.os.tag == .windows) {
        allocator.free(data);
    } else {
        std.posix.munmap(@alignCast(data));
    }
}
/// Collapse `""` escape pairs to `"` within an already quote-stripped field.
/// Only called when the field is known to contain `""` — see issue #89.
fn unescapeQuotesArena(arena: *std.heap.ArenaAllocator, field: []const u8) ![]const u8 {
    const out = try arena.allocator().alloc(u8, field.len);
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

const ArenaBuffer = arena_buffer.ArenaBuffer;
const appendJsonStringToArena = arena_buffer.appendJsonStringToArena;

/// Sort entry for ORDER BY — uses fast_sort SortKey
const MmapSortEntry = fast_sort.SortKey;

/// A row's sort key + output line recorded as (start, len) offsets into the
/// ORDER BY arena, rather than live slices.
///
/// `ArenaBuffer.append` may realloc its backing buffer (see arena_buffer.zig),
/// which invalidates any slice returned by an earlier `append` call — not just
/// within the current row, but for every row appended before it, since later
/// rows keep appending to and potentially reallocating the same shared arena.
/// Offsets stay valid across reallocation; convert to slices only once all
/// rows have been appended and the arena is done growing (issue #99).
const PendingSortEntry = struct {
    numeric_key: f64,
    sort_key_start: usize,
    sort_key_len: usize,
    line_start: usize,
    line_len: usize,
};

/// Append one row's sort key + rendered output line to the ORDER BY arena,
/// and push the corresponding offsets onto `entries`.
fn appendSortEntry(
    a: *ArenaBuffer,
    entries: *std.ArrayList(PendingSortEntry),
    allocator: Allocator,
    format: options_mod.OutputFormat,
    output_row: []const []const u8,
    output_header: []const []const u8,
    order_by_col_idx: usize,
) !void {
    const sort_key_start = a.pos;
    _ = try a.append(output_row[order_by_col_idx]);
    const sort_key_end = a.pos;
    const numeric_key = std.fmt.parseFloat(f64, a.data[sort_key_start..sort_key_end]) catch std.math.nan(f64);

    const line_buf_start = a.pos;
    switch (format) {
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
                try appendJsonStringToArena(a, output_header[i]);
                _ = try a.append(":");
                try appendJsonStringToArena(a, field);
            }
            _ = try a.append("}");
        },
    }
    try entries.append(allocator, .{
        .numeric_key = numeric_key,
        .sort_key_start = sort_key_start,
        .sort_key_len = sort_key_end - sort_key_start,
        .line_start = line_buf_start,
        .line_len = a.pos - line_buf_start,
    });
}

pub fn executeMapped(
    allocator: Allocator,
    query: parser.Query,
    input_file: std.fs.File,
    output_file: std.fs.File,
    opts: options_mod.Options,
) !void {
    const file_size = (try input_file.stat()).size;

    const data = try mapFile(allocator, input_file, file_size);
    defer unmapFile(allocator, data);

    // Scratch arena for the rare field containing an escaped "" quote pair,
    // which simd.parseCSVFieldsStatic returns unescaped (issue #89). Reset
    // every row; the common case allocates nothing.
    var unescape_arena = std.heap.ArenaAllocator.init(allocator);
    defer unescape_arena.deinit();

    // Find end of header line. Strip a leading UTF-8 BOM (Excel's "CSV UTF-8"
    // export writes one) so it doesn't glue onto the first column name (#103).
    const bom_len: usize = if (std.mem.startsWith(u8, data, "\xEF\xBB\xBF")) 3 else 0;
    const header_end = std.mem.indexOfScalar(u8, data, '\n') orelse return error.NoHeader;
    const header_line_raw = data[bom_len..header_end];
    // Strip trailing \r for CRLF files
    const header_line = if (header_line_raw.len > 0 and header_line_raw[header_line_raw.len - 1] == '\r') header_line_raw[0 .. header_line_raw.len - 1] else header_line_raw;

    // Parse header
    var header = std.ArrayList([]const u8){};
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

    for (header.items, 0..) |col_name_raw, idx| {
        const col_name = std.mem.trim(u8, col_name_raw, " \t\r\n"); // trim header whitespace (#101)
        const lower_name = try allocator.alloc(u8, col_name.len);
        _ = std.ascii.lowerString(lower_name, col_name);
        lower_header[idx] = lower_name;
        _ = try column_map.getOrPutValue(lower_name, idx); // first occurrence of a duplicate header name wins (#102)
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

    var output_header = std.ArrayList([]const u8){};
    defer output_header.deinit(allocator);

    if (query.all_columns) {
        for (output_indices.items) |idx| {
            try output_header.append(allocator, std.mem.trim(u8, header.items[idx], " \t\r\n")); // trim header whitespace (#101)
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

    if (query.where_expr) |we| try parser.validateWhereExprColumns(we, lower_header);

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
            // Unresolved column: error, don't silently treat as zero matches (#138-class bug).
            if (where_column_idx == null) return error.ColumnNotFound;
        }
    }

    // ORDER BY support
    var sort_entries: ?std.ArrayList(PendingSortEntry) = null;
    var arena: ?ArenaBuffer = null;
    var order_by_col_idx: ?usize = null;
    defer {
        if (sort_entries) |*entries| entries.deinit(allocator);
        if (arena) |*a| a.deinit();
    }

    if (query.order_by) |order_by| {
        sort_entries = std.ArrayList(PendingSortEntry){};
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
    var distinct_key_buf = std.ArrayListUnmanaged(u8){};
    defer distinct_key_buf.deinit(allocator);

    // Pre-allocate output row buffer (reused across all rows)
    var output_row = try allocator.alloc([]const u8, output_indices.items.len);
    defer allocator.free(output_row);

    // Process data starting after header
    const data_start = header_end + 1;
    var rows_written: i32 = 0;

    // Split into lines using bulk operations
    var line_start: usize = data_start;
    while (line_start < data.len) {
        // Fused single-pass scan (#139): finds the record end, every comma
        // position, and whether a quote byte is present all in one SIMD
        // sweep, instead of findRecordEnd's own newline+quote scan followed
        // by parseCSVFieldsStatic's separate comma scan over the same bytes.
        // Falls back to the original two-call path (still correct, just
        // slower) whenever a quote is present.
        var field_buf: [256][]const u8 = undefined;
        var comma_positions: [256]usize = undefined;
        const scan = simd.scanRecordFused(data, line_start, opts.delimiter, &comma_positions);

        var line_end: usize = undefined;
        var field_count: usize = undefined;

        if (scan.had_quote) {
            line_end = (csv.findRecordEnd(data, line_start, opts.delimiter) orelse data.len) - line_start;
            var line = data[line_start..][0..line_end];
            if (line.len > 0 and line[line.len - 1] == '\r') {
                line = line[0 .. line.len - 1];
            }
            field_count = if (line.len > 0)
                simd.parseCSVFieldsStatic(line, &field_buf, opts.delimiter) catch break
            else
                0;
        } else {
            const abs_end = scan.end orelse data.len;
            line_end = abs_end - line_start;
            var content_end = abs_end;
            if (content_end > line_start and data[content_end - 1] == '\r') content_end -= 1;
            if (content_end > line_start) {
                var start = line_start;
                var count: usize = 0;
                for (comma_positions[0..scan.comma_count]) |p| {
                    field_buf[count] = data[start..p];
                    count += 1;
                    start = p + 1;
                }
                field_buf[count] = data[start..content_end];
                count += 1;
                field_count = count;
            } else {
                field_count = 0;
            }
        }

        if (field_count > 0) {
            const fields = field_buf[0..field_count];
            _ = unescape_arena.reset(.retain_capacity);
            for (fields) |*field| {
                if (std.mem.indexOf(u8, field.*, "\"\"") != null) {
                    field.* = unescapeQuotesArena(&unescape_arena, field.*) catch field.*;
                }
            }

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
                        // WHERE references a column that isn't a plain header
                        // name — erroring is correct here; silently treating
                        // this as "zero rows match" would be indistinguishable
                        // from a genuine negative result to the caller (#138).
                        return error.ColumnNotFound;
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
                distinct_key_buf.clearRetainingCapacity();
                for (output_row, 0..) |field, fi| {
                    if (fi > 0) try distinct_key_buf.append(allocator, 0);
                    try distinct_key_buf.appendSlice(allocator, field);
                }
                const row_key = distinct_key_buf.items;
                if (distinct_seen.contains(row_key)) {
                    line_start += line_end + 1;
                    continue;
                }
                try distinct_seen.put(try distinct_arena.allocator().dupe(u8, row_key), {});
            }

            if (sort_entries) |*entries| {
                try appendSortEntry(&(arena.?), entries, allocator, opts.format, output_row, output_header.items, order_by_col_idx.?);
                rows_written += 1;
            } else {
                if (query.limit == 0) break; // stop before the first write (#111)
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
            // The arena is done growing now that every row has been appended —
            // safe to materialize final slices from the recorded offsets (issue #99).
            const a = &(arena.?);
            const materialized = try allocator.alloc(MmapSortEntry, entries.items.len);
            defer allocator.free(materialized);
            for (entries.items, 0..) |pending, i| {
                materialized[i] = fast_sort.makeSortKey(
                    pending.numeric_key,
                    a.data[pending.sort_key_start..][0..pending.sort_key_len],
                    a.data[pending.line_start..][0..pending.line_len],
                );
            }

            const limit: ?usize = if (query.limit >= 0) @intCast(query.limit) else null;
            const sorted = try fast_sort.sortEntries(
                allocator,
                materialized,
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

// TDD Test: DISTINCT dedup key must not silently truncate long rows — two
// rows that share the same first 8192 bytes but differ after that must NOT
// be collapsed into one (issue #97).
test "DISTINCT does not falsely collapse rows longer than the dedup key buffer" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const prefix = "A" ** 9000;
    const data = try std.fmt.allocPrint(allocator, "val\n{s}X\n{s}Y\n", .{ prefix, prefix });
    defer allocator.free(data);

    const in_file = try tmp.dir.createFile("in.csv", .{ .read = true });
    defer in_file.close();
    try in_file.writeAll(data);
    try in_file.seekTo(0);

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try tmp.dir.realpath("in.csv", &path_buf);
    const sql = try std.fmt.allocPrint(allocator, "SELECT DISTINCT val FROM '{s}'", .{path});
    defer allocator.free(sql);
    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out_file.close();
    try executeMapped(allocator, query, in_file, out_file, .{});

    try out_file.seekTo(0);
    const out = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(out);

    var row_count: usize = 0;
    for (out) |c| if (c == '\n') {
        row_count += 1;
    };
    // header + 2 distinct rows
    try std.testing.expectEqual(@as(usize, 3), row_count);
}

// TDD Test: sort-key/line offsets recorded by appendSortEntry must still
// resolve to the correct bytes after a LATER row's append forces ArenaBuffer
// to realloc the shared backing buffer — a raw slice captured at append time
// would dangle once any subsequent row triggers growth (issue #99).
test "appendSortEntry offsets survive arena realloc from a later row" {
    const allocator = std.testing.allocator;

    // Tiny initial size guarantees appending row2 forces a realloc that would
    // invalidate any slice captured while appending row1.
    var a = try ArenaBuffer.init(allocator, 4);
    defer a.deinit();
    var entries = std.ArrayList(PendingSortEntry){};
    defer entries.deinit(allocator);

    const header = &[_][]const u8{ "id", "val" };

    var row1 = [_][]const u8{ "1", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" };
    try appendSortEntry(&a, &entries, allocator, .csv, &row1, header, 1);

    var row2 = [_][]const u8{ "2", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" };
    try appendSortEntry(&a, &entries, allocator, .csv, &row2, header, 1);

    // Materialize only after all rows are appended, per the documented
    // ArenaBuffer offset pattern.
    const e1 = entries.items[0];
    const e2 = entries.items[1];
    try std.testing.expectEqualStrings(row1[1], a.data[e1.sort_key_start..][0..e1.sort_key_len]);
    try std.testing.expectEqualStrings("1,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", a.data[e1.line_start..][0..e1.line_len]);
    try std.testing.expectEqualStrings(row2[1], a.data[e2.sort_key_start..][0..e2.sort_key_len]);
    try std.testing.expectEqualStrings("2,bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", a.data[e2.line_start..][0..e2.line_len]);
}
