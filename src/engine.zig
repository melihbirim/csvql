const std = @import("std");
const parser = @import("parser.zig");
const csv = @import("csv.zig");
const bulk_csv = @import("bulk_csv.zig");
const mmap_engine = @import("mmap_engine.zig");
const parallel_mmap = @import("parallel_mmap.zig");
const fast_sort = @import("fast_sort.zig");
const aggregation = @import("aggregation.zig");
const Allocator = std.mem.Allocator;

/// Result row for ORDER BY buffering — uses fast_sort SortKey
const SortEntry = fast_sort.SortKey;

/// Arena buffer for ORDER BY — single large allocation instead of per-field allocs
const ArenaBuffer = struct {
    data: []u8,
    pos: usize,
    allocator: Allocator,

    pub fn init(allocator: Allocator, initial_size: usize) !ArenaBuffer {
        return ArenaBuffer{
            .data = try allocator.alloc(u8, initial_size),
            .pos = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ArenaBuffer) void {
        self.allocator.free(self.data);
    }

    pub fn append(self: *ArenaBuffer, bytes: []const u8) ![]const u8 {
        if (self.pos + bytes.len > self.data.len) {
            // Grow by doubling
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

/// Execute a SQL query on a CSV file
pub fn execute(allocator: Allocator, query: parser.Query, output_file: std.fs.File) !void {
    // Check if reading from stdin
    const is_stdin = std.mem.eql(u8, query.file_path, "-") or std.mem.eql(u8, query.file_path, "stdin");

    if (is_stdin) {
        try executeFromStdin(allocator, query, output_file);
        return;
    }

    // Check for GROUP BY - requires sequential processing
    if (query.group_by.len > 0) {
        try executeGroupBy(allocator, query, output_file);
        return;
    }

    // Open CSV file
    const file = try std.fs.cwd().openFile(query.file_path, .{});
    defer file.close();

    // Check file size for processing strategy
    const file_stat = try file.stat();

    // Use parallel memory-mapped I/O for large files (2+ cores, no LIMIT unless ORDER BY)
    if (file_stat.size > 10 * 1024 * 1024 and (query.limit < 0 or query.limit > 100000 or query.order_by != null)) {
        const num_cores = try std.Thread.getCpuCount();
        if (num_cores > 1) {
            try parallel_mmap.executeParallelMapped(allocator, query, file, output_file);
            return;
        }
    }

    // Use memory-mapped I/O for medium-large files
    if (file_stat.size > 5 * 1024 * 1024) {
        try mmap_engine.executeMapped(allocator, query, file, output_file);
        return;
    }

    // Sequential execution for smaller files
    try executeSequential(allocator, query, file, output_file);
}

/// Execute query sequentially
fn executeSequential(
    allocator: Allocator,
    query: parser.Query,
    input_file: std.fs.File,
    output_file: std.fs.File,
) !void {
    // Use bulk CSV reader for much better performance
    var reader = try bulk_csv.BulkCsvReader.init(allocator, input_file);
    defer reader.deinit();

    var writer = csv.CsvWriter.init(output_file);

    // Read header
    const header = try reader.readRecord() orelse return error.EmptyFile;
    defer reader.freeRecord(header);

    // Build column index map (case-insensitive)
    var column_map = std.StringHashMap(usize).init(allocator);
    defer column_map.deinit();

    // Build lowercase header once for WHERE clause evaluation
    var lower_header = try allocator.alloc([]u8, header.len);
    defer {
        for (lower_header) |lower_name| {
            allocator.free(lower_name);
        }
        allocator.free(lower_header);
    }

    for (header, 0..) |col_name, idx| {
        // Store lowercase version for case-insensitive lookup
        const lower_name = try allocator.alloc(u8, col_name.len);
        _ = std.ascii.lowerString(lower_name, col_name);
        lower_header[idx] = lower_name;
        try column_map.put(lower_name, idx);
    }

    // Determine output columns
    var output_indices = std.ArrayList(usize){};
    defer output_indices.deinit(allocator);

    var output_header = std.ArrayList([]const u8){};
    defer output_header.deinit(allocator);

    if (query.all_columns) {
        for (header, 0..) |col_name, idx| {
            try output_indices.append(allocator, idx);
            try output_header.append(allocator, col_name);
        }
    } else {
        for (query.columns) |col| {
            const lower_col = try allocator.alloc(u8, col.len);
            defer allocator.free(lower_col);
            _ = std.ascii.lowerString(lower_col, col);

            const idx = column_map.get(lower_col) orelse return error.ColumnNotFound;
            try output_indices.append(allocator, idx);
            try output_header.append(allocator, header[idx]);
        }
    }

    // Write output header
    try writer.writeRecord(output_header.items);

    // OPTIMIZATION: Find WHERE column index for fast lookup (avoid HashMap in hot path)
    var where_column_idx: ?usize = null;
    if (query.where_expr) |expr| {
        if (expr == .comparison) {
            const comp = expr.comparison;
            // Find the column index using lowercase header
            for (lower_header, 0..) |lower_name, idx| {
                if (std.mem.eql(u8, lower_name, comp.column)) {
                    where_column_idx = idx;
                    break;
                }
            }
        }
    }

    // Buffer for ORDER BY support
    var sort_entries: ?std.ArrayList(SortEntry) = null;
    var arena: ?ArenaBuffer = null;
    var order_by_column_idx: ?usize = null;
    defer {
        if (sort_entries) |*entries| entries.deinit(allocator);
        if (arena) |*a| a.deinit();
    }

    // If ORDER BY is specified, prepare buffer and find column index
    if (query.order_by) |order_by| {
        sort_entries = std.ArrayList(SortEntry){};
        arena = try ArenaBuffer.init(allocator, 1024 * 1024); // 1MB initial
        // Find the ORDER BY column index in output columns
        // order_by.column is already lowercase from parser
        for (output_indices.items) |out_idx| {
            if (out_idx < lower_header.len) {
                if (std.mem.eql(u8, lower_header[out_idx], order_by.column)) {
                    // Find position in output columns
                    for (output_indices.items, 0..) |idx, pos| {
                        if (idx == out_idx) {
                            order_by_column_idx = pos;
                            break;
                        }
                    }
                    break;
                }
            }
        }
        if (order_by_column_idx == null) {
            return error.OrderByColumnNotFound;
        }
    }

    // Process rows
    var row_count: i32 = 0;
    var rows_written: i32 = 0;

    // Pre-allocate output row buffer (reused across all rows)
    var output_row = try allocator.alloc([]const u8, output_indices.items.len);
    defer allocator.free(output_row);

    while (try reader.readRecordSlices()) |record| {
        // Zero-copy: record slices point into reader buffer, no freeRecord needed
        row_count += 1;

        // OPTIMIZATION: Fast WHERE evaluation using direct index lookup
        if (query.where_expr) |expr| {
            if (expr == .comparison) {
                const comp = expr.comparison;

                // Use precomputed column index for direct access
                if (where_column_idx) |col_idx| {
                    if (col_idx < record.len) {
                        const field_value = record[col_idx];

                        // Fast evaluation without HashMap
                        var matches = false;

                        if (comp.numeric_value) |threshold| {
                            // Numeric comparison
                            const val = std.fmt.parseFloat(f64, field_value) catch {
                                continue; // Skip invalid rows
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
                            // String comparison
                            matches = switch (comp.operator) {
                                .equal => std.mem.eql(u8, field_value, comp.value),
                                .not_equal => !std.mem.eql(u8, field_value, comp.value),
                                else => false, // String doesn't support < > comparisons
                            };
                        }

                        if (!matches) continue;
                    } else {
                        continue; // Column doesn't exist in this row
                    }
                } else {
                    // Column not found in header, skip all rows
                    continue;
                }
            } else {
                // Complex expressions (AND/OR/NOT) still use HashMap
                // TODO: Optimize these as well
                var row_map = std.StringHashMap([]const u8).init(allocator);
                defer row_map.deinit();

                for (lower_header, 0..) |lower_name, idx| {
                    if (idx < record.len) {
                        try row_map.put(lower_name, record[idx]);
                    }
                }

                if (!parser.evaluate(expr, row_map)) {
                    continue;
                }
            }
        }

        // Project selected columns (reuse pre-allocated output_row)
        for (output_indices.items, 0..) |idx, i| {
            output_row[i] = if (idx < record.len) record[idx] else "";
        }

        // Either buffer for ORDER BY or write directly
        if (sort_entries) |*entries| {
            // Build CSV line into arena and store sort key
            const a = &(arena.?);
            const sort_key = try a.append(output_row[order_by_column_idx.?]);

            // Build the CSV line: field1,field2,...
            const line_start = a.pos;
            for (output_row, 0..) |field, i| {
                if (i > 0) _ = try a.append(",");
                _ = try a.append(field);
            }
            const line = a.data[line_start..a.pos];

            try entries.append(allocator, fast_sort.makeSortKey(
                std.fmt.parseFloat(f64, sort_key) catch std.math.nan(f64),
                sort_key,
                line,
            ));
            rows_written += 1;
        } else {
            // Write directly (no ORDER BY)
            try writer.writeRecord(output_row);
            rows_written += 1;

            // Check LIMIT
            if (query.limit >= 0 and rows_written >= query.limit) {
                break;
            }

            // Flush periodically (less often with 1MB buffer)
            if (@rem(rows_written, 32768) == 0) {
                try writer.flush();
            }
        }
    }

    // Sort and write buffered rows if ORDER BY is specified
    if (sort_entries) |*entries| {
        if (query.order_by) |order_by| {
            const limit: ?usize = if (query.limit >= 0) @intCast(query.limit) else null;
            const sorted = try fast_sort.sortEntries(
                allocator,
                entries.items,
                order_by.order == .desc,
                limit,
            );

            // Write sorted rows
            for (sorted) |entry| {
                try writer.writeToBuffer(entry.line);
                try writer.writeToBuffer("\n");
            }
        }
    }

    try writer.flush();
}

/// Execute query from stdin
fn executeFromStdin(
    allocator: Allocator,
    query: parser.Query,
    output_file: std.fs.File,
) !void {
    const stdin = std.fs.File{ .handle = std.posix.STDIN_FILENO };
    var reader = csv.CsvReader.init(allocator, stdin);
    var writer = csv.CsvWriter.init(output_file);

    // Read header
    const header = try reader.readRecord() orelse return error.EmptyFile;
    defer reader.freeRecord(header);

    // Build column index map (case-insensitive)
    var column_map = std.StringHashMap(usize).init(allocator);
    defer column_map.deinit();

    // Build lowercase header once for WHERE clause evaluation
    var lower_header = try allocator.alloc([]u8, header.len);
    defer {
        for (lower_header) |lower_name| {
            allocator.free(lower_name);
        }
        allocator.free(lower_header);
    }

    for (header, 0..) |col_name, idx| {
        const lower_name = try allocator.alloc(u8, col_name.len);
        _ = std.ascii.lowerString(lower_name, col_name);
        lower_header[idx] = lower_name;
        try column_map.put(lower_name, idx);
    }

    // Determine output columns
    var output_indices = std.ArrayList(usize){};
    defer output_indices.deinit(allocator);

    var output_header = std.ArrayList([]const u8){};
    defer output_header.deinit(allocator);

    if (query.all_columns) {
        for (header, 0..) |col_name, idx| {
            try output_indices.append(allocator, idx);
            try output_header.append(allocator, col_name);
        }
    } else {
        for (query.columns) |col| {
            const lower_col = try allocator.alloc(u8, col.len);
            defer allocator.free(lower_col);
            _ = std.ascii.lowerString(lower_col, col);

            const idx = column_map.get(lower_col) orelse return error.ColumnNotFound;
            try output_indices.append(allocator, idx);
            try output_header.append(allocator, header[idx]);
        }
    }

    // Write output header
    try writer.writeRecord(output_header.items);

    // OPTIMIZATION: Find WHERE column index for fast lookup (avoid HashMap in hot path)
    var where_column_idx_stdin: ?usize = null;
    if (query.where_expr) |expr| {
        if (expr == .comparison) {
            const comp = expr.comparison;
            // Find the column index using lowercase header
            for (lower_header, 0..) |lower_name, idx| {
                if (std.mem.eql(u8, lower_name, comp.column)) {
                    where_column_idx_stdin = idx;
                    break;
                }
            }
        }
    }

    // Process rows
    var rows_written: i32 = 0;

    while (try reader.readRecord()) |record| {
        defer reader.freeRecord(record);

        // OPTIMIZATION: Fast WHERE evaluation using direct index lookup
        if (query.where_expr) |expr| {
            if (expr == .comparison) {
                const comp = expr.comparison;

                // Use precomputed column index for direct access
                if (where_column_idx_stdin) |col_idx| {
                    if (col_idx < record.len) {
                        const field_value = record[col_idx];

                        // Fast evaluation without HashMap
                        var matches = false;

                        if (comp.numeric_value) |threshold| {
                            // Numeric comparison
                            const val = std.fmt.parseFloat(f64, field_value) catch {
                                continue; // Skip invalid rows
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
                            // String comparison
                            matches = switch (comp.operator) {
                                .equal => std.mem.eql(u8, field_value, comp.value),
                                .not_equal => !std.mem.eql(u8, field_value, comp.value),
                                else => false, // String doesn't support < > comparisons
                            };
                        }

                        if (!matches) continue;
                    } else {
                        continue; // Column doesn't exist in this row
                    }
                } else {
                    // Column not found in header, skip all rows
                    continue;
                }
            } else {
                // Complex expressions (AND/OR/NOT) still use HashMap
                // TODO: Optimize these as well
                var row_map = std.StringHashMap([]const u8).init(allocator);
                defer row_map.deinit();

                for (lower_header, 0..) |lower_name, idx| {
                    if (idx < record.len) {
                        try row_map.put(lower_name, record[idx]);
                    }
                }

                if (!parser.evaluate(expr, row_map)) {
                    continue;
                }
            }
        }

        // Project selected columns
        var output_row = try allocator.alloc([]const u8, output_indices.items.len);
        defer allocator.free(output_row);

        for (output_indices.items, 0..) |idx, i| {
            output_row[i] = if (idx < record.len) record[idx] else "";
        }

        try writer.writeRecord(output_row);
        rows_written += 1;

        if (query.limit >= 0 and rows_written >= query.limit) {
            break;
        }
    }

    try writer.flush();
}

/// Describes how each output column is derived
const ColKind = union(enum) {
    /// Index into the CSV row (regular column)
    regular: usize,
    /// Index into the agg_funcs list (aggregate function)
    aggregate: usize,
};

/// Per-group state: stored key column values + aggregate accumulators
const GroupState = struct {
    /// One value per GROUP BY column (slices into key_arena memory)
    key_values: [][]const u8,
    agg: aggregation.Aggregator,
};

/// Execute GROUP BY query using a hash map keyed by group column values.
/// Supports aggregate functions (COUNT, SUM, AVG, MIN, MAX) via aggregation.zig.
fn executeGroupBy(
    allocator: Allocator,
    query: parser.Query,
    output_file: std.fs.File,
) !void {
    const file = try std.fs.cwd().openFile(query.file_path, .{});
    defer file.close();

    var reader = try bulk_csv.BulkCsvReader.init(allocator, file);
    defer reader.deinit();

    var writer = csv.CsvWriter.init(output_file);

    // Read header row
    const header = try reader.readRecord() orelse return error.EmptyFile;
    defer reader.freeRecord(header);

    // Build lowercase header + column index map for case-insensitive lookup
    var lower_header = try allocator.alloc([]u8, header.len);
    defer {
        for (lower_header) |h| allocator.free(h);
        allocator.free(lower_header);
    }

    var column_map = std.StringHashMap(usize).init(allocator);
    defer column_map.deinit();

    for (header, 0..) |col_name, idx| {
        const lower = try allocator.alloc(u8, col_name.len);
        _ = std.ascii.lowerString(lower, col_name);
        lower_header[idx] = lower;
        try column_map.put(lower, idx);
    }

    // Resolve GROUP BY column indices (into the CSV row)
    var group_indices = try allocator.alloc(usize, query.group_by.len);
    defer allocator.free(group_indices);
    for (query.group_by, 0..) |col, i| {
        const lower = try allocator.alloc(u8, col.len);
        defer allocator.free(lower);
        _ = std.ascii.lowerString(lower, col);
        group_indices[i] = column_map.get(lower) orelse return error.ColumnNotFound;
    }

    // Parse SELECT column list: classify each as regular column or aggregate function
    var col_kinds = std.ArrayList(ColKind){};
    defer col_kinds.deinit(allocator);

    var agg_funcs = std.ArrayList(aggregation.AggregateFunc){};
    defer {
        for (agg_funcs.items) |af| af.deinit(allocator);
        agg_funcs.deinit(allocator);
    }

    // Parallel to agg_funcs: the CSV column index for each aggregate's input column
    var agg_col_indices = std.ArrayList(?usize){};
    defer agg_col_indices.deinit(allocator);

    var out_header = std.ArrayList([]const u8){};
    defer out_header.deinit(allocator);

    if (query.all_columns) {
        // SELECT * with GROUP BY: emit one column per GROUP BY column
        for (group_indices) |cidx| {
            try col_kinds.append(allocator, .{ .regular = cidx });
            try out_header.append(allocator, header[cidx]);
        }
    } else {
        for (query.columns) |col| {
            if (try aggregation.parseAggregateFunc(allocator, col)) |agg_func| {
                const agg_idx = agg_funcs.items.len;
                try agg_funcs.append(allocator, agg_func);

                if (agg_func.column) |agg_col| {
                    const lower = try allocator.alloc(u8, agg_col.len);
                    defer allocator.free(lower);
                    _ = std.ascii.lowerString(lower, agg_col);
                    try agg_col_indices.append(allocator, column_map.get(lower));
                } else {
                    try agg_col_indices.append(allocator, null); // COUNT(*)
                }

                try col_kinds.append(allocator, .{ .aggregate = agg_idx });
                try out_header.append(allocator, agg_func.alias);
            } else {
                const lower = try allocator.alloc(u8, col.len);
                defer allocator.free(lower);
                _ = std.ascii.lowerString(lower, col);
                const cidx = column_map.get(lower) orelse return error.ColumnNotFound;
                try col_kinds.append(allocator, .{ .regular = cidx });
                try out_header.append(allocator, header[cidx]);
            }
        }
    }

    // Emit header row
    try writer.writeRecord(out_header.items);

    // Precompute WHERE column index for fast single-comparison path
    var where_col_idx: ?usize = null;
    if (query.where_expr) |expr| {
        if (expr == .comparison) {
            for (lower_header, 0..) |lh, i| {
                if (std.mem.eql(u8, lh, expr.comparison.column)) {
                    where_col_idx = i;
                    break;
                }
            }
        }
    }

    // Arena allocator for group key / key_values memory (bulk-freed at the end)
    var key_arena = std.heap.ArenaAllocator.init(allocator);
    defer key_arena.deinit();
    const ka = key_arena.allocator();

    // Hash map: concatenated group key -> GroupState
    var group_map = std.StringHashMap(GroupState).init(allocator);
    defer {
        var it = group_map.iterator();
        while (it.next()) |entry| entry.value_ptr.agg.deinit();
        group_map.deinit();
    }

    // Reusable buffer for building the per-row group key
    var key_buf = std.ArrayList(u8){};
    defer key_buf.deinit(allocator);

    // --- Main scan loop ---
    while (try reader.readRecordSlices()) |record| {
        // WHERE filtering
        if (query.where_expr) |expr| {
            if (expr == .comparison) {
                const comp = expr.comparison;
                const cidx = where_col_idx orelse continue;
                if (cidx >= record.len) continue;
                const field = record[cidx];
                var matches = false;
                if (comp.numeric_value) |threshold| {
                    const val = std.fmt.parseFloat(f64, field) catch continue;
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
                        .equal => std.mem.eql(u8, field, comp.value),
                        .not_equal => !std.mem.eql(u8, field, comp.value),
                        else => false,
                    };
                }
                if (!matches) continue;
            } else {
                // Complex AND/OR/NOT expression
                var row_map = std.StringHashMap([]const u8).init(allocator);
                defer row_map.deinit();
                for (lower_header, 0..) |lh, i| {
                    if (i < record.len) try row_map.put(lh, record[i]);
                }
                if (!parser.evaluate(expr, row_map)) continue;
            }
        }

        // Build NUL-separated group key
        key_buf.clearRetainingCapacity();
        for (group_indices, 0..) |cidx, i| {
            if (i > 0) try key_buf.append(allocator, 0);
            const val = if (cidx < record.len) record[cidx] else "";
            try key_buf.appendSlice(allocator, val);
        }

        // Look up or insert group
        const gop = try group_map.getOrPut(key_buf.items);
        if (!gop.found_existing) {
            // Persist the key and individual column values in the arena
            const stored_key = try ka.dupe(u8, key_buf.items);
            gop.key_ptr.* = stored_key;

            var key_vals = try ka.alloc([]const u8, group_indices.len);
            for (group_indices, 0..) |cidx, gi| {
                key_vals[gi] = try ka.dupe(u8, if (cidx < record.len) record[cidx] else "");
            }
            gop.value_ptr.* = GroupState{
                .key_values = key_vals,
                .agg = aggregation.Aggregator.init(allocator),
            };
        }

        // Accumulate aggregates for this group
        try gop.value_ptr.agg.addRow(agg_funcs.items, agg_col_indices.items, record);
    }

    // --- Output phase ---
    var output_row = try allocator.alloc([]const u8, col_kinds.items.len);
    defer allocator.free(output_row);

    // Reusable slots for formatted aggregate result strings
    var agg_results = try allocator.alloc(?[]u8, agg_funcs.items.len);
    defer {
        for (agg_results) |r| if (r) |s| allocator.free(s);
        allocator.free(agg_results);
    }
    for (agg_results) |*r| r.* = null;

    var rows_output: i32 = 0;
    var group_it = group_map.iterator();
    while (group_it.next()) |entry| {
        if (query.limit >= 0 and rows_output >= query.limit) break;

        const state = entry.value_ptr;

        // Compute formatted aggregate results for this group
        for (agg_funcs.items, 0..) |func, i| {
            if (agg_results[i]) |old| allocator.free(old);
            agg_results[i] = try state.agg.getResult(func, i, allocator);
        }

        // Build the output row
        for (col_kinds.items, 0..) |kind, i| {
            output_row[i] = switch (kind) {
                .regular => |cidx| blk: {
                    // Find the position of this column in the GROUP BY list
                    for (group_indices, 0..) |gidx, gi| {
                        if (gidx == cidx) break :blk state.key_values[gi];
                    }
                    break :blk ""; // Column not in GROUP BY (invalid SQL, but degrade gracefully)
                },
                .aggregate => |agg_idx| agg_results[agg_idx].?,
            };
        }

        try writer.writeRecord(output_row);
        rows_output += 1;
    }

    try writer.flush();
}

// --- Tests ---

test "GROUP BY basic: unique values per group" {
    const allocator = std.testing.allocator;

    const csv_content = "name,department\nAlice,Engineering\nBob,Marketing\nCarol,Engineering\nDave,Marketing\nEve,Sales\n";
    const in_path = "test_gb_basic_in.csv";
    const out_path = "test_gb_basic_out.csv";

    {
        const f = try std.fs.cwd().createFile(in_path, .{});
        defer f.close();
        try f.writeAll(csv_content);
    }
    defer std.fs.cwd().deleteFile(in_path) catch {};

    var query = try parser.parse(allocator, "SELECT department FROM '" ++ in_path ++ "' GROUP BY department");
    defer query.deinit();

    const out_file = try std.fs.cwd().createFile(out_path, .{ .read = true });
    defer {
        out_file.close();
        std.fs.cwd().deleteFile(out_path) catch {};
    }

    try execute(allocator, query, out_file);

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    // Header + 3 unique departments
    const trimmed = std.mem.trim(u8, output, "\n");
    var line_count: usize = 0;
    var lines = std.mem.splitScalar(u8, trimmed, '\n');
    while (lines.next()) |_| line_count += 1;
    try std.testing.expectEqual(@as(usize, 4), line_count); // header + 3 groups
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "department"));
}

test "GROUP BY with COUNT(*)" {
    const allocator = std.testing.allocator;

    const csv_content = "name,department\nAlice,Engineering\nBob,Marketing\nCarol,Engineering\n";
    const in_path = "test_gb_count_in.csv";
    const out_path = "test_gb_count_out.csv";

    {
        const f = try std.fs.cwd().createFile(in_path, .{});
        defer f.close();
        try f.writeAll(csv_content);
    }
    defer std.fs.cwd().deleteFile(in_path) catch {};

    var query = try parser.parse(allocator, "SELECT department, COUNT(*) FROM '" ++ in_path ++ "' GROUP BY department");
    defer query.deinit();

    const out_file = try std.fs.cwd().createFile(out_path, .{ .read = true });
    defer {
        out_file.close();
        std.fs.cwd().deleteFile(out_path) catch {};
    }

    try execute(allocator, query, out_file);

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    // Should have header + 2 groups
    const trimmed = std.mem.trim(u8, output, "\n");
    var line_count: usize = 0;
    var lines = std.mem.splitScalar(u8, trimmed, '\n');
    while (lines.next()) |_| line_count += 1;
    try std.testing.expectEqual(@as(usize, 3), line_count); // header + 2 groups

    // Engineering appears twice so count should be 2; Marketing once so count is 1
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "2"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "1"));
}

test "GROUP BY with WHERE clause" {
    const allocator = std.testing.allocator;

    const csv_content = "name,age,department\nAlice,30,Engineering\nBob,22,Marketing\nCarol,35,Engineering\nDave,20,Marketing\n";
    const in_path = "tgb_filtered_in.csv";
    const out_path = "tgb_filtered_out.csv";

    {
        const f = try std.fs.cwd().createFile(in_path, .{});
        defer f.close();
        try f.writeAll(csv_content);
    }
    defer std.fs.cwd().deleteFile(in_path) catch {};

    var query = try parser.parse(allocator, "SELECT department FROM '" ++ in_path ++ "' WHERE age > 25 GROUP BY department");
    defer query.deinit();

    const out_file = try std.fs.cwd().createFile(out_path, .{ .read = true });
    defer {
        out_file.close();
        std.fs.cwd().deleteFile(out_path) catch {};
    }

    try execute(allocator, query, out_file);

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    // Alice(30) and Carol(35) → only Engineering survives the WHERE filter
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "Engineering"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, output, 1, "Marketing"));
}

test "GROUP BY with LIMIT" {
    const allocator = std.testing.allocator;

    const csv_content = "name,department\nAlice,Engineering\nBob,Marketing\nCarol,Sales\n";
    const in_path = "tgb_capped_in.csv";
    const out_path = "tgb_capped_out.csv";

    {
        const f = try std.fs.cwd().createFile(in_path, .{});
        defer f.close();
        try f.writeAll(csv_content);
    }
    defer std.fs.cwd().deleteFile(in_path) catch {};

    var query = try parser.parse(allocator, "SELECT department FROM '" ++ in_path ++ "' GROUP BY department LIMIT 2");
    defer query.deinit();

    const out_file = try std.fs.cwd().createFile(out_path, .{ .read = true });
    defer {
        out_file.close();
        std.fs.cwd().deleteFile(out_path) catch {};
    }

    try execute(allocator, query, out_file);

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    // header + at most 2 groups
    const trimmed = std.mem.trim(u8, output, "\n");
    var line_count: usize = 0;
    var lines = std.mem.splitScalar(u8, trimmed, '\n');
    while (lines.next()) |_| line_count += 1;
    try std.testing.expectEqual(@as(usize, 3), line_count); // header + 2 groups (LIMIT 2)
}
