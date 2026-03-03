const std = @import("std");
const parser = @import("parser.zig");
const csv = @import("csv.zig");
const bulk_csv = @import("bulk_csv.zig");
const mmap_engine = @import("mmap_engine.zig");
const parallel_mmap = @import("parallel_mmap.zig");
const fast_sort = @import("fast_sort.zig");
const aggregation = @import("aggregation.zig");
const simd = @import("simd.zig");
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

/// Re-export parser.parse so callers (benchmarks, tests) don't need a
/// separate parser import — avoiding Zig 0.15 module-duplication errors.
pub const parseQuery = parser.parse;
/// Re-export Query type for the same reason.
pub const Query = parser.Query;

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

/// Describes how each SELECT output column is derived.
const ColKind = union(enum) {
    regular: usize, // direct CSV column index
    aggregate: usize, // index into AggSpec list
};

/// Pre-resolved aggregate function spec — no per-row allocations in hot loop.
const AggSpec = struct {
    func_type: aggregation.AggregateType,
    col_idx: ?usize, // resolved CSV column index (null for COUNT(*))
    alias: []const u8, // output header name (allocator-owned)
};

/// Compact per-group accumulator using flat arrays instead of HashMaps.
///
/// Replaces `aggregation.Aggregator` (which held 6 AutoHashMap per group).
/// For N aggregate functions: N direct array r/w per row vs 2N HashMap ops.
/// Mins initialised to +inf, maxs to -inf — no branch needed in accumulation.
const CompactAccum = struct {
    key_values: [][]const u8, // GROUP BY column values, arena-owned
    count: i64, // row count for COUNT(*)
    sums: []f64, // sum[i] for agg_spec i, arena-alloc, zero-init
    sum_counts: []i64, // non-null input count per agg (for AVG), arena-alloc
    mins: []f64, // min[i], arena-alloc, init to +inf
    maxs: []f64, // max[i], arena-alloc, init to -inf

    fn init(ka: Allocator, key_vals: [][]const u8, n_aggs: usize) !CompactAccum {
        const sums = try ka.alloc(f64, n_aggs);
        const sum_counts = try ka.alloc(i64, n_aggs);
        const mins = try ka.alloc(f64, n_aggs);
        const maxs = try ka.alloc(f64, n_aggs);
        @memset(sums, 0.0);
        @memset(sum_counts, 0);
        for (mins, maxs) |*mn, *mx| {
            mn.* = std.math.inf(f64);
            mx.* = -std.math.inf(f64);
        }
        return CompactAccum{
            .key_values = key_vals,
            .count = 0,
            .sums = sums,
            .sum_counts = sum_counts,
            .mins = mins,
            .maxs = maxs,
        };
    }
};

/// Fast numeric parser: try fast integer path first (most CSV numbers are
/// integers), fall back to float.  ~3-4x faster than parseFloat on integer
/// strings (salary, age, count columns).
inline fn parseNumericFast(s: []const u8) !f64 {
    if (simd.parseIntFast(s)) |iv| {
        return @as(f64, @floatFromInt(iv));
    } else |_| {}
    return std.fmt.parseFloat(f64, s);
}

/// Split a CSV line into fields using a pre-allocated stack buffer.
/// Zero-copy: returned slices point into `line`.  Max 256 fields.
inline fn splitLine(line: []const u8, buf: [][]const u8) []const []const u8 {
    var n: usize = 0;
    var it = std.mem.splitScalar(u8, line, ',');
    while (it.next()) |f| {
        if (n >= buf.len) break;
        buf[n] = f;
        n += 1;
    }
    return buf[0..n];
}

/// Execute GROUP BY query.
///
/// Uses mmap for zero-copy sequential scanning and CompactAccum for
/// O(1) aggregate accumulation per row (flat array r/w, no per-group HashMaps).
fn executeGroupBy(
    allocator: Allocator,
    query: parser.Query,
    output_file: std.fs.File,
) !void {
    const file = try std.fs.cwd().openFile(query.file_path, .{});
    defer file.close();

    const file_size = (try file.stat()).size;
    if (file_size == 0) return error.EmptyFile;

    // Memory-map the file: zero-copy sequential scan, better prefetch than
    // buffered I/O for the full-table reads that GROUP BY requires.
    const mapped = try std.posix.mmap(
        null,
        file_size,
        std.posix.PROT.READ,
        .{ .TYPE = .SHARED },
        file.handle,
        0,
    );
    defer std.posix.munmap(mapped);
    const data = mapped[0..file_size];

    var writer = csv.CsvWriter.init(output_file);

    // -- Header parsing -----------------------------------------------------
    const header_nl = std.mem.indexOfScalar(u8, data, '\n') orelse return error.NoHeader;
    var header_line = data[0..header_nl];
    if (header_line.len > 0 and header_line[header_line.len - 1] == '\r')
        header_line = header_line[0 .. header_line.len - 1];

    var header_list = std.ArrayList([]const u8){};
    defer header_list.deinit(allocator);
    {
        var it = std.mem.splitScalar(u8, header_line, ',');
        while (it.next()) |col| try header_list.append(allocator, col);
    }
    const header = header_list.items;

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

    // -- Resolve GROUP BY columns -------------------------------------------
    var group_indices = try allocator.alloc(usize, query.group_by.len);
    defer allocator.free(group_indices);
    for (query.group_by, 0..) |col, i| {
        const lower = try allocator.alloc(u8, col.len);
        defer allocator.free(lower);
        _ = std.ascii.lowerString(lower, col);
        group_indices[i] = column_map.get(lower) orelse return error.ColumnNotFound;
    }

    // -- Resolve SELECT columns into ColKind + AggSpec lists ---------------
    var col_kinds = std.ArrayList(ColKind){};
    defer col_kinds.deinit(allocator);

    var agg_specs = std.ArrayList(AggSpec){};
    defer {
        for (agg_specs.items) |spec| allocator.free(spec.alias);
        agg_specs.deinit(allocator);
    }

    var out_header_list = std.ArrayList([]const u8){};
    defer out_header_list.deinit(allocator);

    if (query.all_columns) {
        for (group_indices) |cidx| {
            try col_kinds.append(allocator, .{ .regular = cidx });
            try out_header_list.append(allocator, header[cidx]);
        }
    } else {
        for (query.columns) |col| {
            if (try aggregation.parseAggregateFunc(allocator, col)) |agg_func| {
                const agg_idx = agg_specs.items.len;
                var col_idx: ?usize = null;
                if (agg_func.column) |agg_col| {
                    const lower = try allocator.alloc(u8, agg_col.len);
                    defer allocator.free(lower);
                    _ = std.ascii.lowerString(lower, agg_col);
                    col_idx = column_map.get(lower);
                    allocator.free(agg_func.column.?);
                }
                // alias ownership transfers to AggSpec
                try agg_specs.append(allocator, AggSpec{
                    .func_type = agg_func.func_type,
                    .col_idx = col_idx,
                    .alias = agg_func.alias,
                });
                try col_kinds.append(allocator, .{ .aggregate = agg_idx });
                try out_header_list.append(allocator, agg_specs.items[agg_idx].alias);
            } else {
                const lower = try allocator.alloc(u8, col.len);
                defer allocator.free(lower);
                _ = std.ascii.lowerString(lower, col);
                const cidx = column_map.get(lower) orelse return error.ColumnNotFound;
                try col_kinds.append(allocator, .{ .regular = cidx });
                try out_header_list.append(allocator, header[cidx]);
            }
        }
    }
    try writer.writeRecord(out_header_list.items);

    // -- Precompute WHERE fast path -----------------------------------------
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

    // -- Arena + group hash map ---------------------------------------------
    var key_arena = std.heap.ArenaAllocator.init(allocator);
    defer key_arena.deinit();
    const ka = key_arena.allocator();

    var group_map = std.StringHashMap(CompactAccum).init(allocator);
    defer group_map.deinit();
    // Pre-size for typical low-cardinality GROUP BY (avoids rehashing)
    try group_map.ensureTotalCapacity(64);

    const n_aggs = agg_specs.items.len;

    // Reusable key builder (grows once, then reused without allocation)
    var key_buf = std.ArrayList(u8){};
    defer key_buf.deinit(allocator);

    // Stack field buffer: zero heap allocation for field splitting per row
    var field_stk: [256][]const u8 = undefined;

    // -- Main scan loop (mmap sequential scan) ------------------------------
    var pos: usize = header_nl + 1;
    while (pos < data.len) {
        const line_start = pos;
        const nl = std.mem.indexOfScalarPos(u8, data, pos, '\n');
        var line_end = nl orelse data.len;
        pos = if (nl) |n| n + 1 else data.len;
        if (line_end > line_start and data[line_end - 1] == '\r') line_end -= 1;
        if (line_end <= line_start) continue;

        const record = splitLine(data[line_start..line_end], &field_stk);

        // WHERE filter (fast single-comparison path with integer fast parse)
        if (query.where_expr) |expr| {
            if (expr == .comparison) {
                const comp = expr.comparison;
                const cidx = where_col_idx orelse continue;
                if (cidx >= record.len) continue;
                const fv = record[cidx];
                var matches = false;
                if (comp.numeric_value) |threshold| {
                    const val = parseNumericFast(fv) catch continue;
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
                        .equal => std.mem.eql(u8, fv, comp.value),
                        .not_equal => !std.mem.eql(u8, fv, comp.value),
                        else => false,
                    };
                }
                if (!matches) continue;
            } else {
                var row_map = std.StringHashMap([]const u8).init(allocator);
                defer row_map.deinit();
                for (lower_header, 0..) |lh, i| {
                    if (i < record.len) try row_map.put(lh, record[i]);
                }
                if (!parser.evaluate(expr, row_map)) continue;
            }
        }

        // Build NUL-separated group key (no alloc after first row warmup)
        key_buf.clearRetainingCapacity();
        for (group_indices, 0..) |cidx, i| {
            if (i > 0) try key_buf.append(allocator, 0);
            try key_buf.appendSlice(allocator, if (cidx < record.len) record[cidx] else "");
        }

        // Look up or create group
        const gop = try group_map.getOrPut(key_buf.items);
        if (!gop.found_existing) {
            const stored_key = try ka.dupe(u8, key_buf.items);
            gop.key_ptr.* = stored_key;
            var key_vals = try ka.alloc([]const u8, group_indices.len);
            for (group_indices, 0..) |cidx, gi| {
                key_vals[gi] = try ka.dupe(u8, if (cidx < record.len) record[cidx] else "");
            }
            gop.value_ptr.* = try CompactAccum.init(ka, key_vals, n_aggs);
        }
        const accum = gop.value_ptr;

        // Accumulate: direct array r/w, no HashMap operations per row ------
        accum.count += 1;
        for (agg_specs.items, 0..) |spec, i| {
            switch (spec.func_type) {
                .count => {},
                .sum, .avg => {
                    if (spec.col_idx) |cidx| {
                        if (cidx < record.len) {
                            if (parseNumericFast(record[cidx])) |val| {
                                accum.sums[i] += val;
                                accum.sum_counts[i] += 1;
                            } else |_| {}
                        }
                    }
                },
                .min => {
                    if (spec.col_idx) |cidx| {
                        if (cidx < record.len) {
                            if (parseNumericFast(record[cidx])) |val| {
                                if (val < accum.mins[i]) accum.mins[i] = val;
                            } else |_| {}
                        }
                    }
                },
                .max => {
                    if (spec.col_idx) |cidx| {
                        if (cidx < record.len) {
                            if (parseNumericFast(record[cidx])) |val| {
                                if (val > accum.maxs[i]) accum.maxs[i] = val;
                            } else |_| {}
                        }
                    }
                },
            }
        }
    }

    // -- Output phase -------------------------------------------------------
    var output_row = try allocator.alloc([]const u8, col_kinds.items.len);
    defer allocator.free(output_row);

    var rows_output: i32 = 0;
    var group_it = group_map.iterator();
    while (group_it.next()) |entry| {
        if (query.limit >= 0 and rows_output >= query.limit) break;
        const accum = entry.value_ptr;

        // Format aggregate results (handful of groups, negligible cost)
        var agg_allocs = std.ArrayList([]u8){};
        defer {
            for (agg_allocs.items) |s| allocator.free(s);
            agg_allocs.deinit(allocator);
        }
        var agg_results = try allocator.alloc([]const u8, n_aggs);
        defer allocator.free(agg_results);

        for (agg_specs.items, 0..) |spec, i| {
            const s: []u8 = switch (spec.func_type) {
                .count => try std.fmt.allocPrint(allocator, "{d}", .{accum.count}),
                .sum => try std.fmt.allocPrint(allocator, "{d:.2}", .{accum.sums[i]}),
                .avg => blk: {
                    const cnt = accum.sum_counts[i];
                    break :blk if (cnt > 0)
                        try std.fmt.allocPrint(allocator, "{d:.2}", .{accum.sums[i] / @as(f64, @floatFromInt(cnt))})
                    else
                        try allocator.dupe(u8, "0");
                },
                .min => blk: {
                    const v = accum.mins[i];
                    break :blk if (v < std.math.inf(f64))
                        try std.fmt.allocPrint(allocator, "{d:.2}", .{v})
                    else
                        try allocator.dupe(u8, "");
                },
                .max => blk: {
                    const v = accum.maxs[i];
                    break :blk if (v > -std.math.inf(f64))
                        try std.fmt.allocPrint(allocator, "{d:.2}", .{v})
                    else
                        try allocator.dupe(u8, "");
                },
            };
            try agg_allocs.append(allocator, s);
            agg_results[i] = s;
        }

        for (col_kinds.items, 0..) |kind, i| {
            output_row[i] = switch (kind) {
                .regular => |cidx| blk: {
                    for (group_indices, 0..) |gidx, gi| {
                        if (gidx == cidx) break :blk accum.key_values[gi];
                    }
                    break :blk "";
                },
                .aggregate => |agg_idx| agg_results[agg_idx],
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
