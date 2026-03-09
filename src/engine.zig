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

    // Scalar aggregate (no GROUP BY): SELECT COUNT(*)/SUM/AVG/MIN/MAX
    if (query.group_by.len == 0 and hasAggregates(query)) {
        if (hasRegularColumns(query)) return error.MixedAggregateAndNonAggregateSelect;
        try executeScalarAgg(allocator, query, output_file);
        return;
    }

    // DISTINCT without GROUP BY: route through the hash-based GROUP BY engine.
    // SELECT DISTINCT c1,c2 ≡ SELECT c1,c2 GROUP BY c1,c2 — same O(unique rows)
    // memory and a single sequential mmap scan, just like our GROUP BY path.
    if (query.distinct and query.group_by.len == 0) {
        try executeDistinct(allocator, query, output_file);
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

    // DISTINCT dedup state (only used when query.distinct == true)
    var distinct_arena = std.heap.ArenaAllocator.init(allocator);
    defer distinct_arena.deinit();
    var distinct_seen = std.StringHashMap(void).init(allocator);
    defer distinct_seen.deinit();

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
            if (distinct_seen.contains(row_key)) continue;
            try distinct_seen.put(try distinct_arena.allocator().dupe(u8, row_key), {});
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

            // Write sorted rows (with optional DISTINCT dedup on the output line)
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

    // DISTINCT dedup state (only used when query.distinct == true)
    var distinct_arena_stdin = std.heap.ArenaAllocator.init(allocator);
    defer distinct_arena_stdin.deinit();
    var distinct_seen_stdin = std.StringHashMap(void).init(allocator);
    defer distinct_seen_stdin.deinit();

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
            if (distinct_seen_stdin.contains(row_key)) continue;
            try distinct_seen_stdin.put(try distinct_arena_stdin.allocator().dupe(u8, row_key), {});
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

/// Returns true if any SELECT column is an aggregate expression (no alloc).
inline fn isAggregateExpr(col: []const u8) bool {
    const paren = std.mem.indexOf(u8, col, "(") orelse return false;
    const name_part = std.mem.trim(u8, col[0..paren], &std.ascii.whitespace);
    if (name_part.len == 0 or name_part.len > 5) return false;
    var buf: [5]u8 = undefined;
    const lower = std.ascii.lowerString(buf[0..name_part.len], name_part);
    return std.mem.eql(u8, lower, "count") or
        std.mem.eql(u8, lower, "sum") or
        std.mem.eql(u8, lower, "avg") or
        std.mem.eql(u8, lower, "min") or
        std.mem.eql(u8, lower, "max");
}

/// Returns true if any SELECT column is an aggregate expression (no alloc).
fn hasAggregates(query: parser.Query) bool {
    if (query.all_columns) return false;
    for (query.columns) |col| {
        if (isAggregateExpr(col)) return true;
    }
    return false;
}

/// Returns true if any SELECT column is a non-aggregate expression.
fn hasRegularColumns(query: parser.Query) bool {
    if (query.all_columns) return true;
    for (query.columns) |col| {
        if (!isAggregateExpr(col)) return true;
    }
    return false;
}

/// Format an f64 for aggregate output.
/// Whole numbers print as integers; fractional values use Zig's shortest
/// round-trip decimal representation (Ryu algorithm via `{d}`).
inline fn formatF64(alloc: Allocator, val: f64) ![]u8 {
    const as_int: i64 = @intFromFloat(val);
    if (@as(f64, @floatFromInt(as_int)) == val) {
        return std.fmt.allocPrint(alloc, "{d}", .{as_int});
    }
    return std.fmt.allocPrint(alloc, "{d}", .{val});
}

/// Execute SELECT DISTINCT query.
///
/// Routes through a single-pass mmap scan with a StringHashMap dedup set —
/// same O(unique rows) memory and ~same throughput as the GROUP BY engine.
/// This avoids sending 1M rows through the parallel engine only to dedup at
/// the end, which is the bottleneck for low-cardinality DISTINCT columns.
fn executeDistinct(
    allocator: Allocator,
    query: parser.Query,
    output_file: std.fs.File,
) !void {
    const file = try std.fs.cwd().openFile(query.file_path, .{});
    defer file.close();

    const file_size = (try file.stat()).size;
    if (file_size == 0) return error.EmptyFile;

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

    // Header
    const header_nl = std.mem.indexOfScalar(u8, data, '\n') orelse return error.NoHeader;
    var header_line = data[0..header_nl];
    if (header_line.len > 0 and header_line[header_line.len - 1] == '\r')
        header_line = header_line[0 .. header_line.len - 1];

    var header_list = std.ArrayListUnmanaged([]const u8){};
    defer header_list.deinit(allocator);
    {
        var it = std.mem.splitScalar(u8, header_line, ',');
        while (it.next()) |col| try header_list.append(allocator, col);
    }
    const header = header_list.items;

    // Build lowercase column map
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

    // Resolve output column indices
    var out_indices = std.ArrayListUnmanaged(usize){};
    defer out_indices.deinit(allocator);
    var out_header = std.ArrayListUnmanaged([]const u8){};
    defer out_header.deinit(allocator);

    if (query.all_columns) {
        for (0..header.len) |i| {
            try out_indices.append(allocator, i);
            try out_header.append(allocator, header[i]);
        }
    } else {
        for (query.columns) |col| {
            const lower = try allocator.alloc(u8, col.len);
            defer allocator.free(lower);
            _ = std.ascii.lowerString(lower, col);
            const idx = column_map.get(lower) orelse return error.ColumnNotFound;
            try out_indices.append(allocator, idx);
            try out_header.append(allocator, header[idx]);
        }
    }
    try writer.writeRecord(out_header.items);

    // WHERE fast-path index
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

    // ORDER BY support: buffer unique rows then sort
    var sort_entries: ?std.ArrayList(fast_sort.SortKey) = null;
    var sort_arena: ?ArenaBuffer = null;
    var order_by_out_pos: ?usize = null;
    defer {
        if (sort_entries) |*e| e.deinit(allocator);
        if (sort_arena) |*a| a.deinit();
    }
    if (query.order_by) |ob| {
        sort_entries = std.ArrayList(fast_sort.SortKey){};
        sort_arena = try ArenaBuffer.init(allocator, 4 * 1024 * 1024);
        for (out_indices.items, 0..) |oi, pos| {
            if (oi < lower_header.len and std.mem.eql(u8, lower_header[oi], ob.column)) {
                order_by_out_pos = pos;
                break;
            }
        }
        if (order_by_out_pos == null) return error.OrderByColumnNotFound;
    }

    // Dedup set + arena for key storage
    var key_arena = std.heap.ArenaAllocator.init(allocator);
    defer key_arena.deinit();
    var seen = std.StringHashMap(void).init(allocator);
    defer seen.deinit();
    try seen.ensureTotalCapacity(256);

    var fallback_row_map = std.StringHashMap([]const u8).init(allocator);
    defer fallback_row_map.deinit();

    var field_stk: [256][]const u8 = undefined;
    var out_row = try allocator.alloc([]const u8, out_indices.items.len);
    defer allocator.free(out_row);
    var key_buf = std.ArrayListUnmanaged(u8){};
    defer key_buf.deinit(allocator);

    var rows_written: i32 = 0;
    var pos: usize = header_nl + 1;

    while (pos < data.len) {
        const line_start = pos;
        const nl = std.mem.indexOfScalarPos(u8, data, pos, '\n');
        var line_end = nl orelse data.len;
        pos = if (nl) |n| n + 1 else data.len;
        if (line_end > line_start and data[line_end - 1] == '\r') line_end -= 1;
        if (line_end <= line_start) continue;

        const record = splitLine(data[line_start..line_end], &field_stk);

        // WHERE filter
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
                fallback_row_map.clearRetainingCapacity();
                for (lower_header, 0..) |lh, i| {
                    if (i < record.len) try fallback_row_map.put(lh, record[i]);
                }
                if (!parser.evaluate(expr, fallback_row_map)) continue;
            }
        }

        // Project output columns
        for (out_indices.items, 0..) |idx, i| {
            out_row[i] = if (idx < record.len) record[idx] else "";
        }

        // Build NUL-separated dedup key (no alloc after warmup)
        key_buf.clearRetainingCapacity();
        for (out_row, 0..) |field, i| {
            if (i > 0) try key_buf.append(allocator, 0);
            try key_buf.appendSlice(allocator, field);
        }

        // Skip if already seen
        if (seen.contains(key_buf.items)) continue;
        try seen.put(try key_arena.allocator().dupe(u8, key_buf.items), {});

        if (query.limit >= 0 and rows_written >= query.limit and sort_entries == null) break;

        // Either buffer for ORDER BY or write directly
        if (sort_entries) |*entries| {
            const a = &(sort_arena.?);
            const sort_key = try a.append(out_row[order_by_out_pos.?]);
            const line_start_a = a.pos;
            for (out_row, 0..) |field, i| {
                if (i > 0) _ = try a.append(",");
                _ = try a.append(field);
            }
            const line = a.data[line_start_a..a.pos];
            try entries.append(allocator, fast_sort.makeSortKey(
                std.fmt.parseFloat(f64, sort_key) catch std.math.nan(f64),
                sort_key,
                line,
            ));
        } else {
            try writer.writeRecord(out_row);
            rows_written += 1;
        }
    }

    // Sort and emit if ORDER BY
    if (sort_entries) |*entries| {
        if (query.order_by) |ob| {
            const limit: ?usize = if (query.limit >= 0) @intCast(query.limit) else null;
            const sorted = try fast_sort.sortEntries(allocator, entries.items, ob.order == .desc, limit);
            for (sorted) |entry| {
                try writer.writeToBuffer(entry.line);
                try writer.writeToBuffer("\n");
            }
        }
    }

    try writer.flush();
}

/// Execute a scalar aggregate query (aggregate functions without GROUP BY).
/// Performs a single mmap scan accumulating one CompactAccum and emits
/// exactly one result row.  Supports WHERE, all five aggregate functions,
/// and mixed regular+aggregate SELECT lists.
fn executeScalarAgg(
    allocator: Allocator,
    query: parser.Query,
    output_file: std.fs.File,
) !void {
    const file = try std.fs.cwd().openFile(query.file_path, .{});
    defer file.close();

    const file_size = (try file.stat()).size;
    if (file_size == 0) return error.EmptyFile;

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

    // -- Header --
    const header_nl = std.mem.indexOfScalar(u8, data, '\n') orelse return error.NoHeader;
    var header_line = data[0..header_nl];
    if (header_line.len > 0 and header_line[header_line.len - 1] == '\r')
        header_line = header_line[0 .. header_line.len - 1];

    var header_list = std.ArrayListUnmanaged([]const u8){};
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

    // -- Resolve SELECT into ColKind + AggSpec lists --
    var col_kinds = std.ArrayListUnmanaged(ColKind){};
    defer col_kinds.deinit(allocator);
    var agg_specs = std.ArrayListUnmanaged(AggSpec){};
    defer {
        for (agg_specs.items) |spec| allocator.free(spec.alias);
        agg_specs.deinit(allocator);
    }
    var out_header_list = std.ArrayListUnmanaged([]const u8){};
    defer out_header_list.deinit(allocator);

    for (query.columns) |col| {
        if (try aggregation.parseAggregateFunc(allocator, col)) |parsed_agg| {
            var agg_func = parsed_agg;
            errdefer agg_func.deinit(allocator);
            const agg_idx = agg_specs.items.len;
            var col_idx: ?usize = null;
            if (agg_func.column) |agg_col| {
                const lower = try allocator.alloc(u8, agg_col.len);
                defer allocator.free(lower);
                _ = std.ascii.lowerString(lower, agg_col);
                col_idx = column_map.get(lower) orelse return error.ColumnNotFound;
                allocator.free(agg_col);
                agg_func.column = null;
            }
            try col_kinds.append(allocator, .{ .aggregate = agg_idx });
            try out_header_list.append(allocator, agg_func.alias);
            try agg_specs.append(allocator, AggSpec{
                .func_type = agg_func.func_type,
                .col_idx = col_idx,
                .alias = agg_func.alias,
            });
        } else {
            const lower = try allocator.alloc(u8, col.len);
            defer allocator.free(lower);
            _ = std.ascii.lowerString(lower, col);
            const cidx = column_map.get(lower) orelse return error.ColumnNotFound;
            try col_kinds.append(allocator, .{ .regular = cidx });
            try out_header_list.append(allocator, header[cidx]);
        }
    }
    try writer.writeRecord(out_header_list.items);

    // -- WHERE fast-path index --
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

    // Single global accumulator
    var ka = std.heap.ArenaAllocator.init(allocator);
    defer ka.deinit();
    const n_aggs = agg_specs.items.len;
    const empty_keys = try ka.allocator().alloc([]const u8, 0);
    var accum = try CompactAccum.init(ka.allocator(), empty_keys, n_aggs);

    var fallback_row_map = std.StringHashMap([]const u8).init(allocator);
    defer fallback_row_map.deinit();
    var field_stk: [256][]const u8 = undefined;

    // -- Scan --
    var scan_pos: usize = header_nl + 1;
    while (scan_pos < data.len) {
        const line_start = scan_pos;
        const nl = std.mem.indexOfScalarPos(u8, data, scan_pos, '\n');
        var line_end = nl orelse data.len;
        scan_pos = if (nl) |n| n + 1 else data.len;
        if (line_end > line_start and data[line_end - 1] == '\r') line_end -= 1;
        if (line_end <= line_start) continue;

        const record = splitLine(data[line_start..line_end], &field_stk);

        // WHERE filter
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
                fallback_row_map.clearRetainingCapacity();
                for (lower_header, 0..) |lh, i| {
                    if (i < record.len) try fallback_row_map.put(lh, record[i]);
                }
                if (!parser.evaluate(expr, fallback_row_map)) continue;
            }
        }

        // Accumulate
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

    // -- Emit one result row --
    var agg_allocs = std.ArrayListUnmanaged([]u8){};
    defer {
        for (agg_allocs.items) |s| allocator.free(s);
        agg_allocs.deinit(allocator);
    }
    var agg_results = try allocator.alloc([]const u8, n_aggs);
    defer allocator.free(agg_results);

    for (agg_specs.items, 0..) |spec, i| {
        const s: []u8 = switch (spec.func_type) {
            .count => try std.fmt.allocPrint(allocator, "{d}", .{accum.count}),
            .sum => try formatF64(allocator, accum.sums[i]),
            .avg => blk: {
                const cnt = accum.sum_counts[i];
                break :blk if (cnt > 0)
                    try formatF64(allocator, accum.sums[i] / @as(f64, @floatFromInt(cnt)))
                else
                    try allocator.dupe(u8, "0");
            },
            .min => blk: {
                const v = accum.mins[i];
                break :blk if (v < std.math.inf(f64))
                    try formatF64(allocator, v)
                else
                    try allocator.dupe(u8, "");
            },
            .max => blk: {
                const v = accum.maxs[i];
                break :blk if (v > -std.math.inf(f64))
                    try formatF64(allocator, v)
                else
                    try allocator.dupe(u8, "");
            },
        };
        try agg_allocs.append(allocator, s);
        agg_results[i] = s;
    }

    var output_row = try allocator.alloc([]const u8, col_kinds.items.len);
    defer allocator.free(output_row);
    for (col_kinds.items, 0..) |kind, i| {
        output_row[i] = switch (kind) {
            .regular => "",
            .aggregate => |agg_idx| agg_results[agg_idx],
        };
    }
    try writer.writeRecord(output_row);
    try writer.flush();
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

    var header_list = std.ArrayListUnmanaged([]const u8){};
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
    var col_kinds = std.ArrayListUnmanaged(ColKind){};
    defer col_kinds.deinit(allocator);

    var agg_specs = std.ArrayListUnmanaged(AggSpec){};
    defer {
        for (agg_specs.items) |spec| allocator.free(spec.alias);
        agg_specs.deinit(allocator);
    }

    var out_header_list = std.ArrayListUnmanaged([]const u8){};
    defer out_header_list.deinit(allocator);

    if (query.all_columns) {
        for (group_indices) |cidx| {
            try col_kinds.append(allocator, .{ .regular = cidx });
            try out_header_list.append(allocator, header[cidx]);
        }
    } else {
        for (query.columns) |col| {
            if (try aggregation.parseAggregateFunc(allocator, col)) |parsed_agg| {
                var agg_func = parsed_agg;
                errdefer agg_func.deinit(allocator);
                const agg_idx = agg_specs.items.len;
                var col_idx: ?usize = null;
                if (agg_func.column) |agg_col| {
                    const lower = try allocator.alloc(u8, agg_col.len);
                    defer allocator.free(lower);
                    _ = std.ascii.lowerString(lower, agg_col);
                    col_idx = column_map.get(lower) orelse return error.ColumnNotFound;
                    allocator.free(agg_col);
                    agg_func.column = null;
                }
                try col_kinds.append(allocator, .{ .aggregate = agg_idx });
                try out_header_list.append(allocator, agg_func.alias);
                // alias ownership transfers to AggSpec
                try agg_specs.append(allocator, AggSpec{
                    .func_type = agg_func.func_type,
                    .col_idx = col_idx,
                    .alias = agg_func.alias,
                });
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
    var key_buf = std.ArrayListUnmanaged(u8){};
    defer key_buf.deinit(allocator);

    // Stack field buffer: zero heap allocation for field splitting per row
    var field_stk: [256][]const u8 = undefined;

    // Fallback HashMap for complex WHERE expressions (AND/OR) — hoisted out of
    // the hot loop so that only one allocation happens for the entire scan.
    var fallback_row_map = std.StringHashMap([]const u8).init(allocator);
    defer fallback_row_map.deinit();

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
                // Fallback map is allocated once before the loop and cleared here.
                fallback_row_map.clearRetainingCapacity();
                for (lower_header, 0..) |lh, i| {
                    if (i < record.len) try fallback_row_map.put(lh, record[i]);
                }
                if (!parser.evaluate(expr, fallback_row_map)) continue;
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

    // -- Sorted output phase -----------------------------------------------
    // Collect group keys and sort for deterministic output order.
    var sorted_keys = try allocator.alloc([]const u8, group_map.count());
    defer allocator.free(sorted_keys);
    {
        var kit = group_map.keyIterator();
        var ki: usize = 0;
        while (kit.next()) |k| : (ki += 1) sorted_keys[ki] = k.*;
    }
    std.mem.sort([]const u8, sorted_keys, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.lt);

    var output_row = try allocator.alloc([]const u8, col_kinds.items.len);
    defer allocator.free(output_row);

    // DISTINCT dedup for GROUP BY output (groups are unique by key but SELECT
    // may project fewer columns making two groups map to the same output row)
    var distinct_arena_gb = std.heap.ArenaAllocator.init(allocator);
    defer distinct_arena_gb.deinit();
    var distinct_seen_gb = std.StringHashMap(void).init(allocator);
    defer distinct_seen_gb.deinit();

    var rows_output: i32 = 0;
    for (sorted_keys) |key| {
        if (query.limit >= 0 and rows_output >= query.limit) break;
        const accum = group_map.getPtr(key).?;

        // Format aggregate results (handful of groups, negligible cost)
        var agg_allocs = std.ArrayListUnmanaged([]u8){};
        defer {
            for (agg_allocs.items) |s| allocator.free(s);
            agg_allocs.deinit(allocator);
        }
        var agg_results = try allocator.alloc([]const u8, n_aggs);
        defer allocator.free(agg_results);

        for (agg_specs.items, 0..) |spec, i| {
            const s: []u8 = switch (spec.func_type) {
                .count => try std.fmt.allocPrint(allocator, "{d}", .{accum.count}),
                .sum => try formatF64(allocator, accum.sums[i]),
                .avg => blk: {
                    const cnt = accum.sum_counts[i];
                    break :blk if (cnt > 0)
                        try formatF64(allocator, accum.sums[i] / @as(f64, @floatFromInt(cnt)))
                    else
                        try allocator.dupe(u8, "0");
                },
                .min => blk: {
                    const v = accum.mins[i];
                    break :blk if (v < std.math.inf(f64))
                        try formatF64(allocator, v)
                    else
                        try allocator.dupe(u8, "");
                },
                .max => blk: {
                    const v = accum.maxs[i];
                    break :blk if (v > -std.math.inf(f64))
                        try formatF64(allocator, v)
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

        // DISTINCT check for GROUP BY output
        if (query.distinct) {
            var distinct_key_buf: [8192]u8 = undefined;
            var klen: usize = 0;
            for (output_row, 0..) |field, fi| {
                if (fi > 0 and klen < distinct_key_buf.len) {
                    distinct_key_buf[klen] = 0;
                    klen += 1;
                }
                const n = @min(field.len, distinct_key_buf.len - klen);
                if (n > 0) @memcpy(distinct_key_buf[klen..][0..n], field[0..n]);
                klen += n;
            }
            const row_key = distinct_key_buf[0..klen];
            if (distinct_seen_gb.contains(row_key)) continue;
            try distinct_seen_gb.put(try distinct_arena_gb.allocator().dupe(u8, row_key), {});
        }

        try writer.writeRecord(output_row);
        rows_output += 1;
    }

    try writer.flush();
}

// --- Tests ---
// Note: tests use std.testing.tmpDir so that parallel test runs don't race
// on shared temp-file names.

test "GROUP BY basic: unique values per group" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "name,department\nAlice,Engineering\nBob,Marketing\nCarol,Engineering\nDave,Marketing\nEve,Sales\n";

    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);

    const sql = try std.fmt.allocPrint(allocator, "SELECT department FROM '{s}' GROUP BY department", .{in_path});
    defer allocator.free(sql);
    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output.csv", .{ .read = true });
    defer out_file.close();

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

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "name,department\nAlice,Engineering\nBob,Marketing\nCarol,Engineering\n";

    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);

    const sql = try std.fmt.allocPrint(allocator, "SELECT department, COUNT(*) FROM '{s}' GROUP BY department", .{in_path});
    defer allocator.free(sql);
    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output.csv", .{ .read = true });
    defer out_file.close();

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

    // Engineering appears twice so count should be 2; Marketing once so count is 1.
    // Output is sorted: Engineering row comes first (alphabetically).
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "Engineering,2"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "Marketing,1"));
}

test "GROUP BY with WHERE clause" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "name,age,department\nAlice,30,Engineering\nBob,22,Marketing\nCarol,35,Engineering\nDave,20,Marketing\n";

    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);

    const sql = try std.fmt.allocPrint(allocator, "SELECT department FROM '{s}' WHERE age > 25 GROUP BY department", .{in_path});
    defer allocator.free(sql);
    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output.csv", .{ .read = true });
    defer out_file.close();

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

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "name,department\nAlice,Engineering\nBob,Marketing\nCarol,Sales\n";

    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);

    const sql = try std.fmt.allocPrint(allocator, "SELECT department FROM '{s}' GROUP BY department LIMIT 2", .{in_path});
    defer allocator.free(sql);
    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output.csv", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file);

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    // header + at most 2 groups (sorted: Engineering, Marketing — Sales excluded by LIMIT)
    const trimmed = std.mem.trim(u8, output, "\n");
    var line_count: usize = 0;
    var lines = std.mem.splitScalar(u8, trimmed, '\n');
    while (lines.next()) |_| line_count += 1;
    try std.testing.expectEqual(@as(usize, 3), line_count); // header + 2 groups (LIMIT 2)
}

test "DISTINCT basic: deduplicates repeated values" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "name,department\nAlice,Engineering\nBob,Marketing\nCarol,Engineering\nDave,Marketing\nEve,Sales\n";

    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);

    const sql = try std.fmt.allocPrint(allocator, "SELECT DISTINCT department FROM '{s}'", .{in_path});
    defer allocator.free(sql);
    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    try std.testing.expect(query.distinct);

    const out_file = try tmp.dir.createFile("output.csv", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file);

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    // Should have header + 3 unique departments (Engineering, Marketing, Sales)
    const trimmed_out = std.mem.trim(u8, output, "\n");
    var lc: usize = 0;
    var ls = std.mem.splitScalar(u8, trimmed_out, '\n');
    while (ls.next()) |_| lc += 1;
    try std.testing.expectEqual(@as(usize, 4), lc); // header + 3 unique values

    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "Engineering"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "Marketing"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "Sales"));
    // Each value should appear exactly once
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, output, "Engineering"));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, output, "Marketing"));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, output, "Sales"));
}

test "DISTINCT with WHERE: deduplicates filtered rows" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "name,age,department\nAlice,30,Engineering\nBob,22,Marketing\nCarol,35,Engineering\nDave,20,Marketing\n";

    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);

    const sql = try std.fmt.allocPrint(allocator, "SELECT DISTINCT department FROM '{s}' WHERE age > 25", .{in_path});
    defer allocator.free(sql);
    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output.csv", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file);

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    // age > 25: Alice(30)→Engineering, Carol(35)→Engineering → only 1 unique dept
    const trimmed_out = std.mem.trim(u8, output, "\n");
    var lc: usize = 0;
    var ls = std.mem.splitScalar(u8, trimmed_out, '\n');
    while (ls.next()) |_| lc += 1;
    try std.testing.expectEqual(@as(usize, 2), lc); // header + 1 unique department
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, output, "Engineering"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, output, 1, "Marketing"));
}

test "DISTINCT star: deduplicates full rows" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "city,country\nParis,France\nBerlin,Germany\nParis,France\nTokyo,Japan\nBerlin,Germany\n";

    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);

    const sql = try std.fmt.allocPrint(allocator, "SELECT DISTINCT * FROM '{s}'", .{in_path});
    defer allocator.free(sql);
    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output.csv", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file);

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    // 5 rows but only 3 unique (Paris/France ×2, Berlin/Germany ×2, Tokyo/Japan ×1)
    const trimmed_out = std.mem.trim(u8, output, "\n");
    var lc: usize = 0;
    var ls = std.mem.splitScalar(u8, trimmed_out, '\n');
    while (ls.next()) |_| lc += 1;
    try std.testing.expectEqual(@as(usize, 4), lc); // header + 3 unique rows
}

test "DISTINCT with LIMIT: returns limited unique rows" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "name,department\nAlice,Engineering\nBob,Marketing\nCarol,Engineering\nDave,Sales\nEve,Marketing\n";

    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);

    const sql = try std.fmt.allocPrint(allocator, "SELECT DISTINCT department FROM '{s}' LIMIT 2", .{in_path});
    defer allocator.free(sql);
    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output.csv", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file);

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    const trimmed_out = std.mem.trim(u8, output, "\n");
    var lc: usize = 0;
    var ls = std.mem.splitScalar(u8, trimmed_out, '\n');
    while (ls.next()) |_| lc += 1;
    try std.testing.expectEqual(@as(usize, 3), lc); // header + LIMIT(2) unique rows
}

test "COUNT(*) with GROUP BY: counts per group" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "name,department\nAlice,Engineering\nBob,Marketing\nCarol,Engineering\nDave,Marketing\nEve,Sales\n";

    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);

    const sql = try std.fmt.allocPrint(allocator, "SELECT department, COUNT(*) FROM '{s}' GROUP BY department", .{in_path});
    defer allocator.free(sql);
    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output.csv", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file);

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "Engineering,2"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "Marketing,2"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "Sales,1"));
}

test "COUNT(*) with WHERE and GROUP BY" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "name,age,department\nAlice,30,Engineering\nBob,22,Marketing\nCarol,35,Engineering\nDave,20,Marketing\nEve,28,Sales\n";

    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);

    const sql = try std.fmt.allocPrint(allocator, "SELECT department, COUNT(*) FROM '{s}' WHERE age > 25 GROUP BY department", .{in_path});
    defer allocator.free(sql);
    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output.csv", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file);

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    // age > 25: Alice(30)→Eng, Carol(35)→Eng, Eve(28)→Sales
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "Engineering,2"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "Sales,1"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, output, 1, "Marketing"));
}

test "scalar COUNT(*): counts all rows" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "name,age\nAlice,30\nBob,22\nCarol,35\n";
    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);

    const sql = try std.fmt.allocPrint(allocator, "SELECT COUNT(*) FROM '{s}'", .{in_path});
    defer allocator.free(sql);
    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output.csv", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file);

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "COUNT(*)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "3"));
}

test "scalar SUM/AVG/MIN/MAX: correct values" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "name,salary,age\nAlice,60000,30\nBob,40000,22\nCarol,50000,35\n";
    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);

    const sql = try std.fmt.allocPrint(
        allocator,
        "SELECT SUM(salary), MIN(age), MAX(age) FROM '{s}'",
        .{in_path},
    );
    defer allocator.free(sql);
    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output.csv", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file);

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    // SUM(salary) = 150000, MIN(age) = 22, MAX(age) = 35
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "150000"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "22"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "35"));
    // Integer SUM should not have .00
    try std.testing.expect(!std.mem.containsAtLeast(u8, output, 1, "150000."));
}

test "scalar COUNT(*) with WHERE filter" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "name,age\nAlice,30\nBob,22\nCarol,35\nDave,25\n";
    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);

    const sql = try std.fmt.allocPrint(allocator, "SELECT COUNT(*) FROM '{s}' WHERE age > 25", .{in_path});
    defer allocator.free(sql);
    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output.csv", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file);

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    // age > 25: Alice(30), Carol(35) → 2 rows
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "2"));
    // Should NOT count rows where age <= 25
    const trimmed = std.mem.trim(u8, output, "\n");
    var lines = std.mem.splitScalar(u8, trimmed, '\n');
    _ = lines.next(); // skip header
    const data_line = lines.next() orelse "";
    try std.testing.expectEqualStrings("2", std.mem.trim(u8, data_line, "\r"));
}

test "scalar aggregate with missing column returns ColumnNotFound" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "name,age\nAlice,30\nBob,22\n";
    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);
    const sql = try std.fmt.allocPrint(allocator, "SELECT SUM(missing) FROM '{s}'", .{in_path});
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output.csv", .{});
    defer out_file.close();

    try std.testing.expectError(error.ColumnNotFound, execute(allocator, query, out_file));
}

test "mixed scalar aggregate and regular column requires GROUP BY" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "name,age\nAlice,30\nBob,22\n";
    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);
    const sql = try std.fmt.allocPrint(allocator, "SELECT name, COUNT(*) FROM '{s}'", .{in_path});
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output.csv", .{});
    defer out_file.close();

    try std.testing.expectError(error.MixedAggregateAndNonAggregateSelect, execute(allocator, query, out_file));
}

test "DISTINCT with scalar aggregate follows scalar path" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "name,age\nAlice,30\nBob,22\n";
    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);
    const sql = try std.fmt.allocPrint(allocator, "SELECT DISTINCT COUNT(*) FROM '{s}'", .{in_path});
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output.csv", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file);

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "COUNT(*)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "2"));
}
