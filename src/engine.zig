const std = @import("std");
const parser = @import("parser.zig");
const csv = @import("csv.zig");
const bulk_csv = @import("bulk_csv.zig");
const mmap_engine = @import("mmap_engine.zig");
const parallel_mmap = @import("parallel_mmap.zig");
const fast_sort = @import("fast_sort.zig");
const aggregation = @import("aggregation.zig");
const simd = @import("simd.zig");
const options_mod = @import("options.zig");
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

/// Append a JSON-escaped, double-quoted string to an ArenaBuffer.
/// Used by the ORDER BY path when building JSON objects for sorting.
fn appendJsonStringToArena(arena: *ArenaBuffer, s: []const u8) !void {
    _ = try arena.append("\"");
    for (s) |c| {
        switch (c) {
            '"' => _ = try arena.append("\\\""),
            '\\' => _ = try arena.append("\\\\"),
            '\n' => _ = try arena.append("\\n"),
            '\r' => _ = try arena.append("\\r"),
            '\t' => _ = try arena.append("\\t"),
            0x00...0x08, 0x0B...0x0C, 0x0E...0x1F => {
                var buf: [6]u8 = undefined;
                const encoded = std.fmt.bufPrint(&buf, "\\u{X:0>4}", .{c}) catch unreachable;
                _ = try arena.append(encoded);
            },
            else => _ = try arena.append(&[_]u8{c}),
        }
    }
    _ = try arena.append("\"");
}

/// separate parser import — avoiding Zig 0.15 module-duplication errors.
pub const parseQuery = parser.parse;
/// Re-export Query type for the same reason.
pub const Query = parser.Query;

/// Execute a SQL query on a CSV file
pub fn execute(allocator: Allocator, query: parser.Query, output_file: std.fs.File, opts: options_mod.Options) !void {
    // Check if reading from stdin
    const is_stdin = std.mem.eql(u8, query.file_path, "-") or std.mem.eql(u8, query.file_path, "stdin");

    if (is_stdin) {
        try executeFromStdin(allocator, query, output_file, opts);
        return;
    }

    // JOIN query: load right table into hash map, probe with left table
    if (query.joins.len > 0) {
        try executeJoin(allocator, query, output_file, opts);
        return;
    }

    // Scalar aggregate (no GROUP BY): SELECT COUNT(*)/SUM/AVG/MIN/MAX
    if (query.group_by.len == 0 and hasAggregates(query)) {
        if (hasRegularColumns(query)) return error.MixedAggregateAndNonAggregateSelect;
        try executeScalarAgg(allocator, query, output_file, opts);
        return;
    }

    // DISTINCT without GROUP BY: route through the hash-based GROUP BY engine.
    // SELECT DISTINCT c1,c2 ≡ SELECT c1,c2 GROUP BY c1,c2 — same O(unique rows)
    // memory and a single sequential mmap scan, just like our GROUP BY path.
    if (query.distinct and query.group_by.len == 0) {
        try executeDistinct(allocator, query, output_file, opts);
        return;
    }

    // Check for GROUP BY - requires sequential processing
    if (query.group_by.len > 0) {
        try executeGroupBy(allocator, query, output_file, opts);
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
            try parallel_mmap.executeParallelMapped(allocator, query, file, output_file, opts);
            return;
        }
    }

    // Use memory-mapped I/O for medium-large files
    if (file_stat.size > 5 * 1024 * 1024) {
        try mmap_engine.executeMapped(allocator, query, file, output_file, opts);
        return;
    }

    // Sequential execution for smaller files
    try executeSequential(allocator, query, file, output_file, opts);
}

/// Execute query sequentially
fn executeSequential(
    allocator: Allocator,
    query: parser.Query,
    input_file: std.fs.File,
    output_file: std.fs.File,
    opts: options_mod.Options,
) !void {
    // Use bulk CSV reader for much better performance
    var reader = try bulk_csv.BulkCsvReader.init(allocator, input_file);
    defer reader.deinit();
    reader.delimiter = opts.delimiter;

    var writer = csv.RecordWriter.init(output_file, opts);
    defer writer.deinit();

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
    try writer.writeHeader(output_header.items, opts.no_header);

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
                                .like => parser.matchLike(field_value, comp.value),
                            };
                        } else {
                            // String comparison
                            matches = switch (comp.operator) {
                                .equal => std.mem.eql(u8, field_value, comp.value),
                                .not_equal => !std.mem.eql(u8, field_value, comp.value),
                                .like => parser.matchLike(field_value, comp.value),
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
            // Build output line into arena and store sort key
            const a = &(arena.?);
            const sort_key = try a.append(output_row[order_by_column_idx.?]);

            // Build the line representation: CSV or JSON depending on format
            const line_start = a.pos;
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
                try writer.writeRawLine(entry.line);
            }
        }
    }

    try writer.finish();
    try writer.flush();
}

/// A fully-materialised in-memory table produced by hash-join steps.
/// Rows are owned by the arena passed to joinOneStep().
const JoinTable = struct {
    /// Lowercased column names in order (owned by arena).
    headers: [][]const u8,
    /// All matched rows; each row slice and each field slice owned by arena.
    rows: std.ArrayList([][]const u8),
};

/// Perform one hash-join step: probe `left` rows against the CSV file named
/// in `join.right_file`, returning all matched merged rows.
/// All output strings are allocated from `arena`.
fn joinOneStep(
    allocator: Allocator,
    arena: Allocator,
    left_headers: []const []const u8,
    left_rows: []const []const u8,
    join: parser.JoinClause,
    delimiter: u8,
) !JoinTable {
    const lw = left_headers.len;

    // --- Open and read the right (build-side) file ---
    const right_file_h = try std.fs.cwd().openFile(join.right_file, .{});
    defer right_file_h.close();

    var right_reader = try bulk_csv.BulkCsvReader.init(allocator, right_file_h);
    defer right_reader.deinit();
    right_reader.delimiter = delimiter;

    const rh_raw = try right_reader.readRecord() orelse return error.EmptyFile;
    defer right_reader.freeRecord(rh_raw);
    const rw = rh_raw.len;

    // Build lowercase right header array (in arena)
    var right_lh = try arena.alloc([]const u8, rw);
    var right_col_map = std.StringHashMap(usize).init(arena);
    for (rh_raw, 0..) |col, i| {
        const lower = try arena.alloc(u8, col.len);
        _ = std.ascii.lowerString(lower, col);
        right_lh[i] = lower;
        try right_col_map.put(lower, i);
        if (join.right_alias.len > 0) {
            const qname = try std.fmt.allocPrint(arena, "{s}.{s}", .{ join.right_alias, lower });
            try right_col_map.put(qname, i);
        }
    }

    // Find left join-key index using left_col_map
    var left_col_map = std.StringHashMap(usize).init(arena);
    for (left_headers, 0..) |h, i| {
        try left_col_map.put(h, i);
        // Also register qualified name if the header already encodes an alias prefix
        // (headers from previous join steps may be plain lowercase names)
    }
    // Register left alias mappings if present
    if (join.left_alias.len > 0) {
        for (left_headers, 0..) |h, i| {
            // Strip any existing alias prefix before re-qualifying
            const bare = if (std.mem.indexOf(u8, h, ".")) |d| h[d + 1 ..] else h;
            const qname = try std.fmt.allocPrint(arena, "{s}.{s}", .{ join.left_alias, bare });
            try left_col_map.put(qname, i);
        }
    }

    const ljidx = left_col_map.get(join.left_col) orelse return error.JoinColumnNotFound;
    const rjidx = right_col_map.get(join.right_col) orelse return error.JoinColumnNotFound;

    // --- Load right table into hash map ---
    var right_index = std.StringHashMap(std.ArrayList([][]const u8)).init(allocator);
    defer {
        var it = right_index.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(allocator);
        right_index.deinit();
    }

    while (try right_reader.readRecordSlices()) |rrow| {
        if (rjidx >= rrow.len) continue;
        const key = try arena.dupe(u8, rrow[rjidx]);
        const gop = try right_index.getOrPut(key);
        if (!gop.found_existing) {
            gop.value_ptr.* = std.ArrayList([][]const u8){};
        }
        const rcopy = try arena.alloc([]const u8, rrow.len);
        for (rrow, 0..) |f, fi| rcopy[fi] = try arena.dupe(u8, f);
        try gop.value_ptr.append(allocator, rcopy);
    }

    // --- Build merged headers for this step ---
    var merged_headers = try arena.alloc([]const u8, lw + rw);
    for (left_headers, 0..) |h, i| merged_headers[i] = h;
    for (right_lh, 0..) |h, i| merged_headers[lw + i] = h;

    // --- Probe: build output rows ---
    var out_rows = std.ArrayList([][]const u8){};

    // left_rows is a flat slice of lw fields per logical row
    const n_left_rows = left_rows.len / lw;
    var ri: usize = 0;
    while (ri < n_left_rows) : (ri += 1) {
        const lrow = left_rows[ri * lw .. ri * lw + lw];
        if (ljidx >= lrow.len) continue;
        const lkey = lrow[ljidx];
        const matches = right_index.get(lkey) orelse continue;
        for (matches.items) |rrow| {
            var merged = try arena.alloc([]const u8, lw + rw);
            for (0..lw) |ci| merged[ci] = lrow[ci];
            for (0..rw) |ci| merged[lw + ci] = if (ci < rrow.len) rrow[ci] else "";
            try out_rows.append(allocator, merged);
        }
    }

    return JoinTable{
        .headers = merged_headers,
        .rows = out_rows,
    };
}

/// Pipelined context threaded through the recursive expand-and-emit helper.
const JoinCtx = struct {
    right_maps: []const std.StringHashMap(std.ArrayList([][]const u8)),
    left_key_idxs: []const usize,
    right_ws: []const usize,
    n_steps: usize,
    final_headers: []const []const u8,
    output_indices: []const usize,
    output_row: [][]const u8,
    where_merged_idx: ?usize,
    where_expr: ?parser.Expression,
    writer: *csv.RecordWriter,
    rows_written: i32,
    limit: i32,
    allocator: Allocator, // for complex WHERE row_map only
};

/// Recursively fill `merged[filled_w..]` from right-table matches for `step_idx`,
/// then emit the fully-merged row when all steps are satisfied.
/// Base-row slices in `merged[0..base_w]` are zero-copy (valid for this base row's
/// expansion) and right-table slices are arena-duped (valid for the whole query).
fn expandAndEmit(merged: [][]const u8, filled_w: usize, step_idx: usize, ctx: *JoinCtx) !void {
    if (step_idx == ctx.n_steps) {
        // All join steps satisfied — apply WHERE then project and write.
        if (ctx.where_expr) |expr| {
            if (expr == .comparison) {
                const comp = expr.comparison;
                const wi = ctx.where_merged_idx orelse return; // col not resolved → skip
                const fv = merged[wi];
                const ok = if (comp.numeric_value) |thr| blk: {
                    const v = std.fmt.parseFloat(f64, fv) catch return;
                    break :blk switch (comp.operator) {
                        .equal => v == thr,
                        .not_equal => v != thr,
                        .greater => v > thr,
                        .greater_equal => v >= thr,
                        .less => v < thr,
                        .less_equal => v <= thr,
                        .like => parser.matchLike(fv, comp.value),
                    };
                } else switch (comp.operator) {
                    .equal => std.mem.eql(u8, fv, comp.value),
                    .not_equal => !std.mem.eql(u8, fv, comp.value),
                    .like => parser.matchLike(fv, comp.value),
                    else => false,
                };
                if (!ok) return;
            } else {
                // Complex WHERE (AND/OR/NOT): build a temporary row map.
                var row_map = std.StringHashMap([]const u8).init(ctx.allocator);
                defer row_map.deinit();
                for (ctx.final_headers, 0..) |h, ci| try row_map.put(h, merged[ci]);
                if (!parser.evaluate(expr, row_map)) return;
            }
        }
        for (ctx.output_indices, 0..) |midx, oi| ctx.output_row[oi] = merged[midx];
        try ctx.writer.writeRecord(ctx.output_row);
        ctx.rows_written += 1;
        if (ctx.limit >= 0 and ctx.rows_written >= ctx.limit) return error.LimitReached;
        return;
    }

    // Probe the right hash map for this step using the left key from the merged row.
    const lkey = merged[ctx.left_key_idxs[step_idx]];
    const matches = ctx.right_maps[step_idx].get(lkey) orelse return; // INNER: no match → discard
    const rw = ctx.right_ws[step_idx];
    for (matches.items) |rrow| {
        // Fill right fields in-place; the next recursion level handles the next step.
        for (0..rw) |ci| merged[filled_w + ci] = if (ci < rrow.len) rrow[ci] else "";
        try expandAndEmit(merged, filled_w + rw, step_idx + 1, ctx);
    }
}

/// Execute an INNER JOIN between two or more CSV files using a pipelined hash-join.
///
/// All right-side tables are loaded into hash maps upfront (they are typically small
/// lookup tables).  The left (base) table is then streamed row-by-row using the
/// zero-copy BulkCsvReader slices.  For each base row, expandAndEmit() recursively
/// fills a single pre-allocated merged_row[] buffer through all join probes and
/// writes matching rows directly to output.
///
/// No materialisation of the left or any intermediate result; no per-row allocations
/// in the hot loop — only a pointer-copy into the stack-like merged_row[] buffer.
fn executeJoin(
    allocator: Allocator,
    query: parser.Query,
    output_file: std.fs.File,
    opts: options_mod.Options,
) !void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();
    const joins = query.joins;
    std.debug.assert(joins.len > 0);

    // ── Step 1: Read base schema (header only; rows are streamed in Step 4) ──
    const base_file = try std.fs.cwd().openFile(query.file_path, .{});
    defer base_file.close();
    var base_reader = try bulk_csv.BulkCsvReader.init(allocator, base_file);
    defer base_reader.deinit();
    base_reader.delimiter = opts.delimiter;

    const base_hdr_raw = try base_reader.readRecord() orelse return error.EmptyFile;
    defer base_reader.freeRecord(base_hdr_raw);
    const base_w = base_hdr_raw.len;
    var base_headers = try aa.alloc([]const u8, base_w);
    for (base_hdr_raw, 0..) |col, i| {
        const lower = try aa.alloc(u8, col.len);
        _ = std.ascii.lowerString(lower, col);
        base_headers[i] = lower;
    }

    // ── Step 2: Load all right hash maps; pre-resolve join key indices ────────
    // right_maps[ji] : right join col value → list of right rows (arena strings)
    // left_key_idxs[ji]: index of the left join key in the accumulated schema at step ji
    // right_ws[ji]   : column count for right table ji
    //
    // alias_ranges[0] = base table; alias_ranges[1..] = right tables in join order.
    // Each entry records which contiguous slice of acc_hdrs belongs to that alias,
    // enabling precise alias.col → index lookups without first-hit collisions.
    const AliasRange = struct { name: []const u8, start: usize, width: usize };
    var right_maps = try aa.alloc(std.StringHashMap(std.ArrayList([][]const u8)), joins.len);
    var left_key_idxs = try aa.alloc(usize, joins.len);
    var right_ws = try aa.alloc(usize, joins.len);
    var alias_ranges = try aa.alloc(AliasRange, joins.len + 1);
    alias_ranges[0] = .{ .name = joins[0].left_alias, .start = 0, .width = base_w };
    var acc_offset: usize = base_w;

    var acc_hdrs = std.ArrayList([]const u8){};
    for (base_headers) |h| try acc_hdrs.append(aa, h);

    for (joins, 0..) |join, ji| {
        // Resolve left join key using alias-range-precise lmap.
        // For each table already in acc_hdrs, register both bare (first-seen wins)
        // and alias-qualified (precise, always-correct) names.
        var lmap = std.StringHashMap(usize).init(aa);
        for (alias_ranges[0 .. ji + 1]) |ar| {
            for (0..ar.width) |ci| {
                const idx = ar.start + ci;
                const bare_col = acc_hdrs.items[idx];
                // bare: first-seen wins (unambiguous bare refs still work)
                const bgop = try lmap.getOrPut(bare_col);
                if (!bgop.found_existing) bgop.value_ptr.* = idx;
                // qualified alias.col: always set the correct value for this alias
                if (ar.name.len > 0) {
                    const qname = try std.fmt.allocPrint(aa, "{s}.{s}", .{ ar.name, bare_col });
                    try lmap.put(qname, idx);
                }
            }
        }
        left_key_idxs[ji] = lmap.get(join.left_col) orelse return error.JoinColumnNotFound;

        // Open right table, build hash map.
        const rf = try std.fs.cwd().openFile(join.right_file, .{});
        defer rf.close();
        var rr = try bulk_csv.BulkCsvReader.init(allocator, rf);
        defer rr.deinit();
        rr.delimiter = opts.delimiter;

        const rh_raw = try rr.readRecord() orelse return error.EmptyFile;
        defer rr.freeRecord(rh_raw);
        const rw = rh_raw.len;
        right_ws[ji] = rw;
        // Record alias range for this right table before extending acc_hdrs.
        alias_ranges[ji + 1] = .{ .name = join.right_alias, .start = acc_offset, .width = rw };
        acc_offset += rw;

        var right_lh = try aa.alloc([]const u8, rw);
        var right_col_map = std.StringHashMap(usize).init(aa);
        for (rh_raw, 0..) |col, i| {
            const lower = try aa.alloc(u8, col.len);
            _ = std.ascii.lowerString(lower, col);
            right_lh[i] = lower;
            try right_col_map.put(lower, i);
            if (join.right_alias.len > 0) {
                const qname = try std.fmt.allocPrint(aa, "{s}.{s}", .{ join.right_alias, lower });
                try right_col_map.put(qname, i);
            }
        }
        const rjidx = right_col_map.get(join.right_col) orelse return error.JoinColumnNotFound;

        var rmap = std.StringHashMap(std.ArrayList([][]const u8)).init(allocator);
        while (try rr.readRecordSlices()) |rrow| {
            if (rjidx >= rrow.len) continue;
            const key = try aa.dupe(u8, rrow[rjidx]);
            const gop = try rmap.getOrPut(key);
            if (!gop.found_existing) gop.value_ptr.* = std.ArrayList([][]const u8){};
            const rcopy = try aa.alloc([]const u8, rrow.len);
            for (rrow, 0..) |f, fi| rcopy[fi] = try aa.dupe(u8, f);
            try gop.value_ptr.append(allocator, rcopy);
        }
        right_maps[ji] = rmap;

        // Extend accumulated schema with this right table's headers.
        for (right_lh) |h| try acc_hdrs.append(aa, h);
    }

    // Free ArrayList.items in right hash maps on exit (strings live in arena).
    defer for (right_maps) |*rmap| {
        var it = rmap.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(allocator);
        rmap.deinit();
    };

    const final_headers = acc_hdrs.items;
    const total_w = final_headers.len;

    // ── Step 3: col_map, ambiguity counts, output indices, WHERE index ────────
    var col_map = std.StringHashMap(usize).init(aa);
    var bare_count = std.StringHashMap(usize).init(aa);
    for (final_headers, 0..) |h, i| {
        // bare name: first-seen wins (col_map lookup for unqualified columns)
        const bgop = try col_map.getOrPut(h);
        if (!bgop.found_existing) bgop.value_ptr.* = i;
        const bare_h = if (std.mem.indexOf(u8, h, ".")) |d| h[d + 1 ..] else h;
        const bc_gop = try bare_count.getOrPut(bare_h);
        if (!bc_gop.found_existing) bc_gop.value_ptr.* = 0;
        bc_gop.value_ptr.* += 1;
    }
    // Register alias.col → precise index using alias_ranges.
    // This replaces the old first-hit getOrPut loops that could map b.id → a.id.
    for (alias_ranges) |ar| {
        if (ar.name.len == 0) continue;
        for (0..ar.width) |ci| {
            const idx = ar.start + ci;
            const qname = try std.fmt.allocPrint(aa, "{s}.{s}", .{ ar.name, final_headers[idx] });
            try col_map.put(qname, idx); // always correct: no first-hit collision
        }
    }

    var output_indices = std.ArrayList(usize){};
    var output_header_names = std.ArrayList([]const u8){};

    if (query.all_columns) {
        for (0..total_w) |i| {
            try output_indices.append(aa, i);
            try output_header_names.append(aa, final_headers[i]);
        }
    } else {
        for (query.columns) |col_raw| {
            const col = blk: {
                const tmp = try aa.alloc(u8, col_raw.len);
                _ = std.ascii.lowerString(tmp, col_raw);
                break :blk tmp;
            };
            if (std.mem.indexOfScalar(u8, col, '.') == null) {
                if ((bare_count.get(col) orelse 0) > 1) return error.AmbiguousColumnReference;
            }
            if (col_map.get(col)) |idx| {
                try output_indices.append(aa, idx);
                try output_header_names.append(aa, final_headers[idx]);
            } else {
                // Qualified reference (alias.col) that missed col_map means the alias is
                // unknown — never silently fall back to bare name.
                if (std.mem.indexOf(u8, col, ".") != null) return error.ColumnNotFound;
                const idx = col_map.get(col) orelse return error.ColumnNotFound;
                try output_indices.append(aa, idx);
                try output_header_names.append(aa, final_headers[idx]);
            }
        }
    }

    var where_merged_idx: ?usize = null;
    if (query.where_expr) |expr| {
        if (expr == .comparison) {
            const col = expr.comparison.column;
            if (col_map.get(col)) |idx| {
                where_merged_idx = idx;
            } else if (std.mem.indexOf(u8, col, ".") != null) {
                // Qualified reference (alias.col) that col_map didn't find means unknown
                // alias — never silently fall back to bare name.
                return error.ColumnNotFound;
            }
        }
    }

    // ── Step 4: Stream base rows → expand through hash maps → emit ───────────
    // merged_row[] is allocated once and reused as a workspace for every base row.
    // Slots [0..base_w] are filled from zero-copy BulkCsvReader slices (valid until
    // the next readRecordSlices call).  Deeper slots are filled by expandAndEmit()
    // from arena-duped right-table strings (valid for the whole query).
    var writer = csv.RecordWriter.init(output_file, opts);
    defer writer.deinit();
    try writer.writeHeader(output_header_names.items, opts.no_header);

    var merged_row = try aa.alloc([]const u8, total_w);
    const output_row = try aa.alloc([]const u8, output_indices.items.len);

    var ctx = JoinCtx{
        .right_maps = right_maps,
        .left_key_idxs = left_key_idxs,
        .right_ws = right_ws,
        .n_steps = joins.len,
        .final_headers = final_headers,
        .output_indices = output_indices.items,
        .output_row = output_row,
        .where_merged_idx = where_merged_idx,
        .where_expr = query.where_expr,
        .writer = &writer,
        .rows_written = 0,
        .limit = query.limit,
        .allocator = allocator,
    };

    while (try base_reader.readRecordSlices()) |base_row| {
        for (0..base_w) |ci| merged_row[ci] = if (ci < base_row.len) base_row[ci] else "";
        expandAndEmit(merged_row, base_w, 0, &ctx) catch |err| switch (err) {
            error.LimitReached => break,
            else => return err,
        };
    }

    try writer.finish();
    try writer.flush();
}

/// Execute query from stdin
fn executeFromStdin(
    allocator: Allocator,
    query: parser.Query,
    output_file: std.fs.File,
    opts: options_mod.Options,
) !void {
    const stdin = std.fs.File{ .handle = std.posix.STDIN_FILENO };
    var reader = csv.CsvReader.init(allocator, stdin);
    reader.delimiter = opts.delimiter;
    var writer = csv.RecordWriter.init(output_file, opts);
    defer writer.deinit();

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
    try writer.writeHeader(output_header.items, opts.no_header);

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
                                .like => parser.matchLike(field_value, comp.value),
                            };
                        } else {
                            // String comparison
                            matches = switch (comp.operator) {
                                .equal => std.mem.eql(u8, field_value, comp.value),
                                .not_equal => !std.mem.eql(u8, field_value, comp.value),
                                .like => parser.matchLike(field_value, comp.value),
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

    try writer.finish();
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
inline fn splitLine(line: []const u8, buf: [][]const u8, delim: u8) []const []const u8 {
    var n: usize = 0;
    var it = std.mem.splitScalar(u8, line, delim);
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
    opts: options_mod.Options,
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
    std.posix.madvise(mapped.ptr, mapped.len, std.posix.MADV.SEQUENTIAL) catch {};
    const data = mapped[0..file_size];

    var writer = csv.RecordWriter.init(output_file, opts);
    defer writer.deinit();

    // Header
    const header_nl = std.mem.indexOfScalar(u8, data, '\n') orelse return error.NoHeader;
    var header_line = data[0..header_nl];
    if (header_line.len > 0 and header_line[header_line.len - 1] == '\r')
        header_line = header_line[0 .. header_line.len - 1];

    var header_list = std.ArrayListUnmanaged([]const u8){};
    defer header_list.deinit(allocator);
    {
        var it = std.mem.splitScalar(u8, header_line, opts.delimiter);
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
    try writer.writeHeader(out_header.items, opts.no_header);

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

        const record = splitLine(data[line_start..line_end], &field_stk, opts.delimiter);

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
                        .like => parser.matchLike(fv, comp.value),
                    };
                } else {
                    matches = switch (comp.operator) {
                        .equal => std.mem.eql(u8, fv, comp.value),
                        .not_equal => !std.mem.eql(u8, fv, comp.value),
                        .like => parser.matchLike(fv, comp.value),
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
            switch (opts.format) {
                .csv => {
                    for (out_row, 0..) |field, i| {
                        if (i > 0) _ = try a.append(",");
                        _ = try a.append(field);
                    }
                },
                .json, .jsonl => {
                    _ = try a.append("{");
                    for (out_row, 0..) |field, i| {
                        if (i > 0) _ = try a.append(",");
                        try appendJsonStringToArena(a, out_header.items[i]);
                        _ = try a.append(":");
                        try appendJsonStringToArena(a, field);
                    }
                    _ = try a.append("}");
                },
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
                try writer.writeRawLine(entry.line);
            }
        }
    }

    try writer.finish();
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
    opts: options_mod.Options,
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
    std.posix.madvise(mapped.ptr, mapped.len, std.posix.MADV.SEQUENTIAL) catch {};
    const data = mapped[0..file_size];

    var writer = csv.RecordWriter.init(output_file, opts);
    defer writer.deinit();

    // -- Header --
    const header_nl = std.mem.indexOfScalar(u8, data, '\n') orelse return error.NoHeader;
    var header_line = data[0..header_nl];
    if (header_line.len > 0 and header_line[header_line.len - 1] == '\r')
        header_line = header_line[0 .. header_line.len - 1];

    var header_list = std.ArrayListUnmanaged([]const u8){};
    defer header_list.deinit(allocator);
    {
        var it = std.mem.splitScalar(u8, header_line, opts.delimiter);
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
    try writer.writeHeader(out_header_list.items, opts.no_header);

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

    // -- Scan (parallel on large files, sequential on small) --
    const num_cores_sa = try std.Thread.getCpuCount();
    if (num_cores_sa > 1 and file_size > 10 * 1024 * 1024) {
        const n_threads = num_cores_sa;
        const chunks = try splitLineChunks(data, header_nl + 1, n_threads, allocator);
        defer allocator.free(chunks);

        var thread_ctxs = try allocator.alloc(ScalarAggWorkerCtx, n_threads);
        defer allocator.free(thread_ctxs);
        var threads = try allocator.alloc(std.Thread, n_threads);
        defer allocator.free(threads);

        for (0..n_threads) |i| {
            thread_ctxs[i] = .{
                .data = data,
                .chunk_start = chunks[i][0],
                .chunk_end = chunks[i][1],
                .lower_header = lower_header,
                .agg_specs = agg_specs.items,
                .where_col_idx = where_col_idx,
                .where_expr = query.where_expr,
                .n_aggs = n_aggs,
                .delimiter = opts.delimiter,
                .arena = std.heap.ArenaAllocator.init(allocator),
                .partial_accum = undefined,
                .err = null,
            };
            threads[i] = try std.Thread.spawn(.{}, scalarAggWorkerThread, .{&thread_ctxs[i]});
        }
        for (threads) |t| t.join();

        for (thread_ctxs) |ctx| {
            if (ctx.err) |e| return e;
        }

        // Merge partial accumulators into the single `accum`
        for (thread_ctxs) |*ctx| {
            defer ctx.arena.deinit();
            const partial = &ctx.partial_accum;
            accum.count += partial.count;
            for (0..n_aggs) |i| {
                accum.sums[i] += partial.sums[i];
                accum.sum_counts[i] += partial.sum_counts[i];
                if (partial.mins[i] < accum.mins[i]) accum.mins[i] = partial.mins[i];
                if (partial.maxs[i] > accum.maxs[i]) accum.maxs[i] = partial.maxs[i];
            }
        }
    } else {
        // -- Sequential scan --
        var scan_pos: usize = header_nl + 1;
        while (scan_pos < data.len) {
            const line_start = scan_pos;
            const nl = std.mem.indexOfScalarPos(u8, data, scan_pos, '\n');
            var line_end = nl orelse data.len;
            scan_pos = if (nl) |n| n + 1 else data.len;
            if (line_end > line_start and data[line_end - 1] == '\r') line_end -= 1;
            if (line_end <= line_start) continue;

            const record = splitLine(data[line_start..line_end], &field_stk, opts.delimiter);

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
                            .like => parser.matchLike(fv, comp.value),
                        };
                    } else {
                        matches = switch (comp.operator) {
                            .equal => std.mem.eql(u8, fv, comp.value),
                            .not_equal => !std.mem.eql(u8, fv, comp.value),
                            .like => parser.matchLike(fv, comp.value),
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
    try writer.finish();
    try writer.flush();
}

// =============================================================================
// Parallel GROUP BY / Scalar Agg — map-reduce worker infrastructure
//
// Strategy: split the mmap data into N line-aligned chunks (one per core),
// each thread independently accumulates into its own partial HashMap / accum,
// main thread merges arithmetic results (no locking during scan).
// Merge is O(num_groups × num_threads) which is negligible for GROUP BY since
// cardinality is typically tiny (6–50 groups).
// =============================================================================

/// Split [data_start, data.len) into n line-aligned byte ranges.
/// Each boundary is snapped to the next '\n' so no line is split across chunks.
fn splitLineChunks(
    data: []const u8,
    data_start: usize,
    n: usize,
    allocator: Allocator,
) ![][2]usize {
    const result = try allocator.alloc([2]usize, n);
    const body_len = data.len - data_start;
    const chunk_size = body_len / n;
    for (0..n) |i| {
        var start = data_start + i * chunk_size;
        var end = if (i + 1 == n) data.len else data_start + (i + 1) * chunk_size;
        if (i > 0) {
            if (std.mem.indexOfScalarPos(u8, data, start, '\n')) |nl| start = nl + 1;
        }
        if (i + 1 < n) {
            if (std.mem.indexOfScalarPos(u8, data, end, '\n')) |nl| end = nl + 1;
        }
        result[i] = .{ start, end };
    }
    return result;
}

/// Per-thread context for a parallel GROUP BY chunk scan.
/// All per-thread allocations go through `arena`; calling arena.deinit()
/// after the merge frees everything (partial_map, keys, CompactAccum arrays).
const GbWorkerCtx = struct {
    // Shared read-only inputs
    data: []const u8,
    chunk_start: usize,
    chunk_end: usize,
    lower_header: []const []const u8,
    group_indices: []const usize,
    agg_specs: []const AggSpec,
    where_col_idx: ?usize,
    where_expr: ?parser.Expression, // shallow copy — read-only
    n_aggs: usize,
    delimiter: u8,
    // Per-thread outputs (all arena-owned, freed together after merge)
    arena: std.heap.ArenaAllocator,
    partial_map: std.StringHashMap(CompactAccum),
    err: ?anyerror = null,
};

fn gbWorkerThread(ctx: *GbWorkerCtx) void {
    gbWorkerScan(ctx) catch |e| {
        ctx.err = e;
    };
}

fn gbWorkerScan(ctx: *GbWorkerCtx) !void {
    const aa = ctx.arena.allocator();
    ctx.partial_map = std.StringHashMap(CompactAccum).init(aa);
    try ctx.partial_map.ensureTotalCapacity(64);

    var field_stk: [256][]const u8 = undefined;
    var key_buf = std.ArrayListUnmanaged(u8){};
    var fallback_row_map = std.StringHashMap([]const u8).init(aa);

    var pos = ctx.chunk_start;
    while (pos < ctx.chunk_end) {
        const line_start = pos;
        const nl_pos = std.mem.indexOfScalarPos(u8, ctx.data, pos, '\n');
        var line_end = if (nl_pos) |n| @min(n, ctx.chunk_end) else ctx.chunk_end;
        pos = if (nl_pos) |n| @min(n + 1, ctx.chunk_end) else ctx.chunk_end;
        if (line_end > line_start and ctx.data[line_end - 1] == '\r') line_end -= 1;
        if (line_end <= line_start) continue;

        const record = splitLine(ctx.data[line_start..line_end], &field_stk, ctx.delimiter);

        // WHERE filter
        if (ctx.where_expr) |expr| {
            if (expr == .comparison) {
                const comp = expr.comparison;
                const cidx = ctx.where_col_idx orelse continue;
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
                        .like => parser.matchLike(fv, comp.value),
                    };
                } else {
                    matches = switch (comp.operator) {
                        .equal => std.mem.eql(u8, fv, comp.value),
                        .not_equal => !std.mem.eql(u8, fv, comp.value),
                        .like => parser.matchLike(fv, comp.value),
                        else => false,
                    };
                }
                if (!matches) continue;
            } else {
                fallback_row_map.clearRetainingCapacity();
                for (ctx.lower_header, 0..) |lh, i| {
                    if (i < record.len) try fallback_row_map.put(lh, record[i]);
                }
                if (!parser.evaluate(expr, fallback_row_map)) continue;
            }
        }

        // Build NUL-separated group key
        key_buf.clearRetainingCapacity();
        for (ctx.group_indices, 0..) |cidx, i| {
            if (i > 0) try key_buf.append(aa, 0);
            try key_buf.appendSlice(aa, if (cidx < record.len) record[cidx] else "");
        }

        const gop = try ctx.partial_map.getOrPut(key_buf.items);
        if (!gop.found_existing) {
            gop.key_ptr.* = try aa.dupe(u8, key_buf.items);
            var key_vals = try aa.alloc([]const u8, ctx.group_indices.len);
            for (ctx.group_indices, 0..) |gcidx, gi| {
                key_vals[gi] = try aa.dupe(u8, if (gcidx < record.len) record[gcidx] else "");
            }
            gop.value_ptr.* = try CompactAccum.init(aa, key_vals, ctx.n_aggs);
        }
        const accum = gop.value_ptr;

        accum.count += 1;
        for (ctx.agg_specs, 0..) |spec, i| {
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
}

/// Per-thread context for a parallel scalar aggregate chunk scan.
const ScalarAggWorkerCtx = struct {
    data: []const u8,
    chunk_start: usize,
    chunk_end: usize,
    lower_header: []const []const u8,
    agg_specs: []const AggSpec,
    where_col_idx: ?usize,
    where_expr: ?parser.Expression,
    n_aggs: usize,
    delimiter: u8,
    arena: std.heap.ArenaAllocator,
    partial_accum: CompactAccum,
    err: ?anyerror = null,
};

fn scalarAggWorkerThread(ctx: *ScalarAggWorkerCtx) void {
    scalarAggWorkerScan(ctx) catch |e| {
        ctx.err = e;
    };
}

fn scalarAggWorkerScan(ctx: *ScalarAggWorkerCtx) !void {
    const aa = ctx.arena.allocator();
    const empty_keys = try aa.alloc([]const u8, 0);
    ctx.partial_accum = try CompactAccum.init(aa, empty_keys, ctx.n_aggs);

    var field_stk: [256][]const u8 = undefined;
    var fallback_row_map = std.StringHashMap([]const u8).init(aa);

    var pos = ctx.chunk_start;
    while (pos < ctx.chunk_end) {
        const line_start = pos;
        const nl_pos = std.mem.indexOfScalarPos(u8, ctx.data, pos, '\n');
        var line_end = if (nl_pos) |n| @min(n, ctx.chunk_end) else ctx.chunk_end;
        pos = if (nl_pos) |n| @min(n + 1, ctx.chunk_end) else ctx.chunk_end;
        if (line_end > line_start and ctx.data[line_end - 1] == '\r') line_end -= 1;
        if (line_end <= line_start) continue;

        const record = splitLine(ctx.data[line_start..line_end], &field_stk, ctx.delimiter);

        if (ctx.where_expr) |expr| {
            if (expr == .comparison) {
                const comp = expr.comparison;
                const cidx = ctx.where_col_idx orelse continue;
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
                        .like => parser.matchLike(fv, comp.value),
                    };
                } else {
                    matches = switch (comp.operator) {
                        .equal => std.mem.eql(u8, fv, comp.value),
                        .not_equal => !std.mem.eql(u8, fv, comp.value),
                        .like => parser.matchLike(fv, comp.value),
                        else => false,
                    };
                }
                if (!matches) continue;
            } else {
                fallback_row_map.clearRetainingCapacity();
                for (ctx.lower_header, 0..) |lh, i| {
                    if (i < record.len) try fallback_row_map.put(lh, record[i]);
                }
                if (!parser.evaluate(expr, fallback_row_map)) continue;
            }
        }

        ctx.partial_accum.count += 1;
        for (ctx.agg_specs, 0..) |spec, i| {
            switch (spec.func_type) {
                .count => {},
                .sum, .avg => {
                    if (spec.col_idx) |cidx| {
                        if (cidx < record.len) {
                            if (parseNumericFast(record[cidx])) |val| {
                                ctx.partial_accum.sums[i] += val;
                                ctx.partial_accum.sum_counts[i] += 1;
                            } else |_| {}
                        }
                    }
                },
                .min => {
                    if (spec.col_idx) |cidx| {
                        if (cidx < record.len) {
                            if (parseNumericFast(record[cidx])) |val| {
                                if (val < ctx.partial_accum.mins[i]) ctx.partial_accum.mins[i] = val;
                            } else |_| {}
                        }
                    }
                },
                .max => {
                    if (spec.col_idx) |cidx| {
                        if (cidx < record.len) {
                            if (parseNumericFast(record[cidx])) |val| {
                                if (val > ctx.partial_accum.maxs[i]) ctx.partial_accum.maxs[i] = val;
                            } else |_| {}
                        }
                    }
                },
            }
        }
    }
}

/// Execute GROUP BY query.
///
/// Uses mmap for zero-copy sequential scanning and CompactAccum for
/// O(1) aggregate accumulation per row (flat array r/w, no per-group HashMaps).
fn executeGroupBy(
    allocator: Allocator,
    query: parser.Query,
    output_file: std.fs.File,
    opts: options_mod.Options,
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
    std.posix.madvise(mapped.ptr, mapped.len, std.posix.MADV.SEQUENTIAL) catch {};
    const data = mapped[0..file_size];

    var writer = csv.RecordWriter.init(output_file, opts);
    defer writer.deinit();

    // -- Header parsing -----------------------------------------------------
    const header_nl = std.mem.indexOfScalar(u8, data, '\n') orelse return error.NoHeader;
    var header_line = data[0..header_nl];
    if (header_line.len > 0 and header_line[header_line.len - 1] == '\r')
        header_line = header_line[0 .. header_line.len - 1];

    var header_list = std.ArrayListUnmanaged([]const u8){};
    defer header_list.deinit(allocator);
    {
        var it = std.mem.splitScalar(u8, header_line, opts.delimiter);
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
    try writer.writeHeader(out_header_list.items, opts.no_header);

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

    // -- Main scan loop -------------------------------------------------------
    // For large files on multi-core machines: parallel map-reduce.
    // Each thread independently scans its chunk into its own partial_map;
    // main thread merges arithmetic results after join (O(num_groups), negligible).
    // Single-threaded fallback for small files or single-core environments.
    const num_cores = try std.Thread.getCpuCount();
    if (num_cores > 1 and file_size > 10 * 1024 * 1024) {
        const n_threads = num_cores;
        const chunks = try splitLineChunks(data, header_nl + 1, n_threads, allocator);
        defer allocator.free(chunks);

        var thread_ctxs = try allocator.alloc(GbWorkerCtx, n_threads);
        defer allocator.free(thread_ctxs);
        var threads = try allocator.alloc(std.Thread, n_threads);
        defer allocator.free(threads);

        for (0..n_threads) |i| {
            thread_ctxs[i] = .{
                .data = data,
                .chunk_start = chunks[i][0],
                .chunk_end = chunks[i][1],
                .lower_header = lower_header,
                .group_indices = group_indices,
                .agg_specs = agg_specs.items,
                .where_col_idx = where_col_idx,
                .where_expr = query.where_expr,
                .n_aggs = n_aggs,
                .delimiter = opts.delimiter,
                .arena = std.heap.ArenaAllocator.init(allocator),
                .partial_map = undefined,
                .err = null,
            };
            threads[i] = try std.Thread.spawn(.{}, gbWorkerThread, .{&thread_ctxs[i]});
        }
        for (threads) |t| t.join();

        // Propagate first worker error, if any
        for (thread_ctxs) |ctx| {
            if (ctx.err) |e| return e;
        }

        // Merge partial maps into the main group_map (output phase reads group_map)
        for (thread_ctxs) |*ctx| {
            defer ctx.arena.deinit();
            var it = ctx.partial_map.iterator();
            while (it.next()) |entry| {
                const partial = entry.value_ptr;
                const gop = try group_map.getOrPut(entry.key_ptr.*);
                if (!gop.found_existing) {
                    // New group: copy key + accum arrays into main arena (ka)
                    gop.key_ptr.* = try ka.dupe(u8, entry.key_ptr.*);
                    var key_vals = try ka.alloc([]const u8, partial.key_values.len);
                    for (partial.key_values, 0..) |kv, ki| {
                        key_vals[ki] = try ka.dupe(u8, kv);
                    }
                    gop.value_ptr.* = CompactAccum{
                        .key_values = key_vals,
                        .count = partial.count,
                        .sums = try ka.dupe(f64, partial.sums),
                        .sum_counts = try ka.dupe(i64, partial.sum_counts),
                        .mins = try ka.dupe(f64, partial.mins),
                        .maxs = try ka.dupe(f64, partial.maxs),
                    };
                } else {
                    // Existing group: fold arithmetic values
                    const accum = gop.value_ptr;
                    accum.count += partial.count;
                    for (0..n_aggs) |i| {
                        accum.sums[i] += partial.sums[i];
                        accum.sum_counts[i] += partial.sum_counts[i];
                        if (partial.mins[i] < accum.mins[i]) accum.mins[i] = partial.mins[i];
                        if (partial.maxs[i] > accum.maxs[i]) accum.maxs[i] = partial.maxs[i];
                    }
                }
            }
        }
    } else {
        // -- Sequential mmap scan (single-core or small file) --
        var pos: usize = header_nl + 1;
        while (pos < data.len) {
            const line_start = pos;
            const nl = std.mem.indexOfScalarPos(u8, data, pos, '\n');
            var line_end = nl orelse data.len;
            pos = if (nl) |n| n + 1 else data.len;
            if (line_end > line_start and data[line_end - 1] == '\r') line_end -= 1;
            if (line_end <= line_start) continue;

            const record = splitLine(data[line_start..line_end], &field_stk, opts.delimiter);

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
                            .like => parser.matchLike(fv, comp.value),
                        };
                    } else {
                        matches = switch (comp.operator) {
                            .equal => std.mem.eql(u8, fv, comp.value),
                            .not_equal => !std.mem.eql(u8, fv, comp.value),
                            .like => parser.matchLike(fv, comp.value),
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

    try writer.finish();
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

    try execute(allocator, query, out_file, .{});

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

    try execute(allocator, query, out_file, .{});

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

    try execute(allocator, query, out_file, .{});

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

    try execute(allocator, query, out_file, .{});

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

    try execute(allocator, query, out_file, .{});

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

    try execute(allocator, query, out_file, .{});

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

    try execute(allocator, query, out_file, .{});

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

    try execute(allocator, query, out_file, .{});

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

    try execute(allocator, query, out_file, .{});

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

    try execute(allocator, query, out_file, .{});

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

    try execute(allocator, query, out_file, .{});

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

    try execute(allocator, query, out_file, .{});

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

    try execute(allocator, query, out_file, .{});

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

    try std.testing.expectError(error.ColumnNotFound, execute(allocator, query, out_file, .{}));
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

    try std.testing.expectError(error.MixedAggregateAndNonAggregateSelect, execute(allocator, query, out_file, .{}));
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

    try execute(allocator, query, out_file, .{});

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "COUNT(*)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "2"));
}

test "JSON output: basic SELECT produces JSON array" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "name,age\nAlice,35\nBob,42\n";
    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);
    const sql = try std.fmt.allocPrint(allocator, "SELECT name, age FROM '{s}'", .{in_path});
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output.json", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file, .{ .format = .json });

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    const expected =
        \\[
        \\{"name":"Alice","age":35},
        \\{"name":"Bob","age":42}
        \\]
        \\
    ;
    try std.testing.expectEqualStrings(expected, output);
}

test "JSONL output: basic SELECT produces newline-delimited JSON" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "name,age\nAlice,35\nBob,42\n";
    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);
    const sql = try std.fmt.allocPrint(allocator, "SELECT name, age FROM '{s}'", .{in_path});
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output.jsonl", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file, .{ .format = .jsonl });

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    const expected = "{\"name\":\"Alice\",\"age\":35}\n{\"name\":\"Bob\",\"age\":42}\n";
    try std.testing.expectEqualStrings(expected, output);
}

test "JSON output: WHERE clause filters rows" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "name,age\nAlice,35\nBob,42\nCarol,28\n";
    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);
    const sql = try std.fmt.allocPrint(allocator, "SELECT name, age FROM '{s}' WHERE age > 30", .{in_path});
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output_where.json", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file, .{ .format = .json });

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    // Only Alice (35) and Bob (42) match age > 30
    const expected =
        \\[
        \\{"name":"Alice","age":35},
        \\{"name":"Bob","age":42}
        \\]
        \\
    ;
    try std.testing.expectEqualStrings(expected, output);
}

test "JSON output: escapes special characters in field values" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // CSV field value contains a literal double-quote via RFC 4180 escaping ("")
    const csv_content = "id,note\n1,\"say \"\"hi\"\"\"\n";
    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);
    const sql = try std.fmt.allocPrint(allocator, "SELECT id, note FROM '{s}'", .{in_path});
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output_escape.jsonl", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file, .{ .format = .jsonl });

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    // The note value `say "hi"` must be JSON-escaped to `say \"hi\"`
    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "\\\"hi\\\""));
}

test "JSON output: ORDER BY path escapes control characters" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "id,note\n2,\"ok\"\n1,\"has\x0Cff\"\n";
    {
        const f = try tmp.dir.createFile("input_order_by_control.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input_order_by_control.csv", &in_path_buf);
    const sql = try std.fmt.allocPrint(allocator, "SELECT id, note FROM '{s}' ORDER BY id ASC", .{in_path});
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output_order_by_control.jsonl", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file, .{ .format = .jsonl });

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    try std.testing.expect(std.mem.containsAtLeast(u8, output, 1, "\\u000C"));
}

test "JSON output: CSV mode still produces CSV-compatible output" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const csv_content = "name,age\nAlice,35\nBob,42\n";
    {
        const f = try tmp.dir.createFile("input.csv", .{});
        defer f.close();
        try f.writeAll(csv_content);
    }

    var in_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const in_path = try tmp.dir.realpath("input.csv", &in_path_buf);
    const sql = try std.fmt.allocPrint(allocator, "SELECT name, age FROM '{s}'", .{in_path});
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("output_csv_compat.csv", .{ .read = true });
    defer out_file.close();

    // Default format is CSV - must produce identical output to old CsvWriter behaviour
    try execute(allocator, query, out_file, .{});

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    try std.testing.expectEqualStrings("name,age\nAlice,35\nBob,42\n", output);
}

// --- JOIN integration tests ---

test "INNER JOIN: basic two-file join" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // left: employees
    {
        const f = try tmp.dir.createFile("emp.csv", .{});
        defer f.close();
        try f.writeAll("id,name,dept_id\n1,Alice,10\n2,Bob,20\n3,Carol,10\n");
    }
    // right: departments
    {
        const f = try tmp.dir.createFile("dept.csv", .{});
        defer f.close();
        try f.writeAll("id,dept_name\n10,Engineering\n20,Marketing\n");
    }

    var emp_buf: [std.fs.max_path_bytes]u8 = undefined;
    var dept_buf: [std.fs.max_path_bytes]u8 = undefined;
    const emp_path = try tmp.dir.realpath("emp.csv", &emp_buf);
    const dept_path = try tmp.dir.realpath("dept.csv", &dept_buf);

    const sql = try std.fmt.allocPrint(
        allocator,
        "SELECT a.name, b.dept_name FROM '{s}' a INNER JOIN '{s}' b ON a.dept_id = b.id",
        .{ emp_path, dept_path },
    );
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("join_out.csv", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file, .{});

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    // Alice → Engineering, Bob → Marketing, Carol → Engineering (order matches left table)
    try std.testing.expectEqualStrings(
        "name,dept_name\nAlice,Engineering\nBob,Marketing\nCarol,Engineering\n",
        output,
    );
}

test "INNER JOIN with WHERE clause" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("emp.csv", .{});
        defer f.close();
        try f.writeAll("id,name,dept_id\n1,Alice,10\n2,Bob,20\n3,Carol,10\n");
    }
    {
        const f = try tmp.dir.createFile("dept.csv", .{});
        defer f.close();
        try f.writeAll("id,dept_name\n10,Engineering\n20,Marketing\n");
    }

    var emp_buf: [std.fs.max_path_bytes]u8 = undefined;
    var dept_buf: [std.fs.max_path_bytes]u8 = undefined;
    const emp_path = try tmp.dir.realpath("emp.csv", &emp_buf);
    const dept_path = try tmp.dir.realpath("dept.csv", &dept_buf);

    const sql = try std.fmt.allocPrint(
        allocator,
        "SELECT a.name, b.dept_name FROM '{s}' a JOIN '{s}' b ON a.dept_id = b.id WHERE b.dept_name = 'Engineering'",
        .{ emp_path, dept_path },
    );
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("join_where_out.csv", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file, .{});

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    try std.testing.expectEqualStrings(
        "name,dept_name\nAlice,Engineering\nCarol,Engineering\n",
        output,
    );
}

test "INNER JOIN SELECT *: all columns from both tables" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("l.csv", .{});
        defer f.close();
        try f.writeAll("id,val\n1,aaa\n2,bbb\n");
    }
    {
        const f = try tmp.dir.createFile("r.csv", .{});
        defer f.close();
        try f.writeAll("fk,extra\n1,xxx\n2,yyy\n");
    }

    var lb: [std.fs.max_path_bytes]u8 = undefined;
    var rb: [std.fs.max_path_bytes]u8 = undefined;
    const lp = try tmp.dir.realpath("l.csv", &lb);
    const rp = try tmp.dir.realpath("r.csv", &rb);

    const sql = try std.fmt.allocPrint(
        allocator,
        "SELECT * FROM '{s}' l JOIN '{s}' r ON l.id = r.fk",
        .{ lp, rp },
    );
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("join_star_out.csv", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file, .{});

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    try std.testing.expectEqualStrings(
        "id,val,fk,extra\n1,aaa,1,xxx\n2,bbb,2,yyy\n",
        output,
    );
}

test "INNER JOIN with LIMIT" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("emp.csv", .{});
        defer f.close();
        try f.writeAll("id,name,dept_id\n1,Alice,10\n2,Bob,20\n3,Carol,10\n");
    }
    {
        const f = try tmp.dir.createFile("dept.csv", .{});
        defer f.close();
        try f.writeAll("id,dept_name\n10,Engineering\n20,Marketing\n");
    }

    var emp_buf: [std.fs.max_path_bytes]u8 = undefined;
    var dept_buf: [std.fs.max_path_bytes]u8 = undefined;
    const emp_path = try tmp.dir.realpath("emp.csv", &emp_buf);
    const dept_path = try tmp.dir.realpath("dept.csv", &dept_buf);

    const sql = try std.fmt.allocPrint(
        allocator,
        "SELECT a.name FROM '{s}' a JOIN '{s}' b ON a.dept_id = b.id LIMIT 1",
        .{ emp_path, dept_path },
    );
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("join_limit_out.csv", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file, .{});

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    try std.testing.expectEqualStrings("name\nAlice\n", output);
}

test "INNER JOIN chained: three tables" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // employees: id, name, dept_id
    {
        const f = try tmp.dir.createFile("emp.csv", .{});
        defer f.close();
        try f.writeAll("id,name,dept_id\n1,Alice,10\n2,Bob,20\n3,Carol,10\n");
    }
    // departments: id, dept_name, region_id
    {
        const f = try tmp.dir.createFile("dept.csv", .{});
        defer f.close();
        try f.writeAll("id,dept_name,region_id\n10,Engineering,100\n20,Marketing,200\n");
    }
    // regions: id, region_name
    {
        const f = try tmp.dir.createFile("region.csv", .{});
        defer f.close();
        try f.writeAll("id,region_name\n100,West\n200,East\n");
    }

    var emp_buf: [std.fs.max_path_bytes]u8 = undefined;
    var dept_buf: [std.fs.max_path_bytes]u8 = undefined;
    var reg_buf: [std.fs.max_path_bytes]u8 = undefined;
    const emp_path = try tmp.dir.realpath("emp.csv", &emp_buf);
    const dept_path = try tmp.dir.realpath("dept.csv", &dept_buf);
    const reg_path = try tmp.dir.realpath("region.csv", &reg_buf);

    const sql = try std.fmt.allocPrint(
        allocator,
        "SELECT a.name, b.dept_name, c.region_name FROM '{s}' a " ++
            "JOIN '{s}' b ON a.dept_id = b.id " ++
            "JOIN '{s}' c ON b.region_id = c.id",
        .{ emp_path, dept_path, reg_path },
    );
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("join3_out.csv", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file, .{});

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    // Alice → Engineering → West
    // Bob   → Marketing   → East
    // Carol → Engineering → West
    try std.testing.expectEqualStrings(
        "name,dept_name,region_name\nAlice,Engineering,West\nBob,Marketing,East\nCarol,Engineering,West\n",
        output,
    );
}

test "INNER JOIN chained: WHERE on third table" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("emp.csv", .{});
        defer f.close();
        try f.writeAll("id,name,dept_id\n1,Alice,10\n2,Bob,20\n3,Carol,10\n");
    }
    {
        const f = try tmp.dir.createFile("dept.csv", .{});
        defer f.close();
        try f.writeAll("id,dept_name,region_id\n10,Engineering,100\n20,Marketing,200\n");
    }
    {
        const f = try tmp.dir.createFile("region.csv", .{});
        defer f.close();
        try f.writeAll("id,region_name\n100,West\n200,East\n");
    }

    var emp_buf: [std.fs.max_path_bytes]u8 = undefined;
    var dept_buf: [std.fs.max_path_bytes]u8 = undefined;
    var reg_buf: [std.fs.max_path_bytes]u8 = undefined;
    const emp_path = try tmp.dir.realpath("emp.csv", &emp_buf);
    const dept_path = try tmp.dir.realpath("dept.csv", &dept_buf);
    const reg_path = try tmp.dir.realpath("region.csv", &reg_buf);

    const sql = try std.fmt.allocPrint(
        allocator,
        "SELECT a.name FROM '{s}' a " ++
            "JOIN '{s}' b ON a.dept_id = b.id " ++
            "JOIN '{s}' c ON b.region_id = c.id WHERE c.region_name = 'West'",
        .{ emp_path, dept_path, reg_path },
    );
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();
    try std.testing.expectEqual(@as(usize, 2), query.joins.len);

    const out_file = try tmp.dir.createFile("join3_where_out.csv", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file, .{});

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(output);

    try std.testing.expectEqualStrings("name\nAlice\nCarol\n", output);
}

test "INNER JOIN: ambiguous unqualified column returns AmbiguousColumnReference" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // Both tables have a column named "id" — SELECT id (unqualified) must error.
    {
        const f = try tmp.dir.createFile("left.csv", .{});
        defer f.close();
        try f.writeAll("id,name\n1,Alice\n2,Bob\n");
    }
    {
        const f = try tmp.dir.createFile("right.csv", .{});
        defer f.close();
        try f.writeAll("id,dept\n1,Engineering\n2,Marketing\n");
    }

    var left_buf: [std.fs.max_path_bytes]u8 = undefined;
    var right_buf: [std.fs.max_path_bytes]u8 = undefined;
    const left_path = try tmp.dir.realpath("left.csv", &left_buf);
    const right_path = try tmp.dir.realpath("right.csv", &right_buf);

    const sql = try std.fmt.allocPrint(
        allocator,
        "SELECT id FROM '{s}' a JOIN '{s}' b ON a.id = b.id",
        .{ left_path, right_path },
    );
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("ambig_out.csv", .{});
    defer out_file.close();

    try std.testing.expectError(error.AmbiguousColumnReference, execute(allocator, query, out_file, .{}));
}

// Regression test: Bug 1 — SELECT b.id must return b's values, not a's.
// When both tables share a bare column name ("id"), the old first-hit getOrPut in col_map
// mapped b.id → a's column index.  alias_ranges-based registration fixes this.
test "INNER JOIN: qualified SELECT on shared column name returns correct table's values" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // a: id,link   – id = 1,2
    {
        const f = try tmp.dir.createFile("a.csv", .{});
        defer f.close();
        try f.writeAll("id,link\n1,X\n2,Y\n");
    }
    // b: link,id   – id = 10,20  (both tables have "id")
    {
        const f = try tmp.dir.createFile("b.csv", .{});
        defer f.close();
        try f.writeAll("link,id\nX,10\nY,20\n");
    }

    var a_buf: [std.fs.max_path_bytes]u8 = undefined;
    var b_buf: [std.fs.max_path_bytes]u8 = undefined;
    const a_path = try tmp.dir.realpath("a.csv", &a_buf);
    const b_path = try tmp.dir.realpath("b.csv", &b_buf);

    const sql = try std.fmt.allocPrint(
        allocator,
        "SELECT b.id FROM '{s}' a JOIN '{s}' b ON a.link = b.link",
        .{ a_path, b_path },
    );
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("bug1_out.csv", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file, .{});

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 4096);
    defer allocator.free(output);

    // Must return b's id values (10, 20), NOT a's id values (1, 2).
    try std.testing.expectEqualStrings("id\n10\n20\n", output);
}

// Regression test: Bug 2 — ON a.id = c.id must probe using a's column index, not b's.
// The old parser stripped alias prefixes (a.id → id) and the engine lmap used put()
// (overwrite), so the last-appended table's bare "id" won.  Preserving full qualified
// names in the parser and building lmap from alias_ranges fixes this.
test "INNER JOIN chained: ON clause uses correct table for join key when column names clash" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // a: id,name
    {
        const f = try tmp.dir.createFile("a.csv", .{});
        defer f.close();
        try f.writeAll("id,name\n1,Alice\n2,Bob\n");
    }
    // b: a_id,id,dept  – b.id = 100,200 (different from a.id = 1,2)
    {
        const f = try tmp.dir.createFile("b.csv", .{});
        defer f.close();
        try f.writeAll("a_id,id,dept\n1,100,Eng\n2,200,Mkt\n");
    }
    // c: id,region  – c.id = 1,2 (matches a.id, NOT b.id)
    {
        const f = try tmp.dir.createFile("c.csv", .{});
        defer f.close();
        try f.writeAll("id,region\n1,West\n2,East\n");
    }

    var a_buf: [std.fs.max_path_bytes]u8 = undefined;
    var b_buf: [std.fs.max_path_bytes]u8 = undefined;
    var c_buf: [std.fs.max_path_bytes]u8 = undefined;
    const a_path = try tmp.dir.realpath("a.csv", &a_buf);
    const b_path = try tmp.dir.realpath("b.csv", &b_buf);
    const c_path = try tmp.dir.realpath("c.csv", &c_buf);

    // Second join: ON a.id = c.id — must use a's id (1,2), not b's id (100,200).
    const sql = try std.fmt.allocPrint(
        allocator,
        "SELECT a.name, c.region FROM '{s}' a " ++
            "JOIN '{s}' b ON a.id = b.a_id " ++
            "JOIN '{s}' c ON a.id = c.id",
        .{ a_path, b_path, c_path },
    );
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("bug2_out.csv", .{ .read = true });
    defer out_file.close();

    try execute(allocator, query, out_file, .{});

    try out_file.seekTo(0);
    const output = try out_file.readToEndAlloc(allocator, 4096);
    defer allocator.free(output);

    // If Bug 2 exists: engine probes c using b.id (100,200) instead of a.id (1,2) → 0 rows.
    // Correct:  Alice → West, Bob → East.
    try std.testing.expectEqualStrings("name,region\nAlice,West\nBob,East\n", output);
}

// Regression: unknown qualified alias in SELECT must return ColumnNotFound, not silently
// fall back to bare-name lookup (e.g. SELECT z.id must not resolve as SELECT id).
test "INNER JOIN: unknown alias in SELECT returns ColumnNotFound" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("left.csv", .{});
        defer f.close();
        try f.writeAll("id,name\n1,Alice\n2,Bob\n");
    }
    {
        const f = try tmp.dir.createFile("right.csv", .{});
        defer f.close();
        try f.writeAll("id,dept\n1,Eng\n2,Mkt\n");
    }

    var lb: [std.fs.max_path_bytes]u8 = undefined;
    var rb: [std.fs.max_path_bytes]u8 = undefined;
    const lp = try tmp.dir.realpath("left.csv", &lb);
    const rp = try tmp.dir.realpath("right.csv", &rb);

    // 'z' is not a registered alias — must error, not silently return a.id rows.
    const sql = try std.fmt.allocPrint(allocator, "SELECT z.id FROM '{s}' a JOIN '{s}' b ON a.id = b.id", .{ lp, rp });
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("unk_sel_out.csv", .{});
    defer out_file.close();

    try std.testing.expectError(error.ColumnNotFound, execute(allocator, query, out_file, .{}));
}

// Regression: unknown qualified alias in WHERE must return ColumnNotFound, not silently
// filter by the bare column (e.g. WHERE z.id = 1 must not behave as WHERE id = 1).
test "INNER JOIN: unknown alias in WHERE returns ColumnNotFound" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("left.csv", .{});
        defer f.close();
        try f.writeAll("id,name\n1,Alice\n2,Bob\n");
    }
    {
        const f = try tmp.dir.createFile("right.csv", .{});
        defer f.close();
        try f.writeAll("id,dept\n1,Eng\n2,Mkt\n");
    }

    var lb: [std.fs.max_path_bytes]u8 = undefined;
    var rb: [std.fs.max_path_bytes]u8 = undefined;
    const lp = try tmp.dir.realpath("left.csv", &lb);
    const rp = try tmp.dir.realpath("right.csv", &rb);

    // 'z' is not a registered alias — must error, not filter by bare 'id'.
    const sql = try std.fmt.allocPrint(allocator, "SELECT a.name FROM '{s}' a JOIN '{s}' b ON a.id = b.id WHERE z.id = 1", .{ lp, rp });
    defer allocator.free(sql);

    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const out_file = try tmp.dir.createFile("unk_whr_out.csv", .{});
    defer out_file.close();

    try std.testing.expectError(error.ColumnNotFound, execute(allocator, query, out_file, .{}));
}
