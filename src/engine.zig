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
const scalar = @import("scalar.zig");
const arena_buffer = @import("arena_buffer.zig");
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");

/// macOS libc sysctl accessor — resolved at link time via linkLibC().
extern fn sysctlbyname(
    name: [*:0]const u8,
    oldp: ?*anyopaque,
    oldlenp: ?*usize,
    newp: ?*anyopaque,
    newlen: usize,
) c_int;

/// macOS pthread QoS — forces worker threads onto performance cores.
/// QOS_CLASS_USER_INTERACTIVE (0x21) = highest UI priority, P-cores only.
/// No-op on non-macOS (conditional compile).
extern fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: i32) c_int;

/// Returns the thread count for parallel scans.
/// Uses all logical CPUs — worker threads set QOS_CLASS_USER_INTERACTIVE to
/// preference P-cores on Apple Silicon where that improves scheduling.
fn getEffectiveThreadCount() usize {
    return std.Thread.getCpuCount() catch 1;
}
const ArenaBuffer = arena_buffer.ArenaBuffer;
const appendJsonStringToArena = arena_buffer.appendJsonStringToArena;

/// Result row for ORDER BY buffering — uses fast_sort SortKey
const SortEntry = fast_sort.SortKey;

/// Stores arena-relative offsets for an ORDER BY row during buffering.
/// Converted to real slices (SortEntry) AFTER the arena stops growing.
/// This avoids the dangling-pointer bug where ArenaBuffer.append() can
/// reallocate a.data, invalidating all previously returned slices.
const SortRowOffsets = struct {
    numeric_key: f64,
    sort_key_start: usize,
    sort_key_len: usize,
    line_start: usize,
    line_len: usize,
};

/// Append a CSV-escaped field to an ArenaBuffer.
/// Fields containing the delimiter, `"`, `\r`, or `\n` are wrapped in
/// double-quotes with any internal `"` doubled (RFC 4180).
fn appendCsvFieldToArena(arena: *ArenaBuffer, field: []const u8, delimiter: u8) !void {
    var needs_quotes = false;
    for (field) |c| {
        if (c == '"' or c == '\r' or c == '\n' or c == delimiter) {
            needs_quotes = true;
            break;
        }
    }
    if (needs_quotes) {
        _ = try arena.append("\"");
        for (field) |c| {
            if (c == '"') {
                _ = try arena.append("\"\"");
            } else {
                _ = try arena.append(&[_]u8{c});
            }
        }
        _ = try arena.append("\"");
    } else {
        _ = try arena.append(field);
    }
}

/// separate parser import — avoiding Zig 0.15 module-duplication errors.
pub const parseQuery = parser.parse;
/// Re-export Query type for the same reason.
pub const Query = parser.Query;

/// Return true when any SELECT column is a scalar function that needs per-row
/// evaluation (UPPER, LOWER, TRIM, LENGTH, SUBSTR, ABS, CEIL, FLOOR, MOD,
/// COALESCE, CAST).  Aggregate functions, STRFTIME, SUBSTR-as-GROUP-BY-key,
/// and CASE WHEN are handled by their own dedicated paths and are excluded.
fn hasScalarSelectFunctions(query: parser.Query) bool {
    if (query.all_columns) return false;
    for (query.columns) |col| {
        const sa = splitAlias(col);
        const e = std.mem.trim(u8, sa.expr, &std.ascii.whitespace);
        const open = std.mem.indexOf(u8, e, "(") orelse continue;
        const fn_raw = std.mem.trim(u8, e[0..open], &std.ascii.whitespace);
        // Skip empty (would be a syntax error anyway)
        if (fn_raw.len == 0) continue;
        // Aggregate + special functions handled elsewhere
        if (std.ascii.eqlIgnoreCase(fn_raw, "count") or
            std.ascii.eqlIgnoreCase(fn_raw, "sum") or
            std.ascii.eqlIgnoreCase(fn_raw, "avg") or
            std.ascii.eqlIgnoreCase(fn_raw, "min") or
            std.ascii.eqlIgnoreCase(fn_raw, "max") or
            std.ascii.eqlIgnoreCase(fn_raw, "count_distinct") or
            std.ascii.eqlIgnoreCase(fn_raw, "strftime") or
            std.ascii.eqlIgnoreCase(fn_raw, "round") or
            std.ascii.startsWithIgnoreCase(fn_raw, "case")) continue;
        return true;
    }
    return false;
}

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

    // Check for GROUP BY - requires sequential processing
    if (query.group_by.len > 0) {
        try executeGroupBy(allocator, query, output_file, opts);
        return;
    }

    // DISTINCT without GROUP BY and without scalar functions: equivalent to
    // GROUP BY all select columns. Route through the fast parallel GROUP BY engine
    // for O(n) hash-based dedup instead of the O(n log n) sort-based fallback.
    if (query.distinct and !query.all_columns and !hasScalarSelectFunctions(query)) {
        var synth_gb = try allocator.alloc([]u8, query.columns.len);
        defer allocator.free(synth_gb);
        for (query.columns, 0..) |col, i| {
            const sa = splitAlias(col);
            synth_gb[i] = @constCast(sa.expr);
        }
        var dq = query;
        dq.distinct = false;
        dq.group_by = synth_gb;
        try executeGroupBy(allocator, dq, output_file, opts);
        return;
    }

    // Scalar SELECT functions (UPPER, LOWER, TRIM, etc.) need per-row eval.
    // For large files on multi-core machines, use the parallel scalar executor.
    // For ORDER BY, DISTINCT, or LIMIT — fall back to sequential (handles all cases).
    if (hasScalarSelectFunctions(query)) {
        const file_s = try std.fs.cwd().openFile(query.file_path, .{});
        defer file_s.close();
        const stat_s = try file_s.stat();
        if (stat_s.size > 10 * 1024 * 1024 and
            query.order_by == null and
            !query.distinct and
            query.limit < 0)
        {
            const nc = getEffectiveThreadCount();
            if (nc > 1) {
                try executeParallelScalar(allocator, query, file_s, output_file, opts);
                return;
            }
        }
        try executeSequential(allocator, query, file_s, output_file, opts);
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
    var output_specs = std.ArrayListUnmanaged(scalar.OutputColSpec){};
    defer output_specs.deinit(allocator);

    var output_header = std.ArrayList([]const u8){};
    defer output_header.deinit(allocator);

    if (query.all_columns) {
        for (header, 0..) |col_name, idx| {
            try output_specs.append(allocator, .{ .column = idx });
            try output_header.append(allocator, col_name);
        }
    } else {
        for (query.columns) |col| {
            const sa = splitAlias(col);
            const expr = sa.expr;

            // Try scalar function first (UPPER, LOWER, TRIM, etc.)
            if (try scalar.tryParseScalar(expr, column_map, allocator)) |sc_spec| {
                try output_specs.append(allocator, .{ .scalar = sc_spec });
                try output_header.append(allocator, if (sa.alias) |a| a else expr);
                continue;
            }

            // Fall back to plain column lookup
            const lower_col = try allocator.alloc(u8, expr.len);
            defer allocator.free(lower_col);
            _ = std.ascii.lowerString(lower_col, expr);

            const idx = column_map.get(lower_col) orelse return error.ColumnNotFound;
            try output_specs.append(allocator, .{ .column = idx });
            try output_header.append(allocator, if (sa.alias) |a| a else header[idx]);
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

    // Buffer for ORDER BY support (offset-based to avoid dangling slices on arena growth)
    var sort_offsets: ?std.ArrayList(SortRowOffsets) = null;
    var arena: ?ArenaBuffer = null;
    var order_by_column_idx: ?usize = null;
    defer {
        if (sort_offsets) |*offsets| offsets.deinit(allocator);
        if (arena) |*a| a.deinit();
    }

    // If ORDER BY is specified, prepare buffer and find column index
    if (query.order_by) |order_by| {
        sort_offsets = std.ArrayList(SortRowOffsets){};
        arena = try ArenaBuffer.init(allocator, 16 * 1024 * 1024); // 16MB initial for large result sets
        // Positional ORDER BY: "ORDER BY 1" uses the 1-based column position.
        const pos_num = std.fmt.parseInt(usize, order_by.column, 10) catch 0;
        if (pos_num >= 1 and pos_num <= output_header.items.len) {
            order_by_column_idx = pos_num - 1;
        }
        if (order_by_column_idx == null) {
            // Match against output header first (supports AS aliases), then raw header.
            // order_by.column is already lowercase from parser.
            for (output_header.items, 0..) |hdr, pos| {
                const lower_hdr = try allocator.alloc(u8, hdr.len);
                defer allocator.free(lower_hdr);
                _ = std.ascii.lowerString(lower_hdr, hdr);
                if (std.mem.eql(u8, lower_hdr, order_by.column)) {
                    order_by_column_idx = pos;
                    break;
                }
            }
        }
        // Fall back to raw column name match
        if (order_by_column_idx == null) {
            for (output_specs.items, 0..) |spec, pos| {
                const raw_idx: ?usize = switch (spec) {
                    .column => |i| i,
                    .scalar => null,
                };
                if (raw_idx) |i| {
                    if (i < lower_header.len and
                        std.mem.eql(u8, lower_header[i], order_by.column))
                    {
                        order_by_column_idx = pos;
                        break;
                    }
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
    var output_row = try allocator.alloc([]const u8, output_specs.items.len);
    defer allocator.free(output_row);

    // Arena for per-row scalar function allocations (UPPER/LOWER/numeric formatting).
    // Retained capacity after each reset so warmup cost is amortised.
    var scalar_arena = std.heap.ArenaAllocator.init(allocator);
    defer scalar_arena.deinit();

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
                                .ilike => parser.matchILike(field_value, comp.value),
                                .between, .is_null, .is_not_null => parser.compareValues(comp, field_value),
                            };
                        } else {
                            // String comparison
                            matches = parser.compareValues(comp, field_value);
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
                // Complex expressions (AND/OR/NOT): evaluate directly without per-row HashMap
                if (!parser.evaluateDirect(expr, record, lower_header)) {
                    continue;
                }
            }
        }

        // Project selected columns using scalar specs (no alloc for plain columns)
        _ = scalar_arena.reset(.retain_capacity);
        for (output_specs.items, 0..) |spec, i| {
            output_row[i] = scalar.evalOutputCol(spec, record, scalar_arena.allocator());
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
        if (sort_offsets) |*offsets| {
            // Parse numeric key NOW from the source slice (stable, from record).
            // Then store offsets into the arena — NOT slices — because arena can
            // reallocate and invalidate pointers during later rows.
            const sort_val = output_row[order_by_column_idx.?];
            const numeric_key = std.fmt.parseFloat(f64, sort_val) catch std.math.nan(f64);
            const a = &(arena.?);
            const sort_key_start = a.pos;
            _ = try a.append(sort_val);
            const sort_key_len = a.pos - sort_key_start;

            // Build the line representation: CSV or JSON depending on format
            const line_start = a.pos;
            switch (opts.format) {
                .csv => {
                    for (output_row, 0..) |field, i| {
                        if (i > 0) _ = try a.append(&[_]u8{opts.delimiter});
                        try appendCsvFieldToArena(a, field, opts.delimiter);
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
            const line_len = a.pos - line_start;

            try offsets.append(allocator, .{
                .numeric_key = numeric_key,
                .sort_key_start = sort_key_start,
                .sort_key_len = sort_key_len,
                .line_start = line_start,
                .line_len = line_len,
            });
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
    if (sort_offsets) |*offsets| {
        if (query.order_by) |order_by| {
            // Arena is done growing — now safe to materialise real slices from offsets.
            const a = &(arena.?);
            var entries = std.ArrayList(SortEntry){};
            defer entries.deinit(allocator);
            for (offsets.items) |oe| {
                try entries.append(allocator, fast_sort.makeSortKey(
                    oe.numeric_key,
                    a.data[oe.sort_key_start..][0..oe.sort_key_len],
                    a.data[oe.line_start..][0..oe.line_len],
                ));
            }

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

// ─────────────────────────────────────────────────────────────────────────────
// Parallel scalar SELECT executor
//
// Handles queries like  SELECT UPPER(name), ABS(salary) FROM 'large.csv'
// by spinning up N worker threads, each scanning its mmap chunk independently,
// applying per-row scalar transforms, and writing directly into a per-thread
// byte buffer.  Main thread stitches the buffers in order — no sorting needed,
// no mutex in the hot path.
// ─────────────────────────────────────────────────────────────────────────────

const ScalarWorkerCtx = struct {
    data: []const u8,
    chunk_start: usize,
    chunk_end: usize,
    output_specs: []const scalar.OutputColSpec,
    lower_header: []const []const u8,
    where_column_idx: ?usize,
    where_expr: ?parser.Expression,
    delimiter: u8,
    output_buf: std.ArrayList(u8),
    allocator: Allocator,
    err: ?anyerror = null,
};

fn scalarWorkerThread(ctx: *ScalarWorkerCtx) void {
    scalarProcessChunk(ctx) catch |e| {
        ctx.err = e;
    };
}

fn scalarProcessChunk(ctx: *ScalarWorkerCtx) !void {
    const chunk_data = ctx.data[ctx.chunk_start..ctx.chunk_end];

    // Per-row arena: reset after each row so scalar allocations (UPPER/LOWER etc.)
    // don't accumulate.  Capacity is retained so there's no system-call overhead
    // after the first few rows.
    var row_arena = std.heap.ArenaAllocator.init(ctx.allocator);
    defer row_arena.deinit();

    var field_buf: [256][]const u8 = undefined;
    var line_start: usize = 0;

    while (line_start < chunk_data.len) {
        _ = row_arena.reset(.retain_capacity);

        const remaining = chunk_data[line_start..];
        const line_end_off = std.mem.indexOfScalar(u8, remaining, '\n') orelse
            chunk_data.len - line_start;
        var line = remaining[0..line_end_off];
        if (line.len > 0 and line[line.len - 1] == '\r') line = line[0 .. line.len - 1];
        line_start += line_end_off + 1;
        if (line.len == 0) continue;

        // Parse all fields into stack buffer (zero-alloc)
        var n_fields: usize = 0;
        var it = std.mem.splitScalar(u8, line, ctx.delimiter);
        while (it.next()) |f| {
            if (n_fields >= field_buf.len) break;
            field_buf[n_fields] = f;
            n_fields += 1;
        }
        const fields = field_buf[0..n_fields];

        // WHERE filter
        if (ctx.where_expr) |expr| {
            if (expr == .comparison) {
                const comp = expr.comparison;
                if (ctx.where_column_idx) |ci| {
                    const fv = if (ci < fields.len) fields[ci] else "";
                    if (comp.numeric_value) |threshold| {
                        const val = std.fmt.parseFloat(f64, fv) catch continue;
                        const ok = switch (comp.operator) {
                            .equal => val == threshold,
                            .not_equal => val != threshold,
                            .greater => val > threshold,
                            .greater_equal => val >= threshold,
                            .less => val < threshold,
                            .less_equal => val <= threshold,
                            .like => parser.matchLike(fv, comp.value),
                            .ilike => parser.matchILike(fv, comp.value),
                            .between, .is_null, .is_not_null => parser.compareValues(comp, fv),
                        };
                        if (!ok) continue;
                    } else {
                        if (!parser.compareValues(comp, fv)) continue;
                    }
                } else continue;
            } else {
                // AND/OR/NOT — no HashMap needed
                if (!parser.evaluateDirect(expr, fields, ctx.lower_header)) continue;
            }
        }

        // Project through scalar specs and write CSV row to per-thread buffer
        for (ctx.output_specs, 0..) |spec, j| {
            if (j > 0) try ctx.output_buf.append(ctx.allocator, ctx.delimiter);
            const value = scalar.evalOutputCol(spec, fields, row_arena.allocator());
            // Minimal CSV quoting
            var needs_quote = false;
            for (value) |c| {
                if (c == ctx.delimiter or c == '"' or c == '\r' or c == '\n') {
                    needs_quote = true;
                    break;
                }
            }
            if (needs_quote) {
                try ctx.output_buf.append(ctx.allocator, '"');
                for (value) |c| {
                    if (c == '"') try ctx.output_buf.append(ctx.allocator, '"');
                    try ctx.output_buf.append(ctx.allocator, c);
                }
                try ctx.output_buf.append(ctx.allocator, '"');
            } else {
                try ctx.output_buf.appendSlice(ctx.allocator, value);
            }
        }
        try ctx.output_buf.append(ctx.allocator, '\n');
    }
}

fn executeParallelScalar(
    allocator: Allocator,
    query: parser.Query,
    input_file: std.fs.File,
    output_file: std.fs.File,
    opts: options_mod.Options,
) !void {
    const file_size = (try input_file.stat()).size;
    if (file_size == 0) return error.EmptyFile;

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

    // Parse header
    const header_nl = std.mem.indexOfScalar(u8, data, '\n') orelse return error.NoHeader;
    var header_line = data[0..header_nl];
    if (header_line.len > 0 and header_line[header_line.len - 1] == '\r')
        header_line = header_line[0 .. header_line.len - 1];

    var header_list = std.ArrayListUnmanaged([]const u8){};
    defer header_list.deinit(allocator);
    {
        var hi = std.mem.splitScalar(u8, header_line, opts.delimiter);
        while (hi.next()) |col| try header_list.append(allocator, col);
    }
    const header = header_list.items;

    // Build lowercase column map
    var lower_header_buf = try allocator.alloc([]u8, header.len);
    defer {
        for (lower_header_buf) |h| allocator.free(h);
        allocator.free(lower_header_buf);
    }
    var column_map = std.StringHashMap(usize).init(allocator);
    defer column_map.deinit();
    for (header, 0..) |col, idx| {
        const lower = try allocator.alloc(u8, col.len);
        _ = std.ascii.lowerString(lower, col);
        lower_header_buf[idx] = lower;
        try column_map.put(lower, idx);
    }
    const lower_header: []const []const u8 = lower_header_buf;

    // Build output_specs + output header names
    var output_specs_list = std.ArrayListUnmanaged(scalar.OutputColSpec){};
    defer output_specs_list.deinit(allocator);
    var output_header = std.ArrayList([]const u8){};
    defer output_header.deinit(allocator);

    if (query.all_columns) {
        for (header, 0..) |col_name, idx| {
            try output_specs_list.append(allocator, .{ .column = idx });
            try output_header.append(allocator, col_name);
        }
    } else {
        for (query.columns) |col| {
            const sa = splitAlias(col);
            const expr = sa.expr;
            if (try scalar.tryParseScalar(expr, column_map, allocator)) |sc_spec| {
                try output_specs_list.append(allocator, .{ .scalar = sc_spec });
                try output_header.append(allocator, if (sa.alias) |a| a else expr);
                continue;
            }
            const lower_col = try allocator.alloc(u8, expr.len);
            defer allocator.free(lower_col);
            _ = std.ascii.lowerString(lower_col, expr);
            const idx = column_map.get(lower_col) orelse return error.ColumnNotFound;
            try output_specs_list.append(allocator, .{ .column = idx });
            try output_header.append(allocator, if (sa.alias) |a| a else header[idx]);
        }
    }
    const output_specs = output_specs_list.items;

    // WHERE column index
    var where_column_idx: ?usize = null;
    if (query.where_expr) |expr| {
        if (expr == .comparison) {
            for (lower_header, 0..) |lh, i| {
                if (std.mem.eql(u8, lh, expr.comparison.column)) {
                    where_column_idx = i;
                    break;
                }
            }
        }
    }

    // Write header
    var writer = csv.RecordWriter.init(output_file, opts);
    defer writer.deinit();
    try writer.writeHeader(output_header.items, opts.no_header);
    try writer.flush();

    // Split into N worker chunks aligned to line boundaries
    const num_cores = getEffectiveThreadCount();
    const n_threads = num_cores;
    const data_start = header_nl + 1;
    const data_len = data.len - data_start;
    const chunk_size = data_len / n_threads;

    var worker_ctxs = try allocator.alloc(ScalarWorkerCtx, n_threads);
    defer allocator.free(worker_ctxs);
    var threads = try allocator.alloc(std.Thread, n_threads);
    defer allocator.free(threads);

    for (0..n_threads) |i| {
        var start = data_start + i * chunk_size;
        var end = if (i == n_threads - 1) data.len else data_start + (i + 1) * chunk_size;
        // Align to line boundaries
        if (i > 0) {
            if (std.mem.indexOfScalarPos(u8, data, start, '\n')) |nl| start = nl + 1;
        }
        if (i < n_threads - 1) {
            if (std.mem.indexOfScalarPos(u8, data, end, '\n')) |nl| end = nl + 1;
        }
        worker_ctxs[i] = ScalarWorkerCtx{
            .data = data,
            .chunk_start = start,
            .chunk_end = end,
            .output_specs = output_specs,
            .lower_header = lower_header,
            .where_column_idx = where_column_idx,
            .where_expr = query.where_expr,
            .delimiter = opts.delimiter,
            .output_buf = std.ArrayList(u8){},
            .allocator = allocator,
        };
        threads[i] = try std.Thread.spawn(.{}, scalarWorkerThread, .{&worker_ctxs[i]});
    }

    for (threads) |t| t.join();

    // Propagate errors and stitch buffers
    for (worker_ctxs) |*ctx| {
        if (ctx.err) |e| return e;
    }
    for (worker_ctxs) |*ctx| {
        if (ctx.output_buf.items.len > 0)
            try output_file.writeAll(ctx.output_buf.items);
        ctx.output_buf.deinit(allocator);
    }
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
                        .ilike => parser.matchILike(fv, comp.value),
                        .between, .is_null, .is_not_null => parser.compareValues(comp, fv),
                    };
                } else parser.compareValues(comp, fv);
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
    var output_specs = std.ArrayListUnmanaged(scalar.OutputColSpec){};
    defer output_specs.deinit(allocator);

    var output_header = std.ArrayList([]const u8){};
    defer output_header.deinit(allocator);

    if (query.all_columns) {
        for (header, 0..) |col_name, idx| {
            try output_specs.append(allocator, .{ .column = idx });
            try output_header.append(allocator, col_name);
        }
    } else {
        for (query.columns) |col| {
            const sa = splitAlias(col);
            const expr = sa.expr;

            // Try scalar function first (UPPER, LOWER, TRIM, etc.)
            if (try scalar.tryParseScalar(expr, column_map, allocator)) |sc_spec| {
                try output_specs.append(allocator, .{ .scalar = sc_spec });
                try output_header.append(allocator, if (sa.alias) |a| a else expr);
                continue;
            }

            // Fall back to plain column lookup
            const lower_col = try allocator.alloc(u8, expr.len);
            defer allocator.free(lower_col);
            _ = std.ascii.lowerString(lower_col, expr);

            const idx = column_map.get(lower_col) orelse return error.ColumnNotFound;
            try output_specs.append(allocator, .{ .column = idx });
            try output_header.append(allocator, if (sa.alias) |a| a else header[idx]);
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

    // Arena for per-row scalar function allocations (UPPER/LOWER/numeric formatting).
    var scalar_stdin_arena = std.heap.ArenaAllocator.init(allocator);
    defer scalar_stdin_arena.deinit();

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
                                .ilike => parser.matchILike(field_value, comp.value),
                                .between, .is_null, .is_not_null => parser.compareValues(comp, field_value),
                            };
                        } else {
                            // String comparison
                            matches = parser.compareValues(comp, field_value);
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
                // Complex expressions (AND/OR/NOT): evaluate directly without per-row HashMap
                if (!parser.evaluateDirect(expr, record, lower_header)) {
                    continue;
                }
            }
        }

        // Project selected columns using scalar specs
        var output_row = try allocator.alloc([]const u8, output_specs.items.len);
        defer allocator.free(output_row);

        _ = scalar_stdin_arena.reset(.retain_capacity);
        for (output_specs.items, 0..) |spec, i| {
            output_row[i] = scalar.evalOutputCol(spec, record, scalar_stdin_arena.allocator());
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
    group_key: usize, // key_values slot gi (used for STRFTIME group expressions)
    aggregate: usize, // index into AggSpec list
    /// scalar function applied to a group key at output time
    /// (e.g. SELECT UPPER(city), COUNT(*) FROM x GROUP BY city)
    group_key_scalar: struct { gi: usize, spec: scalar.ScalarSpec },
};

/// Describes a STRFTIME('%Y-%m', col) GROUP BY / SELECT expression.
const StrftimeSpec = struct {
    col_idx: usize,
    fmt: []const u8, // allocator-owned format string, e.g. "%Y-%m"
    header: []const u8, // allocator-owned display name, e.g. "STRFTIME('%Y-%m', order_date)"
};

const SubstrSpec = struct {
    col_idx: usize,
    start: usize, // 1-based SQL start position
    length: ?usize, // null = to end of string
    header: []const u8, // allocator-owned display name
};

/// Describes how a single GROUP BY key is extracted from a CSV row.
const GroupSpec = union(enum) {
    column: usize, // plain CSV column index
    strftime: StrftimeSpec, // STRFTIME transform applied to a column value
    substr: SubstrSpec, // SUBSTR(col, start[, length]) extraction
};

/// Parse a SUBSTR(col, start[, length]) expression. Returns an allocator-owned SubstrSpec
/// on success, or null if `raw` is not a SUBSTR call.
fn parseSubstrRaw(allocator: Allocator, raw: []const u8, column_map: std.StringHashMap(usize)) !?SubstrSpec {
    if (raw.len < 8 or !std.ascii.startsWithIgnoreCase(raw, "SUBSTR(")) return null;
    const after = raw[7..]; // skip "SUBSTR("
    const close = std.mem.lastIndexOfScalar(u8, after, ')') orelse return error.InvalidQuery;
    const inner = std.mem.trim(u8, after[0..close], &std.ascii.whitespace);
    // Split arguments (col, start[, length]) — simple comma split safe here (no nested parens)
    var args_it = std.mem.splitScalar(u8, inner, ',');
    const col_raw_opt = args_it.next();
    const start_raw_opt = args_it.next();
    const length_raw_opt = args_it.next();
    const col_raw = std.mem.trim(u8, col_raw_opt orelse return error.InvalidQuery, &std.ascii.whitespace);
    const start_raw = std.mem.trim(u8, start_raw_opt orelse return error.InvalidQuery, &std.ascii.whitespace);
    const col_lower = try allocator.alloc(u8, col_raw.len);
    defer allocator.free(col_lower);
    _ = std.ascii.lowerString(col_lower, col_raw);
    const col_idx = column_map.get(col_lower) orelse return error.ColumnNotFound;
    const start_1based = std.fmt.parseInt(usize, start_raw, 10) catch return error.InvalidQuery;
    const length: ?usize = if (length_raw_opt) |lr| blk: {
        const lt = std.mem.trim(u8, lr, &std.ascii.whitespace);
        break :blk std.fmt.parseInt(usize, lt, 10) catch return error.InvalidQuery;
    } else null;
    return SubstrSpec{
        .col_idx = col_idx,
        .start = if (start_1based > 0) start_1based - 1 else 0, // convert to 0-based
        .length = length,
        .header = try allocator.dupe(u8, raw),
    };
}

/// Apply SUBSTR extraction to a string value.
fn applySubstr(spec: SubstrSpec, s: []const u8) []const u8 {
    if (spec.start >= s.len) return "";
    const tail = s[spec.start..];
    if (spec.length) |len| return tail[0..@min(len, tail.len)];
    return tail;
}

/// Parse a STRFTIME('fmt', col) expression.  Returns an allocator-owned StrftimeSpec
/// on success, or null if `raw` is not a STRFTIME call.  The fmt and header fields
/// must be freed by the caller via allocator.free when no longer needed.
fn parseStrftimeRaw(allocator: Allocator, raw: []const u8, column_map: std.StringHashMap(usize)) !?StrftimeSpec {
    if (raw.len < 11 or !std.ascii.startsWithIgnoreCase(raw, "STRFTIME(")) return null;
    const after = raw[9..]; // skip "STRFTIME("
    // Quoted format string: find the pair of single-quotes
    const q1 = std.mem.indexOfScalar(u8, after, '\'') orelse return error.InvalidQuery;
    const q2 = std.mem.indexOfScalarPos(u8, after, q1 + 1, '\'') orelse return error.InvalidQuery;
    const fmt = after[q1 + 1 .. q2];
    // Column name: after the comma, before the last ')'
    const comma = std.mem.indexOfScalarPos(u8, after, q2 + 1, ',') orelse return error.InvalidQuery;
    const close = std.mem.lastIndexOfScalar(u8, after, ')') orelse return error.InvalidQuery;
    const col_raw = std.mem.trim(u8, after[comma + 1 .. close], &std.ascii.whitespace);
    const col_lower = try allocator.alloc(u8, col_raw.len);
    defer allocator.free(col_lower);
    _ = std.ascii.lowerString(col_lower, col_raw);
    const col_idx = column_map.get(col_lower) orelse return error.ColumnNotFound;
    const fmt_copy = try allocator.dupe(u8, fmt);
    errdefer allocator.free(fmt_copy);
    return StrftimeSpec{
        .col_idx = col_idx,
        .fmt = fmt_copy,
        .header = try allocator.dupe(u8, raw),
    };
}

/// Apply a strftime format to an ISO-8601 datetime string (YYYY-MM-DD HH:MM:SS).
/// Writes the result into `buf` (64 bytes) and returns the filled slice.
/// Supported specifiers: %Y %m %d %H %M %S; all other characters are copied literally.
fn applyStrftime(fmt: []const u8, date_str: []const u8, buf: *[64]u8) []const u8 {
    var pos: usize = 0;
    var fi: usize = 0;
    while (fi < fmt.len and pos + 4 <= buf.len) : (fi += 1) {
        if (fmt[fi] == '%' and fi + 1 < fmt.len) {
            fi += 1;
            switch (fmt[fi]) {
                'Y' => if (date_str.len >= 4) {
                    @memcpy(buf[pos..][0..4], date_str[0..4]);
                    pos += 4;
                },
                'm' => if (date_str.len >= 7) {
                    @memcpy(buf[pos..][0..2], date_str[5..7]);
                    pos += 2;
                },
                'd' => if (date_str.len >= 10) {
                    @memcpy(buf[pos..][0..2], date_str[8..10]);
                    pos += 2;
                },
                'H' => if (date_str.len >= 13) {
                    @memcpy(buf[pos..][0..2], date_str[11..13]);
                    pos += 2;
                },
                'M' => if (date_str.len >= 16) {
                    @memcpy(buf[pos..][0..2], date_str[14..16]);
                    pos += 2;
                },
                'S' => if (date_str.len >= 19) {
                    @memcpy(buf[pos..][0..2], date_str[17..19]);
                    pos += 2;
                },
                else => {
                    buf[pos] = '%';
                    pos += 1;
                    buf[pos] = fmt[fi];
                    pos += 1;
                },
            }
        } else {
            buf[pos] = fmt[fi];
            pos += 1;
        }
    }
    return buf[0..pos];
}

/// Pre-resolved aggregate function spec — no per-row allocations in hot loop.
const AggSpec = struct {
    func_type: aggregation.AggregateType,
    col_idx: ?usize, // resolved CSV column index (null for COUNT(*))
    alias: []const u8, // output header name (allocator-owned)
    round_digits: ?u8 = null, // non-null for ROUND(expr, n)
    case_when: ?CaseWhenSpec = null, // non-null for CASE WHEN conditional aggregates
};

/// THEN/ELSE value in a CASE WHEN — either a numeric literal or a column reference.
const CaseWhenValue = union(enum) {
    constant: f64,
    col_idx: usize,

    fn resolve(self: CaseWhenValue, record: []const []const u8) f64 {
        return switch (self) {
            .constant => |v| v,
            .col_idx => |idx| if (idx < record.len) parseNumericFast(record[idx]) catch 0.0 else 0.0,
        };
    }
};

/// Represents a parsed CASE WHEN condition inside an aggregate function,
/// e.g. SUM(CASE WHEN status = 'returned' THEN 1 ELSE 0 END).
/// All string fields inside comp are allocator-owned via comp.deinit().
const CaseWhenSpec = struct {
    cond_col_idx: usize, // pre-resolved column index for the WHEN condition
    comp: parser.Comparison, // comparison operator + values (column field unused at eval time)
    then_val: CaseWhenValue, // value to accumulate when condition is true
    else_val: CaseWhenValue, // value to accumulate when condition is false
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
    distinct_sets: []?std.StringHashMap(void), // per-slot distinct-value set for COUNT(DISTINCT)

    fn init(ka: Allocator, key_vals: [][]const u8, n_aggs: usize) !CompactAccum {
        const sums = try ka.alloc(f64, n_aggs);
        const sum_counts = try ka.alloc(i64, n_aggs);
        const mins = try ka.alloc(f64, n_aggs);
        const maxs = try ka.alloc(f64, n_aggs);
        const distinct_sets = try ka.alloc(?std.StringHashMap(void), n_aggs);
        @memset(sums, 0.0);
        @memset(sum_counts, 0);
        for (mins, maxs) |*mn, *mx| {
            mn.* = std.math.inf(f64);
            mx.* = -std.math.inf(f64);
        }
        for (distinct_sets) |*ds| ds.* = null;
        return CompactAccum{
            .key_values = key_vals,
            .count = 0,
            .sums = sums,
            .sum_counts = sum_counts,
            .mins = mins,
            .maxs = maxs,
            .distinct_sets = distinct_sets,
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

/// Strip a top-level " AS alias" suffix from a SELECT column expression.
/// Scans left-to-right while skipping quoted strings and parenthesised sub-
/// expressions, so `STRFTIME('%Y-%m', col) AS month` correctly splits into
/// `STRFTIME('%Y-%m', col)` and `month`.
/// The returned slices point into the original `expr` memory.
fn splitAlias(expr: []const u8) struct { expr: []const u8, alias: ?[]const u8 } {
    var depth: usize = 0;
    var i: usize = 0;
    var last_as: ?usize = null;
    while (i < expr.len) {
        if (expr[i] == '\'') {
            i += 1;
            while (i < expr.len and expr[i] != '\'') : (i += 1) {}
            if (i < expr.len) i += 1;
            continue;
        }
        if (expr[i] == '(') {
            depth += 1;
            i += 1;
            continue;
        }
        if (expr[i] == ')') {
            if (depth > 0) depth -= 1;
            i += 1;
            continue;
        }
        // Check for " AS " (space-insensitive keyword) at nesting depth 0
        if (depth == 0 and i + 4 <= expr.len and
            std.ascii.eqlIgnoreCase(expr[i .. i + 4], " as "))
        {
            last_as = i;
        }
        i += 1;
    }
    if (last_as) |idx| {
        const alias = std.mem.trim(u8, expr[idx + 4 ..], &std.ascii.whitespace);
        // Alias must be a single bare identifier — no spaces, no parens
        if (alias.len > 0 and
            std.mem.indexOfScalar(u8, alias, ' ') == null and
            std.mem.indexOfScalar(u8, alias, '(') == null)
        {
            return .{
                .expr = std.mem.trim(u8, expr[0..idx], &std.ascii.whitespace),
                .alias = alias,
            };
        }
    }
    return .{ .expr = expr, .alias = null };
}

/// Parse a ROUND(inner_expr, digits) wrapper.  Returns the inner expression and
/// the number of decimal places, or null if the column is not a ROUND() call.
/// Parse a CASE WHEN conditional expression inside an aggregate function call.
/// Supports: FUNC(CASE WHEN col OP val THEN n ELSE m END) where FUNC is
/// SUM, AVG, MIN, MAX, or COUNT, and n/m are numeric constants.
/// Returns null if the expression does not match this pattern.
/// Returns error.ColumnNotFound if the WHEN column is not in the schema.
fn parseCaseAggCall(
    allocator: Allocator,
    expr: []const u8,
    column_map: std.StringHashMap(usize),
) !?struct { func_type: aggregation.AggregateType, case_spec: CaseWhenSpec } {
    const t = std.mem.trim(u8, expr, &std.ascii.whitespace);
    // Outer call: FUNCNAME(...)
    const open = std.mem.indexOf(u8, t, "(") orelse return null;
    if (t[t.len - 1] != ')') return null;
    const func_name = std.mem.trim(u8, t[0..open], &std.ascii.whitespace);
    const inner = std.mem.trim(u8, t[open + 1 .. t.len - 1], &std.ascii.whitespace);

    // Inner must start with "CASE WHEN "
    if (inner.len < 10) return null;
    if (!std.ascii.eqlIgnoreCase(inner[0..4], "CASE")) return null;
    const after_case = std.mem.trim(u8, inner[4..], &std.ascii.whitespace);
    if (after_case.len < 5 or !std.ascii.eqlIgnoreCase(after_case[0..5], "WHEN ")) return null;
    const cond_and_rest = after_case[5..];

    // Find " THEN " (first occurrence — conditions are simple, no nesting)
    const then_idx = std.ascii.indexOfIgnoreCase(cond_and_rest, " THEN ") orelse return null;
    const cond_str = std.mem.trim(u8, cond_and_rest[0..then_idx], &std.ascii.whitespace);
    const after_then = cond_and_rest[then_idx + 6 ..];

    // Find " ELSE "
    const else_idx = std.ascii.indexOfIgnoreCase(after_then, " ELSE ") orelse return null;
    const then_str = std.mem.trim(u8, after_then[0..else_idx], &std.ascii.whitespace);
    const after_else = after_then[else_idx + 6 ..];

    // Strip trailing "END"
    const else_str = blk: {
        const trimmed_end = std.mem.trim(u8, after_else, &std.ascii.whitespace);
        if (trimmed_end.len >= 3 and
            std.ascii.eqlIgnoreCase(trimmed_end[trimmed_end.len - 3 ..], "END"))
        {
            break :blk std.mem.trim(u8, trimmed_end[0 .. trimmed_end.len - 3], &std.ascii.whitespace);
        }
        return null;
    };

    // THEN/ELSE: numeric constant or column reference.
    const then_val: CaseWhenValue = if (std.fmt.parseFloat(f64, then_str)) |v|
        .{ .constant = v }
    else |_| blk: {
        const lower_then = try allocator.alloc(u8, then_str.len);
        defer allocator.free(lower_then);
        _ = std.ascii.lowerString(lower_then, then_str);
        const idx = column_map.get(lower_then) orelse return error.ColumnNotFound;
        break :blk .{ .col_idx = idx };
    };
    const else_val: CaseWhenValue = if (std.fmt.parseFloat(f64, else_str)) |v|
        .{ .constant = v }
    else |_| blk: {
        const lower_else = try allocator.alloc(u8, else_str.len);
        defer allocator.free(lower_else);
        _ = std.ascii.lowerString(lower_else, else_str);
        const idx = column_map.get(lower_else) orelse return error.ColumnNotFound;
        break :blk .{ .col_idx = idx };
    };

    // Resolve outer function name
    const func_lower_buf = try allocator.alloc(u8, func_name.len);
    defer allocator.free(func_lower_buf);
    _ = std.ascii.lowerString(func_lower_buf, func_name);
    const func_type: aggregation.AggregateType = blk: {
        if (std.mem.eql(u8, func_lower_buf, "sum")) break :blk .sum;
        if (std.mem.eql(u8, func_lower_buf, "avg")) break :blk .avg;
        if (std.mem.eql(u8, func_lower_buf, "min")) break :blk .min;
        if (std.mem.eql(u8, func_lower_buf, "max")) break :blk .max;
        if (std.mem.eql(u8, func_lower_buf, "count")) break :blk .count;
        return null;
    };

    // Parse the WHEN condition using the existing expression parser.
    // Only simple (single-column) comparisons are supported.
    const cond_expr = try parser.parseExpression(allocator, cond_str);
    if (cond_expr != .comparison) {
        cond_expr.deinit(allocator);
        return null;
    }
    const comp = cond_expr.comparison;
    // comp.column is already lowercase (parseExpression lowercases it)
    const cond_col_idx = column_map.get(comp.column) orelse {
        comp.deinit(allocator);
        return error.ColumnNotFound;
    };

    return .{
        .func_type = func_type,
        .case_spec = CaseWhenSpec{
            .cond_col_idx = cond_col_idx,
            .comp = comp,
            .then_val = then_val,
            .else_val = else_val,
        },
    };
}

fn parseRoundWrapper(col: []const u8) ?struct { inner: []const u8, digits: u8 } {
    const t = std.mem.trim(u8, col, &std.ascii.whitespace);
    if (t.len < 9) return null; // minimum length for ROUND(X,0)
    if (!std.ascii.eqlIgnoreCase(t[0..6], "ROUND(")) return null;
    const close = std.mem.lastIndexOfScalar(u8, t, ')') orelse return null;
    if (close != t.len - 1) return null;
    const inner_raw = t[6..close]; // content between ROUND( and )
    const last_comma = std.mem.lastIndexOfScalar(u8, inner_raw, ',') orelse return null;
    const agg_expr = std.mem.trim(u8, inner_raw[0..last_comma], &std.ascii.whitespace);
    const digits_str = std.mem.trim(u8, inner_raw[last_comma + 1 ..], &std.ascii.whitespace);
    const digits = std.fmt.parseInt(u8, digits_str, 10) catch return null;
    return .{ .inner = agg_expr, .digits = digits };
}

/// Format a float with exactly `digits` decimal places (for ROUND(expr, n)).
fn formatF64Rounded(allocator: Allocator, val: f64, digits: u8) ![]u8 {
    return switch (digits) {
        0 => std.fmt.allocPrint(allocator, "{d:.0}", .{val}),
        1 => std.fmt.allocPrint(allocator, "{d:.1}", .{val}),
        2 => std.fmt.allocPrint(allocator, "{d:.2}", .{val}),
        3 => std.fmt.allocPrint(allocator, "{d:.3}", .{val}),
        4 => std.fmt.allocPrint(allocator, "{d:.4}", .{val}),
        5 => std.fmt.allocPrint(allocator, "{d:.5}", .{val}),
        6 => std.fmt.allocPrint(allocator, "{d:.6}", .{val}),
        else => std.fmt.allocPrint(allocator, "{d:.6}", .{val}),
    };
}

/// Format a float aggregate result, applying ROUND if `round_digits` is set.
fn fmtAggrF64(allocator: Allocator, val: f64, round_digits: ?u8) ![]u8 {
    if (round_digits) |d| return formatF64Rounded(allocator, val, d);
    return formatF64(allocator, val);
}

/// Returns true if any SELECT column is an aggregate expression (no alloc).
fn isAggregateExpr(col: []const u8) bool {
    // Check for ROUND(inner, n) wrapper first
    if (parseRoundWrapper(col)) |rw| {
        return isAggregateExpr(rw.inner);
    }
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

    // ORDER BY support: buffer unique rows then sort (offset-based, avoids dangling slices)
    var sort_offsets_mmap: ?std.ArrayList(SortRowOffsets) = null;
    var sort_arena: ?ArenaBuffer = null;
    var order_by_out_pos: ?usize = null;
    defer {
        if (sort_offsets_mmap) |*e| e.deinit(allocator);
        if (sort_arena) |*a| a.deinit();
    }
    if (query.order_by) |ob| {
        sort_offsets_mmap = std.ArrayList(SortRowOffsets){};
        sort_arena = try ArenaBuffer.init(allocator, 16 * 1024 * 1024);
        // Positional ORDER BY: "ORDER BY 1" uses the 1-based column position.
        const pos_num = std.fmt.parseInt(usize, ob.column, 10) catch 0;
        if (pos_num >= 1 and pos_num <= out_header.items.len) {
            order_by_out_pos = pos_num - 1;
        }
        if (order_by_out_pos == null) {
            // Match against output header first (supports AS aliases)
            for (out_header.items, 0..) |hdr, pos| {
                const lower_hdr = try allocator.alloc(u8, hdr.len);
                defer allocator.free(lower_hdr);
                _ = std.ascii.lowerString(lower_hdr, hdr);
                if (std.mem.eql(u8, lower_hdr, ob.column)) {
                    order_by_out_pos = pos;
                    break;
                }
            }
        }
        // Fall back to raw column name match
        if (order_by_out_pos == null) {
            for (out_indices.items, 0..) |oi, pos| {
                if (oi < lower_header.len and std.mem.eql(u8, lower_header[oi], ob.column)) {
                    order_by_out_pos = pos;
                    break;
                }
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
                        .ilike => parser.matchILike(fv, comp.value),
                        .between, .is_null, .is_not_null => parser.compareValues(comp, fv),
                    };
                } else {
                    matches = parser.compareValues(comp, fv);
                }
                if (!matches) continue;
            } else {
                if (!parser.evaluateDirect(expr, record, lower_header)) continue;
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

        if (query.limit >= 0 and rows_written >= query.limit and sort_offsets_mmap == null) break;

        // Either buffer for ORDER BY or write directly
        if (sort_offsets_mmap) |*offsets| {
            const a = &(sort_arena.?);
            const sort_val = out_row[order_by_out_pos.?];
            const numeric_key = std.fmt.parseFloat(f64, sort_val) catch std.math.nan(f64);
            const sort_key_start = a.pos;
            _ = try a.append(sort_val);
            const sort_key_len = a.pos - sort_key_start;
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
            const line_len = a.pos - line_start_a;
            try offsets.append(allocator, .{
                .numeric_key = numeric_key,
                .sort_key_start = sort_key_start,
                .sort_key_len = sort_key_len,
                .line_start = line_start_a,
                .line_len = line_len,
            });
        } else {
            try writer.writeRecord(out_row);
            rows_written += 1;
        }
    }

    // Sort and emit if ORDER BY
    if (sort_offsets_mmap) |*offsets| {
        if (query.order_by) |ob| {
            // Arena is done growing — materialise real slices from offsets.
            const a = &(sort_arena.?);
            var entries = std.ArrayList(fast_sort.SortKey){};
            defer entries.deinit(allocator);
            for (offsets.items) |oe| {
                try entries.append(allocator, fast_sort.makeSortKey(
                    oe.numeric_key,
                    a.data[oe.sort_key_start..][0..oe.sort_key_len],
                    a.data[oe.line_start..][0..oe.line_len],
                ));
            }
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
        for (agg_specs.items) |spec| {
            allocator.free(spec.alias);
            if (spec.case_when) |cw| cw.comp.deinit(allocator);
        }
        agg_specs.deinit(allocator);
    }
    var out_header_list = std.ArrayListUnmanaged([]const u8){};
    defer out_header_list.deinit(allocator);

    for (query.columns) |col| {
        const sa = splitAlias(col);
        const col_base = sa.expr;
        // Detect ROUND(inner_agg, n) wrapper
        const rw = parseRoundWrapper(col_base);
        const effective_col: []const u8 = if (rw) |r| r.inner else col_base;
        const round_digits: ?u8 = if (rw) |r| r.digits else null;

        // CASE WHEN: check before parseAggregateFunc
        if (std.ascii.indexOfIgnoreCase(effective_col, "CASE") != null) cw_blk: {
            const cw_result = try parseCaseAggCall(allocator, effective_col, column_map) orelse break :cw_blk;
            const alias = if (sa.alias) |ua|
                try allocator.dupe(u8, ua)
            else
                try allocator.dupe(u8, effective_col);
            errdefer allocator.free(alias);
            var cs = cw_result.case_spec;
            errdefer cs.comp.deinit(allocator);
            const agg_idx = agg_specs.items.len;
            try col_kinds.append(allocator, .{ .aggregate = agg_idx });
            try out_header_list.append(allocator, alias);
            try agg_specs.append(allocator, AggSpec{
                .func_type = cw_result.func_type,
                .col_idx = null,
                .alias = alias,
                .round_digits = round_digits,
                .case_when = cs,
            });
            continue;
        }
        if (try aggregation.parseAggregateFunc(allocator, effective_col)) |parsed_agg| {
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
            // User alias overrides the auto-generated one; ROUND wrapper uses col_base
            if (sa.alias) |user_alias| {
                allocator.free(agg_func.alias);
                agg_func.alias = try allocator.dupe(u8, user_alias);
            } else if (rw != null) {
                allocator.free(agg_func.alias);
                agg_func.alias = try allocator.dupe(u8, col_base);
            }
            try col_kinds.append(allocator, .{ .aggregate = agg_idx });
            try out_header_list.append(allocator, agg_func.alias);
            try agg_specs.append(allocator, AggSpec{
                .func_type = agg_func.func_type,
                .col_idx = col_idx,
                .alias = agg_func.alias,
                .round_digits = round_digits,
            });
        } else {
            const lower = try allocator.alloc(u8, effective_col.len);
            defer allocator.free(lower);
            _ = std.ascii.lowerString(lower, effective_col);
            const cidx = column_map.get(lower) orelse return error.ColumnNotFound;
            try col_kinds.append(allocator, .{ .regular = cidx });
            try out_header_list.append(allocator, if (sa.alias) |a| a else header[cidx]);
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

    var field_stk: [256][]const u8 = undefined;

    // -- Scan (parallel on large files, sequential on small) --
    const num_cores_sa = getEffectiveThreadCount();
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
                .file_path = query.file_path,
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
                // Union distinct sets for COUNT(DISTINCT)
                if (partial.distinct_sets[i] != null) {
                    if (accum.distinct_sets[i] == null)
                        accum.distinct_sets[i] = std.StringHashMap(void).init(ka.allocator());
                    var partial_set = partial.distinct_sets[i].?;
                    var it = partial_set.iterator();
                    while (it.next()) |entry| {
                        const gop_e = try accum.distinct_sets[i].?.getOrPut(entry.key_ptr.*);
                        if (!gop_e.found_existing)
                            gop_e.key_ptr.* = try ka.allocator().dupe(u8, entry.key_ptr.*);
                    }
                }
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
                            .ilike => parser.matchILike(fv, comp.value),
                            .between, .is_null, .is_not_null => parser.compareValues(comp, fv),
                        };
                    } else {
                        matches = parser.compareValues(comp, fv);
                    }
                    if (!matches) continue;
                } else {
                    if (!parser.evaluateDirect(expr, record, lower_header)) continue;
                }
            }

            // Accumulate
            accum.count += 1;
            for (agg_specs.items, 0..) |spec, i| {
                switch (spec.func_type) {
                    .count => {
                        // COUNT(col): only count rows where the column is non-empty.
                        // COUNT(*) (col_idx == null) relies on accum.count above.
                        if (spec.col_idx) |cidx| {
                            if (cidx < record.len and record[cidx].len > 0) {
                                accum.sum_counts[i] += 1;
                            }
                        }
                    },
                    .count_distinct => {
                        if (spec.col_idx) |cidx| {
                            if (cidx < record.len) {
                                const ds_ptr = &accum.distinct_sets[i];
                                if (ds_ptr.* == null) ds_ptr.* = std.StringHashMap(void).init(ka.allocator());
                                const gop_e = try ds_ptr.*.?.getOrPut(record[cidx]);
                                if (!gop_e.found_existing) gop_e.key_ptr.* = try ka.allocator().dupe(u8, record[cidx]);
                            }
                        }
                    },
                    .sum, .avg => {
                        if (spec.case_when) |cw| {
                            const fv = if (cw.cond_col_idx < record.len) record[cw.cond_col_idx] else "";
                            const val = if (parser.compareValues(cw.comp, fv)) cw.then_val.resolve(record) else cw.else_val.resolve(record);
                            accum.sums[i] += val;
                            accum.sum_counts[i] += 1;
                        } else if (spec.col_idx) |cidx| {
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
            .count => if (spec.col_idx != null)
                // COUNT(col): only non-empty values were counted into sum_counts[i]
                try std.fmt.allocPrint(allocator, "{d}", .{accum.sum_counts[i]})
            else
                // COUNT(*): all rows
                try std.fmt.allocPrint(allocator, "{d}", .{accum.count}),
            .count_distinct => blk: {
                const cnt: u32 = if (accum.distinct_sets[i]) |ds| ds.count() else 0;
                break :blk try std.fmt.allocPrint(allocator, "{d}", .{cnt});
            },
            .sum => try fmtAggrF64(allocator, accum.sums[i], spec.round_digits),
            .avg => blk: {
                const cnt = accum.sum_counts[i];
                break :blk if (cnt > 0)
                    try fmtAggrF64(allocator, accum.sums[i] / @as(f64, @floatFromInt(cnt)), spec.round_digits)
                else
                    try allocator.dupe(u8, "0");
            },
            .min => blk: {
                const v = accum.mins[i];
                break :blk if (v < std.math.inf(f64))
                    try fmtAggrF64(allocator, v, spec.round_digits)
                else
                    try allocator.dupe(u8, "");
            },
            .max => blk: {
                const v = accum.maxs[i];
                break :blk if (v > -std.math.inf(f64))
                    try fmtAggrF64(allocator, v, spec.round_digits)
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
            .group_key => "",
            .group_key_scalar => "",
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
    data: []const u8, // mmap slice for this worker's chunk (zero-copy)
    lower_header: []const []const u8,
    group_specs: []const GroupSpec,
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

/// WHERE filter + GROUP BY accumulation for a single parsed line.
/// Returns early without error if the row is filtered by WHERE.
fn gbProcessRecord(
    ctx: *GbWorkerCtx,
    aa: Allocator,
    field_stk: *[256][]const u8,
    key_buf: *std.ArrayListUnmanaged(u8),
    line: []const u8,
) !void {
    const record = splitLine(line, field_stk, ctx.delimiter);

    // WHERE filter
    if (ctx.where_expr) |expr| {
        if (expr == .comparison) {
            const comp = expr.comparison;
            const cidx = ctx.where_col_idx orelse return;
            if (cidx >= record.len) return;
            const fv = record[cidx];
            if (comp.numeric_value) |threshold| {
                const val = parseNumericFast(fv) catch return;
                const matches = switch (comp.operator) {
                    .equal => val == threshold,
                    .not_equal => val != threshold,
                    .greater => val > threshold,
                    .greater_equal => val >= threshold,
                    .less => val < threshold,
                    .less_equal => val <= threshold,
                    .like => parser.matchLike(fv, comp.value),
                    .ilike => parser.matchILike(fv, comp.value),
                    .between, .is_null, .is_not_null => parser.compareValues(comp, fv),
                };
                if (!matches) return;
            } else {
                if (!parser.compareValues(comp, fv)) return;
            }
        } else {
            if (!parser.evaluateDirect(expr, record, ctx.lower_header)) return;
        }
    }

    // Build NUL-separated group key
    key_buf.clearRetainingCapacity();
    for (ctx.group_specs, 0..) |spec, i| {
        if (i > 0) try key_buf.append(aa, 0);
        var date_buf: [64]u8 = undefined;
        const val: []const u8 = switch (spec) {
            .column => |cidx| if (cidx < record.len) record[cidx] else "",
            .strftime => |sf| if (sf.col_idx < record.len)
                applyStrftime(sf.fmt, record[sf.col_idx], &date_buf)
            else
                "",
            .substr => |ss| if (ss.col_idx < record.len)
                applySubstr(ss, record[ss.col_idx])
            else
                "",
        };
        try key_buf.appendSlice(aa, val);
    }

    const gop = try ctx.partial_map.getOrPut(key_buf.items);
    if (!gop.found_existing) {
        gop.key_ptr.* = try aa.dupe(u8, key_buf.items);
        var key_vals = try aa.alloc([]const u8, ctx.group_specs.len);
        for (ctx.group_specs, 0..) |spec, gi| {
            var date_buf_kv: [64]u8 = undefined;
            const kv: []const u8 = switch (spec) {
                .column => |cidx| if (cidx < record.len) record[cidx] else "",
                .strftime => |sf| if (sf.col_idx < record.len)
                    applyStrftime(sf.fmt, record[sf.col_idx], &date_buf_kv)
                else
                    "",
                .substr => |ss| if (ss.col_idx < record.len)
                    applySubstr(ss, record[ss.col_idx])
                else
                    "",
            };
            key_vals[gi] = try aa.dupe(u8, kv);
        }
        gop.value_ptr.* = try CompactAccum.init(aa, key_vals, ctx.n_aggs);
    }
    const accum = gop.value_ptr;

    accum.count += 1;
    for (ctx.agg_specs, 0..) |spec, i| {
        switch (spec.func_type) {
            .count => {},
            .count_distinct => {
                if (spec.col_idx) |cidx| {
                    if (cidx < record.len) {
                        const ds_ptr = &accum.distinct_sets[i];
                        if (ds_ptr.* == null) ds_ptr.* = std.StringHashMap(void).init(aa);
                        const gop_e = try ds_ptr.*.?.getOrPut(record[cidx]);
                        if (!gop_e.found_existing) gop_e.key_ptr.* = try aa.dupe(u8, record[cidx]);
                    }
                }
            },
            .sum, .avg => {
                if (spec.case_when) |cw| {
                    const fv = if (cw.cond_col_idx < record.len) record[cw.cond_col_idx] else "";
                    const val = if (parser.compareValues(cw.comp, fv)) cw.then_val.resolve(record) else cw.else_val.resolve(record);
                    accum.sums[i] += val;
                    accum.sum_counts[i] += 1;
                } else if (spec.col_idx) |cidx| {
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

fn gbWorkerThread(ctx: *GbWorkerCtx) void {
    gbWorkerScan(ctx) catch |e| {
        ctx.err = e;
    };
}

fn gbWorkerScan(ctx: *GbWorkerCtx) !void {
    const aa = ctx.arena.allocator();
    ctx.partial_map = std.StringHashMap(CompactAccum).init(aa);
    // Per-thread map pre-sizing: each worker handles 1/N of the file
    // Use same adaptive sizing as main thread since chunk size correlates with total file size
    const chunk_size = ctx.data.len;
    const worker_capacity: u32 = if (chunk_size < 10 * 1024 * 1024)
        128
    else if (chunk_size < 100 * 1024 * 1024)
        512
    else
        2048;
    try ctx.partial_map.ensureTotalCapacity(worker_capacity);

    // Zero-copy scan: iterate directly over the mmap'd data slice.
    // The full file is already mmap'd by the calling thread; each worker
    // just reads a pointer range — no pread syscalls, no data copies,
    // no IO buffers, no seam handling.  After the warmup run the pages
    // are in the page cache so access is at memory speed (~100 GB/s).
    var field_stk: [256][]const u8 = undefined;
    var key_buf = std.ArrayListUnmanaged(u8){};

    var pos: usize = 0;
    const data = ctx.data;
    while (pos < data.len) {
        const nl = std.mem.indexOfScalarPos(u8, data, pos, '\n') orelse {
            // Last line with no trailing newline
            var line: []const u8 = data[pos..];
            if (line.len > 0 and line[line.len - 1] == '\r') line = line[0 .. line.len - 1];
            if (line.len > 0) try gbProcessRecord(ctx, aa, &field_stk, &key_buf, line);
            break;
        };
        var line: []const u8 = data[pos..nl];
        if (line.len > 0 and line[line.len - 1] == '\r') line = line[0 .. line.len - 1];
        pos = nl + 1;
        if (line.len == 0) continue;
        try gbProcessRecord(ctx, aa, &field_stk, &key_buf, line);
    }
}

/// Per-thread context for a parallel scalar aggregate chunk scan.
const ScalarAggWorkerCtx = struct {
    file_path: []const u8,
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

    const file = try std.fs.cwd().openFile(ctx.file_path, .{});
    defer file.close();

    const IO_BUF: usize = 2 * 1024 * 1024;
    const io_buf = try aa.alloc(u8, IO_BUF);
    var seam_buf = std.ArrayListUnmanaged(u8){};
    var combined_buf = std.ArrayListUnmanaged(u8){};

    var field_stk: [256][]const u8 = undefined;

    var file_pos: usize = ctx.chunk_start;
    while (file_pos < ctx.chunk_end) {
        const to_read = @min(IO_BUF, ctx.chunk_end - file_pos);
        const n = try std.posix.pread(file.handle, io_buf[0..to_read], @intCast(file_pos));
        if (n == 0) break;
        file_pos += n;

        var scan: usize = 0;
        while (scan < n) {
            const nl = std.mem.indexOfScalarPos(u8, io_buf, scan, '\n') orelse {
                try seam_buf.appendSlice(aa, io_buf[scan..n]);
                break;
            };
            var line: []const u8 = undefined;
            if (seam_buf.items.len > 0) {
                combined_buf.clearRetainingCapacity();
                try combined_buf.appendSlice(aa, seam_buf.items);
                try combined_buf.appendSlice(aa, io_buf[scan..nl]);
                seam_buf.clearRetainingCapacity();
                line = combined_buf.items;
            } else {
                line = io_buf[scan..nl];
            }
            if (line.len > 0 and line[line.len - 1] == '\r') line = line[0 .. line.len - 1];
            scan = nl + 1;
            if (line.len == 0) continue;

            const record = splitLine(line, &field_stk, ctx.delimiter);
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
                            .ilike => parser.matchILike(fv, comp.value),
                            .between, .is_null, .is_not_null => parser.compareValues(comp, fv),
                        };
                    } else {
                        matches = parser.compareValues(comp, fv);
                    }
                    if (!matches) continue;
                } else {
                    if (!parser.evaluateDirect(expr, record, ctx.lower_header)) continue;
                }
            }
            ctx.partial_accum.count += 1;
            for (ctx.agg_specs, 0..) |spec, i| {
                switch (spec.func_type) {
                    .count => {},
                    .count_distinct => {
                        if (spec.col_idx) |cidx| {
                            if (cidx < record.len) {
                                const ds_ptr = &ctx.partial_accum.distinct_sets[i];
                                if (ds_ptr.* == null) ds_ptr.* = std.StringHashMap(void).init(aa);
                                const gop_e = try ds_ptr.*.?.getOrPut(record[cidx]);
                                if (!gop_e.found_existing) gop_e.key_ptr.* = try aa.dupe(u8, record[cidx]);
                            }
                        }
                    },
                    .sum, .avg => {
                        if (spec.case_when) |cw| {
                            const fv = if (cw.cond_col_idx < record.len) record[cw.cond_col_idx] else "";
                            const val = if (parser.compareValues(cw.comp, fv)) cw.then_val.resolve(record) else cw.else_val.resolve(record);
                            ctx.partial_accum.sums[i] += val;
                            ctx.partial_accum.sum_counts[i] += 1;
                        } else if (spec.col_idx) |cidx| {
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
    // Flush remaining seam bytes (last line without trailing newline).
    if (seam_buf.items.len > 0) {
        var line: []const u8 = seam_buf.items;
        if (line.len > 0 and line[line.len - 1] == '\r') line = line[0 .. line.len - 1];
        if (line.len > 0) row: {
            const record = splitLine(line, &field_stk, ctx.delimiter);
            if (ctx.where_expr) |expr| {
                if (expr == .comparison) {
                    const comp = expr.comparison;
                    const cidx = ctx.where_col_idx orelse break :row;
                    if (cidx >= record.len) break :row;
                    const fv = record[cidx];
                    if (comp.numeric_value) |threshold| {
                        const val = parseNumericFast(fv) catch break :row;
                        const matches = switch (comp.operator) {
                            .equal => val == threshold,
                            .not_equal => val != threshold,
                            .greater => val > threshold,
                            .greater_equal => val >= threshold,
                            .less => val < threshold,
                            .less_equal => val <= threshold,
                            .like => parser.matchLike(fv, comp.value),
                            .ilike => parser.matchILike(fv, comp.value),
                            .between, .is_null, .is_not_null => parser.compareValues(comp, fv),
                        };
                        if (!matches) break :row;
                    } else {
                        if (!parser.compareValues(comp, fv)) break :row;
                    }
                } else {
                    if (!parser.evaluateDirect(expr, record, ctx.lower_header)) break :row;
                }
            }
            ctx.partial_accum.count += 1;
            for (ctx.agg_specs, 0..) |spec, i| {
                switch (spec.func_type) {
                    .count => {},
                    .count_distinct => {
                        if (spec.col_idx) |cidx| {
                            if (cidx < record.len) {
                                const ds_ptr = &ctx.partial_accum.distinct_sets[i];
                                if (ds_ptr.* == null) ds_ptr.* = std.StringHashMap(void).init(aa);
                                const gop_e = try ds_ptr.*.?.getOrPut(record[cidx]);
                                if (!gop_e.found_existing) gop_e.key_ptr.* = try aa.dupe(u8, record[cidx]);
                            }
                        }
                    },
                    .sum, .avg => {
                        if (spec.case_when) |cw| {
                            const fv = if (cw.cond_col_idx < record.len) record[cw.cond_col_idx] else "";
                            const val = if (parser.compareValues(cw.comp, fv)) cw.then_val.resolve(record) else cw.else_val.resolve(record);
                            ctx.partial_accum.sums[i] += val;
                            ctx.partial_accum.sum_counts[i] += 1;
                        } else if (spec.col_idx) |cidx| {
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
    var group_specs = try allocator.alloc(GroupSpec, query.group_by.len);
    var n_specs_init: usize = 0;
    defer {
        for (group_specs[0..n_specs_init]) |spec| {
            switch (spec) {
                .column => {},
                .strftime => |sf| {
                    allocator.free(sf.fmt);
                    allocator.free(sf.header);
                },
                .substr => |ss| allocator.free(ss.header),
            }
        }
        allocator.free(group_specs);
    }
    for (query.group_by, 0..) |col, i| {
        if (try parseStrftimeRaw(allocator, col, column_map)) |sf| {
            group_specs[i] = .{ .strftime = sf };
        } else if (try parseSubstrRaw(allocator, col, column_map)) |ss| {
            group_specs[i] = .{ .substr = ss };
        } else {
            const lower = try allocator.alloc(u8, col.len);
            defer allocator.free(lower);
            _ = std.ascii.lowerString(lower, col);
            if (column_map.get(lower)) |cidx| {
                group_specs[i] = .{ .column = cidx };
            } else {
                // Try to resolve as a SELECT alias (e.g. GROUP BY month where SELECT STRFTIME(...) AS month)
                const resolved: ?[]const u8 = blk: {
                    if (!query.all_columns) {
                        for (query.columns) |qcol| {
                            const sa = splitAlias(qcol);
                            if (sa.alias) |alias| {
                                const lower_alias = try allocator.alloc(u8, alias.len);
                                defer allocator.free(lower_alias);
                                _ = std.ascii.lowerString(lower_alias, alias);
                                if (std.mem.eql(u8, lower_alias, lower)) {
                                    break :blk sa.expr;
                                }
                            }
                        }
                    }
                    break :blk null;
                };
                if (resolved) |expr| {
                    if (try parseStrftimeRaw(allocator, expr, column_map)) |sf| {
                        group_specs[i] = .{ .strftime = sf };
                    } else if (try parseSubstrRaw(allocator, expr, column_map)) |ss| {
                        group_specs[i] = .{ .substr = ss };
                    } else {
                        const lower2 = try allocator.alloc(u8, expr.len);
                        defer allocator.free(lower2);
                        _ = std.ascii.lowerString(lower2, expr);
                        group_specs[i] = .{ .column = column_map.get(lower2) orelse return error.ColumnNotFound };
                    }
                } else {
                    return error.ColumnNotFound;
                }
            }
        }
        n_specs_init += 1;
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
        for (group_specs, 0..) |spec, gi| {
            switch (spec) {
                .column => |cidx| {
                    try col_kinds.append(allocator, .{ .regular = cidx });
                    try out_header_list.append(allocator, header[cidx]);
                },
                .strftime => |sf| {
                    try col_kinds.append(allocator, .{ .group_key = gi });
                    try out_header_list.append(allocator, sf.header);
                },
                .substr => |ss| {
                    try col_kinds.append(allocator, .{ .group_key = gi });
                    try out_header_list.append(allocator, ss.header);
                },
            }
        }
    } else {
        for (query.columns) |col| {
            const sa = splitAlias(col);
            const col_base = sa.expr;
            // Detect ROUND(inner_agg, n) wrapper
            const rw = parseRoundWrapper(col_base);
            const effective_col: []const u8 = if (rw) |r| r.inner else col_base;
            const round_digits: ?u8 = if (rw) |r| r.digits else null;

            // CASE WHEN: check before parseAggregateFunc
            if (std.ascii.indexOfIgnoreCase(effective_col, "CASE") != null) cw_blk: {
                const cw_result = try parseCaseAggCall(allocator, effective_col, column_map) orelse break :cw_blk;
                const alias = if (sa.alias) |ua|
                    try allocator.dupe(u8, ua)
                else
                    try allocator.dupe(u8, effective_col);
                errdefer allocator.free(alias);
                var cs = cw_result.case_spec;
                errdefer cs.comp.deinit(allocator);
                const agg_idx = agg_specs.items.len;
                try col_kinds.append(allocator, .{ .aggregate = agg_idx });
                try out_header_list.append(allocator, alias);
                try agg_specs.append(allocator, AggSpec{
                    .func_type = cw_result.func_type,
                    .col_idx = null,
                    .alias = alias,
                    .round_digits = round_digits,
                    .case_when = cs,
                });
                continue;
            }
            if (try aggregation.parseAggregateFunc(allocator, effective_col)) |parsed_agg| {
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
                // User alias overrides the auto-generated one; ROUND wrapper uses col_base
                if (sa.alias) |user_alias| {
                    allocator.free(agg_func.alias);
                    agg_func.alias = try allocator.dupe(u8, user_alias);
                } else if (rw != null) {
                    allocator.free(agg_func.alias);
                    agg_func.alias = try allocator.dupe(u8, col_base);
                }
                try col_kinds.append(allocator, .{ .aggregate = agg_idx });
                try out_header_list.append(allocator, agg_func.alias);
                // alias ownership transfers to AggSpec
                try agg_specs.append(allocator, AggSpec{
                    .func_type = agg_func.func_type,
                    .col_idx = col_idx,
                    .alias = agg_func.alias,
                    .round_digits = round_digits,
                });
            } else {
                const lower = try allocator.alloc(u8, effective_col.len);
                defer allocator.free(lower);
                _ = std.ascii.lowerString(lower, effective_col);
                if (column_map.get(lower)) |cidx| {
                    try col_kinds.append(allocator, .{ .regular = cidx });
                    try out_header_list.append(allocator, if (sa.alias) |a| a else header[cidx]);
                } else {
                    // Maybe a STRFTIME/SUBSTR expression that also appears in GROUP BY
                    var gi_found: ?usize = null;
                    var fn_hdr: []const u8 = "";
                    for (group_specs, 0..) |spec, gi| {
                        switch (spec) {
                            .strftime => |sf| if (std.ascii.eqlIgnoreCase(col_base, sf.header)) {
                                gi_found = gi;
                                fn_hdr = sf.header;
                                break;
                            },
                            .substr => |ss| if (std.ascii.eqlIgnoreCase(col_base, ss.header)) {
                                gi_found = gi;
                                fn_hdr = ss.header;
                                break;
                            },
                            else => {},
                        }
                    }
                    if (gi_found) |gi| {
                        try col_kinds.append(allocator, .{ .group_key = gi });
                        try out_header_list.append(allocator, if (sa.alias) |a| a else fn_hdr);
                    } else {
                        // Try scalar function applied to a plain GROUP BY column
                        // e.g. SELECT UPPER(city), COUNT(*) FROM x GROUP BY city
                        if (try scalar.tryParseScalar(effective_col, column_map, allocator)) |sc_spec| {
                            // Find which group_spec slot contains this column
                            const sc_col_idx = sc_spec.colIdx();
                            var sc_gi: ?usize = null;
                            for (group_specs, 0..) |spec, gi| {
                                if (spec == .column and spec.column == sc_col_idx) {
                                    sc_gi = gi;
                                    break;
                                }
                            }
                            if (sc_gi) |gi| {
                                try col_kinds.append(allocator, .{ .group_key_scalar = .{ .gi = gi, .spec = sc_spec } });
                                try out_header_list.append(allocator, if (sa.alias) |a| a else effective_col);
                            } else {
                                return error.ColumnNotFound;
                            }
                        } else {
                            return error.ColumnNotFound;
                        }
                    }
                }
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
    // Adaptive pre-sizing: larger files → more groups → fewer rehashes
    const initial_capacity: u32 = if (file_size < 10 * 1024 * 1024)
        128
    else if (file_size < 100 * 1024 * 1024)
        512
    else if (file_size < 1024 * 1024 * 1024)
        2048
    else
        8192; // 200GB files: starts at 8K, grows as needed
    try group_map.ensureTotalCapacity(initial_capacity);

    const n_aggs = agg_specs.items.len;

    // Reusable key builder (grows once, then reused without allocation)
    var key_buf = std.ArrayListUnmanaged(u8){};
    defer key_buf.deinit(allocator);

    // Stack field buffer: zero heap allocation for field splitting per row
    var field_stk: [256][]const u8 = undefined;

    // -- Main scan loop -------------------------------------------------------
    // For large files on multi-core machines: parallel map-reduce.
    // Each thread independently scans its chunk into its own partial_map;
    // main thread merges arithmetic results after join (O(num_groups), negligible).
    // Single-threaded fallback for small files or single-core environments.
    const num_cores = getEffectiveThreadCount();
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
                .data = data[chunks[i][0]..chunks[i][1]],
                .lower_header = lower_header,
                .group_specs = group_specs,
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
                    const new_ds = try ka.alloc(?std.StringHashMap(void), n_aggs);
                    for (new_ds) |*d| d.* = null;
                    for (new_ds, 0..) |*nd, i| {
                        if (partial.distinct_sets[i] != null) {
                            var new_set = std.StringHashMap(void).init(ka);
                            var partial_set = partial.distinct_sets[i].?;
                            var ds_it = partial_set.iterator();
                            while (ds_it.next()) |e| {
                                try new_set.put(try ka.dupe(u8, e.key_ptr.*), {});
                            }
                            nd.* = new_set;
                        }
                    }
                    gop.value_ptr.* = CompactAccum{
                        .key_values = key_vals,
                        .count = partial.count,
                        .sums = try ka.dupe(f64, partial.sums),
                        .sum_counts = try ka.dupe(i64, partial.sum_counts),
                        .mins = try ka.dupe(f64, partial.mins),
                        .maxs = try ka.dupe(f64, partial.maxs),
                        .distinct_sets = new_ds,
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
                        // Union distinct sets for COUNT(DISTINCT)
                        if (partial.distinct_sets[i] != null) {
                            if (accum.distinct_sets[i] == null)
                                accum.distinct_sets[i] = std.StringHashMap(void).init(ka);
                            var partial_set = partial.distinct_sets[i].?;
                            var ds_it = partial_set.iterator();
                            while (ds_it.next()) |e| {
                                const gop_e = try accum.distinct_sets[i].?.getOrPut(e.key_ptr.*);
                                if (!gop_e.found_existing)
                                    gop_e.key_ptr.* = try ka.dupe(u8, e.key_ptr.*);
                            }
                        }
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
                            .ilike => parser.matchILike(fv, comp.value),
                            .between, .is_null, .is_not_null => parser.compareValues(comp, fv),
                        };
                    } else {
                        matches = parser.compareValues(comp, fv);
                    }
                    if (!matches) continue;
                } else {
                    if (!parser.evaluateDirect(expr, record, lower_header)) continue;
                }
            }

            // Build NUL-separated group key (no alloc after first row warmup)
            key_buf.clearRetainingCapacity();
            for (group_specs, 0..) |spec, i| {
                if (i > 0) try key_buf.append(allocator, 0);
                var date_buf: [64]u8 = undefined;
                const val: []const u8 = switch (spec) {
                    .column => |cidx| if (cidx < record.len) record[cidx] else "",
                    .strftime => |sf| if (sf.col_idx < record.len)
                        applyStrftime(sf.fmt, record[sf.col_idx], &date_buf)
                    else
                        "",
                    .substr => |ss| if (ss.col_idx < record.len)
                        applySubstr(ss, record[ss.col_idx])
                    else
                        "",
                };
                try key_buf.appendSlice(allocator, val);
            }

            // Look up or create group
            const gop = try group_map.getOrPut(key_buf.items);
            if (!gop.found_existing) {
                const stored_key = try ka.dupe(u8, key_buf.items);
                gop.key_ptr.* = stored_key;
                var key_vals = try ka.alloc([]const u8, group_specs.len);
                for (group_specs, 0..) |spec, gi| {
                    var date_buf_kv: [64]u8 = undefined;
                    const kv: []const u8 = switch (spec) {
                        .column => |cidx| if (cidx < record.len) record[cidx] else "",
                        .strftime => |sf| if (sf.col_idx < record.len)
                            applyStrftime(sf.fmt, record[sf.col_idx], &date_buf_kv)
                        else
                            "",
                        .substr => |ss| if (ss.col_idx < record.len)
                            applySubstr(ss, record[ss.col_idx])
                        else
                            "",
                    };
                    key_vals[gi] = try ka.dupe(u8, kv);
                }
                gop.value_ptr.* = try CompactAccum.init(ka, key_vals, n_aggs);
            }
            const accum = gop.value_ptr;

            // Accumulate: direct array r/w, no HashMap operations per row ------
            accum.count += 1;
            for (agg_specs.items, 0..) |spec, i| {
                switch (spec.func_type) {
                    .count => {
                        // COUNT(col): only count rows where the column is non-empty.
                        // COUNT(*) (col_idx == null) relies on accum.count above.
                        if (spec.col_idx) |cidx| {
                            if (cidx < record.len and record[cidx].len > 0) {
                                accum.sum_counts[i] += 1;
                            }
                        }
                    },
                    .count_distinct => {
                        if (spec.col_idx) |cidx| {
                            if (cidx < record.len) {
                                const ds_ptr = &accum.distinct_sets[i];
                                if (ds_ptr.* == null) ds_ptr.* = std.StringHashMap(void).init(ka);
                                const gop_e = try ds_ptr.*.?.getOrPut(record[cidx]);
                                if (!gop_e.found_existing) gop_e.key_ptr.* = try ka.dupe(u8, record[cidx]);
                            }
                        }
                    },
                    .sum, .avg => {
                        if (spec.case_when) |cw| {
                            const fv = if (cw.cond_col_idx < record.len) record[cw.cond_col_idx] else "";
                            const val = if (parser.compareValues(cw.comp, fv)) cw.then_val.resolve(record) else cw.else_val.resolve(record);
                            accum.sums[i] += val;
                            accum.sum_counts[i] += 1;
                        } else if (spec.col_idx) |cidx| {
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

    // Arena for scalar function eval per output row (UPPER/LOWER/etc.)
    var gb_scalar_arena = std.heap.ArenaAllocator.init(allocator);
    defer gb_scalar_arena.deinit();

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
                .count => if (spec.col_idx != null)
                    // COUNT(col): only non-empty values were counted into sum_counts[i]
                    try std.fmt.allocPrint(allocator, "{d}", .{accum.sum_counts[i]})
                else
                    // COUNT(*): all rows
                    try std.fmt.allocPrint(allocator, "{d}", .{accum.count}),
                .count_distinct => blk: {
                    const cnt: u32 = if (accum.distinct_sets[i]) |ds| ds.count() else 0;
                    break :blk try std.fmt.allocPrint(allocator, "{d}", .{cnt});
                },
                .sum => try fmtAggrF64(allocator, accum.sums[i], spec.round_digits),
                .avg => blk: {
                    const cnt = accum.sum_counts[i];
                    break :blk if (cnt > 0)
                        try fmtAggrF64(allocator, accum.sums[i] / @as(f64, @floatFromInt(cnt)), spec.round_digits)
                    else
                        try allocator.dupe(u8, "0");
                },
                .min => blk: {
                    const v = accum.mins[i];
                    break :blk if (v < std.math.inf(f64))
                        try fmtAggrF64(allocator, v, spec.round_digits)
                    else
                        try allocator.dupe(u8, "");
                },
                .max => blk: {
                    const v = accum.maxs[i];
                    break :blk if (v > -std.math.inf(f64))
                        try fmtAggrF64(allocator, v, spec.round_digits)
                    else
                        try allocator.dupe(u8, "");
                },
            };
            try agg_allocs.append(allocator, s);
            agg_results[i] = s;
        }

        _ = gb_scalar_arena.reset(.retain_capacity);
        for (col_kinds.items, 0..) |kind, i| {
            output_row[i] = switch (kind) {
                .regular => |cidx| blk: {
                    for (group_specs, 0..) |spec, gi| {
                        switch (spec) {
                            .column => |gcidx| if (gcidx == cidx) break :blk accum.key_values[gi],
                            .strftime, .substr => {},
                        }
                    }
                    break :blk "";
                },
                .group_key => |gi| accum.key_values[gi],
                .aggregate => |agg_idx| agg_results[agg_idx],
                .group_key_scalar => |gks| blk: {
                    // Apply scalar function to the group key value at output time
                    const key_val: []const u8 = if (gks.gi < accum.key_values.len) accum.key_values[gks.gi] else "";
                    // Wrap into a single-element slice for scalar.eval
                    const rec: [1][]const u8 = .{key_val};
                    // Build a spec with col_idx=0 pointing into our single-element record
                    var adj_spec = gks.spec;
                    switch (adj_spec) {
                        .upper => |*ci| ci.* = 0,
                        .lower => |*ci| ci.* = 0,
                        .trim => |*ci| ci.* = 0,
                        .length => |*ci| ci.* = 0,
                        .abs => |*ci| ci.* = 0,
                        .ceil => |*ci| ci.* = 0,
                        .floor => |*ci| ci.* = 0,
                        .cast_int => |*ci| ci.* = 0,
                        .cast_float => |*ci| ci.* = 0,
                        .cast_text => |*ci| ci.* = 0,
                        .substr => |*a| a.col_idx = 0,
                        .mod_op => |*a| a.col_idx = 0,
                        .coalesce => |*a| {
                            for (a.colsMut()) |*ci| ci.* = 0;
                        },
                        .datediff => |*a| {
                            a.start_col = 0;
                            a.end_col = 0;
                        },
                        .dateadd => |*a| a.date_col = 0,
                        .extract => |*a| a.date_col = 0,
                        .round_op => |*a| a.col_idx = 0,
                    }
                    break :blk scalar.eval(adj_spec, &rec, gb_scalar_arena.allocator());
                },
            };
        }

        // HAVING filter — evaluated against the assembled output row headers
        if (query.having_expr) |hav_expr| {
            var hav_arena = std.heap.ArenaAllocator.init(allocator);
            defer hav_arena.deinit();
            const ha = hav_arena.allocator();
            var having_map = std.StringHashMap([]const u8).init(ha);
            // Map alias names (from out_header_list)
            for (out_header_list.items, 0..) |hdr, hi| {
                const lower_hdr = try ha.alloc(u8, hdr.len);
                _ = std.ascii.lowerString(lower_hdr, hdr);
                try having_map.put(lower_hdr, output_row[hi]);
            }
            // Also map original column expressions (pre-alias) so that
            // HAVING COUNT(*) > n works even when the column is aliased AS n.
            for (query.columns, 0..) |col, hi| {
                if (hi >= output_row.len) break;
                const sa = splitAlias(col);
                const expr_lower = try ha.alloc(u8, sa.expr.len);
                _ = std.ascii.lowerString(expr_lower, sa.expr);
                if (!having_map.contains(expr_lower)) {
                    try having_map.put(expr_lower, output_row[hi]);
                }
            }
            if (!parser.evaluate(hav_expr, having_map)) continue;
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

// ── Scalar SELECT function tests ──────────────────────────────────────────────

test "UPPER: transforms column to uppercase" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("sc.csv", .{});
        defer f.close();
        try f.writeAll("name\nAlice\nbob\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("sc.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT UPPER(name) FROM '{s}'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "ALICE"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "BOB"));
}

test "LOWER: transforms column to lowercase" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("sc.csv", .{});
        defer f.close();
        try f.writeAll("name\nALICE\nBOB\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("sc.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT LOWER(name) FROM '{s}'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "alice"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "bob"));
}

test "TRIM: strips leading and trailing whitespace" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("sc.csv", .{});
        defer f.close();
        try f.writeAll("name\n  hello  \n  world\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("sc.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT TRIM(name) FROM '{s}'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "hello"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "world"));
    // whitespace must not surround the values in the output
    try std.testing.expect(!std.mem.containsAtLeast(u8, data, 1, "  hello  "));
}

test "LENGTH: returns string length as integer" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("sc.csv", .{});
        defer f.close();
        try f.writeAll("name\nAlice\nBo\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("sc.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT LENGTH(name) FROM '{s}'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "5")); // len("Alice")
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "2")); // len("Bo")
}

test "SUBSTR: extracts substring (1-based start index)" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("sc.csv", .{});
        defer f.close();
        try f.writeAll("name\nAlice\nBobby\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("sc.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT SUBSTR(name,1,3) FROM '{s}'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "Ali"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "Bob"));
}

test "ABS: returns absolute value of negative numbers" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("sc.csv", .{});
        defer f.close();
        try f.writeAll("n\n-42\n7\n-5\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("sc.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT ABS(n) FROM '{s}'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "42"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "7"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "5"));
    // negative sign must be gone
    try std.testing.expect(!std.mem.containsAtLeast(u8, data, 1, "-42"));
}

test "CEIL: rounds up and emits .0 suffix" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("sc.csv", .{});
        defer f.close();
        try f.writeAll("v\n42.3\n7.0\n-1.7\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("sc.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT CEIL(v) FROM '{s}'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "43.0"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "7.0"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "-1.0"));
}

test "FLOOR: rounds down and emits .0 suffix" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("sc.csv", .{});
        defer f.close();
        try f.writeAll("v\n42.9\n7.0\n-1.2\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("sc.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT FLOOR(v) FROM '{s}'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "42.0"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "7.0"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "-2.0"));
}

test "MOD: returns remainder" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("sc.csv", .{});
        defer f.close();
        try f.writeAll("n\n10\n9\n6\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("sc.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT MOD(n,3) FROM '{s}'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "1")); // 10 % 3 = 1
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "0")); // 9 % 3 = 0 and 6 % 3 = 0
}

test "COALESCE: returns fallback for empty field" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("sc.csv", .{});
        defer f.close();
        try f.writeAll("name\n\nBob\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("sc.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT COALESCE(name,'unknown') FROM '{s}'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "unknown"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "Bob"));
}

test "CAST AS INT: truncates float to integer" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("sc.csv", .{});
        defer f.close();
        try f.writeAll("v\n3.7\n-2.1\n5.0\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("sc.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT CAST(v AS INT) FROM '{s}'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "3"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "-2"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "5"));
    // decimal form must not appear in the output
    try std.testing.expect(!std.mem.containsAtLeast(u8, data, 1, "3.7"));
}

test "CAST AS TEXT: passes value through unchanged" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("sc.csv", .{});
        defer f.close();
        try f.writeAll("v\nhello\n42\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("sc.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT CAST(v AS TEXT) FROM '{s}'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "hello"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "42"));
}

test "scalar AS alias: output header uses alias name" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("sc.csv", .{});
        defer f.close();
        try f.writeAll("name\nAlice\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("sc.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT UPPER(name) AS uname FROM '{s}'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "uname"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "ALICE"));
}

test "ILIKE WHERE: case-insensitive match filters rows" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("sc.csv", .{});
        defer f.close();
        try f.writeAll("name\nAlice\nBob\nalice\nALICE\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("sc.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT name FROM '{s}' WHERE name ILIKE 'alice'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    // all three alice variants must appear
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "Alice"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "alice"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "ALICE"));
    // Bob must be absent
    try std.testing.expect(!std.mem.containsAtLeast(u8, data, 1, "Bob"));
}

test "WHERE AND: evaluateDirect filters with compound condition" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("sc.csv", .{});
        defer f.close();
        try f.writeAll("name,age\nAlice,30\nBob,25\nCarol,30\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("sc.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT name FROM '{s}' WHERE age = 30 AND name != 'Carol'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "Alice"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, data, 1, "Bob"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, data, 1, "Carol"));
}

test "WHERE OR: evaluateDirect passes rows matching either branch" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("sc.csv", .{});
        defer f.close();
        try f.writeAll("name,city\nAlice,NYC\nBob,LA\nCarol,NYC\nDave,Chicago\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("sc.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT name FROM '{s}' WHERE city = 'NYC' OR city = 'LA'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "Alice"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "Bob"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "Carol"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, data, 1, "Dave"));
}

// ── DateTime function tests ────────────────────────────────────────────────────

test "DATEDIFF minutes: calculates time difference in minutes" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("dt.csv", .{});
        defer f.close();
        try f.writeAll("id,start_time,end_time\n1,2026-01-15 09:30:00,2026-01-15 10:45:00\n2,01/15/2026 08:00:00,01/15/2026 09:30:00\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("dt.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT id, DATEDIFF('minute', start_time, end_time) FROM '{s}'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "75")); // 1:15 hours = 75 minutes
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "90")); // 1:30 hours = 90 minutes
}

test "DATEDIFF hours: calculates time difference in hours" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("dt.csv", .{});
        defer f.close();
        try f.writeAll("id,ordered_at,packaged_at\n1,2026-01-15 09:00:00,2026-01-15 11:00:00\n2,2026-01-16T08:00:00,2026-01-16T11:30:00\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("dt.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT id, DATEDIFF('hour', ordered_at, packaged_at) FROM '{s}'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "id,"));
    // Validate exact per-row values by parsing lines
    var lines_h = std.mem.tokenizeAny(u8, data, "\r\n");
    _ = lines_h.next(); // skip header
    const row1_h = lines_h.next() orelse "";
    const row2_h = lines_h.next() orelse "";
    try std.testing.expectEqualStrings("1,2", row1_h); // row 1: 2 hours
    try std.testing.expectEqualStrings("2,3.5", row2_h); // row 2: 3.5 hours
}

test "DATEDIFF days: calculates date difference in days" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("dt.csv", .{});
        defer f.close();
        try f.writeAll("id,shipped,delivered\n1,2026-01-15 14:00:00,2026-01-16 10:30:00\n2,15.01.2026 13:00:00,17.01.2026 09:00:00\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("dt.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT id, DATEDIFF('day', shipped, delivered) FROM '{s}'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "0.85")); // ~20.5 hours
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "1.83")); // ~44 hours
}

test "DATEADD: adds days to date" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("dt.csv", .{});
        defer f.close();
        try f.writeAll("id,shipped_at\n1,2026-01-20 14:30:00\n2,01/21/2026 13:00:00\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("dt.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT id, DATEADD('day', 2, shipped_at) FROM '{s}'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "2026-01-22 14:30:00"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "2026-01-23 13:00:00"));
}

test "DATEADD: adds hours to datetime" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("dt.csv", .{});
        defer f.close();
        try f.writeAll("id,time\n1,2026-01-15 09:00:00\n2,2026-01-15 14:30:00\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("dt.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT id, DATEADD('hour', 5, time) FROM '{s}'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "2026-01-15 14:00:00"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "2026-01-15 19:30:00"));
}

test "DateTime workflow: order processing time analysis" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("orders.csv", .{});
        defer f.close();
        try f.writeAll("order_id,ordered_at,picked_at,shipped_at,delivered_at\n1001,2026-01-15 09:30:00,2026-01-15 10:45:00,2026-01-15 14:00:00,2026-01-16 10:30:00\n1002,01/15/2026 08:00:00,01/15/2026 09:30:00,01/15/2026 13:30:00,01/16/2026 11:15:00\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("orders.csv", &pb);

    const sql = try std.fmt.allocPrint(allocator, "SELECT order_id, DATEDIFF('hour', ordered_at, picked_at) AS pick_hours, DATEDIFF('day', shipped_at, delivered_at) AS ship_days FROM '{s}'", .{p});
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    // Check headers
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "pick_hours"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "ship_days"));
    // Check calculated values - picking takes 1.25-1.5 hours
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "1.25") or std.mem.containsAtLeast(u8, data, 1, "1.50"));
}

test "EXTRACT: extracts year, month, day from datetime column" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("dt.csv", .{});
        defer f.close();
        try f.writeAll("id,event_date\n1,2026-03-15 09:30:00\n2,01/07/2026 14:00:00\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("dt.csv", &pb);

    const sql_year = try std.fmt.allocPrint(allocator, "SELECT id, EXTRACT(year FROM event_date) FROM '{s}'", .{p});
    defer allocator.free(sql_year);
    var q = try parser.parse(allocator, sql_year);
    defer q.deinit();

    const out = try tmp.dir.createFile("out_year.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "2026")); // year extracted
}

test "EXTRACT: extracts month and day from datetime column" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("dt.csv", .{});
        defer f.close();
        try f.writeAll("id,event_date\n1,2026-03-15 09:30:00\n2,2026-07-04 00:00:00\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("dt.csv", &pb);

    const sql_month = try std.fmt.allocPrint(allocator, "SELECT id, EXTRACT(month FROM event_date), EXTRACT(day FROM event_date) FROM '{s}'", .{p});
    defer allocator.free(sql_month);
    var q = try parser.parse(allocator, sql_month);
    defer q.deinit();

    const out = try tmp.dir.createFile("out_month.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "id,"));
    // Validate exact per-row values: id, EXTRACT(month), EXTRACT(day)
    var lines_md = std.mem.tokenizeAny(u8, data, "\r\n");
    _ = lines_md.next(); // skip header
    const row1_md = lines_md.next() orelse "";
    const row2_md = lines_md.next() orelse "";
    try std.testing.expectEqualStrings("1,3,15", row1_md); // March (3), 15th
    try std.testing.expectEqualStrings("2,7,4", row2_md); // July (7), 4th
}

test "EXTRACT: combined with DATEDIFF in same query" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("orders.csv", .{});
        defer f.close();
        try f.writeAll("id,ordered_at,delivered_at\n1,2026-01-15 09:00:00,2026-01-16 09:00:00\n2,2026-03-10 08:00:00,2026-03-11 08:00:00\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("orders.csv", &pb);

    const sql = try std.fmt.allocPrint(
        allocator,
        "SELECT id, EXTRACT(month FROM ordered_at) AS order_month, DATEDIFF('day', ordered_at, delivered_at) AS ship_days FROM '{s}'",
        .{p},
    );
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out_combo.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "order_month"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "ship_days"));
    // Validate exact per-row values: id, order_month, ship_days
    var lines_cd = std.mem.tokenizeAny(u8, data, "\r\n");
    _ = lines_cd.next(); // skip header
    const row1_cd = lines_cd.next() orelse "";
    const row2_cd = lines_cd.next() orelse "";
    try std.testing.expectEqualStrings("1,1,1", row1_cd); // January=1, 1 day
    try std.testing.expectEqualStrings("2,3,1", row2_cd); // March=3, 1 day
}

test "ROUND: rounds to integer when no digits given" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("prices.csv", .{});
        defer f.close();
        try f.writeAll("id,price\n1,1.20\n2,5.99\n3,2.30\n4,3.50\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("prices.csv", &pb);

    const sql = try std.fmt.allocPrint(
        allocator,
        "SELECT id, ROUND(price) AS rounded_price FROM '{s}'",
        .{p},
    );
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out_round_int.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "rounded_price"));
    // price=1.20 → 1, price=5.99 → 6, price=2.30 → 2, price=3.50 → 4
    var lines_ri = std.mem.tokenizeAny(u8, data, "\r\n");
    _ = lines_ri.next(); // skip header
    const row1_ri = lines_ri.next() orelse "";
    const row2_ri = lines_ri.next() orelse "";
    const row3_ri = lines_ri.next() orelse "";
    const row4_ri = lines_ri.next() orelse "";
    try std.testing.expectEqualStrings("1,1", row1_ri);
    try std.testing.expectEqualStrings("2,6", row2_ri);
    try std.testing.expectEqualStrings("3,2", row3_ri);
    try std.testing.expectEqualStrings("4,4", row4_ri);
}

test "ROUND: rounds to specified decimal places" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("prices.csv", .{});
        defer f.close();
        try f.writeAll("id,price\n1,1.85\n2,1.20\n3,2.50\n4,5.994\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("prices.csv", &pb);

    const sql = try std.fmt.allocPrint(
        allocator,
        "SELECT id, ROUND(price, 1) AS price_1dp FROM '{s}'",
        .{p},
    );
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out_round_1dp.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "price_1dp"));
    // price=1.85 → 1.9, price=1.20 → 1.2, price=2.50 → 2.5, price=5.994 → 6.0
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "1.9"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "1.2"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "2.5"));
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "6.0"));
}

test "DATEDIFF in WHERE: filters rows by time difference greater than threshold" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("orders.csv", .{});
        defer f.close();
        // order 1: 2 days (keep), order 2: 0.5 days (drop), order 3: 3 days (keep)
        try f.writeAll("order_id,shipped_at,delivered_at\n" ++
            "1001,2026-01-15 08:00:00,2026-01-17 08:00:00\n" ++
            "1002,2026-01-15 08:00:00,2026-01-15 20:00:00\n" ++
            "1003,2026-01-15 08:00:00,2026-01-18 08:00:00\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("orders.csv", &pb);

    const sql = try std.fmt.allocPrint(
        allocator,
        "SELECT order_id FROM '{s}' WHERE DATEDIFF('day', shipped_at, delivered_at) > 1",
        .{p},
    );
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out_dd_where.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "1001")); // 2 days > 1
    try std.testing.expect(!std.mem.containsAtLeast(u8, data, 1, "1002")); // 0.5 days, excluded
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "1003")); // 3 days > 1
}

test "DATEDIFF in WHERE: combined with AND condition" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("orders.csv", .{});
        defer f.close();
        // status=delivered: order 1 (75 mins, keep), order 3 (120 mins, keep)
        // status=pending: order 2 (60 mins, excluded by status filter)
        try f.writeAll("order_id,status,ordered_at,picked_at\n" ++
            "1001,delivered,2026-01-15 09:00:00,2026-01-15 10:15:00\n" ++
            "1002,pending,2026-01-15 09:00:00,2026-01-15 10:00:00\n" ++
            "1003,delivered,2026-01-15 09:00:00,2026-01-15 11:00:00\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("orders.csv", &pb);

    const sql = try std.fmt.allocPrint(
        allocator,
        "SELECT order_id FROM '{s}' WHERE status = 'delivered' AND DATEDIFF('minute', ordered_at, picked_at) > 60",
        .{p},
    );
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out_dd_and.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "1001")); // delivered + 75 min
    try std.testing.expect(!std.mem.containsAtLeast(u8, data, 1, "1002")); // pending, excluded
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "1003")); // delivered + 120 min
}

test "EXTRACT in WHERE: filters rows by year" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("events.csv", .{});
        defer f.close();
        try f.writeAll("id,event_date\n" ++
            "1,2025-06-15 09:00:00\n" ++
            "2,2026-01-10 14:00:00\n" ++
            "3,2026-03-20 08:00:00\n");
    }
    var pb: [std.fs.max_path_bytes]u8 = undefined;
    const p = try tmp.dir.realpath("events.csv", &pb);

    const sql = try std.fmt.allocPrint(
        allocator,
        "SELECT id FROM '{s}' WHERE EXTRACT(year FROM event_date) = 2026",
        .{p},
    );
    defer allocator.free(sql);
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const out = try tmp.dir.createFile("out_ext_where.csv", .{ .read = true });
    defer out.close();
    try execute(allocator, q, out, .{});

    try out.seekTo(0);
    const data = try out.readToEndAlloc(allocator, 64 * 1024);
    defer allocator.free(data);

    try std.testing.expect(!std.mem.containsAtLeast(u8, data, 1, "1,")); // 2025, excluded
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "2")); // 2026
    try std.testing.expect(std.mem.containsAtLeast(u8, data, 1, "3")); // 2026
}
