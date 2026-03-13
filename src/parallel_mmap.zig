const std = @import("std");
const parser = @import("parser.zig");
const csv = @import("csv.zig");
const simd = @import("simd.zig");
const fast_sort = @import("fast_sort.zig");
const options_mod = @import("options.zig");
const Allocator = std.mem.Allocator;

const WorkChunk = struct {
    start: usize,
    end: usize,
    thread_id: usize,
};

/// Lightweight sort entry — zero-copy slices into mmap data
const SortLine = struct {
    numeric_key: f64, // pre-parsed numeric value (NaN if not numeric)
    sort_key: []const u8, // the sort column value (slice into mmap)
    line: []const u8, // the full raw CSV line (slice into mmap)
};

const WorkerResult = struct {
    // Store output rows as arrays of zero-copy slices into mmap data
    rows: std.ArrayList([][]const u8),
    allocator: Allocator,

    fn deinit(self: *WorkerResult) void {
        for (self.rows.items) |row| {
            // Only free the outer array — field slices point into mmap data
            self.allocator.free(row);
        }
        self.rows.deinit(self.allocator);
    }
};

const WorkerContext = struct {
    data: []const u8,
    chunk: WorkChunk,
    query: parser.Query,
    lower_header: []const []const u8,
    output_indices: []const usize,
    where_column_idx: ?usize,
    result: std.ArrayList([][]const u8),
    /// When true, workers serialize output directly into `output_buf` instead of collecting rows.
    /// key_fragments holds pre-computed `"colname":` strings shared across workers (read-only).
    use_parallel_output: bool,
    output_buf: std.ArrayList(u8),
    key_fragments: []const []const u8,
    format: options_mod.OutputFormat,
    allocator: Allocator,
    mutex: *std.Thread.Mutex,
    delimiter: u8,
};

/// Worker context for ORDER BY — stores lightweight SortLine entries
const SortWorkerContext = struct {
    data: []const u8,
    chunk: WorkChunk,
    query: parser.Query,
    lower_header: []const []const u8,
    where_column_idx: ?usize,
    order_by_col_idx: usize, // column index in the raw CSV (not output)
    result: std.ArrayList(SortLine),
    allocator: Allocator,
    delimiter: u8,
};

/// Parallel memory-mapped CSV processing
pub fn executeParallelMapped(
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
    var writer = csv.RecordWriter.init(output_file, opts);
    defer writer.deinit();

    var output_header = std.ArrayList([]const u8){};
    defer output_header.deinit(allocator);

    for (output_indices.items) |idx| {
        try output_header.append(allocator, header.items[idx]);
    }
    try writer.writeHeader(output_header.items, opts.no_header);

    // Find WHERE column index for fast lookup (avoid HashMap in hot path)
    var where_column_idx: ?usize = null;
    if (query.where_expr) |expr| {
        if (expr == .comparison) {
            const comp = expr.comparison;
            // Find the column index
            for (lower_header, 0..) |name, idx| {
                if (std.mem.eql(u8, name, comp.column)) {
                    where_column_idx = idx;
                    break;
                }
            }
        }
    }

    // Process data in parallel
    const data_start = header_end + 1;
    const data_len = data.len - data_start;

    // Get number of threads
    const num_cores = try std.Thread.getCpuCount();
    const num_threads = @min(num_cores, 8);

    // Split into chunks aligned to line boundaries
    const chunk_size = data_len / num_threads;
    var chunks = try allocator.alloc(WorkChunk, num_threads);
    defer allocator.free(chunks);

    for (0..num_threads) |i| {
        var start = data_start + (i * chunk_size);
        var end = if (i == num_threads - 1) data.len else data_start + ((i + 1) * chunk_size);

        if (i > 0) {
            if (std.mem.indexOfScalarPos(u8, data, start, '\n')) |newline| {
                start = newline + 1;
            }
        }
        if (i < num_threads - 1) {
            if (std.mem.indexOfScalarPos(u8, data, end, '\n')) |newline| {
                end = newline + 1;
            }
        }

        chunks[i] = WorkChunk{ .start = start, .end = end, .thread_id = i };
    }

    if (query.order_by) |order_by| {
        // ===== ORDER BY path: lightweight SortLine entries (zero per-row allocs) =====

        // Find ORDER BY column index in raw CSV header
        var order_by_raw_idx: ?usize = null;
        for (lower_header, 0..) |name, idx| {
            if (std.mem.eql(u8, name, order_by.column)) {
                order_by_raw_idx = idx;
                break;
            }
        }
        if (order_by_raw_idx == null) return error.OrderByColumnNotFound;

        var threads = try allocator.alloc(std.Thread, num_threads);
        defer allocator.free(threads);

        var sort_contexts = try allocator.alloc(SortWorkerContext, num_threads);
        defer allocator.free(sort_contexts);

        for (0..num_threads) |i| {
            sort_contexts[i] = SortWorkerContext{
                .data = data,
                .chunk = chunks[i],
                .query = query,
                .lower_header = lower_header,
                .where_column_idx = where_column_idx,
                .order_by_col_idx = order_by_raw_idx.?,
                .result = std.ArrayList(SortLine){},
                .allocator = allocator,
                .delimiter = opts.delimiter,
            };
            threads[i] = try std.Thread.spawn(.{}, sortWorkerThread, .{&sort_contexts[i]});
        }

        for (threads) |thread| thread.join();

        // Merge all sort entries and convert to fast_sort.SortKey
        var total_entries: usize = 0;
        for (sort_contexts) |ctx| total_entries += ctx.result.items.len;

        var all_entries = try allocator.alloc(fast_sort.SortKey, total_entries);
        defer allocator.free(all_entries);

        var offset: usize = 0;
        for (sort_contexts) |*ctx| {
            for (ctx.result.items) |entry| {
                all_entries[offset] = fast_sort.makeSortKey(
                    entry.numeric_key,
                    entry.sort_key,
                    entry.line,
                );
                offset += 1;
            }
            ctx.result.deinit(allocator);
        }

        // Sort using hardware-aware strategy (radix/heap/comparison)
        const limit: ?usize = if (query.limit >= 0) @intCast(query.limit) else null;
        const sorted = try fast_sort.sortEntries(
            allocator,
            all_entries,
            order_by.order == .desc,
            limit,
        );

        // Write top K rows — re-parse only these lines to extract output columns
        var output_row = try allocator.alloc([]const u8, output_indices.items.len);
        defer allocator.free(output_row);

        var ob_distinct_arena = std.heap.ArenaAllocator.init(allocator);
        defer ob_distinct_arena.deinit();
        var ob_distinct_seen = std.StringHashMap(void).init(allocator);
        defer ob_distinct_seen.deinit();

        var written: usize = 0;
        for (sorted) |entry| {
            if (query.limit >= 0 and written >= @as(usize, @intCast(query.limit))) break;

            // Parse the raw line to extract output columns
            var field_buf: [256][]const u8 = undefined;
            var field_count: usize = 0;
            var field_iter = std.mem.splitScalar(u8, entry.line, opts.delimiter);
            while (field_iter.next()) |field| {
                if (field_count >= field_buf.len) break;
                field_buf[field_count] = field;
                field_count += 1;
            }

            for (output_indices.items, 0..) |idx, j| {
                output_row[j] = if (idx < field_count) field_buf[idx] else "";
            }

            // DISTINCT dedup on the projected output row
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
                if (ob_distinct_seen.contains(row_key)) continue;
                try ob_distinct_seen.put(try ob_distinct_arena.allocator().dupe(u8, row_key), {});
            }

            try writer.writeRecord(output_row);
            written += 1;
        }
    } else {
        // ===== Non-ORDER BY path =====
        var threads = try allocator.alloc(std.Thread, num_threads);
        defer allocator.free(threads);

        var contexts = try allocator.alloc(WorkerContext, num_threads);
        defer allocator.free(contexts);

        var mutex = std.Thread.Mutex{};

        // For unlimited JSON/JSONL/CSV queries (no DISTINCT, no LIMIT): let workers
        // serialize directly into per-thread byte buffers, eliminating the single-threaded
        // serialization bottleneck.
        const use_parallel_output = query.limit < 0 and !query.distinct;
        var key_fragments: []const []const u8 = &[_][]const u8{};
        defer {
            if (key_fragments.len > 0) {
                for (key_fragments) |frag| allocator.free(frag);
                allocator.free(key_fragments);
            }
        }
        if (use_parallel_output and opts.format != .csv) {
            const frags = try allocator.alloc([]const u8, output_header.items.len);
            for (output_header.items, 0..) |key, ki| {
                frags[ki] = try csv.allocJsonKeyFragment(allocator, key);
            }
            key_fragments = frags;
        }

        for (0..num_threads) |i| {
            contexts[i] = WorkerContext{
                .data = data,
                .chunk = chunks[i],
                .query = query,
                .lower_header = lower_header,
                .output_indices = output_indices.items,
                .where_column_idx = where_column_idx,
                .result = std.ArrayList([][]const u8){},
                .use_parallel_output = use_parallel_output,
                .output_buf = std.ArrayList(u8){},
                .key_fragments = key_fragments,
                .format = opts.format,
                .allocator = allocator,
                .mutex = &mutex,
                .delimiter = opts.delimiter,
            };
            threads[i] = try std.Thread.spawn(.{}, workerThread, .{&contexts[i]});
        }

        for (threads) |thread| thread.join();

        // Fast path: stitch per-thread output buffers directly to output_file.
        // Workers serialized in parallel so we only need to concatenate here.
        if (use_parallel_output) {
            defer {
                for (contexts) |*ctx| {
                    ctx.result.deinit(allocator);
                    ctx.output_buf.deinit(allocator);
                }
            }
            // Flush the writer so any buffered header bytes reach the file first.
            try writer.flush();
            if (opts.format == .json) {
                var first_non_empty = true;
                for (contexts) |*ctx| {
                    if (ctx.output_buf.items.len == 0) continue;
                    if (first_non_empty) {
                        try output_file.writeAll("[\n");
                    } else {
                        try output_file.writeAll(",\n");
                    }
                    try output_file.writeAll(ctx.output_buf.items);
                    first_non_empty = false;
                }
                if (first_non_empty) {
                    try output_file.writeAll("[]\n");
                } else {
                    try output_file.writeAll("\n]\n");
                }
            } else {
                // JSONL or CSV: just concatenate chunks in order
                for (contexts) |*ctx| {
                    if (ctx.output_buf.items.len > 0) {
                        try output_file.writeAll(ctx.output_buf.items);
                    }
                }
            }
            return;
        }

        // CSV mode (or JSON with finite LIMIT): use writer.writeRecord with DISTINCT + LIMIT
        var distinct_arena = std.heap.ArenaAllocator.init(allocator);
        defer distinct_arena.deinit();
        var distinct_seen = std.StringHashMap(void).init(allocator);
        defer distinct_seen.deinit();

        var total_written: usize = 0;
        for (contexts) |*ctx| {
            defer ctx.result.deinit(allocator);
            defer ctx.output_buf.deinit(allocator);

            for (ctx.result.items) |row| {
                defer allocator.free(row);

                if (query.distinct) {
                    var key_buf: [8192]u8 = undefined;
                    var klen: usize = 0;
                    for (row, 0..) |field, fi| {
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

                try writer.writeRecord(row);
                total_written += 1;

                if (query.limit >= 0 and total_written >= @as(usize, @intCast(query.limit))) break;
            }

            if (query.limit >= 0 and total_written >= @as(usize, @intCast(query.limit))) break;
        }
    }

    try writer.finish();
    try writer.flush();
}

fn sortWorkerThread(ctx: *SortWorkerContext) void {
    processSortChunk(ctx) catch |err| {
        std.debug.print("Sort worker thread error: {}\n", .{err});
    };
}

/// ORDER BY worker: collects {sort_key, raw_line} with zero per-row allocations
fn processSortChunk(ctx: *SortWorkerContext) !void {
    const chunk_data = ctx.data[ctx.chunk.start..ctx.chunk.end];
    const order_col = ctx.order_by_col_idx;

    var line_start: usize = 0;
    while (line_start < chunk_data.len) {
        const remaining = chunk_data[line_start..];
        const line_end = std.mem.indexOfScalar(u8, remaining, '\n') orelse chunk_data.len - line_start;

        var line = remaining[0..line_end];
        if (line.len > 0 and line[line.len - 1] == '\r') {
            line = line[0 .. line.len - 1];
        }

        if (line.len > 0) {
            // Parse fields into stack buffer (zero-alloc)
            var field_buf: [256][]const u8 = undefined;
            var field_count: usize = 0;
            var field_iter = std.mem.splitScalar(u8, line, ctx.delimiter);
            while (field_iter.next()) |field| {
                if (field_count >= field_buf.len) break;
                field_buf[field_count] = field;
                field_count += 1;
            }

            // Fast WHERE evaluation
            if (ctx.query.where_expr) |expr| {
                if (expr == .comparison) {
                    const comp = expr.comparison;
                    if (ctx.where_column_idx) |col_idx| {
                        if (col_idx < field_count) {
                            const field_value = field_buf[col_idx];
                            if (comp.numeric_value) |threshold| {
                                const val = std.fmt.parseFloat(f64, field_value) catch {
                                    line_start += line_end + 1;
                                    continue;
                                };
                                const matches = switch (comp.operator) {
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
                                if (!matches) {
                                    line_start += line_end + 1;
                                    continue;
                                }
                            } else {
                                const matches = switch (comp.operator) {
                                    .equal => std.mem.eql(u8, field_value, comp.value),
                                    .not_equal => !std.mem.eql(u8, field_value, comp.value),
                                    .like => parser.matchLike(field_value, comp.value),
                                    .ilike => parser.matchILike(field_value, comp.value),
                                    .between, .is_null, .is_not_null => parser.compareValues(comp, field_value),
                                    else => false,
                                };
                                if (!matches) {
                                    line_start += line_end + 1;
                                    continue;
                                }
                            }
                        } else {
                            line_start += line_end + 1;
                            continue;
                        }
                    } else {
                        line_start += line_end + 1;
                        continue;
                    }
                }
                // Note: complex WHERE (AND/OR) not supported in sort worker — parser guarantees simple comparisons for ORDER BY queries
            }

            // Extract sort key and pre-parse numeric value — zero per-row allocation!
            const sort_key = if (order_col < field_count) field_buf[order_col] else "";
            const numeric_key = std.fmt.parseFloat(f64, sort_key) catch std.math.nan(f64);
            try ctx.result.append(ctx.allocator, SortLine{
                .numeric_key = numeric_key,
                .sort_key = sort_key,
                .line = line,
            });
        }

        line_start += line_end + 1;
    }
}

fn workerThread(ctx: *WorkerContext) void {
    processChunk(ctx) catch |err| {
        std.debug.print("Worker thread error: {}\n", .{err});
    };
}

fn processChunk(ctx: *WorkerContext) !void {
    const chunk_data = ctx.data[ctx.chunk.start..ctx.chunk.end];

    // Use arena allocator for temporary allocations
    var arena = std.heap.ArenaAllocator.init(ctx.allocator);
    defer arena.deinit();
    const arena_alloc = arena.allocator();

    // Preallocate fields array (assume max 20 columns)
    var fields = try std.ArrayList([]const u8).initCapacity(arena_alloc, 20);

    var line_start: usize = 0;
    while (line_start < chunk_data.len) {
        const remaining = chunk_data[line_start..];
        const line_end = std.mem.indexOfScalar(u8, remaining, '\n') orelse chunk_data.len - line_start;

        var line = remaining[0..line_end];
        // Trim \r if present
        if (line.len > 0 and line[line.len - 1] == '\r') {
            line = line[0 .. line.len - 1];
        }

        if (line.len > 0) {
            // Reset for reuse
            fields.clearRetainingCapacity();

            // SIMD-accelerated CSV field parsing
            try simd.parseCSVFields(line, &fields, arena_alloc, ctx.delimiter);

            // Fast WHERE evaluation using direct index lookup (avoid HashMap!)
            if (ctx.query.where_expr) |expr| {
                if (expr == .comparison) {
                    const comp = expr.comparison;

                    // Direct column access by index
                    if (ctx.where_column_idx) |col_idx| {
                        if (col_idx < fields.items.len) {
                            const field_value = fields.items[col_idx];

                            // Fast numeric comparison
                            if (comp.numeric_value) |threshold| {
                                const val = std.fmt.parseFloat(f64, field_value) catch {
                                    line_start += line_end + 1;
                                    continue;
                                };

                                const matches = switch (comp.operator) {
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

                                if (!matches) {
                                    line_start += line_end + 1;
                                    continue;
                                }
                            } else {
                                // String comparison
                                const matches = switch (comp.operator) {
                                    .equal => std.mem.eql(u8, field_value, comp.value),
                                    .not_equal => !std.mem.eql(u8, field_value, comp.value),
                                    .like => parser.matchLike(field_value, comp.value),
                                    .ilike => parser.matchILike(field_value, comp.value),
                                    .between, .is_null, .is_not_null => parser.compareValues(comp, field_value),
                                    else => blk: {
                                        const cmp = std.mem.order(u8, field_value, comp.value);
                                        break :blk switch (comp.operator) {
                                            .greater => cmp == .gt,
                                            .greater_equal => cmp == .gt or cmp == .eq,
                                            .less => cmp == .lt,
                                            .less_equal => cmp == .lt or cmp == .eq,
                                            else => false,
                                        };
                                    },
                                };

                                if (!matches) {
                                    line_start += line_end + 1;
                                    continue;
                                }
                            }
                        } else {
                            line_start += line_end + 1;
                            continue;
                        }
                    } else {
                        // Fallback to HashMap (complex WHERE clauses)
                        var row_map = std.StringHashMap([]const u8).init(arena_alloc);
                        for (ctx.lower_header, 0..) |lower_name, idx| {
                            if (idx < fields.items.len) {
                                try row_map.put(lower_name, fields.items[idx]);
                            }
                        }
                        if (!parser.evaluate(expr, row_map)) {
                            line_start += line_end + 1;
                            continue;
                        }
                    }
                } else {
                    // Complex expression - use HashMap fallback
                    var row_map = std.StringHashMap([]const u8).init(arena_alloc);
                    for (ctx.lower_header, 0..) |lower_name, idx| {
                        if (idx < fields.items.len) {
                            try row_map.put(lower_name, fields.items[idx]);
                        }
                    }
                    if (!parser.evaluate(expr, row_map)) {
                        line_start += line_end + 1;
                        continue;
                    }
                }
            }

            if (ctx.use_parallel_output) {
                if (ctx.format == .csv) {
                    // Fast parallel CSV serialization
                    for (ctx.output_indices, 0..) |idx, j| {
                        if (j > 0) try ctx.output_buf.append(ctx.allocator, ctx.delimiter);
                        const value = if (idx < fields.items.len) fields.items[idx] else "";
                        // Check if quoting is needed
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
                } else {
                    // Serialize directly into the per-thread JSON buffer (no heap alloc per row)
                    const is_jsonl = ctx.format == .jsonl;
                    if (ctx.output_buf.items.len > 0 and !is_jsonl) {
                        try ctx.output_buf.appendSlice(ctx.allocator, ",\n");
                    }
                    try ctx.output_buf.append(ctx.allocator, '{');
                    for (ctx.output_indices, 0..) |idx, j| {
                        if (j > 0) try ctx.output_buf.append(ctx.allocator, ',');
                        try ctx.output_buf.appendSlice(ctx.allocator, ctx.key_fragments[j]);
                        const value = if (idx < fields.items.len) fields.items[idx] else "";
                        if (csv.isJsonNumber(value)) {
                            try ctx.output_buf.appendSlice(ctx.allocator, value);
                        } else {
                            try ctx.output_buf.append(ctx.allocator, '"');
                            try csv.appendJsonEscapedToList(&ctx.output_buf, ctx.allocator, value);
                            try ctx.output_buf.append(ctx.allocator, '"');
                        }
                    }
                    try ctx.output_buf.append(ctx.allocator, '}');
                    if (is_jsonl) try ctx.output_buf.append(ctx.allocator, '\n');
                }
            } else {
                // CSV mode (or JSON with LIMIT): build zero-copy output row slices into mmap data
                var output_row = try ctx.allocator.alloc([]const u8, ctx.output_indices.len);
                for (ctx.output_indices, 0..) |idx, j| {
                    output_row[j] = if (idx < fields.items.len) fields.items[idx] else "";
                }
                try ctx.result.append(ctx.allocator, output_row);
            }
        }

        line_start += line_end + 1;
    }
}
