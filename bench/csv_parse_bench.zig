const std = @import("std");
const builtin = @import("builtin");
const simd = @import("simd");

fn mapFile(allocator: std.mem.Allocator, file: std.fs.File, size: u64) ![]const u8 {
    if (builtin.os.tag == .windows) return file.readToEndAlloc(allocator, @intCast(size));
    const mapped = try std.posix.mmap(null, @intCast(size), std.posix.PROT.READ, .{ .TYPE = .SHARED }, file.handle, 0);
    std.posix.madvise(mapped.ptr, mapped.len, std.posix.MADV.SEQUENTIAL) catch {};
    return mapped;
}

fn unmapFile(allocator: std.mem.Allocator, data: []const u8) void {
    if (builtin.os.tag == .windows) {
        allocator.free(data);
        return;
    }
    std.posix.munmap(@alignCast(data));
}

/// Benchmark: Pure CSV parsing (count rows and fields)
/// Tests raw parsing performance without query overhead
///
/// Usage:
///   csv_parse_bench <file.csv> [--include-naive]
///
/// The naive (byte-by-byte) benchmark is skipped by default because it takes
/// several minutes on large files. Pass --include-naive to run it.
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        std.debug.print("Usage: csv_parse_bench <file.csv> [--include-naive]\n", .{});
        std.process.exit(1);
    }

    const file_path = args[1];
    var include_naive = false;
    for (args[2..]) |arg| {
        if (std.mem.eql(u8, arg, "--include-naive")) include_naive = true;
    }

    std.debug.print("Benchmarking CSV parsing on: {s}\n", .{file_path});
    std.debug.print("===========================================\n\n", .{});

    // Benchmark 1: Our buffered reader
    try benchmarkOurReader(allocator, file_path);

    // Benchmark 2: Naive line-by-line (skipped by default — very slow on large files)
    if (include_naive) {
        try benchmarkNaive(allocator, file_path);
    } else {
        std.debug.print("2. Naive (line-by-line): skipped (pass --include-naive to run)\n\n", .{});
    }

    // Benchmark 3: Memory-mapped (our best approach)
    try benchmarkMmap(allocator, file_path);

    // Benchmark 4: real field parser (parseCSVFieldsStatic) — this is the
    // actual production parsing cost, not just a byte scan. Single-threaded.
    try benchmarkRealParser(allocator, file_path);

    // Benchmark 5: fused single-pass scanner prototype (#139) — finds
    // newline/quote/comma all in one SIMD sweep instead of three passes.
    // Not wired into production yet; measuring the real win first.
    try benchmarkFusedScan(allocator, file_path);
}

fn benchmarkFusedScan(allocator: std.mem.Allocator, file_path: []const u8) !void {
    var timer = try std.time.Timer.start();

    const file = try std.fs.cwd().openFile(file_path, .{});
    defer file.close();
    const file_size = (try file.stat()).size;
    const data = try mapFile(allocator, file, file_size);
    defer unmapFile(allocator, data);

    var row_count: usize = 0;
    var field_total: usize = 0;
    var positions: [256]usize = undefined;

    var pos: usize = 0;
    while (pos < data.len) {
        const r = simd.scanRecordFused(data, pos, ',', &positions);
        if (r.had_quote) {
            // Fall back to the existing quote-aware path for this record.
            const fallback_end = csv_module_findRecordEndScalarLike(data, pos);
            var field_buf: [256][]const u8 = undefined;
            var line_end = fallback_end orelse data.len;
            const next_pos = if (fallback_end) |e| e + 1 else data.len;
            if (line_end > pos and data[line_end - 1] == '\r') line_end -= 1;
            const line = data[pos..line_end];
            if (line.len > 0) {
                const n = simd.parseCSVFieldsStatic(line, &field_buf, ',') catch 0;
                field_total += n;
                row_count += 1;
            }
            pos = next_pos;
            continue;
        }
        var line_end = r.end orelse data.len;
        const next_pos = if (r.end) |e| e + 1 else data.len;
        if (line_end > pos and data[line_end - 1] == '\r') line_end -= 1;
        _ = &line_end;
        if (line_end > pos) {
            field_total += r.comma_count + 1;
            row_count += 1;
        }
        pos = next_pos;
    }

    const elapsed = timer.read();
    const ms = @as(f64, @floatFromInt(elapsed)) / 1_000_000.0;
    const mb = @as(f64, @floatFromInt(file_size)) / (1024.0 * 1024.0);

    std.debug.print("5. Fused single-pass scanner (scanRecordFused, single-thread):\n", .{});
    std.debug.print("   Rows: {d}\n", .{row_count});
    std.debug.print("   Fields: {d}\n", .{field_total});
    std.debug.print("   Time: {d:.2}ms\n", .{ms});
    std.debug.print("   Speed: {d:.0} rows/sec\n", .{@as(f64, @floatFromInt(row_count)) / (ms / 1000.0)});
    std.debug.print("   Throughput: {d:.0} MB/sec\n\n", .{mb / (ms / 1000.0)});
}

/// Mirrors csv.findRecordEnd's SIMD fast path exactly (production, as of
/// this session's earlier SIMD work) — local copy so the "real parser"
/// baseline below is quote-aware for line splitting like production
/// actually is, instead of the old naive \n-only split this bench used to
/// do (which double-counted embedded newlines inside quoted fields as row
/// breaks — a bench bug, not a production one; verified against real
/// csvql + DuckDB output before fixing this).
fn findRecordEndLocal(data: []const u8, start: usize) ?usize {
    const nl = std.mem.indexOfScalarPos(u8, data, start, '\n') orelse return null;
    if (std.mem.indexOfScalarPos(u8, data[0..nl], start, '"') == null) {
        return nl;
    }
    return csv_module_findRecordEndScalarLike(data, start);
}

/// Minimal scalar quote-aware record-end finder for the fused-scan
/// benchmark's fallback path — mirrors csv.findRecordEndScalar without
/// pulling in the csv module (bench only imports simd here).
fn csv_module_findRecordEndScalarLike(data: []const u8, start: usize) ?usize {
    var i = start;
    var in_quote = false;
    var at_field_start = true;
    while (i < data.len) {
        const c = data[i];
        if (in_quote) {
            if (c == '"') {
                if (i + 1 < data.len and data[i + 1] == '"') {
                    i += 2;
                    continue;
                }
                in_quote = false;
                at_field_start = false;
            }
            i += 1;
            continue;
        }
        if (c == '"' and at_field_start) {
            in_quote = true;
            at_field_start = false;
        } else if (c == ',') {
            at_field_start = true;
        } else if (c == '\n') {
            return i;
        } else {
            at_field_start = false;
        }
        i += 1;
    }
    return null;
}

fn benchmarkRealParser(allocator: std.mem.Allocator, file_path: []const u8) !void {
    var timer = try std.time.Timer.start();

    const file = try std.fs.cwd().openFile(file_path, .{});
    defer file.close();
    const file_size = (try file.stat()).size;
    const data = try mapFile(allocator, file, file_size);
    defer unmapFile(allocator, data);

    var row_count: usize = 0;
    var field_total: usize = 0;
    var field_buf: [256][]const u8 = undefined;

    // Quote-aware record-boundary search, matching production's
    // csv.findRecordEnd — a naive \n-only split here would double-count
    // embedded newlines inside quoted fields as row breaks.
    var line_start: usize = 0;
    while (findRecordEndLocal(data, line_start)) |nl| {
        var line_end = nl;
        if (line_end > line_start and data[line_end - 1] == '\r') line_end -= 1;
        const line = data[line_start..line_end];
        if (line.len > 0) {
            const n = simd.parseCSVFieldsStatic(line, &field_buf, ',') catch 0;
            field_total += n;
            row_count += 1;
        }
        line_start = nl + 1;
    }
    if (line_start < data.len) {
        const line = data[line_start..data.len];
        const n = simd.parseCSVFieldsStatic(line, &field_buf, ',') catch 0;
        field_total += n;
        row_count += 1;
    }

    const elapsed = timer.read();
    const ms = @as(f64, @floatFromInt(elapsed)) / 1_000_000.0;
    const mb = @as(f64, @floatFromInt(file_size)) / (1024.0 * 1024.0);

    std.debug.print("4. Real field parser (parseCSVFieldsStatic, single-thread):\n", .{});
    std.debug.print("   Rows: {d}\n", .{row_count});
    std.debug.print("   Fields: {d}\n", .{field_total});
    std.debug.print("   Time: {d:.2}ms\n", .{ms});
    std.debug.print("   Speed: {d:.0} rows/sec\n", .{@as(f64, @floatFromInt(row_count)) / (ms / 1000.0)});
    std.debug.print("   Throughput: {d:.0} MB/sec\n\n", .{mb / (ms / 1000.0)});
}

fn benchmarkOurReader(_: std.mem.Allocator, file_path: []const u8) !void {
    var timer = try std.time.Timer.start();

    const file = try std.fs.cwd().openFile(file_path, .{});
    defer file.close();

    var buffer: [262144]u8 = undefined; // 256KB
    var row_count: usize = 0;
    var field_count: usize = 0;

    while (true) {
        const bytes_read = try file.read(&buffer);
        if (bytes_read == 0) break;

        for (buffer[0..bytes_read]) |c| {
            if (c == '\n') row_count += 1;
            if (c == ',') field_count += 1;
        }
    }
    // Adjust for fields per row
    if (row_count > 0) {
        field_count += row_count;
    }

    const elapsed = timer.read();
    const ms = @as(f64, @floatFromInt(elapsed)) / 1_000_000.0;

    std.debug.print("1. Buffered Reader (256KB buffer):\n", .{});
    std.debug.print("   Rows: {d}\n", .{row_count});
    std.debug.print("   Fields: {d}\n", .{field_count});
    std.debug.print("   Time: {d:.2}ms\n", .{ms});
    std.debug.print("   Speed: {d:.0} rows/sec\n\n", .{@as(f64, @floatFromInt(row_count)) / (ms / 1000.0)});
}

fn benchmarkNaive(allocator: std.mem.Allocator, file_path: []const u8) !void {
    var timer = try std.time.Timer.start();

    const file = try std.fs.cwd().openFile(file_path, .{});
    defer file.close();

    var row_count: usize = 0;
    var field_count: usize = 0;

    var line_buf = std.ArrayList(u8){};
    defer line_buf.deinit(allocator);

    var one_byte: [1]u8 = undefined;
    while (true) {
        line_buf.clearRetainingCapacity();
        while (true) {
            const bytes_read = try file.read(&one_byte);
            if (bytes_read == 0) break;
            if (one_byte[0] == '\n') break;
            try line_buf.append(allocator, one_byte[0]);
        }

        if (line_buf.items.len > 0) {
            row_count += 1;
            for (line_buf.items) |c| {
                if (c == ',') field_count += 1;
            }
            field_count += 1;
        }

        if (line_buf.items.len == 0 and (try file.read(&one_byte)) == 0) break;
    }

    const elapsed = timer.read();
    const ms = @as(f64, @floatFromInt(elapsed)) / 1_000_000.0;

    std.debug.print("2. Naive (line-by-line with ArrayList):\n", .{});
    std.debug.print("   Rows: {d}\n", .{row_count});
    std.debug.print("   Fields: {d}\n", .{field_count});
    std.debug.print("   Time: {d:.2}ms\n", .{ms});
    std.debug.print("   Speed: {d:.0} rows/sec\n\n", .{@as(f64, @floatFromInt(row_count)) / (ms / 1000.0)});
}

fn benchmarkMmap(allocator: std.mem.Allocator, file_path: []const u8) !void {
    var timer = try std.time.Timer.start();

    const file = try std.fs.cwd().openFile(file_path, .{});
    defer file.close();

    const file_size = (try file.stat()).size;

    const data = try mapFile(allocator, file, file_size);
    defer unmapFile(allocator, data);

    var row_count: usize = 0;
    var field_count: usize = 0;

    for (data) |c| {
        if (c == '\n') row_count += 1;
        if (c == ',') field_count += 1;
    }
    // Adjust for fields per row
    if (row_count > 0) {
        field_count += row_count; // Each row has one more field than commas
    }

    const elapsed = timer.read();
    const ms = @as(f64, @floatFromInt(elapsed)) / 1_000_000.0;

    std.debug.print("3. Memory-Mapped (zero-copy scan):\n", .{});
    std.debug.print("   Rows: {d}\n", .{row_count});
    std.debug.print("   Fields: {d}\n", .{field_count});
    std.debug.print("   Time: {d:.2}ms\n", .{ms});
    std.debug.print("   Speed: {d:.0} rows/sec\n\n", .{@as(f64, @floatFromInt(row_count)) / (ms / 1000.0)});

    const mb = @as(f64, @floatFromInt(file_size)) / (1024.0 * 1024.0);
    const throughput = mb / (ms / 1000.0);
    std.debug.print("   Throughput: {d:.0} MB/sec\n", .{throughput});
}
