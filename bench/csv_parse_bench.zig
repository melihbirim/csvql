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

    var line_start: usize = 0;
    var i: usize = 0;
    while (i < data.len) : (i += 1) {
        if (data[i] == '\n') {
            var line_end = i;
            if (line_end > line_start and data[line_end - 1] == '\r') line_end -= 1;
            const line = data[line_start..line_end];
            if (line.len > 0) {
                const n = simd.parseCSVFieldsStatic(line, &field_buf, ',') catch 0;
                field_total += n;
                row_count += 1;
            }
            line_start = i + 1;
        }
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
