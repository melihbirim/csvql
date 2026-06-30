const std = @import("std");
const builtin = @import("builtin");

fn mapFile(allocator: std.mem.Allocator, file: std.fs.File, size: u64) ![]const u8 {
    if (builtin.os.tag == .windows) return file.readToEndAlloc(allocator, @intCast(size));
    const mapped = try std.posix.mmap(null, @intCast(size), std.posix.PROT.READ, .{ .TYPE = .SHARED }, file.handle, 0);
    return mapped;
}

fn unmapFile(allocator: std.mem.Allocator, data: []const u8) void {
    if (builtin.os.tag == .windows) {
        allocator.free(data);
        return;
    }
    std.posix.munmap(@alignCast(data));
}

/// Example: High-performance CSV parsing with memory-mapped I/O
/// This is the fastest way to parse CSV files - used in csvq's parallel engine
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        std.debug.print("Usage: mmap_example <file.csv>\n", .{});
        std.process.exit(1);
    }

    const file_path = args[1];
    const file = try std.fs.cwd().openFile(file_path, .{});
    defer file.close();

    const file_size = (try file.stat()).size;
    std.debug.print("File size: {d} bytes\n", .{file_size});

    const data = try mapFile(allocator, file, file_size);
    defer unmapFile(allocator, data);

    // Parse header
    const header_end = std.mem.indexOfScalar(u8, data, '\n') orelse return error.NoHeader;
    const header_line = data[0..header_end];

    std.debug.print("Header: {s}\n\n", .{header_line});

    // Count rows and process (zero-copy!)
    var row_count: usize = 0;
    var field_count: usize = 0;
    var line_start: usize = header_end + 1;

    const start_time = std.time.milliTimestamp();

    while (line_start < data.len) {
        // Find next newline (this is where SIMD acceleration happens)
        const remaining = data[line_start..];
        const line_end = std.mem.indexOfScalar(u8, remaining, '\n') orelse data.len - line_start;

        var line = remaining[0..line_end];
        if (line.len > 0 and line[line.len - 1] == '\r') {
            line = line[0 .. line.len - 1];
        }

        if (line.len > 0) {
            row_count += 1;

            // Parse fields (zero-copy - just find comma positions)
            var field_start: usize = 0;
            var this_row_fields: usize = 0;
            for (line, 0..) |c, i| {
                if (c == ',') {
                    // Field found: line[field_start..i]
                    this_row_fields += 1;
                    field_start = i + 1;
                }
            }
            this_row_fields += 1; // Last field
            field_count += this_row_fields;

            // Print first few rows
            if (row_count <= 5) {
                std.debug.print("Row {d}: {s}\n", .{ row_count, line });
            }
        }

        line_start += line_end + 1;
    }

    const elapsed = std.time.milliTimestamp() - start_time;

    std.debug.print("\n=== Statistics ===\n", .{});
    std.debug.print("Rows: {d}\n", .{row_count});
    std.debug.print("Fields: {d}\n", .{field_count});
    std.debug.print("Parse time: {d}ms\n", .{elapsed});
    std.debug.print("Speed: {d:.0} rows/sec\n", .{@as(f64, @floatFromInt(row_count)) / (@as(f64, @floatFromInt(elapsed)) / 1000.0)});

    const mb = @as(f64, @floatFromInt(file_size)) / (1024.0 * 1024.0);
    const throughput = mb / (@as(f64, @floatFromInt(elapsed)) / 1000.0);
    std.debug.print("Throughput: {d:.0} MB/sec\n", .{throughput});
}
