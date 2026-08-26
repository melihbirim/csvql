/// csvql C ABI — shared library entry points for Python (ctypes) and other FFI callers.
///
/// Exported functions:
///   csvql_query_json  — run SQL, return JSON array of objects
///   csvql_query_csv   — run SQL, return CSV (header + rows)
///   csvql_free        — release memory returned by the above
///
/// Caller contract:
///   - The returned pointer is null-terminated and heap-allocated by libc malloc.
///   - Always call csvql_free() on a non-null result, even after an error.
///   - Return code 0 = success, non-zero = error (message in *out_ptr).
const std = @import("std");
const builtin = @import("builtin");
const parser = @import("parser.zig");
const engine = @import("engine.zig");
const options_mod = @import("options.zig");

/// Allocator for all lib allocations (#149).
///
/// libc malloc, not `GeneralPurposeAllocator`. This is a shared library
/// loaded into a host process (the Python binding dlopen()s it), and Zig's
/// GPA does not survive that: it manages its own page buckets via
/// `PageAllocator`, which faults inside the loaded library the moment a
/// fresh page is needed rather than an existing bucket being reused. The
/// N-API addon hit exactly this and crashed the host on ordinary queries —
/// see the matching comment in node_binding.zig for the full story.
///
/// libc malloc is thread-safe, already present in the host, and makes no
/// assumptions about page size or address-space layout.
const gpa_alloc = std.heap.c_allocator;

/// Stack for the thread the engine actually runs on (#149).
///
/// The engine's sequential path is designed around large stack buffers —
/// `csv.CsvWriter` embeds a 1 MB write buffer by value, `JsonWriter` another
/// 1 MB, the readers 256 KB each. The standalone binary starts on the
/// process main stack and has room. A host that dlopen()s this library does
/// not necessarily: the caller's thread may already be deep, or have been
/// created with a small stack, and the engine then walks off the end of it.
///
/// Sizing our own thread removes the dependency on the host's stack. This is
/// an address-space reserve, not a commitment — only touched pages are ever
/// backed by real memory.
const engine_stack_size = 64 * 1024 * 1024;

/// Run `runQuery` on a thread with a stack big enough for the engine.
fn runQueryOwnStack(sql: []const u8, format: options_mod.OutputFormat) ![*:0]u8 {
    const Ctx = struct {
        sql: []const u8,
        format: options_mod.OutputFormat,
        result: ?[*:0]u8 = null,
        err: ?anyerror = null,
        fn run(ctx: *@This()) void {
            ctx.result = runQuery(ctx.sql, ctx.format) catch |e| {
                ctx.err = e;
                return;
            };
        }
    };
    var ctx = Ctx{ .sql = sql, .format = format };
    // Never fall back to running on the caller's stack — that is the crash
    // this exists to prevent.
    const t = try std.Thread.spawn(.{ .stack_size = engine_stack_size }, Ctx.run, .{&ctx});
    t.join();
    if (ctx.err) |e| return e;
    return ctx.result.?;
}

/// Internal: run SQL and write output to an ArrayList, then return a
/// heap-allocated null-terminated copy the caller must free via csvql_free.
fn runQuery(sql: []const u8, format: options_mod.OutputFormat) ![*:0]u8 {
    const allocator = gpa_alloc;

    // Parse the SQL string.
    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    const opts = options_mod.Options{
        .format = format,
        .table_mode = .off,
    };

    const output: []u8 = if (builtin.os.tag == .windows) blk: {
        // Windows: write to temp file (no std.posix.pipe)
        const tmp_dir = std.process.getEnvVarOwned(allocator, "TEMP") catch
            try allocator.dupe(u8, "C:\\Windows\\Temp");
        defer allocator.free(tmp_dir);
        const tmp_path = try std.fmt.allocPrint(allocator, "{s}\\csvql_{d}.tmp", .{ tmp_dir, std.time.nanoTimestamp() });
        defer allocator.free(tmp_path);
        const tmp_file = try std.fs.createFileAbsolute(tmp_path, .{ .read = true });
        defer {
            tmp_file.close();
            std.fs.deleteFileAbsolute(tmp_path) catch {};
        }
        try engine.execute(allocator, query, tmp_file, opts);
        try tmp_file.seekTo(0);
        break :blk try tmp_file.readToEndAlloc(allocator, 256 * 1024 * 1024);
    } else blk: {
        // POSIX: pipe + drain thread to avoid deadlock on large outputs
        var pipe_fds: [2]std.posix.fd_t = undefined;
        pipe_fds = try std.posix.pipe();
        const read_fd = pipe_fds[0];
        const write_fd = pipe_fds[1];
        var buf = std.ArrayList(u8){};
        errdefer buf.deinit(allocator);
        const DrainCtx = struct {
            rfd: std.posix.fd_t,
            out: *std.ArrayList(u8),
            alloc: std.mem.Allocator,
            err: ?anyerror = null,
            fn run(ctx: *@This()) void {
                var tmp: [4096]u8 = undefined;
                while (true) {
                    const n = std.posix.read(ctx.rfd, &tmp) catch |e| {
                        ctx.err = e;
                        return;
                    };
                    if (n == 0) return;
                    ctx.out.appendSlice(ctx.alloc, tmp[0..n]) catch |e| {
                        ctx.err = e;
                        return;
                    };
                }
            }
        };
        var drain_ctx = DrainCtx{ .rfd = read_fd, .out = &buf, .alloc = allocator };
        const drain_thread = try std.Thread.spawn(.{}, DrainCtx.run, .{&drain_ctx});
        const engine_result = engine.execute(allocator, query, std.fs.File{ .handle = write_fd }, opts);
        std.posix.close(write_fd);
        drain_thread.join();
        std.posix.close(read_fd);
        try engine_result;
        if (drain_ctx.err) |e| return e;
        break :blk try buf.toOwnedSlice(allocator);
    };
    defer allocator.free(output);

    // Return a heap-allocated null-terminated copy.
    const result = try allocator.allocSentinel(u8, output.len, 0);
    @memcpy(result, output);
    return result;
}

/// Execute a SQL query and return results as a JSON array of objects.
///
/// @param sql      Null-terminated SQL string, e.g. "SELECT * FROM 'data.csv'"
/// @param out_ptr  Set to the result buffer on success, or an error message on failure.
/// @return         0 on success, non-zero on error.
export fn csvql_query_json(sql: [*:0]const u8, out_ptr: *[*:0]u8) c_int {
    const sql_slice = std.mem.sliceTo(sql, 0);
    const result = runQueryOwnStack(sql_slice, .json) catch |err| {
        const allocator = gpa_alloc;
        const plain = std.fmt.allocPrint(allocator, "error: {s}", .{@errorName(err)}) catch
            return 2;
        const msg = allocator.dupeZ(u8, plain) catch return 2;
        allocator.free(plain);
        out_ptr.* = msg;
        return 1;
    };
    out_ptr.* = result;
    return 0;
}

/// Execute a SQL query and return results as CSV (header row + data rows).
///
/// @param sql      Null-terminated SQL string.
/// @param out_ptr  Set to the result buffer on success, or an error message on failure.
/// @return         0 on success, non-zero on error.
export fn csvql_query_csv(sql: [*:0]const u8, out_ptr: *[*:0]u8) c_int {
    const sql_slice = std.mem.sliceTo(sql, 0);
    const result = runQueryOwnStack(sql_slice, .csv) catch |err| {
        const allocator = gpa_alloc;
        const plain = std.fmt.allocPrint(allocator, "error: {s}", .{@errorName(err)}) catch
            return 2;
        const msg = allocator.dupeZ(u8, plain) catch return 2;
        allocator.free(plain);
        out_ptr.* = msg;
        return 1;
    };
    out_ptr.* = result;
    return 0;
}

/// Free a buffer returned by csvql_query_json or csvql_query_csv.
/// Safe to call with a null pointer.
export fn csvql_free(ptr: ?*anyopaque) void {
    if (ptr) |p| {
        const allocator = gpa_alloc;
        const typed: [*:0]u8 = @ptrCast(p);
        const len = std.mem.len(typed);
        allocator.free(typed[0 .. len + 1]);
    }
}
