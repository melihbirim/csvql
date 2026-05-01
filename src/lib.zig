/// csvql C ABI — shared library entry points for Python (ctypes) and other FFI callers.
///
/// Exported functions:
///   csvql_query_json  — run SQL, return JSON array of objects
///   csvql_query_csv   — run SQL, return CSV (header + rows)
///   csvql_free        — release memory returned by the above
///
/// Caller contract:
///   - The returned pointer is null-terminated and heap-allocated by the Zig GPA.
///   - Always call csvql_free() on a non-null result, even after an error.
///   - Return code 0 = success, non-zero = error (message in *out_ptr).
const std = @import("std");
const parser = @import("parser.zig");
const engine = @import("engine.zig");
const options_mod = @import("options.zig");

/// Thread-safe GPA for all lib allocations.
/// A single instance is fine — each call is independent and frees everything
/// before returning via the arena trick below.
var gpa = std.heap.GeneralPurposeAllocator(.{}){};

/// Internal: run SQL and write output to an ArrayList, then return a
/// heap-allocated null-terminated copy the caller must free via csvql_free.
fn runQuery(sql: []const u8, format: options_mod.OutputFormat) ![*:0]u8 {
    const allocator = gpa.allocator();

    // Parse the SQL string.
    var query = try parser.parse(allocator, sql);
    defer query.deinit();

    // Capture engine output into a buffer instead of a real file.
    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(allocator);

    // The engine writes to a std.fs.File. We create a pipe: engine writes to
    // the write end; we read from the read end into buf.
    // Simpler alternative: write to a temp file. Simplest: use a pipe fd pair.
    //
    // Actually the cleanest approach for our engine is to give it a writable
    // file descriptor backed by an in-memory pipe.
    var pipe_fds: [2]std.posix.fd_t = undefined;
    pipe_fds = try std.posix.pipe();

    const read_fd = pipe_fds[0];
    const write_fd = pipe_fds[1];

    const write_file = std.fs.File{ .handle = write_fd };

    const opts = options_mod.Options{
        .format = format,
        .table_mode = .off, // never table-format in lib mode
    };

    // Spawn a thread to drain the pipe while the engine writes, to avoid
    // deadlock on large outputs that exceed the pipe buffer (typically 64 KB).
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
                if (n == 0) return; // EOF
                ctx.out.appendSlice(ctx.alloc, tmp[0..n]) catch |e| {
                    ctx.err = e;
                    return;
                };
            }
        }
    };

    var drain_ctx = DrainCtx{ .rfd = read_fd, .out = &buf, .alloc = allocator };
    const drain_thread = try std.Thread.spawn(.{}, DrainCtx.run, .{&drain_ctx});

    // Run the engine — writes to write_file (the pipe write end).
    const engine_result = engine.execute(allocator, query, write_file, opts);

    // Close the write end first so the drain thread sees EOF.
    std.posix.close(write_fd);
    drain_thread.join();
    std.posix.close(read_fd);

    // Propagate errors after cleanup.
    try engine_result;
    if (drain_ctx.err) |e| return e;

    // Return a heap-allocated null-terminated copy.
    const result = try allocator.allocSentinel(u8, buf.items.len, 0);
    @memcpy(result, buf.items);
    return result;
}

/// Execute a SQL query and return results as a JSON array of objects.
///
/// @param sql      Null-terminated SQL string, e.g. "SELECT * FROM 'data.csv'"
/// @param out_ptr  Set to the result buffer on success, or an error message on failure.
/// @return         0 on success, non-zero on error.
export fn csvql_query_json(sql: [*:0]const u8, out_ptr: *[*:0]u8) c_int {
    const sql_slice = std.mem.sliceTo(sql, 0);
    const result = runQuery(sql_slice, .json) catch |err| {
        const allocator = gpa.allocator();
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
    const result = runQuery(sql_slice, .csv) catch |err| {
        const allocator = gpa.allocator();
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
        const allocator = gpa.allocator();
        const typed: [*:0]u8 = @ptrCast(p);
        const len = std.mem.len(typed);
        allocator.free(typed[0 .. len + 1]);
    }
}
