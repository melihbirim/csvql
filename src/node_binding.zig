/// Node.js N-API binding for csvql.
/// Compiled to csvql.node — loaded by nodejs/index.js via require().
///
/// Exports two JS functions:
///   queryJson(sql: string) -> string   — JSON array string; JS caller does JSON.parse()
///   queryCsv(sql: string)  -> string   — CSV text (header + rows)
const std = @import("std");
const builtin = @import("builtin");
const parser = @import("parser.zig");
const engine = @import("engine.zig");
const options_mod = @import("options.zig");

const napi = @cImport(@cInclude("node_api.h"));

/// Allocator for everything this addon does (#149).
///
/// Deliberately libc malloc, NOT `GeneralPurposeAllocator`. This addon is a
/// shared object dlopen()ed into Node, and Zig's GPA does not survive that
/// context: it manages its own page buckets through `PageAllocator`, and
/// inside the loaded library that backing allocator faults — a SIGSEGV in
/// `PageAllocator.map` the moment the GPA needs a fresh page rather than
/// serving from an existing bucket.
///
/// That page boundary is why this looked like a bizarre query-shape bug.
/// `SELECT name FROM t WHERE salary > 100000` crossed it and died;
/// `SELECT name, city FROM t WHERE ...` did not and passed. Nothing about
/// projection or filtering was ever involved — those shapes just allocate
/// slightly different amounts. In ReleaseFast the same fault surfaced later
/// and much more confusingly, as "Invalid free" while releasing a result
/// buffer that held the completely correct answer.
///
/// libc malloc is also simply the right choice here: the host process
/// already has one, it is thread-safe, and it makes no assumptions about
/// page size or address-space layout that a dlopen()ed library can violate.
const gpa_alloc = std.heap.c_allocator;

/// Stack for the thread the engine actually runs on (#149).
///
/// The engine's sequential path puts megabytes on the stack by design —
/// `csv.CsvWriter` alone embeds a 1 MB write buffer by value
/// (`buffer: [1048576]u8`), `JsonWriter` another 1 MB, and the readers carry
/// 256 KB each. That is fine in the standalone binary, which starts on the
/// process's main stack (8 MB by default) and does nothing else with it.
///
/// It is NOT fine here. An N-API method runs on whatever stack Node hands
/// us, already partly consumed by V8 frames, and the engine walked straight
/// off the end of it: a SIGSEGV in `RecordWriter.init`'s prologue in Debug,
/// and — worse — in ReleaseFast a silent write past the guard that produced
/// the *right answer* and then aborted with "Invalid free" when the result
/// buffer was released. Correct output followed by a corrupted heap is the
/// dangerous failure mode, because it looks like a heap bug and is not one.
///
/// Running the query on a thread we size ourselves removes the dependency on
/// the host's stack entirely, the same way seeding our own PRNG in
/// query_fuzz.sh removed the dependency on the host's awk. This is a reserve,
/// not a commitment — only touched pages are ever backed by memory.
const engine_stack_size = 64 * 1024 * 1024;

/// Run `runQuery` on a thread with a stack big enough for the engine.
/// Both branches of runQuery (POSIX pipe, Windows temp file) call into the
/// engine, so the guard belongs here, around all of it, rather than around
/// one branch.
fn runQueryOwnStack(sql: []const u8, format: options_mod.OutputFormat) ![]u8 {
    const Ctx = struct {
        sql: []const u8,
        format: options_mod.OutputFormat,
        result: ?[]u8 = null,
        err: ?anyerror = null,
        fn run(ctx: *@This()) void {
            ctx.result = runQuery(ctx.sql, ctx.format) catch |e| {
                ctx.err = e;
                return;
            };
        }
    };
    var ctx = Ctx{ .sql = sql, .format = format };
    const t = std.Thread.spawn(.{ .stack_size = engine_stack_size }, Ctx.run, .{&ctx}) catch |e| {
        // If the thread can't be spawned we must not fall back to running on
        // the caller's stack — that is exactly the crash this exists to
        // prevent. Surface it instead.
        return e;
    };
    t.join();
    if (ctx.err) |e| return e;
    return ctx.result.?;
}

// ── Core query (same pipe+drain pattern as lib.zig) ───────────────────────────

fn runQuery(sql: []const u8, format: options_mod.OutputFormat) ![]u8 {
    const allocator = gpa_alloc;
    var q = try parser.parse(allocator, sql);
    defer q.deinit();

    const opts = options_mod.Options{ .format = format, .table_mode = .off };

    if (builtin.os.tag == .windows) {
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
        try engine.execute(allocator, q, tmp_file, opts);
        try tmp_file.seekTo(0);
        return tmp_file.readToEndAlloc(allocator, 256 * 1024 * 1024);
    }

    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);
    var pipe_fds: [2]std.posix.fd_t = undefined;
    pipe_fds = try std.posix.pipe();
    const read_fd = pipe_fds[0];
    const write_fd = pipe_fds[1];
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
    const eng_result = engine.execute(allocator, q, std.fs.File{ .handle = write_fd }, opts);
    std.posix.close(write_fd);
    drain_thread.join();
    std.posix.close(read_fd);
    try eng_result;
    if (drain_ctx.err) |e| return e;
    return buf.toOwnedSlice(allocator);
}

// ── N-API helpers ─────────────────────────────────────────────────────────────

fn napiFail(env: napi.napi_env, msg: [*:0]const u8) napi.napi_value {
    _ = napi.napi_throw_error(env, null, msg);
    var undef: napi.napi_value = undefined;
    _ = napi.napi_get_undefined(env, &undef);
    return undef;
}

fn getSql(env: napi.napi_env, info: napi.napi_callback_info, allocator: std.mem.Allocator) ![]u8 {
    var argc: usize = 1;
    var args: [1]napi.napi_value = undefined;
    _ = napi.napi_get_cb_info(env, info, &argc, &args, null, null);

    // First call: get byte length
    var sql_len: usize = 0;
    _ = napi.napi_get_value_string_utf8(env, args[0], null, 0, &sql_len);

    // Allocate and fill (napi writes sql_len bytes + null terminator)
    const buf = try allocator.alloc(u8, sql_len + 1);
    _ = napi.napi_get_value_string_utf8(env, args[0], buf.ptr, buf.len, &sql_len);
    return buf[0..sql_len];
}

// ── Exported JS functions ─────────────────────────────────────────────────────

fn napiQueryJson(env: napi.napi_env, info: napi.napi_callback_info) callconv(.c) napi.napi_value {
    const allocator = gpa_alloc;

    const sql = getSql(env, info, allocator) catch return napiFail(env, "out of memory");
    defer allocator.free(sql);

    const result = runQueryOwnStack(sql, .json) catch |err| {
        var msg: [128]u8 = undefined;
        const m = std.fmt.bufPrintZ(&msg, "{s}", .{@errorName(err)}) catch "QueryFailed";
        return napiFail(env, m.ptr);
    };
    defer allocator.free(result);

    var js_str: napi.napi_value = undefined;
    _ = napi.napi_create_string_utf8(env, result.ptr, result.len, &js_str);
    return js_str;
}

fn napiQueryCsv(env: napi.napi_env, info: napi.napi_callback_info) callconv(.c) napi.napi_value {
    const allocator = gpa_alloc;

    const sql = getSql(env, info, allocator) catch return napiFail(env, "out of memory");
    defer allocator.free(sql);

    const result = runQueryOwnStack(sql, .csv) catch |err| {
        var msg: [128]u8 = undefined;
        const m = std.fmt.bufPrintZ(&msg, "{s}", .{@errorName(err)}) catch "QueryFailed";
        return napiFail(env, m.ptr);
    };
    defer allocator.free(result);

    var js_str: napi.napi_value = undefined;
    _ = napi.napi_create_string_utf8(env, result.ptr, result.len, &js_str);
    return js_str;
}

// ── Module registration ───────────────────────────────────────────────────────

export fn napi_register_module_v1(env: napi.napi_env, exports: napi.napi_value) callconv(.c) napi.napi_value {
    const props = [_]napi.napi_property_descriptor{
        .{
            .utf8name = "queryJson",
            .name = null,
            .method = napiQueryJson,
            .getter = null,
            .setter = null,
            .value = null,
            .attributes = napi.napi_default,
            .data = null,
        },
        .{
            .utf8name = "queryCsv",
            .name = null,
            .method = napiQueryCsv,
            .getter = null,
            .setter = null,
            .value = null,
            .attributes = napi.napi_default,
            .data = null,
        },
    };
    _ = napi.napi_define_properties(env, exports, props.len, &props);
    return exports;
}
