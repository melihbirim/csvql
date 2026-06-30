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

var gpa = std.heap.GeneralPurposeAllocator(.{}){};

// ── Core query (same pipe+drain pattern as lib.zig) ───────────────────────────

fn runQuery(sql: []const u8, format: options_mod.OutputFormat) ![]u8 {
    const allocator = gpa.allocator();
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
    const allocator = gpa.allocator();

    const sql = getSql(env, info, allocator) catch return napiFail(env, "out of memory");
    defer allocator.free(sql);

    const result = runQuery(sql, .json) catch |err| {
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
    const allocator = gpa.allocator();

    const sql = getSql(env, info, allocator) catch return napiFail(env, "out of memory");
    defer allocator.free(sql);

    const result = runQuery(sql, .csv) catch |err| {
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
