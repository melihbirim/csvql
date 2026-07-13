//! `csvql install` — register csvql as an MCP server in Claude, no manual config.
//!
//!   * Claude Code  — runs `claude mcp add --scope user csvql -- <csvql> --mcp`
//!   * Claude Desktop — merges an `mcpServers.csvql` entry into the desktop config JSON
//!
//! Idempotent: re-running overwrites the csvql entry, never touches other servers.
//! `csvql install --print` dry-runs (shows what it would do, writes nothing).

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

pub fn run(allocator: Allocator, print_only: bool) !void {
    const out = std.fs.File.stdout();
    var buf: [4096]u8 = undefined;

    const self_path = try std.fs.selfExePathAlloc(allocator);
    defer allocator.free(self_path);

    try print(out, &buf, "csvql install — registering the MCP server\n  binary: {s}\n\n", .{self_path});

    try installClaudeCode(allocator, out, &buf, self_path, print_only);
    try installClaudeDesktop(allocator, out, &buf, self_path, print_only);

    if (print_only)
        try out.writeAll("\n(--print: nothing was written.)\n")
    else
        try out.writeAll("\nDone. Restart Claude to load the csvql tools.\n");
}

fn print(f: std.fs.File, buf: []u8, comptime fmt: []const u8, args: anytype) !void {
    try f.writeAll(try std.fmt.bufPrint(buf, fmt, args));
}

// --- Claude Code (CLI) ------------------------------------------------------

fn installClaudeCode(allocator: Allocator, out: std.fs.File, buf: []u8, self_path: []const u8, print_only: bool) !void {
    // Is the `claude` CLI available?
    const probe = std.process.Child.run(.{ .allocator = allocator, .argv = &.{ "claude", "--version" } }) catch {
        try out.writeAll("Claude Code:    claude CLI not found on PATH — skipped.\n");
        return;
    };
    allocator.free(probe.stdout);
    allocator.free(probe.stderr);

    const argv = [_][]const u8{ "claude", "mcp", "add", "--scope", "user", "csvql", "--", self_path, "--mcp" };
    if (print_only) {
        try print(out, buf, "Claude Code:    would run: claude mcp add --scope user csvql -- {s} --mcp\n", .{self_path});
        return;
    }
    // Remove any stale entry first so re-running is clean (ignore failure — may not exist).
    const rm = std.process.Child.run(.{ .allocator = allocator, .argv = &.{ "claude", "mcp", "remove", "--scope", "user", "csvql" } }) catch null;
    if (rm) |r| {
        allocator.free(r.stdout);
        allocator.free(r.stderr);
    }
    const res = std.process.Child.run(.{ .allocator = allocator, .argv = &argv }) catch |e| {
        try print(out, buf, "Claude Code:    failed to run claude mcp add ({s}).\n", .{@errorName(e)});
        return;
    };
    defer allocator.free(res.stdout);
    defer allocator.free(res.stderr);
    const ok = res.term == .Exited and res.term.Exited == 0;
    try print(out, buf, "Claude Code:    {s}\n", .{if (ok) "added (scope: user)." else "claude mcp add returned an error."});
}

// --- Claude Desktop (config JSON merge) -------------------------------------

fn desktopConfigPath(allocator: Allocator) !?[]u8 {
    const home = std.process.getEnvVarOwned(allocator, if (builtin.os.tag == .windows) "APPDATA" else "HOME") catch return null;
    defer allocator.free(home);
    const rel = switch (builtin.os.tag) {
        .macos => "Library/Application Support/Claude/claude_desktop_config.json",
        .windows => "Claude\\claude_desktop_config.json",
        else => ".config/Claude/claude_desktop_config.json",
    };
    return try std.fs.path.join(allocator, &.{ home, rel });
}

fn installClaudeDesktop(allocator: Allocator, out: std.fs.File, buf: []u8, self_path: []const u8, print_only: bool) !void {
    const path = (try desktopConfigPath(allocator)) orelse {
        try out.writeAll("Claude Desktop: home directory not found — skipped.\n");
        return;
    };
    defer allocator.free(path);

    // Read existing config (or start from an empty object).
    const existing = std.fs.cwd().readFileAlloc(allocator, path, 4 * 1024 * 1024) catch |e| switch (e) {
        error.FileNotFound => try allocator.dupe(u8, "{}"),
        else => return e,
    };
    defer allocator.free(existing);

    const bytes = try mergeDesktopConfig(allocator, existing, self_path);
    defer allocator.free(bytes);

    if (print_only) {
        try print(out, buf, "Claude Desktop: would write {s}\n", .{path});
        return;
    }
    if (std.fs.path.dirname(path)) |dir| std.fs.cwd().makePath(dir) catch {};
    try std.fs.cwd().writeFile(.{ .sub_path = path, .data = bytes });
    try print(out, buf, "Claude Desktop: configured {s}\n", .{path});
}

/// Merge an `mcpServers.csvql` entry into `existing` JSON, preserving every other
/// key and server. Returns the pretty-printed result (caller frees). Invalid or
/// non-object input is replaced with a fresh object rather than crashing.
fn mergeDesktopConfig(allocator: Allocator, existing: []const u8, self_path: []const u8) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var parsed = std.json.parseFromSliceLeaky(std.json.Value, a, existing, .{}) catch std.json.Value{ .object = std.json.ObjectMap.init(a) };
    if (parsed != .object) parsed = std.json.Value{ .object = std.json.ObjectMap.init(a) };

    var root = &parsed.object;
    const servers_v = root.get("mcpServers");
    var servers = if (servers_v != null and servers_v.? == .object)
        servers_v.?.object
    else
        std.json.ObjectMap.init(a);

    // csvql entry: { "command": <self>, "args": ["--mcp"] }
    var entry = std.json.ObjectMap.init(a);
    try entry.put("command", .{ .string = try a.dupe(u8, self_path) });
    var args_arr = std.json.Array.init(a);
    try args_arr.append(.{ .string = "--mcp" });
    try entry.put("args", .{ .array = args_arr });
    try servers.put("csvql", .{ .object = entry });
    try root.put("mcpServers", .{ .object = servers });

    var aw = std.Io.Writer.Allocating.init(allocator);
    defer aw.deinit();
    try std.json.Stringify.value(parsed, .{ .whitespace = .indent_2 }, &aw.writer);
    return allocator.dupe(u8, aw.written());
}

test "mergeDesktopConfig preserves other servers and keys" {
    const allocator = std.testing.allocator;
    const in =
        \\{ "mcpServers": { "other": { "command": "/bin/other", "args": [] } }, "theme": "dark" }
    ;
    const out = try mergeDesktopConfig(allocator, in, "/usr/local/bin/csvql");
    defer allocator.free(out);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"other\"") != null); // kept
    try std.testing.expect(std.mem.indexOf(u8, out, "\"theme\"") != null); // kept
    try std.testing.expect(std.mem.indexOf(u8, out, "\"csvql\"") != null); // added
    try std.testing.expect(std.mem.indexOf(u8, out, "/usr/local/bin/csvql") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "--mcp") != null);
}

test "mergeDesktopConfig on empty/missing config creates a valid object" {
    const allocator = std.testing.allocator;
    const out = try mergeDesktopConfig(allocator, "{}", "/x/csvql");
    defer allocator.free(out);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"mcpServers\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "\"csvql\"") != null);
}
