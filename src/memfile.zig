//! Anonymous, memory-backed file descriptors — used everywhere the engine
//! needs a std.fs.File-shaped scratch buffer (subquery resolution, join
//! staging, --table rendering) but must never touch a real disk block, per
//! the on-prem/air-gapped promise: those internal staging steps are an
//! implementation detail, not something the caller asked for, and used to
//! silently write to $TMPDIR/csvql_*.tmp before this existed.
//!
//! Linux/FreeBSD: memfd_create() — an anonymous file that lives entirely in
//! page cache/RAM, was never given a path, and disappears with the last
//! close().
//!
//! macOS and Windows are known, documented exceptions, not silent gaps:
//! macOS has no memfd_create, and its POSIX shm_open() alternative turns out
//! not to work for what this actually needs — verified by hand, a freshly
//! shm_open'd fd rejects plain read()/write() with ENXIO; Darwin's shared
//! memory objects are mmap-only, not stream-writable. The engine's output
//! path (engine.execute takes a concrete std.fs.File and calls writeAll
//! repeatedly as rows are produced, an unknown-length stream, not a
//! fixed-size buffer sized once upfront) doesn't fit that shape without a
//! larger rewrite of engine.execute's signature to accept a generic writer
//! instead of std.fs.File. Windows has no equivalent primitive exposed by
//! Zig's std at all today. Both fall back to the same real-temp-file
//! behavior this file replaced on Linux — not a regression, just not yet
//! improved there. See CORRECTNESS.md / the on-prem security doc for the
//! current platform-by-platform status of this guarantee.
const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

pub fn createMemoryBackedFile(allocator: Allocator, name_hint: []const u8) !std.fs.File {
    switch (builtin.os.tag) {
        .linux, .freebsd => {
            const fd = try std.posix.memfd_create(name_hint, 0);
            return std.fs.File{ .handle = fd };
        },
        else => {
            // No memory-backed primitive available here — fall back to a
            // real (self-deleting) temp file, same behavior this replaced.
            const env_var = if (builtin.os.tag == .windows) "TEMP" else "TMPDIR";
            const tmp_dir_owned = std.process.getEnvVarOwned(allocator, env_var) catch null;
            defer if (tmp_dir_owned) |t| allocator.free(t);
            const tmp_dir: []const u8 = tmp_dir_owned orelse (if (builtin.os.tag == .windows) "." else "/tmp");
            const stamp = std.time.nanoTimestamp();
            const tmp_path = try std.fmt.allocPrint(allocator, "{s}{c}csvql_{s}_{d}.tmp", .{ tmp_dir, std.fs.path.sep, name_hint, stamp });
            defer allocator.free(tmp_path);
            const f = try std.fs.cwd().createFile(tmp_path, .{ .truncate = true, .read = true });
            std.fs.cwd().deleteFile(tmp_path) catch {}; // unlink now; fd stays valid until close()
            return f;
        },
    }
}

/// Same idea, but for the one call site that needs a real re-openable path
/// afterward (join+aggregate staging: stage 2 reopens the staged result via
/// query.file_path, not through the handle stage 1 wrote with). A bare
/// memfd has no path — Linux's procfs gives it one anyway via
/// /proc/self/fd/N, which still resolves to the same anonymous, RAM-backed
/// file, not a real disk entry. Other platforms fall back to a real temp
/// file that stays on disk (with a real path) until the caller calls
/// cleanup() — same behavior this replaced there, not yet improved.
pub const FileWithPath = struct {
    file: std.fs.File,
    path: []const u8,
    owns_path: bool,
    is_real_file: bool,

    pub fn cleanup(self: FileWithPath, allocator: Allocator) void {
        if (self.is_real_file) std.fs.cwd().deleteFile(self.path) catch {};
        if (self.owns_path) allocator.free(self.path);
    }
};

pub fn createMemoryBackedFileWithPath(allocator: Allocator, name_hint: []const u8) !FileWithPath {
    switch (builtin.os.tag) {
        .linux => {
            const fd = try std.posix.memfd_create(name_hint, 0);
            const path = try std.fmt.allocPrint(allocator, "/proc/self/fd/{d}", .{fd});
            return .{ .file = std.fs.File{ .handle = fd }, .path = path, .owns_path = true, .is_real_file = false };
        },
        else => {
            const env_var = if (builtin.os.tag == .windows) "TEMP" else "TMPDIR";
            const tmp_dir_owned = std.process.getEnvVarOwned(allocator, env_var) catch null;
            defer if (tmp_dir_owned) |t| allocator.free(t);
            const tmp_dir: []const u8 = tmp_dir_owned orelse (if (builtin.os.tag == .windows) "." else "/tmp");
            const stamp = std.time.nanoTimestamp();
            const path = try std.fmt.allocPrint(allocator, "{s}{c}csvql_{s}_{d}.tmp", .{ tmp_dir, std.fs.path.sep, name_hint, stamp });
            const f = try std.fs.cwd().createFile(path, .{ .truncate = true, .read = true });
            return .{ .file = f, .path = path, .owns_path = true, .is_real_file = true };
        },
    }
}

test "createMemoryBackedFile writes and reads back in RAM" {
    var f = try createMemoryBackedFile(std.testing.allocator, "test");
    defer f.close();
    try f.writeAll("hello world\n");
    try f.seekTo(0);
    var buf: [64]u8 = undefined;
    const n = try f.readAll(&buf);
    try std.testing.expectEqualStrings("hello world\n", buf[0..n]);
}

test "createMemoryBackedFileWithPath is reopenable by its own path" {
    const allocator = std.testing.allocator;
    const fwp = try createMemoryBackedFileWithPath(allocator, "test");
    defer fwp.cleanup(allocator);
    try fwp.file.writeAll("row1\nrow2\n");
    try fwp.file.sync();

    var reopened = try std.fs.cwd().openFile(fwp.path, .{});
    defer reopened.close();
    var buf: [64]u8 = undefined;
    const n = try reopened.readAll(&buf);
    try std.testing.expectEqualStrings("row1\nrow2\n", buf[0..n]);
}
