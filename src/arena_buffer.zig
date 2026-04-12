/// arena_buffer.zig — shared growable byte buffer used by engine.zig and mmap_engine.zig.
const std = @import("std");
const Allocator = std.mem.Allocator;

/// Growable byte buffer for accumulating output lines.
/// Uses a single backing allocation that doubles on overflow to minimise
/// allocator calls on large result sets.
///
/// IMPORTANT — slice lifetime: `append` may reallocate `data`, which
/// invalidates any previously returned slice. Callers that need to
/// reference multiple appended regions while still accumulating MUST
/// store (start, len) offsets into `data` and convert to slices only
/// AFTER the final `append`. Storing raw slices across appends leads
/// to dangling pointers.
pub const ArenaBuffer = struct {
    data: []u8,
    pos: usize,
    allocator: Allocator,

    pub fn init(allocator: Allocator, initial_size: usize) !ArenaBuffer {
        return ArenaBuffer{
            .data = try allocator.alloc(u8, initial_size),
            .pos = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ArenaBuffer) void {
        self.allocator.free(self.data);
    }

    pub fn append(self: *ArenaBuffer, bytes: []const u8) ![]const u8 {
        if (self.pos + bytes.len > self.data.len) {
            const new_size = @max(self.data.len * 2, self.pos + bytes.len);
            const new_data = try self.allocator.alloc(u8, new_size);
            @memcpy(new_data[0..self.pos], self.data[0..self.pos]);
            self.allocator.free(self.data);
            self.data = new_data;
        }
        const start = self.pos;
        @memcpy(self.data[start .. start + bytes.len], bytes);
        self.pos += bytes.len;
        return self.data[start .. start + bytes.len];
    }
};

/// Append a JSON-escaped, double-quoted string to an ArenaBuffer.
pub fn appendJsonStringToArena(arena: *ArenaBuffer, s: []const u8) !void {
    _ = try arena.append("\"");
    for (s) |c| {
        switch (c) {
            '"' => _ = try arena.append("\\\""),
            '\\' => _ = try arena.append("\\\\"),
            '\n' => _ = try arena.append("\\n"),
            '\r' => _ = try arena.append("\\r"),
            '\t' => _ = try arena.append("\\t"),
            0x00...0x08, 0x0B...0x0C, 0x0E...0x1F => {
                var buf: [6]u8 = undefined;
                const encoded = std.fmt.bufPrint(&buf, "\\u{X:0>4}", .{c}) catch unreachable;
                _ = try arena.append(encoded);
            },
            else => _ = try arena.append(&[_]u8{c}),
        }
    }
    _ = try arena.append("\"");
}
