const std = @import("std");

/// Output format for query results.
pub const OutputFormat = enum {
    /// Comma-separated values (default).
    csv,
    /// JSON array of objects: [{"col":"val",...},...]
    json,
    /// Newline-delimited JSON (one object per line).
    jsonl,
};

/// Table rendering mode.
pub const TableMode = enum {
    /// Auto-detect: use table when stdout is a TTY and format is csv.
    auto,
    /// Always render as a table (--table flag).
    on,
    /// Never render as a table (--no-table flag).
    off,
};

/// Runtime options parsed from CLI flags.
pub const Options = struct {
    /// Suppress the header row in output.
    no_header: bool = false,
    /// Field delimiter byte (default: comma).
    delimiter: u8 = ',',
    /// Output format (default: csv).
    format: OutputFormat = .csv,
    /// Table rendering mode (default: auto TTY detection).
    table_mode: TableMode = .auto,
    /// Wrap cell content to multiple lines instead of truncating with '…'.
    wrap_cells: bool = false,
    /// Number of worker threads. 0 = auto-detect logical CPU count.
    threads: usize = 0,
    /// Allowed root directories (`--root`). Empty = unrestricted (current behavior).
    /// When set, file access is confined to these trees (see engine.ensurePathAllowed).
    roots: []const []const u8 = &.{},
};

/// Resolve the configured worker count.
/// A value of 0 preserves the current automatic CPU-count behavior.
/// The effective value is clamped to the range 1...1024.
pub fn effectiveThreadCount(opts: Options) usize {
    const n = if (opts.threads != 0)
        opts.threads
    else
        (std.Thread.getCpuCount() catch 1);

    return @max(1, @min(n, 1024));
}

test "effectiveThreadCount uses configured value" {
    const opts = Options{ .threads = 4 };
    try std.testing.expectEqual(@as(usize, 4), effectiveThreadCount(opts));
}

test "effectiveThreadCount auto-detects when threads is zero" {
    const opts = Options{};
    try std.testing.expect(effectiveThreadCount(opts) >= 1);
}

test "effectiveThreadCount caps configured value" {
    const opts = Options{ .threads = 2_000_000 };
    try std.testing.expectEqual(@as(usize, 1024), effectiveThreadCount(opts));
}
