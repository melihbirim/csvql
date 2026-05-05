const std = @import("std");
const mem = std.mem;
const Allocator = std.mem.Allocator;

pub const Alignment = enum {
    left,
    center,
    right,
};

pub const BorderStyle = enum {
    ascii,
    unicode,
    minimal,
    markdown,
    none,

    pub fn chars(self: BorderStyle) BorderChars {
        return switch (self) {
            .ascii => .{
                .top_left = '+',
                .top_right = '+',
                .bottom_left = '+',
                .bottom_right = '+',
                .horizontal = '-',
                .vertical = '|',
                .cross = '+',
                .t_down = '+',
                .t_up = '+',
                .t_right = '+',
                .t_left = '+',
            },
            .unicode => .{
                .top_left = '┌',
                .top_right = '┐',
                .bottom_left = '└',
                .bottom_right = '┘',
                .horizontal = '─',
                .vertical = '│',
                .cross = '┼',
                .t_down = '┬',
                .t_up = '┴',
                .t_right = '├',
                .t_left = '┤',
            },
            .minimal => .{
                .top_left = ' ',
                .top_right = ' ',
                .bottom_left = ' ',
                .bottom_right = ' ',
                .horizontal = '-',
                .vertical = ' ',
                .cross = ' ',
                .t_down = ' ',
                .t_up = ' ',
                .t_right = ' ',
                .t_left = ' ',
            },
            .markdown => .{
                .top_left = '|',
                .top_right = '|',
                .bottom_left = '|',
                .bottom_right = '|',
                .horizontal = '-',
                .vertical = '|',
                .cross = '|',
                .t_down = '|',
                .t_up = '|',
                .t_right = '|',
                .t_left = '|',
            },
            .none => .{
                .top_left = ' ',
                .top_right = ' ',
                .bottom_left = ' ',
                .bottom_right = ' ',
                .horizontal = ' ',
                .vertical = ' ',
                .cross = ' ',
                .t_down = ' ',
                .t_up = ' ',
                .t_right = ' ',
                .t_left = ' ',
            },
        };
    }
};

pub const BorderChars = struct {
    top_left: u21,
    top_right: u21,
    bottom_left: u21,
    bottom_right: u21,
    horizontal: u21,
    vertical: u21,
    cross: u21,
    t_down: u21,
    t_up: u21,
    t_right: u21,
    t_left: u21,
};

pub const Column = struct {
    name: []const u8,
    alignment: Alignment = .left,
    max_width: ?usize = null,
};

pub const Table = struct {
    allocator: Allocator,
    columns: []const Column,
    rows: std.ArrayList([]const []const u8),
    border_style: BorderStyle = .unicode,
    show_header: bool = true,
    padding: usize = 1,
    /// When set, column widths are proportionally fitted so the total rendered
    /// table width equals exactly this many terminal columns.  Wide columns
    /// shrink first; narrow columns keep their natural width.
    terminal_width: ?usize = null,
    /// When true, cells wider than their column wrap to additional lines inside
    /// the same row instead of being truncated with '…'.
    wrap_cells: bool = false,
    /// Number of real columns shown when terminal overflow drops columns (0 = all).
    /// Set internally by calculateColumnWidths(); do not set manually.
    num_visible_cols: usize = 0,

    pub fn init(allocator: Allocator, columns: []const Column) Table {
        return Table{
            .allocator = allocator,
            .columns = columns,
            .rows = std.ArrayList([]const []const u8){},
            .border_style = .unicode,
            .show_header = true,
            .padding = 1,
            .terminal_width = null,
            .wrap_cells = false,
            .num_visible_cols = 0,
        };
    }

    pub fn deinit(self: *Table) void {
        for (self.rows.items) |row| {
            self.allocator.free(row);
        }
        self.rows.deinit(self.allocator);
    }

    pub fn addRow(self: *Table, row: []const []const u8) !void {
        if (row.len != self.columns.len) {
            return error.ColumnCountMismatch;
        }
        const row_copy = try self.allocator.dupe([]const u8, row);
        try self.rows.append(self.allocator, row_copy);
    }

    pub fn render(self: *Table, writer: anytype) !void {
        const col_widths = try self.calculateColumnWidths();
        defer self.allocator.free(col_widths);

        const border_chars = self.border_style.chars();

        // Top border
        if (self.border_style != .none and self.border_style != .markdown) {
            try self.renderBorder(writer, col_widths, border_chars.top_left, border_chars.horizontal, border_chars.t_down, border_chars.top_right);
        }

        // Overflow: calculateColumnWidths set num_visible_cols > 0 when columns were dropped.
        const overflow = self.num_visible_cols > 0;
        // Format "+N" label for the overflow pseudo-column header.
        var plus_n_buf: [16]u8 = undefined;
        const plus_n: []const u8 = if (overflow) blk: {
            const hidden = self.columns.len - self.num_visible_cols;
            break :blk std.fmt.bufPrint(&plus_n_buf, "+{d}", .{hidden}) catch "+?";
        } else "";

        // Header
        if (self.show_header) {
            const header_row = try self.getHeaderRow();
            defer self.allocator.free(header_row);

            if (overflow) {
                const visible = self.num_visible_cols;
                const eff = try self.allocator.alloc([]const u8, col_widths.len);
                defer self.allocator.free(eff);
                @memcpy(eff[0..visible], header_row[0..visible]);
                eff[visible] = plus_n; // "+N" in the header
                try self.renderRow(writer, eff, col_widths, border_chars.vertical);
            } else {
                try self.renderRow(writer, header_row, col_widths, border_chars.vertical);
            }

            // Header separator
            if (self.border_style == .markdown) {
                try self.renderMarkdownSeparator(writer, col_widths);
            } else if (self.border_style != .none) {
                try self.renderBorder(writer, col_widths, border_chars.t_right, border_chars.horizontal, border_chars.cross, border_chars.t_left);
            }
        }

        // Data rows
        for (self.rows.items, 0..) |row, i| {
            if (overflow) {
                const visible = self.num_visible_cols;
                const eff = try self.allocator.alloc([]const u8, col_widths.len);
                defer self.allocator.free(eff);
                @memcpy(eff[0..visible], row[0..@min(visible, row.len)]);
                eff[visible] = "…"; // just ellipsis in data rows — count is in the header
                try self.renderRow(writer, eff, col_widths, border_chars.vertical);
            } else {
                try self.renderRow(writer, row, col_widths, border_chars.vertical);
            }

            // Row separator (except after last row)
            if (self.border_style != .none and self.border_style != .markdown and self.border_style != .minimal and i < self.rows.items.len - 1) {
                try self.renderBorder(writer, col_widths, border_chars.t_right, border_chars.horizontal, border_chars.cross, border_chars.t_left);
            }
        }

        // Bottom border
        if (self.border_style != .none and self.border_style != .markdown) {
            try self.renderBorder(writer, col_widths, border_chars.bottom_left, border_chars.horizontal, border_chars.t_up, border_chars.bottom_right);
        }
    }

    fn getHeaderRow(self: *Table) ![]const []const u8 {
        const headers = try self.allocator.alloc([]const u8, self.columns.len);
        for (self.columns, 0..) |col, i| {
            headers[i] = col.name;
        }
        return headers;
    }

    /// Compute column widths.
    ///
    /// Default (no wrap): keep natural widths, greedily fit columns L→R within
    /// `terminal_width`, drop the rest, and append a "…" pseudo-column to
    /// signal the hidden columns.  All visible cells render at full width.
    ///
    /// With `wrap_cells = true`: proportional fair-share shrink so every
    /// column fits and cells wrap to additional lines.
    fn calculateColumnWidths(self: *Table) ![]usize {
        const n = self.columns.len;

        // Compute natural widths (always needed).
        const natural = try self.allocator.alloc(usize, n);
        defer self.allocator.free(natural);

        for (self.columns, 0..) |col, i| {
            natural[i] = displayLen(col.name);
        }
        for (self.rows.items) |row| {
            for (row, 0..) |cell, i| {
                const dlen = displayLen(cell);
                if (dlen > natural[i]) natural[i] = dlen;
            }
        }
        for (self.columns, 0..) |col, i| {
            if (col.max_width) |max| {
                if (natural[i] > max) natural[i] = max;
            }
        }

        if (self.terminal_width) |term_w| {
            // Per-column overhead: left-border(1) + padding + content + padding.
            const per_col_overhead = 2 * self.padding + 1;

            if (self.wrap_cells) {
                // --- Proportional fair-share: all columns visible, cells wrap. ---
                self.num_visible_cols = 0;
                const fixed_overhead = 1 + n * per_col_overhead;
                var total: usize = fixed_overhead;
                for (natural) |w| total += w;

                if (total > term_w and term_w > fixed_overhead) {
                    const budget = term_w - fixed_overhead;
                    var remaining_budget = budget;
                    var settled = try self.allocator.alloc(bool, n);
                    defer self.allocator.free(settled);
                    @memset(settled, false);

                    var unsettled: usize = n;
                    while (unsettled > 0) {
                        const share = remaining_budget / unsettled;
                        var newly_settled: usize = 0;
                        for (natural, 0..) |w, i| {
                            if (!settled[i] and w <= share) {
                                settled[i] = true;
                                remaining_budget -= w;
                                unsettled -= 1;
                                newly_settled += 1;
                            }
                        }
                        if (newly_settled == 0) {
                            const cap = @max(4, remaining_budget / unsettled);
                            for (natural, 0..) |_, i| {
                                if (!settled[i]) natural[i] = cap;
                            }
                            break;
                        }
                    }
                }
                return try self.allocator.dupe(usize, natural);
            } else {
                // --- Column-drop mode: natural widths, rightmost cols dropped. ---
                // Check if everything fits.
                var total: usize = 1; // left border
                for (natural) |w| total += per_col_overhead + w;

                if (total <= term_w) {
                    self.num_visible_cols = 0; // all visible, no overflow
                    return try self.allocator.dupe(usize, natural);
                }

                // Reserve space for the "…" pseudo-column (content width = 1).
                const pseudo_cost = per_col_overhead + 1;
                const budget: usize = if (term_w > 1 + pseudo_cost) term_w - 1 - pseudo_cost else 0;

                var used: usize = 0;
                var visible: usize = 0;
                for (natural) |w| {
                    const cost = per_col_overhead + w;
                    if (used + cost <= budget) {
                        used += cost;
                        visible += 1;
                    } else break;
                }
                if (visible == 0) visible = 1; // always show at least one column

                self.num_visible_cols = visible;
                const hidden = n - visible;
                // Width of the pseudo-column: "+N" label (e.g. "+9" = 2, "+12" = 3).
                var pseudo_w: usize = 1;
                var tmp = hidden;
                while (tmp > 0) : (tmp /= 10) pseudo_w += 1;
                const result = try self.allocator.alloc(usize, visible + 1);
                @memcpy(result[0..visible], natural[0..visible]);
                result[visible] = pseudo_w; // pseudo-column width
                return result;
            }
        }

        // No terminal_width: all natural widths, no overflow.
        self.num_visible_cols = 0;
        return try self.allocator.dupe(usize, natural);
    }

    fn renderBorder(self: *Table, writer: anytype, col_widths: []const usize, left: u21, fill: u21, sep: u21, right: u21) !void {
        var buf: [4]u8 = undefined;

        const left_len = std.unicode.utf8Encode(left, &buf) catch unreachable;
        _ = try writer.write(buf[0..left_len]);

        for (col_widths, 0..) |width, i| {
            const total_width = width + self.padding * 2;
            const fill_len = std.unicode.utf8Encode(fill, &buf) catch unreachable;
            for (0..total_width) |_| {
                _ = try writer.write(buf[0..fill_len]);
            }
            if (i < col_widths.len - 1) {
                const sep_len = std.unicode.utf8Encode(sep, &buf) catch unreachable;
                _ = try writer.write(buf[0..sep_len]);
            }
        }

        const right_len = std.unicode.utf8Encode(right, &buf) catch unreachable;
        _ = try writer.write(buf[0..right_len]);
        _ = try writer.write("\n");
    }

    fn renderMarkdownSeparator(self: *Table, writer: anytype, col_widths: []const usize) !void {
        _ = try writer.write("|");
        for (col_widths, 0..) |width, i| {
            for (0..self.padding) |_| {
                _ = try writer.write(" ");
            }

            const real_count = if (self.num_visible_cols > 0) self.num_visible_cols else self.columns.len;
            const alignment: Alignment = if (i < real_count) self.columns[i].alignment else .left;
            if (alignment == .center or alignment == .left) {
                _ = try writer.write(":");
            } else {
                _ = try writer.write("-");
            }

            for (1..width) |_| {
                _ = try writer.write("-");
            }

            if (alignment == .center or alignment == .right) {
                _ = try writer.write(":");
            } else {
                _ = try writer.write("-");
            }

            for (0..self.padding) |_| {
                _ = try writer.write(" ");
            }
            _ = try writer.write("|");
        }
        _ = try writer.write("\n");
    }

    fn renderRow(self: *Table, writer: anytype, row: []const []const u8, col_widths: []const usize, sep: u21) !void {
        if (self.wrap_cells) {
            try self.renderRowWrapped(writer, row, col_widths, sep);
        } else {
            try self.renderRowTruncated(writer, row, col_widths, sep);
        }
    }

    /// Render a row where overflowing cells are truncated with '…'.
    fn renderRowTruncated(self: *Table, writer: anytype, row: []const []const u8, col_widths: []const usize, sep: u21) !void {
        var buf: [4]u8 = undefined;

        if (self.border_style != .none) {
            const sep_len = std.unicode.utf8Encode(sep, &buf) catch unreachable;
            _ = try writer.write(buf[0..sep_len]);
        }

        for (row, 0..) |cell, i| {
            for (0..self.padding) |_| {
                _ = try writer.write(" ");
            }

            const width = col_widths[i];
            // num_visible_cols > 0 means overflow is active; the last slot is the pseudo-col.
            const real_count = if (self.num_visible_cols > 0) self.num_visible_cols else self.columns.len;
            const alignment: Alignment = if (i < real_count) self.columns[i].alignment else .center;
            const cell_dlen = displayLen(cell);

            if (cell_dlen > width) {
                // Truncate with ellipsis. Ellipsis (…) is 1 display col, 3 UTF-8 bytes.
                const text_cols = if (width > 1) width - 1 else width;
                const byte_end = displayColsToBytes(cell, text_cols);
                if (width > 1) {
                    _ = try writer.write(cell[0..byte_end]);
                    _ = try writer.write("…"); // U+2026, 3 bytes, 1 display col
                } else {
                    _ = try writer.write(cell[0..byte_end]);
                }
            } else {
                try self.renderCell(writer, cell, cell_dlen, width, alignment);
            }

            for (0..self.padding) |_| {
                _ = try writer.write(" ");
            }

            if (self.border_style != .none) {
                const sep_len = std.unicode.utf8Encode(sep, &buf) catch unreachable;
                _ = try writer.write(buf[0..sep_len]);
            } else if (i < row.len - 1) {
                _ = try writer.write(" ");
            }
        }
        _ = try writer.write("\n");
    }

    /// Render a row where overflowing cells wrap to additional lines.
    fn renderRowWrapped(self: *Table, writer: anytype, row: []const []const u8, col_widths: []const usize, sep: u21) !void {
        var buf: [4]u8 = undefined;

        // Compute the number of lines this row needs.
        var row_height: usize = 1;
        for (row, 0..) |cell, i| {
            const w = col_widths[i];
            const dlen = displayLen(cell);
            const lines = if (w > 0) (dlen + w - 1) / w else 1;
            if (lines > row_height) row_height = lines;
        }

        for (0..row_height) |line_idx| {
            if (self.border_style != .none) {
                const sep_len = std.unicode.utf8Encode(sep, &buf) catch unreachable;
                _ = try writer.write(buf[0..sep_len]);
            }

            for (row, 0..) |cell, i| {
                for (0..self.padding) |_| _ = try writer.write(" ");

                const w = col_widths[i];
                const real_count = if (self.num_visible_cols > 0) self.num_visible_cols else self.columns.len;
                const alignment: Alignment = if (i < real_count) self.columns[i].alignment else .center;

                // Find the slice for this line.
                const start_byte = displayColsToBytes(cell, line_idx * w);
                const chunk_bytes = displayColsToBytes(cell[start_byte..], w);
                const chunk = cell[start_byte .. start_byte + chunk_bytes];
                const chunk_dlen = displayLen(chunk);

                try self.renderCell(writer, chunk, chunk_dlen, w, alignment);

                for (0..self.padding) |_| _ = try writer.write(" ");

                if (self.border_style != .none) {
                    const sep_len = std.unicode.utf8Encode(sep, &buf) catch unreachable;
                    _ = try writer.write(buf[0..sep_len]);
                } else if (i < row.len - 1) {
                    _ = try writer.write(" ");
                }
            }
            _ = try writer.write("\n");
        }
    }

    fn renderCell(self: *Table, writer: anytype, text: []const u8, text_display_len: usize, width: usize, alignment: Alignment) !void {
        _ = self;
        const padding_needed = width - text_display_len;

        switch (alignment) {
            .left => {
                _ = try writer.write(text);
                for (0..padding_needed) |_| {
                    _ = try writer.write(" ");
                }
            },
            .right => {
                for (0..padding_needed) |_| {
                    _ = try writer.write(" ");
                }
                _ = try writer.write(text);
            },
            .center => {
                const left_pad = padding_needed / 2;
                const right_pad = padding_needed - left_pad;
                for (0..left_pad) |_| {
                    _ = try writer.write(" ");
                }
                _ = try writer.write(text);
                for (0..right_pad) |_| {
                    _ = try writer.write(" ");
                }
            },
        }
    }
};

/// Returns the number of display columns (Unicode codepoints) in a UTF-8 string.
/// Treats each codepoint as 1 display column (good enough for typical CSV content).
fn displayLen(s: []const u8) usize {
    var len: usize = 0;
    var i: usize = 0;
    while (i < s.len) {
        const b = s[i];
        if (b < 0x80) {
            i += 1;
        } else if (b < 0xE0) {
            i += 2;
        } else if (b < 0xF0) {
            i += 3;
        } else {
            i += 4;
        }
        len += 1;
    }
    return len;
}

/// Returns the byte offset in `s` after `n` display columns.
fn displayColsToBytes(s: []const u8, n: usize) usize {
    var cols: usize = 0;
    var i: usize = 0;
    while (i < s.len and cols < n) {
        const b = s[i];
        if (b < 0x80) {
            i += 1;
        } else if (b < 0xE0) {
            i += 2;
        } else if (b < 0xF0) {
            i += 3;
        } else {
            i += 4;
        }
        cols += 1;
    }
    return i;
}

// Convenience function for quick table creation
pub fn render(allocator: Allocator, columns: []const Column, rows: []const []const []const u8, writer: anytype) !void {
    var table = Table.init(allocator, columns);
    defer table.deinit();

    for (rows) |row| {
        try table.addRow(row);
    }

    try table.render(writer);
}

test "basic table creation" {
    const allocator = std.testing.allocator;

    const columns = [_]Column{
        .{ .name = "Name" },
        .{ .name = "Age" },
        .{ .name = "City" },
    };

    var table = Table.init(allocator, &columns);
    defer table.deinit();

    try table.addRow(&[_][]const u8{ "Alice", "30", "NYC" });
    try table.addRow(&[_][]const u8{ "Bob", "25", "LA" });

    var buffer = std.ArrayList(u8){};
    defer buffer.deinit(allocator);

    try table.render(buffer.writer(allocator));
    try std.testing.expect(buffer.items.len > 0);
}

test "alignment" {
    const allocator = std.testing.allocator;

    const columns = [_]Column{
        .{ .name = "Left", .alignment = .left },
        .{ .name = "Center", .alignment = .center },
        .{ .name = "Right", .alignment = .right },
    };

    var table = Table.init(allocator, &columns);
    defer table.deinit();

    try table.addRow(&[_][]const u8{ "A", "B", "C" });

    var buffer = std.ArrayList(u8){};
    defer buffer.deinit(allocator);

    try table.render(buffer.writer(allocator));
    try std.testing.expect(buffer.items.len > 0);
}

test "border styles" {
    const allocator = std.testing.allocator;

    const columns = [_]Column{
        .{ .name = "Col1" },
        .{ .name = "Col2" },
    };

    inline for (std.meta.tags(BorderStyle)) |style| {
        var table = Table.init(allocator, &columns);
        defer table.deinit();
        table.border_style = style;

        try table.addRow(&[_][]const u8{ "A", "B" });

        var buffer = std.ArrayList(u8){};
        defer buffer.deinit(allocator);

        try table.render(buffer.writer(allocator));
        try std.testing.expect(buffer.items.len > 0);
    }
}

test "proportional terminal fit" {
    const allocator = std.testing.allocator;

    const columns = [_]Column{
        .{ .name = "ShortCol" },
        .{ .name = "A Very Long Column Name That Takes Space" },
        .{ .name = "Mid" },
    };

    var table = Table.init(allocator, &columns);
    defer table.deinit();
    table.terminal_width = 60;

    try table.addRow(&[_][]const u8{ "x", "some long content here that overflows", "y" });

    var buffer = std.ArrayList(u8){};
    defer buffer.deinit(allocator);

    try table.render(buffer.writer(allocator));

    // Each rendered line (the border lines) must be <= 60 bytes wide.
    var lines = std.mem.splitScalar(u8, buffer.items, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        // Count display columns (ASCII border lines are 1 byte = 1 col).
        try std.testing.expect(displayLen(line) <= 60);
    }
}
