const std = @import("std");
const Allocator = std.mem.Allocator;

/// Aggregate function types
pub const AggregateType = enum {
    count,
    sum,
    avg,
    min,
    max,
};

/// Represents an aggregate function in SELECT
pub const AggregateFunc = struct {
    func_type: AggregateType,
    column: ?[]const u8, // null for COUNT(*)
    alias: []const u8, // Original expression

    pub fn deinit(self: AggregateFunc, allocator: Allocator) void {
        if (self.column) |col| {
            allocator.free(col);
        }
        allocator.free(self.alias);
    }
};

/// Accumulates aggregate values for a group
pub const Aggregator = struct {
    row_count: i64,
    sums: std.AutoHashMap(usize, f64),
    counts: std.AutoHashMap(usize, i64),
    mins: std.AutoHashMap(usize, f64),
    maxs: std.AutoHashMap(usize, f64),
    has_min: std.AutoHashMap(usize, bool),
    has_max: std.AutoHashMap(usize, bool),
    allocator: Allocator,

    pub fn init(allocator: Allocator) Aggregator {
        return Aggregator{
            .row_count = 0,
            .sums = std.AutoHashMap(usize, f64).init(allocator),
            .counts = std.AutoHashMap(usize, i64).init(allocator),
            .mins = std.AutoHashMap(usize, f64).init(allocator),
            .maxs = std.AutoHashMap(usize, f64).init(allocator),
            .has_min = std.AutoHashMap(usize, bool).init(allocator),
            .has_max = std.AutoHashMap(usize, bool).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Aggregator) void {
        self.sums.deinit();
        self.counts.deinit();
        self.mins.deinit();
        self.maxs.deinit();
        self.has_min.deinit();
        self.has_max.deinit();
    }

    /// Add a row to this aggregator
    pub fn addRow(
        self: *Aggregator,
        agg_funcs: []const AggregateFunc,
        agg_indices: []const ?usize,
        row: []const []const u8,
    ) !void {
        self.row_count += 1;

        for (agg_funcs, 0..) |func, i| {
            const idx = agg_indices[i];

            switch (func.func_type) {
                .count => {
                    // COUNT(*) handled by row_count
                    // COUNT(column) would be similar
                },
                .sum, .avg => {
                    if (idx) |col_idx| {
                        if (col_idx < row.len) {
                            if (std.fmt.parseFloat(f64, row[col_idx])) |val| {
                                const current_sum = self.sums.get(i) orelse 0.0;
                                try self.sums.put(i, current_sum + val);

                                const current_count = self.counts.get(i) orelse 0;
                                try self.counts.put(i, current_count + 1);
                            } else |_| {
                                // Ignore non-numeric values
                            }
                        }
                    }
                },
                .min => {
                    if (idx) |col_idx| {
                        if (col_idx < row.len) {
                            if (std.fmt.parseFloat(f64, row[col_idx])) |val| {
                                const has = self.has_min.get(i) orelse false;
                                if (!has) {
                                    try self.mins.put(i, val);
                                    try self.has_min.put(i, true);
                                } else {
                                    const current = self.mins.get(i).?;
                                    if (val < current) {
                                        try self.mins.put(i, val);
                                    }
                                }
                            } else |_| {}
                        }
                    }
                },
                .max => {
                    if (idx) |col_idx| {
                        if (col_idx < row.len) {
                            if (std.fmt.parseFloat(f64, row[col_idx])) |val| {
                                const has = self.has_max.get(i) orelse false;
                                if (!has) {
                                    try self.maxs.put(i, val);
                                    try self.has_max.put(i, true);
                                } else {
                                    const current = self.maxs.get(i).?;
                                    if (val > current) {
                                        try self.maxs.put(i, val);
                                    }
                                }
                            } else |_| {}
                        }
                    }
                },
            }
        }
    }

    /// Get the result for an aggregate function
    pub fn getResult(self: *Aggregator, func: AggregateFunc, index: usize, allocator: Allocator) ![]u8 {
        switch (func.func_type) {
            .count => {
                return try std.fmt.allocPrint(allocator, "{d}", .{self.row_count});
            },
            .sum => {
                const sum = self.sums.get(index) orelse 0.0;
                // Print as integer when the value has no fractional part
                if (@abs(sum - @trunc(sum)) < 1e-9) {
                    return try std.fmt.allocPrint(allocator, "{d:.0}", .{sum});
                }
                return try std.fmt.allocPrint(allocator, "{d:.2}", .{sum});
            },
            .avg => {
                const sum = self.sums.get(index) orelse 0.0;
                const count = self.counts.get(index) orelse 0;
                if (count > 0) {
                    const avg = sum / @as(f64, @floatFromInt(count));
                    // Use full precision (shortest round-trip representation)
                    return try std.fmt.allocPrint(allocator, "{d}", .{avg});
                }
                return try allocator.dupe(u8, "0");
            },
            .min => {
                const has = self.has_min.get(index) orelse false;
                if (has) {
                    const val = self.mins.get(index).?;
                    if (@abs(val - @trunc(val)) < 1e-9) {
                        return try std.fmt.allocPrint(allocator, "{d:.0}", .{val});
                    }
                    return try std.fmt.allocPrint(allocator, "{d:.2}", .{val});
                }
                return try allocator.dupe(u8, "");
            },
            .max => {
                const has = self.has_max.get(index) orelse false;
                if (has) {
                    const val = self.maxs.get(index).?;
                    if (@abs(val - @trunc(val)) < 1e-9) {
                        return try std.fmt.allocPrint(allocator, "{d:.0}", .{val});
                    }
                    return try std.fmt.allocPrint(allocator, "{d:.2}", .{val});
                }
                return try allocator.dupe(u8, "");
            },
        }
    }
};

/// Parse an aggregate function expression
pub fn parseAggregateFunc(allocator: Allocator, expr: []const u8) !?AggregateFunc {
    const trimmed = std.mem.trim(u8, expr, &std.ascii.whitespace);

    // Check for aggregate function pattern: FUNC(column)
    const open_paren = std.mem.indexOf(u8, trimmed, "(") orelse return null;
    const close_paren = std.mem.lastIndexOf(u8, trimmed, ")") orelse return null;

    if (close_paren <= open_paren) return null;

    const func_name = std.mem.trim(u8, trimmed[0..open_paren], &std.ascii.whitespace);
    const column_part = std.mem.trim(u8, trimmed[open_paren + 1 .. close_paren], &std.ascii.whitespace);

    // Determine function type
    var func_type: AggregateType = undefined;
    const func_lower = try allocator.alloc(u8, func_name.len);
    defer allocator.free(func_lower);
    _ = std.ascii.lowerString(func_lower, func_name);

    if (std.mem.eql(u8, func_lower, "count")) {
        func_type = .count;
    } else if (std.mem.eql(u8, func_lower, "sum")) {
        func_type = .sum;
    } else if (std.mem.eql(u8, func_lower, "avg")) {
        func_type = .avg;
    } else if (std.mem.eql(u8, func_lower, "min")) {
        func_type = .min;
    } else if (std.mem.eql(u8, func_lower, "max")) {
        func_type = .max;
    } else {
        return null;
    }

    // Handle column (or * for COUNT(*))
    const column = if (std.mem.eql(u8, column_part, "*"))
        null
    else
        try allocator.dupe(u8, column_part);

    return AggregateFunc{
        .func_type = func_type,
        .column = column,
        .alias = try allocator.dupe(u8, trimmed),
    };
}

test "parse aggregate functions" {
    const allocator = std.testing.allocator;

    // COUNT(*)
    const count_star = try parseAggregateFunc(allocator, "COUNT(*)");
    try std.testing.expect(count_star != null);
    try std.testing.expectEqual(AggregateType.count, count_star.?.func_type);
    try std.testing.expect(count_star.?.column == null);
    count_star.?.deinit(allocator);

    // SUM(amount)
    const sum_func = try parseAggregateFunc(allocator, "SUM(amount)");
    try std.testing.expect(sum_func != null);
    try std.testing.expectEqual(AggregateType.sum, sum_func.?.func_type);
    try std.testing.expectEqualStrings("amount", sum_func.?.column.?);
    sum_func.?.deinit(allocator);

    // Not an aggregate
    const not_agg = try parseAggregateFunc(allocator, "name");
    try std.testing.expect(not_agg == null);
}
