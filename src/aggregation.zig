const std = @import("std");
const Allocator = std.mem.Allocator;

/// Aggregate function types
pub const AggregateType = enum {
    count,
    count_distinct,
    sum,
    avg,
    min,
    max,
    median,
    variance, // population variance (VAR_POP)
    stddev, // population standard deviation (STDDEV_POP)
    group_concat, // GROUP_CONCAT(col [, 'sep']) — concatenate group values
};

/// Represents an aggregate function in SELECT
pub const AggregateFunc = struct {
    func_type: AggregateType,
    column: ?[]const u8, // null for COUNT(*)
    alias: []const u8, // Original expression
    sep: ?[]const u8 = null, // GROUP_CONCAT separator (allocator-owned); null otherwise

    pub fn deinit(self: AggregateFunc, allocator: Allocator) void {
        if (self.column) |col| {
            allocator.free(col);
        }
        if (self.sep) |s| allocator.free(s);
        allocator.free(self.alias);
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
        // COUNT(DISTINCT col) or COUNT(*) / COUNT(col)
        if (column_part.len >= 9 and std.ascii.eqlIgnoreCase(column_part[0..9], "DISTINCT ")) {
            func_type = .count_distinct;
        } else {
            func_type = .count;
        }
    } else if (std.mem.eql(u8, func_lower, "sum")) {
        func_type = .sum;
    } else if (std.mem.eql(u8, func_lower, "avg")) {
        func_type = .avg;
    } else if (std.mem.eql(u8, func_lower, "min")) {
        func_type = .min;
    } else if (std.mem.eql(u8, func_lower, "max")) {
        func_type = .max;
    } else if (std.mem.eql(u8, func_lower, "median")) {
        func_type = .median;
    } else if (std.mem.eql(u8, func_lower, "variance") or std.mem.eql(u8, func_lower, "var_pop")) {
        func_type = .variance;
    } else if (std.mem.eql(u8, func_lower, "stddev") or std.mem.eql(u8, func_lower, "stddev_pop") or std.mem.eql(u8, func_lower, "std")) {
        func_type = .stddev;
    } else if (std.mem.eql(u8, func_lower, "group_concat") or std.mem.eql(u8, func_lower, "string_agg")) {
        func_type = .group_concat;
    } else {
        return null;
    }

    // Handle column (or * for COUNT(*)); strip DISTINCT prefix if present
    var actual_column_part = column_part;
    if (func_type == .count_distinct) {
        // Strip "DISTINCT " prefix (9 chars) already verified above
        actual_column_part = std.mem.trim(u8, column_part[9..], &std.ascii.whitespace);
    }

    // GROUP_CONCAT(col [, 'sep']): the column is the first arg; the optional second
    // arg is the separator literal (default ",").
    var gc_sep: ?[]const u8 = null;
    if (func_type == .group_concat) {
        if (std.mem.indexOfScalar(u8, column_part, ',')) |c| {
            actual_column_part = std.mem.trim(u8, column_part[0..c], &std.ascii.whitespace);
            const sep_raw = std.mem.trim(u8, column_part[c + 1 ..], &std.ascii.whitespace);
            const sep_str = if (sep_raw.len >= 2 and sep_raw[0] == '\'' and sep_raw[sep_raw.len - 1] == '\'')
                sep_raw[1 .. sep_raw.len - 1]
            else
                sep_raw;
            gc_sep = try allocator.dupe(u8, sep_str);
        } else {
            gc_sep = try allocator.dupe(u8, ",");
        }
    }
    errdefer if (gc_sep) |s| allocator.free(s);

    const column = if (std.mem.eql(u8, actual_column_part, "*"))
        null
    else
        try allocator.dupe(u8, actual_column_part);

    return AggregateFunc{
        .func_type = func_type,
        .column = column,
        .alias = try allocator.dupe(u8, trimmed),
        .sep = gc_sep,
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
