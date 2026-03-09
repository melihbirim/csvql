const std = @import("std");
const simd = @import("simd.zig");
const Allocator = std.mem.Allocator;

/// Represents a comparison operator
pub const Operator = enum {
    equal,
    not_equal,
    greater,
    greater_equal,
    less,
    less_equal,

    pub fn fromString(s: []const u8) ?Operator {
        if (std.mem.eql(u8, s, "=")) return .equal;
        if (std.mem.eql(u8, s, "!=")) return .not_equal;
        if (std.mem.eql(u8, s, ">")) return .greater;
        if (std.mem.eql(u8, s, ">=")) return .greater_equal;
        if (std.mem.eql(u8, s, "<")) return .less;
        if (std.mem.eql(u8, s, "<=")) return .less_equal;
        return null;
    }
};

/// Represents a WHERE clause expression
pub const Expression = union(enum) {
    comparison: Comparison,
    binary: *BinaryExpr,
    unary: *UnaryExpr,

    pub fn deinit(self: Expression, allocator: Allocator) void {
        switch (self) {
            .comparison => |c| c.deinit(allocator),
            .binary => |b| {
                b.left.deinit(allocator);
                b.right.deinit(allocator);
                allocator.destroy(b);
            },
            .unary => |u| {
                u.expr.deinit(allocator);
                allocator.destroy(u);
            },
        }
    }
};

/// Represents a comparison in WHERE clause
pub const Comparison = struct {
    column: []u8,
    operator: Operator,
    value: []u8,
    numeric_value: ?f64,

    pub fn deinit(self: Comparison, allocator: Allocator) void {
        allocator.free(self.column);
        allocator.free(self.value);
    }
};

/// Binary expression (AND, OR)
pub const BinaryExpr = struct {
    op: enum { @"and", @"or" },
    left: Expression,
    right: Expression,
};

/// Unary expression (NOT)
pub const UnaryExpr = struct {
    expr: Expression,
};

/// Represents a parsed SQL query
pub const SortOrder = enum {
    asc,
    desc,
};

pub const OrderBy = struct {
    column: []u8,
    order: SortOrder,

    pub fn deinit(self: *OrderBy, allocator: Allocator) void {
        allocator.free(self.column);
    }
};

pub const Query = struct {
    columns: [][]u8,
    all_columns: bool,
    distinct: bool,
    file_path: []u8,
    where_expr: ?Expression,
    group_by: [][]u8,
    limit: i32,
    order_by: ?OrderBy,
    allocator: Allocator,

    pub fn deinit(self: *Query) void {
        for (self.columns) |col| {
            self.allocator.free(col);
        }
        self.allocator.free(self.columns);
        self.allocator.free(self.file_path);

        if (self.where_expr) |expr| {
            expr.deinit(self.allocator);
        }

        for (self.group_by) |col| {
            self.allocator.free(col);
        }
        self.allocator.free(self.group_by);

        if (self.order_by) |*ob| {
            ob.deinit(self.allocator);
        }
    }
};

/// Parse a SQL query string
pub fn parse(allocator: Allocator, input: []const u8) !Query {
    // FIXED: Use undefined instead of static slices for initialization
    // These will all be properly allocated before the function returns
    var query = Query{
        .columns = undefined,
        .all_columns = false,
        .distinct = false,
        .file_path = undefined,
        .where_expr = null,
        .group_by = undefined,
        .limit = -1,
        .order_by = null,
        .allocator = allocator,
    };

    // This is a simplified parser - full implementation would use proper regex or parser combinator
    // For now, we'll do basic string parsing

    var trimmed = std.mem.trim(u8, input, &std.ascii.whitespace);

    // Extract SELECT clause
    const select_idx = std.ascii.indexOfIgnoreCase(trimmed, "SELECT") orelse return error.InvalidQuery;
    const from_idx = std.ascii.indexOfIgnoreCase(trimmed, "FROM") orelse return error.InvalidQuery;

    var columns_part = std.mem.trim(u8, trimmed[select_idx + 6 .. from_idx], &std.ascii.whitespace);

    // Detect DISTINCT keyword after SELECT
    if (columns_part.len >= 8 and std.ascii.eqlIgnoreCase(columns_part[0..8], "DISTINCT")) {
        const after = columns_part[8..];
        // Must be followed by whitespace or end of string
        if (after.len == 0 or std.ascii.isWhitespace(after[0])) {
            query.distinct = true;
            columns_part = std.mem.trim(u8, after, &std.ascii.whitespace);
        }
    }

    // Check for SELECT *
    if (std.mem.eql(u8, columns_part, "*")) {
        query.all_columns = true;
        // FIXED: Always allocate empty slice instead of using static slice
        query.columns = try allocator.alloc([]u8, 0);
    } else {
        // Parse column list
        var col_list = std.ArrayList([]u8){};
        defer col_list.deinit(allocator);

        var col_iter = std.mem.splitSequence(u8, columns_part, ",");
        while (col_iter.next()) |col| {
            const trimmed_col = std.mem.trim(u8, col, &std.ascii.whitespace);
            if (trimmed_col.len > 0) {
                try col_list.append(allocator, try allocator.dupe(u8, trimmed_col));
            }
        }
        query.columns = try col_list.toOwnedSlice(allocator);
    }

    // Extract file path from FROM clause
    var rest = trimmed[from_idx + 4 ..];
    const where_idx = std.ascii.indexOfIgnoreCase(rest, "WHERE");
    const group_by_idx = std.ascii.indexOfIgnoreCase(rest, "GROUP BY");
    const order_by_idx = std.ascii.indexOfIgnoreCase(rest, "ORDER BY");
    const limit_idx = std.ascii.indexOfIgnoreCase(rest, "LIMIT");

    var file_end = rest.len;
    if (where_idx) |idx| file_end = @min(file_end, idx);
    if (group_by_idx) |idx| file_end = @min(file_end, idx);
    if (order_by_idx) |idx| file_end = @min(file_end, idx);
    if (limit_idx) |idx| file_end = @min(file_end, idx);

    const file_part = std.mem.trim(u8, rest[0..file_end], &std.ascii.whitespace);
    query.file_path = try allocator.dupe(u8, trimQuotes(file_part));

    // Parse WHERE clause if present
    if (where_idx) |idx| {
        var where_part = rest[idx + 5 ..];
        if (group_by_idx) |gidx| {
            where_part = where_part[0..@min(where_part.len, gidx - idx - 5)];
        } else if (order_by_idx) |oidx| {
            where_part = where_part[0..@min(where_part.len, oidx - idx - 5)];
        } else if (limit_idx) |lidx| {
            where_part = where_part[0..@min(where_part.len, lidx - idx - 5)];
        }
        where_part = std.mem.trim(u8, where_part, &std.ascii.whitespace);
        query.where_expr = try parseExpression(allocator, where_part);
    }

    // Parse GROUP BY clause if present
    if (group_by_idx) |idx| {
        var group_by_part = rest[idx + 8 ..];
        if (order_by_idx) |oidx| {
            group_by_part = group_by_part[0..@min(group_by_part.len, oidx - idx - 8)];
        } else if (limit_idx) |lidx| {
            group_by_part = group_by_part[0..@min(group_by_part.len, lidx - idx - 8)];
        }
        group_by_part = std.mem.trim(u8, group_by_part, &std.ascii.whitespace);

        var group_list = std.ArrayList([]u8){};
        defer group_list.deinit(allocator);

        var group_iter = std.mem.splitSequence(u8, group_by_part, ",");
        while (group_iter.next()) |col| {
            const trimmed_col = std.mem.trim(u8, col, &std.ascii.whitespace);
            if (trimmed_col.len > 0) {
                try group_list.append(allocator, try allocator.dupe(u8, trimmed_col));
            }
        }
        query.group_by = try group_list.toOwnedSlice(allocator);
    } else {
        // FIXED: Always allocate empty slice instead of using static slice
        query.group_by = try allocator.alloc([]u8, 0);
    }

    // Parse ORDER BY clause if present
    if (order_by_idx) |idx| {
        var order_by_part = rest[idx + 8 ..];
        if (limit_idx) |lidx| {
            order_by_part = order_by_part[0..@min(order_by_part.len, lidx - idx - 8)];
        }
        order_by_part = std.mem.trim(u8, order_by_part, &std.ascii.whitespace);

        // Parse column and direction (ASC/DESC)
        const asc_idx = std.ascii.indexOfIgnoreCase(order_by_part, " ASC");
        const desc_idx = std.ascii.indexOfIgnoreCase(order_by_part, " DESC");

        var column_part: []const u8 = undefined;
        var order: SortOrder = .asc; // Default to ascending

        if (desc_idx) |didx| {
            column_part = std.mem.trim(u8, order_by_part[0..didx], &std.ascii.whitespace);
            order = .desc;
        } else if (asc_idx) |aidx| {
            column_part = std.mem.trim(u8, order_by_part[0..aidx], &std.ascii.whitespace);
            order = .asc;
        } else {
            column_part = order_by_part;
        }

        // Lowercase the column name for case-insensitive matching
        const column_lower = try allocator.alloc(u8, column_part.len);
        _ = std.ascii.lowerString(column_lower, column_part);

        query.order_by = OrderBy{
            .column = column_lower,
            .order = order,
        };
    }

    // Parse LIMIT clause if present
    if (limit_idx) |idx| {
        const limit_part = std.mem.trim(u8, rest[idx + 5 ..], &std.ascii.whitespace);
        query.limit = try std.fmt.parseInt(i32, limit_part, 10);
    }

    return query;
}

fn parseExpression(allocator: Allocator, input: []const u8) !Expression {
    // Simplified expression parser - would need full implementation for complex cases
    const trimmed = std.mem.trim(u8, input, &std.ascii.whitespace);

    // Check for operators (simple case - no parentheses)
    if (std.mem.indexOf(u8, trimmed, ">=")) |idx| {
        return parseComparison(allocator, trimmed, ">=", idx);
    }
    if (std.mem.indexOf(u8, trimmed, "<=")) |idx| {
        return parseComparison(allocator, trimmed, "<=", idx);
    }
    if (std.mem.indexOf(u8, trimmed, "!=")) |idx| {
        return parseComparison(allocator, trimmed, "!=", idx);
    }
    if (std.mem.indexOf(u8, trimmed, "=")) |idx| {
        return parseComparison(allocator, trimmed, "=", idx);
    }
    if (std.mem.indexOf(u8, trimmed, ">")) |idx| {
        return parseComparison(allocator, trimmed, ">", idx);
    }
    if (std.mem.indexOf(u8, trimmed, "<")) |idx| {
        return parseComparison(allocator, trimmed, "<", idx);
    }

    return error.InvalidExpression;
}

fn parseComparison(allocator: Allocator, input: []const u8, op_str: []const u8, op_idx: usize) !Expression {
    const column_part = std.mem.trim(u8, input[0..op_idx], &std.ascii.whitespace);
    const value_part = std.mem.trim(u8, input[op_idx + op_str.len ..], &std.ascii.whitespace);

    const operator = Operator.fromString(op_str) orelse return error.InvalidOperator;

    const value_clean = trimQuotes(value_part);
    const numeric_value = std.fmt.parseFloat(f64, value_clean) catch null;

    // FIXED: Normalize column name to lowercase for case-insensitive matching
    const column_lower = try allocator.alloc(u8, column_part.len);
    _ = std.ascii.lowerString(column_lower, column_part);

    return Expression{
        .comparison = Comparison{
            .column = column_lower, // Use lowercased version
            .operator = operator,
            .value = try allocator.dupe(u8, value_clean),
            .numeric_value = numeric_value,
        },
    };
}

fn trimQuotes(input: []const u8) []const u8 {
    if (input.len >= 2) {
        if ((input[0] == '\'' and input[input.len - 1] == '\'') or
            (input[0] == '"' and input[input.len - 1] == '"'))
        {
            return input[1 .. input.len - 1];
        }
    }
    return input;
}

/// Evaluate an expression against a row
pub fn evaluate(expr: Expression, row: std.StringHashMap([]const u8)) bool {
    switch (expr) {
        .comparison => |comp| {
            const value = row.get(comp.column) orelse return false;
            return compareValues(comp, value);
        },
        .binary => |bin| {
            const left_result = evaluate(bin.left, row);
            const right_result = evaluate(bin.right, row);
            return switch (bin.op) {
                .@"and" => left_result and right_result,
                .@"or" => left_result or right_result,
            };
        },
        .unary => |un| {
            return !evaluate(un.expr, row);
        },
    }
}

fn compareValues(comp: Comparison, candidate: []const u8) bool {
    if (comp.numeric_value) |expected| {
        // Try SIMD fast integer parsing first
        if (simd.parseIntFast(candidate)) |candidate_int| {
            const candidate_num: f64 = @floatFromInt(candidate_int);
            return switch (comp.operator) {
                .equal => candidate_num == expected,
                .not_equal => candidate_num != expected,
                .greater => candidate_num > expected,
                .greater_equal => candidate_num >= expected,
                .less => candidate_num < expected,
                .less_equal => candidate_num <= expected,
            };
        } else |_| {
            // Fall back to standard float parsing for decimals or parse errors
            const candidate_num = std.fmt.parseFloat(f64, candidate) catch return false;
            return switch (comp.operator) {
                .equal => candidate_num == expected,
                .not_equal => candidate_num != expected,
                .greater => candidate_num > expected,
                .greater_equal => candidate_num >= expected,
                .less => candidate_num < expected,
                .less_equal => candidate_num <= expected,
            };
        }
    }

    // Use SIMD for string equality checks
    if (comp.operator == .equal) {
        return simd.stringsEqualFast(candidate, comp.value);
    } else if (comp.operator == .not_equal) {
        return !simd.stringsEqualFast(candidate, comp.value);
    }

    // Fall back to standard comparison for ordered operators
    const cmp = std.mem.order(u8, candidate, comp.value);
    return switch (comp.operator) {
        .equal => cmp == .eq,
        .not_equal => cmp != .eq,
        .greater => cmp == .gt,
        .greater_equal => cmp == .gt or cmp == .eq,
        .less => cmp == .lt,
        .less_equal => cmp == .lt or cmp == .eq,
    };
}

test "parse simple query" {
    const allocator = std.testing.allocator;

    var query = try parse(allocator, "SELECT name, age FROM 'data.csv' WHERE age > 25 LIMIT 10");
    defer query.deinit();

    try std.testing.expect(!query.all_columns);
    try std.testing.expectEqual(@as(usize, 2), query.columns.len);
    try std.testing.expectEqualStrings("name", query.columns[0]);
    try std.testing.expectEqualStrings("age", query.columns[1]);
    try std.testing.expectEqualStrings("data.csv", query.file_path);
    try std.testing.expectEqual(@as(i32, 10), query.limit);
}
