const std = @import("std");
const parser = @import("parser.zig");
const Allocator = std.mem.Allocator;

/// Parse simple query syntax: csvq <file> [columns] [where] [limit] [orderby]
pub fn parseSimple(allocator: Allocator, args: []const []const u8) !parser.Query {
    if (args.len == 0) {
        return error.MissingFile;
    }

    // FIXED: Initialize with undefined, will be properly allocated
    var query = parser.Query{
        .columns = undefined,
        .all_columns = false,
        .distinct = false,
        .file_path = undefined,
        .where_expr = null,
        .having_expr = null,
        .group_by = undefined,
        .limit = 10, // Default limit is 10
        .order_by = null,
        .joins = &.{},
        .allocator = allocator,
    };

    // 1. Parse file path (required)
    query.file_path = try allocator.dupe(u8, args[0]);

    // 2. Parse columns (optional, default: *)
    if (args.len > 1 and args[1].len > 0 and !std.mem.eql(u8, args[1], "*")) {
        query.columns = try parseColumns(allocator, args[1]);
        query.all_columns = false;
    } else {
        query.columns = try allocator.alloc([]u8, 0);
        query.all_columns = true;
    }

    // 3. Parse WHERE clause (optional)
    if (args.len > 2 and args[2].len > 0) {
        query.where_expr = try parseWhere(allocator, args[2]);
    }

    // 4. Parse LIMIT (optional, default: 10)
    if (args.len > 3 and args[3].len > 0) {
        const limit_val = try std.fmt.parseInt(i32, args[3], 10);
        // 0 means unlimited, convert to -1 for engine
        query.limit = if (limit_val == 0) -1 else limit_val;
    }

    // 5. Parse ORDER BY (optional)
    if (args.len > 4 and args[4].len > 0) {
        query.order_by = try parseOrderBy(allocator, args[4]);
    }

    // Initialize empty GROUP BY (not supported in simple syntax)
    query.group_by = try allocator.alloc([]u8, 0);

    return query;
}

/// Parse columns: "id,name,score" -> ["id", "name", "score"]
fn parseColumns(allocator: Allocator, input: []const u8) ![][]u8 {
    var columns = std.ArrayList([]u8){};
    errdefer {
        for (columns.items) |col| allocator.free(col);
        columns.deinit(allocator);
    }

    var iter = std.mem.splitSequence(u8, input, ",");
    while (iter.next()) |part| {
        const trimmed = std.mem.trim(u8, part, &std.ascii.whitespace);
        if (trimmed.len > 0) {
            // Lowercase the column name for case-insensitive matching
            const col_lower = try allocator.alloc(u8, trimmed.len);
            _ = std.ascii.lowerString(col_lower, trimmed);
            try columns.append(allocator, col_lower);
        }
    }

    return columns.toOwnedSlice(allocator);
}

/// Parse WHERE clause: "age>30" or "age>30 AND score>=80"
fn parseWhere(allocator: Allocator, input: []const u8) (Allocator.Error || error{InvalidWhereClause})!parser.Expression {
    // Check for AND/OR operators
    if (std.mem.indexOf(u8, input, " AND ")) |and_pos| {
        return try parseBinaryExpr(allocator, input, and_pos, " AND ", .@"and");
    }
    if (std.mem.indexOf(u8, input, " OR ")) |or_pos| {
        return try parseBinaryExpr(allocator, input, or_pos, " OR ", .@"or");
    }

    // Simple comparison
    return parser.Expression{ .comparison = try parseComparison(allocator, input) };
}

/// Parse binary expression (AND/OR)
fn parseBinaryExpr(allocator: Allocator, input: []const u8, split_pos: usize, op_str: []const u8, op: anytype) (Allocator.Error || error{InvalidWhereClause})!parser.Expression {
    const left_str = input[0..split_pos];
    const right_str = input[split_pos + op_str.len ..];

    const binary = try allocator.create(parser.BinaryExpr);
    binary.* = .{
        .left = try parseWhere(allocator, left_str),
        .right = try parseWhere(allocator, right_str),
        .op = op,
    };

    return parser.Expression{ .binary = binary };
}

/// Parse single comparison: "age>30" or "name=Alice"
fn parseComparison(allocator: Allocator, input: []const u8) (Allocator.Error || error{InvalidWhereClause})!parser.Comparison {
    // Try to find operators (longest first to handle >=, <=, !=)
    const operators = [_][]const u8{ ">=", "<=", "!=", "=", ">", "<" };

    for (operators) |op_str| {
        if (std.mem.indexOf(u8, input, op_str)) |op_pos| {
            const column_part = std.mem.trim(u8, input[0..op_pos], &std.ascii.whitespace);
            const value_part = std.mem.trim(u8, input[op_pos + op_str.len ..], &std.ascii.whitespace);

            if (column_part.len == 0 or value_part.len == 0) {
                continue; // Not a valid comparison, try next operator
            }

            // Lowercase the column name for case-insensitive matching
            const column_lower = try allocator.alloc(u8, column_part.len);
            _ = std.ascii.lowerString(column_lower, column_part);

            const value_copy = try allocator.dupe(u8, value_part);

            // Parse numeric value if possible
            const numeric_value = std.fmt.parseFloat(f64, value_part) catch null;

            return parser.Comparison{
                .column = column_lower,
                .operator = parser.Operator.fromString(op_str).?,
                .value = value_copy,
                .numeric_value = numeric_value,
            };
        }
    }

    return error.InvalidWhereClause;
}

/// Parse ORDER BY: "age" or "age:asc" or "score:desc"
fn parseOrderBy(allocator: Allocator, input: []const u8) !parser.OrderBy {
    if (std.mem.indexOf(u8, input, ":")) |colon_pos| {
        const column_part = input[0..colon_pos];
        const order_part = input[colon_pos + 1 ..];

        // Lowercase the column name
        const column_lower = try allocator.alloc(u8, column_part.len);
        _ = std.ascii.lowerString(column_lower, column_part);

        const order = if (std.mem.eql(u8, order_part, "desc"))
            parser.SortOrder.desc
        else
            parser.SortOrder.asc;

        return parser.OrderBy{
            .column = column_lower,
            .order = order,
        };
    } else {
        // No colon, default to ascending
        const column_lower = try allocator.alloc(u8, input.len);
        _ = std.ascii.lowerString(column_lower, input);

        return parser.OrderBy{
            .column = column_lower,
            .order = .asc,
        };
    }
}
