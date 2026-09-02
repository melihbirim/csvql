// WHERE-expression grammar tests: written before the tokenizer/recursive-
// descent rewrite of parser.zig's expression parsing (parseExpression and
// friends). These are the acceptance criteria for that rewrite and the
// permanent regression tests for #154 (BETWEEN...AND swallowing a
// trailing OR) and #155 (a subquery's own LIKE/ILIKE/IS NULL/IN mistaken
// for the outer predicate's operator), both found by bench/query_fuzz.sh.
//
// Structural assertions walk the parsed Expression tree directly rather
// than evaluating rows — the bug class here is entirely about which node
// the parser builds, not about evaluation logic (already covered
// elsewhere).
const std = @import("std");
const parser = @import("parser");

fn expectComparisonColumn(expr: parser.Expression, expected_col: []const u8) !void {
    switch (expr) {
        .comparison => |c| try std.testing.expectEqualStrings(expected_col, c.column),
        else => return error.NotAComparison,
    }
}

// ── #154: BETWEEN...AND combined with a trailing OR/AND ────────────────

test "BETWEEN followed by OR splits at top level, not inside BETWEEN's own AND" {
    const allocator = std.testing.allocator;
    var query = try parser.parse(allocator, "SELECT id FROM 'f.csv' WHERE id BETWEEN 10 AND 20 OR city = 'Austin'");
    defer query.deinit();

    const expr = query.where_expr.?;
    try std.testing.expect(expr == .binary);
    try std.testing.expectEqual(.@"or", expr.binary.op);
    try std.testing.expect(expr.binary.left == .comparison);
    try std.testing.expectEqual(.between, expr.binary.left.comparison.operator);
    try std.testing.expectEqualStrings("10", expr.binary.left.comparison.value);
    try std.testing.expectEqualStrings("20", expr.binary.left.comparison.between_high.?);
    try expectComparisonColumn(expr.binary.right, "city");
}

test "BETWEEN followed by AND then another predicate splits at the second AND" {
    const allocator = std.testing.allocator;
    var query = try parser.parse(allocator, "SELECT id FROM 'f.csv' WHERE id BETWEEN 10 AND 20 AND city = 'Austin'");
    defer query.deinit();

    const expr = query.where_expr.?;
    try std.testing.expect(expr == .binary);
    try std.testing.expectEqual(.@"and", expr.binary.op);
    try std.testing.expectEqual(.between, expr.binary.left.comparison.operator);
    try expectComparisonColumn(expr.binary.right, "city");
}

test "two BETWEEN clauses joined by AND both parse correctly" {
    const allocator = std.testing.allocator;
    var query = try parser.parse(allocator, "SELECT id FROM 'f.csv' WHERE id BETWEEN 1 AND 5 AND age BETWEEN 20 AND 30");
    defer query.deinit();

    const expr = query.where_expr.?;
    try std.testing.expect(expr == .binary);
    try std.testing.expectEqual(.@"and", expr.binary.op);
    try std.testing.expectEqual(.between, expr.binary.left.comparison.operator);
    try std.testing.expectEqualStrings("id", expr.binary.left.comparison.column);
    try std.testing.expectEqual(.between, expr.binary.right.comparison.operator);
    try std.testing.expectEqualStrings("age", expr.binary.right.comparison.column);
}

test "BETWEEN wrapped in NOT still parses its own AND correctly" {
    const allocator = std.testing.allocator;
    var query = try parser.parse(allocator, "SELECT id FROM 'f.csv' WHERE NOT (id BETWEEN 10 AND 20)");
    defer query.deinit();

    const expr = query.where_expr.?;
    try std.testing.expect(expr == .unary);
    try std.testing.expectEqual(.between, expr.unary.expr.comparison.operator);
}

// ── #155: a subquery's own predicate keywords must not leak to the outer
//    IN/NOT IN detection ─────────────────────────────────────────────────

test "IN subquery whose WHERE uses LIKE parses the outer clause as IN, not LIKE" {
    const allocator = std.testing.allocator;
    var query = try parser.parse(allocator, "SELECT id FROM 'f.csv' WHERE id IN (SELECT id FROM 'g.csv' WHERE department LIKE 'Op%')");
    defer query.deinit();

    const expr = query.where_expr.?;
    try std.testing.expect(expr == .comparison);
    try std.testing.expectEqualStrings("id", expr.comparison.column);
    try std.testing.expect(expr.comparison.in_subquery_sql != null);
    try std.testing.expect(std.mem.indexOf(u8, expr.comparison.in_subquery_sql.?, "LIKE") != null);
}

test "IN subquery whose WHERE uses ILIKE parses the outer clause as IN" {
    const allocator = std.testing.allocator;
    var query = try parser.parse(allocator, "SELECT id FROM 'f.csv' WHERE id IN (SELECT id FROM 'g.csv' WHERE department ILIKE 'op%')");
    defer query.deinit();

    const expr = query.where_expr.?;
    try std.testing.expect(expr == .comparison);
    try std.testing.expect(expr.comparison.in_subquery_sql != null);
}

test "NOT IN subquery whose WHERE uses IS NOT NULL parses the outer clause as NOT IN" {
    const allocator = std.testing.allocator;
    var query = try parser.parse(allocator, "SELECT id FROM 'f.csv' WHERE id NOT IN (SELECT id FROM 'g.csv' WHERE department IS NOT NULL)");
    defer query.deinit();

    const expr = query.where_expr.?;
    try std.testing.expect(expr == .comparison);
    try std.testing.expect(expr.comparison.in_negate);
    try std.testing.expect(expr.comparison.in_subquery_sql != null);
}

test "IN subquery whose WHERE itself contains a literal IN list" {
    const allocator = std.testing.allocator;
    var query = try parser.parse(allocator, "SELECT id FROM 'f.csv' WHERE id IN (SELECT id FROM 'g.csv' WHERE city IN ('Austin', 'Denver'))");
    defer query.deinit();

    const expr = query.where_expr.?;
    try std.testing.expect(expr == .comparison);
    try std.testing.expect(expr.comparison.in_subquery_sql != null);
    try std.testing.expect(std.mem.indexOf(u8, expr.comparison.in_subquery_sql.?, "IN (") != null);
}

test "IN subquery combined with an outer AND still splits at the outer AND" {
    const allocator = std.testing.allocator;
    var query = try parser.parse(allocator, "SELECT id FROM 'f.csv' WHERE id IN (SELECT id FROM 'g.csv' WHERE department LIKE 'Op%') AND age > 30");
    defer query.deinit();

    const expr = query.where_expr.?;
    try std.testing.expect(expr == .binary);
    try std.testing.expectEqual(.@"and", expr.binary.op);
    try std.testing.expect(expr.binary.left.comparison.in_subquery_sql != null);
    try expectComparisonColumn(expr.binary.right, "age");
}

// ── General precedence / structure sanity ───────────────────────────────

test "OR has lower precedence than AND" {
    const allocator = std.testing.allocator;
    var query = try parser.parse(allocator, "SELECT id FROM 'f.csv' WHERE a = 1 AND b = 2 OR c = 3");
    defer query.deinit();

    const expr = query.where_expr.?;
    try std.testing.expect(expr == .binary);
    try std.testing.expectEqual(.@"or", expr.binary.op);
    try std.testing.expect(expr.binary.left == .binary);
    try std.testing.expectEqual(.@"and", expr.binary.left.binary.op);
    try expectComparisonColumn(expr.binary.right, "c");
}

test "explicit parens override default precedence" {
    const allocator = std.testing.allocator;
    var query = try parser.parse(allocator, "SELECT id FROM 'f.csv' WHERE (a = 1 OR b = 2) AND c = 3");
    defer query.deinit();

    const expr = query.where_expr.?;
    try std.testing.expect(expr == .binary);
    try std.testing.expectEqual(.@"and", expr.binary.op);
    try std.testing.expect(expr.binary.left == .binary);
    try std.testing.expectEqual(.@"or", expr.binary.left.binary.op);
}

test "three-way AND chain parses all three predicates" {
    const allocator = std.testing.allocator;
    var query = try parser.parse(allocator, "SELECT id FROM 'f.csv' WHERE a = 1 AND b = 2 AND c = 3");
    defer query.deinit();

    // AND is associative, so either grouping is a valid parse; this pins
    // down the first-split-point shape the current recursive descent
    // produces (left = first predicate, right = the rest, itself an AND).
    const expr = query.where_expr.?;
    try std.testing.expect(expr == .binary);
    try std.testing.expectEqual(.@"and", expr.binary.op);
    try expectComparisonColumn(expr.binary.left, "a");
    try std.testing.expect(expr.binary.right == .binary);
    try expectComparisonColumn(expr.binary.right.binary.left, "b");
    try expectComparisonColumn(expr.binary.right.binary.right, "c");
}

test "modulo comparison combined with AND still splits at the AND" {
    const allocator = std.testing.allocator;
    var query = try parser.parse(allocator, "SELECT id FROM 'f.csv' WHERE id % 2 = 0 AND city = 'Austin'");
    defer query.deinit();

    const expr = query.where_expr.?;
    try std.testing.expect(expr == .binary);
    try std.testing.expectEqual(.@"and", expr.binary.op);
    try std.testing.expect(expr.binary.left.comparison.mod_divisor != null);
    try expectComparisonColumn(expr.binary.right, "city");
}

test "scalar function comparison combined with OR still splits at the OR" {
    const allocator = std.testing.allocator;
    var query = try parser.parse(allocator, "SELECT id FROM 'f.csv' WHERE DATEDIFF('day', start_col, end_col) > 5 OR city = 'Austin'");
    defer query.deinit();

    const expr = query.where_expr.?;
    try std.testing.expect(expr == .binary);
    try std.testing.expectEqual(.@"or", expr.binary.op);
    try std.testing.expect(expr.binary.left == .scalar_comparison);
    try expectComparisonColumn(expr.binary.right, "city");
}

test "IS NULL and IS NOT NULL combined with AND/OR" {
    const allocator = std.testing.allocator;
    var query = try parser.parse(allocator, "SELECT id FROM 'f.csv' WHERE age IS NULL OR city IS NOT NULL");
    defer query.deinit();

    const expr = query.where_expr.?;
    try std.testing.expect(expr == .binary);
    try std.testing.expectEqual(.@"or", expr.binary.op);
    try std.testing.expectEqual(.is_null, expr.binary.left.comparison.operator);
    try std.testing.expectEqual(.is_not_null, expr.binary.right.comparison.operator);
}

test "NOT IN literal list does not get misparsed as IN with NOT in the column name" {
    const allocator = std.testing.allocator;
    var query = try parser.parse(allocator, "SELECT id FROM 'f.csv' WHERE department NOT IN ('Sales', 'HR')");
    defer query.deinit();

    const expr = query.where_expr.?;
    try std.testing.expect(expr == .comparison);
    try std.testing.expectEqualStrings("department", expr.comparison.column);
    try std.testing.expect(expr.comparison.in_negate);
    try std.testing.expectEqual(@as(usize, 2), expr.comparison.in_values.?.len);
}

test "ILIKE combined with AND still splits at the AND" {
    const allocator = std.testing.allocator;
    var query = try parser.parse(allocator, "SELECT id FROM 'f.csv' WHERE name ILIKE '%smith%' AND age > 30");
    defer query.deinit();

    const expr = query.where_expr.?;
    try std.testing.expect(expr == .binary);
    try std.testing.expectEqual(.@"and", expr.binary.op);
    try std.testing.expectEqual(.ilike, expr.binary.left.comparison.operator);
    try expectComparisonColumn(expr.binary.right, "age");
}
