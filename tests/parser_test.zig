const std = @import("std");
const parser = @import("parser");

// TDD Test 1: Query.deinit should not crash on empty allocations (SELECT *)
test "Query deinit with SELECT *" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT * FROM 'test.csv'");
    defer query.deinit();

    try std.testing.expectEqual(@as(usize, 0), query.columns.len);
}

// TDD Test: IN (...) list splitting must be quote-aware — a comma inside a
// quoted value is part of the value, not a list separator (issue #96).
test "IN comparison handles quoted value containing a comma" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT * FROM 'test.csv' WHERE note IN ('a,b', 'c')");
    defer query.deinit();

    const expr = query.where_expr.?;
    const in_values = expr.comparison.in_values.?;
    try std.testing.expectEqual(@as(usize, 2), in_values.len);
    try std.testing.expectEqualStrings("a,b", in_values[0]);
    try std.testing.expectEqualStrings("c", in_values[1]);
}

// TDD Test 2: Query.deinit should not crash when no GROUP BY clause
test "Query deinit without GROUP BY" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT name FROM 'test.csv'");
    defer query.deinit();

    try std.testing.expectEqual(@as(usize, 0), query.group_by.len);
}

// TDD Test 3: Query.deinit with explicit columns and GROUP BY should work
test "Query deinit with columns and GROUP BY" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT name, count FROM 'test.csv' GROUP BY name");
    defer query.deinit();

    try std.testing.expectEqual(@as(usize, 2), query.columns.len);
    try std.testing.expectEqual(@as(usize, 1), query.group_by.len);
}

// TDD Test 4: WHERE with mixed case column should match header (case-insensitive)
test "WHERE clause is case-insensitive for column names" {
    const allocator = std.testing.allocator;

    // Query uses lowercase 'name', but CSV header might be 'Name' or 'NAME'
    var query = try parser.parse(allocator, "SELECT * FROM 'test.csv' WHERE name = 'Alice'");
    defer query.deinit();

    try std.testing.expect(query.where_expr != null);
    if (query.where_expr) |expr| {
        switch (expr) {
            .comparison => |comp| {
                // Column name should be normalized to lowercase
                try std.testing.expectEqualStrings("name", comp.column);
            },
            else => try std.testing.expect(false), // Should be comparison
        }
    }
}

// TDD Test 5: Performance - WHERE evaluation should use direct index lookup
test "WHERE evaluation uses precomputed column index" {
    const allocator = std.testing.allocator;

    // This test just verifies the API exists for fast WHERE evaluation
    // The actual performance benefit is measured in benchmarks, not unit tests
    var query = try parser.parse(allocator, "SELECT * FROM 'test.csv' WHERE age > 30");
    defer query.deinit();

    // Verify we have a WHERE clause with a simple comparison
    try std.testing.expect(query.where_expr != null);
    if (query.where_expr) |expr| {
        switch (expr) {
            .comparison => |comp| {
                // Should have normalized column name
                try std.testing.expectEqualStrings("age", comp.column);
                // Should have numeric value for numeric comparison
                try std.testing.expect(comp.numeric_value != null);
            },
            else => try std.testing.expect(false),
        }
    }
}

// TDD Test 7: WHERE with mixed case column names
test "WHERE with mixed case column" {
    const allocator = std.testing.allocator;

    // Create test CSV with uppercase column name
    const tmp_file = try std.fs.cwd().createFile("test_mixed_case.csv", .{ .read = true, .truncate = true });
    defer {
        tmp_file.close();
        std.fs.cwd().deleteFile("test_mixed_case.csv") catch {};
    }

    try tmp_file.writeAll("Name,Age\nAlice,30\nBob,25\n");
    try tmp_file.seekTo(0);

    // Query uses lowercase 'age' but CSV has 'Age'
    var query = try parser.parse(allocator, "SELECT * FROM 'test_mixed_case.csv' WHERE age > 25");
    defer query.deinit();

    // Verify the query parses correctly with normalized column name
    try std.testing.expectEqualStrings("age", query.where_expr.?.comparison.column);
}

// LIKE operator: matchLike function tests
test "matchLike: prefix wildcard %foo" {
    try std.testing.expect(parser.matchLike("foo", "%foo"));
    try std.testing.expect(parser.matchLike("barfoo", "%foo"));
    try std.testing.expect(!parser.matchLike("foobar", "%foo"));
}

test "matchLike: suffix wildcard foo%" {
    try std.testing.expect(parser.matchLike("foo", "foo%"));
    try std.testing.expect(parser.matchLike("foobar", "foo%"));
    try std.testing.expect(!parser.matchLike("barfoo", "foo%"));
}

test "matchLike: both ends %foo%" {
    try std.testing.expect(parser.matchLike("foo", "%foo%"));
    try std.testing.expect(parser.matchLike("xfoox", "%foo%"));
    try std.testing.expect(parser.matchLike("foo_bar", "%foo%"));
    try std.testing.expect(!parser.matchLike("bar", "%foo%"));
}

test "matchLike: single-char wildcard _" {
    try std.testing.expect(parser.matchLike("abc", "a_c"));
    try std.testing.expect(parser.matchLike("axc", "a_c"));
    try std.testing.expect(!parser.matchLike("ac", "a_c"));
    try std.testing.expect(!parser.matchLike("abbc", "a_c"));
}

test "matchLike: no wildcards (exact match)" {
    try std.testing.expect(parser.matchLike("hello", "hello"));
    try std.testing.expect(!parser.matchLike("hello", "world"));
    try std.testing.expect(!parser.matchLike("hello!", "hello"));
}

test "matchLike: empty pattern and text" {
    try std.testing.expect(parser.matchLike("", ""));
    try std.testing.expect(parser.matchLike("", "%"));
    try std.testing.expect(!parser.matchLike("a", ""));
}

test "matchLike: email pattern" {
    try std.testing.expect(parser.matchLike("user@gmail.com", "%@gmail.com"));
    try std.testing.expect(!parser.matchLike("user@yahoo.com", "%@gmail.com"));
}

// LIKE operator: parser parses LIKE keyword correctly
test "parse LIKE operator" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT * FROM 'users.csv' WHERE email LIKE '%@gmail.com'");
    defer query.deinit();

    try std.testing.expect(query.where_expr != null);
    const comp = query.where_expr.?.comparison;
    try std.testing.expectEqualStrings("email", comp.column);
    try std.testing.expectEqual(parser.Operator.like, comp.operator);
    try std.testing.expectEqualStrings("%@gmail.com", comp.value);
    try std.testing.expect(comp.numeric_value == null);
}

test "parse LIKE operator case-insensitive keyword" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT name FROM 'data.csv' WHERE name like 'John%'");
    defer query.deinit();

    try std.testing.expect(query.where_expr != null);
    const comp = query.where_expr.?.comparison;
    try std.testing.expectEqual(parser.Operator.like, comp.operator);
    try std.testing.expectEqualStrings("John%", comp.value);
}

// --- JOIN parser tests ---

test "parse INNER JOIN with aliases" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(
        allocator,
        "SELECT a.name, b.dept FROM 'employees.csv' a INNER JOIN 'departments.csv' b ON a.dept_id = b.id",
    );
    defer query.deinit();

    try std.testing.expectEqualStrings("employees.csv", query.file_path);
    try std.testing.expectEqual(@as(usize, 1), query.joins.len);
    const j = query.joins[0];
    try std.testing.expectEqualStrings("departments.csv", j.right_file);
    try std.testing.expectEqualStrings("a", j.left_alias);
    try std.testing.expectEqualStrings("b", j.right_alias);
    try std.testing.expectEqualStrings("a.dept_id", j.left_col);
    try std.testing.expectEqualStrings("b.id", j.right_col);
    // Columns were requested as "a.name" and "b.dept"
    try std.testing.expectEqual(@as(usize, 2), query.columns.len);
}

test "parse bare JOIN (no INNER keyword)" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(
        allocator,
        "SELECT * FROM 'left.csv' l JOIN 'right.csv' r ON l.id = r.fk",
    );
    defer query.deinit();

    try std.testing.expectEqualStrings("left.csv", query.file_path);
    try std.testing.expectEqual(@as(usize, 1), query.joins.len);
    const j = query.joins[0];
    try std.testing.expectEqualStrings("right.csv", j.right_file);
    try std.testing.expectEqualStrings("l", j.left_alias);
    try std.testing.expectEqualStrings("r", j.right_alias);
    try std.testing.expectEqualStrings("l.id", j.left_col);
    try std.testing.expectEqualStrings("r.fk", j.right_col);
    try std.testing.expect(query.all_columns);
}

test "parse JOIN with WHERE clause" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(
        allocator,
        "SELECT a.name FROM 'emp.csv' a JOIN 'dept.csv' b ON a.dept_id = b.id WHERE b.name = 'Engineering'",
    );
    defer query.deinit();

    try std.testing.expectEqual(@as(usize, 1), query.joins.len);
    try std.testing.expect(query.where_expr != null);
    const comp = query.where_expr.?.comparison;
    try std.testing.expectEqualStrings("b.name", comp.column);
    try std.testing.expectEqualStrings("Engineering", comp.value);
}

test "parse JOIN with LIMIT" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(
        allocator,
        "SELECT * FROM 'a.csv' a JOIN 'b.csv' b ON a.id = b.id LIMIT 5",
    );
    defer query.deinit();

    try std.testing.expectEqual(@as(usize, 1), query.joins.len);
    try std.testing.expectEqual(@as(i32, 5), query.limit);
}

test "single-file query still works after JOIN parser changes" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT name, age FROM 'people.csv' WHERE age > 30");
    defer query.deinit();

    try std.testing.expectEqual(@as(usize, 0), query.joins.len);
    try std.testing.expectEqualStrings("people.csv", query.file_path);
    try std.testing.expectEqual(@as(usize, 2), query.columns.len);
}

test "parse chained JOIN: three tables" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(
        allocator,
        "SELECT a.name, b.dept_name, c.region FROM 'emp.csv' a " ++
            "JOIN 'dept.csv' b ON a.dept_id = b.id " ++
            "JOIN 'region.csv' c ON b.region_id = c.id",
    );
    defer query.deinit();

    try std.testing.expectEqualStrings("emp.csv", query.file_path);
    try std.testing.expectEqual(@as(usize, 2), query.joins.len);

    const j1 = query.joins[0];
    try std.testing.expectEqualStrings("dept.csv", j1.right_file);
    try std.testing.expectEqualStrings("a", j1.left_alias);
    try std.testing.expectEqualStrings("b", j1.right_alias);
    try std.testing.expectEqualStrings("a.dept_id", j1.left_col);
    try std.testing.expectEqualStrings("b.id", j1.right_col);

    const j2 = query.joins[1];
    try std.testing.expectEqualStrings("region.csv", j2.right_file);
    try std.testing.expectEqualStrings("b", j2.left_alias);
    try std.testing.expectEqualStrings("c", j2.right_alias);
    try std.testing.expectEqualStrings("b.region_id", j2.left_col);
    try std.testing.expectEqualStrings("c.id", j2.right_col);
}

test "parse chained JOIN with WHERE clause" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(
        allocator,
        "SELECT a.name FROM 'emp.csv' a " ++
            "JOIN 'dept.csv' b ON a.dept_id = b.id " ++
            "JOIN 'reg.csv' c ON b.region_id = c.id WHERE c.name = 'West'",
    );
    defer query.deinit();

    try std.testing.expectEqual(@as(usize, 2), query.joins.len);
    try std.testing.expect(query.where_expr != null);
    const comp = query.where_expr.?.comparison;
    try std.testing.expectEqualStrings("c.name", comp.column);
    try std.testing.expectEqualStrings("West", comp.value);
}

test "unsupported join types return UnsupportedJoinType error" {
    const allocator = std.testing.allocator;

    try std.testing.expectError(
        error.UnsupportedJoinType,
        parser.parse(allocator, "SELECT * FROM 'a.csv' a LEFT JOIN 'b.csv' b ON a.id = b.id"),
    );
    try std.testing.expectError(
        error.UnsupportedJoinType,
        parser.parse(allocator, "SELECT * FROM 'a.csv' a RIGHT JOIN 'b.csv' b ON a.id = b.id"),
    );
    try std.testing.expectError(
        error.UnsupportedJoinType,
        parser.parse(allocator, "SELECT * FROM 'a.csv' a FULL OUTER JOIN 'b.csv' b ON a.id = b.id"),
    );
    try std.testing.expectError(
        error.UnsupportedJoinType,
        parser.parse(allocator, "SELECT * FROM 'a.csv' a CROSS JOIN 'b.csv' b ON a.id = b.id"),
    );
}

test "quoted filename containing join keyword is not flagged as UnsupportedJoinType" {
    const allocator = std.testing.allocator;

    // '/tmp/left join.csv' contains "left join" inside quotes — must NOT trigger error.
    var query = try parser.parse(
        allocator,
        "SELECT a.id FROM '/tmp/left join.csv' a INNER JOIN 'right.csv' b ON a.id = b.id",
    );
    defer query.deinit();

    try std.testing.expectEqual(@as(usize, 1), query.joins.len);
    try std.testing.expectEqualStrings("/tmp/left join.csv", query.file_path);
}

// ── ILIKE and evaluateDirect tests ──────────────────────────────────────────

test "ILIKE: parser recognises ILIKE operator" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT name FROM 'test.csv' WHERE name ILIKE 'alice'");
    defer query.deinit();

    const where = query.where_expr orelse return error.TestUnexpectedNull;
    const comp = switch (where) {
        .comparison => |c| c,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(parser.Operator.ilike, comp.operator);
    try std.testing.expectEqualStrings("name", comp.column);
    try std.testing.expectEqualStrings("alice", comp.value);
}

test "evaluateDirect: AND passes when both conditions true" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT * FROM 'test.csv' WHERE age = 30 AND city = 'NYC'");
    defer query.deinit();

    const where = query.where_expr orelse return error.TestUnexpectedNull;
    const header = [_][]const u8{ "age", "city" };
    const fields = [_][]const u8{ "30", "NYC" };
    try std.testing.expect(parser.evaluateDirect(where, &fields, &header));

    const fields_no = [_][]const u8{ "25", "NYC" };
    try std.testing.expect(!parser.evaluateDirect(where, &fields_no, &header));
}

test "evaluateDirect: OR passes when at least one condition true" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT * FROM 'test.csv' WHERE city = 'NYC' OR city = 'LA'");
    defer query.deinit();

    const where = query.where_expr orelse return error.TestUnexpectedNull;
    const header = [_][]const u8{"city"};

    const fields_nyc = [_][]const u8{"NYC"};
    try std.testing.expect(parser.evaluateDirect(where, &fields_nyc, &header));

    const fields_la = [_][]const u8{"LA"};
    try std.testing.expect(parser.evaluateDirect(where, &fields_la, &header));

    const fields_chi = [_][]const u8{"Chicago"};
    try std.testing.expect(!parser.evaluateDirect(where, &fields_chi, &header));
}

test "evaluateDirect: NOT negates condition" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT * FROM 'test.csv' WHERE NOT city = 'NYC'");
    defer query.deinit();

    const where = query.where_expr orelse return error.TestUnexpectedNull;
    const header = [_][]const u8{"city"};

    const fields_nyc = [_][]const u8{"NYC"};
    try std.testing.expect(!parser.evaluateDirect(where, &fields_nyc, &header));

    const fields_la = [_][]const u8{"LA"};
    try std.testing.expect(parser.evaluateDirect(where, &fields_la, &header));
}

// col IN (SELECT ...) is now the one supported subquery shape (#124) — see
// the "WHERE col IN (SELECT ...)" tests further down for its parsing, and
// engine.zig for the resolved, end-to-end behavior. Every other subquery
// shape (FROM, SELECT list, HAVING) is still a clear error, covered below.
test "subquery in WHERE IN parses and captures the inner query, no longer errors (#124)" {
    const allocator = std.testing.allocator;
    var query = try parser.parse(allocator, "SELECT name FROM 't.csv' WHERE dept IN (SELECT dept FROM 't.csv' WHERE salary > 80000)");
    defer query.deinit();
    try std.testing.expectEqualStrings("SELECT dept FROM 't.csv' WHERE salary > 80000", query.where_expr.?.comparison.in_subquery_sql.?);
}

test "subquery in HAVING errors clearly instead of silently misparsing (#124)" {
    const allocator = std.testing.allocator;
    const result = parser.parse(allocator, "SELECT dept, AVG(salary) FROM 't.csv' GROUP BY dept HAVING AVG(salary) > (SELECT AVG(salary) FROM 't.csv')");
    try std.testing.expectError(error.SubqueriesNotSupported, result);
}

test "INTERSECT errors clearly instead of silently returning empty (#127)" {
    const allocator = std.testing.allocator;
    const result = parser.parse(allocator, "SELECT name FROM 't.csv' WHERE dept='Eng' INTERSECT SELECT name FROM 't.csv' WHERE salary > 80000");
    try std.testing.expectError(error.UnsupportedSetOperation, result);
}

test "EXCEPT errors clearly instead of silently returning empty (#127)" {
    const allocator = std.testing.allocator;
    const result = parser.parse(allocator, "SELECT name FROM 't.csv' EXCEPT SELECT name FROM 't.csv' WHERE salary > 80000");
    try std.testing.expectError(error.UnsupportedSetOperation, result);
}

test "WHERE col % divisor op value filters correctly (#119)" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT * FROM 'test.csv' WHERE id % 2 = 0");
    defer query.deinit();

    const where = query.where_expr orelse return error.TestUnexpectedNull;
    const header = [_][]const u8{"id"};

    const even = [_][]const u8{"4"};
    try std.testing.expect(parser.evaluateDirect(where, &even, &header));

    const odd = [_][]const u8{"3"};
    try std.testing.expect(!parser.evaluateDirect(where, &odd, &header));
}

test "WHERE IS NOT NULL AND another condition — both conditions apply, not just IS NOT NULL (#142)" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT * FROM 'test.csv' WHERE age IS NOT NULL AND city = 'Austin'");
    defer query.deinit();

    const where = query.where_expr orelse return error.TestUnexpectedNull;
    const header = [_][]const u8{ "age", "city" };

    // age IS NOT NULL is true for both, so the result must depend entirely
    // on the city comparison — if the AND clause got silently dropped
    // (#142), both of these would incorrectly return true.
    const austin = [_][]const u8{ "30", "Austin" };
    try std.testing.expect(parser.evaluateDirect(where, &austin, &header));

    const boston = [_][]const u8{ "30", "Boston" };
    try std.testing.expect(!parser.evaluateDirect(where, &boston, &header));
}

test "WHERE comparison AND IS NULL — the comparison must still apply, not just IS NULL (#142)" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT * FROM 'test.csv' WHERE age > 5 AND city IS NULL");
    defer query.deinit();

    const where = query.where_expr orelse return error.TestUnexpectedNull;
    const header = [_][]const u8{ "age", "city" };

    // city IS NULL (empty) is true for both rows, so the result must depend
    // entirely on age > 5 — if the parser instead treated "age > 5 AND
    // city" as a single bogus column name (#142), this would error at
    // parse time instead of reaching evaluateDirect at all.
    const old_empty_city = [_][]const u8{ "10", "" };
    try std.testing.expect(parser.evaluateDirect(where, &old_empty_city, &header));

    const young_empty_city = [_][]const u8{ "3", "" };
    try std.testing.expect(!parser.evaluateDirect(where, &young_empty_city, &header));
}

// TDD: WHERE col IN (SELECT ...) is the one supported subquery shape (#124).
// The parser only captures the inner SELECT's raw text here — it can't run
// it (no file I/O at parse time); the engine resolves in_subquery_sql into
// in_values before evaluation. See engine.zig's "IN (SELECT ...)" tests for
// the resolved, end-to-end behavior.
test "WHERE col IN (SELECT ...) captures the inner query text, not a literal list" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT * FROM 'a.csv' WHERE id IN (SELECT id FROM 'b.csv')");
    defer query.deinit();

    const comp = query.where_expr.?.comparison;
    try std.testing.expectEqualStrings("id", comp.column);
    try std.testing.expect(comp.in_values == null);
    try std.testing.expect(!comp.in_negate);
    try std.testing.expectEqualStrings("SELECT id FROM 'b.csv'", comp.in_subquery_sql.?);
}

test "WHERE col NOT IN (SELECT ...) sets in_negate and captures the inner query" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT * FROM 'a.csv' WHERE id NOT IN (SELECT id FROM 'b.csv' WHERE active = 'y')");
    defer query.deinit();

    const comp = query.where_expr.?.comparison;
    try std.testing.expect(comp.in_negate);
    try std.testing.expect(comp.in_values == null);
    try std.testing.expectEqualStrings("SELECT id FROM 'b.csv' WHERE active = 'y'", comp.in_subquery_sql.?);
}

// A subquery whose own WHERE clause contains AND/parens must be captured
// whole, not split by the outer AND/OR splitter (#124) — findTopLevelOp is
// paren-depth-aware, so this exercises that the IN-subquery path relies on
// that correctly rather than re-deriving its own paren matching.
test "WHERE col IN (SELECT ...) with AND inside the subquery's own WHERE is captured whole" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT * FROM 'a.csv' WHERE id IN (SELECT id FROM 'b.csv' WHERE x = 1 AND y = 2)");
    defer query.deinit();

    const comp = query.where_expr.?.comparison;
    try std.testing.expectEqualStrings("SELECT id FROM 'b.csv' WHERE x = 1 AND y = 2", comp.in_subquery_sql.?);
}

// Every OTHER subquery shape stays a clear error (#124) — only col IN
// (SELECT ...) / col NOT IN (SELECT ...) is supported.
test "subquery in FROM clause still errors clearly" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(
        error.SubqueriesNotSupported,
        parser.parse(allocator, "SELECT * FROM (SELECT * FROM 'a.csv')"),
    );
}

test "scalar subquery in SELECT list still errors clearly" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(
        error.SubqueriesNotSupported,
        parser.parse(allocator, "SELECT (SELECT 1) FROM 'a.csv'"),
    );
}

// (HAVING subquery coverage already exists above: "subquery in HAVING
// errors clearly instead of silently misparsing (#124)".)
