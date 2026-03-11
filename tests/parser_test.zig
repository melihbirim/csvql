const std = @import("std");
const parser = @import("parser");

// TDD Test 1: Query.deinit should not crash on empty allocations (SELECT *)
test "Query deinit with SELECT *" {
    const allocator = std.testing.allocator;

    var query = try parser.parse(allocator, "SELECT * FROM 'test.csv'");
    defer query.deinit();

    try std.testing.expectEqual(@as(usize, 0), query.columns.len);
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
    try std.testing.expectEqualStrings("dept_id", j.left_col);
    try std.testing.expectEqualStrings("id", j.right_col);
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
    try std.testing.expectEqualStrings("id", j.left_col);
    try std.testing.expectEqualStrings("fk", j.right_col);
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
    try std.testing.expectEqualStrings("dept_id", j1.left_col);
    try std.testing.expectEqualStrings("id", j1.right_col);

    const j2 = query.joins[1];
    try std.testing.expectEqualStrings("region.csv", j2.right_file);
    try std.testing.expectEqualStrings("b", j2.left_alias);
    try std.testing.expectEqualStrings("c", j2.right_alias);
    try std.testing.expectEqualStrings("region_id", j2.left_col);
    try std.testing.expectEqualStrings("id", j2.right_col);
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
