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
    like,

    pub fn fromString(s: []const u8) ?Operator {
        if (std.mem.eql(u8, s, "=")) return .equal;
        if (std.mem.eql(u8, s, "!=")) return .not_equal;
        if (std.mem.eql(u8, s, ">")) return .greater;
        if (std.mem.eql(u8, s, ">=")) return .greater_equal;
        if (std.mem.eql(u8, s, "<")) return .less;
        if (std.mem.eql(u8, s, "<=")) return .less_equal;
        if (std.ascii.eqlIgnoreCase(s, "LIKE")) return .like;
        return null;
    }
};

/// Match text against a SQL LIKE pattern.
/// `%` matches any sequence of characters (including empty).
/// `_` matches exactly one character.
pub fn matchLike(text: []const u8, pattern: []const u8) bool {
    var t: usize = 0;
    var p: usize = 0;
    var star_p: usize = std.math.maxInt(usize); // sentinel: no wildcard seen yet
    var star_t: usize = 0;
    while (t < text.len) {
        if (p < pattern.len and (pattern[p] == '_' or pattern[p] == text[t])) {
            t += 1;
            p += 1;
        } else if (p < pattern.len and pattern[p] == '%') {
            star_p = p;
            star_t = t;
            p += 1;
        } else if (star_p != std.math.maxInt(usize)) {
            star_t += 1;
            t = star_t;
            p = star_p + 1;
        } else {
            return false;
        }
    }
    // Consume trailing %
    while (p < pattern.len and pattern[p] == '%') p += 1;
    return p == pattern.len;
}

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

/// Describes a JOIN between two CSV files
pub const JoinClause = struct {
    right_file: []u8,
    left_alias: []u8,
    right_alias: []u8,
    left_col: []u8,
    right_col: []u8,

    pub fn deinit(self: *JoinClause, allocator: Allocator) void {
        allocator.free(self.right_file);
        allocator.free(self.left_alias);
        allocator.free(self.right_alias);
        allocator.free(self.left_col);
        allocator.free(self.right_col);
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
    /// All JOIN clauses in order (empty slice when there is no JOIN).
    joins: []JoinClause,
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

        for (self.joins) |*j| {
            j.deinit(self.allocator);
        }
        self.allocator.free(self.joins);
    }
};

/// Describes the position of a JOIN keyword within the FROM clause string
const JoinKeyword = struct {
    /// Index in the source string where the JOIN keyword (or INNER JOIN) begins
    kw_start: usize,
    /// Index in the source string just after the JOIN keyword (points into right-file part)
    kw_end: usize,
};

/// Returns true if `needle` appears case-insensitively in `s` at a position that is NOT
/// enclosed in single-quotes (so that filenames like '/tmp/left join.csv' are ignored).
fn containsKeywordOutsideQuotes(s: []const u8, needle: []const u8) bool {
    var i: usize = 0;
    while (i < s.len) {
        if (s[i] == '\'') {
            // Skip everything inside the quoted string.
            i += 1;
            while (i < s.len and s[i] != '\'') : (i += 1) {}
            if (i < s.len) i += 1; // skip closing quote
            continue;
        }
        if (i + needle.len <= s.len and std.ascii.eqlIgnoreCase(s[i..][0..needle.len], needle)) {
            return true;
        }
        i += 1;
    }
    return false;
}

/// Returns the position of an INNER JOIN or bare JOIN keyword in `s`, or null if not found.
/// Requires the keyword to be preceded by whitespace (or start of string) and followed by a space
/// so that "join" inside a file name (e.g. 'data_join.csv') is not mistaken for the keyword.
fn detectJoinKeyword(s: []const u8) ?JoinKeyword {
    // Prefer "INNER JOIN " over bare "JOIN " to get the right kw_start
    if (std.ascii.indexOfIgnoreCase(s, "INNER JOIN ")) |idx| {
        // Verify it's preceded by whitespace or at position 0
        if (idx == 0 or std.ascii.isWhitespace(s[idx - 1])) {
            return JoinKeyword{ .kw_start = idx, .kw_end = idx + 10 }; // "INNER JOIN".len = 10
        }
    }
    // Search for " JOIN " (with leading space) to avoid matching inside identifiers
    var i: usize = 1;
    while (i + 5 <= s.len) : (i += 1) {
        if (std.ascii.isWhitespace(s[i - 1]) and
            std.ascii.eqlIgnoreCase(s[i .. i + 4], "JOIN") and
            std.ascii.isWhitespace(s[i + 4]))
        {
            return JoinKeyword{ .kw_start = i, .kw_end = i + 4 }; // "JOIN".len = 4
        }
    }
    return null;
}

const FileAlias = struct { file: []u8, alias: []u8 };

/// Extract a file path and optional alias from a string like:
///   'path/to/file.csv' alias
///   'path/to/file.csv'
///   path/to/file.csv alias
///   path/to/file.csv
/// The alias is lowercased for case-insensitive matching later.
fn extractFileAndAlias(allocator: Allocator, input: []const u8) !FileAlias {
    const s = std.mem.trim(u8, input, &std.ascii.whitespace);
    if (s.len == 0) return error.InvalidQuery;

    if (s[0] == '\'') {
        // Quoted path: find closing single-quote
        const end_q = std.mem.indexOfScalar(u8, s[1..], '\'') orelse return error.InvalidQuery;
        const file_raw = s[1 .. end_q + 1]; // inside the quotes
        const after = std.mem.trim(u8, s[end_q + 2 ..], &std.ascii.whitespace);
        const alias_buf = try allocator.alloc(u8, after.len);
        _ = std.ascii.lowerString(alias_buf, after);
        return FileAlias{
            .file = try allocator.dupe(u8, file_raw),
            .alias = alias_buf,
        };
    } else {
        // Unquoted path: split on first whitespace
        if (std.mem.indexOfAny(u8, s, &std.ascii.whitespace)) |sp| {
            const file_raw = s[0..sp];
            const after = std.mem.trim(u8, s[sp + 1 ..], &std.ascii.whitespace);
            const alias_buf = try allocator.alloc(u8, after.len);
            _ = std.ascii.lowerString(alias_buf, after);
            return FileAlias{
                .file = try allocator.dupe(u8, file_raw),
                .alias = alias_buf,
            };
        } else {
            return FileAlias{
                .file = try allocator.dupe(u8, s),
                .alias = try allocator.dupe(u8, ""),
            };
        }
    }
}

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
        .joins = &.{},
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
    // Ensure query.columns is freed on any error path from here on.
    errdefer {
        for (query.columns) |col| allocator.free(col);
        allocator.free(query.columns);
    }

    // Extract file path (and optional JOIN clauses) from FROM clause.
    // Supports chained JOINs:  FROM a JOIN b ON ... JOIN c ON ... WHERE ...
    const from_rest = trimmed[from_idx + 4 ..];
    var rest: []const u8 = undefined;

    // Reject unsupported JOIN types before they silently fall through to single-file mode.
    // Use the quote-aware scanner so that filenames like '/tmp/left join.csv' are not flagged.
    inline for ([_][]const u8{ "LEFT JOIN", "RIGHT JOIN", "OUTER JOIN", "FULL JOIN", "FULL OUTER JOIN", "CROSS JOIN" }) |kw| {
        if (containsKeywordOutsideQuotes(from_rest, kw)) {
            return error.UnsupportedJoinType;
        }
    }

    // Collect all JOIN clauses into a temporary list.
    var joins_list = std.ArrayList(JoinClause){};
    // On error, free any already-collected JoinClause items.
    errdefer {
        for (joins_list.items) |*jc| jc.deinit(allocator);
        joins_list.deinit(allocator);
    }

    if (detectJoinKeyword(from_rest)) |first_jk| {
        // ---- There is at least one JOIN ----
        // Left file + optional alias is everything before the first JOIN keyword.
        const first_left_raw = std.mem.trim(u8, from_rest[0..first_jk.kw_start], &std.ascii.whitespace);
        const first_left_info = try extractFileAndAlias(allocator, first_left_raw);
        errdefer {
            allocator.free(first_left_info.file);
            allocator.free(first_left_info.alias);
        }
        query.file_path = first_left_info.file;

        // Walk the remaining string, collecting one JoinClause per JOIN keyword.
        // `cursor` always points into `from_rest` just after the last consumed JOIN keyword.
        var cursor = std.mem.trimLeft(u8, from_rest[first_jk.kw_end..], &std.ascii.whitespace);
        // left_alias for the *current* join step starts as the alias of the base left file.
        var current_left_alias = first_left_info.alias;
        // We'll re-assign current_left_alias at each iteration; ownership of the original
        // string is transferred into the JoinClause we pushed in the *previous* iteration
        // (or remains as first_left_info.alias for the very first clause), so no extra free.

        while (true) {
            // cursor = "right_file [alias] ON left.col = right.col [JOIN ...] [WHERE ...]"

            // Find ON keyword for this join step.
            const on_pos = std.ascii.indexOfIgnoreCase(cursor, " ON ") orelse return error.InvalidJoinSyntax;
            const right_raw = std.mem.trim(u8, cursor[0..on_pos], &std.ascii.whitespace);
            const after_on = cursor[on_pos + 4 ..]; // skip " ON "

            // Find end of ON condition: stop at the next JOIN keyword OR at WHERE/GROUP/ORDER/LIMIT.
            // We must stop at the *next* JOIN keyword before any clause keyword so that chained
            // joins are parsed left–to–right rather than lumping everything into one condition.
            const next_join_kw = detectJoinKeyword(after_on);
            const on_where = std.ascii.indexOfIgnoreCase(after_on, "WHERE");
            const on_group = std.ascii.indexOfIgnoreCase(after_on, "GROUP BY");
            const on_order = std.ascii.indexOfIgnoreCase(after_on, "ORDER BY");
            const on_limit = std.ascii.indexOfIgnoreCase(after_on, "LIMIT");
            var on_end = after_on.len;
            if (next_join_kw) |njk| on_end = @min(on_end, njk.kw_start);
            if (on_where) |i| on_end = @min(on_end, i);
            if (on_group) |i| on_end = @min(on_end, i);
            if (on_order) |i| on_end = @min(on_end, i);
            if (on_limit) |i| on_end = @min(on_end, i);

            const on_cond = std.mem.trim(u8, after_on[0..on_end], &std.ascii.whitespace);

            // Parse right file + alias.
            const right_info = try extractFileAndAlias(allocator, right_raw);
            errdefer {
                allocator.free(right_info.file);
                allocator.free(right_info.alias);
            }

            // Parse ON condition: "left_alias.col = right_alias.col"
            const eq_pos = blk: {
                var si: usize = 0;
                while (si < on_cond.len) : (si += 1) {
                    if (on_cond[si] == '=' and
                        (si == 0 or (on_cond[si - 1] != '<' and on_cond[si - 1] != '>' and on_cond[si - 1] != '!')))
                    {
                        break :blk si;
                    }
                }
                return error.InvalidJoinSyntax;
            };
            const on_left_expr = std.mem.trim(u8, on_cond[0..eq_pos], &std.ascii.whitespace);
            const on_right_expr = std.mem.trim(u8, on_cond[eq_pos + 1 ..], &std.ascii.whitespace);

            // Preserve the full ON operand (including alias prefix if present).
            // e.g. "a.dept_id" stays "a.dept_id", not stripped to "dept_id".
            // The engine resolves qualified names against alias-range-precise maps.
            const ljc = try allocator.alloc(u8, on_left_expr.len);
            _ = std.ascii.lowerString(ljc, on_left_expr);
            const rjc = try allocator.alloc(u8, on_right_expr.len);
            _ = std.ascii.lowerString(rjc, on_right_expr);

            try joins_list.append(allocator, JoinClause{
                .right_file = right_info.file,
                .left_alias = current_left_alias,
                .right_alias = right_info.alias,
                .left_col = ljc,
                .right_col = rjc,
            });

            // The right alias of this step becomes the left alias of the *next* step
            // so that the ON condition in the next join can reference columns from the
            // accumulated result.  We hand off ownership of right_info.alias into the
            // JoinClause we just appended; we need a fresh copy for the next iteration.
            if (next_join_kw) |njk| {
                // Alias ownership already transferred above.  Derive new current_left_alias
                // from right_info.alias (already stored in the clause, so dupe it).
                current_left_alias = try allocator.dupe(u8, right_info.alias);
                cursor = std.mem.trimLeft(u8, after_on[njk.kw_end..], &std.ascii.whitespace);
            } else {
                // current_left_alias was transferred into the JoinClause; we're done.
                rest = after_on[on_end..];
                break;
            }
        }

        query.joins = try joins_list.toOwnedSlice(allocator);
    } else {
        // No JOIN: single-file query — extract file path from between FROM and first clause keyword
        const where_idx_pre = std.ascii.indexOfIgnoreCase(from_rest, "WHERE");
        const group_by_idx_pre = std.ascii.indexOfIgnoreCase(from_rest, "GROUP BY");
        const order_by_idx_pre = std.ascii.indexOfIgnoreCase(from_rest, "ORDER BY");
        const limit_idx_pre = std.ascii.indexOfIgnoreCase(from_rest, "LIMIT");

        var fend = from_rest.len;
        if (where_idx_pre) |i| fend = @min(fend, i);
        if (group_by_idx_pre) |i| fend = @min(fend, i);
        if (order_by_idx_pre) |i| fend = @min(fend, i);
        if (limit_idx_pre) |i| fend = @min(fend, i);

        const fp = std.mem.trim(u8, from_rest[0..fend], &std.ascii.whitespace);
        query.file_path = try allocator.dupe(u8, trimQuotes(fp));
        query.joins = try allocator.alloc(JoinClause, 0);
        rest = from_rest;
    }

    // Parse the clause keywords (WHERE / GROUP BY / ORDER BY / LIMIT) from `rest`
    const where_idx = std.ascii.indexOfIgnoreCase(rest, "WHERE");
    const group_by_idx = std.ascii.indexOfIgnoreCase(rest, "GROUP BY");
    const order_by_idx = std.ascii.indexOfIgnoreCase(rest, "ORDER BY");
    const limit_idx = std.ascii.indexOfIgnoreCase(rest, "LIMIT");

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

    // Check for LIKE operator before symbol operators (LIKE patterns may contain = > <)
    if (std.ascii.indexOfIgnoreCase(trimmed, " LIKE ")) |idx| {
        return parseLikeComparison(allocator, trimmed, idx);
    }

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

fn parseLikeComparison(allocator: Allocator, input: []const u8, like_idx: usize) !Expression {
    const column_part = std.mem.trim(u8, input[0..like_idx], &std.ascii.whitespace);
    // " LIKE " is 6 bytes: space + L + I + K + E + space
    const value_part = std.mem.trim(u8, input[like_idx + 6 ..], &std.ascii.whitespace);
    const value_clean = trimQuotes(value_part);

    const column_lower = try allocator.alloc(u8, column_part.len);
    _ = std.ascii.lowerString(column_lower, column_part);

    return Expression{
        .comparison = Comparison{
            .column = column_lower,
            .operator = .like,
            .value = try allocator.dupe(u8, value_clean),
            .numeric_value = null, // LIKE is always a string pattern match
        },
    };
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
    // LIKE is always a string pattern match — skip numeric path entirely
    if (comp.operator == .like) return matchLike(candidate, comp.value);

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
                .like => unreachable, // handled above
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
                .like => unreachable, // handled above
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
        .like => matchLike(candidate, comp.value),
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

test "parse distinct aggregate query" {
    const allocator = std.testing.allocator;

    var query = try parse(allocator, "SELECT DISTINCT COUNT(*) FROM 'data.csv'");
    defer query.deinit();

    try std.testing.expect(query.distinct);
    try std.testing.expect(!query.all_columns);
    try std.testing.expectEqual(@as(usize, 1), query.columns.len);
    try std.testing.expectEqualStrings("COUNT(*)", query.columns[0]);
    try std.testing.expectEqualStrings("data.csv", query.file_path);
}
