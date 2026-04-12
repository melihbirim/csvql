const std = @import("std");
const simd = @import("simd.zig");
const scalar = @import("scalar.zig");
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
    ilike,
    between,
    is_null,
    is_not_null,

    pub fn fromString(s: []const u8) ?Operator {
        if (std.mem.eql(u8, s, "=")) return .equal;
        if (std.mem.eql(u8, s, "!=")) return .not_equal;
        if (std.mem.eql(u8, s, ">")) return .greater;
        if (std.mem.eql(u8, s, ">=")) return .greater_equal;
        if (std.mem.eql(u8, s, "<")) return .less;
        if (std.mem.eql(u8, s, "<=")) return .less_equal;
        if (std.ascii.eqlIgnoreCase(s, "LIKE")) return .like;
        if (std.ascii.eqlIgnoreCase(s, "ILIKE")) return .ilike;
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

/// Match text against a SQL ILIKE pattern (case-insensitive variant of LIKE).
/// `%` matches any sequence, `_` matches exactly one character.
pub fn matchILike(text: []const u8, pattern: []const u8) bool {
    var t: usize = 0;
    var p: usize = 0;
    var star_p: usize = std.math.maxInt(usize);
    var star_t: usize = 0;
    while (t < text.len) {
        const tc = std.ascii.toLower(text[t]);
        if (p < pattern.len and (pattern[p] == '_' or std.ascii.toLower(pattern[p]) == tc)) {
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
    while (p < pattern.len and pattern[p] == '%') p += 1;
    return p == pattern.len;
}

/// Scalar function comparison in WHERE clause.
/// e.g. DATEDIFF('day', shipped_at, delivered_at) > 5
pub const ScalarWhereComparison = struct {
    /// Lowercase function name: "datediff", "dateadd", or "extract".
    /// Owned: allocated by the parser allocator, freed in deinit.
    func: []const u8,
    /// Unit string without quotes (e.g. "day") or EXTRACT part (e.g. "year").
    /// Owned: allocated by the parser allocator, freed in deinit.
    unit: []const u8,
    /// First column name (start_col for datediff, date_col for dateadd/extract), lowercase.
    /// Owned: allocated by the parser allocator, freed in deinit.
    col1_name: []const u8,
    /// Second column name (end_col for datediff). null for dateadd/extract.
    /// Owned when non-null: allocated by the parser allocator, freed in deinit.
    col2_name: ?[]const u8,
    /// Numeric amount for DATEADD; unused for datediff/extract
    amount: i32,
    operator: Operator,
    rhs_value: f64,

    pub fn deinit(self: ScalarWhereComparison, allocator: Allocator) void {
        allocator.free(self.func);
        allocator.free(self.unit);
        allocator.free(self.col1_name);
        if (self.col2_name) |c| allocator.free(c);
    }
};

/// Represents a WHERE clause expression
pub const Expression = union(enum) {
    comparison: Comparison,
    scalar_comparison: ScalarWhereComparison,
    binary: *BinaryExpr,
    unary: *UnaryExpr,

    pub fn deinit(self: Expression, allocator: Allocator) void {
        switch (self) {
            .comparison => |c| c.deinit(allocator),
            .scalar_comparison => |sc| sc.deinit(allocator),
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
    /// Non-null when this is an IN (...) expression; holds the list of values to match.
    in_values: ?[][]u8 = null,
    /// BETWEEN: upper bound string (value field holds lower bound)
    between_high: ?[]u8 = null,
    /// BETWEEN: upper bound as f64 (null if non-numeric)
    between_high_num: ?f64 = null,

    pub fn deinit(self: Comparison, allocator: Allocator) void {
        allocator.free(self.column);
        allocator.free(self.value);
        if (self.in_values) |vals| {
            for (vals) |v| allocator.free(v);
            allocator.free(vals);
        }
        if (self.between_high) |h| allocator.free(h);
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
    having_expr: ?Expression,
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

        if (self.having_expr) |expr| {
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

/// Find the index of the table-level FROM keyword, starting the scan at `from`.
/// Skips any FROM tokens that appear inside parentheses, so that
/// EXTRACT(year FROM col) does not confuse the parser.
/// Returns the byte offset of the 'F' in the matching FROM, or null.
fn findTableFromIdx(s: []const u8, from: usize) ?usize {
    var depth: usize = 0;
    var in_quote = false;
    var i: usize = from;
    while (i < s.len) {
        const c = s[i];
        if (in_quote) {
            if (c == '\'') in_quote = false;
            i += 1;
            continue;
        }
        switch (c) {
            '\'' => {
                in_quote = true;
                i += 1;
            },
            '(' => {
                depth += 1;
                i += 1;
            },
            ')' => {
                if (depth > 0) depth -= 1;
                i += 1;
            },
            else => {
                if (depth == 0 and
                    i + 4 <= s.len and
                    std.ascii.eqlIgnoreCase(s[i .. i + 4], "FROM") and
                    (i == 0 or std.ascii.isWhitespace(s[i - 1])) and
                    (i + 4 >= s.len or std.ascii.isWhitespace(s[i + 4])))
                {
                    return i;
                }
                i += 1;
            },
        }
    }
    return null;
}

/// Split a comma-separated list while respecting nested parentheses and single-quoted strings.
/// Commas inside `(...)` or `'...'` are treated as part of the current token, not separators.
/// Returns an ArrayList of trimmed, allocator-owned strings; caller frees each item + the list.
fn splitCommaTerms(allocator: Allocator, input: []const u8) !std.ArrayList([]u8) {
    var items = std.ArrayList([]u8){};
    errdefer {
        for (items.items) |s| allocator.free(s);
        items.deinit(allocator);
    }
    var depth: usize = 0;
    var in_quote = false;
    var start: usize = 0;
    for (input, 0..) |c, i| {
        if (in_quote) {
            if (c == '\'') in_quote = false;
            continue;
        }
        switch (c) {
            '\'' => in_quote = true,
            '(' => depth += 1,
            ')' => if (depth > 0) {
                depth -= 1;
            },
            ',' => if (depth == 0) {
                const term = std.mem.trim(u8, input[start..i], &std.ascii.whitespace);
                if (term.len > 0) try items.append(allocator, try allocator.dupe(u8, term));
                start = i + 1;
            },
            else => {},
        }
    }
    // Last / only term
    const last = std.mem.trim(u8, input[start..], &std.ascii.whitespace);
    if (last.len > 0) try items.append(allocator, try allocator.dupe(u8, last));
    return items;
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
        .having_expr = null,
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
    // Use paren-aware search so EXTRACT(year FROM col) doesn't match as the table FROM.
    const from_idx = findTableFromIdx(trimmed, select_idx + 6) orelse return error.InvalidQuery;

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
        // Parse column list — use paren/quote-aware splitter so STRFTIME('%Y-%m', col)
        // is kept as a single token instead of splitting on the inner comma.
        var col_list = try splitCommaTerms(allocator, columns_part);
        defer col_list.deinit(allocator);
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
        const where_idx_pre = findClauseKeyword(from_rest, "WHERE");
        const group_by_idx_pre = findClauseKeyword(from_rest, "GROUP BY");
        const order_by_idx_pre = findClauseKeyword(from_rest, "ORDER BY");
        const limit_idx_pre = findClauseKeyword(from_rest, "LIMIT");

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
    const where_idx = findClauseKeyword(rest, "WHERE");
    const group_by_idx = findClauseKeyword(rest, "GROUP BY");
    const having_idx = findClauseKeyword(rest, "HAVING");
    const order_by_idx = findClauseKeyword(rest, "ORDER BY");
    const limit_idx = findClauseKeyword(rest, "LIMIT");

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
        if (having_idx) |hidx| {
            group_by_part = group_by_part[0..@min(group_by_part.len, hidx - idx - 8)];
        } else if (order_by_idx) |oidx| {
            group_by_part = group_by_part[0..@min(group_by_part.len, oidx - idx - 8)];
        } else if (limit_idx) |lidx| {
            group_by_part = group_by_part[0..@min(group_by_part.len, lidx - idx - 8)];
        }
        group_by_part = std.mem.trim(u8, group_by_part, &std.ascii.whitespace);

        // Use paren/quote-aware splitter so STRFTIME('%Y-%m', col) stays as one token.
        var group_list = try splitCommaTerms(allocator, group_by_part);
        defer group_list.deinit(allocator);
        query.group_by = try group_list.toOwnedSlice(allocator);
    } else {
        // FIXED: Always allocate empty slice instead of using static slice
        query.group_by = try allocator.alloc([]u8, 0);
    }

    // Parse HAVING clause if present (filter on aggregated results, evaluated post-GROUP-BY)
    if (having_idx) |idx| {
        var having_part = rest[idx + 6 ..];
        if (order_by_idx) |oidx| {
            having_part = having_part[0..@min(having_part.len, oidx - idx - 6)];
        } else if (limit_idx) |lidx| {
            having_part = having_part[0..@min(having_part.len, lidx - idx - 6)];
        }
        having_part = std.mem.trim(u8, having_part, &std.ascii.whitespace);
        query.having_expr = try parseExpression(allocator, having_part);
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

/// Find the first occurrence of `keyword` in `s` that is:
///   - not inside a single-quoted string, and
///   - preceded by whitespace or start-of-string, and
///   - followed by whitespace or end-of-string.
/// Returns the byte index of the start of `keyword` in `s`, or null.
fn findClauseKeyword(s: []const u8, keyword: []const u8) ?usize {
    var i: usize = 0;
    while (i < s.len) {
        if (s[i] == '\'') {
            i += 1;
            while (i < s.len and s[i] != '\'') : (i += 1) {}
            if (i < s.len) i += 1;
            continue;
        }
        if (i + keyword.len <= s.len and
            std.ascii.eqlIgnoreCase(s[i .. i + keyword.len], keyword))
        {
            const pre_ok = (i == 0) or std.ascii.isWhitespace(s[i - 1]);
            const post_idx = i + keyword.len;
            const post_ok = (post_idx >= s.len) or std.ascii.isWhitespace(s[post_idx]);
            if (pre_ok and post_ok) return i;
        }
        i += 1;
    }
    return null;
}

/// Scan `s` for " KEYWORD " (case-insensitive, with a space on each side) while
/// skipping over single-quoted strings and nested parentheses.
/// Returns the index of the leading space.
fn findTopLevelOp(s: []const u8, keyword: []const u8) ?usize {
    const need = keyword.len + 2; // leading ' ' + keyword + trailing ' '
    if (s.len < need) return null;
    var i: usize = 0;
    var depth: usize = 0;
    while (i < s.len) {
        if (s[i] == '\'') {
            i += 1;
            while (i < s.len and s[i] != '\'') : (i += 1) {}
            if (i < s.len) i += 1; // skip closing quote
            continue;
        }
        if (s[i] == '(') {
            depth += 1;
            i += 1;
            continue;
        }
        if (s[i] == ')') {
            if (depth > 0) depth -= 1;
            i += 1;
            continue;
        }
        if (depth == 0 and s[i] == ' ' and i + need <= s.len and
            std.ascii.eqlIgnoreCase(s[i + 1 .. i + 1 + keyword.len], keyword) and
            s[i + 1 + keyword.len] == ' ')
        {
            return i;
        }
        i += 1;
    }
    return null;
}

pub fn parseExpression(allocator: Allocator, input: []const u8) !Expression {
    const trimmed = std.mem.trim(u8, input, &std.ascii.whitespace);

    // Strip outer parentheses: (expr) → expr
    if (trimmed.len >= 2 and trimmed[0] == '(' and trimmed[trimmed.len - 1] == ')') {
        // Verify the opening '(' closes at the very last position (not earlier).
        var depth: usize = 0;
        var outer = true;
        for (trimmed, 0..) |c, ci| {
            if (c == '(') depth += 1;
            if (c == ')') {
                depth -= 1;
                if (depth == 0 and ci < trimmed.len - 1) {
                    outer = false;
                    break;
                }
            }
        }
        if (outer) {
            return parseExpression(allocator, trimmed[1 .. trimmed.len - 1]);
        }
    }

    // NOT prefix — must be checked BEFORE IS NULL/IS NOT NULL so that
    // "NOT col IS NULL" recurses correctly (inner "col IS NULL" is parsed separately).
    if (trimmed.len > 4 and std.ascii.eqlIgnoreCase(trimmed[0..4], "NOT ")) {
        const inner = try parseExpression(allocator, std.mem.trim(u8, trimmed[4..], &std.ascii.whitespace));
        const un = try allocator.create(UnaryExpr);
        un.* = .{ .expr = inner };
        return Expression{ .unary = un };
    }

    // IS NULL / IS NOT NULL — check before BETWEEN/AND/OR.
    if (std.ascii.indexOfIgnoreCase(trimmed, " IS NOT NULL")) |idx| {
        const col_part = std.mem.trim(u8, trimmed[0..idx], &std.ascii.whitespace);
        const col_lower = try allocator.alloc(u8, col_part.len);
        _ = std.ascii.lowerString(col_lower, col_part);
        return Expression{ .comparison = Comparison{
            .column = col_lower,
            .operator = .is_not_null,
            .value = try allocator.dupe(u8, ""),
            .numeric_value = null,
        } };
    }
    if (std.ascii.indexOfIgnoreCase(trimmed, " IS NULL")) |idx| {
        const col_part = std.mem.trim(u8, trimmed[0..idx], &std.ascii.whitespace);
        const col_lower = try allocator.alloc(u8, col_part.len);
        _ = std.ascii.lowerString(col_lower, col_part);
        return Expression{ .comparison = Comparison{
            .column = col_lower,
            .operator = .is_null,
            .value = try allocator.dupe(u8, ""),
            .numeric_value = null,
        } };
    }

    // BETWEEN must be detected BEFORE the AND/OR split because
    // "col BETWEEN 1 AND 5" contains " AND " as syntax, not a logical operator.
    if (findTopLevelOp(trimmed, "BETWEEN")) |idx| {
        return parseBetween(allocator, trimmed, idx);
    }

    // AND/OR must be split before LIKE/IN/operators so that compound conditions
    // such as "col LIKE 'x%' AND other = 'y'" are not misinterpreted as a single
    // LIKE expression whose pattern swallows the rest of the string.
    // OR has lower precedence → check it first.
    if (findTopLevelOp(trimmed, "OR")) |idx| {
        const left = try parseExpression(allocator, trimmed[0..idx]);
        const right = try parseExpression(allocator, std.mem.trim(u8, trimmed[idx + 4 ..], &std.ascii.whitespace));
        const bin = try allocator.create(BinaryExpr);
        bin.* = .{ .op = .@"or", .left = left, .right = right };
        return Expression{ .binary = bin };
    }
    if (findTopLevelOp(trimmed, "AND")) |idx| {
        const left = try parseExpression(allocator, trimmed[0..idx]);
        const right = try parseExpression(allocator, std.mem.trim(u8, trimmed[idx + 5 ..], &std.ascii.whitespace));
        const bin = try allocator.create(BinaryExpr);
        bin.* = .{ .op = .@"and", .left = left, .right = right };
        return Expression{ .binary = bin };
    }

    // Check for ILIKE before LIKE (both before symbol operators since patterns may contain = > <)
    if (std.ascii.indexOfIgnoreCase(trimmed, " ILIKE ")) |idx| {
        return parseILikeComparison(allocator, trimmed, idx);
    }
    if (std.ascii.indexOfIgnoreCase(trimmed, " LIKE ")) |idx| {
        return parseLikeComparison(allocator, trimmed, idx);
    }

    // Check for IN (...) operator before = (so "col IN (...)" doesn't match the = path)
    if (std.ascii.indexOfIgnoreCase(trimmed, " IN (")) |idx| {
        const col_candidate = std.mem.trim(u8, trimmed[0..idx], &std.ascii.whitespace);
        // Only treat as IN if the left-hand side looks like a plain identifier (no operators/quotes)
        if (std.mem.indexOfAny(u8, col_candidate, "=<>!'\"(") == null) {
            return parseInComparison(allocator, trimmed, idx);
        }
    }

    // Check for scalar function as LHS: DATEDIFF(...) op rhs, DATEADD(...) op rhs,
    // EXTRACT(... FROM ...) op rhs.  Must come before the bare-operator checks below
    // so that e.g. DATEDIFF(...) > 5 is not parsed as a plain `>` comparison.
    if (try parseScalarWhereComparison(allocator, trimmed)) |expr| return expr;

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

fn parseBetween(allocator: Allocator, input: []const u8, between_idx: usize) !Expression {
    // " BETWEEN " is 9 bytes
    const col_part = std.mem.trim(u8, input[0..between_idx], &std.ascii.whitespace);
    const rest = std.mem.trim(u8, input[between_idx + 9 ..], &std.ascii.whitespace);
    // rest = "low AND high" — split on " AND "
    const and_idx = std.ascii.indexOfIgnoreCase(rest, " AND ") orelse return error.InvalidExpression;
    const low_str = std.mem.trim(u8, rest[0..and_idx], &std.ascii.whitespace);
    const high_str = std.mem.trim(u8, rest[and_idx + 5 ..], &std.ascii.whitespace);
    const low_clean = trimQuotes(low_str);
    const high_clean = trimQuotes(high_str);

    const col_lower = try allocator.alloc(u8, col_part.len);
    _ = std.ascii.lowerString(col_lower, col_part);

    return Expression{ .comparison = Comparison{
        .column = col_lower,
        .operator = .between,
        .value = try allocator.dupe(u8, low_clean),
        .numeric_value = std.fmt.parseFloat(f64, low_clean) catch null,
        .between_high = try allocator.dupe(u8, high_clean),
        .between_high_num = std.fmt.parseFloat(f64, high_clean) catch null,
    } };
}

fn parseInComparison(allocator: Allocator, input: []const u8, in_idx: usize) !Expression {
    const column_part = std.mem.trim(u8, input[0..in_idx], &std.ascii.whitespace);
    // Skip to the opening paren: " IN (" so paren is 4 chars after in_idx
    const after_in = input[in_idx..];
    const open_paren = std.mem.indexOfScalar(u8, after_in, '(') orelse return error.InvalidExpression;
    const close_paren = std.mem.lastIndexOfScalar(u8, after_in, ')') orelse return error.InvalidExpression;
    if (close_paren <= open_paren) return error.InvalidExpression;
    const inner = std.mem.trim(u8, after_in[open_paren + 1 .. close_paren], &std.ascii.whitespace);

    // Parse comma-separated values, trimming quotes from each
    var values = std.ArrayListUnmanaged([]u8){};
    errdefer {
        for (values.items) |v| allocator.free(v);
        values.deinit(allocator);
    }
    var it = std.mem.splitScalar(u8, inner, ',');
    while (it.next()) |tok| {
        const t = std.mem.trim(u8, tok, &std.ascii.whitespace);
        try values.append(allocator, try allocator.dupe(u8, trimQuotes(t)));
    }

    const column_lower = try allocator.alloc(u8, column_part.len);
    errdefer allocator.free(column_lower);
    _ = std.ascii.lowerString(column_lower, column_part);

    return Expression{
        .comparison = Comparison{
            .column = column_lower,
            .operator = .equal,
            .value = try allocator.dupe(u8, ""),
            .numeric_value = null,
            .in_values = try values.toOwnedSlice(allocator),
        },
    };
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

fn parseILikeComparison(allocator: Allocator, input: []const u8, ilike_idx: usize) !Expression {
    const column_part = std.mem.trim(u8, input[0..ilike_idx], &std.ascii.whitespace);
    // " ILIKE " is 7 bytes: space + I + L + I + K + E + space
    const value_part = std.mem.trim(u8, input[ilike_idx + 7 ..], &std.ascii.whitespace);
    const value_clean = trimQuotes(value_part);

    const column_lower = try allocator.alloc(u8, column_part.len);
    _ = std.ascii.lowerString(column_lower, column_part);

    return Expression{
        .comparison = Comparison{
            .column = column_lower,
            .operator = .ilike,
            .value = try allocator.dupe(u8, value_clean),
            .numeric_value = null, // ILIKE is always a string pattern match
        },
    };
}

/// Attempt to parse a scalar-function comparison such as
///   DATEDIFF('day', col1, col2) > 5
///   EXTRACT(year FROM date_col) = 2024
///   DATEADD('day', 7, date_col) < '2024-06-01'   (treated as string RHS; rarely numeric)
///
/// Returns null when the input does not start with a recognised scalar function name,
/// so the caller can fall through to regular operator parsing.
fn parseScalarWhereComparison(allocator: Allocator, input: []const u8) !?Expression {
    const t = input; // caller has already trimmed

    // Must look like FUNCNAME(...)
    const open = std.mem.indexOfScalar(u8, t, '(') orelse return null;
    const func_raw = std.mem.trim(u8, t[0..open], &std.ascii.whitespace);

    // Reject if func_raw contains whitespace (e.g. column names with spaces never reach here)
    if (std.mem.indexOfAny(u8, func_raw, " \t\r\n") != null) return null;

    var fn_buf: [16]u8 = undefined;
    if (func_raw.len > fn_buf.len) return null;
    const fn_lower = std.ascii.lowerString(fn_buf[0..func_raw.len], func_raw);

    const is_datediff = std.mem.eql(u8, fn_lower, "datediff");
    const is_dateadd = std.mem.eql(u8, fn_lower, "dateadd");
    const is_extract = std.mem.eql(u8, fn_lower, "extract");

    if (!is_datediff and !is_dateadd and !is_extract) return null;

    // Find the matching closing parenthesis (depth-aware)
    var depth: usize = 0;
    var close_idx: ?usize = null;
    for (t[open..], open..) |c, i| {
        if (c == '(') depth += 1;
        if (c == ')') {
            depth -= 1;
            if (depth == 0) {
                close_idx = i;
                break;
            }
        }
    }
    const close = close_idx orelse return error.InvalidExpression;

    const args_str = std.mem.trim(u8, t[open + 1 .. close], &std.ascii.whitespace);

    // Parse operator and RHS that follow the closing paren
    const after_fn = std.mem.trim(u8, t[close + 1 ..], &std.ascii.whitespace);
    if (after_fn.len == 0) return null; // bare function call in WHERE, not a comparison

    const op_str: []const u8 =
        if (std.mem.startsWith(u8, after_fn, ">=")) ">=" else if (std.mem.startsWith(u8, after_fn, "<=")) "<=" else if (std.mem.startsWith(u8, after_fn, "!=")) "!=" else if (after_fn[0] == '=') "=" else if (after_fn[0] == '>') ">" else if (after_fn[0] == '<') "<" else return null;

    const operator = Operator.fromString(op_str) orelse return null;
    const rhs_raw = std.mem.trim(u8, after_fn[op_str.len..], &std.ascii.whitespace);
    const rhs_value = std.fmt.parseFloat(f64, rhs_raw) catch return error.InvalidExpression;

    // Allocate the (lowercase) function name — used as a tag at eval time
    const func_alloc = try allocator.dupe(u8, fn_lower);
    errdefer allocator.free(func_alloc);

    if (is_datediff) {
        // DATEDIFF('unit', start_col, end_col)
        var parts = std.mem.splitScalar(u8, args_str, ',');
        const unit_raw = std.mem.trim(u8, parts.next() orelse return error.InvalidExpression, &std.ascii.whitespace);
        const start_str = std.mem.trim(u8, parts.next() orelse return error.InvalidExpression, &std.ascii.whitespace);
        const end_str = std.mem.trim(u8, parts.next() orelse return error.InvalidExpression, &std.ascii.whitespace);

        const unit_clean = trimQuotes(unit_raw);
        const unit_alloc = try allocator.dupe(u8, unit_clean);
        errdefer allocator.free(unit_alloc);

        const col1_buf = try allocator.alloc(u8, start_str.len);
        errdefer allocator.free(col1_buf);
        _ = std.ascii.lowerString(col1_buf, start_str);

        const col2_buf = try allocator.alloc(u8, end_str.len);
        errdefer allocator.free(col2_buf);
        _ = std.ascii.lowerString(col2_buf, end_str);

        return Expression{ .scalar_comparison = .{
            .func = func_alloc,
            .unit = unit_alloc,
            .col1_name = col1_buf,
            .col2_name = col2_buf,
            .amount = 0,
            .operator = operator,
            .rhs_value = rhs_value,
        } };
    }

    if (is_dateadd) {
        // DATEADD('unit', amount, date_col)
        var parts = std.mem.splitScalar(u8, args_str, ',');
        const unit_raw = std.mem.trim(u8, parts.next() orelse return error.InvalidExpression, &std.ascii.whitespace);
        const amount_str = std.mem.trim(u8, parts.next() orelse return error.InvalidExpression, &std.ascii.whitespace);
        const date_str = std.mem.trim(u8, parts.next() orelse return error.InvalidExpression, &std.ascii.whitespace);

        const unit_clean = trimQuotes(unit_raw);
        const amount = std.fmt.parseInt(i32, amount_str, 10) catch return error.InvalidExpression;

        const unit_alloc = try allocator.dupe(u8, unit_clean);
        errdefer allocator.free(unit_alloc);

        const col1_buf = try allocator.alloc(u8, date_str.len);
        errdefer allocator.free(col1_buf);
        _ = std.ascii.lowerString(col1_buf, date_str);

        return Expression{ .scalar_comparison = .{
            .func = func_alloc,
            .unit = unit_alloc,
            .col1_name = col1_buf,
            .col2_name = null,
            .amount = amount,
            .operator = operator,
            .rhs_value = rhs_value,
        } };
    }

    // is_extract
    // EXTRACT(part FROM date_col)
    // Safety: args_str is the raw content between EXTRACT( and its closing ), e.g.
    // "year FROM event_date". EXTRACT syntax never contains string literals inside
    // the parentheses, so a simple case-insensitive search for " FROM " is safe.
    const from_idx = std.ascii.indexOfIgnoreCase(args_str, " FROM ") orelse return error.InvalidExpression;
    const part_raw = std.mem.trim(u8, args_str[0..from_idx], &std.ascii.whitespace);
    const date_str = std.mem.trim(u8, args_str[from_idx + 6 ..], &std.ascii.whitespace);

    const unit_alloc = try allocator.dupe(u8, part_raw);
    errdefer allocator.free(unit_alloc);

    const col1_buf = try allocator.alloc(u8, date_str.len);
    errdefer allocator.free(col1_buf);
    _ = std.ascii.lowerString(col1_buf, date_str);

    return Expression{ .scalar_comparison = .{
        .func = func_alloc,
        .unit = unit_alloc,
        .col1_name = col1_buf,
        .col2_name = null,
        .amount = 0,
        .operator = operator,
        .rhs_value = rhs_value,
    } };
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
        .scalar_comparison => |sc| {
            // Evaluate scalar comparison against the HashMap row (used by the JOIN path).
            // Look up column values by name, build a tiny temporary slice, and delegate to
            // the same scalar.eval hot path used by evaluateDirect.
            var stack_buf: [64]u8 = undefined;
            var fba = std.heap.FixedBufferAllocator.init(&stack_buf);
            const arena = fba.allocator();

            const result_str: []const u8 = if (std.mem.eql(u8, sc.func, "datediff")) blk: {
                const v1 = row.get(sc.col1_name) orelse return false;
                const col2_name = sc.col2_name orelse return false;
                const v2 = row.get(col2_name) orelse return false;
                const tmp = [_][]const u8{ v1, v2 };
                const spec = scalar.ScalarSpec{ .datediff = .{
                    .unit = sc.unit,
                    .start_col = 0,
                    .end_col = 1,
                } };
                break :blk scalar.eval(spec, &tmp, arena);
            } else if (std.mem.eql(u8, sc.func, "extract")) blk: {
                const v1 = row.get(sc.col1_name) orelse return false;
                const tmp = [_][]const u8{v1};
                const spec = scalar.ScalarSpec{ .extract = .{
                    .part = sc.unit,
                    .date_col = 0,
                } };
                break :blk scalar.eval(spec, &tmp, arena);
            } else return false; // DATEADD returns a datetime string; numeric comparison not supported

            const result = std.fmt.parseFloat(f64, result_str) catch return false;
            return switch (sc.operator) {
                .equal => result == sc.rhs_value,
                .not_equal => result != sc.rhs_value,
                .greater => result > sc.rhs_value,
                .greater_equal => result >= sc.rhs_value,
                .less => result < sc.rhs_value,
                .less_equal => result <= sc.rhs_value,
                .like, .ilike, .between, .is_null, .is_not_null => false,
            };
        },
        .binary => |bin| {
            const left_result = evaluate(bin.left, row);
            return switch (bin.op) {
                .@"and" => left_result and evaluate(bin.right, row),
                .@"or" => left_result or evaluate(bin.right, row),
            };
        },
        .unary => |un| {
            return !evaluate(un.expr, row);
        },
    }
}

/// Fast expression evaluation without HashMap construction.
/// Takes the parsed field array and lowercase column-name array directly.
/// Avoids per-row heap allocation for AND/OR/NOT WHERE clauses in hot loops.
pub fn evaluateDirect(expr: Expression, fields: []const []const u8, lower_header: []const []const u8) bool {
    switch (expr) {
        .comparison => |comp| {
            for (lower_header, 0..) |col, i| {
                if (std.mem.eql(u8, col, comp.column)) {
                    const value = if (i < fields.len) fields[i] else "";
                    return compareValues(comp, value);
                }
            }
            return false;
        },
        .scalar_comparison => |sc| {
            // Resolve col1_name → index
            const col1_idx: usize = blk: {
                for (lower_header, 0..) |col, i| {
                    if (std.mem.eql(u8, col, sc.col1_name)) break :blk i;
                }
                return false; // column not found
            };

            // Build the ScalarSpec and evaluate it using a tiny stack arena
            var stack_buf: [64]u8 = undefined;
            var fba = std.heap.FixedBufferAllocator.init(&stack_buf);
            const arena = fba.allocator();

            const result_str: []const u8 = if (std.mem.eql(u8, sc.func, "datediff")) inner: {
                const col2_name = sc.col2_name orelse return false;
                const col2_idx: usize = idx: {
                    for (lower_header, 0..) |col, i| {
                        if (std.mem.eql(u8, col, col2_name)) break :idx i;
                    }
                    return false;
                };
                const spec = scalar.ScalarSpec{ .datediff = .{
                    .unit = sc.unit,
                    .start_col = col1_idx,
                    .end_col = col2_idx,
                } };
                break :inner scalar.eval(spec, fields, arena);
            } else if (std.mem.eql(u8, sc.func, "dateadd")) {
                // DATEADD returns a formatted datetime string (e.g. "2026-01-22 08:00:00"),
                // which cannot be parsed as f64. Numeric/date comparisons on DATEADD results
                // in WHERE are not supported; skip this predicate.
                return false;
            } else if (std.mem.eql(u8, sc.func, "extract")) inner: {
                const spec = scalar.ScalarSpec{ .extract = .{
                    .part = sc.unit,
                    .date_col = col1_idx,
                } };
                break :inner scalar.eval(spec, fields, arena);
            } else return false;

            const result = std.fmt.parseFloat(f64, result_str) catch return false;

            return switch (sc.operator) {
                .equal => result == sc.rhs_value,
                .not_equal => result != sc.rhs_value,
                .greater => result > sc.rhs_value,
                .greater_equal => result >= sc.rhs_value,
                .less => result < sc.rhs_value,
                .less_equal => result <= sc.rhs_value,
                .like, .ilike, .between, .is_null, .is_not_null => false,
            };
        },
        .binary => |bin| {
            return switch (bin.op) {
                .@"and" => evaluateDirect(bin.left, fields, lower_header) and
                    evaluateDirect(bin.right, fields, lower_header),
                .@"or" => evaluateDirect(bin.left, fields, lower_header) or
                    evaluateDirect(bin.right, fields, lower_header),
            };
        },
        .unary => |un| {
            return !evaluateDirect(un.expr, fields, lower_header);
        },
    }
}

pub fn compareValues(comp: Comparison, candidate: []const u8) bool {
    // IS NULL / IS NOT NULL
    if (comp.operator == .is_null) return candidate.len == 0;
    if (comp.operator == .is_not_null) return candidate.len != 0;

    // IN (...) — check membership against the list of values
    if (comp.in_values) |vals| {
        for (vals) |v| {
            if (std.mem.eql(u8, candidate, v)) return true;
        }
        return false;
    }

    // BETWEEN low AND high
    if (comp.operator == .between) {
        if (comp.numeric_value) |low| {
            const high = comp.between_high_num orelse return false;
            const val = std.fmt.parseFloat(f64, candidate) catch return false;
            return val >= low and val <= high;
        } else {
            const high = comp.between_high orelse return false;
            return std.mem.order(u8, candidate, comp.value) != .lt and
                std.mem.order(u8, candidate, high) != .gt;
        }
    }

    // LIKE / ILIKE are always string pattern matches — skip numeric path entirely
    if (comp.operator == .like) return matchLike(candidate, comp.value);
    if (comp.operator == .ilike) return matchILike(candidate, comp.value);

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
                .like, .ilike, .between, .is_null, .is_not_null => unreachable, // handled above
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
                .like, .ilike, .between, .is_null, .is_not_null => unreachable, // handled above
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
        .ilike => matchILike(candidate, comp.value),
        .between, .is_null, .is_not_null => unreachable, // handled above
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
