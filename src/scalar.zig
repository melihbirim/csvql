/// scalar.zig — Scalar function evaluation for SELECT columns.
///
/// Supports: UPPER, LOWER, TRIM, LENGTH, SUBSTR/SUBSTRING,
///           ABS, CEIL, FLOOR, MOD, COALESCE, CAST(col AS type)
///
/// Usage:
///   1. Call tryParseScalar(expr, column_map, allocator) at query setup time to
///      get a ScalarSpec.
///   2. Call eval(spec, record, arena) per row in the hot loop; arena is reset
///      after each row so allocations are free-list amortized.
///   3. Use OutputColSpec + evalOutputCol for unified column projection.
const std = @import("std");
const Allocator = std.mem.Allocator;

// ──────────────────────────────────────────────────────────────────────────────
// Public types
// ──────────────────────────────────────────────────────────────────────────────

/// A parsed scalar function applied to a single CSV column.
pub const ScalarSpec = union(enum) {
    upper: usize, // UPPER(col)
    lower: usize, // LOWER(col)
    trim: usize, // TRIM(col)
    length: usize, // LENGTH(col) — returns character count as string
    substr: SubstrArgs, // SUBSTR(col, start[, len]) — 1-based SQL semantics
    abs: usize, // ABS(col)
    ceil: usize, // CEIL(col)
    floor: usize, // FLOOR(col)
    mod_op: ModArgs, // MOD(col, divisor)
    coalesce: CoalesceArgs, // COALESCE(col, fallback_literal)
    cast_int: usize, // CAST(col AS INTEGER/BIGINT)
    cast_float: usize, // CAST(col AS FLOAT/REAL/NUMERIC/DECIMAL)
    cast_text: usize, // CAST(col AS TEXT/VARCHAR/CHAR/STRING) — identity

    pub const SubstrArgs = struct {
        col_idx: usize,
        start: i32, // 1-based; negative = count from end
        len: i32, // -1 = to end of string
    };

    pub const ModArgs = struct {
        col_idx: usize,
        divisor: f64,
    };

    /// fallback points into the original query string (borrowed, not owned).
    pub const CoalesceArgs = struct {
        col_idx: usize,
        fallback: []const u8,
    };

    /// Return the primary column index this spec operates on.
    pub fn colIdx(self: ScalarSpec) usize {
        return switch (self) {
            .upper, .lower, .trim, .length, .abs, .ceil, .floor, .cast_int, .cast_float, .cast_text => |i| i,
            .substr => |a| a.col_idx,
            .mod_op => |a| a.col_idx,
            .coalesce => |a| a.col_idx,
        };
    }
};

/// A SELECT output column: either a direct field pass-through or a scalar transform.
pub const OutputColSpec = union(enum) {
    column: usize,
    scalar: ScalarSpec,
};

// ──────────────────────────────────────────────────────────────────────────────
// Parsing
// ──────────────────────────────────────────────────────────────────────────────

/// Try to parse `expr` (alias already stripped) as a scalar function call.
/// Returns null  — not a scalar function (caller should try plain column lookup).
/// Returns spec  — parsed scalar function.
/// Returns error — function recognised but column/args invalid.
///
/// `column_map` maps lowercase column names to CSV field indices.
/// `allocator`  is used only for temporary lowercase buffers; nothing is retained.
pub fn tryParseScalar(
    expr: []const u8,
    column_map: std.StringHashMap(usize),
    allocator: Allocator,
) !?ScalarSpec {
    const t = std.mem.trim(u8, expr, &std.ascii.whitespace);

    // Must have balanced parens: '(' somewhere and ')' as last non-whitespace char.
    const open = std.mem.indexOf(u8, t, "(") orelse return null;
    if (t[t.len - 1] != ')') return null;

    const func_raw = std.mem.trim(u8, t[0..open], &std.ascii.whitespace);
    const args_str = std.mem.trim(u8, t[open + 1 .. t.len - 1], &std.ascii.whitespace);

    // Lowercase function name (stack buffer for names ≤64 chars)
    var fn_buf: [64]u8 = undefined;
    if (func_raw.len > fn_buf.len) return null;
    const fn_lower = std.ascii.lowerString(fn_buf[0..func_raw.len], func_raw);

    // ── Single-argument functions ──────────────────────────────────────────
    const single_arg_fn = std.mem.eql(u8, fn_lower, "upper") or
        std.mem.eql(u8, fn_lower, "lower") or
        std.mem.eql(u8, fn_lower, "trim") or
        std.mem.eql(u8, fn_lower, "length") or
        std.mem.eql(u8, fn_lower, "abs") or
        std.mem.eql(u8, fn_lower, "ceil") or
        std.mem.eql(u8, fn_lower, "ceiling") or
        std.mem.eql(u8, fn_lower, "floor");

    if (single_arg_fn) {
        const cidx = try resolveCol(args_str, column_map, allocator) orelse
            return error.ColumnNotFound;
        if (std.mem.eql(u8, fn_lower, "upper")) return .{ .upper = cidx };
        if (std.mem.eql(u8, fn_lower, "lower")) return .{ .lower = cidx };
        if (std.mem.eql(u8, fn_lower, "trim")) return .{ .trim = cidx };
        if (std.mem.eql(u8, fn_lower, "length")) return .{ .length = cidx };
        if (std.mem.eql(u8, fn_lower, "abs")) return .{ .abs = cidx };
        if (std.mem.eql(u8, fn_lower, "ceil") or std.mem.eql(u8, fn_lower, "ceiling")) return .{ .ceil = cidx };
        if (std.mem.eql(u8, fn_lower, "floor")) return .{ .floor = cidx };
    }

    // ── SUBSTR / SUBSTRING ─────────────────────────────────────────────────
    if (std.mem.eql(u8, fn_lower, "substr") or std.mem.eql(u8, fn_lower, "substring")) {
        // Split by first comma (col, start[, len])
        const comma1 = std.mem.indexOfScalar(u8, args_str, ',') orelse return null;
        const col_str = std.mem.trim(u8, args_str[0..comma1], &std.ascii.whitespace);
        const rest = std.mem.trim(u8, args_str[comma1 + 1 ..], &std.ascii.whitespace);

        const cidx = try resolveCol(col_str, column_map, allocator) orelse
            return error.ColumnNotFound;

        const comma2 = std.mem.indexOfScalar(u8, rest, ',');
        const start_str = if (comma2) |c| std.mem.trim(u8, rest[0..c], &std.ascii.whitespace) else rest;
        const len_str: ?[]const u8 = if (comma2) |c| std.mem.trim(u8, rest[c + 1 ..], &std.ascii.whitespace) else null;

        const start = std.fmt.parseInt(i32, start_str, 10) catch return null;
        const len: i32 = if (len_str) |ls|
            std.fmt.parseInt(i32, ls, 10) catch return null
        else
            -1;

        return .{ .substr = .{ .col_idx = cidx, .start = start, .len = len } };
    }

    // ── MOD ────────────────────────────────────────────────────────────────
    if (std.mem.eql(u8, fn_lower, "mod")) {
        const comma = std.mem.indexOfScalar(u8, args_str, ',') orelse return null;
        const col_str = std.mem.trim(u8, args_str[0..comma], &std.ascii.whitespace);
        const div_str = std.mem.trim(u8, args_str[comma + 1 ..], &std.ascii.whitespace);

        const cidx = try resolveCol(col_str, column_map, allocator) orelse
            return error.ColumnNotFound;
        const divisor = std.fmt.parseFloat(f64, div_str) catch return null;

        return .{ .mod_op = .{ .col_idx = cidx, .divisor = divisor } };
    }

    // ── COALESCE ───────────────────────────────────────────────────────────
    if (std.mem.eql(u8, fn_lower, "coalesce")) {
        const comma = std.mem.indexOfScalar(u8, args_str, ',') orelse return null;
        const col_str = std.mem.trim(u8, args_str[0..comma], &std.ascii.whitespace);
        const fallback_raw = std.mem.trim(u8, args_str[comma + 1 ..], &std.ascii.whitespace);

        const cidx = try resolveCol(col_str, column_map, allocator) orelse
            return error.ColumnNotFound;

        // Strip surrounding single-quotes from the fallback literal
        const fallback = if (fallback_raw.len >= 2 and
            fallback_raw[0] == '\'' and fallback_raw[fallback_raw.len - 1] == '\'')
            fallback_raw[1 .. fallback_raw.len - 1]
        else
            fallback_raw;

        return .{ .coalesce = .{ .col_idx = cidx, .fallback = fallback } };
    }

    // ── CAST ───────────────────────────────────────────────────────────────
    if (std.mem.eql(u8, fn_lower, "cast")) {
        // Expect: col AS type
        const as_idx = std.ascii.indexOfIgnoreCase(args_str, " AS ") orelse return null;
        const col_str = std.mem.trim(u8, args_str[0..as_idx], &std.ascii.whitespace);
        const type_raw = std.mem.trim(u8, args_str[as_idx + 4 ..], &std.ascii.whitespace);

        const cidx = try resolveCol(col_str, column_map, allocator) orelse
            return error.ColumnNotFound;

        var type_buf: [32]u8 = undefined;
        const tlen = @min(type_raw.len, type_buf.len);
        const type_lower = std.ascii.lowerString(type_buf[0..tlen], type_raw[0..tlen]);

        if (std.mem.startsWith(u8, type_lower, "int") or
            std.mem.eql(u8, type_lower, "bigint") or
            std.mem.eql(u8, type_lower, "smallint") or
            std.mem.eql(u8, type_lower, "tinyint"))
            return .{ .cast_int = cidx };

        if (std.mem.startsWith(u8, type_lower, "float") or
            std.mem.startsWith(u8, type_lower, "double") or
            std.mem.startsWith(u8, type_lower, "real") or
            std.mem.startsWith(u8, type_lower, "numeric") or
            std.mem.startsWith(u8, type_lower, "decimal"))
            return .{ .cast_float = cidx };

        if (std.mem.startsWith(u8, type_lower, "text") or
            std.mem.startsWith(u8, type_lower, "varchar") or
            std.mem.startsWith(u8, type_lower, "char") or
            std.mem.startsWith(u8, type_lower, "string") or
            std.mem.startsWith(u8, type_lower, "nvar"))
            return .{ .cast_text = cidx };

        return null; // unknown type — fall through to column lookup
    }

    return null; // not a recognized scalar function
}

// ──────────────────────────────────────────────────────────────────────────────
// Evaluation
// ──────────────────────────────────────────────────────────────────────────────

/// Evaluate a scalar spec against a CSV record row.
/// Transformations that produce new strings (UPPER, LOWER, numeric formatting)
/// allocate from `arena`; the caller is responsible for resetting the arena
/// between rows to bound memory usage.
pub fn eval(spec: ScalarSpec, record: []const []const u8, arena: Allocator) []const u8 {
    switch (spec) {
        .upper => |cidx| {
            const v = field(record, cidx);
            if (v.len == 0) return v;
            const buf = arena.alloc(u8, v.len) catch return v;
            return std.ascii.upperString(buf, v);
        },
        .lower => |cidx| {
            const v = field(record, cidx);
            if (v.len == 0) return v;
            const buf = arena.alloc(u8, v.len) catch return v;
            return std.ascii.lowerString(buf, v);
        },
        .trim => |cidx| {
            return std.mem.trim(u8, field(record, cidx), &std.ascii.whitespace);
        },
        .length => |cidx| {
            const v = field(record, cidx);
            const buf = arena.alloc(u8, 20) catch return "0";
            return std.fmt.bufPrint(buf, "{d}", .{v.len}) catch "0";
        },
        .substr => |args| {
            const v = field(record, args.col_idx);
            if (v.len == 0) return v;
            // SQL SUBSTR is 1-based; start <= 0 treated as 1.
            const start0: usize = if (args.start >= 1)
                @min(@as(usize, @intCast(args.start)) - 1, v.len)
            else if (args.start < 0)
                @intCast(@max(0, @as(i32, @intCast(v.len)) + args.start))
            else
                0;
            if (args.len < 0) return v[start0..];
            const end = @min(start0 + @as(usize, @intCast(args.len)), v.len);
            return v[start0..end];
        },
        .abs => |cidx| {
            const v = field(record, cidx);
            const n = std.fmt.parseFloat(f64, v) catch return v;
            const buf = arena.alloc(u8, 32) catch return v;
            return fmtNum(buf, @abs(n));
        },
        .ceil => |cidx| {
            const v = field(record, cidx);
            const n = std.fmt.parseFloat(f64, v) catch return v;
            const buf = arena.alloc(u8, 32) catch return v;
            return fmtNum(buf, @ceil(n));
        },
        .floor => |cidx| {
            const v = field(record, cidx);
            const n = std.fmt.parseFloat(f64, v) catch return v;
            const buf = arena.alloc(u8, 32) catch return v;
            return fmtNum(buf, @floor(n));
        },
        .mod_op => |args| {
            const v = field(record, args.col_idx);
            const n = std.fmt.parseFloat(f64, v) catch return v;
            const buf = arena.alloc(u8, 32) catch return v;
            return fmtNum(buf, @mod(n, args.divisor));
        },
        .coalesce => |args| {
            const v = field(record, args.col_idx);
            const trimmed = std.mem.trim(u8, v, &std.ascii.whitespace);
            return if (trimmed.len == 0) args.fallback else v;
        },
        .cast_int => |cidx| {
            const v = field(record, cidx);
            const n = std.fmt.parseFloat(f64, v) catch return v;
            const buf = arena.alloc(u8, 32) catch return v;
            return std.fmt.bufPrint(buf, "{d}", .{@as(i64, @intFromFloat(n))}) catch v;
        },
        .cast_float => |cidx| {
            const v = field(record, cidx);
            _ = std.fmt.parseFloat(f64, v) catch return v; // validate; return original if ok (no-op)
            return v;
        },
        .cast_text => |cidx| {
            return field(record, cidx);
        },
    }
}

/// Evaluate an OutputColSpec (direct pass-through or scalar transform).
pub fn evalOutputCol(spec: OutputColSpec, record: []const []const u8, arena: Allocator) []const u8 {
    return switch (spec) {
        .column => |cidx| field(record, cidx),
        .scalar => |sc| eval(sc, record, arena),
    };
}

// ──────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ──────────────────────────────────────────────────────────────────────────────

/// Safe indexed field access (returns "" when index is out of range).
inline fn field(record: []const []const u8, idx: usize) []const u8 {
    return if (idx < record.len) record[idx] else "";
}

/// Resolve a column name string to its CSV field index.
/// Returns null when `col_str` is not a plain column name (e.g. contains parens).
fn resolveCol(col_str: []const u8, column_map: std.StringHashMap(usize), allocator: Allocator) !?usize {
    const trimmed = std.mem.trim(u8, col_str, &std.ascii.whitespace);
    // Reject anything that looks like a nested function call
    if (std.mem.indexOf(u8, trimmed, "(") != null) return null;

    var buf: [256]u8 = undefined;
    if (trimmed.len > buf.len) {
        // Fallback: heap allocation for very long names
        const lower = try allocator.alloc(u8, trimmed.len);
        defer allocator.free(lower);
        _ = std.ascii.lowerString(lower, trimmed);
        return column_map.get(lower);
    }
    const lower = std.ascii.lowerString(buf[0..trimmed.len], trimmed);
    return column_map.get(lower);
}

/// Format a float as an integer string when it has no fractional part,
/// otherwise format as a float.  Writes into `buf` (must be ≥32 bytes).
pub fn fmtNum(buf: []u8, n: f64) []const u8 {
    if (n == @trunc(n) and n >= -1e15 and n <= 1e15) {
        return std.fmt.bufPrint(buf, "{d}", .{@as(i64, @intFromFloat(n))}) catch buf[0..0];
    }
    return std.fmt.bufPrint(buf, "{d}", .{n}) catch buf[0..0];
}
