/// scalar.zig — Scalar function evaluation for SELECT columns.
///
/// Supports: UPPER, LOWER, TRIM, LENGTH, SUBSTR/SUBSTRING,
///           ABS, CEIL, FLOOR, MOD, COALESCE, CAST(col AS type), REPLACE,
///           SPLIT_PART, GREATEST, LEAST
///
/// Usage:
///   1. Call tryParseScalar(expr, column_map, allocator) at query setup time to
///      get a ScalarSpec.
///   2. Call eval(spec, record, arena) per row in the hot loop; arena is reset
///      after each row so allocations are free-list amortized.
///   3. Use OutputColSpec + evalOutputCol for unified column projection.
const std = @import("std");
const Allocator = std.mem.Allocator;
const datetime = @import("datetime.zig");

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
    datediff: DatediffArgs, // DATEDIFF(unit, start_col, end_col)
    dateadd: DateaddArgs, // DATEADD(unit, amount, date_col)
    extract: ExtractArgs, // EXTRACT(part FROM date_col)
    round_op: RoundArgs, // ROUND(col[, digits])
    replace: ReplaceArgs, // REPLACE(col, 'from', 'to') — replace all occurrences
    split_part: SplitPartArgs, // SPLIT_PART(col, 'delim', n) — n-th field (1-based)
    greatest: VariadicArgs, // GREATEST(a, b, ...) — row-wise max
    least: VariadicArgs, // LEAST(a, b, ...) — row-wise min
    case_when: CaseWhenArgs, // bare CASE WHEN ... THEN ... ELSE ... END (#105)

    pub const ReplaceArgs = struct {
        col_idx: usize,
        from: []const u8, // slice into the query expr (query-lifetime)
        to: []const u8,
    };

    pub const SplitPartArgs = struct {
        col_idx: usize,
        delim: []const u8, // slice into the query expr
        n: usize, // 1-based field index
    };

    /// Up to 8 column args for GREATEST/LEAST.
    pub const VariadicArgs = struct {
        cols_buf: [8]usize = undefined,
        cols_len: usize,

        pub fn cols(self: *const VariadicArgs) []const usize {
            return self.cols_buf[0..self.cols_len];
        }
        pub fn colsMut(self: *VariadicArgs) []usize {
            return self.cols_buf[0..self.cols_len];
        }
    };

    pub const SubstrArgs = struct {
        col_idx: usize,
        start: i32, // 1-based; negative = count from end
        len: i32, // -1 = to end of string
    };

    pub const ModArgs = struct {
        col_idx: usize,
        divisor: f64,
    };

    /// Inline fixed-size buffer — no heap allocation. Supports up to 8 column args.
    pub const CoalesceArgs = struct {
        cols_buf: [8]usize = undefined,
        cols_len: usize,
        fallback: []const u8,

        pub fn cols(self: *const CoalesceArgs) []const usize {
            return self.cols_buf[0..self.cols_len];
        }

        pub fn colsMut(self: *CoalesceArgs) []usize {
            return self.cols_buf[0..self.cols_len];
        }
    };

    pub const DatediffArgs = struct {
        unit: []const u8, // 'second', 'minute', 'hour', 'day', 'week', 'month', 'year'
        start_col: usize,
        end_col: usize,
    };

    pub const DateaddArgs = struct {
        unit: []const u8,
        amount: i32,
        date_col: usize,
    };

    pub const ExtractArgs = struct {
        part: []const u8, // 'year', 'month', 'day', 'hour', 'minute', 'second'
        date_col: usize,
    };

    pub const RoundArgs = struct {
        col_idx: usize,
        digits: u8, // number of decimal places (0 = round to integer)
    };

    /// THEN/ELSE branch value for a bare CASE WHEN (issue #105): a quoted
    /// string literal, a bare numeric literal, or a column reference.
    pub const CaseValue = union(enum) {
        string_lit: []const u8,
        numeric_lit: f64,
        col_idx: usize,
    };

    pub const CaseOp = enum { eq, ne, gt, ge, lt, le };

    /// A bare `CASE WHEN col op value THEN a ELSE b END` in a plain SELECT
    /// column (not wrapped in an aggregate — that path is parseCaseAggCall
    /// in engine.zig, numeric-only). Single condition, no AND/OR/nesting,
    /// same scope the aggregate version already supports.
    pub const CaseWhenArgs = struct {
        cond_col_idx: usize,
        op: CaseOp,
        rhs_numeric: ?f64, // set when the WHEN condition's RHS is numeric
        rhs_string: ?[]const u8, // set when the WHEN condition's RHS is a string literal
        then_val: CaseValue,
        else_val: CaseValue,
    };

    /// Return the primary column index this spec operates on.
    pub fn colIdx(self: ScalarSpec) usize {
        return switch (self) {
            .upper, .lower, .trim, .length, .abs, .ceil, .floor, .cast_int, .cast_float, .cast_text => |i| i,
            .substr => |a| a.col_idx,
            .mod_op => |a| a.col_idx,
            .coalesce => |a| a.cols()[0],
            .datediff => |a| a.start_col, // return first column
            .dateadd => |a| a.date_col,
            .extract => |a| a.date_col,
            .round_op => |a| a.col_idx,
            .replace => |a| a.col_idx,
            .split_part => |a| a.col_idx,
            .greatest, .least => |a| a.cols()[0],
            .case_when => |a| a.cond_col_idx,
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

    // Bare CASE WHEN has no parens at all, so it must be checked before the
    // paren-based dispatch below rejects it outright (#105).
    if (std.ascii.startsWithIgnoreCase(t, "case")) {
        if (try tryParseCaseWhen(t, column_map, allocator)) |args| {
            return .{ .case_when = args };
        }
        return null;
    }

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

    // ── REPLACE(col, 'from', 'to') ─────────────────────────────────────────
    if (std.mem.eql(u8, fn_lower, "replace")) {
        // col is up to the first comma (column names have no commas); the two
        // string literals may themselves contain commas, so scan by quotes.
        const comma1 = std.mem.indexOfScalar(u8, args_str, ',') orelse return null;
        const col_str = std.mem.trim(u8, args_str[0..comma1], &std.ascii.whitespace);
        const cidx = try resolveCol(col_str, column_map, allocator) orelse
            return error.ColumnNotFound;

        const rest = args_str[comma1 + 1 ..];
        const q1 = std.mem.indexOfScalar(u8, rest, '\'') orelse return null;
        const q2 = std.mem.indexOfScalarPos(u8, rest, q1 + 1, '\'') orelse return null;
        const from = rest[q1 + 1 .. q2];
        if (from.len == 0) return null; // empty search would be a no-op / infinite

        const after = rest[q2 + 1 ..];
        const sep = std.mem.indexOfScalar(u8, after, ',') orelse return null;
        const tail = after[sep + 1 ..];
        const q3 = std.mem.indexOfScalar(u8, tail, '\'') orelse return null;
        const q4 = std.mem.indexOfScalarPos(u8, tail, q3 + 1, '\'') orelse return null;
        const to = tail[q3 + 1 .. q4];

        return .{ .replace = .{ .col_idx = cidx, .from = from, .to = to } };
    }

    // ── SPLIT_PART(col, 'delim', n) ────────────────────────────────────────
    if (std.mem.eql(u8, fn_lower, "split_part")) {
        const comma1 = std.mem.indexOfScalar(u8, args_str, ',') orelse return null;
        const col_str = std.mem.trim(u8, args_str[0..comma1], &std.ascii.whitespace);
        const cidx = try resolveCol(col_str, column_map, allocator) orelse
            return error.ColumnNotFound;
        const rest = args_str[comma1 + 1 ..];
        const q1 = std.mem.indexOfScalar(u8, rest, '\'') orelse return null;
        const q2 = std.mem.indexOfScalarPos(u8, rest, q1 + 1, '\'') orelse return null;
        const delim = rest[q1 + 1 .. q2];
        if (delim.len == 0) return null;
        const after = rest[q2 + 1 ..];
        const sep = std.mem.indexOfScalar(u8, after, ',') orelse return null;
        const n_str = std.mem.trim(u8, after[sep + 1 ..], &std.ascii.whitespace);
        const n = std.fmt.parseInt(usize, n_str, 10) catch return null;
        if (n == 0) return null; // SPLIT_PART is 1-based
        return .{ .split_part = .{ .col_idx = cidx, .delim = delim, .n = n } };
    }

    // ── GREATEST / LEAST(a, b, ...) ────────────────────────────────────────
    if (std.mem.eql(u8, fn_lower, "greatest") or std.mem.eql(u8, fn_lower, "least")) {
        var v = ScalarSpec.VariadicArgs{ .cols_len = 0 };
        var it = std.mem.splitScalar(u8, args_str, ',');
        while (it.next()) |token| {
            const arg = std.mem.trim(u8, token, &std.ascii.whitespace);
            if (arg.len == 0) continue;
            if (v.cols_len >= v.cols_buf.len) return error.TooManyArgs;
            const cidx = try resolveCol(arg, column_map, allocator) orelse
                return error.ColumnNotFound;
            v.cols_buf[v.cols_len] = cidx;
            v.cols_len += 1;
        }
        if (v.cols_len < 2) return null;
        return if (std.mem.eql(u8, fn_lower, "greatest")) .{ .greatest = v } else .{ .least = v };
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

    // ── ROUND ──────────────────────────────────────────────────────────────
    if (std.mem.eql(u8, fn_lower, "round")) {
        // ROUND(col) or ROUND(col, digits)
        const comma = std.mem.indexOfScalar(u8, args_str, ',');
        const col_str = std.mem.trim(u8, if (comma) |c| args_str[0..c] else args_str, &std.ascii.whitespace);
        const digits: u8 = if (comma) |c| blk: {
            const d_str = std.mem.trim(u8, args_str[c + 1 ..], &std.ascii.whitespace);
            const d = std.fmt.parseInt(u8, d_str, 10) catch return null;
            break :blk d;
        } else 0;

        const cidx = try resolveCol(col_str, column_map, allocator) orelse
            return error.ColumnNotFound;

        return .{ .round_op = .{ .col_idx = cidx, .digits = digits } };
    }

    // ── COALESCE ───────────────────────────────────────────────────────────
    if (std.mem.eql(u8, fn_lower, "coalesce")) {
        var args = ScalarSpec.CoalesceArgs{ .cols_len = 0, .fallback = "" };
        var fallback: []const u8 = "";

        var it = std.mem.splitScalar(u8, args_str, ',');
        while (it.next()) |token| {
            const arg = std.mem.trim(u8, token, &std.ascii.whitespace);
            if (arg.len >= 2 and arg[0] == '\'' and arg[arg.len - 1] == '\'') {
                fallback = arg[1 .. arg.len - 1];
            } else {
                if (args.cols_len >= args.cols_buf.len) return error.TooManyArgs;
                const cidx = try resolveCol(arg, column_map, allocator) orelse
                    return error.ColumnNotFound;
                args.cols_buf[args.cols_len] = cidx;
                args.cols_len += 1;
            }
        }

        if (args.cols_len == 0) return null;
        args.fallback = fallback;
        return .{ .coalesce = args };
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

    // ── DATEDIFF ───────────────────────────────────────────────────────────
    if (std.mem.eql(u8, fn_lower, "datediff")) {
        // DATEDIFF(unit, start_col, end_col)
        // Split by commas
        var parts = std.mem.splitScalar(u8, args_str, ',');
        const unit_raw = std.mem.trim(u8, parts.next() orelse return null, &std.ascii.whitespace);
        const start_str = std.mem.trim(u8, parts.next() orelse return null, &std.ascii.whitespace);
        const end_str = std.mem.trim(u8, parts.next() orelse return null, &std.ascii.whitespace);

        // Strip quotes from unit string
        const unit = if (unit_raw.len >= 2 and unit_raw[0] == '\'' and unit_raw[unit_raw.len - 1] == '\'')
            unit_raw[1 .. unit_raw.len - 1]
        else
            unit_raw;

        const start_col = try resolveCol(start_str, column_map, allocator) orelse
            return error.ColumnNotFound;
        const end_col = try resolveCol(end_str, column_map, allocator) orelse
            return error.ColumnNotFound;

        return .{ .datediff = .{ .unit = unit, .start_col = start_col, .end_col = end_col } };
    }

    // ── DATEADD ────────────────────────────────────────────────────────────
    if (std.mem.eql(u8, fn_lower, "dateadd")) {
        // DATEADD(unit, amount, date_col)
        var parts = std.mem.splitScalar(u8, args_str, ',');
        const unit_raw = std.mem.trim(u8, parts.next() orelse return null, &std.ascii.whitespace);
        const amount_str = std.mem.trim(u8, parts.next() orelse return null, &std.ascii.whitespace);
        const date_str = std.mem.trim(u8, parts.next() orelse return null, &std.ascii.whitespace);

        // Strip quotes from unit string
        const unit = if (unit_raw.len >= 2 and unit_raw[0] == '\'' and unit_raw[unit_raw.len - 1] == '\'')
            unit_raw[1 .. unit_raw.len - 1]
        else
            unit_raw;

        const amount = std.fmt.parseInt(i32, amount_str, 10) catch return null;
        const date_col = try resolveCol(date_str, column_map, allocator) orelse
            return error.ColumnNotFound;

        return .{ .dateadd = .{ .unit = unit, .amount = amount, .date_col = date_col } };
    }

    // ── EXTRACT ────────────────────────────────────────────────────────────
    if (std.mem.eql(u8, fn_lower, "extract")) {
        // EXTRACT(part FROM date_col)
        const from_idx = std.ascii.indexOfIgnoreCase(args_str, " FROM ") orelse return null;
        const part_raw = std.mem.trim(u8, args_str[0..from_idx], &std.ascii.whitespace);
        const date_str = std.mem.trim(u8, args_str[from_idx + 6 ..], &std.ascii.whitespace);

        const date_col = try resolveCol(date_str, column_map, allocator) orelse
            return error.ColumnNotFound;

        return .{ .extract = .{ .part = part_raw, .date_col = date_col } };
    }

    return null; // not a recognized scalar function
}

/// Parse a bare `CASE WHEN col op value THEN a ELSE b END` (no outer
/// aggregate wrapper — that's parseCaseAggCall in engine.zig). `t` is
/// already known to start with "CASE" (case-insensitive).
/// Single condition, no AND/OR/nesting — same scope as the aggregate version.
fn tryParseCaseWhen(t: []const u8, column_map: std.StringHashMap(usize), allocator: Allocator) !?ScalarSpec.CaseWhenArgs {
    const after_case = std.mem.trim(u8, t[4..], &std.ascii.whitespace);
    if (after_case.len < 5 or !std.ascii.eqlIgnoreCase(after_case[0..5], "WHEN ")) return null;
    const cond_and_rest = after_case[5..];

    const then_idx = std.ascii.indexOfIgnoreCase(cond_and_rest, " THEN ") orelse return null;
    const cond_str = std.mem.trim(u8, cond_and_rest[0..then_idx], &std.ascii.whitespace);
    const after_then = cond_and_rest[then_idx + 6 ..];

    const else_idx = std.ascii.indexOfIgnoreCase(after_then, " ELSE ") orelse return null;
    const then_str = std.mem.trim(u8, after_then[0..else_idx], &std.ascii.whitespace);
    const after_else = after_then[else_idx + 6 ..];

    const else_str = blk: {
        const trimmed_end = std.mem.trim(u8, after_else, &std.ascii.whitespace);
        if (trimmed_end.len >= 3 and std.ascii.eqlIgnoreCase(trimmed_end[trimmed_end.len - 3 ..], "END")) {
            break :blk std.mem.trim(u8, trimmed_end[0 .. trimmed_end.len - 3], &std.ascii.whitespace);
        }
        return null;
    };

    // Condition: "<col> <op> <value>" — single comparison, matching the
    // aggregate CASE WHEN's scope. Find the operator by scanning for the
    // longest match first so ">=" doesn't get cut short as ">".
    const Op = struct { text: []const u8, op: ScalarSpec.CaseOp };
    const ops = [_]Op{
        .{ .text = "!=", .op = .ne },
        .{ .text = "<>", .op = .ne },
        .{ .text = ">=", .op = .ge },
        .{ .text = "<=", .op = .le },
        .{ .text = "=", .op = .eq },
        .{ .text = ">", .op = .gt },
        .{ .text = "<", .op = .lt },
    };
    var found_op: ?ScalarSpec.CaseOp = null;
    var op_pos: usize = 0;
    var op_len: usize = 0;
    for (ops) |o| {
        if (std.mem.indexOf(u8, cond_str, o.text)) |pos| {
            if (found_op == null or pos < op_pos) {
                found_op = o.op;
                op_pos = pos;
                op_len = o.text.len;
            }
        }
    }
    const op = found_op orelse return null;
    const cond_col_str = std.mem.trim(u8, cond_str[0..op_pos], &std.ascii.whitespace);
    const rhs_str = std.mem.trim(u8, cond_str[op_pos + op_len ..], &std.ascii.whitespace);

    const cond_col_idx = try resolveCol(cond_col_str, column_map, allocator) orelse
        return error.ColumnNotFound;

    var rhs_numeric: ?f64 = null;
    var rhs_string: ?[]const u8 = null;
    if (rhs_str.len >= 2 and rhs_str[0] == '\'' and rhs_str[rhs_str.len - 1] == '\'') {
        rhs_string = rhs_str[1 .. rhs_str.len - 1];
    } else if (std.fmt.parseFloat(f64, rhs_str)) |v| {
        rhs_numeric = v;
    } else |_| {
        return null; // unrecognized RHS — not a column ref, matches aggregate CASE's scope
    }

    const then_val = try parseCaseValue(then_str, column_map, allocator);
    const else_val = try parseCaseValue(else_str, column_map, allocator);

    return ScalarSpec.CaseWhenArgs{
        .cond_col_idx = cond_col_idx,
        .op = op,
        .rhs_numeric = rhs_numeric,
        .rhs_string = rhs_string,
        .then_val = then_val,
        .else_val = else_val,
    };
}

/// THEN/ELSE branch: quoted string literal, numeric literal, or column reference.
fn parseCaseValue(s: []const u8, column_map: std.StringHashMap(usize), allocator: Allocator) !ScalarSpec.CaseValue {
    if (s.len >= 2 and s[0] == '\'' and s[s.len - 1] == '\'') {
        return .{ .string_lit = s[1 .. s.len - 1] };
    }
    if (std.fmt.parseFloat(f64, s)) |v| {
        return .{ .numeric_lit = v };
    } else |_| {}
    const idx = try resolveCol(s, column_map, allocator) orelse return error.ColumnNotFound;
    return .{ .col_idx = idx };
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
        .replace => |args| {
            const v = field(record, args.col_idx);
            if (args.from.len == 0 or std.mem.indexOf(u8, v, args.from) == null) return v;
            const size = std.mem.replacementSize(u8, v, args.from, args.to);
            const buf = arena.alloc(u8, size) catch return v;
            _ = std.mem.replace(u8, v, args.from, args.to, buf);
            return buf;
        },
        .split_part => |args| {
            const v = field(record, args.col_idx);
            var it = std.mem.splitSequence(u8, v, args.delim);
            var i: usize = 1;
            while (it.next()) |part| : (i += 1) {
                if (i == args.n) return part;
            }
            return "";
        },
        .greatest => |args| return pickExtreme(record, args.cols(), true),
        .least => |args| return pickExtreme(record, args.cols(), false),
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
            return fmtFloat(buf, @ceil(n));
        },
        .floor => |cidx| {
            const v = field(record, cidx);
            const n = std.fmt.parseFloat(f64, v) catch return v;
            const buf = arena.alloc(u8, 32) catch return v;
            return fmtFloat(buf, @floor(n));
        },
        .mod_op => |args| {
            const v = field(record, args.col_idx);
            const n = std.fmt.parseFloat(f64, v) catch return v;
            const buf = arena.alloc(u8, 32) catch return v;
            return fmtNum(buf, @mod(n, args.divisor));
        },
        .coalesce => |args| {
            for (args.cols()) |cidx| {
                const v = field(record, cidx);
                if (std.mem.trim(u8, v, &std.ascii.whitespace).len > 0) return v;
            }
            return args.fallback;
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
        .datediff => |args| {
            const start_val = field(record, args.start_col);
            const end_val = field(record, args.end_col);

            // Parse both dates
            const start_ts = datetime.parseDateTime(start_val) catch return "0";
            const end_ts = datetime.parseDateTime(end_val) catch return "0";

            // Calculate difference based on unit
            const diff_seconds = end_ts - start_ts;
            const result: f64 = if (std.ascii.eqlIgnoreCase(args.unit, "second"))
                @floatFromInt(diff_seconds)
            else if (std.ascii.eqlIgnoreCase(args.unit, "minute"))
                @as(f64, @floatFromInt(diff_seconds)) / 60.0
            else if (std.ascii.eqlIgnoreCase(args.unit, "hour"))
                @as(f64, @floatFromInt(diff_seconds)) / 3600.0
            else if (std.ascii.eqlIgnoreCase(args.unit, "day"))
                @as(f64, @floatFromInt(diff_seconds)) / 86400.0
            else if (std.ascii.eqlIgnoreCase(args.unit, "week"))
                @as(f64, @floatFromInt(diff_seconds)) / 604800.0
            else if (std.ascii.eqlIgnoreCase(args.unit, "month"))
                @as(f64, @floatFromInt(diff_seconds)) / 2592000.0 // approximate: 30 days
            else if (std.ascii.eqlIgnoreCase(args.unit, "year"))
                @as(f64, @floatFromInt(diff_seconds)) / 31536000.0 // 365 days
            else
                0.0;

            const buf = arena.alloc(u8, 32) catch return "0";
            return fmtNum(buf, result);
        },
        .dateadd => |args| {
            const date_val = field(record, args.date_col);
            const base_ts = datetime.parseDateTime(date_val) catch return date_val;

            // Add amount based on unit
            const seconds_to_add: i64 = if (std.ascii.eqlIgnoreCase(args.unit, "second"))
                args.amount
            else if (std.ascii.eqlIgnoreCase(args.unit, "minute"))
                @as(i64, args.amount) * 60
            else if (std.ascii.eqlIgnoreCase(args.unit, "hour"))
                @as(i64, args.amount) * 3600
            else if (std.ascii.eqlIgnoreCase(args.unit, "day"))
                @as(i64, args.amount) * 86400
            else if (std.ascii.eqlIgnoreCase(args.unit, "week"))
                @as(i64, args.amount) * 604800
            else if (std.ascii.eqlIgnoreCase(args.unit, "month"))
                @as(i64, args.amount) * 2592000 // approximate: 30 days
            else if (std.ascii.eqlIgnoreCase(args.unit, "year"))
                @as(i64, args.amount) * 31536000 // 365 days
            else
                0;

            const new_ts = base_ts + seconds_to_add;
            return datetime.formatDateTime(arena, new_ts) catch date_val;
        },
        .round_op => |args| {
            const v = field(record, args.col_idx);
            const n = std.fmt.parseFloat(f64, v) catch return v;
            const buf = arena.alloc(u8, 64) catch return v;
            if (args.digits == 0) {
                const rounded = @round(n);
                return std.fmt.bufPrint(buf, "{d}", .{@as(i64, @intFromFloat(rounded))}) catch v;
            }
            // Shift, round, shift back
            const factor = std.math.pow(f64, 10.0, @as(f64, @floatFromInt(args.digits)));
            const rounded = @round(n * factor) / factor;
            return switch (args.digits) {
                1 => std.fmt.bufPrint(buf, "{d:.1}", .{rounded}) catch v,
                2 => std.fmt.bufPrint(buf, "{d:.2}", .{rounded}) catch v,
                3 => std.fmt.bufPrint(buf, "{d:.3}", .{rounded}) catch v,
                4 => std.fmt.bufPrint(buf, "{d:.4}", .{rounded}) catch v,
                5 => std.fmt.bufPrint(buf, "{d:.5}", .{rounded}) catch v,
                6 => std.fmt.bufPrint(buf, "{d:.6}", .{rounded}) catch v,
                7 => std.fmt.bufPrint(buf, "{d:.7}", .{rounded}) catch v,
                8 => std.fmt.bufPrint(buf, "{d:.8}", .{rounded}) catch v,
                else => std.fmt.bufPrint(buf, "{d:.9}", .{rounded}) catch v,
            };
        },
        .extract => |args| {
            const date_val = field(record, args.date_col);
            const ts = datetime.parseDateTime(date_val) catch return "0";
            const dt = datetime.DateTime.fromTimestamp(ts);

            const result: i64 = if (std.ascii.eqlIgnoreCase(args.part, "year"))
                dt.year
            else if (std.ascii.eqlIgnoreCase(args.part, "month"))
                dt.month
            else if (std.ascii.eqlIgnoreCase(args.part, "day"))
                dt.day
            else if (std.ascii.eqlIgnoreCase(args.part, "hour"))
                dt.hour
            else if (std.ascii.eqlIgnoreCase(args.part, "minute"))
                dt.minute
            else if (std.ascii.eqlIgnoreCase(args.part, "second"))
                dt.second
            else
                0;

            const buf = arena.alloc(u8, 32) catch return "0";
            return std.fmt.bufPrint(buf, "{d}", .{result}) catch "0";
        },
        .case_when => |args| {
            const fv = field(record, args.cond_col_idx);
            var matches = false;
            if (args.rhs_numeric) |threshold| {
                const val = std.fmt.parseFloat(f64, fv) catch {
                    return resolveCaseValue(args.else_val, record, arena);
                };
                matches = switch (args.op) {
                    .eq => val == threshold,
                    .ne => val != threshold,
                    .gt => val > threshold,
                    .ge => val >= threshold,
                    .lt => val < threshold,
                    .le => val <= threshold,
                };
            } else if (args.rhs_string) |rhs| {
                // Only equality/inequality are meaningful for string comparisons.
                matches = switch (args.op) {
                    .eq => std.mem.eql(u8, fv, rhs),
                    .ne => !std.mem.eql(u8, fv, rhs),
                    else => false,
                };
            }
            return resolveCaseValue(if (matches) args.then_val else args.else_val, record, arena);
        },
    }
}

fn resolveCaseValue(v: ScalarSpec.CaseValue, record: []const []const u8, arena: Allocator) []const u8 {
    return switch (v) {
        .string_lit => |s| s,
        .col_idx => |idx| field(record, idx),
        .numeric_lit => |n| blk: {
            const buf = arena.alloc(u8, 32) catch break :blk "";
            break :blk fmtNum(buf, n);
        },
    };
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
/// Row-wise max/min across columns. Numeric compare when every value parses as a
/// number, otherwise lexicographic. Returns the winning field slice (no alloc).
fn pickExtreme(record: []const []const u8, cols: []const usize, want_max: bool) []const u8 {
    var all_num = true;
    for (cols) |c| {
        _ = std.fmt.parseFloat(f64, field(record, c)) catch {
            all_num = false;
            break;
        };
    }
    var best_idx = cols[0];
    if (all_num) {
        var best = std.fmt.parseFloat(f64, field(record, cols[0])) catch 0;
        for (cols[1..]) |c| {
            const v = std.fmt.parseFloat(f64, field(record, c)) catch 0;
            if ((want_max and v > best) or (!want_max and v < best)) {
                best = v;
                best_idx = c;
            }
        }
    } else {
        for (cols[1..]) |c| {
            const cmp = std.mem.order(u8, field(record, c), field(record, best_idx));
            if ((want_max and cmp == .gt) or (!want_max and cmp == .lt)) best_idx = c;
        }
    }
    return field(record, best_idx);
}

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

/// Like fmtNum but always emits a decimal point (e.g. "101055.0").
/// Used for CEIL/FLOOR which always return DOUBLE in SQL.
pub fn fmtFloat(buf: []u8, n: f64) []const u8 {
    if (n == @trunc(n) and n >= -1e15 and n <= 1e15) {
        return std.fmt.bufPrint(buf, "{d}.0", .{@as(i64, @intFromFloat(n))}) catch buf[0..0];
    }
    return std.fmt.bufPrint(buf, "{d}", .{n}) catch buf[0..0];
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

test "COALESCE 2-arg backwards compat" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var column_map = std.StringHashMap(usize).init(allocator);
    try column_map.put("name", 0);

    const spec = (try tryParseScalar("COALESCE(name, 'unknown')", column_map, allocator)).?;

    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);

    try std.testing.expectEqualStrings("unknown", eval(spec, &.{""}, fba.allocator()));
    fba.reset();
    try std.testing.expectEqualStrings("Alice", eval(spec, &.{"Alice"}, fba.allocator()));
}

test "COALESCE 3-arg returns first non-empty column" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var column_map = std.StringHashMap(usize).init(allocator);
    try column_map.put("phone", 0);
    try column_map.put("email", 1);

    const spec = (try tryParseScalar("COALESCE(phone, email, 'N/A')", column_map, allocator)).?;

    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);

    // phone empty, email has value
    try std.testing.expectEqualStrings("user@example.com", eval(spec, &.{ "", "user@example.com" }, fba.allocator()));
    fba.reset();
    // both empty → fallback literal
    try std.testing.expectEqualStrings("N/A", eval(spec, &.{ "", "" }, fba.allocator()));
    fba.reset();
    // phone has value → return immediately
    try std.testing.expectEqualStrings("555-1234", eval(spec, &.{ "555-1234", "user@example.com" }, fba.allocator()));
}

test "COALESCE: whitespace-only field is treated as empty" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var column_map = std.StringHashMap(usize).init(allocator);
    try column_map.put("val", 0);

    const spec = (try tryParseScalar("COALESCE(val, 'default')", column_map, allocator)).?;

    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);

    try std.testing.expectEqualStrings("default", eval(spec, &.{"   "}, fba.allocator()));
}

test "COALESCE: returns error.TooManyArgs for more than 8 column args" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var column_map = std.StringHashMap(usize).init(allocator);
    for (0..9) |i| {
        var name_buf: [8]u8 = undefined;
        const name = try std.fmt.bufPrint(&name_buf, "c{d}", .{i});
        try column_map.put(try allocator.dupe(u8, name), i);
    }

    // 9 column args exceeds the inline buffer limit of 8
    try std.testing.expectError(
        error.TooManyArgs,
        tryParseScalar("COALESCE(c0, c1, c2, c3, c4, c5, c6, c7, c8, 'x')", column_map, allocator),
    );
}

test "ROUND: digits > 6 produces correct decimal places" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var column_map = std.StringHashMap(usize).init(allocator);
    try column_map.put("val", 0);

    const spec7 = (try tryParseScalar("ROUND(val, 7)", column_map, allocator)).?;
    const spec9 = (try tryParseScalar("ROUND(val, 9)", column_map, allocator)).?;

    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);

    try std.testing.expectEqualStrings("1.0000000", eval(spec7, &.{"1.0"}, fba.allocator()));
    fba.reset();
    try std.testing.expectEqualStrings("1.000000000", eval(spec9, &.{"1.0"}, fba.allocator()));
}

test "REPLACE: replaces all occurrences, handles comma literals and no-match" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var column_map = std.StringHashMap(usize).init(allocator);
    try column_map.put("s", 0);

    const dash = (try tryParseScalar("REPLACE(s, '-', '')", column_map, allocator)).?;
    const comma = (try tryParseScalar("REPLACE(s, ',', ';')", column_map, allocator)).?;

    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    try std.testing.expectEqualStrings("5551234567", eval(dash, &.{"555-123-4567"}, fba.allocator()));
    fba.reset();
    // from-literal contains a comma; field contains commas
    try std.testing.expectEqualStrings("a;b;c", eval(comma, &.{"a,b,c"}, fba.allocator()));
    fba.reset();
    // no match: passthrough
    try std.testing.expectEqualStrings("hello", eval(dash, &.{"hello"}, fba.allocator()));

    // empty search literal is rejected (would be a no-op / infinite)
    try std.testing.expect((try tryParseScalar("REPLACE(s, '', 'x')", column_map, allocator)) == null);
}

test "SPLIT_PART: n-th field, 1-based, out of range empty" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("s", 0);
    const sp2 = (try tryParseScalar("SPLIT_PART(s, '@', 2)", cm, allocator)).?;
    const sp5 = (try tryParseScalar("SPLIT_PART(s, '@', 5)", cm, allocator)).?;
    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    try std.testing.expectEqualStrings("example.com", eval(sp2, &.{"alice@example.com"}, fba.allocator()));
    fba.reset();
    try std.testing.expectEqualStrings("", eval(sp5, &.{"alice@example.com"}, fba.allocator()));
    try std.testing.expect((try tryParseScalar("SPLIT_PART(s, '@', 0)", cm, allocator)) == null);
}

test "GREATEST/LEAST: numeric and lexicographic" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("a", 0);
    try cm.put("b", 1);
    const g = (try tryParseScalar("GREATEST(a, b)", cm, allocator)).?;
    const l = (try tryParseScalar("LEAST(a, b)", cm, allocator)).?;
    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    try std.testing.expectEqualStrings("7", eval(g, &.{ "3", "7" }, fba.allocator()));
    fba.reset();
    try std.testing.expectEqualStrings("3", eval(l, &.{ "3", "7" }, fba.allocator()));
    fba.reset();
    // non-numeric → lexicographic
    try std.testing.expectEqualStrings("pear", eval(g, &.{ "apple", "pear" }, fba.allocator()));
    // single arg rejected
    try std.testing.expect((try tryParseScalar("GREATEST(a)", cm, allocator)) == null);
}
