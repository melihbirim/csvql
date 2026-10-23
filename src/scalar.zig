/// scalar.zig — Scalar function evaluation for SELECT columns.
///
/// Supports: UPPER, LOWER, TRIM, REVERSE, LENGTH, SUBSTR/SUBSTRING,
///           ABS, SIGN, CEIL, FLOOR, MOD, COALESCE, CAST(col AS type), REPLACE,
///           SPLIT_PART, GREATEST, LEAST, LPAD, RPAD, SQRT, POWER/POW, LN, LOG,
///           EXP, TRUNC, NULLIF, IFNULL,
///           LTRIM, RTRIM, LEFT, RIGHT, REPEAT, POSITION, INSTR,
///           CURRENT_DATE, NOW, CURRENT_TIMESTAMP
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
    reverse: usize, // REVERSE(col)
    length: usize, // LENGTH(col) — returns character count as string
    substr: SubstrArgs, // SUBSTR(col, start[, len]) — 1-based SQL semantics
    ltrim: usize, // LTRIM(col) — trim whitespace on the left only
    rtrim: usize, // RTRIM(col) — trim whitespace on the right only
    left: CountArgs, // LEFT(col, n) — first n bytes; negative n drops the last |n|
    right: CountArgs, // RIGHT(col, n) — last n bytes; negative n drops the first |n|
    repeat: CountArgs, // REPEAT(col, n) — n copies; n <= 0, or a result over max_repeat_bytes, is empty
    position: PositionArgs, // POSITION('sub' IN col) / INSTR(col, 'sub') — 1-based byte index, 0 when absent
    lpad: PadArgs, // LPAD(col, len[, pad]) — pad on the left, default pad is a space
    rpad: PadArgs, // RPAD(col, len[, pad]) — pad on the right, default pad is a space
    abs: usize, // ABS(col)
    sign: usize, // SIGN(col)
    ceil: usize, // CEIL(col)
    floor: usize, // FLOOR(col)
    mod_op: ModArgs, // MOD(col, divisor)
    math: MathArgs, // SQRT, POWER/POW, LN, LOG, EXP, TRUNC
    coalesce: CoalesceArgs, // COALESCE(col, fallback_literal); IFNULL is the two-argument form
    nullif: NullifArgs, // NULLIF(col, literal) — empty when the field equals the literal
    cast_int: usize, // CAST(col AS INTEGER/BIGINT)
    cast_float: usize, // CAST(col AS FLOAT/REAL/NUMERIC/DECIMAL)
    cast_text: usize, // CAST(col AS TEXT/VARCHAR/CHAR/STRING) — identity
    datediff: DatediffArgs, // DATEDIFF(unit, start_col, end_col)
    dateadd: DateaddArgs, // DATEADD(unit, amount, date_col)
    extract: ExtractArgs, // EXTRACT(part FROM date_col)
    strftime: StrftimeArgs, // STRFTIME('fmt', date_col) (#133)
    round_op: RoundArgs, // ROUND(col[, digits])
    replace: ReplaceArgs, // REPLACE(col, 'from', 'to') — replace all occurrences
    split_part: SplitPartArgs, // SPLIT_PART(col, 'delim', n) — n-th field (1-based)
    greatest: ConcatArgs, // GREATEST(a, b, ...) — row-wise max, columns and/or literals (#134)
    least: ConcatArgs, // LEAST(a, b, ...) — row-wise min, columns and/or literals (#134)
    concat: ConcatArgs, // CONCAT(a, b, ...) — column refs and/or string literals
    nested: NestedArgs, // UPPER(TRIM(col)) etc — one level of function composition (#120)
    case_when: CaseWhenArgs, // bare CASE WHEN ... THEN ... ELSE ... END (#105)
    is_null_check: IsNullArgs, // col IS NULL / col IS NOT NULL as a SELECT expression
    clock: []const u8, // CURRENT_DATE / NOW() / CURRENT_TIMESTAMP, resolved once per query (#202); owned

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

    pub const SubstrArgs = struct {
        col_idx: usize,
        start: i32, // 1-based; negative = count from end
        len: i32, // -1 = to end of string
    };

    pub const CountArgs = struct {
        col_idx: usize,
        n: i64,
    };

    pub const PositionArgs = struct {
        col_idx: usize,
        needle: []const u8, // without the quotes, doubled quotes collapsed (#257)
        needle_owned: bool = false, // true when `needle` was allocated to collapse a doubled quote
    };

    pub const PadArgs = struct {
        col_idx: usize,
        target_len: usize,
        pad: []const u8, // slice into the query expr; defaults to " " when omitted
    };

    pub const ModArgs = struct {
        col_idx: usize,
        divisor: f64,
    };

    /// The floating-point math functions. A result that is NaN or infinite
    /// (SQRT of a negative, LN/LOG of zero or a negative, an overflowing
    /// EXP/POWER) is NULL, so it is written as an empty field.
    pub const MathArgs = struct {
        pub const Func = enum { sqrt, power, ln, log, exp, trunc };

        col_idx: usize,
        func: Func,
        /// POWER: the exponent. LOG: the base (10 for the one-argument form).
        param: f64 = 0,

        pub fn apply(self: MathArgs, x: f64) f64 {
            return switch (self.func) {
                .sqrt => @sqrt(x),
                .power => std.math.pow(f64, x, self.param),
                .ln => @log(x),
                .log => if (self.param == 10)
                    @log10(x)
                else if (self.param == 2)
                    @log2(x)
                else
                    @log(x) / @log(self.param),
                .exp => @exp(x),
                .trunc => @trunc(x),
            };
        }
    };

    pub const NullifArgs = struct {
        col_idx: usize,
        /// A quoted literal, without the quotes: compared byte for byte.
        literal: []const u8,
        /// Set for a bare numeric literal: the field is compared as a number
        /// instead, so `NULLIF(age, 30)` also matches `30.0`.
        numeric: ?f64,
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

    pub const StrftimeArgs = struct {
        fmt: []const u8, // e.g. "%Y-%m" — slice into the query expr
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

    /// One CONCAT argument: either a column reference or a string literal.
    pub const ConcatPart = union(enum) {
        col_idx: usize,
        literal: []const u8, // slice into the query expr (query-lifetime)
    };

    /// Up to 8 args for CONCAT.
    pub const ConcatArgs = struct {
        parts_buf: [8]ConcatPart = undefined,
        parts_len: usize,

        pub fn parts(self: *const ConcatArgs) []const ConcatPart {
            return self.parts_buf[0..self.parts_len];
        }
    };

    /// UPPER(TRIM(col)) etc: outer is one of the plain single-arg string
    /// functions, inner is any other ScalarSpec evaluated first against the
    /// same record. Heap-allocated (query-lifetime allocator) since ScalarSpec
    /// can't hold itself by value.
    pub const NestedArgs = struct {
        pub const OuterFn = enum { upper, lower, trim, reverse, length };
        outer_fn: OuterFn,
        inner: *const ScalarSpec,
    };

    pub const IsNullArgs = struct {
        col_idx: usize,
        negate: bool, // true = IS NOT NULL
    };

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
            .upper, .lower, .trim, .ltrim, .rtrim, .reverse, .length, .abs, .sign, .ceil, .floor, .cast_int, .cast_float, .cast_text => |i| i,
            .substr => |a| a.col_idx,
            .left, .right, .repeat => |a| a.col_idx,
            .position => |a| a.col_idx,
            .lpad, .rpad => |a| a.col_idx,
            .mod_op => |a| a.col_idx,
            .math => |a| a.col_idx,
            .coalesce => |a| a.cols()[0],
            .nullif => |a| a.col_idx,
            .datediff => |a| a.start_col, // return first column
            .dateadd => |a| a.date_col,
            .extract => |a| a.date_col,
            .strftime => |a| a.date_col,
            .round_op => |a| a.col_idx,
            .replace => |a| a.col_idx,
            .split_part => |a| a.col_idx,
            .greatest, .least => |a| for (a.parts()) |p| {
                if (p == .col_idx) break p.col_idx;
            } else 0,
            .case_when => |a| a.cond_col_idx,
            .is_null_check => |a| a.col_idx,
            .concat => |a| for (a.parts()) |p| {
                if (p == .col_idx) break p.col_idx;
            } else 0,
            .nested => |a| a.inner.colIdx(),
            .clock => 0,
        };
    }

    /// Release query-lifetime allocations owned by this spec.
    pub fn deinit(self: *ScalarSpec, allocator: Allocator) void {
        switch (self.*) {
            .nested => |args| {
                const inner = @constCast(args.inner);
                inner.deinit(allocator);
                allocator.destroy(inner);
            },
            .clock => |text| allocator.free(text),
            .position => |a| if (a.needle_owned) allocator.free(a.needle),
            else => {},
        }
    }
};

/// A SELECT output column: either a direct field pass-through or a scalar transform.
pub const OutputColSpec = union(enum) {
    column: usize,
    scalar: ScalarSpec,

    pub fn deinit(self: *OutputColSpec, allocator: Allocator) void {
        switch (self.*) {
            .scalar => |*spec| spec.deinit(allocator),
            .column => {},
        }
    }
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
    return tryParseScalarAt(expr, column_map, allocator, std.time.timestamp());
}

/// `tryParseScalar` with the instant CURRENT_DATE, NOW() and CURRENT_TIMESTAMP
/// resolve to, as Unix seconds (UTC). Queries pass `Query.now_ts` so every
/// expression in one query sees the same instant.
pub fn tryParseScalarAt(
    expr: []const u8,
    column_map: std.StringHashMap(usize),
    allocator: Allocator,
    now_ts: i64,
) !?ScalarSpec {
    const t = std.mem.trim(u8, expr, &std.ascii.whitespace);

    if (datetime.matchClockKeyword(t)) |m| {
        if (m.len == t.len) {
            var buf: [19]u8 = undefined;
            const text = try datetime.formatClock(&buf, m.kind, now_ts);
            return .{ .clock = try allocator.dupe(u8, text) };
        }
    }

    // col IS NULL / col IS NOT NULL as a SELECT expression has no wrapping
    // parens either, so it must be checked before the paren-based dispatch.
    if (std.ascii.indexOfIgnoreCase(t, " is not null")) |idx| {
        const cidx = try resolveCol(t[0..idx], column_map, allocator) orelse return error.ColumnNotFound;
        return .{ .is_null_check = .{ .col_idx = cidx, .negate = true } };
    }
    if (std.ascii.indexOfIgnoreCase(t, " is null")) |idx| {
        const cidx = try resolveCol(t[0..idx], column_map, allocator) orelse return error.ColumnNotFound;
        return .{ .is_null_check = .{ .col_idx = cidx, .negate = false } };
    }

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
        std.mem.eql(u8, fn_lower, "ltrim") or
        std.mem.eql(u8, fn_lower, "rtrim") or
        std.mem.eql(u8, fn_lower, "reverse") or
        std.mem.eql(u8, fn_lower, "length") or
        std.mem.eql(u8, fn_lower, "abs") or
        std.mem.eql(u8, fn_lower, "sign") or
        std.mem.eql(u8, fn_lower, "ceil") or
        std.mem.eql(u8, fn_lower, "ceiling") or
        std.mem.eql(u8, fn_lower, "floor");

    if (single_arg_fn) {
        // Nested call, e.g. LOWER(TRIM(col)) (#120) — args_str itself is a
        // function call rather than a bare column, which resolveCol rejects
        // outright (it treats any '(' in the arg as invalid). Parse the
        // inner expression recursively and compose, restricted to the four
        // outer functions that only need a string in, string out.
        if (std.mem.indexOf(u8, args_str, "(") != null) {
            const outer_fn: ScalarSpec.NestedArgs.OuterFn = if (std.mem.eql(u8, fn_lower, "upper"))
                .upper
            else if (std.mem.eql(u8, fn_lower, "lower"))
                .lower
            else if (std.mem.eql(u8, fn_lower, "trim"))
                .trim
            else if (std.mem.eql(u8, fn_lower, "reverse"))
                .reverse
            else if (std.mem.eql(u8, fn_lower, "length"))
                .length
            else
                return error.NestedFunctionNotSupported;
            var inner_spec = try tryParseScalarAt(args_str, column_map, allocator, now_ts) orelse
                return error.ColumnNotFound;
            errdefer inner_spec.deinit(allocator);
            const inner_ptr = try allocator.create(ScalarSpec);
            inner_ptr.* = inner_spec;
            return .{ .nested = .{ .outer_fn = outer_fn, .inner = inner_ptr } };
        }
        const cidx = try resolveCol(args_str, column_map, allocator) orelse
            return error.ColumnNotFound;
        if (std.mem.eql(u8, fn_lower, "upper")) return .{ .upper = cidx };
        if (std.mem.eql(u8, fn_lower, "lower")) return .{ .lower = cidx };
        if (std.mem.eql(u8, fn_lower, "trim")) return .{ .trim = cidx };
        if (std.mem.eql(u8, fn_lower, "ltrim")) return .{ .ltrim = cidx };
        if (std.mem.eql(u8, fn_lower, "rtrim")) return .{ .rtrim = cidx };
        if (std.mem.eql(u8, fn_lower, "reverse")) return .{ .reverse = cidx };
        if (std.mem.eql(u8, fn_lower, "length")) return .{ .length = cidx };
        if (std.mem.eql(u8, fn_lower, "abs")) return .{ .abs = cidx };
        if (std.mem.eql(u8, fn_lower, "sign")) return .{ .sign = cidx };
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

    // ── LEFT / RIGHT(col, n) ───────────────────────────────────────────────
    if (std.mem.eql(u8, fn_lower, "left") or std.mem.eql(u8, fn_lower, "right")) {
        // Exactly two arguments, the second an integer literal.
        const comma = std.mem.indexOfScalar(u8, args_str, ',') orelse return error.InvalidQuery;
        const col_str = std.mem.trim(u8, args_str[0..comma], &std.ascii.whitespace);
        const n_str = std.mem.trim(u8, args_str[comma + 1 ..], &std.ascii.whitespace);
        const n = std.fmt.parseInt(i64, n_str, 10) catch return error.InvalidQuery;
        const cidx = try resolveCol(col_str, column_map, allocator) orelse
            return error.ColumnNotFound;
        if (std.mem.eql(u8, fn_lower, "left")) return .{ .left = .{ .col_idx = cidx, .n = n } };
        return .{ .right = .{ .col_idx = cidx, .n = n } };
    }

    // ── REPEAT(col, n) ─────────────────────────────────────────────────────
    if (std.mem.eql(u8, fn_lower, "repeat")) {
        const parts = try splitTwoArgs(args_str);
        const n = std.fmt.parseInt(i64, parts[1], 10) catch return error.InvalidQuery;
        const cidx = try resolveCol(parts[0], column_map, allocator) orelse
            return error.ColumnNotFound;
        return .{ .repeat = .{ .col_idx = cidx, .n = n } };
    }

    // ── INSTR(col, 'sub') and POSITION('sub' IN col) ───────────────────────
    if (std.mem.eql(u8, fn_lower, "instr")) {
        const parts = try splitTwoArgs(args_str);
        if (!isQuoted(parts[1])) return error.InvalidQuery;
        const cidx = try resolveCol(parts[0], column_map, allocator) orelse
            return error.ColumnNotFound;
        return try positionSpec(allocator, cidx, parts[1][1 .. parts[1].len - 1]);
    }
    if (std.mem.eql(u8, fn_lower, "position")) {
        if (args_str.len < 2 or args_str[0] != '\'') return error.InvalidQuery;
        const close = closingQuote(args_str, 1) orelse return error.InvalidQuery;
        const rest = std.mem.trim(u8, args_str[close + 1 ..], &std.ascii.whitespace);
        if (rest.len < 3 or !std.ascii.eqlIgnoreCase(rest[0..2], "in") or !std.ascii.isWhitespace(rest[2]))
            return error.InvalidQuery;
        const cidx = try resolveCol(rest[2..], column_map, allocator) orelse
            return error.ColumnNotFound;
        return try positionSpec(allocator, cidx, args_str[1..close]);
    }

    // ── LPAD / RPAD(col, len[, pad]) ────────────────────────────────────────
    if (std.mem.eql(u8, fn_lower, "lpad") or std.mem.eql(u8, fn_lower, "rpad")) {
        const comma1 = std.mem.indexOfScalar(u8, args_str, ',') orelse return null;
        const col_str = std.mem.trim(u8, args_str[0..comma1], &std.ascii.whitespace);
        const rest = std.mem.trim(u8, args_str[comma1 + 1 ..], &std.ascii.whitespace);

        const cidx = try resolveCol(col_str, column_map, allocator) orelse
            return error.ColumnNotFound;

        const comma2 = std.mem.indexOfScalar(u8, rest, ',');
        const len_str = if (comma2) |c| std.mem.trim(u8, rest[0..c], &std.ascii.whitespace) else rest;
        const target_len = std.fmt.parseInt(usize, len_str, 10) catch return null;

        const pad: []const u8 = if (comma2) |c| blk: {
            const pad_raw = std.mem.trim(u8, rest[c + 1 ..], &std.ascii.whitespace);
            if (pad_raw.len < 2 or pad_raw[0] != '\'' or pad_raw[pad_raw.len - 1] != '\'') return null;
            break :blk pad_raw[1 .. pad_raw.len - 1];
        } else " ";

        if (std.mem.eql(u8, fn_lower, "lpad"))
            return .{ .lpad = .{ .col_idx = cidx, .target_len = target_len, .pad = pad } };
        return .{ .rpad = .{ .col_idx = cidx, .target_len = target_len, .pad = pad } };
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

    // ── GREATEST / LEAST(a, b, ...) — columns and/or numeric literals (#134) ─
    if (std.mem.eql(u8, fn_lower, "greatest") or std.mem.eql(u8, fn_lower, "least")) {
        var v = ScalarSpec.ConcatArgs{ .parts_len = 0 };
        // Quote-aware top-level comma split (a literal arg could itself be a
        // quoted string containing a comma), same technique as CONCAT.
        var start: usize = 0;
        var in_quote = false;
        var i: usize = 0;
        while (i <= args_str.len) : (i += 1) {
            const at_end = i == args_str.len;
            const c: u8 = if (at_end) ',' else args_str[i];
            if (!at_end and c == '\'') {
                in_quote = !in_quote;
                continue;
            }
            if (in_quote) continue;
            if (c == ',') {
                const arg = std.mem.trim(u8, args_str[start..i], &std.ascii.whitespace);
                start = i + 1;
                if (arg.len == 0) continue;
                if (v.parts_len >= v.parts_buf.len) return error.TooManyArgs;
                if (arg.len >= 2 and arg[0] == '\'' and arg[arg.len - 1] == '\'') {
                    v.parts_buf[v.parts_len] = .{ .literal = arg[1 .. arg.len - 1] };
                } else if (try resolveCol(arg, column_map, allocator)) |cidx| {
                    v.parts_buf[v.parts_len] = .{ .col_idx = cidx };
                } else if (std.fmt.parseFloat(f64, arg)) |_| {
                    // Bare numeric literal, e.g. GREATEST(age, 30). Only
                    // numeric — an unquoted non-numeric, non-column token
                    // (likely a typo) still errors instead of silently
                    // becoming a literal string.
                    v.parts_buf[v.parts_len] = .{ .literal = arg };
                } else |_| {
                    return error.ColumnNotFound;
                }
                v.parts_len += 1;
            }
        }
        if (v.parts_len < 2) return null;
        return if (std.mem.eql(u8, fn_lower, "greatest")) .{ .greatest = v } else .{ .least = v };
    }

    // ── CONCAT(a, b, ...) — column refs and/or 'string' literals ────────────
    if (std.mem.eql(u8, fn_lower, "concat")) {
        var v = ScalarSpec.ConcatArgs{ .parts_len = 0 };
        var start: usize = 0;
        var in_quote = false;
        var i: usize = 0;
        while (i <= args_str.len) : (i += 1) {
            const at_end = i == args_str.len;
            const c: u8 = if (at_end) ',' else args_str[i];
            if (!at_end and c == '\'') {
                in_quote = !in_quote;
                continue;
            }
            if (in_quote) continue;
            if (c == ',') {
                const arg = std.mem.trim(u8, args_str[start..i], &std.ascii.whitespace);
                start = i + 1;
                if (arg.len == 0) continue;
                if (v.parts_len >= v.parts_buf.len) return error.TooManyArgs;
                if (arg.len >= 2 and arg[0] == '\'' and arg[arg.len - 1] == '\'') {
                    v.parts_buf[v.parts_len] = .{ .literal = arg[1 .. arg.len - 1] };
                } else {
                    const cidx = try resolveCol(arg, column_map, allocator) orelse
                        return error.ColumnNotFound;
                    v.parts_buf[v.parts_len] = .{ .col_idx = cidx };
                }
                v.parts_len += 1;
            }
        }
        if (v.parts_len < 1) return null;
        return .{ .concat = v };
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

    // ── SQRT / LN / EXP / TRUNC / LOG / POWER ──────────────────────────────
    if (parseMathFunc(fn_lower)) |func| {
        switch (func) {
            .sqrt, .ln, .exp, .trunc => {
                const cidx = try resolveCol(args_str, column_map, allocator) orelse
                    return error.ColumnNotFound;
                return .{ .math = .{ .col_idx = cidx, .func = func } };
            },
            .log => {
                // LOG(x) is base 10; LOG(base, x) takes a numeric literal base.
                const comma = std.mem.indexOfScalar(u8, args_str, ',') orelse {
                    const cidx = try resolveCol(args_str, column_map, allocator) orelse
                        return error.ColumnNotFound;
                    return .{ .math = .{ .col_idx = cidx, .func = .log, .param = 10 } };
                };
                const base_str = std.mem.trim(u8, args_str[0..comma], &std.ascii.whitespace);
                const col_str = std.mem.trim(u8, args_str[comma + 1 ..], &std.ascii.whitespace);
                const base = std.fmt.parseFloat(f64, base_str) catch return null;
                if (!(base > 0) or base == 1 or !std.math.isFinite(base)) return error.InvalidQuery;
                const cidx = try resolveCol(col_str, column_map, allocator) orelse
                    return error.ColumnNotFound;
                return .{ .math = .{ .col_idx = cidx, .func = .log, .param = base } };
            },
            .power => {
                const comma = std.mem.indexOfScalar(u8, args_str, ',') orelse return null;
                const col_str = std.mem.trim(u8, args_str[0..comma], &std.ascii.whitespace);
                const exp_str = std.mem.trim(u8, args_str[comma + 1 ..], &std.ascii.whitespace);
                const cidx = try resolveCol(col_str, column_map, allocator) orelse
                    return error.ColumnNotFound;
                const exponent = std.fmt.parseFloat(f64, exp_str) catch return null;
                return .{ .math = .{ .col_idx = cidx, .func = .power, .param = exponent } };
            },
        }
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
            } else if (std.fmt.parseFloat(f64, arg)) |_| {
                // Bare numeric literal fallback, e.g. COALESCE(a, 999) (#109).
                fallback = arg;
            } else |_| {
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

    // ── IFNULL(col, fallback) — COALESCE with exactly two arguments ────────
    if (std.mem.eql(u8, fn_lower, "ifnull")) {
        const parts = try splitTwoArgs(args_str);
        var args = ScalarSpec.CoalesceArgs{ .cols_len = 1, .fallback = "" };
        // COALESCE tries its columns before the fallback whatever their order
        // in the call, so a literal first argument would silently change the
        // result. Require the column first.
        args.cols_buf[0] = try resolveCol(parts[0], column_map, allocator) orelse
            return error.ColumnNotFound;
        const second = parts[1];
        if (isQuoted(second)) {
            args.fallback = second[1 .. second.len - 1];
        } else if (std.fmt.parseFloat(f64, second)) |_| {
            args.fallback = second;
        } else |_| {
            args.cols_buf[1] = try resolveCol(second, column_map, allocator) orelse
                return error.ColumnNotFound;
            args.cols_len = 2;
        }
        return .{ .coalesce = args };
    }

    // ── NULLIF(col, literal) ───────────────────────────────────────────────
    if (std.mem.eql(u8, fn_lower, "nullif")) {
        const parts = try splitTwoArgs(args_str);
        const cidx = try resolveCol(parts[0], column_map, allocator) orelse
            return error.ColumnNotFound;
        const lit = parts[1];
        if (isQuoted(lit)) {
            return .{ .nullif = .{ .col_idx = cidx, .literal = lit[1 .. lit.len - 1], .numeric = null } };
        }
        // An unquoted token must be a finite number; anything else is more
        // likely a mistyped column or a forgotten quote than a literal.
        const n = std.fmt.parseFloat(f64, lit) catch return error.InvalidQuery;
        if (!std.math.isFinite(n)) return error.InvalidQuery;
        return .{ .nullif = .{ .col_idx = cidx, .literal = lit, .numeric = n } };
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

    // ── STRFTIME('fmt', date_col) (#133) ────────────────────────────────────
    if (std.mem.eql(u8, fn_lower, "strftime")) {
        const comma1 = std.mem.indexOfScalar(u8, args_str, ',') orelse return null;
        const fmt_raw = std.mem.trim(u8, args_str[0..comma1], &std.ascii.whitespace);
        const fmt = if (fmt_raw.len >= 2 and fmt_raw[0] == '\'' and fmt_raw[fmt_raw.len - 1] == '\'')
            fmt_raw[1 .. fmt_raw.len - 1]
        else
            fmt_raw;
        const date_str = std.mem.trim(u8, args_str[comma1 + 1 ..], &std.ascii.whitespace);
        const date_col = try resolveCol(date_str, column_map, allocator) orelse
            return error.ColumnNotFound;
        return .{ .strftime = .{ .fmt = fmt, .date_col = date_col } };
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
        .ltrim => |cidx| return std.mem.trimLeft(u8, field(record, cidx), &std.ascii.whitespace),
        .rtrim => |cidx| return std.mem.trimRight(u8, field(record, cidx), &std.ascii.whitespace),
        .left => |args| return leftBytes(field(record, args.col_idx), args.n),
        .right => |args| return rightBytes(field(record, args.col_idx), args.n),
        .repeat => |args| return repeatBytes(field(record, args.col_idx), args.n, arena),
        .position => |args| {
            const v = field(record, args.col_idx);
            if (v.len == 0) return v; // an empty field is NULL
            const pos = if (std.mem.indexOf(u8, v, args.needle)) |i| i + 1 else 0;
            const buf = arena.alloc(u8, 20) catch return "0";
            return std.fmt.bufPrint(buf, "{d}", .{pos}) catch "0";
        },
        .reverse => |cidx| {
            const v = field(record, cidx);
            if (v.len == 0) return v;
            const buf = arena.alloc(u8, v.len) catch return v;
            if (!std.unicode.utf8ValidateSlice(v)) {
                @memcpy(buf, v);
                std.mem.reverse(u8, buf);
                return buf;
            }
            var src_end = v.len;
            var dst_start: usize = 0;
            while (src_end > 0) {
                var src_start = src_end - 1;
                while (src_start > 0 and v[src_start] & 0xc0 == 0x80) {
                    src_start -= 1;
                }
                const codepoint = v[src_start..src_end];
                @memcpy(buf[dst_start .. dst_start + codepoint.len], codepoint);
                dst_start += codepoint.len;
                src_end = src_start;
            }
            return buf;
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
        .nested => |args| {
            const inner_val = eval(args.inner.*, record, arena);
            const inner_rec: [1][]const u8 = .{inner_val};
            return switch (args.outer_fn) {
                .upper => eval(.{ .upper = 0 }, &inner_rec, arena),
                .lower => eval(.{ .lower = 0 }, &inner_rec, arena),
                .trim => eval(.{ .trim = 0 }, &inner_rec, arena),
                .reverse => eval(.{ .reverse = 0 }, &inner_rec, arena),
                .length => eval(.{ .length = 0 }, &inner_rec, arena),
            };
        },
        .clock => |text| return text,
        .is_null_check => |args| {
            const is_null = field(record, args.col_idx).len == 0;
            const result = if (args.negate) !is_null else is_null;
            return if (result) "true" else "false";
        },
        .greatest => |args| {
            var vals: [8][]const u8 = undefined;
            for (args.parts(), 0..) |p, idx| {
                vals[idx] = switch (p) {
                    .col_idx => |ci| field(record, ci),
                    .literal => |lit| lit,
                };
            }
            return pickExtremeValues(vals[0..args.parts().len], true);
        },
        .least => |args| {
            var vals: [8][]const u8 = undefined;
            for (args.parts(), 0..) |p, idx| {
                vals[idx] = switch (p) {
                    .col_idx => |ci| field(record, ci),
                    .literal => |lit| lit,
                };
            }
            return pickExtremeValues(vals[0..args.parts().len], false);
        },
        .concat => |args| {
            var total: usize = 0;
            for (args.parts()) |p| {
                total += switch (p) {
                    .col_idx => |ci| field(record, ci).len,
                    .literal => |lit| lit.len,
                };
            }
            const buf = arena.alloc(u8, total) catch return "";
            var off: usize = 0;
            for (args.parts()) |p| {
                const s = switch (p) {
                    .col_idx => |ci| field(record, ci),
                    .literal => |lit| lit,
                };
                @memcpy(buf[off .. off + s.len], s);
                off += s.len;
            }
            return buf;
        },
        .length => |cidx| {
            const v = field(record, cidx);
            // An empty field is NULL (what IS NULL / COUNT(col) / COALESCE
            // all treat it as), and NULL has no length — propagate it
            // instead of reporting 0, which is a real length and would turn
            // a missing value into a present one. Every other scalar
            // function here already short-circuits empty this way; LENGTH
            // was the sole outlier. See #147.
            if (v.len == 0) return v;
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
        .lpad => |args| return padTo(field(record, args.col_idx), args.target_len, args.pad, true, arena),
        .rpad => |args| return padTo(field(record, args.col_idx), args.target_len, args.pad, false, arena),
        .abs => |cidx| {
            const v = field(record, cidx);
            const n = std.fmt.parseFloat(f64, v) catch return v;
            const buf = arena.alloc(u8, 32) catch return v;
            return fmtNum(buf, @abs(n));
        },
        .sign => |cidx| {
            const v = field(record, cidx);
            const n = std.fmt.parseFloat(f64, v) catch return v;
            return if (n < 0) "-1" else if (n > 0) "1" else "0";
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
        .math => |args| {
            const v = field(record, args.col_idx);
            const n = std.fmt.parseFloat(f64, v) catch return v;
            const buf = arena.alloc(u8, 32) catch return v;
            return fmtMath(buf, args.apply(n));
        },
        .coalesce => |args| {
            for (args.cols()) |cidx| {
                const v = field(record, cidx);
                if (std.mem.trim(u8, v, &std.ascii.whitespace).len > 0) return v;
            }
            return args.fallback;
        },
        .nullif => |args| {
            const v = field(record, args.col_idx);
            if (args.numeric) |want| {
                const n = std.fmt.parseFloat(f64, std.mem.trim(u8, v, &std.ascii.whitespace)) catch return v;
                return if (n == want) "" else v;
            }
            return if (std.mem.eql(u8, v, args.literal)) "" else v;
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
        .strftime => |args| {
            const date_val = field(record, args.date_col);
            var stack_buf: [64]u8 = undefined;
            const formatted = datetime.applyStrftime(args.fmt, date_val, &stack_buf);
            const buf = arena.alloc(u8, formatted.len) catch return "";
            @memcpy(buf, formatted);
            return buf;
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
fn pickExtremeValues(values: []const []const u8, want_max: bool) []const u8 {
    var all_num = true;
    for (values) |v| {
        _ = std.fmt.parseFloat(f64, v) catch {
            all_num = false;
            break;
        };
    }
    var best = values[0];
    if (all_num) {
        var best_num = std.fmt.parseFloat(f64, values[0]) catch 0;
        for (values[1..]) |v| {
            const n = std.fmt.parseFloat(f64, v) catch 0;
            if ((want_max and n > best_num) or (!want_max and n < best_num)) {
                best_num = n;
                best = v;
            }
        }
    } else {
        for (values[1..]) |v| {
            const cmp = std.mem.order(u8, v, best);
            if ((want_max and cmp == .gt) or (!want_max and cmp == .lt)) best = v;
        }
    }
    return best;
}

/// LPAD/RPAD body: pad `v` out to `target_len` with `pad`, or cut it down to
/// `target_len` when it is already longer — DuckDB and Postgres both truncate
/// rather than returning the value unchanged.
///
/// Lengths are in bytes, like LENGTH and SUBSTR here, so a multi-byte value
/// pads to fewer characters than DuckDB would. The cut is still made on a
/// codepoint boundary so truncation never emits invalid UTF-8.
fn padTo(v: []const u8, target_len: usize, pad: []const u8, comptime left: bool, arena: Allocator) []const u8 {
    // An empty field is NULL, and padding NULL would invent a value. See #147.
    if (v.len == 0 or v.len == target_len) return v;
    if (v.len > target_len) {
        var end = target_len;
        while (end > 0 and v[end] & 0xc0 == 0x80) end -= 1;
        return v[0..end];
    }
    if (pad.len == 0) return v; // nothing to pad with
    const pad_needed = target_len - v.len;
    const buf = arena.alloc(u8, target_len) catch return v;
    var i: usize = 0;
    if (left) {
        while (i < pad_needed) : (i += 1) buf[i] = pad[i % pad.len];
        @memcpy(buf[pad_needed..], v);
    } else {
        @memcpy(buf[0..v.len], v);
        while (i < pad_needed) : (i += 1) buf[v.len + i] = pad[i % pad.len];
    }
    return buf;
}

/// The most bytes REPEAT will produce for one field. A longer result is NULL
/// (an empty field) instead of an allocation sized by the query text.
pub const max_repeat_bytes: usize = 1 << 20;

/// `v` repeated `n` times. An empty field is NULL and stays empty. `n <= 0`
/// gives an empty string, like DuckDB. A result over `max_repeat_bytes` is
/// empty, where DuckDB would build it.
fn repeatBytes(v: []const u8, n: i64, arena: Allocator) []const u8 {
    if (v.len == 0 or n == 1) return v;
    const count = std.math.cast(usize, n) orelse return "";
    const total = std.math.mul(usize, v.len, count) catch return "";
    if (total > max_repeat_bytes) return "";
    const buf = arena.alloc(u8, total) catch return "";
    for (0..count) |i| @memcpy(buf[i * v.len ..][0..v.len], v);
    return buf;
}

/// The first `n` bytes of `v`; a negative `n` drops the last `-n` bytes. Counts
/// are bytes, like LENGTH and SUBSTR, but the cut never lands inside a
/// multi-byte character, so the result is always valid UTF-8 (it can be
/// shorter than `n`).
fn leftBytes(v: []const u8, n: i64) []const u8 {
    if (v.len == 0) return v;
    const len: i64 = @intCast(v.len);
    const m = std.math.clamp(n, -len, len);
    var end: usize = @intCast(if (m >= 0) m else len + m);
    while (end > 0 and end < v.len and v[end] & 0xc0 == 0x80) end -= 1;
    return v[0..end];
}

/// The last `n` bytes of `v`; a negative `n` drops the first `-n` bytes. Same
/// byte counting and character-boundary rule as `leftBytes`.
fn rightBytes(v: []const u8, n: i64) []const u8 {
    if (v.len == 0) return v;
    const len: i64 = @intCast(v.len);
    const m = std.math.clamp(n, -len, len);
    var start: usize = @intCast(if (m >= 0) len - m else -m);
    while (start < v.len and v[start] & 0xc0 == 0x80) start += 1;
    return v[start..];
}

inline fn field(record: []const []const u8, idx: usize) []const u8 {
    return if (idx < record.len) record[idx] else "";
}

/// Index of the quote that closes a literal whose content starts at `from`.
/// A doubled quote inside the literal is an escaped quote, not the end.
fn closingQuote(s: []const u8, from: usize) ?usize {
    var i = from;
    while (i < s.len) : (i += 1) {
        if (s[i] != '\'') continue;
        if (i + 1 < s.len and s[i + 1] == '\'') {
            i += 1;
            continue;
        }
        return i;
    }
    return null;
}

/// POSITION/INSTR spec for the literal `raw` (the text between the quotes).
/// A doubled quote in `raw` stands for one quote, so the needle is an owned
/// copy with each pair collapsed; otherwise it borrows `raw` (#257).
fn positionSpec(allocator: Allocator, col_idx: usize, raw: []const u8) !ScalarSpec {
    if (std.mem.indexOf(u8, raw, "''") == null) return .{ .position = .{ .col_idx = col_idx, .needle = raw } };
    const needle = try std.mem.replaceOwned(u8, allocator, raw, "''", "'");
    return .{ .position = .{ .col_idx = col_idx, .needle = needle, .needle_owned = true } };
}

fn isQuoted(s: []const u8) bool {
    return s.len >= 2 and s[0] == '\'' and s[s.len - 1] == '\'';
}

/// Split a two-argument call's argument list at its one top-level comma. A
/// comma inside a quoted literal does not count. Any other argument count, or
/// an empty argument, is a malformed query.
fn splitTwoArgs(args_str: []const u8) error{InvalidQuery}![2][]const u8 {
    var comma: ?usize = null;
    var in_quote = false;
    for (args_str, 0..) |c, i| {
        if (c == '\'') {
            in_quote = !in_quote;
        } else if (c == ',' and !in_quote) {
            if (comma != null) return error.InvalidQuery;
            comma = i;
        }
    }
    const at = comma orelse return error.InvalidQuery;
    const a = std.mem.trim(u8, args_str[0..at], &std.ascii.whitespace);
    const b = std.mem.trim(u8, args_str[at + 1 ..], &std.ascii.whitespace);
    if (a.len == 0 or b.len == 0) return error.InvalidQuery;
    return .{ a, b };
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

/// Format a math function result. NaN and infinity become an empty field
/// (NULL). A magnitude too long for `buf` in plain decimal falls back to
/// scientific notation instead of being silently dropped.
fn fmtMath(buf: []u8, n: f64) []const u8 {
    if (!std.math.isFinite(n)) return "";
    const plain = fmtFloat(buf, n);
    if (plain.len > 0) return plain;
    return std.fmt.bufPrint(buf, "{e}", .{n}) catch "";
}

fn parseMathFunc(name: []const u8) ?ScalarSpec.MathArgs.Func {
    if (std.mem.eql(u8, name, "sqrt")) return .sqrt;
    if (std.mem.eql(u8, name, "power") or std.mem.eql(u8, name, "pow")) return .power;
    if (std.mem.eql(u8, name, "ln")) return .ln;
    if (std.mem.eql(u8, name, "log")) return .log;
    if (std.mem.eql(u8, name, "exp")) return .exp;
    if (std.mem.eql(u8, name, "trunc")) return .trunc;
    return null;
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

test "REVERSE supports direct and nested string transforms" {
    var parse_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer parse_arena.deinit();
    const allocator = parse_arena.allocator();

    var column_map = std.StringHashMap(usize).init(allocator);
    try column_map.put("word", 0);

    var direct = (try tryParseScalar("REVERSE(word)", column_map, allocator)).?;
    defer direct.deinit(allocator);
    var outer = (try tryParseScalar("REVERSE(UPPER(word))", column_map, allocator)).?;
    defer outer.deinit(allocator);
    var inner = (try tryParseScalar("TRIM(REVERSE(word))", column_map, allocator)).?;
    defer inner.deinit(allocator);

    var buf: [128]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    try std.testing.expectEqualStrings("éfac", eval(direct, &.{"café"}, fba.allocator()));
    fba.reset();
    try std.testing.expectEqualStrings("éFAC", eval(outer, &.{"café"}, fba.allocator()));
    fba.reset();
    try std.testing.expectEqualStrings("cba", eval(inner, &.{" abc "}, fba.allocator()));
    fba.reset();
    const invalid = [_]u8{ 0xff, 'a', 0xc0 };
    const expected_invalid = [_]u8{ 0xc0, 'a', 0xff };
    try std.testing.expectEqualSlices(u8, expected_invalid[0..], eval(direct, &.{invalid[0..]}, fba.allocator()));
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

test "NULLIF: numeric literal compares as a number" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var column_map = std.StringHashMap(usize).init(allocator);
    try column_map.put("age", 0);

    const spec = (try tryParseScalar("NULLIF(age, -1)", column_map, allocator)).?;
    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const a = fba.allocator();

    try std.testing.expectEqualStrings("", eval(spec, &.{"-1"}, a));
    try std.testing.expectEqualStrings("", eval(spec, &.{"-1.0"}, a));
    try std.testing.expectEqualStrings("", eval(spec, &.{" -1 "}, a));
    try std.testing.expectEqualStrings("30", eval(spec, &.{"30"}, a));
    try std.testing.expectEqualStrings("", eval(spec, &.{""}, a));
    // Not a number: nothing to compare, the field passes through.
    try std.testing.expectEqualStrings("n/a", eval(spec, &.{"n/a"}, a));
}

test "NULLIF: quoted literal compares byte for byte" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var column_map = std.StringHashMap(usize).init(allocator);
    try column_map.put("name", 0);

    const spec = (try tryParseScalar("NULLIF(name, 'N/A, unknown')", column_map, allocator)).?;
    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const a = fba.allocator();

    try std.testing.expectEqualStrings("", eval(spec, &.{"N/A, unknown"}, a));
    try std.testing.expectEqualStrings("n/a, unknown", eval(spec, &.{"n/a, unknown"}, a));
    try std.testing.expectEqualStrings("Alice", eval(spec, &.{"Alice"}, a));

    // A quoted number is a string: "30" does not match "30.0".
    const quoted = (try tryParseScalar("NULLIF(name, '30')", column_map, allocator)).?;
    try std.testing.expectEqualStrings("", eval(quoted, &.{"30"}, a));
    try std.testing.expectEqualStrings("30.0", eval(quoted, &.{"30.0"}, a));
}

test "NULLIF: rejects a wrong argument count and a non-literal second argument" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var column_map = std.StringHashMap(usize).init(allocator);
    try column_map.put("age", 0);

    try std.testing.expectError(error.InvalidQuery, tryParseScalar("NULLIF(age)", column_map, allocator));
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("NULLIF(age, 1, 2)", column_map, allocator));
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("NULLIF(age, )", column_map, allocator));
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("NULLIF(age, nan)", column_map, allocator));
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("NULLIF(age, unquoted)", column_map, allocator));
    try std.testing.expectError(error.ColumnNotFound, tryParseScalar("NULLIF(nope, 1)", column_map, allocator));
}

test "IFNULL: literal and column fallbacks" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var column_map = std.StringHashMap(usize).init(allocator);
    try column_map.put("a", 0);
    try column_map.put("b", 1);

    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const alloc = fba.allocator();

    const lit = (try tryParseScalar("IFNULL(a, 'x, y')", column_map, allocator)).?;
    try std.testing.expectEqualStrings("x, y", eval(lit, &.{ "", "B" }, alloc));
    try std.testing.expectEqualStrings("x, y", eval(lit, &.{ "  ", "B" }, alloc));
    try std.testing.expectEqualStrings("A", eval(lit, &.{ "A", "B" }, alloc));

    const num = (try tryParseScalar("IFNULL(a, 0)", column_map, allocator)).?;
    try std.testing.expectEqualStrings("0", eval(num, &.{ "", "B" }, alloc));

    const col = (try tryParseScalar("IFNULL(a, b)", column_map, allocator)).?;
    try std.testing.expectEqualStrings("B", eval(col, &.{ "", "B" }, alloc));
    try std.testing.expectEqualStrings("A", eval(col, &.{ "A", "B" }, alloc));
    try std.testing.expectEqualStrings("", eval(col, &.{ "", "" }, alloc));
}

test "IFNULL: needs exactly two arguments with a column first" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var column_map = std.StringHashMap(usize).init(allocator);
    try column_map.put("a", 0);
    try column_map.put("b", 1);

    try std.testing.expectError(error.InvalidQuery, tryParseScalar("IFNULL(a)", column_map, allocator));
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("IFNULL(a, b, 'x')", column_map, allocator));
    try std.testing.expectError(error.ColumnNotFound, tryParseScalar("IFNULL('x', a)", column_map, allocator));
    try std.testing.expectError(error.ColumnNotFound, tryParseScalar("IFNULL(a, nope)", column_map, allocator));
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

test "CONCAT: mixed columns and string literals" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var column_map = std.StringHashMap(usize).init(allocator);
    try column_map.put("name", 0);
    try column_map.put("dept", 1);

    const spec = (try tryParseScalar("CONCAT(name, '-', dept)", column_map, allocator)).?;
    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    try std.testing.expectEqualStrings("Alice-Eng", eval(spec, &.{ "Alice", "Eng" }, fba.allocator()));
}

test "CONCAT: literal containing a comma is not split as a separate arg" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var column_map = std.StringHashMap(usize).init(allocator);
    try column_map.put("name", 0);

    const spec = (try tryParseScalar("CONCAT(name, ', hello')", column_map, allocator)).?;
    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    try std.testing.expectEqualStrings("Alice, hello", eval(spec, &.{"Alice"}, fba.allocator()));
}

test "CONCAT: unresolvable column errors" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var column_map = std.StringHashMap(usize).init(allocator);
    try column_map.put("name", 0);

    try std.testing.expectError(
        error.ColumnNotFound,
        tryParseScalar("CONCAT(name, missing_col)", column_map, allocator),
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

test "LTRIM/RTRIM: trim one side only" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("s", 0);

    const lt = (try tryParseScalar("LTRIM(s)", cm, allocator)).?;
    const rt = (try tryParseScalar("rtrim(s)", cm, allocator)).?;
    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const a = fba.allocator();

    try std.testing.expectEqualStrings("ab \t", eval(lt, &.{" \t ab \t"}, a));
    try std.testing.expectEqualStrings(" \t ab", eval(rt, &.{" \t ab \t"}, a));
    try std.testing.expectEqualStrings("a b", eval(lt, &.{"a b"}, a));
    try std.testing.expectEqualStrings("", eval(lt, &.{"   "}, a));
    try std.testing.expectEqualStrings("", eval(rt, &.{"   "}, a));
    try std.testing.expectEqualStrings("", eval(lt, &.{""}, a));
}

test "LTRIM/RTRIM: can be the inner call of a nested function" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("s", 0);

    const spec = (try tryParseScalar("UPPER(LTRIM(s))", cm, allocator)).?;
    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    try std.testing.expectEqualStrings("AB ", eval(spec, &.{"  ab "}, fba.allocator()));
}

test "LEFT/RIGHT: counts, boundaries and negative counts" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("s", 0);

    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const a = fba.allocator();

    const cases = [_]struct { expr: []const u8, input: []const u8, want: []const u8 }{
        .{ .expr = "LEFT(s, 3)", .input = "abcdef", .want = "abc" },
        .{ .expr = "RIGHT(s, 3)", .input = "abcdef", .want = "def" },
        .{ .expr = "LEFT(s, 6)", .input = "abcdef", .want = "abcdef" },
        .{ .expr = "RIGHT(s, 6)", .input = "abcdef", .want = "abcdef" },
        .{ .expr = "LEFT(s, 99)", .input = "abcdef", .want = "abcdef" },
        .{ .expr = "RIGHT(s, 99)", .input = "abcdef", .want = "abcdef" },
        .{ .expr = "LEFT(s, 0)", .input = "abcdef", .want = "" },
        .{ .expr = "RIGHT(s, 0)", .input = "abcdef", .want = "" },
        .{ .expr = "LEFT(s, -2)", .input = "abcdef", .want = "abcd" },
        .{ .expr = "RIGHT(s, -2)", .input = "abcdef", .want = "cdef" },
        .{ .expr = "LEFT(s, -6)", .input = "abcdef", .want = "" },
        .{ .expr = "RIGHT(s, -99)", .input = "abcdef", .want = "" },
        .{ .expr = "LEFT(s, 9223372036854775807)", .input = "abc", .want = "abc" },
        .{ .expr = "RIGHT(s, 9223372036854775807)", .input = "abc", .want = "abc" },
        .{ .expr = "LEFT(s, -9223372036854775808)", .input = "abc", .want = "" },
        .{ .expr = "RIGHT(s, -9223372036854775808)", .input = "abc", .want = "" },
        .{ .expr = "left( s ,2 )", .input = "abc", .want = "ab" },
        .{ .expr = "LEFT(s, 2)", .input = "", .want = "" },
        .{ .expr = "RIGHT(s, 2)", .input = "", .want = "" },
    };
    for (cases) |c| {
        const spec = (try tryParseScalar(c.expr, cm, allocator)).?;
        try std.testing.expectEqualStrings(c.want, eval(spec, &.{c.input}, a));
    }
}

test "LEFT/RIGHT: count bytes but never split a multi-byte character" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("s", 0);

    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const a = fba.allocator();

    // "é" is two bytes (c3 a9), "€" is three.
    const word = "h\xc3\xa9llo";
    const left3 = (try tryParseScalar("LEFT(s, 3)", cm, allocator)).?;
    const left2 = (try tryParseScalar("LEFT(s, 2)", cm, allocator)).?;
    const right4 = (try tryParseScalar("RIGHT(s, 4)", cm, allocator)).?;
    const right5 = (try tryParseScalar("RIGHT(s, 5)", cm, allocator)).?;
    try std.testing.expectEqualStrings("h\xc3\xa9", eval(left3, &.{word}, a));
    try std.testing.expectEqualStrings("h", eval(left2, &.{word}, a));
    try std.testing.expectEqualStrings("llo", eval(right4, &.{word}, a));
    try std.testing.expectEqualStrings("\xc3\xa9llo", eval(right5, &.{word}, a));

    const euro = (try tryParseScalar("LEFT(s, 4)", cm, allocator)).?;
    try std.testing.expectEqualStrings("\xe2\x82\xac", eval(euro, &.{"\xe2\x82\xac\xe2\x82\xac"}, a));
}

test "LEFT/RIGHT: malformed calls are query errors" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("s", 0);

    try std.testing.expectError(error.InvalidQuery, tryParseScalar("LEFT(s)", cm, allocator));
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("RIGHT(s, 1, 2)", cm, allocator));
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("LEFT(s, 1.5)", cm, allocator));
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("RIGHT(s, x)", cm, allocator));
    try std.testing.expectError(error.ColumnNotFound, tryParseScalar("LEFT(nope, 1)", cm, allocator));
}

test "LPAD/RPAD: basic pad-left and pad-right" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("s", 0);

    const lp = (try tryParseScalar("LPAD(s, 5, 'x')", cm, allocator)).?;
    const rp = (try tryParseScalar("RPAD(s, 5, 'x')", cm, allocator)).?;
    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);

    try std.testing.expectEqualStrings("xxxhi", eval(lp, &.{"hi"}, fba.allocator()));
    fba.reset();
    try std.testing.expectEqualStrings("hixxx", eval(rp, &.{"hi"}, fba.allocator()));
}

test "LPAD/RPAD: default pad is a space when the third arg is omitted" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("s", 0);

    const lp = (try tryParseScalar("LPAD(s, 5)", cm, allocator)).?;
    const rp = (try tryParseScalar("RPAD(s, 5)", cm, allocator)).?;
    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);

    try std.testing.expectEqualStrings("   hi", eval(lp, &.{"hi"}, fba.allocator()));
    fba.reset();
    try std.testing.expectEqualStrings("hi   ", eval(rp, &.{"hi"}, fba.allocator()));
}

test "LPAD/RPAD: a multi-character pad wraps to fill the remaining width" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("s", 0);

    const lp = (try tryParseScalar("LPAD(s, 8, 'ab')", cm, allocator)).?;
    const rp = (try tryParseScalar("RPAD(s, 8, 'ab')", cm, allocator)).?;
    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);

    try std.testing.expectEqualStrings("abababhi", eval(lp, &.{"hi"}, fba.allocator()));
    fba.reset();
    try std.testing.expectEqualStrings("hiababab", eval(rp, &.{"hi"}, fba.allocator()));
}

test "LPAD/RPAD: a longer string is truncated to the target length" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("s", 0);

    const lp = (try tryParseScalar("LPAD(s, 3, 'x')", cm, allocator)).?;
    const rp = (try tryParseScalar("RPAD(s, 3, 'x')", cm, allocator)).?;
    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);

    // exactly at target length — unchanged
    try std.testing.expectEqualStrings("cat", eval(lp, &.{"cat"}, fba.allocator()));
    fba.reset();
    try std.testing.expectEqualStrings("cat", eval(rp, &.{"cat"}, fba.allocator()));
    fba.reset();
    // longer than target length — cut down, as DuckDB/Postgres do
    try std.testing.expectEqualStrings("ele", eval(lp, &.{"elephant"}, fba.allocator()));
    fba.reset();
    try std.testing.expectEqualStrings("ele", eval(rp, &.{"elephant"}, fba.allocator()));
    fba.reset();
    // truncating never splits a codepoint: the 2-byte 'é' does not fit in 2
    const lp2 = (try tryParseScalar("LPAD(s, 2, 'x')", cm, allocator)).?;
    try std.testing.expectEqualStrings("h", eval(lp2, &.{"h\u{e9}llo"}, fba.allocator()));
}

test "LPAD/RPAD: an empty field is NULL and is left alone" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("s", 0);

    const lp = (try tryParseScalar("LPAD(s, 5, 'x')", cm, allocator)).?;
    const rp = (try tryParseScalar("RPAD(s, 5, 'x')", cm, allocator)).?;
    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);

    try std.testing.expectEqualStrings("", eval(lp, &.{""}, fba.allocator()));
    fba.reset();
    try std.testing.expectEqualStrings("", eval(rp, &.{""}, fba.allocator()));
}

test "LPAD/RPAD: an explicit empty pad string is a no-op when padding is needed" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("s", 0);

    const lp = (try tryParseScalar("LPAD(s, 5, '')", cm, allocator)).?;
    const rp = (try tryParseScalar("RPAD(s, 5, '')", cm, allocator)).?;
    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);

    try std.testing.expectEqualStrings("hi", eval(lp, &.{"hi"}, fba.allocator()));
    fba.reset();
    try std.testing.expectEqualStrings("hi", eval(rp, &.{"hi"}, fba.allocator()));
    fba.reset();
    // truncation needs no pad character, so it still applies
    try std.testing.expectEqualStrings("eleph", eval(lp, &.{"elephant"}, fba.allocator()));
}

fn expectMath(expr: []const u8, input: []const u8, expected: []const u8) !void {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("v", 0);
    const spec = (try tryParseScalar(expr, cm, allocator)).?;
    try std.testing.expectEqualStrings(expected, eval(spec, &.{input}, allocator));
}

/// Like expectMath, but compares numerically with a relative tolerance.
///
/// IEEE 754 requires `sqrt` to be correctly rounded, so SQRT results are
/// bit-identical on every platform and expectMath is right for them. It does
/// not require that of `exp`, `log` or `pow`, so those are whatever the
/// platform's libm returns and can differ in the last bit: glibc on x86_64
/// gives EXP(1) as 2.7182818284590455 where Apple's libm gives
/// 2.718281828459045, and LOG(3, 81) as 4.000000000000001 rather than 4.0.
/// Asserting an exact decimal string for a transcendental therefore passes on
/// one CI runner and fails on another, which is how this landed red.
fn expectMathApprox(expr: []const u8, input: []const u8, expected: f64) !void {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("v", 0);
    const spec = (try tryParseScalar(expr, cm, allocator)).?;
    const out = eval(spec, &.{input}, allocator);
    const got = std.fmt.parseFloat(f64, out) catch {
        std.debug.print("expected a number near {d}, got \"{s}\"\n", .{ expected, out });
        return error.TestExpectedApproxEqAbs;
    };
    // 1e-12 relative is far tighter than any real libm disagreement and far
    // looser than one ulp of decimal formatting.
    try std.testing.expectApproxEqRel(expected, got, 1e-12);
}

test "SQRT: results are doubles, a negative input is NULL" {
    try expectMath("SQRT(v)", "16", "4.0");
    try expectMath("SQRT(v)", "2", "1.4142135623730951");
    try expectMath("SQRT(v)", "0", "0.0");
    try expectMath("sqrt(v)", "2.25", "1.5");
    try expectMath("SQRT(v)", "-4", "");
}

test "POWER/POW: literal exponent, NaN and overflow are NULL" {
    try expectMath("POWER(v, 2)", "3", "9.0");
    try expectMath("POW(v, 2)", "1.5", "2.25");
    try expectMath("POWER(v, 0.5)", "16", "4.0");
    try expectMath("POWER(v, -1)", "4", "0.25");
    try expectMath("POWER(v, 0)", "7", "1.0");
    try expectMath("POWER(v, -1)", "0", "");
    try expectMath("POWER(v, 0.5)", "-4", "");
    try expectMath("POWER(v, 1000)", "10", "");
    try expectMath("POWER(v, 3)", "-2", "-8.0");
}

test "LN: zero and negative inputs are NULL" {
    try expectMath("LN(v)", "1", "0.0");
    try expectMathApprox("LN(v)", "2.718281828459045", 1.0);
    try expectMath("LN(v)", "0", "");
    try expectMath("LN(v)", "-1", "");
}

test "LOG: base 10 by default, optional literal base first" {
    try expectMath("LOG(v)", "1000", "3.0");
    try expectMath("LOG(v)", "0.01", "-2.0");
    try expectMath("LOG(v)", "0", "");
    try expectMath("LOG(v)", "-5", "");
    try expectMath("LOG(2, v)", "8", "3.0");
    try expectMathApprox("LOG(3, v)", "81", 4.0); // ratio of logs, not exact on glibc
    try expectMath("LOG(2.5, v)", "0", "");
    try expectMath("LOG(2, v)", "-8", "");
}

test "LOG: a base that is not positive or is 1 is rejected at parse time" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("v", 0);
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("LOG(1, v)", cm, allocator));
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("LOG(0, v)", cm, allocator));
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("LOG(-2, v)", cm, allocator));
}

test "EXP: overflow is NULL, underflow to zero is kept" {
    try expectMath("EXP(v)", "0", "1.0");
    try expectMathApprox("EXP(v)", "1", 2.718281828459045);
    try expectMath("EXP(v)", "1000", "");
    try expectMath("EXP(v)", "-1000", "0.0");
}

test "TRUNC: rounds toward zero, unlike FLOOR and CEIL" {
    try expectMath("TRUNC(v)", "-2.7", "-2.0");
    try expectMath("TRUNC(v)", "2.7", "2.0");
    try expectMath("TRUNC(v)", "-0.5", "0.0");
    try expectMath("TRUNC(v)", "5", "5.0");
    try expectMath("FLOOR(v)", "-2.7", "-3.0");
    try expectMath("CEIL(v)", "-2.7", "-2.0");
}

test "math functions: empty stays NULL, non-numeric text passes through" {
    const exprs = [_][]const u8{ "SQRT(v)", "POWER(v, 2)", "LN(v)", "LOG(v)", "LOG(2, v)", "EXP(v)", "TRUNC(v)" };
    for (exprs) |expr| {
        try expectMath(expr, "", "");
        try expectMath(expr, "abc", "abc");
    }
}

test "math functions: non-finite input text gives NULL" {
    try expectMath("SQRT(v)", "inf", "");
    try expectMath("TRUNC(v)", "nan", "");
}

test "math functions: huge and tiny results are not dropped" {
    try expectMath("POWER(v, 20)", "10", "100000000000000000000");

    // Too long for the 32-byte buffer in plain decimal, so scientific notation.
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("v", 0);
    const big = (try tryParseScalar("POWER(v, 300)", cm, allocator)).?;
    const big_out = eval(big, &.{"10"}, allocator);
    try std.testing.expect(std.mem.indexOfScalar(u8, big_out, 'e') != null);
    try std.testing.expectApproxEqRel(@as(f64, 1e300), try std.fmt.parseFloat(f64, big_out), 1e-9);
    const tiny = (try tryParseScalar("EXP(v)", cm, allocator)).?;
    const tiny_out = eval(tiny, &.{"-700"}, allocator);
    try std.testing.expect(std.mem.indexOfScalar(u8, tiny_out, 'e') != null);
    try std.testing.expectApproxEqRel(@as(f64, 9.85967654375977e-305), try std.fmt.parseFloat(f64, tiny_out), 1e-9);
}

test "math functions: wrong argument shapes are rejected" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("v", 0);
    try cm.put("w", 1);
    const one_arg = [_][]const u8{ "SQRT", "LN", "EXP", "TRUNC", "LOG" };
    for (one_arg) |name| {
        const two = try std.fmt.allocPrint(allocator, "{s}(v, w, v)", .{name});
        try std.testing.expect((tryParseScalar(two, cm, allocator) catch null) == null);
        const none = try std.fmt.allocPrint(allocator, "{s}()", .{name});
        try std.testing.expectError(error.ColumnNotFound, tryParseScalar(none, cm, allocator));
    }
    try std.testing.expectError(error.ColumnNotFound, tryParseScalar("SQRT(v, w)", cm, allocator));
    try std.testing.expect((try tryParseScalar("POWER(v)", cm, allocator)) == null);
    try std.testing.expect((try tryParseScalar("POWER(v, w)", cm, allocator)) == null);
    try std.testing.expect((try tryParseScalar("POWER(v, 2, 3)", cm, allocator)) == null);
    try std.testing.expectError(error.ColumnNotFound, tryParseScalar("SQRT(nosuch)", cm, allocator));
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

test "clock keywords resolve to a fixed instant, alone or nested" {
    const allocator = std.testing.allocator;
    var cm = std.StringHashMap(usize).init(allocator);
    defer cm.deinit();
    const ts = try datetime.parseDateTime("2026-03-05 07:08:09");
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const cases = [_]struct { sql: []const u8, want: []const u8 }{
        .{ .sql = "CURRENT_DATE", .want = "2026-03-05" },
        .{ .sql = "current_date()", .want = "2026-03-05" },
        .{ .sql = "NOW()", .want = "2026-03-05 07:08:09" },
        .{ .sql = "CURRENT_TIMESTAMP", .want = "2026-03-05 07:08:09" },
        .{ .sql = "UPPER(NOW())", .want = "2026-03-05 07:08:09" },
    };
    for (cases) |c| {
        var spec = (try tryParseScalarAt(c.sql, cm, allocator, ts)).?;
        defer spec.deinit(allocator);
        try std.testing.expectEqualStrings(c.want, eval(spec, &.{}, arena.allocator()));
    }
    try std.testing.expect((try tryParseScalarAt("NOW(1)", cm, allocator, ts)) == null);
}

test "REPEAT: counts, empty field, and the output cap" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("s", 0);

    const cases = [_]struct { expr: []const u8, input: []const u8, want: []const u8 }{
        .{ .expr = "REPEAT(s, 3)", .input = "ab", .want = "ababab" },
        .{ .expr = "REPEAT(s, 1)", .input = "ab", .want = "ab" },
        .{ .expr = "REPEAT(s, 0)", .input = "ab", .want = "" },
        .{ .expr = "REPEAT(s, -2)", .input = "ab", .want = "" },
        .{ .expr = "REPEAT(s, -9223372036854775808)", .input = "ab", .want = "" },
        .{ .expr = "REPEAT(s, 9223372036854775807)", .input = "ab", .want = "" },
        .{ .expr = "repeat( s ,2 )", .input = "x", .want = "xx" },
        .{ .expr = "REPEAT(s, 3)", .input = "", .want = "" },
        // Whole characters are repeated, so multi-byte text stays valid UTF-8.
        .{ .expr = "REPEAT(s, 2)", .input = "\xc3\xa9", .want = "\xc3\xa9\xc3\xa9" },
    };
    for (cases) |c| {
        const spec = (try tryParseScalar(c.expr, cm, allocator)).?;
        try std.testing.expectEqualStrings(c.want, eval(spec, &.{c.input}, allocator));
    }

    // The cap is on the result: exactly max_repeat_bytes is built, one more is empty.
    const exact = (try tryParseScalar("REPEAT(s, 1048576)", cm, allocator)).?;
    const over = (try tryParseScalar("REPEAT(s, 1048577)", cm, allocator)).?;
    const long = (try tryParseScalar("REPEAT(s, 524289)", cm, allocator)).?;
    try std.testing.expectEqual(max_repeat_bytes, eval(exact, &.{"a"}, allocator).len);
    try std.testing.expectEqualStrings("", eval(over, &.{"a"}, allocator));
    try std.testing.expectEqualStrings("", eval(long, &.{"ab"}, allocator));

    try std.testing.expectError(error.InvalidQuery, tryParseScalar("REPEAT(s)", cm, allocator));
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("REPEAT(s, 1, 2)", cm, allocator));
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("REPEAT(s, 1.5)", cm, allocator));
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("REPEAT(s, x)", cm, allocator));
    try std.testing.expectError(error.ColumnNotFound, tryParseScalar("REPEAT(nope, 2)", cm, allocator));
}

test "POSITION/INSTR: 1-based byte index, 0 when absent, empty field is NULL" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    var cm = std.StringHashMap(usize).init(allocator);
    try cm.put("s", 0);

    const cases = [_]struct { expr: []const u8, input: []const u8, want: []const u8 }{
        .{ .expr = "POSITION('lo' IN s)", .input = "hello", .want = "4" },
        .{ .expr = "INSTR(s, 'lo')", .input = "hello", .want = "4" },
        .{ .expr = "position( 'h' in s )", .input = "hello", .want = "1" },
        .{ .expr = "INSTR(s, 'o')", .input = "hello", .want = "5" },
        .{ .expr = "INSTR(s, 'l')", .input = "hello", .want = "3" },
        .{ .expr = "INSTR(s, 'z')", .input = "hello", .want = "0" },
        .{ .expr = "INSTR(s, 'hello!')", .input = "hello", .want = "0" },
        .{ .expr = "INSTR(s, 'Hello')", .input = "hello", .want = "0" },
        .{ .expr = "INSTR(s, '')", .input = "hello", .want = "1" },
        .{ .expr = "POSITION('' IN s)", .input = "hello", .want = "1" },
        .{ .expr = "INSTR(s, ',')", .input = "a,b", .want = "2" },
        .{ .expr = "POSITION(' IN ' IN s)", .input = "x IN y", .want = "2" },
        .{ .expr = "POSITION('a,b' IN s)", .input = "xa,b", .want = "2" },
        .{ .expr = "INSTR(s, 'a')", .input = "", .want = "" },
        .{ .expr = "POSITION('a' IN s)", .input = "", .want = "" },
        // Bytes, like SUBSTR and LENGTH: "é" is two bytes, so "l" is at byte 4.
        .{ .expr = "INSTR(s, 'l')", .input = "h\xc3\xa9llo", .want = "4" },
        .{ .expr = "INSTR(s, '\xc3\xa9')", .input = "h\xc3\xa9llo", .want = "2" },
    };
    for (cases) |c| {
        const spec = (try tryParseScalar(c.expr, cm, allocator)).?;
        try std.testing.expectEqualStrings(c.want, eval(spec, &.{c.input}, allocator));
    }

    try std.testing.expectError(error.InvalidQuery, tryParseScalar("INSTR(s)", cm, allocator));
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("INSTR(s, a)", cm, allocator));
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("INSTR(s, 'a', 'b')", cm, allocator));
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("POSITION('a' s)", cm, allocator));
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("POSITION('a' INs)", cm, allocator));
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("POSITION(a IN s)", cm, allocator));
    try std.testing.expectError(error.InvalidQuery, tryParseScalar("POSITION('a)", cm, allocator));
    try std.testing.expectError(error.ColumnNotFound, tryParseScalar("INSTR(nope, 'a')", cm, allocator));
    try std.testing.expectError(error.ColumnNotFound, tryParseScalar("POSITION('a' IN nope)", cm, allocator));
}

test "POSITION/INSTR: a doubled single quote in the literal is one quote (#257)" {
    var cm = std.StringHashMap(usize).init(std.testing.allocator);
    defer cm.deinit();
    try cm.put("s", 0);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const cases = [_]struct { expr: []const u8, input: []const u8, want: []const u8 }{
        .{ .expr = "INSTR(s, 'it''s')", .input = "say it's ok", .want = "5" },
        .{ .expr = "INSTR(s, 'it''s')", .input = "say it''s ok", .want = "0" },
        .{ .expr = "POSITION('''' IN s)", .input = "it's", .want = "3" },
        .{ .expr = "POSITION('it''s' IN s)", .input = "say it's ok", .want = "5" },
        .{ .expr = "INSTR(s, '''')", .input = "''", .want = "1" },
        .{ .expr = "INSTR(s, '''''')", .input = "a''b", .want = "2" },
    };
    for (cases) |c| {
        // Parse with the testing allocator so deinit must free the needle.
        var spec = (try tryParseScalar(c.expr, cm, std.testing.allocator)).?;
        defer spec.deinit(std.testing.allocator);
        try std.testing.expectEqualStrings(c.want, eval(spec, &.{c.input}, arena.allocator()));
    }
}
