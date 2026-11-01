//! arith.zig — `+ - * /` over columns and numeric literals, with parentheses
//! and unary minus. Used for SELECT columns (`price * qty`) and aggregate
//! arguments (`SUM(price * qty)`).
//!
//! An empty or non-numeric operand, or division by zero, evaluates to null
//! (SQL NULL), so SUM/AVG skip that row and a SELECT column prints empty.
//! Function calls inside an expression (`ROUND(a) * 2`) are not supported yet.
const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Expr = union(enum) {
    col: usize,
    /// A column by lowercase name, for WHERE, which is parsed before the
    /// header is known. Owned.
    name: []const u8,
    num: f64,
    neg: *Expr,
    bin: Bin,

    pub const Bin = struct { op: u8, l: *Expr, r: *Expr };

    pub fn deinit(self: *Expr, allocator: Allocator) void {
        switch (self.*) {
            .neg => |e| e.deinit(allocator),
            .bin => |b| {
                b.l.deinit(allocator);
                b.r.deinit(allocator);
            },
            .name => |n| allocator.free(n),
            else => {},
        }
        allocator.destroy(self);
    }

    pub fn eval(self: *const Expr, record: []const []const u8) ?f64 {
        return self.evalIn(struct {
            record: []const []const u8,
            fn leaf(c: @This(), e: *const Expr) ?f64 {
                return switch (e.*) {
                    .col => |i| if (i < c.record.len) parseNum(c.record[i]) else null,
                    else => null,
                };
            }
        }{ .record = record });
    }

    /// Evaluate with `.name` leaves looked up in `header` (lowercase names).
    pub fn evalNamed(self: *const Expr, fields: []const []const u8, header: []const []const u8) ?f64 {
        return self.evalIn(struct {
            fields: []const []const u8,
            header: []const []const u8,
            fn leaf(c: @This(), e: *const Expr) ?f64 {
                const n = switch (e.*) {
                    .name => |n| n,
                    else => return null,
                };
                for (c.header, 0..) |h, i| {
                    if (std.mem.eql(u8, h, n)) return if (i < c.fields.len) parseNum(c.fields[i]) else null;
                }
                return null;
            }
        }{ .fields = fields, .header = header });
    }

    /// Evaluate with `.name` leaves looked up in a row map (the JOIN path).
    pub fn evalRow(self: *const Expr, row: std.StringHashMap([]const u8)) ?f64 {
        return self.evalIn(struct {
            row: std.StringHashMap([]const u8),
            fn leaf(c: @This(), e: *const Expr) ?f64 {
                return switch (e.*) {
                    .name => |n| if (c.row.get(n)) |v| parseNum(v) else null,
                    else => null,
                };
            }
        }{ .row = row });
    }

    fn evalIn(self: *const Expr, ctx: anytype) ?f64 {
        return switch (self.*) {
            .num => |n| n,
            .col, .name => ctx.leaf(self),
            .neg => |e| if (e.evalIn(ctx)) |v| -v else null,
            .bin => |b| {
                const l = b.l.evalIn(ctx) orelse return null;
                const r = b.r.evalIn(ctx) orelse return null;
                return switch (b.op) {
                    '+' => l + r,
                    '-' => l - r,
                    '*' => l * r,
                    '/' => if (r == 0) null else l / r,
                    else => unreachable,
                };
            },
        };
    }

    /// The first `.name` leaf `has` rejects, for column validation.
    pub fn missingName(self: *const Expr, ctx: anytype) ?[]const u8 {
        return switch (self.*) {
            .name => |n| if (ctx.has(n)) null else n,
            .col, .num => null,
            .neg => |e| e.missingName(ctx),
            .bin => |b| b.l.missingName(ctx) orelse b.r.missingName(ctx),
        };
    }

    /// First column the expression reads, or 0 when it reads none.
    pub fn firstCol(self: *const Expr) usize {
        return switch (self.*) {
            .col => |i| i,
            .num, .name => 0,
            .neg => |e| e.firstCol(),
            .bin => |b| if (b.l.reads()) b.l.firstCol() else b.r.firstCol(),
        };
    }

    fn reads(self: *const Expr) bool {
        return switch (self.*) {
            .col, .name => true,
            .num => false,
            .neg => |e| e.reads(),
            .bin => |b| b.l.reads() or b.r.reads(),
        };
    }
};

fn parseNum(field: []const u8) ?f64 {
    const t = std.mem.trim(u8, field, &std.ascii.whitespace);
    if (t.len == 0) return null;
    return std.fmt.parseFloat(f64, t) catch null;
}

/// True when `text` has an arithmetic operator outside quotes and parentheses,
/// ignoring a leading sign. `SUBSTR(name, -1)` and `'2024-01-01'` do not count.
pub fn hasTopLevelOp(text: []const u8) bool {
    const t = std.mem.trim(u8, text, &std.ascii.whitespace);
    if (std.ascii.startsWithIgnoreCase(t, "case ")) return false;
    var depth: usize = 0;
    var quote: u8 = 0;
    for (t, 0..) |c, i| {
        if (quote != 0) {
            if (c == quote) quote = 0;
            continue;
        }
        switch (c) {
            '\'', '"' => quote = c,
            '(' => depth += 1,
            ')' => depth -|= 1,
            '+', '-', '*', '/' => if (depth == 0 and i > 0) return true,
            else => {},
        }
    }
    return false;
}

/// Parse an arithmetic expression. Column names resolve through `column_map`
/// (lowercase keys); a bare name is `[A-Za-z_][A-Za-z0-9_]*`, anything else
/// needs double quotes.
pub fn parse(allocator: Allocator, text: []const u8, column_map: std.StringHashMap(usize)) !*Expr {
    return parseIn(allocator, text, column_map);
}

/// Parse with columns kept as lowercase names, resolved at evaluation time.
pub fn parseNamed(allocator: Allocator, text: []const u8) !*Expr {
    return parseIn(allocator, text, null);
}

fn parseIn(allocator: Allocator, text: []const u8, column_map: ?std.StringHashMap(usize)) !*Expr {
    var p = Parser{ .allocator = allocator, .s = text, .map = column_map };
    const e = try p.expr();
    errdefer e.deinit(allocator);
    if (p.peek() != null) return error.ArithmeticExpressionsNotSupported;
    return e;
}

const Parser = struct {
    allocator: Allocator,
    s: []const u8,
    i: usize = 0,
    map: ?std.StringHashMap(usize),

    fn peek(p: *Parser) ?u8 {
        while (p.i < p.s.len and std.ascii.isWhitespace(p.s[p.i])) p.i += 1;
        return if (p.i < p.s.len) p.s[p.i] else null;
    }

    fn node(p: *Parser, v: Expr) !*Expr {
        const n = try p.allocator.create(Expr);
        n.* = v;
        return n;
    }

    fn binary(p: *Parser, op: u8, l: *Expr, r: *Expr) !*Expr {
        return p.node(.{ .bin = .{ .op = op, .l = l, .r = r } }) catch |e| {
            r.deinit(p.allocator);
            return e;
        };
    }

    fn expr(p: *Parser) anyerror!*Expr {
        var l = try p.term();
        errdefer l.deinit(p.allocator);
        while (p.peek()) |c| {
            if (c != '+' and c != '-') break;
            p.i += 1;
            l = try p.binary(c, l, try p.term());
        }
        return l;
    }

    fn term(p: *Parser) anyerror!*Expr {
        var l = try p.factor();
        errdefer l.deinit(p.allocator);
        while (p.peek()) |c| {
            if (c != '*' and c != '/') break;
            p.i += 1;
            l = try p.binary(c, l, try p.factor());
        }
        return l;
    }

    fn factor(p: *Parser) anyerror!*Expr {
        const c = p.peek() orelse return error.ArithmeticExpressionsNotSupported;
        if (c == '-' or c == '+') {
            p.i += 1;
            const e = try p.factor();
            if (c == '+') return e;
            return p.node(.{ .neg = e }) catch |err| {
                e.deinit(p.allocator);
                return err;
            };
        }
        if (c == '(') {
            p.i += 1;
            const e = try p.expr();
            errdefer e.deinit(p.allocator);
            if (p.peek() != ')') return error.ArithmeticExpressionsNotSupported;
            p.i += 1;
            return e;
        }
        if (c == '"') {
            const end = std.mem.indexOfScalarPos(u8, p.s, p.i + 1, '"') orelse return error.ArithmeticExpressionsNotSupported;
            const name = p.s[p.i + 1 .. end];
            p.i = end + 1;
            return p.column(name);
        }
        const start = p.i;
        if (std.ascii.isDigit(c) or c == '.') {
            while (p.i < p.s.len and (std.ascii.isDigit(p.s[p.i]) or p.s[p.i] == '.')) p.i += 1;
            const n = std.fmt.parseFloat(f64, p.s[start..p.i]) catch return error.ArithmeticExpressionsNotSupported;
            return p.node(.{ .num = n });
        }
        if (std.ascii.isAlphabetic(c) or c == '_') {
            while (p.i < p.s.len and (std.ascii.isAlphanumeric(p.s[p.i]) or p.s[p.i] == '_')) p.i += 1;
            const name = p.s[start..p.i];
            // A function call inside an expression is not evaluated yet.
            if (p.peek() == '(') return error.ArithmeticExpressionsNotSupported;
            return p.column(name);
        }
        return error.ArithmeticExpressionsNotSupported;
    }

    fn column(p: *Parser, name: []const u8) !*Expr {
        const lower = try std.ascii.allocLowerString(p.allocator, name);
        const map = p.map orelse return p.node(.{ .name = lower }) catch |e| {
            p.allocator.free(lower);
            return e;
        };
        defer p.allocator.free(lower);
        const idx = map.get(lower) orelse return error.ColumnNotFound;
        return p.node(.{ .col = idx });
    }
};

fn testMap(allocator: Allocator) !std.StringHashMap(usize) {
    var m = std.StringHashMap(usize).init(allocator);
    try m.put("price", 0);
    try m.put("qty", 1);
    try m.put("unit cost", 2);
    return m;
}

test "arith precedence, parens, unary minus, quoted names" {
    const a = std.testing.allocator;
    var m = try testMap(a);
    defer m.deinit();
    const rec = [_][]const u8{ "2.5", "4", "1" };
    const cases = [_]struct { src: []const u8, want: f64 }{
        .{ .src = "price * qty", .want = 10 },
        .{ .src = "price + qty * 2", .want = 10.5 },
        .{ .src = "(price + qty) * 2", .want = 13 },
        .{ .src = "-price + 1", .want = -1.5 },
        .{ .src = "qty / 8", .want = 0.5 },
        .{ .src = "PRICE*\"unit cost\"-0.5", .want = 2 },
        .{ .src = "10 - 2 - 3", .want = 5 },
    };
    for (cases) |c| {
        const e = try parse(a, c.src, m);
        defer e.deinit(a);
        try std.testing.expectEqual(c.want, e.eval(&rec).?);
    }
}

test "arith null on empty, non-numeric and divide by zero" {
    const a = std.testing.allocator;
    var m = try testMap(a);
    defer m.deinit();
    const e = try parse(a, "price / qty", m);
    defer e.deinit(a);
    try std.testing.expectEqual(@as(?f64, null), e.eval(&[_][]const u8{ "", "4" }));
    try std.testing.expectEqual(@as(?f64, null), e.eval(&[_][]const u8{ "x", "4" }));
    try std.testing.expectEqual(@as(?f64, null), e.eval(&[_][]const u8{ "1", "0" }));
}

test "arith rejects unknown columns, calls and junk" {
    const a = std.testing.allocator;
    var m = try testMap(a);
    defer m.deinit();
    try std.testing.expectError(error.ColumnNotFound, parse(a, "price * nope", m));
    try std.testing.expectError(error.ArithmeticExpressionsNotSupported, parse(a, "ROUND(price) * 2", m));
    try std.testing.expectError(error.ArithmeticExpressionsNotSupported, parse(a, "price * (qty", m));
    try std.testing.expectError(error.ArithmeticExpressionsNotSupported, parse(a, "price qty", m));
}

test "hasTopLevelOp" {
    try std.testing.expect(hasTopLevelOp("price*qty"));
    try std.testing.expect(hasTopLevelOp("(a + b) / 2"));
    try std.testing.expect(!hasTopLevelOp("SUBSTR(name, -1)"));
    try std.testing.expect(!hasTopLevelOp("SUM(a*b)"));
    try std.testing.expect(!hasTopLevelOp("-price"));
    try std.testing.expect(!hasTopLevelOp("'2024-01-01'"));
    try std.testing.expect(!hasTopLevelOp("CASE WHEN a > 1 THEN a-1 ELSE 0 END"));
}
