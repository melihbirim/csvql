//! Read the first worksheet of an .xlsx file as CSV, entirely in memory.
//!
//! An .xlsx is a zip of XML parts. We need four: xl/workbook.xml (sheet order),
//! xl/_rels/workbook.xml.rels (sheet -> part), xl/sharedStrings.xml (string
//! table) and xl/styles.xml (which number formats are dates). The sheet's cells
//! become CSV rows, so every query path works on an .xlsx unchanged.
//!
//! ponytail: first sheet only, formulas give their cached value, merged cells
//! keep the value in the top-left cell only. Add a sheet selector when asked.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const max_xlsx_bytes = 512 * 1024 * 1024;
const max_part_bytes = 1024 * 1024 * 1024;

pub fn isXlsx(path: []const u8) bool {
    return path.len > 5 and std.ascii.eqlIgnoreCase(path[path.len - 5 ..], ".xlsx");
}

/// Convert the first worksheet of the .xlsx at `path` to CSV. Caller frees.
pub fn fileToCsv(allocator: Allocator, path: []const u8) ![]u8 {
    const data = try std.fs.cwd().readFileAlloc(allocator, path, max_xlsx_bytes);
    defer allocator.free(data);
    return toCsv(allocator, data);
}

pub fn toCsv(allocator: Allocator, zip: []const u8) ![]u8 {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    const sheet_part = try firstSheetPart(a, zip);
    const sheet = (try readPart(a, zip, sheet_part)) orelse return error.XlsxMissingSheet;
    const strings = try sharedStrings(a, zip);
    const date_styles = try dateStyles(a, zip);

    // Collect rows first: the widest row sets the column count for all of them.
    var rows = std.ArrayList([][]const u8){};
    var width: usize = 0;
    var cells = std.ArrayList([]const u8){};
    var pos: usize = 0;
    while (nextTag(sheet, pos, "row")) |row_tag| {
        const row_end = if (row_tag.self_closing) row_tag.end else (std.mem.indexOfPos(u8, sheet, row_tag.end, "</row>") orelse sheet.len);
        cells.clearRetainingCapacity();
        var cpos = row_tag.end;
        while (cpos < row_end) {
            const c = nextTag(sheet[0..row_end], cpos, "c") orelse break;
            const body = if (c.self_closing) "" else sheet[c.end..(std.mem.indexOfPos(u8, sheet, c.end, "</c>") orelse row_end)];
            cpos = if (c.self_closing) c.end else c.end + body.len + 4;

            const col = if (attr(c.attrs, "r")) |ref| columnIndex(ref) else cells.items.len;
            while (cells.items.len < col) try cells.append(a, "");
            const value = try cellValue(a, c.attrs, body, strings, date_styles);
            if (col < cells.items.len) cells.items[col] = value else try cells.append(a, value);
        }
        pos = row_end;
        var last = cells.items.len;
        while (last > 0 and cells.items[last - 1].len == 0) last -= 1;
        if (last == 0) continue; // blank row
        width = @max(width, last);
        try rows.append(a, try a.dupe([]const u8, cells.items[0..last]));
    }

    var out = std.ArrayList(u8){};
    errdefer out.deinit(allocator);
    for (rows.items) |row| {
        for (0..width) |i| {
            if (i > 0) try out.append(allocator, ',');
            if (i < row.len) try writeCsvField(allocator, &out, row[i]);
        }
        try out.append(allocator, '\n');
    }
    return out.toOwnedSlice(allocator);
}

fn cellValue(a: Allocator, attrs: []const u8, body: []const u8, strings: []const []const u8, date_styles: []const DateKind) ![]const u8 {
    const t = attr(attrs, "t") orelse "n";
    if (std.mem.eql(u8, t, "inlineStr")) return textRuns(a, body);
    const v = if (innerText(body, "v")) |raw| try unescape(a, raw) else return "";
    if (std.mem.eql(u8, t, "s")) {
        const idx = std.fmt.parseInt(usize, v, 10) catch return error.XlsxBadSharedString;
        if (idx >= strings.len) return error.XlsxBadSharedString;
        return strings[idx];
    }
    if (std.mem.eql(u8, t, "b")) return if (std.mem.eql(u8, v, "1")) "true" else "false";
    if (std.mem.eql(u8, t, "n")) {
        const style = if (attr(attrs, "s")) |s| std.fmt.parseInt(usize, s, 10) catch 0 else 0;
        if (style < date_styles.len and date_styles[style] != .none) {
            const serial = std.fmt.parseFloat(f64, v) catch return v;
            return formatSerial(a, serial, date_styles[style]);
        }
    }
    return v; // number, str (formula string), e (error text)
}

// --- dates --------------------------------------------------------------------

const DateKind = enum { none, date, datetime, time };

/// Excel serial (days since 1899-12-30, 1900 date system) to ISO text.
fn formatSerial(a: Allocator, serial: f64, kind: DateKind) ![]const u8 {
    if (!(serial >= 0 and serial < 2958466)) return std.fmt.allocPrint(a, "{d}", .{serial});
    var days: i64 = @intFromFloat(@floor(serial));
    var secs: i64 = @intFromFloat(@round((serial - @floor(serial)) * 86400));
    if (secs >= 86400) {
        days += 1;
        secs -= 86400;
    }
    const hh = @divTrunc(secs, 3600);
    const mi = @divTrunc(@mod(secs, 3600), 60);
    const ss = @mod(secs, 60);
    if (kind == .time) return std.fmt.allocPrint(a, "{d:0>2}:{d:0>2}:{d:0>2}", .{ @as(u64, @intCast(hh)), @as(u64, @intCast(mi)), @as(u64, @intCast(ss)) });
    const ymd = civilFromDays(days - 25569); // 25569 = 1970-01-01
    if (kind == .date or secs == 0)
        return std.fmt.allocPrint(a, "{d:0>4}-{d:0>2}-{d:0>2}", .{ ymd.y, ymd.m, ymd.d });
    return std.fmt.allocPrint(a, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{ ymd.y, ymd.m, ymd.d, @as(u64, @intCast(hh)), @as(u64, @intCast(mi)), @as(u64, @intCast(ss)) });
}

/// Days since 1970-01-01 to a civil date (Howard Hinnant's algorithm).
fn civilFromDays(z0: i64) struct { y: u64, m: u64, d: u64 } {
    const z = z0 + 719468;
    const era = @divFloor(z, 146097);
    const doe = z - era * 146097;
    const yoe = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36524) - @divFloor(doe, 146096), 365);
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const d = doy - @divFloor(153 * mp + 2, 5) + 1;
    const m = if (mp < 10) mp + 3 else mp - 9;
    const y = yoe + era * 400 + @as(i64, if (m <= 2) 1 else 0);
    return .{ .y = @intCast(y), .m = @intCast(m), .d = @intCast(d) };
}

fn builtinFormatKind(id: usize) DateKind {
    return switch (id) {
        14...17 => .date,
        22 => .datetime,
        18...21, 45...47 => .time,
        else => .none,
    };
}

/// Classify a custom number format code: y or d means a date, h or s alone a time.
fn formatCodeKind(code: []const u8) DateKind {
    var has_date = false;
    var has_time = false;
    var i: usize = 0;
    while (i < code.len) : (i += 1) {
        switch (code[i]) {
            '"' => i = std.mem.indexOfScalarPos(u8, code, i + 1, '"') orelse code.len,
            '[' => i = std.mem.indexOfScalarPos(u8, code, i + 1, ']') orelse code.len,
            '\\', '_', '*' => i += 1,
            'y', 'Y', 'd', 'D' => has_date = true,
            'h', 'H', 's', 'S' => has_time = true,
            ';' => break, // only the first (positive) section matters
            else => {},
        }
    }
    if (has_date and has_time) return .datetime;
    if (has_date) return .date;
    if (has_time) return .time;
    return .none;
}

/// One DateKind per cellXfs entry (the cell's `s` attribute indexes this list).
fn dateStyles(a: Allocator, zip: []const u8) ![]const DateKind {
    const xml = (try readPart(a, zip, "xl/styles.xml")) orelse return &.{};
    var custom = std.AutoHashMap(usize, DateKind).init(a);
    if (sectionBody(xml, "numFmts")) |fmts| {
        var p: usize = 0;
        while (nextTag(fmts, p, "numFmt")) |t| : (p = t.end) {
            const id = std.fmt.parseInt(usize, attr(t.attrs, "numFmtId") orelse continue, 10) catch continue;
            const code = try unescape(a, attr(t.attrs, "formatCode") orelse continue);
            try custom.put(id, formatCodeKind(code));
        }
    }
    var kinds = std.ArrayList(DateKind){};
    const xfs = sectionBody(xml, "cellXfs") orelse return &.{};
    var p: usize = 0;
    while (nextTag(xfs, p, "xf")) |t| : (p = t.end) {
        const id = std.fmt.parseInt(usize, attr(t.attrs, "numFmtId") orelse "0", 10) catch 0;
        try kinds.append(a, custom.get(id) orelse builtinFormatKind(id));
    }
    return kinds.items;
}

// --- workbook parts -----------------------------------------------------------

fn firstSheetPart(a: Allocator, zip: []const u8) ![]const u8 {
    const fallback = "xl/worksheets/sheet1.xml";
    const wb = (try readPart(a, zip, "xl/workbook.xml")) orelse return fallback;
    const sheet = nextTag(wb, 0, "sheet") orelse return error.XlsxNoSheets;
    const rid = attr(sheet.attrs, "r:id") orelse return fallback;
    const rels = (try readPart(a, zip, "xl/_rels/workbook.xml.rels")) orelse return fallback;
    var p: usize = 0;
    while (nextTag(rels, p, "Relationship")) |t| : (p = t.end) {
        const id = attr(t.attrs, "Id") orelse continue;
        if (!std.mem.eql(u8, id, rid)) continue;
        const target = attr(t.attrs, "Target") orelse return fallback;
        if (target.len > 0 and target[0] == '/') return target[1..];
        return std.fmt.allocPrint(a, "xl/{s}", .{target});
    }
    return fallback;
}

fn sharedStrings(a: Allocator, zip: []const u8) ![]const []const u8 {
    const xml = (try readPart(a, zip, "xl/sharedStrings.xml")) orelse return &.{};
    var list = std.ArrayList([]const u8){};
    var p: usize = 0;
    while (nextTag(xml, p, "si")) |t| {
        if (t.self_closing) {
            try list.append(a, "");
            p = t.end;
            continue;
        }
        const end = std.mem.indexOfPos(u8, xml, t.end, "</si>") orelse xml.len;
        try list.append(a, try textRuns(a, xml[t.end..end]));
        p = end;
    }
    return list.items;
}

/// Concatenate every <t> inside `body` (rich text has one per run), skipping
/// phonetic <rPh> runs.
fn textRuns(a: Allocator, body: []const u8) ![]const u8 {
    var out = std.ArrayList(u8){};
    var p: usize = 0;
    while (nextTag(body, p, "t")) |t| {
        if (t.self_closing) {
            p = t.end;
            continue;
        }
        const end = std.mem.indexOfPos(u8, body, t.end, "</t>") orelse body.len;
        const in_phonetic = blk: {
            const rp = std.mem.lastIndexOf(u8, body[0..t.start], "<rPh") orelse break :blk false;
            break :blk (std.mem.indexOfPos(u8, body, rp, "</rPh>") orelse body.len) > t.start;
        };
        if (!in_phonetic) try out.appendSlice(a, try unescape(a, body[t.end..end]));
        p = end;
    }
    return out.items;
}

// --- tiny XML helpers -----------------------------------------------------------

const Tag = struct { start: usize, end: usize, attrs: []const u8, self_closing: bool };

/// Next `<name ...>` or `<name/>` at or after `from` (exact name, not a prefix).
fn nextTag(xml: []const u8, from: usize, name: []const u8) ?Tag {
    var p = from;
    while (std.mem.indexOfScalarPos(u8, xml, p, '<')) |lt| {
        p = lt + 1;
        if (lt + 1 + name.len > xml.len) return null;
        if (!std.mem.eql(u8, xml[lt + 1 .. lt + 1 + name.len], name)) continue;
        const after = lt + 1 + name.len;
        if (after < xml.len and !(xml[after] == '>' or xml[after] == '/' or std.ascii.isWhitespace(xml[after]))) continue;
        const gt = std.mem.indexOfScalarPos(u8, xml, after, '>') orelse return null;
        const self_closing = gt > after and xml[gt - 1] == '/';
        return .{ .start = lt, .end = gt + 1, .attrs = xml[after..if (self_closing) gt - 1 else gt], .self_closing = self_closing };
    }
    return null;
}

/// Value of ` name="..."` in an attribute string (raw, still escaped).
fn attr(attrs: []const u8, name: []const u8) ?[]const u8 {
    var p: usize = 0;
    while (std.mem.indexOfPos(u8, attrs, p, name)) |i| {
        p = i + 1;
        const before_ok = i > 0 and std.ascii.isWhitespace(attrs[i - 1]);
        const eq = i + name.len;
        if (!before_ok or eq + 1 >= attrs.len or attrs[eq] != '=') continue;
        const q = attrs[eq + 1];
        if (q != '"' and q != '\'') continue;
        const close = std.mem.indexOfScalarPos(u8, attrs, eq + 2, q) orelse return null;
        return attrs[eq + 2 .. close];
    }
    return null;
}

fn innerText(body: []const u8, name: []const u8) ?[]const u8 {
    const t = nextTag(body, 0, name) orelse return null;
    if (t.self_closing) return "";
    var close_buf: [16]u8 = undefined;
    const close = std.fmt.bufPrint(&close_buf, "</{s}>", .{name}) catch return null;
    const end = std.mem.indexOfPos(u8, body, t.end, close) orelse return null;
    return body[t.end..end];
}

fn sectionBody(xml: []const u8, name: []const u8) ?[]const u8 {
    return innerText(xml, name);
}

fn unescape(a: Allocator, s: []const u8) ![]const u8 {
    if (std.mem.indexOfScalar(u8, s, '&') == null) return s;
    var out = std.ArrayList(u8){};
    var i: usize = 0;
    while (i < s.len) {
        if (s[i] != '&') {
            try out.append(a, s[i]);
            i += 1;
            continue;
        }
        const semi = std.mem.indexOfScalarPos(u8, s, i, ';') orelse {
            try out.appendSlice(a, s[i..]);
            break;
        };
        const ent = s[i + 1 .. semi];
        if (std.mem.eql(u8, ent, "amp")) try out.append(a, '&') else if (std.mem.eql(u8, ent, "lt")) try out.append(a, '<') else if (std.mem.eql(u8, ent, "gt")) try out.append(a, '>') else if (std.mem.eql(u8, ent, "quot")) try out.append(a, '"') else if (std.mem.eql(u8, ent, "apos")) try out.append(a, '\'') else if (ent.len > 1 and ent[0] == '#') {
            const cp = (if (ent[1] == 'x' or ent[1] == 'X') std.fmt.parseInt(u21, ent[2..], 16) else std.fmt.parseInt(u21, ent[1..], 10)) catch 0xFFFD;
            var buf: [4]u8 = undefined;
            const n = std.unicode.utf8Encode(cp, &buf) catch std.unicode.utf8Encode(0xFFFD, &buf) catch unreachable;
            try out.appendSlice(a, buf[0..n]);
        } else try out.appendSlice(a, s[i .. semi + 1]);
        i = semi + 1;
    }
    return out.items;
}

/// "AB12" -> 27 (zero-based column of the cell reference).
fn columnIndex(ref: []const u8) usize {
    var n: usize = 0;
    for (ref) |ch| {
        if (!std.ascii.isAlphabetic(ch)) break;
        n = n * 26 + (std.ascii.toUpper(ch) - 'A' + 1);
    }
    return if (n == 0) 0 else n - 1;
}

fn writeCsvField(allocator: Allocator, out: *std.ArrayList(u8), v: []const u8) !void {
    if (std.mem.indexOfAny(u8, v, ",\"\r\n") == null) return out.appendSlice(allocator, v);
    try out.append(allocator, '"');
    for (v) |ch| {
        if (ch == '"') try out.append(allocator, '"');
        try out.append(allocator, ch);
    }
    try out.append(allocator, '"');
}

// --- zip ------------------------------------------------------------------------

/// Decompressed bytes of the zip entry named `name`, or null if absent.
fn readPart(a: Allocator, zip: []const u8, name: []const u8) !?[]const u8 {
    // End of central directory record: last "PK\x05\x06" within 64 KiB + 22 of the end.
    // (std.zip.EndRecord.findBuffer does not compile on 0.15.2.)
    if (zip.len < 22) return error.XlsxNotAZip;
    const floor = zip.len -| (0xFFFF + 22);
    var e: usize = zip.len - 22;
    while (!std.mem.eql(u8, zip[e..][0..4], "PK\x05\x06")) : (e -= 1) {
        if (e == floor) return error.XlsxNotAZip;
    }
    const record_count = std.mem.readInt(u16, zip[e + 10 ..][0..2], .little);
    var off: usize = std.mem.readInt(u32, zip[e + 16 ..][0..4], .little);
    var n: usize = 0;
    while (n < record_count) : (n += 1) {
        if (off + 46 > zip.len or !std.mem.eql(u8, zip[off..][0..4], &std.zip.central_file_header_sig)) return error.XlsxBadZip;
        const method = std.mem.readInt(u16, zip[off + 10 ..][0..2], .little);
        const comp_size = std.mem.readInt(u32, zip[off + 20 ..][0..4], .little);
        const uncomp_size = std.mem.readInt(u32, zip[off + 24 ..][0..4], .little);
        const fn_len = std.mem.readInt(u16, zip[off + 28 ..][0..2], .little);
        const extra_len = std.mem.readInt(u16, zip[off + 30 ..][0..2], .little);
        const comment_len = std.mem.readInt(u16, zip[off + 32 ..][0..2], .little);
        const local_off = std.mem.readInt(u32, zip[off + 42 ..][0..4], .little);
        if (off + 46 + fn_len > zip.len) return error.XlsxBadZip;
        const entry_name = zip[off + 46 ..][0..fn_len];
        off += 46 + fn_len + extra_len + comment_len;
        if (!std.mem.eql(u8, entry_name, name)) continue;

        if (comp_size == 0xFFFFFFFF or local_off == 0xFFFFFFFF) return error.XlsxZip64Unsupported;
        if (local_off + 30 > zip.len or !std.mem.eql(u8, zip[local_off..][0..4], &std.zip.local_file_header_sig)) return error.XlsxBadZip;
        const lfn = std.mem.readInt(u16, zip[local_off + 26 ..][0..2], .little);
        const lex = std.mem.readInt(u16, zip[local_off + 28 ..][0..2], .little);
        const start = @as(usize, local_off) + 30 + lfn + lex;
        if (start + comp_size > zip.len) return error.XlsxBadZip;
        const comp = zip[start .. start + comp_size];
        switch (method) {
            0 => return comp,
            8 => {
                var in: std.Io.Reader = .fixed(comp);
                var window: [std.compress.flate.max_window_len]u8 = undefined;
                var d = std.compress.flate.Decompress.init(&in, .raw, &window);
                var out = std.Io.Writer.Allocating.init(a);
                if (uncomp_size > 0) try out.ensureTotalCapacity(@min(uncomp_size, max_part_bytes));
                _ = d.reader.streamRemaining(&out.writer) catch return error.XlsxBadZip;
                if (out.written().len > max_part_bytes) return error.XlsxTooLarge;
                return out.written();
            },
            else => return error.XlsxUnsupportedCompression,
        }
    }
    return null;
}

// --- tests ----------------------------------------------------------------------

test "columnIndex" {
    try std.testing.expectEqual(@as(usize, 0), columnIndex("A1"));
    try std.testing.expectEqual(@as(usize, 25), columnIndex("Z9"));
    try std.testing.expectEqual(@as(usize, 27), columnIndex("AB12"));
}

test "date serials and format codes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try std.testing.expectEqualStrings("2024-03-15", try formatSerial(a, 45366, .date));
    try std.testing.expectEqualStrings("2024-03-15 12:00:00", try formatSerial(a, 45366.5, .datetime));
    try std.testing.expectEqualStrings("06:00:00", try formatSerial(a, 0.25, .time));
    try std.testing.expectEqual(DateKind.date, formatCodeKind("yyyy\\-mm\\-dd"));
    try std.testing.expectEqual(DateKind.datetime, formatCodeKind("m/d/yy h:mm"));
    try std.testing.expectEqual(DateKind.none, formatCodeKind("#,##0.00\"days\""));
    try std.testing.expectEqual(DateKind.none, formatCodeKind("0.00%"));
}

test "cells, shared strings, gaps and escaping" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const strings = [_][]const u8{ "name", "a, \"b\"" };
    const none = [_]DateKind{};
    try std.testing.expectEqualStrings("name", try cellValue(a, " r=\"A1\" t=\"s\"", "<v>0</v>", &strings, &none));
    try std.testing.expectEqualStrings("x & y", try cellValue(a, " t=\"inlineStr\"", "<is><t>x &amp; y</t></is>", &strings, &none));
    try std.testing.expectEqualStrings("true", try cellValue(a, " t=\"b\"", "<v>1</v>", &strings, &none));
    try std.testing.expectEqualStrings("3.5", try cellValue(a, "", "<f>A1*2</f><v>3.5</v>", &strings, &none));
    var out = std.ArrayList(u8){};
    try writeCsvField(a, &out, strings[1]);
    try std.testing.expectEqualStrings("\"a, \"\"b\"\"\"", out.items);
}

test "openpyxl workbook: types, quoting, dates, gaps, first sheet in tab order" {
    const allocator = std.testing.allocator;
    const out = try toCsv(allocator, @embedFile("testdata/sales.xlsx"));
    defer allocator.free(out);
    try std.testing.expectEqualStrings(
        "id,customer,region,amount,order_date,paid,note\n" ++
            "1,\"Acme, Inc.\",West,1200.5,2024-03-15,true,\"said \"\"hi\"\"\"\n" ++
            "2,Bolt,East,300,2024-04-01 09:30:00,false,\n" ++
            "3,Café Ünïcode,West,,2023-12-31,true,\"line1\nline2\"\n" ++
            "4,Gap Row,North,99,,,\n",
        out,
    );
    const first = try toCsv(allocator, @embedFile("testdata/sales_other_first.xlsx"));
    defer allocator.free(first);
    try std.testing.expectEqualStrings("x\n1\n", first);
    try std.testing.expectError(error.XlsxNotAZip, toCsv(allocator, "not a zip file at all, no"));
}
