const std = @import("std");

/// Represents a parsed date/time
pub const DateTime = struct {
    year: i32,
    month: u8, // 1-12
    day: u8, // 1-31
    hour: u8 = 0, // 0-23
    minute: u8 = 0, // 0-59
    second: u8 = 0, // 0-59
    millisecond: u16 = 0, // 0-999

    /// Convert DateTime to Unix timestamp (seconds since 1970-01-01 00:00:00 UTC)
    pub fn toTimestamp(self: DateTime) i64 {
        // Days since epoch calculation
        var days: i64 = 0;

        // Add years (accounting for leap years, handles pre-1970 dates)
        if (self.year >= 1970) {
            var year: i32 = 1970;
            while (year < self.year) : (year += 1) {
                days += if (isLeapYear(year)) 366 else 365;
            }
        } else {
            var year: i32 = 1969;
            while (year >= self.year) : (year -= 1) {
                days -= if (isLeapYear(year)) 366 else 365;
            }
        }

        // Add months
        const days_in_month = [_]u8{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
        var month: u8 = 1;
        while (month < self.month) : (month += 1) {
            days += days_in_month[month - 1];
            if (month == 2 and isLeapYear(self.year)) {
                days += 1; // Leap year February
            }
        }

        // Add days
        days += self.day - 1;

        // Convert to seconds and add time components
        return days * 86400 + @as(i64, self.hour) * 3600 + @as(i64, self.minute) * 60 + @as(i64, self.second);
    }

    /// Create DateTime from Unix timestamp
    pub fn fromTimestamp(timestamp: i64) DateTime {
        var remaining_seconds = timestamp;

        // Calculate days since epoch
        const days_since_epoch = @divFloor(remaining_seconds, 86400);
        remaining_seconds = @mod(remaining_seconds, 86400);

        // Calculate time components
        const hour = @as(u8, @intCast(@divFloor(remaining_seconds, 3600)));
        remaining_seconds = @mod(remaining_seconds, 3600);
        const minute = @as(u8, @intCast(@divFloor(remaining_seconds, 60)));
        const second = @as(u8, @intCast(@mod(remaining_seconds, 60)));

        // Calculate date from days (handles negative/pre-epoch timestamps)
        var year: i32 = 1970;
        var remaining_days = days_since_epoch;

        if (remaining_days >= 0) {
            while (true) {
                const days_in_year: i64 = if (isLeapYear(year)) 366 else 365;
                if (remaining_days < days_in_year) break;
                remaining_days -= days_in_year;
                year += 1;
            }
        } else {
            year = 1969;
            while (true) {
                const days_in_year: i64 = if (isLeapYear(year)) 366 else 365;
                remaining_days += days_in_year;
                if (remaining_days >= 0) break;
                year -= 1;
            }
        }

        // Find month and day
        const days_in_month = [_]u8{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
        var month: u8 = 1;
        var day_in_month: i64 = remaining_days;

        for (days_in_month) |days| {
            var month_days = @as(i64, days);
            if (month == 2 and isLeapYear(year)) {
                month_days += 1;
            }
            if (day_in_month < month_days) break;
            day_in_month -= month_days;
            month += 1;
        }

        return DateTime{
            .year = year,
            .month = month,
            .day = @as(u8, @intCast(day_in_month + 1)),
            .hour = hour,
            .minute = minute,
            .second = second,
        };
    }
};

fn isLeapYear(year: i32) bool {
    return (@rem(year, 4) == 0 and @rem(year, 100) != 0) or (@rem(year, 400) == 0);
}

fn daysInMonth(month: u8, year: i32) u8 {
    return switch (month) {
        1, 3, 5, 7, 8, 10, 12 => 31,
        4, 6, 9, 11 => 30,
        2 => if (isLeapYear(year)) @as(u8, 29) else @as(u8, 28),
        else => 0,
    };
}

/// Parse a date/datetime string in various formats, auto-detecting the format
/// Returns Unix timestamp (seconds since epoch)
pub fn parseDateTime(s: []const u8) !i64 {
    if (s.len == 0) return error.InvalidDate;

    // Try different formats based on separators
    if (std.mem.indexOf(u8, s, "-") != null) {
        // ISO-8601 format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS
        return parseISO(s);
    } else if (std.mem.indexOf(u8, s, "/") != null) {
        // US format: MM/DD/YYYY HH:MM:SS
        return parseSlashDate(s);
    } else if (std.mem.indexOf(u8, s, ".") != null) {
        // EU/Windows format: DD.MM.YYYY HH:MM:SS
        return parseDotDate(s);
    } else if (s.len >= 8) {
        // Try compact format: YYYYMMDD
        var all_digits = true;
        for (s[0..8]) |c| {
            if (!std.ascii.isDigit(c)) {
                all_digits = false;
                break;
            }
        }
        if (all_digits) {
            return parseCompact(s);
        }
    }

    return error.UnknownDateFormat;
}

/// Parse ISO-8601 format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS
fn parseISO(s: []const u8) !i64 {
    // Find date/time separator (T or space); explicit ?usize so null propagates correctly
    const time_sep_idx: ?usize = std.mem.indexOf(u8, s, "T") orelse std.mem.indexOf(u8, s, " ");

    const date_part = if (time_sep_idx) |idx| s[0..idx] else s;
    const time_part = if (time_sep_idx) |idx|
        if (idx + 1 < s.len) s[idx + 1 ..] else null
    else
        null;

    // Parse date: YYYY-MM-DD
    var it = std.mem.splitScalar(u8, date_part, '-');
    const year_str = it.next() orelse return error.InvalidDate;
    const month_str = it.next() orelse return error.InvalidDate;
    const day_str = it.next() orelse return error.InvalidDate;

    const year = try std.fmt.parseInt(i32, year_str, 10);
    const month = try std.fmt.parseInt(u8, month_str, 10);
    const day = try std.fmt.parseInt(u8, day_str, 10);

    if (month < 1 or month > 12) return error.InvalidMonth;
    if (day < 1 or day > daysInMonth(month, year)) return error.InvalidDay;

    var dt = DateTime{
        .year = year,
        .month = month,
        .day = day,
    };

    // Parse time if present: HH:MM:SS or HH:MM:SS.SSS
    if (time_part) |tp| {
        const time_clean = if (std.mem.indexOf(u8, tp, "Z")) |idx| tp[0..idx] else tp;

        var time_it = std.mem.splitScalar(u8, time_clean, ':');
        const hour_str = time_it.next() orelse return error.InvalidTime;
        const minute_str = time_it.next() orelse return error.InvalidTime;

        dt.hour = try std.fmt.parseInt(u8, hour_str, 10);
        dt.minute = try std.fmt.parseInt(u8, minute_str, 10);

        if (time_it.next()) |second_part| {
            // May contain milliseconds: SS.SSS
            if (std.mem.indexOf(u8, second_part, ".")) |dot_idx| {
                dt.second = try std.fmt.parseInt(u8, second_part[0..dot_idx], 10);
                if (dot_idx + 1 < second_part.len) {
                    dt.millisecond = try std.fmt.parseInt(u16, second_part[dot_idx + 1 ..], 10);
                }
            } else {
                dt.second = try std.fmt.parseInt(u8, second_part, 10);
            }
        }

        if (dt.hour > 23) return error.InvalidHour;
        if (dt.minute > 59) return error.InvalidMinute;
        if (dt.second > 59) return error.InvalidSecond;
    }

    return dt.toTimestamp();
}

/// Parse US format: MM/DD/YYYY or MM/DD/YYYY HH:MM:SS
fn parseSlashDate(s: []const u8) !i64 {
    const space_idx = std.mem.indexOf(u8, s, " ");
    const date_part = if (space_idx) |idx| s[0..idx] else s;
    const time_part = if (space_idx) |idx|
        if (idx + 1 < s.len) s[idx + 1 ..] else null
    else
        null;

    // Parse date: MM/DD/YYYY
    var it = std.mem.splitScalar(u8, date_part, '/');
    const month_str = it.next() orelse return error.InvalidDate;
    const day_str = it.next() orelse return error.InvalidDate;
    const year_str = it.next() orelse return error.InvalidDate;

    const month = try std.fmt.parseInt(u8, month_str, 10);
    const day = try std.fmt.parseInt(u8, day_str, 10);
    const year = try std.fmt.parseInt(i32, year_str, 10);

    if (month < 1 or month > 12) return error.InvalidMonth;
    if (day < 1 or day > daysInMonth(month, year)) return error.InvalidDay;

    var dt = DateTime{
        .year = year,
        .month = month,
        .day = day,
    };

    // Parse time if present
    if (time_part) |tp| {
        var time_it = std.mem.splitScalar(u8, tp, ':');
        const hour_str = time_it.next() orelse return error.InvalidTime;
        const minute_str = time_it.next() orelse return error.InvalidTime;

        dt.hour = try std.fmt.parseInt(u8, hour_str, 10);
        dt.minute = try std.fmt.parseInt(u8, minute_str, 10);

        if (time_it.next()) |second_str| {
            dt.second = try std.fmt.parseInt(u8, second_str, 10);
        }

        if (dt.hour > 23) return error.InvalidHour;
        if (dt.minute > 59) return error.InvalidMinute;
        if (dt.second > 59) return error.InvalidSecond;
    }

    return dt.toTimestamp();
}

/// Parse EU/Windows format: DD.MM.YYYY or DD.MM.YYYY HH:MM:SS
fn parseDotDate(s: []const u8) !i64 {
    const space_idx = std.mem.indexOf(u8, s, " ");
    const date_part = if (space_idx) |idx| s[0..idx] else s;
    const time_part = if (space_idx) |idx|
        if (idx + 1 < s.len) s[idx + 1 ..] else null
    else
        null;

    // Parse date: DD.MM.YYYY
    var it = std.mem.splitScalar(u8, date_part, '.');
    const day_str = it.next() orelse return error.InvalidDate;
    const month_str = it.next() orelse return error.InvalidDate;
    const year_str = it.next() orelse return error.InvalidDate;

    const day = try std.fmt.parseInt(u8, day_str, 10);
    const month = try std.fmt.parseInt(u8, month_str, 10);
    const year = try std.fmt.parseInt(i32, year_str, 10);

    if (month < 1 or month > 12) return error.InvalidMonth;
    if (day < 1 or day > daysInMonth(month, year)) return error.InvalidDay;

    var dt = DateTime{
        .year = year,
        .month = month,
        .day = day,
    };

    // Parse time if present
    if (time_part) |tp| {
        var time_it = std.mem.splitScalar(u8, tp, ':');
        const hour_str = time_it.next() orelse return error.InvalidTime;
        const minute_str = time_it.next() orelse return error.InvalidTime;

        dt.hour = try std.fmt.parseInt(u8, hour_str, 10);
        dt.minute = try std.fmt.parseInt(u8, minute_str, 10);

        if (time_it.next()) |second_str| {
            dt.second = try std.fmt.parseInt(u8, second_str, 10);
        }

        if (dt.hour > 23) return error.InvalidHour;
        if (dt.minute > 59) return error.InvalidMinute;
        if (dt.second > 59) return error.InvalidSecond;
    }

    return dt.toTimestamp();
}

/// Parse compact format: YYYYMMDD or YYYYMMDDHHMMSS
fn parseCompact(s: []const u8) !i64 {
    if (s.len < 8) return error.InvalidDate;

    const year = try std.fmt.parseInt(i32, s[0..4], 10);
    const month = try std.fmt.parseInt(u8, s[4..6], 10);
    const day = try std.fmt.parseInt(u8, s[6..8], 10);

    if (month < 1 or month > 12) return error.InvalidMonth;
    if (day < 1 or day > daysInMonth(month, year)) return error.InvalidDay;

    var dt = DateTime{
        .year = year,
        .month = month,
        .day = day,
    };

    // Parse time if present: HHMMSS
    if (s.len >= 14) {
        dt.hour = try std.fmt.parseInt(u8, s[8..10], 10);
        dt.minute = try std.fmt.parseInt(u8, s[10..12], 10);
        dt.second = try std.fmt.parseInt(u8, s[12..14], 10);

        if (dt.hour > 23) return error.InvalidHour;
        if (dt.minute > 59) return error.InvalidMinute;
        if (dt.second > 59) return error.InvalidSecond;
    }

    return dt.toTimestamp();
}

/// Format a Unix timestamp as ISO-8601: YYYY-MM-DD HH:MM:SS
pub fn formatDateTime(allocator: std.mem.Allocator, timestamp: i64) ![]u8 {
    const dt = DateTime.fromTimestamp(timestamp);
    return std.fmt.allocPrint(allocator, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{
        @as(u32, @intCast(if (dt.year >= 0) dt.year else -dt.year)),
        dt.month,
        dt.day,
        dt.hour,
        dt.minute,
        dt.second,
    });
}

/// Apply a strftime format to an ISO-8601 datetime string (YYYY-MM-DD HH:MM:SS).
/// Writes the result into `buf` (64 bytes) and returns the filled slice.
/// Supported specifiers: %Y %m %d %H %M %S; all other characters are copied literally.
/// Apply a strftime format to a datetime string. Parses `date_str` through
/// `parseDateTime` first (so ISO-8601, US MM/DD/YYYY, EU DD.MM.YYYY, and
/// compact YYYYMMDD are all handled correctly, matching DATEDIFF/DATEADD/
/// EXTRACT's auto-detection) and formats from the resulting DateTime's
/// fields. Falls back to the old fixed-offset ISO-8601 byte slicing only
/// when the string doesn't parse as any recognized date format — garbage
/// input stays garbage-in-garbage-out, but every real supported format now
/// gets its actual year/month/day, not just ones that happen to be
/// ISO-8601-shaped (#140: STRFTIME on a US/EU-formatted date silently
/// returned bytes sliced from the wrong offsets, e.g. "01/20/2026" gave day
/// "26" instead of "20").
pub fn applyStrftime(fmt: []const u8, date_str: []const u8, buf: *[64]u8) []const u8 {
    if (parseDateTime(date_str)) |ts| {
        return applyStrftimeParsed(fmt, DateTime.fromTimestamp(ts), buf);
    } else |_| {
        return applyStrftimeRawFallback(fmt, date_str, buf);
    }
}

fn applyStrftimeParsed(fmt: []const u8, dt: DateTime, buf: *[64]u8) []const u8 {
    var pos: usize = 0;
    var fi: usize = 0;
    while (fi < fmt.len and pos + 4 <= buf.len) : (fi += 1) {
        if (fmt[fi] == '%' and fi + 1 < fmt.len) {
            fi += 1;
            const written = switch (fmt[fi]) {
                'Y' => std.fmt.bufPrint(buf[pos..], "{d:0>4}", .{@as(u32, @intCast(if (dt.year >= 0) dt.year else -dt.year))}) catch "",
                'm' => std.fmt.bufPrint(buf[pos..], "{d:0>2}", .{dt.month}) catch "",
                'd' => std.fmt.bufPrint(buf[pos..], "{d:0>2}", .{dt.day}) catch "",
                'H' => std.fmt.bufPrint(buf[pos..], "{d:0>2}", .{dt.hour}) catch "",
                'M' => std.fmt.bufPrint(buf[pos..], "{d:0>2}", .{dt.minute}) catch "",
                'S' => std.fmt.bufPrint(buf[pos..], "{d:0>2}", .{dt.second}) catch "",
                else => blk: {
                    if (pos + 2 > buf.len) break :blk "";
                    buf[pos] = '%';
                    buf[pos + 1] = fmt[fi];
                    break :blk buf[pos .. pos + 2];
                },
            };
            pos += written.len;
        } else {
            buf[pos] = fmt[fi];
            pos += 1;
        }
    }
    return buf[0..pos];
}

/// Original fixed-offset ISO-8601 (YYYY-MM-DD HH:MM:SS) byte slicing —
/// used only as a fallback when `date_str` doesn't parse as any recognized
/// date format at all.
fn applyStrftimeRawFallback(fmt: []const u8, date_str: []const u8, buf: *[64]u8) []const u8 {
    var pos: usize = 0;
    var fi: usize = 0;
    while (fi < fmt.len and pos + 4 <= buf.len) : (fi += 1) {
        if (fmt[fi] == '%' and fi + 1 < fmt.len) {
            fi += 1;
            switch (fmt[fi]) {
                'Y' => if (date_str.len >= 4) {
                    @memcpy(buf[pos..][0..4], date_str[0..4]);
                    pos += 4;
                },
                'm' => if (date_str.len >= 7) {
                    @memcpy(buf[pos..][0..2], date_str[5..7]);
                    pos += 2;
                },
                'd' => if (date_str.len >= 10) {
                    @memcpy(buf[pos..][0..2], date_str[8..10]);
                    pos += 2;
                },
                'H' => if (date_str.len >= 13) {
                    @memcpy(buf[pos..][0..2], date_str[11..13]);
                    pos += 2;
                },
                'M' => if (date_str.len >= 16) {
                    @memcpy(buf[pos..][0..2], date_str[14..16]);
                    pos += 2;
                },
                'S' => if (date_str.len >= 19) {
                    @memcpy(buf[pos..][0..2], date_str[17..19]);
                    pos += 2;
                },
                else => {
                    buf[pos] = '%';
                    pos += 1;
                    buf[pos] = fmt[fi];
                    pos += 1;
                },
            }
        } else {
            buf[pos] = fmt[fi];
            pos += 1;
        }
    }
    return buf[0..pos];
}

/// Format a Unix timestamp as date only: YYYY-MM-DD
pub fn formatDate(allocator: std.mem.Allocator, timestamp: i64) ![]u8 {
    const dt = DateTime.fromTimestamp(timestamp);
    return std.fmt.allocPrint(allocator, "{d:0>4}-{d:0>2}-{d:0>2}", .{
        @as(u32, @intCast(if (dt.year >= 0) dt.year else -dt.year)),
        dt.month,
        dt.day,
    });
}

// Tests
test "parse ISO-8601 date" {
    const ts = try parseDateTime("2026-01-15");
    const dt = DateTime.fromTimestamp(ts);
    try std.testing.expectEqual(@as(i32, 2026), dt.year);
    try std.testing.expectEqual(@as(u8, 1), dt.month);
    try std.testing.expectEqual(@as(u8, 15), dt.day);
}

test "parse ISO-8601 datetime with space" {
    const ts = try parseDateTime("2026-01-15 09:30:00");
    const dt = DateTime.fromTimestamp(ts);
    try std.testing.expectEqual(@as(i32, 2026), dt.year);
    try std.testing.expectEqual(@as(u8, 1), dt.month);
    try std.testing.expectEqual(@as(u8, 15), dt.day);
    try std.testing.expectEqual(@as(u8, 9), dt.hour);
    try std.testing.expectEqual(@as(u8, 30), dt.minute);
    try std.testing.expectEqual(@as(u8, 0), dt.second);
}

test "parse ISO-8601 datetime with T" {
    const ts = try parseDateTime("2026-01-16T10:00:00");
    const dt = DateTime.fromTimestamp(ts);
    try std.testing.expectEqual(@as(i32, 2026), dt.year);
    try std.testing.expectEqual(@as(u8, 1), dt.month);
    try std.testing.expectEqual(@as(u8, 16), dt.day);
    try std.testing.expectEqual(@as(u8, 10), dt.hour);
    try std.testing.expectEqual(@as(u8, 0), dt.minute);
}

test "parse US format date" {
    const ts = try parseDateTime("01/15/2026");
    const dt = DateTime.fromTimestamp(ts);
    try std.testing.expectEqual(@as(i32, 2026), dt.year);
    try std.testing.expectEqual(@as(u8, 1), dt.month);
    try std.testing.expectEqual(@as(u8, 15), dt.day);
}

test "parse US format datetime" {
    const ts = try parseDateTime("01/15/2026 08:00:00");
    const dt = DateTime.fromTimestamp(ts);
    try std.testing.expectEqual(@as(i32, 2026), dt.year);
    try std.testing.expectEqual(@as(u8, 1), dt.month);
    try std.testing.expectEqual(@as(u8, 15), dt.day);
    try std.testing.expectEqual(@as(u8, 8), dt.hour);
}

test "parse EU/Windows format" {
    const ts = try parseDateTime("15.01.2026 07:30:00");
    const dt = DateTime.fromTimestamp(ts);
    try std.testing.expectEqual(@as(i32, 2026), dt.year);
    try std.testing.expectEqual(@as(u8, 1), dt.month);
    try std.testing.expectEqual(@as(u8, 15), dt.day);
    try std.testing.expectEqual(@as(u8, 7), dt.hour);
    try std.testing.expectEqual(@as(u8, 30), dt.minute);
}

test "parse compact format" {
    const ts = try parseDateTime("20260115");
    const dt = DateTime.fromTimestamp(ts);
    try std.testing.expectEqual(@as(i32, 2026), dt.year);
    try std.testing.expectEqual(@as(u8, 1), dt.month);
    try std.testing.expectEqual(@as(u8, 15), dt.day);
}

test "timestamp round-trip" {
    const original = DateTime{
        .year = 2026,
        .month = 1,
        .day = 15,
        .hour = 9,
        .minute = 30,
        .second = 45,
    };
    const ts = original.toTimestamp();
    const restored = DateTime.fromTimestamp(ts);
    try std.testing.expectEqual(original.year, restored.year);
    try std.testing.expectEqual(original.month, restored.month);
    try std.testing.expectEqual(original.day, restored.day);
    try std.testing.expectEqual(original.hour, restored.hour);
    try std.testing.expectEqual(original.minute, restored.minute);
    try std.testing.expectEqual(original.second, restored.second);
}

test "leap year detection" {
    try std.testing.expect(isLeapYear(2000)); // divisible by 400
    try std.testing.expect(isLeapYear(2024)); // divisible by 4, not by 100
    try std.testing.expect(!isLeapYear(1900)); // divisible by 100, not by 400
    try std.testing.expect(!isLeapYear(2023)); // not divisible by 4
}

test "format datetime" {
    const dt = DateTime{
        .year = 2026,
        .month = 1,
        .day = 15,
        .hour = 9,
        .minute = 30,
        .second = 0,
    };
    const ts = dt.toTimestamp();
    const formatted = try formatDateTime(std.testing.allocator, ts);
    defer std.testing.allocator.free(formatted);
    try std.testing.expectEqualStrings("2026-01-15 09:30:00", formatted);
}

test "applyStrftime extracts the correct fields from US and EU formatted dates, not just ISO-8601 (#140)" {
    var buf: [64]u8 = undefined;

    // ISO-8601 — always worked, byte offsets happened to line up.
    try std.testing.expectEqualStrings("15", applyStrftime("%d", "2026-01-15", &buf));
    try std.testing.expectEqualStrings("01", applyStrftime("%m", "2026-01-15", &buf));
    try std.testing.expectEqualStrings("2026", applyStrftime("%Y", "2026-01-15", &buf));

    // US MM/DD/YYYY — day 20, not the "26" the old byte-slicing produced.
    try std.testing.expectEqualStrings("20", applyStrftime("%d", "01/20/2026", &buf));
    try std.testing.expectEqualStrings("01", applyStrftime("%m", "01/20/2026", &buf));
    try std.testing.expectEqualStrings("2026", applyStrftime("%Y", "01/20/2026", &buf));

    // EU DD.MM.YYYY — day 25, not the "26" the old byte-slicing produced.
    try std.testing.expectEqualStrings("25", applyStrftime("%d", "25.01.2026", &buf));
    try std.testing.expectEqualStrings("01", applyStrftime("%m", "25.01.2026", &buf));
    try std.testing.expectEqualStrings("2026", applyStrftime("%Y", "25.01.2026", &buf));

    // Combined format string, EU date with a time component.
    try std.testing.expectEqualStrings("2026-01-25", applyStrftime("%Y-%m-%d", "25.01.2026 14:30:00", &buf));
}
