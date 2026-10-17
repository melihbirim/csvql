// Shell completion drift tests (#73): every flag listed in the OPTIONS block of
// `csvql --help` (the `help_text` literal in src/main.zig) must appear in both
// completions/csvql.bash and completions/_csvql, so adding a flag without
// updating the completions fails here.
const std = @import("std");

const main_source = @embedFile("main_source");
const bash_completion = @embedFile("bash_completion");
const zsh_completion = @embedFile("zsh_completion");

fn isFlagChar(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '-' or c == '_';
}

/// True when `word` occurs in `text` as a whole token ("--json" must not match
/// inside "--jsonl").
fn containsWord(text: []const u8, word: []const u8) bool {
    var from: usize = 0;
    while (std.mem.indexOfPos(u8, text, from, word)) |at| : (from = at + 1) {
        const before_ok = at == 0 or !isFlagChar(text[at - 1]);
        const end = at + word.len;
        const after_ok = end == text.len or !isFlagChar(text[end]);
        if (before_ok and after_ok) return true;
    }
    return false;
}

/// Visit every flag in the OPTIONS block of the help text. Only the first
/// column of each entry is read, so flags mentioned in descriptions are skipped.
fn forEachHelpFlag(context: anytype, visit: fn (@TypeOf(context), []const u8) anyerror!void) !void {
    const start = std.mem.indexOf(u8, main_source, "\\\\OPTIONS:") orelse return error.OptionsBlockNotFound;
    const rest = main_source[start..];
    const end = std.mem.indexOf(u8, rest, "\\\\EXAMPLES:") orelse return error.OptionsBlockNotFound;
    var lines = std.mem.splitScalar(u8, rest[0..end], '\n');
    _ = lines.next(); // the OPTIONS: line
    while (lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, " \t\r"); // \r: CRLF checkouts (windows)
        // Entries look like `\\  -o, --output <file>  text`; continuation lines are indented deeper.
        if (!std.mem.startsWith(u8, raw, "    \\\\  -")) continue;
        const entry = std.mem.trim(u8, line["\\\\".len..], " ");
        const column = entry[0 .. std.mem.indexOf(u8, entry, "  ") orelse entry.len];
        var tokens = std.mem.tokenizeAny(u8, column, ", ");
        while (tokens.next()) |tok| {
            if (tok[0] == '-') try visit(context, tok);
        }
    }
}

const Missing = struct {
    seen: usize = 0,
    missing: usize = 0,

    fn check(self: *Missing, flag: []const u8) anyerror!void {
        self.seen += 1;
        if (!containsWord(bash_completion, flag)) {
            std.debug.print("completions/csvql.bash is missing {s}\n", .{flag});
            self.missing += 1;
        }
        if (!containsWord(zsh_completion, flag)) {
            std.debug.print("completions/_csvql is missing {s}\n", .{flag});
            self.missing += 1;
        }
    }
};

fn checkFlag(ctx: *Missing, flag: []const u8) anyerror!void {
    try ctx.check(flag);
}

test "completions cover every flag in csvql --help" {
    var ctx = Missing{};
    try forEachHelpFlag(&ctx, checkFlag);
    // Guards against the help parser silently matching nothing.
    try std.testing.expect(ctx.seen >= 20);
    try std.testing.expectEqual(@as(usize, 0), ctx.missing);
}

test "completions cover the install subcommand and its --print flag" {
    for ([_][]const u8{ "install", "--print" }) |word| {
        try std.testing.expect(containsWord(bash_completion, word));
        try std.testing.expect(containsWord(zsh_completion, word));
    }
}

test "containsWord matches whole flags only" {
    try std.testing.expect(containsWord("--json --jsonl", "--json"));
    try std.testing.expect(!containsWord("--jsonl", "--json"));
    try std.testing.expect(containsWord("{-h,--help}'[x]'", "--help"));
    try std.testing.expect(!containsWord("--no-header", "--header"));
}
