// Shell completion drift tests (#73): every flag listed in the OPTIONS block of
// `csvql --help` (the `help_text` literal in src/main.zig) must appear in a
// completion spec line of both completions/csvql.bash and completions/_csvql,
// so adding a flag without updating the completions fails here. Comments do
// not count (#259).
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

/// True when `line` is part of a completion spec rather than a comment or
/// shell plumbing: in bash the word lists passed to `compgen -W`, in zsh the
/// quoted `_arguments` specs and the `compadd` for `install --print`.
fn isSpecLine(line: []const u8) bool {
    const trimmed = std.mem.trim(u8, line, " \t\r");
    if (trimmed.len == 0 or trimmed[0] == '#') return false;
    if (std.mem.indexOf(u8, trimmed, "compgen -W") != null) return true;
    return trimmed[0] == '\'' or std.mem.startsWith(u8, trimmed, "compadd ");
}

/// The part of a spec line that names options. For a zsh `_arguments` spec
/// such as `'(--json --jsonl)--markdown[text]'` that is `--markdown`: the
/// leading exclusion group and the `[description]` are dropped, so a flag
/// listed only in another option's exclusion group does not count.
fn specNames(line: []const u8) []const u8 {
    var rest = std.mem.trim(u8, line, " \t\r");
    if (rest.len == 0 or rest[0] != '\'') return rest; // bash word list, compadd
    rest = rest[1..];
    if (rest.len > 0 and rest[0] == '(') {
        const close = std.mem.indexOfScalar(u8, rest, ')') orelse return rest;
        rest = std.mem.trimLeft(u8, rest[close + 1 ..], "'");
    }
    return rest[0 .. std.mem.indexOfScalar(u8, rest, '[') orelse rest.len];
}

/// True when `word` occurs as a whole token in the option names of a spec
/// line of `script`.
fn specContainsWord(script: []const u8, word: []const u8) bool {
    var lines = std.mem.splitScalar(u8, script, '\n');
    while (lines.next()) |line| {
        if (isSpecLine(line) and containsWord(specNames(line), word)) return true;
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
        if (!specContainsWord(bash_completion, flag)) {
            std.debug.print("completions/csvql.bash is missing {s}\n", .{flag});
            self.missing += 1;
        }
        if (!specContainsWord(zsh_completion, flag)) {
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
        try std.testing.expect(specContainsWord(bash_completion, word));
        try std.testing.expect(specContainsWord(zsh_completion, word));
    }
}

test "containsWord matches whole flags only" {
    try std.testing.expect(containsWord("--json --jsonl", "--json"));
    try std.testing.expect(!containsWord("--jsonl", "--json"));
    try std.testing.expect(containsWord("{-h,--help}'[x]'", "--help"));
    try std.testing.expect(!containsWord("--no-header", "--header"));
}

test "specContainsWord ignores flags mentioned only in comments" {
    const bash =
        \\# keep --strict in sync with csvql --help
        \\    COMPREPLY=( $(compgen -W '--json --jsonl' -- "$cur") )
    ;
    try std.testing.expect(specContainsWord(bash, "--json"));
    try std.testing.expect(!specContainsWord(bash, "--strict"));

    const zsh =
        \\# --strict is documented in csvql --help
        \\    compadd -- --print
        \\    '(--markdown)--json[output a JSON array of objects]' \\
    ;
    try std.testing.expect(specContainsWord(zsh, "--json"));
    try std.testing.expect(specContainsWord(zsh, "--print"));
    try std.testing.expect(!specContainsWord(zsh, "--strict"));
}

test "specContainsWord ignores flags listed only in exclusion groups or descriptions" {
    const zsh =
        \\    '(--markdown)--json[output a JSON array, see --wrap]' \\
        \\    '(-o --output)'{-o,--output}'[write results to a file]:output file:_files' \\
        \\    '1:file or subcommand: _alternative "subcommands:subcommand:(install)"' \\
    ;
    try std.testing.expect(specContainsWord(zsh, "--json"));
    try std.testing.expect(specContainsWord(zsh, "-o"));
    try std.testing.expect(specContainsWord(zsh, "--output"));
    try std.testing.expect(specContainsWord(zsh, "install"));
    try std.testing.expect(!specContainsWord(zsh, "--markdown"));
    try std.testing.expect(!specContainsWord(zsh, "--wrap"));
}
