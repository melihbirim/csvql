/// Output format for query results.
pub const OutputFormat = enum {
    /// Comma-separated values (default).
    csv,
    /// JSON array of objects: [{"col":"val",...},...]
    json,
    /// Newline-delimited JSON (one object per line).
    jsonl,
};

/// Table rendering mode.
pub const TableMode = enum {
    /// Auto-detect: use table when stdout is a TTY and format is csv.
    auto,
    /// Always render as a table (--table flag).
    on,
    /// Never render as a table (--no-table flag).
    off,
};

/// Runtime options parsed from CLI flags.
pub const Options = struct {
    /// Suppress the header row in output.
    no_header: bool = false,
    /// Field delimiter byte (default: comma).
    delimiter: u8 = ',',
    /// Output format (default: csv).
    format: OutputFormat = .csv,
    /// Table rendering mode (default: auto TTY detection).
    table_mode: TableMode = .auto,
    /// Wrap cell content to multiple lines instead of truncating with '…'.
    wrap_cells: bool = false,
};
