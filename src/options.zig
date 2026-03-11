/// Output format for query results.
pub const OutputFormat = enum {
    /// Comma-separated values (default).
    csv,
    /// JSON array of objects: [{"col":"val",...},...]
    json,
    /// Newline-delimited JSON (one object per line).
    jsonl,
};

/// Runtime options parsed from CLI flags.
pub const Options = struct {
    /// Suppress the header row in output.
    no_header: bool = false,
    /// Field delimiter byte (default: comma).
    delimiter: u8 = ',',
    /// Output format (default: csv).
    format: OutputFormat = .csv,
};
