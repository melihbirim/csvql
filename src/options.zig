/// Runtime options parsed from CLI flags.
pub const Options = struct {
    /// Suppress the header row in output.
    no_header: bool = false,
    /// Field delimiter byte (default: comma).
    delimiter: u8 = ',',
};
