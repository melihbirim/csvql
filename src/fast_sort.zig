const std = @import("std");
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");

// ============================================================================
// Hardware-Aware Sort Strategy Selection
// ============================================================================

/// Sort strategy based on data size, LIMIT, and hardware
pub const SortStrategy = enum {
    /// Min-heap top-K: O(N log K) — best when K << N
    heap_topk,
    /// Radix sort on u64 keys: O(8N) — best for large N, numeric data
    radix,
    /// Fallback comparison sort: O(N log N) — for mixed/string-heavy data
    comparison,
};

/// Detect optimal L1 cache-friendly heap size based on architecture.
/// On ARM (M1/M2), L1 data cache is 128KB per performance core.
/// Each heap entry is ~32 bytes (u64 key + 2 slices), so ~4000 entries fit in L1.
/// We use a conservative threshold to leave room for other working data.
pub const HEAP_MAX_K: usize = if (builtin.cpu.arch == .aarch64)
    2048 // ARM M1/M2: 128KB L1, heap fits comfortably
else if (builtin.cpu.arch == .x86_64)
    1024 // x86: typically 32-48KB L1
else
    512; // Conservative default

/// Minimum dataset size where radix sort beats comparison sort.
/// Radix has fixed overhead (8 passes × counting arrays). Below this,
/// pdqsort (Zig's std.mem.sort) is faster due to cache locality.
pub const RADIX_MIN_N: usize = if (builtin.cpu.arch == .aarch64)
    8192 // ARM: lower threshold due to efficient sequential access
else
    16384; // x86: higher due to different cache behavior

/// Choose the optimal sort strategy
pub fn chooseStrategy(n: usize, limit: ?usize, all_numeric: bool) SortStrategy {
    const k = limit orelse n;

    // If K is small relative to N and fits in L1 cache, use heap
    if (k <= HEAP_MAX_K and k < n / 4) {
        return .heap_topk;
    }

    // For large numeric datasets, radix sort is O(N) with no comparisons
    if (all_numeric and n >= RADIX_MIN_N) {
        return .radix;
    }

    // Fallback: comparison sort (pdqsort)
    return .comparison;
}

// ============================================================================
// Generic Sort Entry — used by all engines via this module
// ============================================================================

/// A sortable entry with a u64 key for radix sort compatibility.
/// The u64 key is derived from either f64 (numeric) or first 8 bytes (string).
pub const SortKey = struct {
    /// Radix-sortable key (f64→u64 or string→u64)
    radix_key: u64,
    /// Original numeric key for comparison fallback
    numeric_key: f64,
    /// Original string key for tiebreaking
    sort_key: []const u8,
    /// Payload: the CSV line (arena or mmap slice)
    line: []const u8,
};

// ============================================================================
// Key Conversion: f64 → sortable u64, string → sortable u64
// ============================================================================

/// Convert f64 to a u64 that preserves sort order.
/// IEEE 754 trick: positive floats sort correctly as u64 after flipping sign bit.
/// Negative floats need all bits flipped.
pub fn f64ToSortableU64(val: f64) u64 {
    const bits: u64 = @bitCast(val);
    // Negative: flip all bits. Positive: flip only sign bit.
    const mask: u64 = if (bits >> 63 != 0) 0xFFFFFFFFFFFFFFFF else 0x8000000000000000;
    return bits ^ mask;
}

/// Inverse: convert sortable u64 back to f64 (for verification/debugging)
pub fn sortableU64ToF64(key: u64) f64 {
    const mask: u64 = if (key >> 63 != 0) 0x8000000000000000 else 0xFFFFFFFFFFFFFFFF;
    const bits = key ^ mask;
    return @bitCast(bits);
}

/// Convert the first 8 bytes of a string to a sortable u64 (big-endian).
/// Shorter strings are zero-padded on the right, giving correct lexicographic order.
pub fn stringToSortableU64(s: []const u8) u64 {
    var key: u64 = 0;
    const len = @min(s.len, 8);
    for (0..len) |i| {
        key |= @as(u64, s[i]) << @intCast((7 - i) * 8);
    }
    return key;
}

/// Build a SortKey from the pre-parsed numeric_key and raw sort string.
/// Automatically chooses numeric or string key encoding.
pub fn makeSortKey(numeric_key: f64, sort_key: []const u8, line: []const u8) SortKey {
    const is_numeric = !std.math.isNan(numeric_key);
    return SortKey{
        .radix_key = if (is_numeric) f64ToSortableU64(numeric_key) else stringToSortableU64(sort_key),
        .numeric_key = numeric_key,
        .sort_key = sort_key,
        .line = line,
    };
}

// ============================================================================
// Min-Heap Top-K: O(N log K) — for small LIMIT values
// ============================================================================

/// A max-heap of size K for finding the K smallest (ASC) or K largest (DESC) elements.
/// For ASC ORDER BY: we maintain a max-heap; any element smaller than the max gets swapped in.
/// For DESC ORDER BY: we maintain a min-heap; any element larger than the min gets swapped in.
pub const TopKHeap = struct {
    items: []SortKey,
    len: usize,
    capacity: usize,
    descending: bool,
    allocator: Allocator,

    pub fn init(allocator: Allocator, k: usize, descending: bool) !TopKHeap {
        const items = try allocator.alloc(SortKey, k);
        return TopKHeap{
            .items = items,
            .len = 0,
            .capacity = k,
            .descending = descending,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *TopKHeap) void {
        self.allocator.free(self.items);
    }

    /// Insert an element. If heap is full, only insert if better than current worst.
    pub fn insert(self: *TopKHeap, entry: SortKey) void {
        // k=0 (LIMIT 0): nothing to insert into a zero-capacity heap. Without
        // this guard the else branch below reads self.items[0] on an empty
        // slice — undefined behavior, segfaults under ReleaseFast (#111).
        if (self.capacity == 0) return;
        if (self.len < self.capacity) {
            self.items[self.len] = entry;
            self.len += 1;
            if (self.len == self.capacity) {
                // Build heap when full
                self.buildHeap();
            }
        } else {
            // Compare with root (worst element in our top-K)
            if (self.isBetter(entry, self.items[0])) {
                self.items[0] = entry;
                self.siftDown(0);
            }
        }
    }

    /// Is `a` "better" than `b` for our top-K?
    /// For ASC (we want smallest K): a is better if a < b
    /// For DESC (we want largest K): a is better if a > b
    /// The heap root is the WORST element (largest for ASC, smallest for DESC).
    fn isBetter(self: *const TopKHeap, a: SortKey, b: SortKey) bool {
        if (self.descending) {
            return a.radix_key > b.radix_key;
        } else {
            return a.radix_key < b.radix_key;
        }
    }

    /// Is `a` "worse" than `b`? (Used for heap property: parent is worst)
    fn isWorse(self: *const TopKHeap, a: SortKey, b: SortKey) bool {
        if (self.descending) {
            return a.radix_key < b.radix_key;
        } else {
            return a.radix_key > b.radix_key;
        }
    }

    fn buildHeap(self: *TopKHeap) void {
        if (self.len <= 1) return;
        var i: usize = self.len / 2;
        while (i > 0) {
            i -= 1;
            self.siftDown(i);
        }
    }

    fn siftDown(self: *TopKHeap, start: usize) void {
        var pos = start;
        while (true) {
            var worst = pos;
            const left = 2 * pos + 1;
            const right = 2 * pos + 2;

            if (left < self.len and self.isWorse(self.items[left], self.items[worst])) {
                worst = left;
            }
            if (right < self.len and self.isWorse(self.items[right], self.items[worst])) {
                worst = right;
            }
            if (worst == pos) break;

            const tmp = self.items[pos];
            self.items[pos] = self.items[worst];
            self.items[worst] = tmp;
            pos = worst;
        }
    }

    /// Extract sorted results (best to worst). Sorts the heap in-place.
    pub fn getSorted(self: *TopKHeap) []SortKey {
        const items = self.items[0..self.len];
        // Sort by radix_key with tiebreaking
        const Ctx = struct {
            desc: bool,
            pub fn lessThan(ctx: @This(), a: SortKey, b: SortKey) bool {
                if (ctx.desc) return a.radix_key > b.radix_key else return a.radix_key < b.radix_key;
            }
        };
        std.mem.sort(SortKey, items, Ctx{ .desc = self.descending }, Ctx.lessThan);
        return items;
    }
};

// ============================================================================
// Radix Sort: O(8N) — for large numeric datasets
// ============================================================================

/// LSD (Least Significant Digit) radix sort on u64 keys.
/// 8 passes, one per byte, 256 buckets per pass.
/// Sorts in ascending order; caller reverses for DESC.
/// Compact key+index pair for indirect radix sort (12 bytes vs 48-byte SortKey).
/// Sorting these instead of full SortKey structs gives ~4x less data movement.
const KeyIndex = struct {
    key: u64,
    idx: u32,
};

/// Indirect radix sort with pass-skipping and optional DESC support.
/// 1. Extracts (key, index) pairs — 12 bytes each instead of 48-byte SortKey
/// 2. Skips byte passes where all keys share the same byte value (common for
///    integer salary data where upper bytes are identical)
/// 3. For DESC: XOR keys before sorting to reverse order, avoiding a separate reverse pass
/// After sorting, gathers results back into the original items array.
pub fn radixSortU64(allocator: Allocator, items: []SortKey, descending: bool) !void {
    if (items.len <= 1) return;

    const n = items.len;

    // Build indirect key+index array (12 bytes each vs 48 bytes for full SortKey)
    const keys = try allocator.alloc(KeyIndex, n);
    defer allocator.free(keys);
    const temp = try allocator.alloc(KeyIndex, n);
    defer allocator.free(temp);

    // For DESC, XOR keys to flip sort order — ascending on flipped keys = descending on original
    const xor_mask: u64 = if (descending) 0xFFFFFFFFFFFFFFFF else 0;
    for (items, 0..) |entry, i| {
        keys[i] = .{ .key = entry.radix_key ^ xor_mask, .idx = @intCast(i) };
    }

    // Pre-scan: detect which byte positions actually vary across keys.
    // Skip passes where all keys have the same byte (e.g., upper bytes of small integers).
    var byte_varies: [8]bool = .{ false, false, false, false, false, false, false, false };
    var active_passes: usize = 0;
    {
        const first_key = keys[0].key;
        for (0..8) |byte_idx| {
            const shift: u6 = @intCast(byte_idx * 8);
            const ref_byte: u8 = @truncate(first_key >> shift);
            for (keys[1..]) |ki| {
                const b: u8 = @truncate(ki.key >> shift);
                if (b != ref_byte) {
                    byte_varies[byte_idx] = true;
                    active_passes += 1;
                    break;
                }
            }
        }
    }

    // If no bytes vary, all keys are identical — already sorted
    if (active_passes == 0) return;

    var src = keys;
    var dst = temp;
    var passes_done: usize = 0;

    for (0..8) |byte_idx| {
        if (!byte_varies[byte_idx]) continue;

        const shift: u6 = @intCast(byte_idx * 8);

        // Count occurrences of each byte value
        var counts: [256]usize = [_]usize{0} ** 256;
        for (src) |ki| {
            const byte_val: u8 = @truncate(ki.key >> shift);
            counts[byte_val] += 1;
        }

        // Prefix sum → output positions
        var total: usize = 0;
        for (&counts) |*c| {
            const count = c.*;
            c.* = total;
            total += count;
        }

        // Scatter to destination
        for (src) |ki| {
            const byte_val: u8 = @truncate(ki.key >> shift);
            dst[counts[byte_val]] = ki;
            counts[byte_val] += 1;
        }

        // Swap src and dst for next pass
        const tmp = src;
        src = dst;
        dst = tmp;
        passes_done += 1;
    }

    // Gather: reorder items in-place using the sorted indices.
    // We need a temporary copy of the original items for gathering.
    const items_copy = try allocator.alloc(SortKey, n);
    defer allocator.free(items_copy);
    @memcpy(items_copy, items);

    for (src, 0..) |ki, i| {
        items[i] = items_copy[ki.idx];
    }
}

// ============================================================================
// Unified Sort Interface — used by all engines
// ============================================================================

/// Sort entries using the optimal strategy. Returns sorted slice ready for output.
/// For heap_topk: only the top K entries are returned (already sorted).
/// For radix/comparison: all entries are sorted; caller applies LIMIT during output.
pub fn sortEntries(
    allocator: Allocator,
    items: []SortKey,
    descending: bool,
    limit: ?usize,
) ![]SortKey {
    const all_numeric = blk: {
        for (items) |entry| {
            if (std.math.isNan(entry.numeric_key)) break :blk false;
        }
        break :blk true;
    };

    const strategy = chooseStrategy(items.len, limit, all_numeric);

    switch (strategy) {
        .heap_topk => {
            const k = limit orelse items.len;
            var heap = try TopKHeap.init(allocator, k, descending);
            defer heap.deinit();

            for (items) |entry| {
                heap.insert(entry);
            }

            const sorted = heap.getSorted();
            // Copy results back to the beginning of items
            @memcpy(items[0..sorted.len], sorted);
            return items[0..sorted.len];
        },
        .radix => {
            try radixSortU64(allocator, items, descending);
            if (limit) |k| {
                return items[0..@min(k, items.len)];
            }
            return items;
        },
        .comparison => {
            const Ctx = struct {
                desc: bool,
                pub fn lessThan(ctx: @This(), a: SortKey, b: SortKey) bool {
                    const a_is_num = !std.math.isNan(a.numeric_key);
                    const b_is_num = !std.math.isNan(b.numeric_key);
                    if (a_is_num and b_is_num) {
                        if (ctx.desc) return b.numeric_key < a.numeric_key else return a.numeric_key < b.numeric_key;
                    }
                    if (ctx.desc) return std.mem.lessThan(u8, b.sort_key, a.sort_key) else return std.mem.lessThan(u8, a.sort_key, b.sort_key);
                }
            };
            std.mem.sort(SortKey, items, Ctx{ .desc = descending }, Ctx.lessThan);
            if (limit) |k| {
                return items[0..@min(k, items.len)];
            }
            return items;
        },
    }
}

// ============================================================================
// Tests
// ============================================================================

test "f64 to sortable u64 preserves order" {
    const vals = [_]f64{ -1000.0, -1.0, -0.0, 0.0, 0.001, 1.0, 100.0, 1000.0 };
    var prev: u64 = 0;
    for (vals) |v| {
        const key = f64ToSortableU64(v);
        try std.testing.expect(key >= prev);
        prev = key;
    }
}

test "f64 roundtrip through sortable u64" {
    const vals = [_]f64{ -999.5, -1.0, 0.0, 1.0, 42.0, 99999.0 };
    for (vals) |v| {
        const key = f64ToSortableU64(v);
        const back = sortableU64ToF64(key);
        try std.testing.expectEqual(v, back);
    }
}

test "string to sortable u64 preserves lexicographic order" {
    const a = stringToSortableU64("Alice");
    const b = stringToSortableU64("Bob");
    const c = stringToSortableU64("Charlie");
    const z = stringToSortableU64("Zebra");
    try std.testing.expect(a < b);
    try std.testing.expect(b < c);
    try std.testing.expect(c < z);
}

test "TopKHeap finds top 3 ascending" {
    const allocator = std.testing.allocator;
    var heap = try TopKHeap.init(allocator, 3, false);
    defer heap.deinit();

    const vals = [_]f64{ 50, 10, 80, 30, 90, 20, 70 };
    for (vals) |v| {
        heap.insert(makeSortKey(v, "", ""));
    }

    const sorted = heap.getSorted();
    try std.testing.expectEqual(@as(usize, 3), sorted.len);
    // ASC top 3: 10, 20, 30
    try std.testing.expectEqual(@as(f64, 10), sortableU64ToF64(sorted[0].radix_key));
    try std.testing.expectEqual(@as(f64, 20), sortableU64ToF64(sorted[1].radix_key));
    try std.testing.expectEqual(@as(f64, 30), sortableU64ToF64(sorted[2].radix_key));
}

test "TopKHeap finds top 3 descending" {
    const allocator = std.testing.allocator;
    var heap = try TopKHeap.init(allocator, 3, true);
    defer heap.deinit();

    const vals = [_]f64{ 50, 10, 80, 30, 90, 20, 70 };
    for (vals) |v| {
        heap.insert(makeSortKey(v, "", ""));
    }

    const sorted = heap.getSorted();
    try std.testing.expectEqual(@as(usize, 3), sorted.len);
    // DESC top 3: 90, 80, 70
    try std.testing.expectEqual(@as(f64, 90), sortableU64ToF64(sorted[0].radix_key));
    try std.testing.expectEqual(@as(f64, 80), sortableU64ToF64(sorted[1].radix_key));
    try std.testing.expectEqual(@as(f64, 70), sortableU64ToF64(sorted[2].radix_key));
}

test "radix sort ascending" {
    const allocator = std.testing.allocator;
    var items = [_]SortKey{
        makeSortKey(50, "", ""),
        makeSortKey(10, "", ""),
        makeSortKey(80, "", ""),
        makeSortKey(30, "", ""),
        makeSortKey(20, "", ""),
    };

    try radixSortU64(allocator, &items, false);

    try std.testing.expectEqual(@as(f64, 10), sortableU64ToF64(items[0].radix_key));
    try std.testing.expectEqual(@as(f64, 20), sortableU64ToF64(items[1].radix_key));
    try std.testing.expectEqual(@as(f64, 30), sortableU64ToF64(items[2].radix_key));
    try std.testing.expectEqual(@as(f64, 50), sortableU64ToF64(items[3].radix_key));
    try std.testing.expectEqual(@as(f64, 80), sortableU64ToF64(items[4].radix_key));
}

test "radix sort with negative numbers" {
    const allocator = std.testing.allocator;
    var items = [_]SortKey{
        makeSortKey(10, "", ""),
        makeSortKey(-5, "", ""),
        makeSortKey(0, "", ""),
        makeSortKey(-100, "", ""),
        makeSortKey(50, "", ""),
    };

    try radixSortU64(allocator, &items, false);

    try std.testing.expectEqual(@as(f64, -100), sortableU64ToF64(items[0].radix_key));
    try std.testing.expectEqual(@as(f64, -5), sortableU64ToF64(items[1].radix_key));
    try std.testing.expectEqual(@as(f64, 0), sortableU64ToF64(items[2].radix_key));
    try std.testing.expectEqual(@as(f64, 10), sortableU64ToF64(items[3].radix_key));
    try std.testing.expectEqual(@as(f64, 50), sortableU64ToF64(items[4].radix_key));
}

test "chooseStrategy selects heap for small limit" {
    try std.testing.expectEqual(SortStrategy.heap_topk, chooseStrategy(100000, 10, true));
    try std.testing.expectEqual(SortStrategy.heap_topk, chooseStrategy(100000, 100, true));
    // HEAP_MAX_K is arch-dependent (2048 on ARM, 1024 on x86), so test with a value that fits both
    try std.testing.expectEqual(SortStrategy.heap_topk, chooseStrategy(100000, 512, true));
}

test "chooseStrategy selects radix for large numeric data" {
    try std.testing.expectEqual(SortStrategy.radix, chooseStrategy(100000, null, true));
    try std.testing.expectEqual(SortStrategy.radix, chooseStrategy(100000, 90000, true));
}

test "chooseStrategy selects comparison for string data" {
    try std.testing.expectEqual(SortStrategy.comparison, chooseStrategy(100000, null, false));
    try std.testing.expectEqual(SortStrategy.comparison, chooseStrategy(100000, 90000, false));
}

test "chooseStrategy selects comparison for small datasets" {
    try std.testing.expectEqual(SortStrategy.comparison, chooseStrategy(100, null, true));
}

test "sortEntries unified interface ascending" {
    const allocator = std.testing.allocator;
    var items_arr = [_]SortKey{
        makeSortKey(50, "", "line50"),
        makeSortKey(10, "", "line10"),
        makeSortKey(80, "", "line80"),
    };
    const result = try sortEntries(allocator, &items_arr, false, null);
    try std.testing.expectEqual(@as(f64, 10), sortableU64ToF64(result[0].radix_key));
    try std.testing.expectEqual(@as(f64, 50), sortableU64ToF64(result[1].radix_key));
    try std.testing.expectEqual(@as(f64, 80), sortableU64ToF64(result[2].radix_key));
}

test "sortEntries with limit" {
    const allocator = std.testing.allocator;
    var items_arr = [_]SortKey{
        makeSortKey(50, "", ""),
        makeSortKey(10, "", ""),
        makeSortKey(80, "", ""),
        makeSortKey(30, "", ""),
        makeSortKey(90, "", ""),
    };
    const result = try sortEntries(allocator, &items_arr, false, 2);
    try std.testing.expectEqual(@as(usize, 2), result.len);
    try std.testing.expectEqual(@as(f64, 10), sortableU64ToF64(result[0].radix_key));
    try std.testing.expectEqual(@as(f64, 30), sortableU64ToF64(result[1].radix_key));
}
