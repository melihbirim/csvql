const std = @import("std");
const csv = @import("csv");

// TDD Test 6: CsvWriter properly handles all data (no short writes)
test "CsvWriter writeRecord outputs complete data" {
    const allocator = std.testing.allocator;

    // Create a temporary file
    const tmp_file = try std.fs.cwd().createFile("test_writer.csv", .{ .read = true });
    defer {
        tmp_file.close();
        std.fs.cwd().deleteFile("test_writer.csv") catch {};
    }

    var writer = csv.CsvWriter.init(tmp_file);

    // Write some records
    const fields1 = &[_][]const u8{ "id", "name", "value" };
    const fields2 = &[_][]const u8{ "1", "Alice", "100" };
    const fields3 = &[_][]const u8{ "2", "Bob", "200" };

    try writer.writeRecord(fields1);
    try writer.writeRecord(fields2);
    try writer.writeRecord(fields3);
    try writer.flush();

    // Read back and verify
    try tmp_file.seekTo(0);
    const content = try tmp_file.readToEndAlloc(allocator, 1024);
    defer allocator.free(content);

    // Should have complete lines (no partial writes)
    const expected = "id,name,value\n1,Alice,100\n2,Bob,200\n";
    try std.testing.expectEqualStrings(expected, content);
}

// TDD Test 8: CsvWriter escapes quotes correctly
test "CsvWriter escapes quotes" {
    const allocator = std.testing.allocator;

    const tmp_file = try std.fs.cwd().createFile("test_quotes.csv", .{ .read = true });
    defer {
        tmp_file.close();
        std.fs.cwd().deleteFile("test_quotes.csv") catch {};
    }

    var writer = csv.CsvWriter.init(tmp_file);

    // Write fields with special characters
    const fields = &[_][]const u8{ "Hello", "World, \"foo\"" };
    try writer.writeRecord(fields);
    try writer.flush();

    // Read back and verify proper quoting
    try tmp_file.seekTo(0);
    const content = try tmp_file.readToEndAlloc(allocator, 1024);
    defer allocator.free(content);

    // Should escape quotes as "" and wrap field in quotes
    const expected = "Hello,\"World, \"\"foo\"\"\"\n";
    try std.testing.expectEqualStrings(expected, content);
}

// TDD Test: JsonWriter produces a valid JSON array
test "JsonWriter writeRecord outputs JSON array" {
    const allocator = std.testing.allocator;

    const tmp_file = try std.fs.cwd().createFile("test_json_array.json", .{ .read = true });
    defer {
        tmp_file.close();
        std.fs.cwd().deleteFile("test_json_array.json") catch {};
    }

    var writer = csv.JsonWriter.init(tmp_file, .json);
    defer writer.deinit();
    const header = &[_][]const u8{ "name", "age" };
    try writer.setHeader(header);

    try writer.writeRecord(&[_][]const u8{ "Alice", "35" });
    try writer.writeRecord(&[_][]const u8{ "Bob", "42" });
    try writer.finish();
    try writer.flush();

    try tmp_file.seekTo(0);
    const content = try tmp_file.readToEndAlloc(allocator, 4096);
    defer allocator.free(content);

    const expected =
        \\[
        \\{"name":"Alice","age":35},
        \\{"name":"Bob","age":42}
        \\]
        \\
    ;
    try std.testing.expectEqualStrings(expected, content);
}

// TDD Test: JsonWriter produces newline-delimited JSON (JSONL)
test "JsonWriter writeRecord outputs JSONL" {
    const allocator = std.testing.allocator;

    const tmp_file = try std.fs.cwd().createFile("test_jsonl.jsonl", .{ .read = true });
    defer {
        tmp_file.close();
        std.fs.cwd().deleteFile("test_jsonl.jsonl") catch {};
    }

    var writer = csv.JsonWriter.init(tmp_file, .jsonl);
    defer writer.deinit();
    const header = &[_][]const u8{ "name", "age" };
    try writer.setHeader(header);

    try writer.writeRecord(&[_][]const u8{ "Alice", "35" });
    try writer.writeRecord(&[_][]const u8{ "Bob", "42" });
    try writer.finish(); // no-op for JSONL
    try writer.flush();

    try tmp_file.seekTo(0);
    const content = try tmp_file.readToEndAlloc(allocator, 4096);
    defer allocator.free(content);

    const expected = "{\"name\":\"Alice\",\"age\":35}\n{\"name\":\"Bob\",\"age\":42}\n";
    try std.testing.expectEqualStrings(expected, content);
}

// TDD Test: JsonWriter escapes special characters in values
test "JsonWriter escapes special characters" {
    const allocator = std.testing.allocator;

    const tmp_file = try std.fs.cwd().createFile("test_json_escape.json", .{ .read = true });
    defer {
        tmp_file.close();
        std.fs.cwd().deleteFile("test_json_escape.json") catch {};
    }

    var writer = csv.JsonWriter.init(tmp_file, .jsonl);
    defer writer.deinit();
    const header = &[_][]const u8{"value"};
    try writer.setHeader(header);

    // Value with quotes, backslash, tab, newline
    try writer.writeRecord(&[_][]const u8{"say \"hello\"\nworld\t\\"});
    try writer.flush();

    try tmp_file.seekTo(0);
    const content = try tmp_file.readToEndAlloc(allocator, 4096);
    defer allocator.free(content);

    const expected = "{\"value\":\"say \\\"hello\\\"\\nworld\\t\\\\\"}\n";
    try std.testing.expectEqualStrings(expected, content);
}

// TDD Test: JsonWriter outputs empty array when no records
test "JsonWriter empty JSON array" {
    const allocator = std.testing.allocator;

    const tmp_file = try std.fs.cwd().createFile("test_json_empty.json", .{ .read = true });
    defer {
        tmp_file.close();
        std.fs.cwd().deleteFile("test_json_empty.json") catch {};
    }

    var writer = csv.JsonWriter.init(tmp_file, .json);
    defer writer.deinit();
    const header = &[_][]const u8{ "name", "age" };
    try writer.setHeader(header);
    try writer.finish();
    try writer.flush();

    try tmp_file.seekTo(0);
    const content = try tmp_file.readToEndAlloc(allocator, 4096);
    defer allocator.free(content);

    try std.testing.expectEqualStrings("[]\n", content);
}

// TDD Test: RecordWriter CSV mode is compatible with CsvWriter output
test "RecordWriter CSV mode matches CsvWriter output" {
    const allocator = std.testing.allocator;

    const tmp_file = try std.fs.cwd().createFile("test_record_writer_csv.csv", .{ .read = true });
    defer {
        tmp_file.close();
        std.fs.cwd().deleteFile("test_record_writer_csv.csv") catch {};
    }

    const opts = csv.options.Options{};
    var writer = csv.RecordWriter.init(tmp_file, opts);
    defer writer.deinit();
    try writer.writeHeader(&[_][]const u8{ "id", "name" }, false);
    try writer.writeRecord(&[_][]const u8{ "1", "Alice" });
    try writer.writeRecord(&[_][]const u8{ "2", "Bob" });
    try writer.finish();
    try writer.flush();

    try tmp_file.seekTo(0);
    const content = try tmp_file.readToEndAlloc(allocator, 4096);
    defer allocator.free(content);

    try std.testing.expectEqualStrings("id,name\n1,Alice\n2,Bob\n", content);
}
