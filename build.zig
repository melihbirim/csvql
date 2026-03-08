const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create the executable
    const exe = b.addExecutable(.{
        .name = "csvql",
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("src/main.zig"),
        }),
    });

    b.installArtifact(exe);

    // Run command
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    // Benchmark executables
    const csv_bench = b.addExecutable(.{
        .name = "csv_parse_bench",
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("bench/csv_parse_bench.zig"),
        }),
    });
    b.installArtifact(csv_bench);

    const bench_run = b.addRunArtifact(csv_bench);
    bench_run.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        bench_run.addArgs(args);
    }
    const bench_step = b.step("bench", "Run CSV parsing benchmark");
    bench_step.dependOn(&bench_run.step);

    // Example executables for library users
    const csv_example = b.addExecutable(.{
        .name = "csv_reader_example",
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("examples/csv_reader_example.zig"),
        }),
    });

    // Add src/ as a module so examples can import from it
    const csv_module = b.addModule("csv", .{
        .root_source_file = b.path("src/csv.zig"),
    });
    csv_example.root_module.addImport("csv", csv_module);
    b.installArtifact(csv_example);

    const mmap_example = b.addExecutable(.{
        .name = "mmap_csv_example",
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("examples/mmap_csv_example.zig"),
        }),
    });
    b.installArtifact(mmap_example);

    // Tests
    const test_step = b.step("test", "Run unit tests");

    // Create modules for tests to import
    const parser_module = b.addModule("parser", .{
        .root_source_file = b.path("src/parser.zig"),
    });

    const csv_test_module = b.addModule("csv", .{
        .root_source_file = b.path("src/csv.zig"),
    });

    // Parser tests
    const parser_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("tests/parser_test.zig"),
        }),
    });
    parser_tests.root_module.addImport("parser", parser_module);
    const run_parser_tests = b.addRunArtifact(parser_tests);
    test_step.dependOn(&run_parser_tests.step);

    // CSV tests
    const csv_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("tests/csv_test.zig"),
        }),
    });
    csv_tests.root_module.addImport("csv", csv_test_module);
    const run_csv_tests = b.addRunArtifact(csv_tests);
    test_step.dependOn(&run_csv_tests.step);

    // Fast sort tests (internal module tests)
    const fast_sort_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("src/fast_sort.zig"),
        }),
    });
    const run_fast_sort_tests = b.addRunArtifact(fast_sort_tests);
    test_step.dependOn(&run_fast_sort_tests.step);
}
