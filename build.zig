const std = @import("std");

/// Auto-detect the Node.js include directory by running `node` at build time.
fn detectNodeInclude(allocator: std.mem.Allocator) ?[]const u8 {
    const result = std.process.Child.run(.{
        .allocator = allocator,
        .argv = &.{
            "node",                                                                                        "-e",
            "const p=require('path');process.stdout.write(p.join(process.execPath,'../../include/node'))",
        },
    }) catch return null;
    allocator.free(result.stderr);
    if (result.term != .Exited or result.term.Exited != 0) {
        allocator.free(result.stdout);
        return null;
    }
    return result.stdout; // allocator owns; lives for the duration of build()
}

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
    exe.linkLibC();

    const zigtable_mod = b.createModule(.{
        .root_source_file = b.path("src/zigtable.zig"),
    });
    exe.root_module.addImport("zigtable", zigtable_mod);

    b.installArtifact(exe);

    // Shared library — C ABI for Python ctypes and other FFI callers.
    // Build: zig build lib -Doptimize=ReleaseFast
    // Output: zig-out/lib/libcsvql.dylib (macOS) or libcsvql.so (Linux)
    const lib = b.addLibrary(.{
        .name = "csvql",
        .linkage = .dynamic,
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("src/lib.zig"),
        }),
    });
    lib.linkLibC();
    b.installArtifact(lib);

    const lib_step = b.step("lib", "Build shared library (libcsvql)");
    lib_step.dependOn(&lib.step);

    // Run command
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    // Benchmark executables
    const simd_mod = b.createModule(.{
        .root_source_file = b.path("src/simd.zig"),
        .target = target,
        .optimize = optimize,
    });
    const csv_bench_root = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .root_source_file = b.path("bench/csv_parse_bench.zig"),
    });
    csv_bench_root.addImport("simd", simd_mod);
    const csv_bench = b.addExecutable(.{
        .name = "csv_parse_bench",
        .root_module = csv_bench_root,
    });
    csv_bench.linkLibC();
    b.installArtifact(csv_bench);

    const bench_run = b.addRunArtifact(csv_bench);
    bench_run.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        bench_run.addArgs(args);
    }
    const bench_step = b.step("bench", "Run CSV parsing benchmark");
    bench_step.dependOn(&bench_run.step);

    // One-command correctness reproduction: builds the exe (pass
    // -Doptimize=ReleaseFast, same as CI) then diffs every query in
    // bench/verify_correctness.sh against DuckDB. See CORRECTNESS.md.
    const verify_run = b.addSystemCommand(&.{ "bash", "bench/verify_correctness.sh" });
    verify_run.step.dependOn(b.getInstallStep());
    const verify_step = b.step("verify", "Run the DuckDB differential correctness suite (needs duckdb in PATH; pass -Doptimize=ReleaseFast)");
    verify_step.dependOn(&verify_run.step);

    // GROUP BY benchmark — uses named modules so bench/ can reach src/
    const engine_mod = b.createModule(.{
        .root_source_file = b.path("src/engine.zig"),
        .target = target,
        .optimize = optimize,
    });
    const groupby_bench_root = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .root_source_file = b.path("bench/groupby_bench.zig"),
    });
    groupby_bench_root.addImport("engine", engine_mod);
    const groupby_bench_exe = b.addExecutable(.{
        .name = "groupby_bench",
        .root_module = groupby_bench_root,
    });
    groupby_bench_exe.linkLibC();
    b.installArtifact(groupby_bench_exe);

    const groupby_bench_run = b.addRunArtifact(groupby_bench_exe);
    groupby_bench_run.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        groupby_bench_run.addArgs(args);
    }
    const groupby_bench_step = b.step("bench-groupby", "Run GROUP BY benchmark");
    groupby_bench_step.dependOn(&groupby_bench_run.step);

    // Example executables for library users
    const csv_example = b.addExecutable(.{
        .name = "csv_reader_example",
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("examples/csv_reader_example.zig"),
        }),
    });
    csv_example.linkLibC();

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
    mmap_example.linkLibC();
    b.installArtifact(mmap_example);

    // Node.js N-API addon — zig build node -Doptimize=ReleaseFast
    // Output: zig-out/lib/csvql.node  (loaded by nodejs/index.js)
    const node_include = b.option(
        []const u8,
        "node-include",
        "Path to Node.js include dir containing node_api.h (auto-detected if omitted)",
    ) orelse detectNodeInclude(b.allocator);

    // Windows-only: path to node.lib, the import library that resolves the
    // napi_* symbols at link time.
    //
    // On Linux and macOS a shared object may leave those symbols undefined —
    // the loader binds them from the host process at dlopen(). Windows has no
    // such thing: a DLL must resolve every symbol when it is linked. Without
    // node.lib, lld emits "undefined symbol: napi_..." as a *warning* (see
    // linker_allow_shlib_undefined below), produces a DLL anyway, and that
    // DLL segfaults the moment Node calls into it.
    //
    // node.lib is not part of the headers tarball; it is a separate download
    // (nodejs.org/dist/v<ver>/win-x64/node.lib). See the Windows steps in
    // ci.yml and release.yml.
    const node_lib = b.option(
        []const u8,
        "node-lib",
        "Path to node.lib (Windows only — resolves napi_* at link time)",
    );

    if (node_include) |inc| {
        const node_addon = b.addLibrary(.{
            .name = "csvql_node",
            .linkage = .dynamic,
            .root_module = b.createModule(.{
                .target = target,
                .optimize = optimize,
                .root_source_file = b.path("src/node_binding.zig"),
            }),
        });
        node_addon.linkLibC();
        node_addon.addIncludePath(.{ .cwd_relative = inc });
        // On POSIX, N-API symbols are resolved by Node.js at dlopen() time
        // rather than at link time. Windows cannot do that — it needs the
        // import library instead (see the -Dnode-lib comment above).
        node_addon.linker_allow_shlib_undefined = true;
        if (node_lib) |nlib| {
            node_addon.addObjectFile(.{ .cwd_relative = nlib });
        }

        const install_node = b.addInstallFileWithDir(
            node_addon.getEmittedBin(),
            .lib,
            "csvql.node",
        );

        const node_step = b.step("node", "Build Node.js N-API addon (zig-out/lib/csvql.node)");
        node_step.dependOn(&install_node.step);
    } else {
        const node_step = b.step("node", "Build Node.js N-API addon (requires node in PATH)");
        _ = node_step;
    }

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
    parser_tests.linkLibC();
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
    csv_tests.linkLibC();
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
    fast_sort_tests.linkLibC();
    const run_fast_sort_tests = b.addRunArtifact(fast_sort_tests);
    test_step.dependOn(&run_fast_sort_tests.step);

    // Engine tests (GROUP BY, aggregation integration)
    const engine_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("src/engine.zig"),
        }),
    });
    engine_tests.linkLibC();
    const run_engine_tests = b.addRunArtifact(engine_tests);
    test_step.dependOn(&run_engine_tests.step);

    // Options tests (thread count configuration)
    const options_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("src/options.zig"),
        }),
    });
    const run_options_tests = b.addRunArtifact(options_tests);
    test_step.dependOn(&run_options_tests.step);

    // Main tests (exit code classification for --strict / query errors)
    const main_test_mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .root_source_file = b.path("src/main.zig"),
    });
    main_test_mod.addImport("zigtable", zigtable_mod);
    const main_tests = b.addTest(.{ .root_module = main_test_mod });
    main_tests.linkLibC();
    const run_main_tests = b.addRunArtifact(main_tests);
    test_step.dependOn(&run_main_tests.step);

    // MCP tests (token guardrails: default row cap)
    const mcp_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("src/mcp.zig"),
        }),
    });
    mcp_tests.linkLibC();
    const run_mcp_tests = b.addRunArtifact(mcp_tests);
    test_step.dependOn(&run_mcp_tests.step);

    // Install tests (Claude Desktop config merge preserves other keys)
    const install_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("src/install.zig"),
        }),
    });
    install_tests.linkLibC();
    const run_install_tests = b.addRunArtifact(install_tests);
    test_step.dependOn(&run_install_tests.step);

    // Audit-log tests
    const audit_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("src/audit.zig"),
        }),
    });
    audit_tests.linkLibC();
    const run_audit_tests = b.addRunArtifact(audit_tests);
    test_step.dependOn(&run_audit_tests.step);

    // Scalar tests (COALESCE multi-arg, etc.)
    const scalar_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("src/scalar.zig"),
        }),
    });
    scalar_tests.linkLibC();
    const run_scalar_tests = b.addRunArtifact(scalar_tests);
    test_step.dependOn(&run_scalar_tests.step);

    // BulkCsvReader tests (TooManyColumns, etc.)
    const bulk_csv_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("src/bulk_csv.zig"),
        }),
    });
    bulk_csv_tests.linkLibC();
    const run_bulk_csv_tests = b.addRunArtifact(bulk_csv_tests);
    test_step.dependOn(&run_bulk_csv_tests.step);

    // Mmap engine tests (DISTINCT, ORDER BY, scan paths)
    const mmap_engine_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("src/mmap_engine.zig"),
        }),
    });
    mmap_engine_tests.linkLibC();
    const run_mmap_engine_tests = b.addRunArtifact(mmap_engine_tests);
    test_step.dependOn(&run_mmap_engine_tests.step);

    // Simd utility tests (parseIntFast, stringsEqualFast, parseCSVFields)
    const simd_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("src/simd.zig"),
        }),
    });
    simd_tests.linkLibC();
    const run_simd_tests = b.addRunArtifact(simd_tests);
    test_step.dependOn(&run_simd_tests.step);

    // ArenaBuffer tests (grow/realloc, JSON escaping)
    const arena_buffer_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("src/arena_buffer.zig"),
        }),
    });
    arena_buffer_tests.linkLibC();
    const run_arena_buffer_tests = b.addRunArtifact(arena_buffer_tests);
    test_step.dependOn(&run_arena_buffer_tests.step);
}
