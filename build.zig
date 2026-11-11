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
    // Output: zig-out/lib/libcsvql.{dylib,so} on macOS/Linux; on Windows the
    // DLL goes to zig-out/bin/csvql.dll and only the import library
    // csvql.lib lands in lib/ (python/csvql/_loader.py checks both).
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
    // Depend on the *install* step, not the compile step: depending on
    // lib.step compiled the library but never copied it into zig-out, so
    // `zig build lib` silently produced nothing you could load.
    lib_step.dependOn(&b.addInstallArtifact(lib, .{}).step);

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

    // WHERE-expression parser benchmark
    const parser_bench_mod = b.addModule("parser", .{
        .root_source_file = b.path("src/parser.zig"),
    });
    const parser_bench_root = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .root_source_file = b.path("bench/parser_bench.zig"),
    });
    parser_bench_root.addImport("parser", parser_bench_mod);
    const parser_bench_exe = b.addExecutable(.{
        .name = "parser_bench",
        .root_module = parser_bench_root,
    });
    parser_bench_exe.linkLibC();
    b.installArtifact(parser_bench_exe);

    const parser_bench_run = b.addRunArtifact(parser_bench_exe);
    parser_bench_run.step.dependOn(b.getInstallStep());
    const parser_bench_step = b.step("bench-parser", "Run WHERE-expression parser benchmark");
    parser_bench_step.dependOn(&parser_bench_run.step);

    // Example executables for library users: `zig build examples`.
    // Not part of the default install, so a plain `zig build` stays lean.
    const examples_step = b.step("examples", "Build the library examples into zig-out/bin");
    const csv_module = b.addModule("csv", .{
        .root_source_file = b.path("src/csv.zig"),
    });
    for ([_][]const u8{ "csv_reader_example", "mmap_csv_example" }) |name| {
        const example = b.addExecutable(.{
            .name = name,
            .root_module = b.createModule(.{
                .target = target,
                .optimize = optimize,
                .root_source_file = b.path(b.fmt("examples/{s}.zig", .{name})),
            }),
        });
        example.linkLibC();
        example.root_module.addImport("csv", csv_module);
        examples_step.dependOn(&b.addInstallArtifact(example, .{}).step);
    }

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

    // WHERE-expression precedence/grammar tests (#154, #155 regressions +
    // tokenizer/recursive-descent rewrite acceptance criteria)
    const where_expr_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .root_source_file = b.path("tests/where_expr_precedence_test.zig"),
        }),
    });
    where_expr_tests.linkLibC();
    where_expr_tests.root_module.addImport("parser", parser_module);
    const run_where_expr_tests = b.addRunArtifact(where_expr_tests);
    test_step.dependOn(&run_where_expr_tests.step);

    // Shell completion drift tests (#73): completions/ must list every flag in
    // `csvql --help`. The files are embedded, so no working-directory assumptions.
    const completions_test_mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .root_source_file = b.path("tests/completions_test.zig"),
    });
    completions_test_mod.addAnonymousImport("main_source", .{ .root_source_file = b.path("src/main.zig") });
    completions_test_mod.addAnonymousImport("bash_completion", .{ .root_source_file = b.path("completions/csvql.bash") });
    completions_test_mod.addAnonymousImport("zsh_completion", .{ .root_source_file = b.path("completions/_csvql") });
    const completions_tests = b.addTest(.{ .root_module = completions_test_mod });
    const run_completions_tests = b.addRunArtifact(completions_tests);
    test_step.dependOn(&run_completions_tests.step);

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

    // Single-file test roots: each file's own `test` blocks, no extra imports.
    for ([_][]const u8{
        "fast_sort", // internal module tests
        "engine", // GROUP BY, aggregation integration
        "options", // thread count configuration
        "mcp", // token guardrails: default row cap
        "install", // Claude Desktop config merge preserves other keys
        "audit", // audit log
        "scalar", // COALESCE multi-arg, etc.
        "arith", // price * qty, SUM(price * qty)
        "xlsx", // zip, shared strings, date serials
        "bulk_csv", // TooManyColumns, etc.
        "mmap_engine", // DISTINCT, ORDER BY, scan paths
        "simd", // parseIntFast, stringsEqualFast, parseCSVFields
        "arena_buffer", // grow/realloc, JSON escaping
        "file_map", // mmap on POSIX, MapViewOfFile on Windows
    }) |name| {
        const t = b.addTest(.{
            .root_module = b.createModule(.{
                .target = target,
                .optimize = optimize,
                .root_source_file = b.path(b.fmt("src/{s}.zig", .{name})),
            }),
        });
        t.linkLibC();
        test_step.dependOn(&b.addRunArtifact(t).step);
    }
}
