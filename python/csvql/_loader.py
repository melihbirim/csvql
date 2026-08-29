"""
Locates and loads libcsvql (.dll on Windows, .dylib on macOS, .so elsewhere).

Search order:
  1. Same directory as this file (installed wheel — lib bundled alongside .py)
  2. zig-out/ relative to the repo root (development build)
  3. Directories listed in CSVQL_LIB_PATH environment variable
"""

import ctypes
import os
import sys
from pathlib import Path

_lib_cache: ctypes.CDLL | None = None


def _lib_name() -> str:
    if sys.platform == "win32":
        # Zig names the Windows shared library csvql.dll — no "lib" prefix.
        return "csvql.dll"
    if sys.platform == "darwin":
        return "libcsvql.dylib"
    return "libcsvql.so"


def _candidate_dirs() -> list[Path]:
    dirs: list[Path] = []

    # 1. Next to this .py file (wheel install)
    dirs.append(Path(__file__).parent)

    # 2. zig-out/ — walk up from this file looking for build.zig.
    #    Zig installs a .so/.dylib into zig-out/lib, but puts a Windows DLL
    #    in zig-out/bin alongside the executables (only the import library
    #    csvql.lib lands in lib/). Check both so a development build is
    #    found on every platform.
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "build.zig").exists():
            dirs.append(parent / "zig-out" / "lib")
            dirs.append(parent / "zig-out" / "bin")
            break

    # 3. CSVQL_LIB_PATH env override
    env_path = os.environ.get("CSVQL_LIB_PATH")
    if env_path:
        dirs.append(Path(env_path))

    return dirs


def load() -> ctypes.CDLL:
    global _lib_cache
    if _lib_cache is not None:
        return _lib_cache

    name = _lib_name()
    for d in _candidate_dirs():
        candidate = d / name
        if candidate.exists():
            lib = ctypes.CDLL(str(candidate))
            _setup_signatures(lib)
            _lib_cache = lib
            return lib

    searched = "\n  ".join(str(d / name) for d in _candidate_dirs())
    raise FileNotFoundError(
        f"Could not find {name}. Searched:\n  {searched}\n"
        "Run `zig build lib -Doptimize=ReleaseFast` to build it, "
        "or set CSVQL_LIB_PATH to its directory."
    )


def _setup_signatures(lib: ctypes.CDLL) -> None:
    """Declare argument and return types for all exported functions."""
    lib.csvql_query_json.argtypes = [
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    lib.csvql_query_json.restype = ctypes.c_int

    lib.csvql_query_csv.argtypes = [
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    lib.csvql_query_csv.restype = ctypes.c_int

    lib.csvql_free.argtypes = [ctypes.c_void_p]
    lib.csvql_free.restype = None
