"""
Build script: invokes `zig build -Doptimize=ReleaseFast` to compile
libcsvql.dylib/.so, then copies it into the package directory so it's
included in the wheel.
"""

import os
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py


REPO_ROOT = Path(__file__).parent.parent  # python/ → repo root
LIB_SRC = REPO_ROOT / "zig-out" / "lib"
PKG_DIR = Path(__file__).parent / "csvql"


def _lib_name() -> str:
    if sys.platform == "darwin":
        return "libcsvql.dylib"
    return "libcsvql.so"


class BuildZigLib(build_py):
    def run(self):
        # Build the Zig shared library
        subprocess.check_call(
            ["zig", "build", "-Doptimize=ReleaseFast"],
            cwd=str(REPO_ROOT),
        )
        # Copy library into package directory so it's included in the wheel
        src = LIB_SRC / _lib_name()
        dst = PKG_DIR / _lib_name()
        if not src.exists():
            raise FileNotFoundError(
                f"Expected {src} after `zig build`. Is Zig installed and on PATH?"
            )
        shutil.copy2(src, dst)
        super().run()


setup(
    cmdclass={"build_py": BuildZigLib},
)
