#!/usr/bin/env bash
# build-mcpb.sh — pack csvql into an MCP Bundle (.mcpb) for one-click Claude Desktop install.
#
#   ./scripts/build-mcpb.sh [path/to/csvql-binary]
#
# Defaults to zig-out/bin/csvql. Produces csvql.mcpb in the repo root: a zip of the
# binary (as server/csvql) + manifest.json with the version stamped from the binary.
# Users double-click the .mcpb in Claude Desktop to install — no config, no terminal.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${1:-$ROOT/zig-out/bin/csvql}"
[ -x "$BIN" ] || { echo "csvql binary not found: $BIN  (build: zig build -Doptimize=ReleaseFast)"; exit 1; }

VERSION="$("$BIN" --version 2>&1 | awk '{print $2}')"
VERSION="${VERSION:-0.0.0}"

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
mkdir -p "$STAGE/server"
cp "$BIN" "$STAGE/server/csvql"
chmod +x "$STAGE/server/csvql"
# Stamp version into the manifest (0.0.0 placeholder -> real version).
sed "s/\"version\": \"0.0.0\"/\"version\": \"$VERSION\"/" "$ROOT/mcpb/manifest.json" > "$STAGE/manifest.json"

OUT="$ROOT/csvql.mcpb"
rm -f "$OUT"
( cd "$STAGE" && zip -qr "$OUT" manifest.json server )
echo "built $OUT  (version $VERSION)"
echo "install: open csvql.mcpb in Claude Desktop (Settings → Extensions), or run 'mcpb validate' first."
