#!/usr/bin/env bash
# update-mcp-registry.sh — regenerate server.json for the MCP Registry from a
# tagged GitHub release's .mcpb assets.
#
#   ./scripts/update-mcp-registry.sh v2.7.0
#
# Downloads each platform .mcpb asset for the given tag, computes its
# fileSha256, and rewrites server.json. Run this after cutting a release,
# then `mcp-publisher publish` to push the update to the registry.
set -euo pipefail

TAG="${1:?usage: update-mcp-registry.sh <tag, e.g. v2.7.0>}"
VERSION="${TAG#v}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO="melihbirim/csvql"

ASSETS=(csvql-macos-aarch64.mcpb csvql-macos-x86_64.mcpb csvql-linux-x86_64.mcpb csvql-windows-x86_64.mcpb)

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"
for a in "${ASSETS[@]}"; do
  gh release download "$TAG" -p "$a" --clobber -R "$REPO"
done

sha() { shasum -a 256 "$1" | awk '{print $1}'; }

python3 - "$ROOT/server.json" "$VERSION" "$REPO" "$TAG" \
  "$(sha csvql-macos-aarch64.mcpb)" "$(sha csvql-macos-x86_64.mcpb)" \
  "$(sha csvql-linux-x86_64.mcpb)" "$(sha csvql-windows-x86_64.mcpb)" <<'PY'
import json, sys
out, version, repo, tag, mac_arm, mac_x64, linux_x64, win_x64 = sys.argv[1:]
hashes = {
    "csvql-macos-aarch64.mcpb": mac_arm,
    "csvql-macos-x86_64.mcpb": mac_x64,
    "csvql-linux-x86_64.mcpb": linux_x64,
    "csvql-windows-x86_64.mcpb": win_x64,
}
with open(out) as f:
    doc = json.load(f)
doc["version"] = version
for pkg in doc["packages"]:
    name = pkg["identifier"].rsplit("/", 1)[-1]
    pkg["identifier"] = f"https://github.com/{repo}/releases/download/{tag}/{name}"
    pkg["fileSha256"] = hashes[name]
with open(out, "w") as f:
    json.dump(doc, f, indent=2)
    f.write("\n")
PY

echo "updated $ROOT/server.json for $TAG"
