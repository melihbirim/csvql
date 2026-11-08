#!/usr/bin/env bash
# Regenerate docs/llms-full.txt: the README, the SQL reference and the
# security notes in one file, so an agent that fetches the site once gets
# everything. Run after editing any of those files and commit the result.
set -euo pipefail
cd "$(dirname "$0")/.."
out=docs/llms-full.txt
{
  echo "# csvql, full documentation in one file"
  echo
  echo "> csvql (published as csvql-query on npm and PyPI) runs read-only SQL on CSV, TSV and .xlsx files in place, built for AI agents. This file is the README, the SQL reference and the security notes concatenated. Short index: https://melihbirim.github.io/csvql/llms.txt"
  echo
  echo "Generated from the repository at https://github.com/melihbirim/csvql by scripts/gen_llms_full.sh. Relative links below point into that repository."
  for f in README.md SQL_REFERENCE.md SECURITY.md; do
    echo
    echo
    echo "==================== $f ===================="
    echo
    # Drop the HTML logo block and badge lines at the top of the README.
    sed -e '/^<p align="center">/,/^<\/p>/d' -e '/^\[!\[/d' "$f"
  done
} > "$out"
echo "wrote $out ($(wc -c < "$out" | tr -d ' ') bytes)"
