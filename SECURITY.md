# Security

## Reporting a vulnerability

Report privately through GitHub's [Security Advisories](https://github.com/melihbirim/csvql/security/advisories/new) for this repository. Please do not open a public issue for security reports. You will get an acknowledgement, and a fix or mitigation plan once the report is triaged.

## Supported versions

Fixes land on the latest release. Please upgrade to the latest version before reporting.

## Security posture

csvql is built to be safe to run next to sensitive data, including on corporate servers.

**Read only.** csvql runs `SELECT` only. The parser has no `INSERT`, `UPDATE`, `DELETE`, or `DROP`. It cannot modify, delete, or write your data files.

**No data egress.** Data stays on the machine. A query returns only its result, never the file. Over MCP, `csv_query` results are capped (about 12 KB per response), which also limits how much an agent can pull out across queries.

**No network, air gappable.** csvql makes zero outbound connections. No telemetry, no cloud, no API keys, no update checks. It runs fully offline.

**Filesystem scoping.** By default csvql can read any file the process user can. Pass `--root <dir>[,<dir>]` to confine all file access (CLI and MCP) to those directory trees. Paths are resolved with `realpath`, so `../` traversal and symlink escape are rejected. Always set `--root` when exposing csvql to an agent or another user.

**Minimal attack surface.** A single static binary. No runtime, no interpreter, no dependencies to track for CVEs. With the SSH access model there is no long running daemon and no open port.

## Hardening for on prem and remote access

When running csvql on a server that an agent or remote user queries:

* Set `--root` to the data directory so nothing else is readable.
* Run as a dedicated, unprivileged user with a read only mount of the data.
* Prefer the SSH access model (`ssh host csvql --mcp --root /data`). It reuses your existing SSH keys, bastion, and audit trail, and needs no open port.
* Enable `--audit <file>` to record every query (timestamp and SQL) as JSONL for compliance and forensics.
* If you expose an HTTP transport later, keep csvql behind a reverse proxy that terminates TLS and handles authentication.

See the README section "Remote and on-prem" for the SSH setup.
