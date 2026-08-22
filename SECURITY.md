# Security Policy

**中文：** [SECURITY.zh-CN.md](SECURITY.zh-CN.md)

## Supported versions

| Version | Supported |
|---------|-----------|
| `main` (0.1.x) | Yes |
| Older tags | Best effort |

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

1. Prefer GitHub **Security Advisories** on [lanpishu6300/multiraft](https://github.com/lanpishu6300/multiraft/security/advisories/new) if available
2. Or private contact: **lanpishu6300@gmail.com** with subject `[SECURITY] multiraft`

Include:

- Affected crate / component
- Reproduction steps or PoC (private)
- Impact assessment (auth bypass, DoS, data leak, etc.)

We aim to acknowledge within **72 hours** and provide a remediation plan or fix timeline.

## Scope notes

- Demo admin HTTP and Raft gRPC are intended for lab / local clusters — treat exposure to untrusted networks as in scope if enabled by default in scripts.
- **Admin HTTP has no authentication.** Routes under `/admin/*` (membership promote/demote and snapshot ads) and `/snapshots/*/latest` must stay on loopback or behind an authenticated gateway. `replicate_standby_snapshot` is a historical, contained endpoint that returns typed unsupported (HTTP 409) before any `fetch_url` or other fetch effect. Do not port-forward these routes to untrusted networks. Snapshot SHA-256 verifies integrity, not trust of the fetch source; any future re-enable of `replicate_standby_snapshot` is security-sensitive.
- The 2026-08-22 standby containment is a **P2 threat-boundary correction**: live HTTP/ad/catalog/daisy restore is typed unsupported because C42 and its metadata did not establish complete restore ownership. Treat any route that attempts to re-enable it as security-sensitive until a separately Accepted complete restore protocol exists.
- Dependency CVEs: prefer PRs bumping versions with a short risk note (respect the openraft exact pin unless the bump is intentional).
