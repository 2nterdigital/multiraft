# Upstream pin note

**中文：** [upstream.zh-CN.md](./upstream.zh-CN.md)

## Locked versions

Workspace `Cargo.toml` pins:

```toml
openraft = { git = "https://github.com/2nterdigital/openraft.git", rev = "b5f3dc1a4df74891d4a8236b9b35af90158dbb52", version = "=0.10.1", default-features = false, features = ["serde", "type-alias", "tokio-rt"] }
openraft-multi = { git = "https://github.com/2nterdigital/openraft.git", rev = "b5f3dc1a4df74891d4a8236b9b35af90158dbb52", version = "=0.10.1" }
```

Both crates use an **exact** version requirement (`=`). Do not widen to `^` / `~` without a deliberate bump and retest of the demo + `./scripts/acceptance.sh`.

## Why lock

`openraft-multi` and the Multi-Raft APIs used by `multiraft-net` (`MultiGroup`, shared network / router patterns) track the **0.10** line, where patch releases have changed type aliases, feature flags, and example layouts. Pinning both crates to the same source revision avoids:

- accidental Cargo resolution to a newer incompatible release
- `openraft` / `openraft-multi` version skew within one workspace
- silent breakage of phase-1 in-process `GroupRouter` + demo acceptance

## Reference example

Upstream multi-group KV example (same release train):

- [openraft `examples/multi-raft-kv`](https://github.com/datafuselabs/openraft/tree/v0.10.1/examples/multi-raft-kv)

When bumping the pin, start from that tree’s `Cargo.toml` / README for the target tag, then re-run:

```bash
cargo test --workspace
./scripts/acceptance.sh
```

## Bump checklist

1. Update both workspace deps to the same new `=x.y.z` (or matching alpha).
2. Diff against the upstream multi-raft-kv example for that tag.
3. Fix compile / API breaks in `multiraft-core` and `multiraft-net`.
4. Pass workspace tests and acceptance.

## Fork patches

Both workspace crates resolve to `https://github.com/2nterdigital/openraft.git`
at `b5f3dc1a4df74891d4a8236b9b35af90158dbb52` (branch `claude/v0.10.1-fork`).
That revision is upstream tag `v0.10.1` (`6d3ede17`) plus three patches carried
over from the earlier alpha30 fork (`8f0b2b48`):

- an optional constructor-installed typed election observer
  (see [source facts](specs/election-source-facts.md));
- guarded single-step membership changes and a construction policy; a guarded
  change from a uniform built-in `Membership` always appends the joint step;
- `Config::broadcast_commit` (default `true`); `false` stops sending an empty
  AppendEntries for a commit-index advance alone.

Cargo.lock fixes openraft, openraft-multi, macros and runtime crates to the
same source revision. No registry cache or vendored shim is edited. A future
source update must deliberately update both git revisions and repeat
workspace/acceptance checks.

Wire and disk compatibility with alpha30 nodes is kept on the multiraft side:
the 3-field `SnapshotMetaWithId` still writes a non-empty snapshot id, the
legacy `VoteRequest` layout is decoded as a fallback, and append replies stay
`AppendEntriesResponse` on the wire. `pre_vote` is granted locally and never
sent, as on alpha30.
