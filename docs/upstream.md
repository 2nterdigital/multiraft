# Upstream pin note

**中文：** [upstream.zh-CN.md](./upstream.zh-CN.md)

## Locked versions

Workspace `Cargo.toml` pins:

```toml
openraft = { git = "https://github.com/2nterdigital/openraft.git", rev = "ea46d0e571f7497f1f549e125254dfcb2ff45e2e", version = "=0.10.0-alpha.30", default-features = false, features = ["serde", "type-alias", "tokio-rt"] }
openraft-multi = { git = "https://github.com/2nterdigital/openraft.git", rev = "ea46d0e571f7497f1f549e125254dfcb2ff45e2e", version = "=0.10.0-alpha.30" }
```

Both crates use an **exact** version requirement (`=`). Do not widen to `^` / `~` without a deliberate bump and retest of the demo + `./scripts/acceptance.sh`.

## Why lock

`openraft-multi` and the Multi-Raft APIs used by `multiraft-net` (`MultiGroup`, shared network / router patterns) are still on the **0.10.0-alpha** line. Patch alphas can change type aliases, feature flags, and example layouts. Pinning both crates to the same alpha revision avoids:

- accidental Cargo resolution to a newer incompatible alpha
- `openraft` / `openraft-multi` version skew within one workspace
- silent breakage of phase-1 in-process `GroupRouter` + demo acceptance

## Reference example

Upstream multi-group KV example (same release train):

- [openraft `examples/multi-raft-kv`](https://github.com/datafuselabs/openraft/tree/v0.10.0-alpha.30/examples/multi-raft-kv)

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

## Optional election source observer

The exact alpha30 version is additionally bound to
`https://github.com/2nterdigital/openraft.git` at
`ea46d0e571f7497f1f549e125254dfcb2ff45e2e` for both workspace crates. This source
revision is based on upstream alpha30 `19be0c27e5141d8acea3468cdb8a90875f117c27`
and adds an optional constructor-installed typed election observer. It does not
change native election algorithms, parameters, persisted formats or default
transport/PreVote behavior. Cargo.lock fixes openraft, openraft-multi, macros and
runtime crates to the same source revision. No registry cache or vendored shim
is edited. See [source facts](specs/election-source-facts.md) and the paired
[native PR](https://github.com/2nterdigital/openraft/pull/1). A future source update
must deliberately update both git revisions and repeat workspace/acceptance checks.
