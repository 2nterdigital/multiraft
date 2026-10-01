# 上游版本锁定说明

**English：** [upstream.md](./upstream.md)

## 锁定版本

Workspace `Cargo.toml` 锁定：

```toml
openraft = { git = "https://github.com/2nterdigital/openraft.git", rev = "ea46d0e571f7497f1f549e125254dfcb2ff45e2e", version = "=0.10.0-alpha.30", default-features = false, features = ["serde", "type-alias", "tokio-rt"] }
openraft-multi = { git = "https://github.com/2nterdigital/openraft.git", rev = "ea46d0e571f7497f1f549e125254dfcb2ff45e2e", version = "=0.10.0-alpha.30" }
```

两个 crate 均使用**精确**版本要求（`=`）。未经刻意升版并复测 Demo + `./scripts/acceptance.sh`，不要放宽为 `^` / `~`。

## 为何锁定

`openraft-multi` 以及 `multiraft-net` 所用的 Multi-Raft API（`MultiGroup`、共享网络 / router 模式）仍在 **0.10.0-alpha** 线上。Patch alpha 可能改动 type alias、feature flags 与示例布局。将二者钉在同一 alpha 修订可避免：

- Cargo 意外解析到更新的不兼容 alpha
- 同一 workspace 内 `openraft` / `openraft-multi` 版本漂移
- 一期进程内 `GroupRouter` + Demo 验收被静默破坏

## 参考示例

上游多 Group KV 示例（同一发布列车）：

- [openraft `examples/multi-raft-kv`](https://github.com/datafuselabs/openraft/tree/v0.10.0-alpha.30/examples/multi-raft-kv)

升版锁定时，先从该 tag 的 `Cargo.toml` / README 入手，然后重跑：

```bash
cargo test --workspace
./scripts/acceptance.sh
```

## 升版检查清单

1. 将两个 workspace 依赖更新为相同的新 `=x.y.z`（或匹配的 alpha）。
2. 对照该 tag 的上游 multi-raft-kv 示例做 diff。
3. 修复 `multiraft-core` 与 `multiraft-net` 的编译 / API 破坏。
4. 通过 workspace 测试与 acceptance。

## 可选选举源 observer

两个 workspace crate 除精确 alpha30 版本外，同时固定到
`https://github.com/2nterdigital/openraft.git` 的
`ea46d0e571f7497f1f549e125254dfcb2ff45e2e`。该修订基于原生 alpha30
`19be0c27e5141d8acea3468cdb8a90875f117c27`，只增加构造前安装的可选 typed
选举 observer，不改算法、参数、磁盘格式、默认传输或 PreVote 行为。Cargo.lock
中的 openraft、openraft-multi、macros 和 runtime 一并使用同一 Git 修订；不修改
依赖缓存或伪造 vendor 适配。详见[源事实说明](specs/election-source-facts.zh-CN.md)
和配套[原生 PR](https://github.com/2nterdigital/openraft/pull/1)。后续源更新须同时
固定两个 git revision，重跑 workspace 与 acceptance。
