# 上游版本锁定说明

**English：** [upstream.md](./upstream.md)

## 锁定版本

Workspace `Cargo.toml` 锁定：

```toml
openraft = { git = "https://github.com/2nterdigital/openraft.git", rev = "b5f3dc1a4df74891d4a8236b9b35af90158dbb52", version = "=0.10.1", default-features = false, features = ["serde", "type-alias", "tokio-rt"] }
openraft-multi = { git = "https://github.com/2nterdigital/openraft.git", rev = "b5f3dc1a4df74891d4a8236b9b35af90158dbb52", version = "=0.10.1" }
```

两个 crate 均使用**精确**版本要求（`=`）。未经刻意升版并复测 Demo + `./scripts/acceptance.sh`，不要放宽为 `^` / `~`。

## 为何锁定

`openraft-multi` 以及 `multiraft-net` 所用的 Multi-Raft API（`MultiGroup`、共享网络 / router 模式）跟随 **0.10** 线，patch 版本曾改动 type alias、feature flags 与示例布局。将二者钉在同一源修订可避免：

- Cargo 意外解析到更新的不兼容版本
- 同一 workspace 内 `openraft` / `openraft-multi` 版本漂移
- 一期进程内 `GroupRouter` + Demo 验收被静默破坏

## 参考示例

上游多 Group KV 示例（同一发布列车）：

- [openraft `examples/multi-raft-kv`](https://github.com/datafuselabs/openraft/tree/v0.10.1/examples/multi-raft-kv)

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

## Fork 补丁

两个 workspace crate 都解析到 `https://github.com/2nterdigital/openraft.git`
的 `b5f3dc1a4df74891d4a8236b9b35af90158dbb52`（分支 `claude/v0.10.1-fork`）。
该修订是上游 tag `v0.10.1`（`6d3ede17`）加上从此前 alpha30 fork（`8f0b2b48`）
移植的三处补丁：

- 构造前安装的可选 typed 选举 observer（见[源事实说明](specs/election-source-facts.zh-CN.md)）；
- 带守卫的单步成员变更与构造策略；从内置单一 `Membership` 出发的带守卫变更一律先追加 joint；
- `Config::broadcast_commit`（默认 `true`）；设为 `false` 时只有 commit 前进不再单独发空 AppendEntries。

Cargo.lock 中的 openraft、openraft-multi、macros 和 runtime 一并使用同一 Git 修订；
不修改依赖缓存或伪造 vendor 适配。后续源更新须同时固定两个 git revision，重跑
workspace 与 acceptance。

与 alpha30 节点的线上和磁盘兼容由 multiraft 一侧保证：3 字段的 `SnapshotMetaWithId`
继续写非空快照 ID；旧版 `VoteRequest` 布局作为回退解码；append 应答在线上仍是
`AppendEntriesResponse`。`pre_vote` 与 alpha30 一样在本地授票，不上线路。
