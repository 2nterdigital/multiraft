# multiraft

面向撮合高可用的 **薄 Multi-Raft** 库：每交易对一个 Raft Group、节点间连接复用、FSM 可插拔。

基于 [openraft](https://github.com/databendlabs/openraft) + `openraft-multi`（精确锁版本）。本仓一期交付运行时与 Demo；二期（可选，在下游应用中）接入 RMQ Leader propose 与可插拔撮合 FSM。

**许可：** [Apache License 2.0](LICENSE)  
**English：** [README.md](README.md)  
**Wiki：** [中文](docs/wiki/zh/Home.md) · [English](docs/wiki/en/Home.md)  
**技术亮点：** [中文](docs/spotlight/2026-07-hotpath-sync1.zh-CN.md) · [English](docs/spotlight/2026-07-hotpath-sync1.md)

---

## 特性

- 同进程多 Group；peer 连接 **O(节点)**，非 O(Group)
- `MultiRaft`：`propose` / `propose_batch` / `read_linearizable` / 归一化 Group 观察 / 领导权回调
- 每 Group 文件持久化（可重启恢复）；sync 档位 **0/1/2**（与 Aeron 对齐）
- Aeron 启发式热路径：类型化进程内 RPC、流水线 propose、合并 / 流式落盘
- Standby learner 与成员变更，以及仅实验室使用的 snapshot catalog/checksum/ad 生成；实时 Standby 恢复已禁用
- 多进程 gRPC Demo + Admin HTTP（验收 / Jepsen）
- acceptance、chaos、porcupine、本地 Jepsen

### 与 Aeron Cluster / Standby Premium

multiraft **不是** Aeron 的 fork。它是基于 openraft（Apache 2.0 / Rust）并借鉴 Aeron 热路径思路的 Multi-Raft 库，不做 Media Driver、SBE 或商业 Cluster。

- **本机实测（3 voter、进程内）：** mem 墙钟 **~300k+** TPS；file sync=0 深流水线 **~117k–187k**；顺序 file **~2k**；sync=1 顺序 **~25 TPS**，深流水线 + 大 pe **~15–25万**（见 [技术亮点](docs/spotlight/2026-07-hotpath-sync1.zh-CN.md) / [M4](docs/specs/2026-07-22-sync1-disk-pipeline-merge.zh-CN.md)）。
- **选 multiraft** 做嵌入式 Rust/openraft 撮合 HA；**选 Aeron 商业版** 要 Media Driver、完整 Archive、ClusteredService 与 Real Logic 支持。

完整对照：[docs/compare/aeron-commercial.zh-CN.md](docs/compare/aeron-commercial.zh-CN.md) · [English](docs/compare/aeron-commercial.md)。  
性能上限：[docs/perf.zh-CN.md](docs/perf.zh-CN.md) · [English](docs/perf.md)。

---

## 架构

```text
multiraft-demo → multiraft-net (MultiRaft + 共享 gRPC)
                    ├── multiraft-core
                    ├── multiraft-fsm
                    └── multiraft-store
```

完整说明：[docs/ARCHITECTURE.zh-CN.md](docs/ARCHITECTURE.zh-CN.md) · [English](docs/ARCHITECTURE.md) · Wiki [架构](docs/wiki/zh/Architecture.md)

### 依赖锁定

| Crate | 版本 |
|-------|------|
| `openraft` | `=0.10.0-alpha.30` |
| `openraft-multi` | `=0.10.0-alpha.30` |

见 [docs/upstream.zh-CN.md](docs/upstream.zh-CN.md) · [English](docs/upstream.md)。

---

## 快速开始

```bash
git clone https://github.com/lanpishu6300/multiraft.git
cd multiraft
export PATH="$HOME/.cargo/bin:$HOME/bin:$PATH"
cargo test --workspace
./scripts/run_demo_cluster.sh
```

端口与 Admin API 见 [快速开始](docs/wiki/zh/Getting-Started.md)。

### Standby 运维（可选）

> **仅限实验室：** Admin HTTP **无鉴权**，绑定 `127.0.0.1`。勿在无鉴权网关前将 `/admin/*`、`/snapshots/*` 暴露到不可信网络。

```bash
STANDBY=1 ./scripts/run_demo_cluster.sh
curl -s http://127.0.0.1:21100/admin/groups/0/status
curl -s -X POST http://127.0.0.1:21100/admin/standby_snapshot/0
curl -s http://127.0.0.1:21103/admin/catalog/0
curl -s http://127.0.0.1:21103/groups/0/stale
```

该实验流程只覆盖 learner 成员关系和 catalog/checksum/ad 生成。catalog 不是 current snapshot provider；实时 HTTP/ad/catalog/daisy 恢复端点返回类型化 unsupported。Voter 恢复仍使用正常 OpenRaft recovery。被撤回的 `StandbyOffload` 恢复路径曾采用 precheck -> tail apply -> 仅 FSM restore -> divergence，因此不是 v1 契约。未来实时恢复须有单独 Accepted 的完整 envelope、原子捕获、Vote/完整 `LogId`/membership 和 `install_full_snapshot`。

### 一致性（每 Group）

| API | 模型 |
|-----|------|
| `propose` Ok | Linearizable 写 |
| `read_linearizable` | Linearizable 读 |
| `read_stale` | 本地 + applied 水位（`enable_stale_queries`） |
| `with_fsm` | 本地 / 可能 stale（调试 / 指标） |
| `observe_group` | 仅本地控制面观察；latest/coalescing |

详见 [docs/jepsen.zh-CN.md](docs/jepsen.zh-CN.md) · [English](docs/jepsen.md)。

---

## 验证

```bash
./scripts/acceptance.sh
SCENARIO=standby ./scripts/chaos.sh
STANDBY=1 ./scripts/run_jepsen.sh
./scripts/test_all.sh
```

大重建前建议清理 `target/`。

---

## 下游集成（二期）

```text
一期（本仓）         → 运行时 + Demo + 一致性测试
二期（下游应用）     → 可选 RMQ Leader propose → 可插拔撮合 FSM
```

### 工厂注入 FSM

下游应用可通过 `StateMachineFactory<S>` 构造自己的本地 FSM 类型。工厂收到的
`FsmFactoryContext` 包含本地节点和 Group 的标识：

```rust
let runtime = MultiRaft::<MyFsm>::start_with_factory(config, |context| {
    MyFsm::open(context.node_id(), context.group_id())
}).await?;
```

`MyFsm::open` 返回 `anyhow::Result<MyFsm>`。工厂构造是同步、轻量且非阻塞的：
应在启动前准备共享依赖，工厂中不得启动网络工作、不可逆副作用或后台任务。
`CounterFsm` 仍是 Demo / 默认路径。

公开的工厂构造器为 `MultiRaft::start_with_factory`、
`MultiRaft::start_cluster_with_factory`、`MultiRaft::start_grpc_with_factory`
和 `SharedFabric::start_node_with_factory`。

每个成功结果都是一个本地 Group 单独拥有的 `S`。工厂创建并非 exactly-once：
工厂或其他发布前构造失败后，以及进程重启后，都可能再次请求创建。不同
`(node_id, group_id)` 键的调用可以并发；在 lifecycle 工作落地前，调用方必须
串行化同一键的 `create_group` 调用。

工厂没有回滚回调。工厂错误，或工厂返回后在 registry 插入前发生的 FileLog/Raft
构造失败，都会使本地 Group 保持未发布。drop-counter 覆盖仅证明：在已测试的默认
`NodeRole::Voter` 且 `SnapshotMode::Disabled` 的 FileLog 打开失败路径中，返回的 FSM
会及时释放。该覆盖明确不包含 `StandbyOffload`：当前 state-machine-store/trigger/holder
强引用环可能保留 FSM，因此不能保证及时释放。工厂必须避免不可逆副作用，并且在该
已证明路径之外不得依赖及时 drop。相反，`try_initialize` 在发布后运行，可能在本地
Group 已发布时返回错误。

### 归一化 Group 观察

`MultiRaft::observe_group(group)` 返回一个初始 `GroupObservation` 和一个单 owner 的
`GroupObservationReceiver`。它只是对该本地 Raft 实例 OpenRaft `server_metrics()` 的
无状态只读 adapter；不使用 full `metrics()`、`data_metrics()`、FSM 状态、Standby
恢复状态、transport diagnostics，也不建立第二套 HA 状态。

`GroupObservation` 暴露 `group_id`、`local_node_id`、`local_membership_role`、
`server_state`、`leader_hint`、`flushed_vote`、`effective_membership` 与
`committed_membership`。membership 分开保留 effective/committed，两者的 joint voter
config 保持嵌套集合形状，membership log id 完整保留 `{term, node_id, index}`。节点
地址被刻意省略。

receiver 继承底层 watch 语义：latest value wins，中间控制面状态可被合并，没有
history/replay，也没有 `current()`。关闭时返回类型化 `ObservationClosed`；shutdown
或同 ID 重启后，旧 receiver 不会自动 rebind，调用方必须在新的 `MultiRaft` instance
上重新订阅。

leader 与 role 只是 observation。它们可作为路由 hint 和 desired-mode 输入，但不是
写权限、读权限、availability、quorum、lease、epoch、generation 或 health 的证明。
写入仍由 `propose` 结果校验，linearizable 读仍由 ReadIndex 校验。`on_leader_change()`
继续作为既有 best-effort compatibility callback 保留。

---

## 文档（双语）

`docs/` 下运维与设计文档均成对提供英文 `foo.md` 与中文 `foo.zh-CN.md`（标题下有语言切换链接）。Wiki 已在 `docs/wiki/zh/` 与 `docs/wiki/en/` 双语维护。

| 文档 | 说明 |
|------|------|
| [docs/README.zh-CN.md](docs/README.zh-CN.md) · [English](docs/README.md) | 索引（EN \| 中文列） |
| [docs/ARCHITECTURE.zh-CN.md](docs/ARCHITECTURE.zh-CN.md) · [English](docs/ARCHITECTURE.md) | Crate 边界 |
| [设计规格（中文）](docs/specs/2026-07-18-multiraft-design.zh-CN.md) · [English](docs/specs/2026-07-18-multiraft-design.md) | 设计 |
| [热路径 / 设计理念](docs/specs/2026-07-21-aeron-inspired-hotpath-design.zh-CN.md) · [English](docs/specs/2026-07-21-aeron-inspired-hotpath-design.md) | Aeron 启发热路径 |
| [对标商业 Aeron](docs/compare/aeron-commercial.zh-CN.md) · [English](docs/compare/aeron-commercial.md) | 宣传与技术对照 |
| [Wiki 首页](docs/wiki/zh/Home.md) · [English](docs/wiki/en/Home.md) | Wiki |
| [docs/perf.zh-CN.md](docs/perf.zh-CN.md) · [English](docs/perf.md) | 性能 / 压测 |
| [CONTRIBUTING.zh-CN.md](CONTRIBUTING.zh-CN.md) · [English](CONTRIBUTING.md) | 贡献指南 |
| [SUPPORT.zh-CN.md](SUPPORT.zh-CN.md) · [English](SUPPORT.md) | 支持渠道 |
| [SECURITY.zh-CN.md](SECURITY.zh-CN.md) · [English](SECURITY.md) | 安全报告 |
