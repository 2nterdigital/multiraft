# 架构说明

**English：** [ARCHITECTURE.md](./ARCHITECTURE.md)

面向撮合高可用的薄 Multi-Raft 运行时。每个交易对一个 Raft Group
（`GroupId`）；节点间连接复用；FSM 可插拔。基于
`openraft` + `openraft-multi`（精确锁版本）。

## 阶段规则

1. **一期（本仓）：** 库 + 多进程 Demo + chaos / Jepsen。
   不接 RocketMQ，不接撮合引擎 FSM。
2. **二期（下游应用）：** 可选 Leader 消费 RMQ → `propose`；可插拔撮合引擎 FSM。
   Follower 不消费入站。
3. 不要把撮合 DTO / RMQ 拉进 `multiraft-*` crates。

## Crate 职责

```text
crates/
├── multiraft-core/   # TypeConfig, ClusterConfig, MultiRaftError, ProposeOk
├── multiraft-net/    # Shared GroupRouter / GrpcRouter + MultiRaft facade
├── multiraft-fsm/    # StateMachine trait (apply / snapshot / restore)
├── multiraft-store/  # Per-group file-backed log / state / snapshot
└── multiraft-demo/   # 3-node × N-group CounterFsm + admin HTTP
```

| Crate | 做 | 不做 |
|-------|------|----------|
| `multiraft-core` | 共享类型 / 错误 | 网络、存储 |
| `multiraft-net` | `MultiRaft` API、O(nodes) 连接、通用 FSM 工厂注入 | 业务类型选择、命令语义、持久化业务元数据、业务 registry 或 discriminator |
| `multiraft-fsm` | Trait + demo `CounterFsm` | 依赖撮合引擎 FSM |
| `multiraft-store` | 每 Group 持久化 | 订单簿 |
| `multiraft-demo` | 验收 / Jepsen 靶标 | 生产部署 |

## 拓扑

```text
                    ┌──────────────────────────────────────┐
  Admin HTTP        │  OS process = one Raft node          │
  (per node)        │  MultiRaft + N groups (shared gRPC)  │
                    └───────────────┬──────────────────────┘
                                    │ tonic / openraft-multi
                    ┌───────────────┼──────────────────────┐
                    ▼               ▼                      ▼
                 node-1          node-2                 node-3
              groups 0..N-1   groups 0..N-1          groups 0..N-1
```

- `--mode node`：每个 Raft 节点一个 OS 进程（贴近生产形态）。
- `--mode cluster`：进程内 3 逻辑节点，便于快速测试。
- Peer 连接：**O(nodes)**，非 O(groups)。`unique_peer_links()` 暴露该指标。

## 数据流（二期目标）

```text
RMQ (per-symbol)
  → [Leader only] validate → propose(group, cmd)
  → openraft quorum commit → FSM.apply on all replicas
  → [Leader] egress / ack RMQ after commit+apply
```

一期 Demo 本地注入 `propose`（`POST /groups/{id}/inc` 或后台循环）。

## Consistency Contract（每 Group）

| API | 模型 |
|-----|--------|
| `propose` → Ok | Linearizable 写（已 commit + applied） |
| `read_linearizable` | Linearizable 读（ReadIndex） |
| `read_stale` | 本地 + applied 水位；需 `enable_stale_queries`（Standby 卸载） |
| `with_fsm` | 本地 / 可能 stale — 调试 / 指标 |
| Cross-group | 无跨 symbol 事务 |

失败 / 超时的 `propose` 结果**不确定** — 须用同一幂等键重试。

详情：[specs/2026-07-18-multiraft-design.md](./specs/2026-07-18-multiraft-design.md) · [中文](./specs/2026-07-18-multiraft-design.zh-CN.md) §4.3.1，
[jepsen.md](./jepsen.md) · [中文](./jepsen.zh-CN.md)。

## 下游集成（二期）

```text
撮合进程 / 入站壳（RMQ consumer, Leader only）
  → multiraft::MultiRaft (propose / leader callbacks)
    → FSM 适配器 → 撮合引擎 FSM
```

### 通用 FSM 工厂与生命周期边界

`multiraft-net` 负责通用注入机制，而不负责业务类型选择、业务命令语义、持久化
业务元数据、业务 registry 或 discriminator。应用提供 `StateMachineFactory<S>`；
每个由 `FsmFactoryContext` 调用的工厂成功返回的结果，都是一个本地 Group 单独拥有的
`S`。`CounterFsm` 仍是 Demo / 默认路径。

工厂是同步、轻量且非阻塞的。它不得执行网络工作、启动不可逆副作用或后台任务。
工厂构造并非 exactly-once：工厂或其他发布前失败后，以及进程重启后，调用可能
重复；不同 `(node_id, group_id)` 键的调用可以并发。在单独的 lifecycle 工作落地前，
调用方必须串行化同一键的 `create_group` 调用。

工厂没有回滚回调。工厂错误，或工厂返回后 registry 插入前发生的 FileLog/Raft
构造失败，都会使 Group 保持未发布；返回的 FSM 会被 drop，必须安全释放其资源。
registry 插入后，`try_initialize` 仍可能在 Group 已发布时返回错误。这个边界不定义
snapshot restore、业务 store 协调或任何 Group 生命周期修复。

## Standby 异步快照

可选 `SnapshotMode::StandbyOffload`：voter 在 `build_snapshot` 中不再同步 dump FSM。
**Standby**（openraft Learner）应用魔术 trigger 日志，短暂 freeze FSM，再经
`spawn_blocking` 写入 `{data_dir}/snapshots/` 持久 catalog。voter 恢复时按广告拉取。

详情：[specs/2026-07-20-standby-async-snapshot-design.zh-CN.md](./specs/2026-07-20-standby-async-snapshot-design.zh-CN.md)
· [English](./specs/2026-07-20-standby-async-snapshot-design.md)。

Premium 对等（从 ad HTTP 拉取、standby 复制限速、promote/demote、多 Standby 选最新 ad、
经 `daisy_upstream_base` 的**快照 daisy-chain**、HTTP Range 分块续传、Standby `read_stale`）：
[specs/2026-07-20-aeron-standby-parity-design.zh-CN.md](./specs/2026-07-20-aeron-standby-parity-design.zh-CN.md)
· [English](./specs/2026-07-20-aeron-standby-parity-design.md)。

P2 daisy 是**快照分发链**（不是 openraft log 重定向）。
P3 `read_stale` 明确为非 linearizable。

## 上游锁定

| Crate | Version |
|-------|---------|
| `openraft` | `=0.10.0-alpha.30` |
| `openraft-multi` | `=0.10.0-alpha.30` |

见 [upstream.md](./upstream.md) · [中文](./upstream.zh-CN.md)。
