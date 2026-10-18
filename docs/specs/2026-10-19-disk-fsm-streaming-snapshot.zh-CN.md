# 落盘状态机的流式快照

**English:** [2026-10-19-disk-fsm-streaming-snapshot.md](./2026-10-19-disk-fsm-streaming-snapshot.md)

状态：已为 ech0 #190 实现（长期容量计划第 1 步，父票 ech0 #184），建立在
[落盘 FSM 模式](./2026-10-18-disk-backed-fsm-mode.zh-CN.md)（#186）之上。

## 目标

落盘 Group（每个 Slot 一个 FSM RocksDB）可以压缩 Raft 日志；所需日志已被裁剪的副本通过快照追上。快照大小不设上限（没有 64 MiB 限制）；发送端和接收端内存只受一个传输分块约束，与快照总大小无关。任何失败都 fail closed：接收端保持原状态，不暴露半装状态。

## 设计

### OpenRaft 看到的内容

`TypeConfig::SnapshotData` 仍是 `Cursor<Vec<u8>>`（整块模式不变）。落盘 Group 的这段字节是一份 **manifest**：魔数前缀加 JSON，内容为 Group、`SnapshotMeta`（截止点与成员）、checkpoint 文件列表（名称与大小）以及发送端本地 checkpoint 路径。落盘快照 id 以 `disk-v1-` 开头；只有同时具备前缀和魔数的载荷才按 manifest 处理。manifest 只有几 KB，不触碰任何整块上限；checkpoint 文件从不经过这些路径。

### 生成快照（压缩）

需要 `SnapshotMode::NativeDurable`（与整块 native 快照一样手动压缩），且 FSM 实现 `DiskState` 的快照钩子。`DiskFsmDb` 全部实现；未实现的 FSM 保持原有拒绝（`UnsupportedCapture`），日志永不裁剪。

1. 在桥接层的 FSM 锁内，`DiskState::checkpoint` 把 RocksDB checkpoint（WAL 同步、memtable flush、硬链接）写到 `{fsm}.snapshots/tmp-*`。持锁保证 checkpoint 中的 applied 记录等于桥接层的 applied 位置；持锁时间以一次 flush 为界。
2. 释放锁后，builder 对文件 fsync 并列出，把目录改名为 `checkpoint-{index}-{nonce}`，最后写 manifest（`…manifest`，原子替换），并删除更旧的 checkpoint。已发布的较新 checkpoint 胜过较旧的捕获（与 native 规则相同）。
3. 随后 OpenRaft 把日志裁剪到截止点。此时活库已持久到截止点（checkpoint 做了 flush），满足 10-18 规格要求的“裁剪前先让 FSM 落盘”。

工作区 `{data_dir}/fsm-rocksdb/group-{id}.snapshots/`（FSM 目录的同级目录，同一文件系统）与整块 native 目录分开；落盘模式下 native 目录保持为空。

### 传输（仅 gRPC）

发送端（`GrpcRouter::full_snapshot`）识别 manifest 后：

1. 租用 checkpoint：硬链接到 `checkpoint-….send-{nonce}`，并发压缩删除已发布目录也不会让传输中途丢文件。
2. 发送 `Begin{transfer, manifest}`，随后按顺序以不超过 1 MiB 的 `/raft/snapshot_chunk` 调用发送每个文件；每个文件的最后一块携带读取时计算的该文件 SHA-256。
3. 在 `/raft/snapshot` 上把 manifest 作为 OpenRaft 快照提交。

每次调用各自使用 native 安装超时作为截止时间，因此约束传输的是进度而不是总大小。节点仍只有一个自有的出站快照槽；OpenRaft 放弃等待后，自有任务会在分块之间停止。进程内 router 拒绝 manifest（“disk snapshots require the gRPC transport”）。

### 接收与校验（传输入口）

接收端每个 Group 只暂存一个传输，位于 `staging-{nonce}`：分块必须按序到达，不得超过 manifest 中的大小，下一个文件开始前当前文件摘要必须匹配。新的 `Begin` 替换未完成的传输，但不替换最近 60 s 内已在入口校验通过的传输（它可能即将被安装，替换会让安装致命失败）；这样的 `Begin` 被拒绝，发送端稍后重试。对已校验 manifest 的重复提交直接接受，不重新校验。`Begin` 携带完整 manifest（最多 32 MiB，约可容纳 10 万个文件）；发送端拒绝超过该值或超过快照消息上限的 manifest。`/raft/snapshot` 交给 OpenRaft 之前，服务端检查：

- **摘要**：每个文件完整到达且 SHA-256 匹配；
- **布局**：`DiskState::inspect_checkpoint` 只读打开暂存的 checkpoint，要求列族、schema、Group 与应用格式标记都与本库一致；
- **applied index / epoch**：checkpoint 自带的 applied 记录与快照元数据的 last log id（index **和** term）及成员完全相同；manifest 中的元数据与 RPC 元数据相同。

这里的任何失败都是传输层拒绝（`FAILED_PRECONDITION`），OpenRaft 稍后重试；它不会进入 `install_snapshot`，那里的存储错误会让 Group 停止。

### 安装（原子替换）

`install_snapshot` 要求该 manifest 的暂存传输已在入口校验通过，拒绝让 applied 位置回退，屏蔽读（validation `begin`），然后：

1. 持久写入 `installed` 标记（截止点的 log id）。
2. `DiskState::install_checkpoint`：`DiskFsmDb` 关闭活库，经工作区做两次改名（`live → retired-N`、`staging → live`），每次后同步两个父目录，然后重新打开，且从不创建数据库。任何错误都把旧目录改名回去（若这次改名也失败，则让活路径保持缺失以待恢复），执行替换恢复，只重新打开已存在的库；若都打不开，库保持关闭，之后每次调用都报错。替换之前的失败（工作区、标记、所有者关闭）会删除暂存并重新开放读取。
3. 从换入的库重新读取 applied 位置并与元数据核对，执行恢复校验钩子，在安装截止点发布一个本地 checkpoint（接收端的当前快照），然后重新开放读取。

崩溃恢复在 `DiskFsmDb::open` 中、RocksDB 打开之前进行：活库缺失且存在 `retired-*`，说明崩溃发生在两次改名之间，于是把 retired（旧库）改名回来；存在 `retired-*` 时，没有 `CURRENT` 的活目录不是换入的库，同样被移开；活库存在说明替换已完成，`retired-*` 是垃圾。工作区残留（`tmp-*`、`staging-*`、租约、不完整的 checkpoint）在桥接层打开工作区时删除。

替换完成后、OpenRaft 裁剪日志前崩溃，会让 FSM 超前于本地日志尾。启动基线检查只在 `installed` 标记恰好等于 FSM 的 applied 位置时接受这种情况，若没有更新的 checkpoint 就在该位置发布一个，并让 OpenRaft 裁剪到该位置（已安装状态是已提交的）。其他任何“FSM 超前于日志”的情况仍然 fail closed。

## 上限

整块模式的每个 64 MiB 上限保持不变。落盘快照从不让大块字节经过这些位置：

| 上限位置（ech0 长期规格 §4） | 落盘模式 |
|---|---|
| `multiraft-core` `ClusterConfig::max_snapshot_bytes`（1..=64 MiB） | 只作用于 manifest（KB 级） |
| `sm_bridge/native.rs` `max_snapshot_bytes` / `snapshot_byte_limit` | 只作用于 manifest；从不比较 checkpoint 大小 |
| `snapshot_catalog/native.rs` `BYTE_LIMIT` | 落盘模式不使用 catalog |
| `router.rs`（进程内） | 拒绝 manifest；落盘模式只走 gRPC |
| `api.rs`、`grpc/server.rs`、`grpc/router.rs`、`snapshot_send.rs` | 只承载 manifest 消息；文件走 ≤1 MiB 的 `/raft/snapshot_chunk` 调用 |

## 风险与限制

- 同一 Group 混用两种模式仍不受支持（整块 leader 不能喂落盘 follower，反之亦然）。
- 恢复校验钩子在替换之后执行，因此校验拒绝会在新库已持久的情况下停止 Group（新库是已提交状态）。
- OpenRaft 跳过安装（截止点已在本地提交）时，已校验的暂存目录会留在磁盘上，直到下一次传输或重启。
- 发送端租约、接收端暂存和已发布 checkpoint 各自可能占用一份快照的磁盘空间（硬链接与活库共享 SST 数据，直到压缩重写）。
- `inspect_checkpoint`（入口）和 `install_checkpoint`（关闭、两次改名、重新打开）在持有桥接层 FSM 锁时于 Tokio 工作线程上做阻塞的 RocksDB/文件系统 IO（FSM 位于该异步互斥锁内；移到阻塞线程是后续工作，与 apply 相同）。

## 验证

- `multiraft-store` 单元测试（`disk_snapshot`）：manifest 校验、替换回滚/提交与崩溃恢复、工作区清理与保留、并发重新发布下的分块往返、摘要不匹配。
- `multiraft-store` `tests/disk_snapshot.rs`：构建后重启仍提供 manifest；流式安装替换状态并在 FSM 超前于日志时安全重启；拒绝路径（损坏分块、部分传输、伪造元数据、未校验安装、异种布局）保持旧状态；注入替换失败保持旧库。
- `multiraft-net` `tests/disk_snapshot_catchup.rs`：三个 gRPC 节点；写入与压缩期间停机的副本通过流式 checkpoint 追上并可重启，注入安装失败时保持旧状态直到干净重试。
- `scripts/slot_acceptance.py --case disk-snapshot`（三个 OS 进程）：场景与记录的数字见[槽位验收](../slot-acceptance.zh-CN.md)。

## 记录的运行

`scripts/slot_acceptance.py --case disk-snapshot`，`multiraft-slot-demo` release 构建，一台 macOS（APFS）机器上三个本地
OS 进程，2026-10-19。从接收端启动到追上，每 50 ms 用 `ps -o rss` 采样三个进程的 RSS。“之前”是接收端启动前发送端的 RSS
（写入阶段遗留的内存：memtable、默认 64 MiB memtable 的共享 Raft 日志 RocksDB、分配器缓存）。

| 写入该 Slot 的数据 | 快照（checkpoint 字节、文件数） | 追赶耗时 | 接收端 RSS 峰值 | 发送端 RSS 之前 → 峰值 | 第三节点峰值 |
|---|---|---|---|---|---|
| 1 GiB | 1,073,819,485 B，24 个文件 | 9.3 s | 60.0 MiB | 177.4 → 181.7 MiB | 167.0 MiB |
| 2 GiB | 2,147,601,268 B，40 个文件 | 48.1 s | 60.7 MiB | 176.8 → 178.8 MiB | 153.5 MiB |

同一运行的后续轮次分别流式传输了 1.34 GB / 1.41 GB（1 GiB 运行）和 2.68 GB / 2.75 GB（2 GiB 运行）的快照。故障轮次：接收端在
暂存 72 MB（1 GiB 运行）/ 71 MB（2 GiB 运行）后被 SIGKILL，以旧 applied index（259 / 514）重新打开后追上；在两次改名之间中止
（退出码 -6，活目录缺失、存在一个 `retired-*`）后，回滚并以旧 applied index（324 / 642）重新打开，然后追上。峰值不随快照大小增长：
流式传输只给发送端增加几 MiB（一个分块加 gRPC 缓冲）；接收端从新进程开始，保持在约 60 MiB。
