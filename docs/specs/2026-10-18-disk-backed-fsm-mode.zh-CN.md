# 落盘状态机模式

**English:** [2026-10-18-disk-backed-fsm-mode.md](./2026-10-18-disk-backed-fsm-mode.md)

状态：ech0 #186（长期容量计划第 1 步，父票 ech0 #184）开工前冻结的决定。
该模式的流式快照属于 ech0 #190，本文不实现。

## 目标

状态机可以把状态放在它自己的 RocksDB 里，而不是内存加整块 `Vec<u8>` 快照。
每条 apply 的业务行**和** Raft applied 位置在同一个 `WriteBatch` 里原子提交。
重启时 bridge 从这个库读出 applied 位置，OpenRaft 只重放其后已提交的日志后缀，
不经过整块 restore。整块模式（`StateMachine::snapshot`/`restore`/`freeze_bounded`，
上限 64 MiB）保持不变，仍是小状态机的默认模式。

## 冻结决定：库的划分

**节点上每个 Slot（共识组）各有一个独立的 FSM RocksDB 实例，与节点共享的
Raft 日志引擎分开。**

| 数据 | 位置 | 引擎 |
|------|------|------|
| Raft 日志、vote、committed/purged（节点上所有 Group） | `{data_dir}/raft-rocksdb/` | 现有共享引擎（`LogStorageBackend::RocksDb`），不变 |
| 单个 Group 的 FSM 状态与 applied 位置 | `{data_dir}/fsm-rocksdb/group-{group_id}/` | 每个 Group 一个 RocksDB 实例，归该 Group 的 FSM 所有 |

单个 Group FSM 实例内的列族：

| 列族 | 归属 | 内容 |
|------|------|------|
| `multiraft_meta` | multiraft（保留） | `schema` = `multiraft-disk-fsm-v1`；`group` = 大端 Group ID；`app_format` = 应用格式标记；`applied` = bridge 的 applied 记录 |
| `default` 及其他名字 | 应用 | 业务行；应用打开时声明完整列族集合 |

applied 记录对应用不透明：它是 bridge 对该条目之后的 OpenRaft `LogId` 与
`StoredMembership` 的带版本编码。应用把它原样写进与业务行相同的 batch。

为什么不与 Raft 日志引擎共用，也不用"一个 FSM 引擎、每 Slot 一个列族"：

- **快照（#190）**：RocksDB checkpoint 以库为单位。每 Slot 一个实例时，
  checkpoint 恰好是一个 Slot 的状态及其 applied 记录，天然一致，靠硬链接生成。
  共享引擎会包含所有 Slot（若与日志共用还包含所有日志），快照只能做键范围导出，
  安装时还得把键范围拼进正在运行的共享库。
- **安装（#190）**：每 Slot 一个实例，才能"写临时目录、校验、关闭、原子替换目录、
  重新打开"，且不影响其他 Slot。
- **write stall**：任一列族达到 stall 条件（memtable 数、L0 文件数、待压缩字节）时，
  RocksDB 会暂停整个库的写入。若 FSM 与日志共用引擎，一个 Slot 的压缩欠账会卡住
  本节点所有 Slot 的 Raft 日志追加和 vote 写入；共享 FSM 引擎则会卡住所有 Slot 的 apply。
- **归属**：Group 的 FSM 被 drop 时关闭它的库；现有的 FSM 释放栅栏（关停等待 FSM
  实际销毁）已覆盖该资源。

Raft 日志的原子性不延伸到 FSM 库，也不需要：OpenRaft 以状态机的 applied 位置为准，
从那里重放。

## API 形态

`multiraft-fsm`（不依赖 RocksDB）：

- `StateMachine::disk_state(&mut self) -> Option<&mut dyn DiskState>`：默认 `None`，
  即整块模式；落盘 FSM 返回 `Some`。bridge 在构造时记录一次，FSM 生命周期内不变。
- `DiskState::durable_applied(&self) -> io::Result<Option<Vec<u8>>>`：返回已存的
  applied 记录（新库为 `None`）。
- `DiskState::apply_durable(&mut self, group, index, data: Option<&[u8]>,
  applied: &AppliedState) -> io::Result<ApplyOut>`：每条 apply 的条目恰好调用一次；
  blank、membership 和内部条目的 `data` 为 `None`。实现必须在返回前用**一次**原子写入
  让该条目的业务行和 `applied.record()` 落盘。落盘模式下 bridge 不调用 `apply`、
  `snapshot`、`restore` 和捕获钩子。

`multiraft-store` 的 `DiskFsmDb` 是实现上述布局的 RocksDB 辅助类型：打开（或创建）
实例，校验 schema、Group 和应用格式标记（任何不一致或未知列族都 fail closed），
供读取的应用列族句柄，以及 `commit(batch, applied)`：把 applied 记录加进调用方的
batch 并只写一次。`DiskFsmDb::group_path(data_dir, group)` 给出规范位置。rocksdb
crate 被重新导出，使用方使用同一个锁定版本。

bridge 只在 `apply_durable` 返回之后才发送该条目的客户端响应，并推进内存中的
applied 位置和 membership。

#190 计划给 `DiskState` 增加 checkpoint/导出与安装钩子，默认实现拒绝；现有实现
继续可编译，流式安装可以关闭、替换并重新打开实例。ech0 的组合 FSM（#191）基于
`DiskFsmDb` 和自己的列族实现 `DiskState`。

## 与现有模式的关系

- **`SnapshotMode` 保持正交**：落盘模式由 FSM 声明，不由集群配置声明。可与
  `Disabled`、`NativeDurable` 搭配；`StandbyOffload` 和 Standby 角色被拒绝（Standby
  路径整块热导出）。
- **`NativeDurable` + 落盘模式**：原生快照目录保持为空；捕获以 `Unsupported` 拒绝，
  所以手动压缩返回拒绝、日志从不被清理；整块构建和安装被显式拒绝。目录里已有
  整块快照会让 group 启动失败（安装被拒），因为新格式只在新数据空间启动。#190 用读取
  该 FSM 实例的流式 provider 替换这个拒绝。
- **`LogStorageBackend::RocksDb`**：仍是独立、共享、同步 WAL 的日志引擎，落盘模式
  **必须**使用它（并且 `data_dir` 非空）。内存日志和任何同步等级的 FileLog 都在 group
  启动时被拒绝。锁定的 OpenRaft 把条目 apply 到*已提交给日志 IO* 的位置，而不是已 flush
  的位置（`engine_impl.rs`，`log_submitted.min(apply_accepted)`）。RaftCore 在执行后续
  `SaveCommittedAndApply` 之前会就地 await `log_store.append`（`core/raft_core.rs`）。
  RocksDB 的 `append` 在同步 WAL 写入完成后才返回（`log_rocks/storage.rs`），因此条目在
  FSM 写入之前已在本地日志中落盘。FileLog 的 `append` 可能在合并组提交之前返回，崩溃后
  可能出现已落盘的 FSM 领先本地日志。

## 启动与失败规则

- FSM 有 applied 记录但该 Group 的 Raft 日志是 pristine（无 vote、无日志、无清理）：
  fail closed，说明这个 FSM 目录不属于这份日志。
- Raft 日志已持久而 FSM 库为空：在没有任何清理时，可从 index 1 重放恢复；日志已被
  清理时会在 OpenRaft 的重放检查（或被拒绝的安装）中失败。在 #190 之前，落盘模式
  从不清理日志。
- FSM 领先于本地日志尾部：在 OpenRaft 运行**之前** fail closed。否则 OpenRaft 会把本地
  日志清理到 applied 位置（不可逆），随后发现日志已被清理却没有快照，于是强制构建
  快照，而本模式拒绝构建，该 Group 将永远无法启动。使用 RocksDB 日志时，正常崩溃不会
  进入这个状态（见上文），因此它意味着目录不匹配或损坏。处理办法：删除该 Group 的
  FSM 目录和该节点的数据后重新加入；因为本模式从不清理日志，可以从 leader 重放日志
  追上。#190 可以让这次强制构建成功。
- 默认每次写入都同步 WAL（`DiskFsmOptions::sync_writes`）。关闭同步后 SIGKILL 仍安全
  （WAL 仍然写出）；掉电后 FSM 可能回到更早的 applied 位置，只要本地日志保留 FSM
  持久点之后的所有条目就是安全的。落盘模式不能压缩时这一点总成立；#190 必须在清理
  日志前先 flush FSM。

## 已记录的风险

- **跨 Slot 的 write stall**：每 Slot 独立实例把 RocksDB write stall 限制在一个 Slot
  的 apply 路径内。但它们仍共享磁盘带宽、RocksDB 默认后台线程池，以及应用传入的共享
  block cache 或 write-buffer manager。因此一个 Slot 持续的压缩欠账会拖慢其他 Slot
  的 flush 与压缩，最终也可能让它们 stall。apply 被卡住时，该 Slot 的线性一致读随之
  延迟，在途提议变为 Unknown。Raft 日志引擎是另一个库，FSM 的 stall 不会阻塞其他
  Slot 的日志追加、vote 或心跳；但*共享日志引擎本身*的 stall（日志压缩欠账）仍影响
  本节点所有 Slot。见 [RocksDB Write Stalls](https://github.com/facebook/rocksdb/wiki/Write-Stalls)。
- **每 Slot 的资源**：每个实例有自己的 WAL、memtable 和文件句柄。使用方应通过
  `DiskFsmOptions::db_options` 传入共享的 `Cache`/`WriteBufferManager`，并按进程文件
  句柄上限设置 `max_open_files`（macOS 默认值较低）。
- **首次创建被中断**：RocksDB 创建新 FSM 实例期间（`CURRENT` 写出之前，或各列族
  创建之间）崩溃，会留下 `open` 拒绝的目录；此时尚未 apply 任何条目。处理办法：删除
  该 Group 的 FSM 目录。
- **同一 Group 混用模式**：一个 Group 的所有副本必须在新数据空间中使用同一种 FSM
  模式。整块模式副本可能压缩并发送快照，而落盘副本会拒绝安装（该 follower 致命）。
  配置阶段不做检查。
- **apply 延迟**：每条条目的同步 FSM 写入在 bridge 的异步 apply 中、持有 FSM 锁、
  运行在 Tokio worker 上。大量繁忙 Slot 可能占满 worker 并推迟心跳；批量写入或把写入
  移到阻塞线程是后续工作。
- **不能靠快照追赶**：#190 之前，所需日志已不存在的 follower 无法追上，因此落盘模式
  从不清理日志。
- **64 MiB 上限**：ech0 长期容量规格 §4 列出的存储层和网络层快照上限这里不改。
  它们只作用于整块快照，落盘模式从不生成整块快照；为落盘模式去掉或绕开这些上限属于 #190。

## 验证

- `multiraft-store` 测试：`DiskFsmDb` 标记与 fail closed 打开；bridge 恢复 applied
  位置和 membership，不重放已持久的条目；拒绝整块构建/安装。
- `multiraft-net` 测试：三个 gRPC 节点、RocksDB 日志、落盘 FSM；重启与冷重启后从
  RocksDB 恢复状态和 applied index。
- `multiraft-slot-demo --fsm-storage disk` 配合 `scripts/slot_acceptance.py --case disk`：
  三个 OS 进程、RF3、SIGKILL leader，再 SIGKILL 全部节点并冷重启；每个节点的状态与
  applied index 一致。这些 SIGKILL 发生在空闲集群上；写入过程中的崩溃由上文的顺序论证
  覆盖，没有专门测试。现有整块用例不变。
