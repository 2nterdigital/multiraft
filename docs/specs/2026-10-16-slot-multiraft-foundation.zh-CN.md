# Slot MultiRaft 底座设计

**English：** [2026-10-16-slot-multiraft-foundation.md](./2026-10-16-slot-multiraft-foundation.md)

**日期：** 2026-10-16
**状态：** 设计已采纳，底座实施已授权、实施中；本文尚未记录底座验收通过结果。

本轮交付通过新通用 Slot API 服务多个固定 Slot 的三进程 RF3 可运行底座，使用 RocksDB 共识日志和具有快照/恢复能力的最小确定性测试 FSM。生产 ech0 Admit/TiKV 接入、旧数据迁移、在线 reshard 和容量认证留待后续。[可行性评估](./2026-10-16-slot-multiraft-feasibility.zh-CN.md)保留固定源码事实；[实施计划](../plans/2026-10-16-slot-multiraft-foundation.zh-CN.md)规定实施与证据门槛。

## 1. 基线与兼容性

从 C `28e668b676c803838eb64a04ba160fb10ad88316` 出发，审查吸收 M `44baed14b3717b6b6f5f7327790cc97299889e19` 的必要通用改进，包括 descriptor/shared restart 和 Jepsen portability。专项移植 R `d6a5dbbb6d2f1c68e9e8bddbbfca18f562c1185c` 的 RocksDB 日志，保留 C 的 runtime 合同。维持 OpenRaft fork 精确 pin，依赖变化须明确记录。

保留 C 的公开 FSM factory、owned runtime、精确 applied effects、绝对 deadline、dispatch 分类、每次独立原生 ReadIndex，以及异步 recovery proof/readiness gate。C/M/R 的历史检查只证明各自记录的源码，不证明整合树。

采用独立新 data 目录，旧数据原样保留。新 Slot API 不承诺自动兼容旧 API、FileLog WAL、ech0 OpenRaft 0.9 数据或旧 snapshot。存在 RocksDB marker 但 DB 丢失时 fail closed，不能静默新建空历史。不支持的后端切换须另有迁移过程。

## 2. 职责

| 组件 | 职责 |
| --- | --- |
| `multiraft-slot`（新 crate） | 稳定分区映射、规范 manifest 与磁盘绑定、服务端路由校验、组生命周期编排和有界请求 |
| MultiRaft / OpenRaft | 共识、原生组/成员、Raft 传输、log/FSM 驱动、ReadIndex 与 recovery proof/readiness gate |
| RocksDB 日志后端 | 按组命名空间的 node-local 共识日志与 metadata、耐久 callback 所有权和 DB 生命周期 |
| 测试 FSM | 确定性测试命令/结果、重复处理、查询及完整 snapshot/restore，用于底座验证 |
| 未来 ech0 Admit FSM | 原子 dedup/seq/accepted head/pending/result，以及 flush 进度和 stop 状态 |
| 未来 ech0 adapter / TiKV | 资格输入、长期消息事务 storage、投递、collector 与业务迁移合同 |

RocksDB 不是全部业务 FSM 状态库。复制 FSM 权威由 committed log + 已验证 snapshot 和 applied 位置恢复。effects 是响应，不是另一份耐久事实库。TiKV 继续承担长期消息事务 storage。

未来 SEND 的异步投递与异步 TiKV 持久化是接纳后并行路径。SENDACK 表示接纳 committed + applied，不认证 TiKV commit 或 RECV。本底座不引入替代生产 SEND 路径。

## 3. 固定布局与身份

`businessPartitionKey` 是 opaque bytes，由 caller 提供稳定业务归一和编码；库不解析 Channel/User。manifest 固定 schema version、cluster identity、slot_count、hash version/参数、Slot/Group 映射、静态副本身份及配置 digest。服务前必须按 manifest 重建全部本地组。runtime peer hints 是路由信息，不是执行权，也不能取代 committed membership。

v1 使用正数固定 slot_count，每 Slot 一个 Group。SlotId 范围为 `0..slot_count-1`，GroupId 为 `group_id_base + slot_id`；group_id_base 绑定 manifest digest，并检查加法溢出。测试推荐 base 为 1，预留 GroupId 0。不引入 split、merge、在线重映射或跨 Slot 事务。4 或 8 Slot 可作测试 fixture，不是生产容量建议。

当前 Slot 配置实现使用已有 SHA-256 依赖，定义以下 `sha256-v1` bytes 合同（测试执行仍待完成）：

```text
H = SHA256(UTF8("multiraft-slot/key/sha256-v1\0") || u64_be(key_length) || key)
slot_id = u64_be_decode(H[0..8]) mod slot_count
```

末尾零是单个零字节；长度按 bytes 计，整数为无符号大端，空 key 长度为零。此版本无可变 seed。manifest 保存精确 hash version 和映射参数；禁止 DefaultHasher。已知向量和跨重启映射测试须绑定此实现。改变这些 bytes 必须新增不兼容 hash version，不能当作普通重构。

manifest 的规范 codec/digest 须规定字段顺序、长度、整数编码及副本排序。当前 layout digest 使用 SHA-256 domain `multiraft-slot/layout/v1\0`，绑定 format/hash version、cluster ID、slot_count、group_id_base、排序后的 node 身份及 Raft/application 地址、排序后的 Slot/voter 分配。此版本地址属于持久布局身份，变更地址须使用新数据。runtime 资源预算不属于布局身份。digest 检测不兼容，不提供认证。JSON disk manifest 另将 local_node_id 绑定到该布局。组启动前拒绝重复身份/组、非法 slot_count、非双射映射和不支持版本。data 目录持久化并核对 cluster identity、manifest digest 和本地 node identity；node 身份绑定磁盘，不能从地址推断。首次创建须耐久后才 ready；重开时配置冲突或目录绑定其他 node 均 fail closed。

每个参与 server 都核对 manifest digest，并从传入 key 重算 Slot。客户端给定 Slot 或 leader hint 不能授权另一个分区。proposal dispatch 前校验 cluster/node/group membership 和边界。跨节点错配须在误写组之前拒绝。只持久本地 digest 不证明其他副本一致；请求与启动/peer 校验必须执行该合同。

## 4. API 草案与路由

以下是语义草案，不表示这些方法名已经存在。公开类型和 adapter 细节须对照最终代码与测试调整。

```text
start(manifest, local_identity, limits, storage, fsm_factory) -> owner + handle
propose(key, command_bytes, absolute_deadline)
    -> Applied(exact_effects, position) | NotDispatched(reason) | Unknown(phase)
query(key, query_bytes, Linearizable | Stale, absolute_deadline)
    -> Result(bytes, watermark) | QueryError
shutdown(absolute_deadline) -> shutdown_report
```

测试 FSM 可在 command bytes 内携带 command ID 并持久其对应结果；通用 Slot 层不自创业务 dedup 语义。Applied 对应精确 committed + applied proposal，不是最近观察到的组 index。线性化查询使用目标组原生 ReadIndex 并满足 apply 可见性；stale 查询明确返回 applied watermark。leader 观察只是 hint，不是 fence。

任意 node 可接受入口。从 key 定位 Slot/Group、检查 ready 并获取有界容量，本地执行或向 hint leader 发起至多一次 application forwarding attempt。目标重做 manifest/key/Slot 校验。过时 hint 返回分类结果，不无限转发或自动重试 proposal。application RPC 与 Raft 传输维持独立 purpose；共享 channel pool 不意味着一个物理连接。

本地调用保留同一绝对 deadline；远端传剩余 budget，既不序列化进程本地 Instant，也不重置原预算。尽可能在 dispatch 前拒绝非法/过期预算。明确未 dispatch 的拒绝属于 NotDispatched。一旦可能 dispatch，超时/断连/取消不能声称 NothingApplied；除非掌握精确结果，否则返回 Unknown。纯 query 失败不是 write Unknown；后续结果 miss 不能证明此前 Unknown 写未提交。

## 5. 所有权、恢复与关闭

强 owner 持有已接受异步工作和关闭责任；handle 不能意外保活 runtime。caller 取消或超时只释放 waiter，不撤销已接受执行或 storage completion callback。本地 proposal、远端工作和 RocksDB blocking write 均保留所有权。实现须定义并直接测试 acceptance/dispatch 转换；本文不假定未经核对的方法名。

snapshot 恢复和日志重放受现有 recovery gate 保护。FSM 短锁内 capture proof 状态，锁外异步 proof，保留预算、取消和 panic 处理。proof 和 readiness hook 成功后才能 activate。C 要求的 pending-startup snapshot 仍在 Raft core 前非致命拒绝。purge 依赖充分、已验证的恢复证据，不能只凭响应完成。

本底座 startup/create/recovery proof 错误继承 C 的整 local node rollback，并清理失败操作已经启动的组。这不泛化为所有 runtime Raft fatal 路径。生产接入前须证明局部 Slot 故障可隔离该 Slot、健康 Slot 继续工作。共享 DB 故障和 manifest/磁盘身份错配仍是 node 故障。证据建立边界之前不宣称 runtime 局部故障隔离。

关闭先停 intake 和新 forward，有界 drain 已接受 owned 工作，再关闭组/storage 并 join owner。明确报告未完成/Unknown 工作和清理错误。waiter 完成不等于 clean shutdown。中断后恢复须区分耐久 committed 状态和未确认 caller 结果。

## 6. 资源限额

node 和 Slot 两级限制请求数量及 bytes，包括等待 dispatch/完成的 payload。队列和 forwarding/storage/snapshot 工作有界。限额由配置给定并校验，不按瞬时 heap 裁决。success、拒绝、超时、取消与关闭路径统一获取/释放容量；已接受任务继续持有尚属自己的资源。

固定 Slot 数仍有每组 Raft tasks、心跳、日志和快照成本。共享 RocksDB 不证明公平性、跨组原子性或 group-commit 提速。测试热/冷 Slot 进展、限额饱和与负载下关闭，证据记录全部限额。本文不承诺容量或单热点提速。

## 7. 后续生产接入门槛

未来有序 Admit 必须原子查询既有固定成功 dedup 结果，检查确定性准入输入、分配连续业务 seq 并更新 accepted head、pending 和 result。Raft index 不是业务 seq。保留 flush/stop、exact holder/revision、Unknown 与 fail-stop 合同。外部资格在 proposal 前读取，保留现有已接受 stale 语义；确定性 apply 不执行 TiKV/网络/时钟/随机 I/O。

须在 fence 停旧 admission 与 collector 后，全量迁移 accepted head、pending、results、flush 及匹配 persisted progress。一次读取可能滞后的 TiKV head 不能初始化已有 Channel。不能偷改为只迁 head，也不能在按 Slot 分组 collector 时放宽跨 Channel TiKV 事务语义。

当前 command results 永久保留。本轮不授权 dedup TTL/GC、只存最高 seq、新拒绝结果保留政策或其他长期 dedup 变化。显式相同 ID 重试可取回原结果；Unknown 不授权自动重试。runtime 过载仍可能在到达 FSM 前拒绝。

## 8. 底座验收

验收须运行三个独立 OS 进程、RF3、多 Slot。证据覆盖映射/digest/磁盘错配、本地/远端入口精确结果、snapshot/日志重放与全量重启、leader 丢失/切换、提交后断连产生 Unknown 及安全显式重试、取消所有权、关闭和资源压力。注入 proof 失败，保留 C rollback 行为。后续生产使用前验证 Slot 局部隔离。

记录精确源码、toolchain/features、命令、data 路径、进程身份、限额、断言和结果。C/R 历史证据分列。编译通过、进程内 RF3 测试或已写计划均不能替代本验收。执行状态与缺口写入[实施计划](../plans/2026-10-16-slot-multiraft-foundation.zh-CN.md)。
