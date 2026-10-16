# Slot Admit 优先实施交接

**English：** [English plan](./2026-10-17-slot-admit-first-implementation.md)
**日期：** 2026-10-17
**状态：** 仅执行计划；本次未实施、构建、运行新测试、SSH 或负载实验。

## 1. 已采纳方向与替代顺序

固定 Slot + Atomic Admit 是已采纳的长期主线。
在既有 Slot 底座上实现真实 ech0 准入，再接通异步 TiKV 持久化与投递。
不要把继续优化 client/#173 或新的 Counter 演示当作前置。
不重建 Slot 底座，不要求实施两套完整的新系统。
本计划替代[旧计划](./2026-10-16-slot-channel-fair-admission.zh-CN.md)的 G0–G6 执行顺序。
尤其取消“完整 DRR 与性能 SLO 认证先于真实 Admit”的依赖。
[公平准入规范](../specs/2026-10-16-slot-channel-fair-admission.zh-CN.md)继续约束公平正确性。
固定且持久化的 Slot/hash/layout、每节点一个 RocksDB、所有权与资源硬上限仍然有效。
底座 A–F、430 passed/12 ignored 与七个验收场景仍是[历史证据](../evidence/2026-10-16-slot-foundation.zh-CN.md)。
保留原失败与未定位 observer 风险；旧收据不证明新业务正确性或性能。
里程碑 ready 仅表示对应范围的验收通过，不等于获准部署。

| 里程碑 | 步骤 | 必须交付 | 明确边界 |
| --- | --- | --- | --- |
| M1 | T0–T2 | 纯业务 FSM，真实三个 OS 进程 RF3、多 Slot 准入 | 不接生产 TiKV，不切旧数据 |
| M2 | T3–T4 | 真实 HTTP/WS、TiKV 事务与持久化证明、history/audience/delivery 闭环 | 新隔离业务 namespace；披露保守整节点故障行为 |
| M3 | T5–T7 | 必要公平/热点证据、Slot 故障隔离、observer 收敛、迁移演练与目标验收 | 就绪证据；生产切换仍是单独动作 |

首轮交接：先交 M1 报告，依赖通过后直接继续 M2。
不要在阶段之间重新征求已采纳架构的批准。
不要把 DRR 或尚未给定的性能 SLO 当作 M1 阻塞项。
本交接的实现授权不包含生产切换或改写生产 namespace。

## 2. 源码冻结与仓库所有权

| 仓库 | 起始参考 | worktree 策略 |
| --- | --- | --- |
| multiraft | `9e63b454848d18ceea4510b3f3ea0fb52af71b68`，`codex/slot-multiraft` | 继续 `/Users/wei/Documents/WorktreesSpace/slot-multiraft` |
| ech0 | main `1606f85fc787604057f2ff7829df291c9ddc857b`；代码基线仍为 `8098a4f7` | 冻结实际完整 SHA/tree，创建独立 worktree |
| ech0 编写早期 checkout 观察 | `claude/memory-docs-20261017`，`2f73a7d7` | 之后文档已合并，checkout 现为 main |
| 旧 multiraft checkout | `/Volumes/Space/2nterdigital/multiraft`，旧 `28e…` 参考 | 保持原样 |

multiraft `9e63b45` 是本计划编写前的实现/文档基线：保留承载本交接的后续文档提交，记录实际 HEAD 并验证代码等价；不要为回到该基线 reset 或删除新计划。
编辑前记录每个精确 commit、tree、dirty 状态、manifest 与 lockfile 哈希。
执行时读取实际 branch/HEAD；main `1606f85fc787604057f2ff7829df291c9ddc857b` 已包含后续文档合并。
已核对与 `8098a4f7` 的差异仅 docs/memory；Cargo.toml/Cargo.lock/crates/tools/vendor 未变，故 `8098a4f7` 源码引用仍有效。
不要 switch、reset 或 merge 共享 ech0 主工作目录。
新 worktree 统一放在 `/Users/wei/Documents/WorktreesSpace/`。
建议 ech0 分支名 `codex/admit-fsm`；执行者可选其他无冲突名称。
临时文件仅用当前 worktree `.tmp/`；本文暂存区为 `.tmp/slot-admit-first-plan/`。
保留已有未跟踪的 `scripts/__pycache__/`。
正常跨仓库依赖使用接收执行者已确认能取得 Git 对象的 exact revision。
记录起始 multiraft 提交当前仅本地/未发布；不能暗示已经能远端 pull。
M1 可用自有 local mirror/Git bundle，或 `.tmp/` 中的临时 Cargo patch 桥接未发布对象。
记录源码对象与 lock 哈希；不能提交绝对 path 依赖或浮动 branch。
本地构建成功不能证明实验室可复现就绪。
远端发布由执行线程按其用户授权处理；本计划交付不 push。
实验室优先 pull 已提交的固定源码，再 build/run，遵守 lease、mux 与仅清理自有资源手册。

§2 源码/依赖前置检查先解决依赖图，再由 T0 选择 crate 边界：

- 保持旧 ech0 OpenRaft resolved/locked `0.9.25`（manifest semver `0.9.21`），不意外升级，并与 multiraft `=0.10.0-alpha.30` 隔离。
- 后者 fork revision 固定为 `ea46d0e571f7497f1f549e125254dfcb2ff45e2e`。
- 隔离 native 类型/协议，不能无意升级旧 raftd wire 或磁盘格式。
- 检查两套 SDK 依赖与 feature 合并，固定集成后的 lockfile。
- 通用 multiraft 不依赖 ech0 business、store 或 TiKV。
- 建议新 ech0 crate `crates/admission-log` 只是提案，不是现有 API/路径保证。
- 纯核心可复用现有中立 store model 或提取小接口；避免 business↔新 crate 循环，不大拆 store。

## 3. T0 — 冻结 authority 合同、codec 与来源

| 项目 | 执行工作包 |
| --- | --- |
| 依赖 | §2 源码/依赖前置检查；既有 Slot 底座 |
| 模块/文件 | ech0 manifest/lock、相关 SEND/collector/read 合同；拟建准入 crate；现有 Slot 公共接口 |
| 输出 | 版本化操作映射、命令/结果/快照 schema、上限、可信输入、backend 配置与精确文件归属 |
| 验收 | 所有 authority 写/读有归属；既有强 claim/事务保持或明确解决；依赖图可复现构建 |

T0 交付跨 Slot Claim/Stop 状态转移表、partial success/Unknown/crash 矩阵，以及 coordinator 账本归属、持久化与恢复设计。
证明保留旧 observable 合同，或记录显式受审的设计变更；未解决时不能报 T0 通过，也不能推进相关 FSM/coordinator 实现。
这是工程设计门槛，不是重新批准固定 Slot；不能假定 global atomic 或暗加 2PC。
业务 adapter 负责资格、可信时间/message-ID 输入、request origin 与现有允许短 stale 的 audience 捕获合同。
Slot 仅承担通用命令/查询传输与持久化宿主。
实施前按真实源码冻结下表；名称描述职责，不是假定已存在的可调用 API。

| 既有 authority 职责 | 新职责 | 冻结合同 |
| --- | --- | --- |
| SEND head/dedup/pending 准入 | Admit 命令 | 原子固定重放，或 seq/head/pending/result/flush 初始化 |
| Collector claim | Claim 命令 | 精确 holder/revision、有界请求/输入与跨 Slot 部分结果账本；不新增租约过期接管 |
| 确认消息/进度持久化 | CommitPersisted 命令 | 仅确认 TiKV 事务后，携带精确 claim 证明 |
| 释放 collector 所有权 | Release 命令 | 精确 holder/revision 条件；不删除唯一未知 pending |
| 停止受影响准入 | Stop 命令 | 每 Slot 确认状态；未记录结果明确报告 |
| Flush 记账/屏障 | Flush 命令与屏障查询 | 已 apply 的 authority 与关闭顺序 |
| Head/pending/read-point | 线性 authority 查询 | 读者使用所选 backend；local watermark 不是线性屏障 |
| 其他 legacy cache namespace | 现有 cache API | 继续现有合同 |

冻结 canonical Channel 编码、命令 identity/fingerprint、消息/结果字段、版本与 verdict codes。
ech0 main 来源映射：`crates/business/src/send/admission.rs` 包含 SendCommand/Outcome；T0 核实完整 crate 前缀路径。
`record.rs` 四类状态：head {head, stopped}；command identity→intent/msgid/seq；pending→msgid/command/sender/content；flush Idle/InFlight(holder)/Stopped(holder)。
现 revision 在 cache-entry metadata，新 FSM 必须保存显式等价 version。
找出需替换 SEND authority 表述的 spec/ADR；不要整体替换泛 cache 合同。
Backend 用显式启动/campaign 配置，保留受控 legacy 对照。
禁止每请求 Unknown 回退旧 writer、自动重放写或 writer 双写。
定义 Channel 数、永久结果元数据、pending payload 字节、命令/查询/effects、scan 与 inflight owner 上限。
永久结果容量满时拒绝新命令，不能丢失此前成功结果。
T0 记录新 head/pending + group-version 读集及全部 invalidation/version writers，证明现有资格短 stale 语义未被放宽。
如必须同一视图，则接入相应 replicated version/control 并同步更新其 writers；不能留下隐含 split view。
写明初始化证明与 ownership epoch；曾 accepted 的 Channel 不能从落后 TiKV head 导入。

## 4. T1 — 确定性业务 FSM

| 项目 | 执行工作包 |
| --- | --- |
| 依赖 | T0 合同、依赖与 codec 冻结 |
| 模块/文件 | 拟建准入 crate 纯核心、codec/snapshot 与独立归属测试；必要中立 model seam |
| 输出 | 真实 Admit 与完整 collector/flush/stop authority 状态机，版本化 snapshot/digest |
| 验收 | 确定性重放、拒绝不耗号、identity 冲突、claim、有界状态与恢复测试 |

Direct 按参与者 low/high 规范化，Multi 使用现有 canonical group identity。
命令 identity 为 Channel + sender + command ID，保留既有 fingerprint 字段。
成功重放返回原 message ID、Channel seq 与固定结果；apply 内再次检查 dedup，关闭入口 race。
在 proposal 派发前固定 message-ID candidate，不复刻旧每轮 CAS 随机 candidate。
身份认证与结构有效性先于重放；已存成功结果先于当前资格、stop 与容量判断。
结果重放不能绕过认证 identity 绑定。
新 acceptance 用 checked `head + 1`，原子写 head、pending、永久结果与初始 flush 状态。
不预分配序号段，不以 Raft log index 充当 Channel seq。
拒绝不耗 seq；同 identity 改 intent 不改状态。
精确重放不创建新消息，不再触发 webhook。
定义 Claim、CommitPersisted、Release、Stop、Flush 等全部 authority 变更；仅 Admit 不够。
Apply 内无网络、时钟、随机数或外部 I/O。
可信时间、message ID 与 payload 显式输入，并记录验证与来源。
不以 TTL/GC 删除永久成功结果；payload 限额策略不能暗删 dedup authority。
容量拒绝前先读取已有结果。
只在持久化已确认、holder/revision 精确且满足冻结安全条件时清除 pending body。
快照覆盖 head、pending、成功结果、flush/stop、persisted progress 与 ownership/claim 状态。
完整恢复基底有版本与 digest；坏或不完整状态 fail closed，不能当空 Channel。
测试溢出、冲突、stop/容量/资格变化后的重放与 snapshot + tail 恢复。

## 5. T2 — Slot adapter 与 M1 真进程门槛

| 项目 | 执行工作包 |
| --- | --- |
| 依赖 | T1；现有 Slot factory/recovery 接口 |
| 模块/文件 | ech0 Slot adapter、业务测试宿主；现有 `multiraft-fsm`/`multiraft-slot` 公共 seam；新独立进程 harness |
| 输出 | 实现现有 StateMachine/SlotStateMachine factory 与 recovery proof，真实业务命令/查询 |
| 验收 | 三 OS 进程、RF3、多 Slot、canonical Channel seq/幂等及重启恢复证据 |

复用现有抽象，不另造第二套 Slot runtime。
编码 command/query 与每次 apply effects 均不超过 128 KiB。
大 scan 有界分页，声明总 budget 与稳定语义；不能无限快照/查询响应。
Authority 决策用线性读，明确区分本地 progress/watermark 检查。
Fresh Channel 必须证明无 accepted state，并有适用 ownership fence。
已有 Channel 不能从异步持久化 TiKV head 初始化。
实现同步 StateMachine apply/snapshot/restore、SlotStateMachine query 与具名 Factory.validate_recovered。
NativeDurable freeze_bounded 默认 Unsupported；必须边构造边限额，不能分配后才量大小。
现每 group 全量 native snapshot cap 可配置 1–64 MiB；永久 dedup 增长与该上限冲突。
T0 核算最坏合法完整状态、codec 开销，以及 Claim/Stop/confirmation/cleanup 控制元数据预留是否符合 snapshot cap。
Admit 因容量停收后，控制操作仍须能安全完成；不能仅以 fixtures 小于 64 MiB 证明有界。
T1/T2 近 cap 测试 confirmation、shutdown、snapshot 与 recover；M1/M2 拒绝新命令而不 TTL 淘汰。
Factory 本地 proof 在锁内有界/无 I/O；外部 proof 锁外执行并保持 owned cancellation。
Apply/query 接收 group + payload，不接 routekey；入口核实 routekey 等于 payload Channel，FSM 重算 payload→group。
同 Slot 伪 key 需要目的端验证；必要最小 validator seam 提前 T2，不等 T5。
业务回复预先限额；apply 后才发现 effects 超限会成为 Unknown。Pending lookup 必须分页。
Expected Slot 不提供 expected-term/atomic term fence。
Owned proposal 保留原 deadline；可能派发后丢回执为 Unknown。
Admit Unknown 强读：自身 candidate ID 得 Accepted，别人的 same-intent candidate 得 Replay，miss 仍 Unknown，不重提。
Lookup miss 不是未提交证明。
同 ID 显式 retry 遵冻结策略并返回固定重放；禁止自动重放写。
仅在完整恢复基底存在时 purge log/snapshot。
T1/T2 即实施 count/byte/inflight 硬上限，复杂公平队列可后做。
同/不同节点并发同 Channel；同 Slot/跨 Slot 不同 Channel，验证各自连续业务 seq。
用真实进程覆盖 leader 丢失、重启、落后副本恢复与丢响应 identity 场景。
Mock/unit 是补充，不能替代三进程 RF3 门槛。
M1 报告列通过项、未解决失败及明确缺席的生产 TiKV/旧数据迁移。
M1 正确性通过后继续 T3，不要求完整 DRR 或性能认证。

## 6. T3 — 真实 TiKV 交接与持久化证明

| 项目 | 执行工作包 |
| --- | --- |
| 依赖 | T2；T0 跨 Slot claim 合同已解决 |
| 模块/文件 | ech0 collector/writer adapter、现有 store transaction、authority claim/progress 命令与独立故障测试 |
| 输出 | 真实 TiKV 事务→确认 persisted proof→条件 authority cleanup |
| 验收 | 保持跨 Channel 原子事务；精确 claim、crash/Unknown/stop/cleanup 门槛通过 |

保持跨 Channel 消息行与 persisted heads 在同一 TiKV 事务。
不能静默按 Slot 拆事务。
现 `collector.rs` 批次 claim 是原子的；改为逐 Slot claim 是 coordinator 合同变更。
多 Slot claim 非原子，不能声称 global CAS。
写 TiKV 前冻结逐 Slot acquisition、partial/Unknown 账本与精确 holder/revision release。
全部所需 claim 确认前，不发 TiKV 写。
如公共合同要求强 all-or-nothing claim，明确解决设计，不能暗加 2PC。
现 failed/Unknown TiKV 语义保持为 stop 受影响 Channels + report。
不自动 TiKV retry、查询/对账、claim steal 或恢复不确定写；停止失败/Unknown 批次全部 Channels。
显式映射 registry controls；Claim/FenceUncertain/Release/Stop/DeleteConfirmedPending 仅为命令示意名。
保持 uncertain-idle revision fence、精确 holder 验证、head+flush 原子 stop，以及确认删除后 release 的可重复性。
Local registry phases/held claims 需要 snapshot/restart 证据，不能直接宣称业务自动恢复。
恢复 FSM 状态不授权自动恢复/重发 Unknown TiKV 工作。
覆盖 TiKV commit 后/Raft confirm 前 crash、stale holder、partial claim、Unknown stop、cleanup 回执丢失与 commit 中 shutdown。
Timeout cleanup 不得删除唯一 pending payload。
逐 Slot 记录 Stop；有 Stop 无法确认时，必要时 fail-stop 更广入口。
报告 `stop_unrecorded`；无证据不能声称所有 Channels 已停。

## 7. T4 — 入口、读者、投递与 M2 门槛

| 项目 | 执行工作包 |
| --- | --- |
| 依赖 | T3；backend 集成与完整读者映射 |
| 模块/文件 | ech0 HTTP/WS SEND、业务 delivery/audience/webhook、history/SYNC/join reads、runtime/app lifecycle |
| 输出 | 新隔离 namespace 的真实共享入口、异步持久化/投递与全部 authority 读者 |
| 验收 | 真实 HTTP/WS + TiKV 独立 oracle，保持 wire/auth/origin/replay/shutdown 合同 |

HTTP 与 WS 共用显式选择的 backend 与 verdict 映射。
保持 wire/result codes、exact origin、认证与资格 stale 语义。
保持 `target.rs` fresh qualification capture 与允许短 stale；现 pending/result 不保存 audience。
Replay 清空 audience/origin；不能暗加 durable audience outbox 或严格即时撤销合同。
Accepted 后持久化与投递可并行。
SENDACK 等 committed + applied admission，不等 TiKV 或 RECV acknowledgment。
Webhook 只在首次 acceptance，Replay 不再发送。
所有 head/pending 消费者，包括 history、SYNC、join/read-point，都路由新 authority。
History/SYNC accepted head + pending、group join accepted head 一并切换；group-version 读取与资格来源协调。
其他 cache namespace 保持原 authority，不要求整体迁移泛 cache。
不能用旧 head/pending cache 读来决定新 Slot 状态。
关闭先 fence ingress，在 persistence/delivery 依赖仍活着时 drain/join owned 业务 tasks，再保持 Slot 活着 flush writer，最后 join Slot/RPC/DB owners。
报告未决工作/Unknown，不能强制假成功 flush 来结束关闭。
M2 用新隔离业务 namespace/test data 与真实 TiKV 独立 oracle。
不包含生产旧 namespace、部署或旧数据重写。
Sandbox 可保守整节点故障/回滚，但必须记录，不证明生产 Slot 隔离。

## 8. T5–T7 — 公平、可靠性与迁移就绪

| 步骤 | 依赖与归属模块 | 输出与验收 |
| --- | --- | --- |
| T5 必要公平 | T4 真实 flow；Slot runtime/RPC/limits 与独立 load harness | 有界 RR、local/remote canonical-key 一致；hot/cold、字节倾斜、churn 与下层 reserve |
| T6 隔离/可靠性 | T1 后可由独立通用底座 owner 开始 | Slot quarantine/recovery 与 observer 调查；共享 DB/manifest fatal 保持 node 级 |
| T7 迁移/证据 | T4 状态合同 + T5/T6 门槛 | 全状态 fence 迁移演练与冻结目标验收；ready 不等于 deployed |

T5 先限 active Channel 数/字节并做有界 RR，无需先完整 DRR。
本地与远端目的入口验证相同 canonical key/cost/Slot identity。
旧 G1/G3 reserve、deadline 与 completion ownership 义务不取消。
没 DRR 也须证明饱和时 Slot/native/application-RPC read/control reserve。
真实 flow 后跑同 Slot/跨 Slot hot/cold，记录实际同/异 leader、字节倾斜与 key churn。
区分 planned、issued、unissued、互斥终态、原 cohort Unknown 与 outstanding。
缺数值 SLO 标为 `performance_not_evaluated`，不当核心正确性失败，也不虚构性能 pass。
简单 RR 不证明所有 byte fairness/SLO；需要按 byte 公平时接 G2 DRR。
G5 用相同 caps/reserves 与观测，对比 bounded FIFO 和所选 bounded RR；需要 DRR 时再选 DRR。
旧无队列行为只能单独参考，不能把差异归因于调度算法。
T6 文件独立，不与业务 adapter owner 交叉编辑。
M3 必须实施/测试永久 dedup 的长期恢复容量方案，如流式/分片/增量快照或另一明确设计。
不能只增 cap、假称已支持 streaming，或压缩删除唯一 authority 状态。
未定位 observer assertion 分证调查，不能 suppress 换取通过。
单 Slot 故障可 quarantine 并保持健康 Slot 服务；共享 DB/manifest 故障保持 node 级。
T7 全量导出/导入 dedup/head/pending/flushstop/persisted progress/claims/Unknown，snapshot + tail 对账。
切换前以 ownership epoch fence 旧 SEND、collector 与 read boundaries。
不得直接改 Slot 数/hash、双跑 writer 或 error fallback 当迁移。
新写前可 abort；新写后 rollback 必须新 epoch 下反向迁移。
生产部署/切换是演练后另行授权动作。
目标运行前冻结吞吐、冷 tail、拒绝/Unknown 与资源阈值。
区分监控故障与产品故障，保留原失败与历史收据。

## 9. 分工、命令与最终证据

T0 → T1 → T2 → T3 → T4 是实施关键路径。
每包先分精确文件；独立 model/fault tests、harness 与 T6 可用互不重叠归属并行。
按仓库 workflow 用 explorer 阅读、worker 实施、每包完成交 reviewer。
单 integration owner 控制 manifest/lock 与共享 runtime/app；禁止两线程同时改这些文件。
无需创建新的用户聊天。
新 crate/test/harness target 在 T0 创建并命名，不能宣称将来的 flags/targets 已存在。
现有仓库检查：`cargo fmt --all -- --check`、`cargo clippy --workspace --all-targets`、`cargo test --workspace`。
影响相应路径时运行 multiraft 现有 `./scripts/acceptance.sh`，必须输出 `ACCEPTANCE OK`。
从冻结 ech0 源码选相关 focused tests，报告实际命令与范围。
本计划线程不运行 cargo、SSH 或负载。
报告绑定双仓 exact commits/trees/locks、binary/harness 哈希、codec/layout/hash、limits 与硬件/耐久配置。
报告 commands、pass/fail/ignored 数量、scoped failures、source bindings、进程身份、lease 与 cleanup logs。
保留失败产物、Unknown resolution links 与独立 oracle receipts；旧 430/七场景不作新 pass。

## 10. 可复制执行 prompt

执行本最新计划 T0–T4，先报告 M1，门槛通过后连续推进 M2。
计划：`/Users/wei/Documents/WorktreesSpace/slot-multiraft/docs/plans/2026-10-17-slot-admit-first-implementation.zh-CN.md`。
继续 multiraft worktree `/Users/wei/Documents/WorktreesSpace/slot-multiraft`，基线 `9e63b454848d18ceea4510b3f3ea0fb52af71b68`。
保留承载本计划的后续文档提交并记录实际 multiraft HEAD，不要 reset 到编写前基线。
冻结 ech0 main `1606f85fc787604057f2ff7829df291c9ddc857b`（代码基线仍 `8098a4f7`），重新读取实际 branch/HEAD，在 `/Users/wei/Documents/WorktreesSpace/` 创建新 worktree。
共享 ech0 `/Volumes/Space/2nterdigital/ech0-1delivery` 在文档合并后观察为 main；读取实际 branch/HEAD，不要 switch/reset/merge。
选择如 `codex/admit-fsm` 的无冲突分支，exact dependency revision，单 manifest/lock/runtime integration owner。
隔离旧锁定 OpenRaft 0.9.25（manifest semver 0.9.21）与新 alpha.30/fork `ea46d0e571f7497f1f549e125254dfcb2ff45e2e`，先完成 §2 源码/依赖前置检查，再进入 T0。
实现确定性真实 Admit 与全部 authority 变更、真实三进程 RF3/多 Slot，再接新隔离 namespace 的真实 HTTP/WS + TiKV + delivery/read 闭环。
完整 DRR、client/#173 优化与性能 SLO 认证不是 M1/M2 正确性的前置。
禁止 NULL/Unknown 回退 legacy、自动恢复未知写、writer 双写、生产旧数据重写或 online cutover。
遵 T0–T4 合同与范围门槛，保留失败，报告精确源码/commands/counts/ignored/oracle/cleanup 证据。
