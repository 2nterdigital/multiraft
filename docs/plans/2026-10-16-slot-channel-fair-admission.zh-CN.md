# Slot Channel 公平准入实施计划

**English：** [2026-10-16-slot-channel-fair-admission.md](./2026-10-16-slot-channel-fair-admission.md)

**日期：** 2026-10-16
**状态：** Planned；未实现；未执行任何新增测试或负载运行。

本计划依据[公平准入规范](../specs/2026-10-16-slot-channel-fair-admission.zh-CN.md)，细化[已完成底座计划](./2026-10-16-slot-multiraft-foundation.zh-CN.md)的后续工作。
A–F 完成、workspace 430 passed、七项验收、原失败和未定位 observer 风险仍按[台账](../evidence/2026-10-16-slot-foundation.zh-CN.md)保留为历史事实。
固定 Slot、持久化 hash/layout 和每 node 一个 RocksDB 保持为基线；本文不宣称已实现、性能通过或生产就绪。

## 1. 交付合同与集成顺序

本地 `route_write` 与远端 RPC `accept_write` 必须汇合到同一个目的端准入 seam。
业务 adapter 提供有界、无 I/O 的 key/command 身份及 cost 校验；通用 Slot 将 command 视为 opaque。
node 对 ready Slot 做 round-robin，Slot 内对 Channel 做等权、有界 deficit round-robin（DRR）。
Channel FIFO 仅指调度选择顺序，不承诺全局 FIFO 或全 node 所有 Channel 同吞吐。
每次有界 dispatch 后轮转，不等待 proposal completion；Channel、Slot、node 的 active-key table、queue count/bytes 和 dispatched window 均有界。
cost 为固定每 command 成本加 encoded bytes；quantum 为正，deficit cap 覆盖最大合法 cost，idle 清零，每次 visit 限定消息数/bytes/work。
区分 source-forward、destination-queued、destination-inflight 容量；唯一目的端 reservation 从 queue 原子移交至 inflight；caller Unknown 后仍由 owner 持有至实际完成。目的端 `dispatched` 预算从 Queued→Dispatching 原子转移开始，覆盖 Dispatching/InFlight 及 owner 仍保留的完成资源，不等待 native receipt；这是预算记账，不是实际已提交证明，native dispatch 事件独立记录。
在声明的受保护负载域内预留 new-cold active-key entry、count、bytes 与下层容量；任意 key churn 下不保证 cold always admitted 或无限进展。
写限额必须在 Slot、native、application-RPC 各层给 read、reconcile、hint、maintenance 留容量。
当前 native 最大额度被 read/write 共用，application-RPC handler 等待回复期间仍占 permit：两处都须明确改造并验证饱和行为。

共享 seam 按 G1 → G2 → G3 集成，三项整合门槛均通过后才启用 candidate 调度。
G4 generator/accounting 可在 G0 后独立且分文件并行；candidate 负载仍须等待 G1–G3 与 G4 正确性门槛。
每张实施票在编辑前确定精确文件；下列预计所有权是拆分起点，不代表已有接口。

## 2. G0 — 冻结合同与负载 profile

- **目标：** 按规范冻结 adapter 身份/cost 上界、reservation 阶段、受保护负载域、公平含义和结果分类。
- **依赖：** A–F 已完成证据；文档/模型开发不要求先定位 observer 风险。
- **预计拥有：** 本计划/规范双语文档、未来 `docs/` profile 定义与报告 schema；无生产代码。
- **完成标准：** 审查记录最大 keys/messages/bytes/inflight、visit 上界、read reserve、deadline 和 profile 分母；业务方给出 cold SLO、最低完成率/最高拒绝率/Unknown 阈值，不虚构数值。
- **状态 / 回滚：** Planned，未实现，未运行。冻结 profile 带版本；修改使旧比较失效，须新版本。

## 3. G1 — 目的端 validator、预算与阶段 seam

- **目标：** 汇合本地/远端准入，重算 key/Slot/digest，入队前校验身份/cost，暴露准确 queued/dispatched/terminal 阶段。
- **依赖：** G0；RPC/native reserve 行为须联合约定，不能隐含依赖下层。
- **预计拥有：** `crates/multiraft-slot/src/{runtime,rpc,limits,config,error}.rs`；native `crates/multiraft-net/src/runtime/{requests,proposal,read}.rs` 与 application-RPC 准入模块另票分配不重叠文件。
- **完成标准：** 定向测试证明入口一致、非法输入拒绝、各层预算有界、唯一目的端 reservation、queued→inflight 原子移交和 caller Unknown 不释放；在各层写饱和时证明 read/reconcile/hint/maintenance 进展。
- **状态 / 回滚：** Planned，未实现，未运行。保留 baseline mode；停新 intake 并 drain owned work 后才禁用 candidate，不以释放 dispatched owner 回滚。

## 4. G2 — 有界分层公平调度

- **目标：** 实现 node ready-Slot RR 与 Slot Channel DRR、有界 visit、独立 completion owner；queue depth/fair-gap 观测保持低基数。
- **依赖：** G1 reservation/stage 合同；复用已校验 cost，不在 scheduler 解析业务 command。
- **预计拥有：** `crates/multiraft-slot/src/` 新 scheduler/budget 模块，具体名称在 G0 确定；G1 owner 在 scheduler 审查后集成 `runtime.rs`，避免并发编辑同文件。
- **完成标准：** 模型/性质测试证明合法 cost 进展、idle reset、deficit/work/state 有界、selection FIFO、饱和行为和受保护 new-cold headroom；冻结域内同 Slot hot backlog 不垄断 dispatch。
- **状态 / 回滚：** Planned，未实现，未运行。G3 通过前不启用 candidate；回退 mode 前分类结束 queued reservation 并 drain。

## 5. G3 — Deadline、取消、领导权与关闭

- **目标：** 排队时间计入原 deadline；queued cancel 与 dispatch 在同一同步边界裁决；native dispatch 后保留 owned completion。
- **依赖：** G1–G2；明确 source-RPC 与 native 两个 dispatch 边界及对应结果阶段。
- **预计拥有：** G2 后的 Slot runtime/RPC 生命周期整合及 `multiraft-net` owned request/completion 模块；与 G1 串行编辑，测试独立分文件。
- **完成标准：** 竞态测试覆盖 dispatch 前/中 timeout/cancel、lost reply、已观察 stepdown/owner-generation 变化、shutdown/drain 与 reservation 释放；source RPC 一旦可能 dispatched，没有精确目的端回执就返回 Unknown，不自动 replay。
- **领导权边界：** hint 不是 authority；当前没有 atomic expected-term proposal 接口。只在已观察 stepdown/owner generation 时 fence 旧 queued，再由 native 决定最终资格；不承诺强跨任期 queue fence 或全局 FIFO。
- **状态 / 回滚：** Planned，未实现，未运行。停 intake，分类处理 queued，并 drain/报告 dispatched owner 后回退；未消歧保持 Unknown，不推断未发生。

## 6. G4 — 模型与三进程混合负载 harness

- **目标：** 增加模型/性质检查和可复现 RF3 多源流量，保留旧验收收据及运行其场景的能力。
- **依赖：** generator 独立开发依赖 G0；candidate 负载依赖 G1–G3 及模型正确性。
- **预计拥有：** `crates/multiraft-slot/tests/` 新测试、`scripts/` 新 harness 模块/脚本、`crates/multiraft-slot-demo/src/{main,fsm}.rs` 测试控制/metrics；每文件唯一 owner。保留 `scripts/slot_acceptance.py` 兼容性或采用独立入口。
- **完成标准：** generator 单元检查证明 planned = issued + unissued，issued = 互斥 client-terminal outcomes + outstanding_at_measurement_cutoff；unissued 按真实原因分箱。Unknown 是 client-attempt 终态之一，与 cutoff 时尚无 client 终态的 outstanding 区分。later resolution/显式 retry 在独立 resolution 账本保留 attempt 关联，不抹掉原 cohort Unknown；按 logical command ID 验证幂等。构成/计数不明时 generator_invalid，禁止性能通过。
- **状态 / 回滚：** Planned，未实现，未运行。harness 与生产/旧收据隔离；失败保留产物，仅停止本次拥有的子进程。尚无新增 CLI flags。

使用持久化 hash 本身构造确定性 key fixture，明确产生 same-Slot/distinct-Slot key，不依赖随机碰撞。
通过 node 日志/status 记录实际 leader 随时间变化；期望放置不能证明同 leader/不同 leader。
下列每个 cold profile 保持相同冷流量，并覆盖多源 local/remote ingress：

| Planned profile | 必须观察到 |
| --- | --- |
| Cold-only baseline | cold 绝对延迟、完成和资源参考 |
| Same-Slot 热冷 | 持续 hot backlog 下的公平 selection/gap 与 cold 完成 |
| Cross-Slot，同 leader / 不同 leader | 以实际 leader 观测区分 node 调度与 group 效应 |
| Hot 突发/轮换、new cold 与 key churn | 受保护 headroom、active-key 上界及声明的超载排除域 |
| 不同 encoded size 与重 apply | 成本记账和执行瓶颈的区别；visit 有界 |
| Global/RPC/native 饱和并发 read/reconcile | 各层非写预留服务及 hint/maintenance 进展 |
| Queue timeout/cancel 竞态、stepdown、lost reply、shutdown | 阶段分类正确、无重复作用、owner 保留 |
| 共享 DB 持续 flush/compaction | RocksDB stall/I/O、每 Slot backlog 与 cold 服务同报 |

## 7. G5 — 可比 baseline/candidate 报告

- **目标：** 评估冻结混合 profile，正确性门槛独立于性能阈值。
- **依赖：** G0 profile 签定、G1–G3 整合、G4 已验证；历史 observer 风险单独报告，生产接入前处理。
- **预计拥有：** 新固定源码报告/证据产物与 harness receipt schema；不把底座台账改写为 candidate 证据。
- **完成标准：** 同 binary（记录 ELF/build identity 或平台等价物）、可控有界 FIFO baseline/DRR candidate mode、同 classifier、queue/inflight/byte 限额、new-key/control reserve、观测/硬件/durability；两模式至少各两次交替运行，每次新建自有 data。
- **正确性必过：** 按 logical command ID 零重复 effects、每 Channel scheduler-selection FIFO 无违反、最小 FSM effects 符合已提交顺序、状态恢复及全部资源上界独立通过。不要求并发入口 arrival order 或 owned-task dispatch order 等于实际 log order；失败不能混入平均延迟后宣称可接受。
- **性能必需：** 预先冻结 cold SLO absolute/relative、completion minimum、reject/Unknown maximum、fair-gap/backlog 阈值；业务数值缺失仍是开放门槛，不编造通过条件。
- **状态 / 回滚：** Planned，未实现，未运行。正确性失败或 generator_invalid 阻止 candidate；安全 drain 后仍可用 baseline。

立即 Busy/无 queue 的底座可作额外行为参照，但差异不只 DRR，不能分离调度效应；历史测试收据不是本次新测 baseline。
成功延迟与拒绝/error/Unknown、outstanding_at_measurement_cutoff、unissued 需求分别报告，保留分母与结果阶段。issued attempt 按 G4 的互斥 client-terminal/outstanding 划分核账；原 cohort Unknown 与关联的后续 resolution/retry attempt 同时保留。
纳入 RocksDB stall/flush/compaction/I/O、per-Slot backlog 和低基数公平/资源指标；不导出无限 Channel 标签。
每收据绑定 source/tree、binary/harness SHA、layout/hash、profile、limits、硬件/durability、进程身份与自有清理证据；保留失败及新数据。
旧 different-Slot 单次进展断言不证明 same-Slot 公平、持续 cold SLO 或共享 DB 隔离。

## 8. G6 — 后续 Admit 与生产接入

- **目标：** 设计确定性 Admit，再通过全权威状态迁移接入生产 admission、persistence、delivery。
- **依赖：** 公平证据及独立生产门槛：不压制断言地关闭 observer notification 风险；证明 Slot-local quarantine/recovery，DB/身份失败仍 node-wide。
- **预计拥有：** 后续业务 Admit adapter/FSM、migration/collector 接入模块；具体仓库/文件在独立生产计划确定。
- **完成标准：** 原子 dedup/seq/head/pending/result + flush/stop、永久成功结果重放、persisted progress/claims/Unknown 与既有跨 Channel TiKV 事务合同，经 restart 与 fenced migration 保留；不从滞后 TiKV head 初始化已有 Channel。
- **状态 / 回滚：** Planned，未实现，未运行。fence 旧 admission/collector；checkpoint + tail 和校验先于切换；新写发生后回滚须状态迁移和新 epoch。

RocksDB 保持本地共识 log/metadata，TiKV 为长期消息事务 storage；异步 delivery/TiKV 为接纳后并行路径，SENDACK 保持 committed + applied 接纳。
observer 调查不阻塞 G0 文档或独立模型/generator 开发，但生产 wiring 前必须关闭。

## 9. H1 — 后续热点隔离决策评审

- **目标 / 依赖：** 健康、同硬件 baseline 与 fair candidate 仍违反冻结 profile 时，先归因 group-hot 执行或 node-wide DB/I/O 饱和，再选择隔离。
- **预计拥有：** 未来决策/规范/实施计划；本轮无 routing/storage 代码。
- **选项：** 调整整个 Slot 的 leader/放置（当前无公开操作，后续须明确实现验证），或 versioned routing override/dedicated group。单 hot Channel 仍受自身有序执行上限约束，不保证提升吞吐。
- **迁移门槛：** 全量迁移 dedup/seq/head/pending/flush/stop、persisted progress/claims/Unknown；fence 旧 writer/collector；完整 checkpoint + tail + 校验后切 routing epoch。新写后回滚须 reverse migration 和新 epoch，不是改 hash 或双写。
- **Storage 边界：** dedicated group 不解决 shared-DB 竞争；少量 storage group 或物理隔离须另行评审，当前保持每 node 一个 DB。
- **状态 / 回滚：** Planned，仅设计评审，未实现，未运行。不接受无证隔离结论；另行验证迁移前保持固定 layout。
