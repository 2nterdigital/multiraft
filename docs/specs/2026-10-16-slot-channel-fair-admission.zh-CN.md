# Slot Channel 公平准入设计

**English：** [English version](./2026-10-16-slot-channel-fair-admission.md)
**日期：** 2026-10-16
**状态：** 已采纳的下一阶段设计；尚未实现、尚未验证。

本文规定下一阶段实现合同，不代表当前 runtime 行为。
[底座规范](./2026-10-16-slot-multiraft-foundation.zh-CN.md)仍是已实现基线。
[实施计划](../plans/2026-10-16-slot-channel-fair-admission.zh-CN.md)规定 G0–G6 与后续 H1 门槛。
历史 430-pass 与 A–F 结果保持原有范围，不能证明本调度器有效。
不同 Slot 的一个冷请求获得进展，也不能作为 Channel 公平性证据。

## 1. 范围与固定约束

保持固定 `slot_count`、hash bytes/version、manifest 身份及 Slot-to-Group 映射。
保持每 node 一个 RocksDB，使用 default column family 和 group 命名空间。
本阶段不引入每 Slot DB、生产 Admit、resharding 或 write retry。
Slot 故障隔离与性能公平是不同门槛，不能互相证明。
历史 observer 风险、生产 Admit/TiKV 迁移与容量门槛继续保留。
公平只覆盖已接纳且 eligible 的请求，依据估算成本，不承诺 wall-clock 硬上界。
冷新 key 的 SLO 还要求受保护的入口容量与明确的负载域。

## 2. 统一目的端准入 seam

当前本地入口经过 `route_write → execute_local`；远端入口经过
RPC handler `→ accept_write → submit(routed=true) → execute_local`。
见 [runtime](../../crates/multiraft-slot/src/runtime.rs)、[routing/RPC](../../crates/multiraft-slot/src/rpc.rs)
与 [budget](../../crates/multiraft-slot/src/limits.rs)；底座源码锚点为 runtime 343/390/417/527、
RPC 82/250/283 与 limits 66。
仅包装 source 不能仲裁同时到达的本地与远端请求。
两条路径必须在 native dispatch 前汇合到恰好一次目的端准入 module。
远端接收后保持 local-only：不递归 forward，不自动重试写入。
source 路由/hint 工作与目的端执行分别拥有任务与资源账目。

业务 adapter 注册有界、非阻塞、无 I/O 的 classifier/validator。
它验证完整规范 ChannelKey 与 routing key、command 身份一致，
包括 schema 要求的全部身份维度，并计算 trusted cost。
客户端申报的 key、长度与 cost 不具权威性；不一致须在 enqueue 前拒绝。
通用 module 调用 adapter，不解析 opaque 业务 payload。
最小测试 FSM 也必须实现此验证，包含伪造 key 绕过测试。
非法或超大 command 不能触发无界解析工作或 active-key 分配。

以下是拟新增 Interface，**不是现有 Rust API 或当前错误变体**：

```text
register_admission(adapter: ClassifierValidator, limits: FairLimits)
validate(route_key, encoded_command) -> Validated { channel_key, cost, retained_bytes }
admit_destination(validated, original_deadline, observed_owner_generation)
  -> AdmissionReceipt { completion, stage_receipt }
stage_receipt -> NotSubmitted(TypedRefusal) | MayHaveSubmitted | ExactApplied(result)
```

验证、reservation 与 ownership 注册须一起成功，失败则完整回收。
准入 receipt 只证明本地 ownership，不是共识执行权。
G1 须让两个入口通过同一 Interface，测试 adapter 也如此。

## 3. 有界两级调度

node 轮转 ready Slots；每 Slot 对 Channel 使用等权、有界 DRR 变体。
Channel FIFO 只规定 scheduler 选择顺序，不承诺跨入口到达顺序，
也不承诺多个同时 dispatched owned task 的实际日志顺序。
业务若要求顺序，由未来 Admit 原子分配业务 seq；
Raft index 不能替代业务 seq。

使用 `cost = 固定每命令成本 + 实际编码 command bytes`，加法检查溢出。
schema 对 apply 工作与 payload 设硬上限；bytes 不能衡量任意 apply 工作。
quantum 必须为正；有限 `deficit_cap >= max_acceptable_cost`，避免请求永不能服务。
每次 visit 加 quantum 并饱和到 cap，选择付得起成本的 FIFO head 并扣 cost。
每 visit 的 messages、bytes 与调度工作均有界，即便仍有 backlog 也须轮转。
成本不足的 head 轮转，后续 visit 才再加 credit；不能在同 visit 对其循环。
Channel 变空时 reset deficit；idle Channel 不得积攒未来突发 credit。
只维护有界 active-key 表与 ready list，不为每 Channel 常驻 task。
只有 Queued = Dispatching = InFlight = 0 且没有 owned completion 引用该 entry，
才真正 inactive/可回收；queue empty 但还有 InFlight 不能冒充新 key
取得 new-key reserve。回收与重新注册在同锁及 generation 校验下完成，
旧 completion 不得修改重建 entry；inactive 期间不累计 credit。
资源 blocked 的 Channel/Slot 跳过，由容量或生命周期事件重新激活。
禁止 busy-spin；全部剩余工作 blocked 时等待 notification。
dispatcher 不等待 committed/applied 就选择下一 Channel 或 Slot。
owned completion 路径保留已 dispatch 工作，dispatcher 继续调度。

分别设置 Channel、Slot、node 的 dispatched count/byte 窗口。
一个 Channel 窗口满不能阻止其他 eligible Channel 被选择。
一个 Slot 窗口满不能阻止其他 ready Slot 被选择。
`per_channel_dispatched = 1` 会牺牲 pipeline，不能当作未测的生产默认。
这是按估算成本的公平策略，不继承 Linux/DRR 证明，
也不能在选举、storage stall 或无界故障下承诺硬服务时限。

## 4. 容量与 reservation 移交

显式计入三阶段：source-forward retained、destination queued、
destination dispatched retained（包含释放前的 completion/result buffer）。
目的端 accepted 容量是唯一 reservation：`accepted = queued + dispatched`。
此处预算 `dispatched` 从 Queued → Dispatching 原子移交时开始计数，包含
Dispatching、InFlight 及 owner 仍保留的 completion 资源，不能等 native receipt
返回后才计入。它是预算/执行保留计数，不证明原生命令已提交。native dispatch
事件另计，不能依据此 counter 判断 stage Unknown。该等式因此没有移交空窗，
释放仍以 owner 实际释放资源为准。
queue-to-dispatch 原子移交该 reservation，并取得 execution-window token；
禁止持有 queued reservation 时再次申请同一个 Slot accepted budget。
队列额度与执行窗口是独立控制，分别维护 invariant。
origin 与远端 destination 各计其真实保留资源。
同 node 汇合时转移 shared buffer，不重复计算同一 allocation；
真实保留的每个 clone/copy 与 response reservation 都须计入。
ingress decode buffer 与持有 payload 的 forward task 也必须入账。

Channel、Slot、node 各层限制 count、bytes 与 active keys。
对每层 L，count/bytes 均满足 `queued_L + dispatched_L <= accepted_limit_L`，
另满足 `dispatched_L <= execution_window_L`、`active_keys_L <= key_limit_L`。
Channel active-key 容量为一；Slot/node key limit 限制聚合 metadata。
payload reservation 之外，保守计入每 entry/task/allocator overhead。
每类 allocation 指定唯一 owner；node 总上界为 ingress + source retained
+ destination accepted + key/queue metadata + execution overhead + 有界 result/cleanup storage。
RocksDB/native cache 与 transport pool 另设有限上界，不能视为零内存。
重叠计数描述同一 reservation，不能再当成不同 allocation 相加。

每层为新 Channel entry 保留有限 count、bytes 与 active-key headroom，
或明确限制受保护负载域，使这些资源有保证。
建议为 eligible 新 key 的首个最大请求保留容量，同时限制 key 集合/速率。
非 idle 的已有 key 不能冒充 new；资格依据规范且已认证的身份。
认证 key churn 超出域时明确拒绝；不能承诺任意新 key 在 full node 仍能进入。
公布 reserve 消耗与拒绝规则，已有流量不能吃掉保护 headroom。
启动前由配置算出内存上界与冷准入可行性。
这些 reserve 本身不保证冷 source 能抵达 destination。

## 5. 下层容量闭环

当前 Slot Budget 由读写共享；native `max_inflight` 来自
`global_requests`，application RPC handler 在整个 wait 期间持有 permit。
G1 须限制 write 占用，并为 read、result reconcile、
hint probe 与 maintenance 在 Slot budget、native admission 和 application RPC 全链保留有限容量。
若 write 已耗尽下层池，仅上层 queue reserve 不足以提供保护。
RPC predecode ingress、forward-retained 与 queue-holding permit 均须有界。
避免 write waiter 占满所有 handler，并同时限制 control/read 类自身。
记录每层使用独立池、class cap 或 reserved permit 的具体策略。
source 饱和可能在目的端分类之前拒绝冷请求，须记录该 refusal。

启动配置校验必须覆盖：

- 正 quantum 与 visit-work cap；有限 deficit cap 能支付所有可接纳 command cost。
  visit byte/work cap 至少容纳一个最大可接纳 head，保证能取得进展。
- schema byte/apply-work 上限、cost 溢出检查与保守 retained-byte 上界。
- Channel/Slot/node accepted limit、execution window 与 active-key limit 相容。
- 所需各层都有完整最大请求的新 key count/bytes/keys headroom。
- write cap 加 read/control reserve 不超过真实 Slot/native/RPC 容量。
- 无循环申请：queued reservation 移交时不重拿自身，cleanup 能获得资源。
- ingress、forwarding、completion、metrics、shutdown 的有界资源计入内存账。

生产 SLO 数值、quantum 与窗口大小是 G0 待冻结参数。
现有 default 是基线事实，不是本设计实测过的默认值。

## 6. 生命周期、结果与 fence

owned 状态机为 `New → Queued → Dispatching → InFlight → Terminal`。
准入使用原 deadline；queue 时间计入 deadline，不能续期。
Queued cancel/deadline 与 dispatch 在同一锁/原子 ownership 裁决下竞争。
目的端 native dispatch 前，原子证实尚未 submit 时，可在**新增** stage-receipt 合同返回精确
`NotSubmitted(TypedRefusal)`；不得伪装成当前已存在的能力。
只要 submission 可能发生，就保守分类并继续持有 execution ownership。
source application RPC 一旦进入 MayHaveDispatched，timeout/disconnect 就返回 Unknown，
即便尚未观察到 native submission；只有收到目的端精确 negative（未提交）
receipt 或 exact applied result 才能作更精确分类。
目的端曾 Queued，不能让断连 source 独立证明未提交。

InFlight caller cancel 只移除 waiter，不释放 execution permit 或 durable callback。
completion 或 confirmed cleanup 才释放 retained reservation 与 execution-window token。
所有 error/panic/closed 路径须恰好一次释放，并报告未决结果。
leader hint 不是 authority；当前 native `submit_owned` 无 expected-term 参数。
观察到 owner/leader 变化后，在 queue 锁内 fence 并拒绝仍 Queued 的请求。
Dispatching 保守裁决；首版不能伪造原子 term fence 能力。
不复制 queue 到其他 owner/term，也不自动重投。
native client-write 在 dispatch 时最终判断资格。
这不提供原子 term-bound queue fence，也不提供跨任期 FIFO/公平保证。
快速、未被观察到的失主再当选，仍可能处理 deadline 有效的请求。
更强 term-bound 合同须另行设计未来 native Interface。
shutdown fence 路由与队列，明确拒绝 Queued，保留 InFlight owner。
继承底座的 bounded cleanup 与 unfinished/Unknown/result 显式报告。
FSM apply 保持确定性，不执行外部 I/O。

## 7. 验证合同与冻结 profile

G4 建立模型测试与 RF3 harness；G5 在 correctness 门槛通过后评估 candidate。
显式观察 enqueue、select、dispatch、terminal 事件，不能通过 sleep 猜阶段。
包含 no-hot-Slot 冷 baseline，以及同目的端 local/remote 同时入口。
测试同 Slot 热冷 Channel，以及跨 Slot 的同 leader/不同 leader 情况。
不能把同 Slot 同时拥有不同合法 leader 作为测试拓扑前提。
覆盖 sudden hot、hot-key rotation、新 cold key 与认证 key churn。
覆盖 byte skew、最大有界 apply 工作、容量饱和与 blocked Channel 跳过。
覆盖 RPC/native 饱和，以及每层 read/reconcile/hint/maintenance reserve。
覆盖 queue expiry、cancel/dispatch race、lost reply 与 post-dispatch Unknown。
覆盖 stepdown/generation 观察、shutdown 与 shared-DB flush/compaction 压力。
断言伪造 key 被拒、计数有界、reservation 不泄漏且不重复释放。
模型确定性断言 FIFO 选择、deficit reset/cap 与 eligible 工作最终获服务。

G5 主对照采用有界 FIFO 与 DRR，可行时使用同 ELF 的 feature off/on；
classifier、queue/inflight/byte limit、new-key/control reserve
及 observability 全部相同。同 ELF 不可行时记录各 artifact。
观测开销相等，每条件至少两次交替运行。
当前立即 Busy/no-queue 底座仅作额外参照；其差异不止调度算法，
不能把总改善归因于 DRR，也不能复用历史 430-pass/七场景作为此次对照。
不假定已有调度开关 CLI；实现并记录对照机制。
固定 deadline、offered load、hardware、durability、mapping 与 storage 状态协议。
四次运行是比较最低次数，不是统计证明或生产容量认证。
运行前冻结 profile：cold absolute P99 与最大 terminal latency SLO、
最低 completion rate、最高 rejection/Unknown rate、cold latency relative inflation、
backlog bound 与 eligible fair-service gap bound（selection/cost，并观察 elapsed time）。
不得为未知业务阈值编造已测默认数字。
预先定义 cohort、warmup/measurement window、outstanding cutoff 与排除规则。
profile 未冻结、unissued 原因不明或 generator invalid 时，性能标记为
`performance_not_evaluated`；correctness 结果仍独立报告。

使用 `planned = unissued + issued`，记录每个 unissued 的未发送原因。
issued 分解为互斥 terminal outcomes 与 outstanding。
accepted queued/inflight 是 issued 的阶段/cohort 视图，不能再加进总数。
client attempt 与 logical operation 分开统计：Unknown 是一次 attempt 的终态；
measurement cutoff 时尚无终态的请求为 outstanding。后续 resolve
或显式 same-ID retry 不能从冻结 cohort 抹去 Unknown，也不能重复计算 terminal。
resolution ledger 独立记录，idempotency 按 logical command ID 核验。
按 Channel（有界采样）、cold/hot cohort 与 Slot 报告，明确分母。
success latency 与 all-terminal latency、refusal、Unknown 分开报告。
观察 queue wait、predecessor backlog、RPC/storage wait、bytes、memory 与 RocksDB stall。
使用低基数聚合指标与有界 per-key sampling，禁止无界 Channel label。
[旧 cold case](../../scripts/slot_acceptance.py)约 232 行只检查进展。
它不能替代上述混合负载、同 Slot 与结果纳账的评估。

## 8. 后续热点隔离（H1，不在首实现内）

公平准入与容量调优后，只有 cold SLO/完成率仍不达标，且观测定位到
某 group 或 shared storage 争用，才进入隔离评审。
先考虑合法固定 layout 内的整 Slot leader placement/容量。
当前无公开 migration-control Interface，使用前须实现并验证。
未来 versioned Channel override/dedicated group 须另立完整迁移设计。
迁移永久 dedup result、seq/head/pending、flush/stop、persisted progress、
claims 与未决 Unknown，不能只迁移近期 payload。
freeze/fence 旧 writer 与 collector；checkpoint + tail 与对账收敛后，
原子切换 routing epoch，再验证新 owner。
回滚不能复活双 writer；cutover 后须在新 epoch 下反向迁移。
不能临时改 hash，也不能静默重新解释现有 manifest。
独占 group 不能提高 single Channel ordered execution 上界。
shared-DB 瓶颈可跨 Slot；未来少量 storage 分组或物理 I/O 隔离
须另行设计验证，本阶段保持每 node 一个 DB。
生产 Admit/TiKV readiness 与历史 observer 风险继续作为 G6 前置。

## 9. 参考范围

[Linux DRR](https://raw.githubusercontent.com/torvalds/linux/master/net/sched/sch_drr.c)
按 byte 扣成本，不足时加 quantum 并移队尾，空队列 reset；本有界变体
增加 work/window/Slot 调度，不继承其证明。
[RocksDB Write Stalls](https://github.com/facebook/rocksdb/wiki/Write-Stalls)说明 DB-wide stall；
[RocksDB Overview](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview)说明跨 DB 共享资源。
现有 storage 合同见仓库 [RocksDB log 设计](../rocksdb-raft-log.md)。
