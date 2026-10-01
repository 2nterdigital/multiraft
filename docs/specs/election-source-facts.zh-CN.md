# 有界选举源事实

[英文完整接口与覆盖说明](election-source-facts.md)。本能力属于 Ech0 #199，依赖
#201 的最小原生 observer；版本保持精确 alpha30，源 revision 由 workspace/lock 固定。
保持选举算法、参数、磁盘格式和已有默认构造入口。观测交付不等于完成稳定性修复。

调用者先创建 `ElectionSource`、订阅，然后通过
`NodeOwner::start_with_election_source` 在任何 Group 核心运行前安装原生 observer。
run/boot 只能用 1–128 字节安全 ASCII 的不透明标识；禁止放凭据。每次 Node 启动用
独立 boot，source 只能挂载一次。普通 `NodeOwner::start` 保持无观测默认。

库持有 1–8192 条记录的有界 ring，投票集合／授票者复制上限为 1024 项。
原生回调只用 Tokio 有界 MPSC `try_send` 提交 owned metadata，不取 ring 锁、
不投影、不等待；队列满／关闭才计入明确丢失。普通 emit/read/status/close owner
每次最多投影 capacity 个包；不增加 drainer task/runtime。queue 和 ring 各使用
配置的容量，status 报 native_pending，原生实际 join 后关闭 intake／drain 再封口，
terminal pending 必须为零。
每个 receiver 独立报告 buffer 的准确缺失区间、`NativeDropped` 总数和增量；
多个 receiver 会读到同一记录，应按 run/boot/Node/sequence 去重。容量退役不是每个
receiver 都丢失；原生回调丢失没有伪造的记录序号、时间或 campaign ID。

attempt 是初始化、RPC 或点读操作；campaign_id 是每个 Group/Node/boot 的原生轮次，
不能用 term 替代。sequence 是 ring admission 顺序，不冒充跨 producer 的原生
发生顺序。source_elapsed 是原生事件时间，点读和 ACK metrics 各有采样时间；
均为同一 Node 本地 monotonic clock，禁止跨 Node 直接比较。capability 表示接口
可提供字段，不证明窗口完整；遗漏、争用、退役、限额、未观测和缺绑定均保持 partial。

原生事实包括真实初始化／自然超时／外部竞选／转移入口、实际 R 和重采样、lease 与
更新时间、greater-log、实际授票请求决策、已消费的回复与 quorum／leader 建立。
未读取的开关、无法绑定的 campaign 或超限 membership 保持缺失。observer 不复算
资格，不解析 Debug，不从采样或时间邻近补造原因。公共 RPC 授票位不等于 native 已
消费；leader 建立也不代表持久化、提交或业务可写。公共点读私有计时仍标
`PublicPointNotExposed`；只有原生事件提供实际条件。quorum ACK 仅指 committed
AppendEntries，多数派 RequestVote 另有原生事实。

receiver 不持有 Raft、FSM、listener 或任务。订阅／等待不 spawn；取消点读只放弃
调用方等待，已接受的任务仍由 owner 收尾。只有实际 native/listener/FSM 释放后才
结束源流，构造未产生 Group 的失败／取消也完整封口。公共独立 provider + 真正 RF3
 gRPC 回归验证入口、授票、转移、自然超时、业务提交、缺字段、丢失、取消和资源复用；
确定性回归持有 ring 锁并证明回调仍完成／drop0；满队列另验明确丢失与收尾。
修复前失源证据保留；测试断言失败先 owned shutdown，再保留原错／未释放数据根。实体采证和证实根因后的修复仍由集成阶段负责。
