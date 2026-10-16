# Slot MultiRaft 底座实施计划

**English：** [2026-10-16-slot-multiraft-foundation.md](./2026-10-16-slot-multiraft-foundation.md)

**日期：** 2026-10-16
**状态：** 76ce0dd 首验证版完成，最终整合检查与本机 RF3/legacy 验收通过；不认证生产就绪，历史 observer 未定位风险保留。见[证据台账](../evidence/2026-10-16-slot-foundation.zh-CN.md)。

按[已采纳底座设计](../specs/2026-10-16-slot-multiraft-foundation.zh-CN.md)实施。[可行性评估](../specs/2026-10-16-slot-multiraft-feasibility.zh-CN.md)保留历史源码事实，并解释后续 ech0 接入门槛。用户已确认范围并同意开始实施；以下步骤要求验证证据，不再次审批已定项。

当前后续执行以 [Admit 优先实施计划](./2026-10-17-slot-admit-first-implementation.zh-CN.md) 为最高优先级，覆盖旧公平先于真实 Admit 的顺序；详见第 7 节。A–F 与历史证据保持原范围。

## 1. 固定范围

以 C `28e668b676c803838eb64a04ba160fb10ad88316` 为工作基线，吸收 M `44baed14b3717b6b6f5f7327790cc97299889e19` 必要改进，移植 R `d6a5dbbb6d2f1c68e9e8bddbbfca18f562c1185c` RocksDB 日志。交付新 Slot API、独立新 data 目录和可运行三进程 RF3 多 Slot 测试应用。旧数据保留，不承诺 API/WAL 自动兼容。

manifest 固定 slot_count、hash version 和 Slot/Group 一对一映射。不包含在线 reshard、生产 Admit/TiKV 接入、业务数据迁移、dedup 政策修改或容量认证。本轮包含 snapshot/restart、切主、提交后断连/Unknown、owned 取消、关闭和资源限额。

## 2. 实施步骤与证据

| 步骤 | 当前状态 | 产物 | 验证门槛 |
| --- | --- | --- | --- |
| A — C + RocksDB | 首验证范围已完成 | 专项移植 R 后端，node-owned DB、组命名空间、耐久 callback 和 backend marker 检查；保留 C owner、精确结果、validation/readiness 和关闭行为 | 后端测试；sync/callback/cancel；DB 丢失/后端错配；snapshot/replay/restart；不绕过 C proof gate |
| B — M 吸收审查 | 首验证范围已完成 | M 独有 descriptor/shared restart、scripts/Jepsen portability 的固定源码清单；吸收相关行为并记录明确排除项 | 验证保留的 descriptor 错配与 shared restart；检查受影响脚本，不能仅 cargo 编译 |
| C — Slot 合同 | 首验证范围已完成 | 新 `multiraft-slot` crate，规范 manifest/digest/磁盘绑定、固定 hash/layout、限额配置和测试 FSM 接口 | 已知 hash 向量、重启映射；非法 version/count/mapping/replicas、node/磁盘与跨节点 digest 错配 fail closed |
| D — 路由与生命周期 | 首验证范围已完成 | key/Slot 服务端校验、本地/单次远端路径、精确结果/查询语义、accepted work owner、deadline 和有界 close | key/Slot/digest 错误在写前拒绝；过时 hint 分类；dispatch 后 Unknown；无重试环；取消/drain 与容量释放断言 |
| E — demo 与 RF3 | 首验证范围已完成 | 最小确定性、可返回结果的 FSM；使用 RocksDB 和新目录的可复现三 OS 进程 RF3 多 Slot runner | 本地/远端读写；snapshot/purge/restart；切主；提交后响应丢失与相同 ID 重试；限额/热冷进展；关闭与 recovery proof 失败 |
| F — 独立审查与证据 | 首验证范围已完成 | 审查整合 diff/合同、修复已核实问题、双语记录精确源码命令结果与剩余缺口 | 真实源码相关 tests/build 和实进程验收通过；审查发现已修复或明确记录；不借用历史通过数 |

A/B 完成后才形成最终整合基线检查。C 规定 D/E 使用的合同；只有文件所有权不重叠、接口已协调时才并行实现。F 审查完成后的整合行为。证据台账现已附首验证版完成收据，不关闭后续生产门槛。

## 3. 验证过程

1. 固定实测源码 commit 或可复现 tree digest、OpenRaft pin、toolchain、features 和限额。临时命令、日志及运行产物放本 worktree `.tmp/`。每次新运行独立 data 目录，不复用或删除旧数据。
2. 随改动运行定向 storage/runtime/Slot 测试。Rocks 后端须与 C proof/readiness/cancel/startup gate 联合验证，不能只测新日志便假定组合正确。
3. 对整合结果按适用范围运行仓库要求的 format、lint 和 workspace 检查。准确记录命令、退出码、pass/fail/ignored 和有效限制。依赖环境而未运行的检查仍是缺口，不能算通过。
4. 对 M 改动运行适用 descriptor/shared-restart 与 portability 检查。逐项保留纳入/排除记录与行为保留证据。
5. 使用 E 实现的 demo/acceptance runner 启动三个独立 OS 进程、RF3、多 Slot、RocksDB。记录进程身份、endpoint、manifest digest、node-bound 目录、数据断言和清理。通过公开 Slot 入口测试，避免底层 Raft 成功掩盖 facade 问题。
6. 注入 leader 丢失和提交后响应丢失。验证精确 committed 结果可在显式相同 ID 重试/重启后恢复，FSM 不重复作用。区分 dispatch 前拒绝与 Unknown；query miss 不得错误解除 Unknown。
7. 覆盖 snapshot restore/log replay、recovery proof 拒绝、accepted 后取消、node/Slot count/byte 限额饱和、热冷进展和有界关闭。记录未完成 owned 工作和错误，不能只因 client tasks 已退出就称 clean。
8. 审查最终整合 diff，修复已核实问题，重跑受影响检查，并将证据绑定最终源码。按实际类型、hash bytes 和路由合同修正文档 API。

runner 已实现：根目录 `cargo build -p multiraft-slot-demo` 后以新数据运行 `python3 scripts/slot_acceptance.py --case all`；见[指南](../slot-acceptance.zh-CN.md)。现有 `scripts/acceptance.sh` 只证明它覆盖的行为，不能自动视为新 Slot 入口或 RocksDB 多 Slot 验收。

## 4. 验收矩阵

| 场景 | 必须观察到 |
| --- | --- |
| 映射与身份 | 同 key 重启映射不变；manifest/hash/node-bound disk 不兼容被拒绝；target 重算 key/Slot 并核 digest |
| 精确 apply 与读 | effects 对应精确 applied command；线性化读通过原生 ReadIndex/apply 可见性；stale 读携带 watermark |
| 恢复 | committed 测试状态及结果经全进程重启与 snapshot/log 恢复保留；proof 缺失不得 ready |
| 响应丢失 | commit 后断连可返回 Unknown；accepted 工作仍有 owner；显式相同 ID 重试返回固定结果且无额外作用 |
| 切主 | 任意节点入口在切主期间/之后有界进展或分类拒绝；hint 不成为执行权 |
| startup/create/recovery 失败 | C 整 local node rollback 和清理保留；不泛化所有 runtime fatal 路径 |
| 资源与关闭 | node/Slot 两级 count/bytes 有界；permit 跟随 owned 工作；实测热冷进展；关闭停 intake、drain 并报告残留 |
| 后端 | RocksDB 耐久 callback 不因 waiter 取消而丢失；marker/backend/DB 错误 fail closed；FSM 由 log + snapshot 恢复 |

Slot 局部 runtime 故障隔离是生产接入前独立门槛。共享 DB 或 manifest 身份错误仍为 node 级。本轮可继承已证明的 C rollback 边界，必须明确该限制。

## 5. 证据台账

| 项目 | 实际源码 / 命令 / 结果 |
| --- | --- |
| A/B 整合基线 | 最终实测源码 `76ce0dd6c686367f04de4cfc9772ed3942140c3c`；tree `6a7f7538c641dc3017ab9093283bc2b918f80503`；生产实现与 e554 一致；Rocks/后端/recovery 和 M 选择性 fixture 通过；Jepsen 单元 3 tests/12 assertions 通过 |
| C/D Slot unit/contract 检查 | manifest 9、Slot runtime 12 + budget unit 1；owned proposal 5、watermark 4 + local observation 2 增量通过；源码/日志边界见证据台账 |
| E 三进程 RF3 多 Slot 验收 | 最终全部场景收据 7 项通过，含 raw HTTP RST；13 次启动 PID 均不存在、九端口可复用；保留此前失败 |
| F 独立审查与最终验证 | 最终 format/Clippy/build 通过；串行 workspace 430 passed/0 failed/12 ignored；harness 15 项通过；legacy ACCEPTANCE OK、3 PID 不存在/六端口可复用；保留历史 observer 风险 |

[证据台账](../evidence/2026-10-16-slot-foundation.zh-CN.md)记录命令、收据、失败与未运行场景；增量次数不能累加为最终源码总数。C 历史 `95c6e6c`（366 passed、12 ignored、一次三进程 acceptance）和 R 后端命令不是本 worktree 通过结果。4/8 Slot 小布局验收不认证生产 slot_count、资源、吞吐或 B4 延迟。

## 6. 后续边界

生产接入先要求关闭证据台账记录的未定位 observer 间歇性风险，并可证明 Slot 局部隔离，以及保留原子 dedup/seq/head/pending/result + flush/stop 的确定性 Admit 设计。fence 旧 admission/collector 后全量迁移权威状态及 persisted progress，不能从滞后 TiKV head 启动已有 Channel。永久 command result 保留和跨 Channel TiKV 事务语义不能偷改。

RocksDB 继续只承担本地共识 log/metadata；TiKV 继续承担长期消息事务 storage。异步投递与异步 TiKV 是接纳后并行路径；SENDACK 是 committed + applied 接纳，不是 TiKV 或 RECV 确认。这些合同约束下一阶段，本底座不实现该阶段。

## 7. 可执行后续顺序

当前执行顺序以 [Admit 优先实施计划](./2026-10-17-slot-admit-first-implementation.zh-CN.md) 为最高优先级：T0 合同来源冻结；T1 真实业务 FSM 与 T2 Slot RF3 adapter 构成 M1；T3 跨 Slot claims/TiKV 交接与 T4 HTTP/WS、delivery/history 的隔离新数据空间构成 M2；T5 必要的 Channel 有界公平与热冷验收、T6 生产故障隔离/observer/snapshot 增长、T7 迁移演练与目标验收构成 M3。M2 仅为隔离新命名空间验证，T6 仍是生产接入前门槛；此顺序不自动授权部署。完整 DRR、客户端 NODELAY 和 #173 复测均不是 M1/M2 前置，保留已有必要配置。

旧 [Channel 公平准入计划](./2026-10-16-slot-channel-fair-admission.zh-CN.md)的 G0–G6/H1 ID 保留供引用，其“先公平后 Admit”依赖已由新计划覆盖。G1–G3 仅在 T5 需要该详细实现包时按旧内部依赖推进；G6 的真实 Admit/persistence/delivery 工作提前拆至 T1–T4，不等待 G5 公平证据。故障隔离与 observer 关闭归 T6，完整迁移归 T7；H1 仍是有证据时才考虑的后续热点隔离。M1/M2 先保留硬上界、身份校验、deadline/Unknown/ownership 与 read/control reserve，公平后移不取消这些合同。A–F 与历史证据保持原范围。
