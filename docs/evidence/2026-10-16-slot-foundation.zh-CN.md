# Slot 底座验证证据

**English：** [2026-10-16-slot-foundation.md](./2026-10-16-slot-foundation.md)

**日期：** 2026-10-16
**状态：** 首验证版已在 76ce0dd 通过最终整合检查和本机三进程全部场景验收。历史 observer 未定位风险仍保留；不代表生产就绪、稳定性、容量或延迟认证。

## 源码与复现

最终实测源码为 `76ce0dd6c686367f04de4cfc9772ed3942140c3c`，tree 为 `6a7f7538c641dc3017ab9093283bc2b918f80503`。生产实现与首版本地实现 `e5546f1eb28908577745306cd61a57dd8e19b0dc`（tree `0ffa71d3165a946b2ac32bbf8cc5944edbe0d2c5`）一致，`48af836` 仅调整路由测试前提/读观察，`8d2d003` 仅调整 shutdown fence fixture 观察/清理，`76ce0dd` 仅增加 observer 测试失败诊断。本次文档收尾不改变可执行源码。实现从 C `28e668b676c803838eb64a04ba160fb10ad88316` 出发，选择吸收 M `44baed14b3717b6b6f5f7327790cc97299889e19`，移植 R `d6a5dbbb6d2f1c68e9e8bddbbfca18f562c1185c`。OpenRaft fork 仍精确 pin 到 `ea46d0e571f7497f1f549e125254dfcb2ff45e2e`；C/R 历史通过数不属于整合源码结果。

宿主工具链：aarch64-apple-darwin、rustc 1.98.1（`48a229cea`、LLVM 22.1.8）、cargo 1.98.1（`797e8a9bc`）。未特别说明时 Rust 命令使用 workspace 默认 features。临时依赖、target、日志、新 node 数据和运行收据均保留于 `/Users/wei/Documents/WorktreesSpace/slot-multiraft` 的 `.tmp/slot-foundation/`；这些是本地证据，不是仓库跟踪产物。

```sh
mkdir -p .tmp/slot-foundation/tmp
export TMPDIR="$PWD/.tmp/slot-foundation/tmp"
export CARGO_TARGET_DIR="$PWD/.tmp/slot-foundation/target"
# 本轮 macOS 源码 RocksDB/bindgen 环境：
export LIBCLANG_PATH=/Applications/Xcode.app/Contents/Developer/Toolchains/XcodeDefault.xctoolchain/usr/lib
export DYLD_LIBRARY_PATH="$LIBCLANG_PATH${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
```

[设计](../specs/2026-10-16-slot-multiraft-foundation.zh-CN.md)记录实际 API、hash/layout bytes 与限额；[验收指南](../slot-acceptance.zh-CN.md)说明本机三进程 runner。每次运行使用新 output 目录，保留旧收据和数据。

## 定向实现检查

以下是最终整合验证前的增量包检查，不得将次数累加为最终源码的不同测试总数。日志记录各 binary 和断言，下列复现命令对应相同 test target。

| 命令 | 已记录结果 | 本地日志 |
| --- | --- | --- |
| `cargo test -p multiraft-store --test log_rocks` | 7 passed，0 failed/ignored | `rocks-store-clang.log` |
| `cargo test -p multiraft-net --test owned_rocksdb_guards --test recovery_diagnostics --test rocksdb_async_recovery` | 3 + 6 + 1 passed，0 failed/ignored | `rocks-net-final.log` |
| `cargo test -p multiraft-net --lib log_open_tests -- --test-threads=1` | 3 passed，0 failed/ignored | `rocks-opener-cancel-serial.log` |
| `cargo test -p multiraft-net --test owned_proposal --test owned_runtime` | 5 + 8 passed，0 failed/ignored | `ownproposal-tests-clang.log` |
| `cargo test -p multiraft-net --lib fingerprint` | 10 passed，0 failed/ignored | `fingerprint-grpc-final.log` |
| `cargo test -p multiraft-net --test applied_watermark --test local_observation` | 4 + 2 passed，0 failed/ignored | `watermark-tests-final.log` |
| `cargo test -p multiraft-slot` | 1 unit + 9 manifest + 12 runtime passed，0 failed/ignored；0 doctest | `slot-runtime-review-fixes.log` |
| `cargo test -p multiraft-slot --test manifest` | 9 passed，0 failed/ignored | `manifest-tests-final.log` |
| `python3 scripts/test_slot_acceptance.py` | 最终源码再次 15 项 helper 通过 | `harness-tests-76ce0dd.log` |

storage 检查覆盖耐久写/callback、组命名空间、backend marker、DB 丢失、异步 proof 与清理 owner。串行 opener 测试之前有一次依赖调度的 fixture 失败，保留在 `rocks-opener-cancel-drop.log`，不能把该次计为成功。owned proposal 在 receipt 被丢弃/超时后仍持有原生 admission。manifest/runtime 覆盖稳定向量、磁盘绑定、非法 native mode、单次 application forwarding、非 hosted 入口只读 probe failover、写不重试、dispatch 后 Unknown、budget 保留、panic fence 和启动 rollback。watermark 测试在同一锁内配对 value/applied position，包括并发 apply、从未 apply 和 recovery gate 未通过的读。

原生 fingerprint 检查覆盖 legacy/bound 精确兼容矩阵、规范 metadata、所有 server 路径在 group lookup/snapshot admission 前校验，以及 client 响应检查。digest 是兼容域，不是 peer 认证或全部节点 ready 的证书。

## M 吸收与 Jepsen 单元范围

`.tmp/slot-foundation/main-integration.md` 记录 M 的选择性吸收：version-1 `cluster.json`、descriptor-first admin lookup/shared restart、Java 发现和动态 voter IDs。保留 C containment，拒绝 daisy/第二 standby。Bash `GROUPS` 默认值及 Jepsen 相对 cwd restart 均有明确回归。shell syntax 和 `.tmp/slot-foundation/check-main-integration.py` 通过；可执行 fixture 使用假进程，不证明真实集群恢复。最终选择性整合独立审查无遗留问题。

`python3 .tmp/slot-foundation/run-jepsen-unit.py` **通过 3 tests、12 assertions、0 failures/errors、exit 0**。日志为 `jepsen-lein-unit.log`、`lein-provenance.md`。隔离的官方 Leiningen 2.11.2 launcher/jar 与既有 Zulu JDK17 使用临时 cache/profile/target；jar SHA256 为 `7d31ae23ae769e927438b0cd55d15a93e7dabab09fd4fc15877979161e108774`。此结果取代之前 Leiningen unavailable 限制。restart 单元测试 mock shell 执行，未运行 real Jepsen workload、SSH、实验室或 B4。

## 保留的进程收据与失败

| 本地收据目录 | 实际结果 |
| --- | --- |
| `acceptance-402af21c5ca44c42a6141398fb2c4cac` | 整轮 Unavailable 失败。已记录三场景成功：真实 RF3 转发/精确结果/dedup，gate Unknown/budget 保留/冷 Slot 进展，杀 leader/quorum 进展/同数据重启。compaction 观察过早，不能称全部场景成功。cleanup 记录三个 child exit 0。 |
| `acceptance-3f4282a925194765b197b1d09cb99290` | 未完成场景即因 Busy 整轮失败；harness 尚未等待保留 permit 释放。cleanup 记录三个 child exit 0。 |
| `acceptance-1c96b849162244bea350ea3a6d8b9569` | 仅 recovery 收据通过：耐久 snapshot/purge/suffix/全节点重启/dedup 恢复，以及 manifest 错配/原目录重开。不属于 normal/all 整轮收据。 |
| `acceptance-e32ccb53f27348a3b2924a2b5d1caf8f` | 仅 fingerprint RF3 收据通过：A+A quorum 可提交，A+B 不能 Applied，B 无 A 的值，恢复 A quorum 后显式相同 ID 重试消歧。不属于全部场景收据。 |

各目录保留 `receipt.json`、`requests.jsonl`、layout、child 日志和数据。修正后的 harness 轮询耐久 snapshot/purge 完成及 budget 释放；新增真实 raw HTTP RST 场景，在观察 apply gate entered 后关闭 client socket，不读取响应。该 caller 未收到 Unknown 响应。其目标为证明真实断连后执行 owner 与 dedup；下述最终全部场景收据已通过此场景。

原全量 workspace 在 `workspace-tests.log` 的 `twenty_group_burst_reports_source_coverage_and_releases_all_resources` 五秒等 leader 失败，两次定向复现通过。独立 seam 测试复现测试 startup helper 串行/整轮 probe 阻塞，修正为同一绝对五秒内每 node 独立 retry。`fixture-probe-red.log`、`fixture-probe-retry-red.log` 保留 red；最终 green seam 2 项通过（`fixture-probe-retry-green.log`），election consumer 7 项、twenty-group consumer 1 项通过。这证明 helper 缺陷与修复，不证明它就是原全量失败根因。

随后 e554 全量 workspace 也失败，见下表。写已成功；之后单次线性化读按 voter hint 选目标，原生 ReadIndex 返回分类 NotLeader 拒绝。这不是首写失败证据，也不足以证明生产缺陷或启动 readiness 归因；有界诊断及最终验证见下文。

原 binary 定向 `slot-route-original-binary.log` 则在 runtime.rs:337 首写失败，与 workspace 第 340 行写后读失败分列。六次临时诊断为四次通过、两次失败；一次先写成功（term 1/index 2），随后 provider 1 hint 2/target 2、read NotLeader(None)，调用后 status 为 node 2 Follower/None、node 3 Candidate/None，均 term 1；另一次首写拒绝，hint/target 同为 2。未观察到 target 偏离 hint 或持续 leader 拒绝；事后 status 不能证明精确根因。最终仅测试调整通过有界只读 probe 建立启动成功路径前提，保持写单次，写后允许同一绝对五秒预算内显式只读 NotLeader 重试；每个成功读值立即断言。生产 routing 不变，不声称修复根因；fixture 独立审查无问题。

48af836 串行 workspace 失败属于另一项测试观察：owned_startup_source_uncertainty.rs:91 用外层一秒等待本地 status Closed 超时；之后 startup_support.rs:65 lease 丢失错误为 panic 使临时目录先销毁的次生错误。源码允许本地 probe 通过 accepting 检查后，排在 cleanup writer 后、被 startup 持有的 admission reader 阻塞；日志未证明实际发生此精确交错。此 status probe 不是 Raft RPC。最终仅测试 expired-deadline status probe 只把 Closed 视为 fence，Deadline { phase: Admission, outcome_unknown: false } 表示尚未 fence；不进入 admission 锁，保留原一秒界限与 held/Unknown/lease/reuse 断言。观察失败时保留目录、释放 gate，并以共享额外五秒预算 join 后再报告，保留真实清理错误。`fixture-fence-uncertainty-tests.log` 4 项通过、Clippy 通过，独立审查无问题。临时强制失败 injection 按预期 exit 101，startup Unknown/Released、shutdown Ok、无次生 panic（`fixture-fence-uncertainty-forced-cleanup.log`）；数据保留在 `.tmp/slot-foundation/tmp/.tmpqH2XAe`。injection 撤回后再次 4 项通过，不称生产 deadlock 修复。

六次有界 observer 诊断均通过，未复现 wake：initial 均 Leader/term 1、effective/committed membership 已初始化相等、vote 已 committed，wake 为 None。不能据此声称 data-only 首提交改变 membership、生产通知泄漏或已修复根因。后续仅增加 initial/wake 及失败分支观察后三节点 raw metrics 诊断，原 None 断言、三节点和预算不变，不 ignore/retry，诊断变更独立审查无问题。即使之后全量验证通过，8d2d003 observer 历史失败仍为未定位风险。

中间失败运行继续保留：`workspace-tests-final.log`（e554、exit 101，Slot 写成功后线性化读 NotLeader，其余 11 项 runtime 通过）；`workspace-tests-verified.log` / `verification-48af836.json`（48af836、exit 101，startup uncertainty target 3 passed/1 failed，fence 观察超时）；`workspace-tests-8d2d003.log` / `verification-8d2d003.json`（8d2d003、exit 101，observer 断言失败，net lib 54 passed/1 failed）。此前 format/Clippy 成功不能替代最终运行，未覆盖任何失败日志。

## 最终整合验证

76ce0dd 的七项串行 pipeline 步骤全部 exit 0。`verification-76ce0dd.json`、`root-final-verification-observer-evidence.log` 和各日志绑定命令/退出状态；`workspace-counts-76ce0dd.json` 保存解析计数。

| 命令 | 实际结果 / 日志 |
| --- | --- |
| `cargo fmt --all --check` | 通过；`fmt-76ce0dd.log` |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | 通过；`clippy-76ce0dd.log` |
| `cargo test --workspace --locked -- --test-threads=1` | 92 个成功结果块，430 passed、0 failed、12 ignored；`workspace-tests-76ce0dd.log`。这是执行计数，不是不同测试定义数。 |
| `cargo build -p multiraft-demo -p multiraft-slot-demo --locked` | 通过；`build-76ce0dd.log` |
| `python3 scripts/test_slot_acceptance.py` | 15 项通过；`harness-tests-76ce0dd.log` |
| `python3 .tmp/slot-foundation/run-final-slot-acceptance.py` | 通过；绑定源码后调用 public runner `--case all`，7 场景通过；`slot-all-76ce0dd.log` |
| `python3 .tmp/slot-foundation/run-final-legacy-acceptance.py` | 通过；调用 `./scripts/acceptance.sh`，ACCEPTANCE OK、exit 0；`legacy-76ce0dd.log`、`root-legacy-acceptance.log`、`root-legacy-binding.json` |

12 个 ignored 包含由父测试保护调用的 child fixture、手动 microbenchmark 和专用实验室 recovery/ENOSPC 检查。父测试调用 child 不额外计算一次独立通过；本次没有执行专用 G1 实验室场景。Jepsen 仍仅为上述独立 3 tests/12 assertions 单元结果，不是真实 Jepsen workload。

新 Slot 收据为 `.tmp/slot-foundation/root-final-acceptance-364b75700b804bd7be0e6a548a8c4772/receipt.json`，同目录有 `source-binding.json`、`requests.jsonl`。运行时间为 10:02:54–10:03:04 UTC，真实三进程 RF3、四个 Slot/Group 100–103、RocksDB/NativeDurable、新 node-bound 数据。七个通过场景为：

1. 任意节点转发、精确 effects、相同 ID/改变 intent 的 dedup 行为。
2. apply gate Unknown、Slot budget 保留、即时 Busy、冷 Slot 进展，无自动重提交。
3. gate entered 后真实 public HTTP RST，client 未收到响应；工作保留，显式相同 ID 重试返回原结果，无第二次增量。
4. 杀 leader、存活 quorum 进展、同数据重启。
5. 各 node/Slot 耐久 snapshot/purge 完成、committed suffix、全进程重启并恢复原结果。
6. manifest 改变拒绝、原目录重开。
7. 原生 fingerprint A+A quorum、A+B 不能 Applied、隔离 B 无 A 值，恢复 A quorum 后显式相同 ID 重试消歧。

实跑限额为每 Slot 1 请求、全局 16，每 Slot 16 MiB/全局 128 MiB；command/query/effects 各 128 KiB。测试请求数限额与库默认值不同。完成后 13 次进程启动记录的 PID 全部不存在，九个 loopback 端口均可重新 bind/listen，最终 cleanup 三个 child exit 0；数据、日志和此前失败运行均保留。

`source-binding.json` 核对可执行源码路径与 commit 76ce0dd 一致，并记录默认 workspace features/debug profile、工具链、完整命令、layout、PID/endpoint 和 cleanup。binary SHA256 为 `20a87016c54ff003c6d36263c774440a87b9a9e05b239d912d3948dd4d609211`，harness SHA256 为 `6109063502d58abb11e19a85332ce024ed1f7bcee324ca990a6d18ea4534b825`，Cargo.lock SHA256 为 `17bed111dbc0713fe42dffa336b4b387561ca68963940c232b9b43d4fb1680ba`，另含 receipt/request/layout hashes。

legacy 回归使用 `GROUPS=4 NODES=3 BASE_PORT=30341` 和新数据 `.tmp/slot-foundation/root-legacy-13a40c32d5c441dbbb52c5d99ef71b9a`，六项检查及 `not_leader`（1 test）、`grpc_cluster`（2 tests）均于 10:03:38 UTC 前通过；临时 target symlink 在运行后精确移除。三个记录 PID 全部不存在，六端口均可重新 bind/listen；此项独立验证保留的旧 demo/脚本路径，不替代新 Slot 收据。

各包、fixture 和文档初稿对所审变更未报告问题。上文保留历史失败，包含未定位 observer 间歇性观察。仓库全量 trace-policy 扫描仍因既有内容失败，`.tmp/slot-foundation/attribution-scan-limit.md` 记录基线对照；没有将其报为通过，也没有扩大修改历史文件。

## 剩余生产门槛

上述未定位 observer 间歇性失败是生产接入前明确可靠性事项；之后一次全量通过不认证稳定性，也不关闭该历史风险。

1. 注入可证明局限于单 Slot 的故障，验证健康 Slot 持续服务，quarantine/recovery 不能放行坏 Slot。共享 DB 或 manifest 故障仍为 node 故障；现有 startup/create/proof 失败继承 local-node rollback，runtime panic fence 也为 node 级。
2. 设计并测试确定性 Admit：原子永久成功结果 dedup、业务 seq、accepted head、pending、flush/stop 和 exact holder/revision。注入取消/Unknown，证明无重复 seq；apply 不执行外部 I/O。
3. 设计 fenced 迁移：停止旧 admission/collector，排空，导入全量 accepted/pending/results/flush 及匹配 persisted progress，验证后切换，并定义中断/回滚过程。一次读取滞后 TiKV head 不足以初始化。
4. 将 TiKV 与 delivery 接为接纳后并行路径，保留跨 Channel 事务及 Unknown/fail-stop。SENDACK 仍仅表示 committed/applied 接纳，不表示 TiKV 或 RECV。
5. 性能结论前先固定终态分母与观测方式，验证 issuance/correctness，再以相同耐久/负载进行至少两轮 baseline/candidate 交替对比。报告 offered/issued/completed/failed/unissued、成功延迟 histogram 与资源。固定映射不提供在线迁移，也不能假定多 Slot 加速单热 key。
