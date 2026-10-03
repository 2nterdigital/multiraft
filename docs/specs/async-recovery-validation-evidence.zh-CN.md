# 异步恢复校验验证记录

范围：MultiRaft #4 / Ech0 #245 T03 的通用库恢复、native 同伴安装及资源边界。基线提交 `ff52fc841b093d38e7961ed20e8b604ac186c626`；readiness 发布顺序修正的 review 基线为 `8999092938f3718f893b78ab55b74679bc7b7efb`；peer ingress 修正基线为 `6aa210b28dc25181d0d9f52d7bc340a065b09e9f`。最终实际测试的实现提交为 `95c6e6cad23dd0d8375d282907d61ada04dfcaaa`。OpenRaft 仍精确固定为 `=0.10.0-alpha.30`、revision `ea46d0e571f7497f1f549e125254dfcb2ff45e2e`，workspace Cargo 文件未变化。

## 最终源码检查

- `cargo fmt --all --check`：通过。
- `cargo check --workspace --all-targets`：通过。
- `cargo clippy --workspace --all-targets`：通过。
- `RUST_TEST_THREADS=1 cargo test --workspace`：通过；366 项通过、零失败、12 项声明 ignored、82 个结果块。
- `TMPDIR="$PWD/.tmp/issue-4-review" ACCEPTANCE_DATA="$PWD/.tmp/issue-4-review/ingress-acceptance-final-data" BASE_PORT=33450 ./scripts/acceptance.sh`：最终源码通过，输出 `ACCEPTANCE OK`。使用三个真实 OS 进程，验证领导者停机、已提交值保留、重启 voter 达到逐 Group floor，以及 gRPC 检查。

install/purge/catalog/orphan 的 ignored 子进程函数由启用的父场景实际执行。专用 G1 ENOSPC/RF3 场景和手动 microbench 未执行。本记录不声明 TiKV 事务验证、断电认证、容量资格或远端部署；本库票远端功能验证时间为零。

全部最终检查使用 `CARGO_TARGET_DIR="$PWD/.tmp/issue-4-review/target"` 与 `TMPDIR="$PWD/.tmp/issue-4-review"`。未修改的 acceptance 脚本通过本地 ignored `target` symlink 使用此缓存。整轮最终检查前后 Rust/Cargo SHA-256 完全一致，也与实现提交 `95c6e6cad23dd0d8375d282907d61ada04dfcaaa` 匹配；后续仅验证记录的提交保持该测试源码。第一轮 readiness 顺序检查的 364 项通过记录仍保留在此前提交与原日志。日志、fingerprint、临时文件 lease 与 acceptance data 均在当前 checkout；没有使用远端实验室。

## readiness 发布顺序修正

此前实现的 361-test workspace 与 acceptance 记录通过，但未覆盖 readiness hook 拒绝：native activation（以及 legacy catalog write）发生在可拒绝的 hook 之前，安装虽返回失败，durable 恢复依据却已替换。现在 native 与 legacy 安装在 catalog 发布前执行 hook，并重新核对代次与关闭状态；read、apply 与 capture 始终受门控。native blocking publisher 获准执行时再核对一次，以覆盖调度延迟。startup 在 hook 返回后也重新核对；hook 自身关闭 owner 不能发布应用 ready。

启用校验的 pending startup 期间，owned gRPC/in-process ingress 在 native Core/SM 派发前拒绝新 peer 候选。匹配既有 native active 的加载及 committed suffix 回放继续执行。默认 consumer 行为不变。模块 owner 与依赖方向不变：FSM 声明 consumer 接缝；store validation 拥有准入、代次与门控；native install 调用 catalog 拥有的 durable 发布与 stage 清理；net runtime 在准入 Group 前调用 store 校验。

额外执行 `./scripts/check-no-ai-traces.sh`，在 `CONTRIBUTING.md`、`CONTRIBUTING.zh-CN.md` 与 `docs/superpowers/plans/2026-08-28-node-rpc-transport-foundation.md` 报告三项继承匹配。这三个文件与 review 基线 `8999092938f3718f893b78ab55b74679bc7b7efb` 字节完全一致。独立基线失败保留在 `no-traces-final.log` 与最终重跑 `ingress-no-traces-final.log`，不替代上述已通过的必要 Rust/acceptance 检查。

## peer ingress 修正

第一轮 readiness 顺序修正仍把底层 SM `WouldBlock` 当作可重试结果。精确固定的 OpenRaft worker 将所有 SM install error 转为 fatal storage failure，可能停止实际 Group。现在 owned gRPC ingress 返回 `Unavailable`，in-process dispatcher 返回映射为 `Unreachable` 的 transport 拒绝。两道门都在 Core/SM 派发前执行，立即返回，不等待 startup transition，也不 staging/restore 候选。底层 guard 对 ingress 绕过报告 `InvalidInput`；文档明确原生 install API 会把它转为 fatal，而不是重试协议。

两个启用的真实 peer 回归场景分别运行 gRPC 与 in-process。明确保持 `RECOVERING` 的 native Group 在重复拒绝后保留 applied/membership、authority 与 running state，完成原 snapshot＋suffix 校验，再接纳重试候选。实际 NodeOwner start 在 active proof 期间重复拒绝，之后发布可读 Group，再门控并完成 peer install。后者核验独立代次、候选 `(50, 9)`、peer proof 期间 applied/provider 不变、最终值 99，以及真实 owner/application 回收。既有拒绝、deadline、调用者取消、owner close 测试继续通过。

负对照临时移除 ingress checks，使 `recovering_native_group_refuses_peers_without_invoking_fatal_sm_guard` 的原生 `running_state.is_ok()` 断言失败。预期失败保存在 `ingress-negative-control.log`；最终定向与 workspace 检查前恢复原源码。该结果直接区分实际 native 失败与只直调 Store 的测试。

## 稳定接口证据

`async_validation` 九项测试覆盖候选 read/capture 隔离、apply/install 串行、代次区分、拒绝/panic/deadline/调用者取消/owner 关闭、真实临时文件回收、旧 provider 保留、重复失败与重启，以及无外部证明时跳过准入。新增稳定场景覆盖 native/legacy readiness hook 拒绝并保留旧 provider bytes、applied/membership；直接 Store startup 绕过拒绝后成功验证原 snapshot＋suffix（不声称 native 可重试）；peer/startup readiness hook 自身关闭 owner，不能发布 authority/readiness。

`async_recovery_validation` 六项启用测试校验已签入 `legacy-native-alpha30` fixture 的文件 hash 和生产者 revision；启动证明看到 snapshot＋committed suffix 后的实际值 15。外部证明拒绝、超时和关闭不执行本地 readiness hook。readiness hook 拒绝只发生在外部证明及本地 factory check 成功后；authority 保持、启动返回失败，应用与 listener lease 实际释放。真实独立进程通过公开 catalog API 留下未激活候选；legacy owned startup 在后缀证明前清理该候选。

catalog/staging 共 22 项测试及七项 provenance 测试覆盖同内容 owner、16-slot 部分构造/完整候选、暂停候选 IO 时仍读取旧 active、cleanup debt 和 64 项启动预算。原有 `owned_maintenance_sampler` 保持原测试通过。原有 post-purge/crash 测试在 `staged`、`application_restored`、`activated`、`bridge_updated` 的安装切点通过；active authority 缺失时保留原代文件，支持精确原字节修复。

全部场景通过公开 FSM/store/owner/catalog 或文件接口观察；崩溃 fixture 来自已签入生产者或真实原生写入，不人工补写已 ACK 的业务 SEND。

早期检查发现并修复了候选轮询的新 unavailable 响应、stage IO 阻塞发布锁、继承的安装诊断切点丢失，以及无 active authority 时过早清理原恢复依据。200 ms 回调进入测试在并发冷运行中超时，其精确重跑和最终 serial workspace 均通过。一次中间构建跨越并发编辑，不作为验证记录。上述最终源码检查替代这些中间结果。本轮第一次定向测试将 native 的 builder 拒绝形式用于 legacy，随后修正为验证返回的 legacy builder 执行失败；最终九场景定向测试与完整 workspace 通过。这是测试预期修正，未改变生产 capture 行为。

原始验证记录仍在其原 checkout 的 `.tmp/issue-4/`。readiness 顺序修正日志仍在当前 `.tmp/issue-4-review/` 保留原名。最终 peer ingress 日志为 `ingress-store-focused-final.log`、`ingress-net-focused-final.log`、`ingress-fmt-final.log`、`ingress-check-final.log`、`ingress-clippy-final.log`、`ingress-workspace-tests-final.log`、`ingress-acceptance-final.log`、`ingress-no-traces-final.log`、`ingress-negative-control.log`、`ingress-frozen-source.json`。接口合同见 [async-recovery-validation.zh-CN.md](async-recovery-validation.zh-CN.md)。

## 改动 Rust 文件行数

最大生产文件 602 行；全部生产文件低于 800 行，测试文件低于 1500 行。

| 文件 | 行数 |
| --- | ---: |
| `crates/multiraft-fsm/src/lib.rs` | 94 |
| `crates/multiraft-net/src/fsm_factory.rs` | 85 |
| `crates/multiraft-net/src/grpc/server.rs` | 175 |
| `crates/multiraft-net/src/multiraft/application.rs` | 308 |
| `crates/multiraft-net/src/multiraft/group_start.rs` | 480 |
| `crates/multiraft-net/src/multiraft/lifecycle.rs` | 124 |
| `crates/multiraft-net/src/multiraft/recovery.rs` | 172 |
| `crates/multiraft-net/src/multiraft/snapshot_runtime.rs` | 92 |
| `crates/multiraft-net/src/node.rs` | 247 |
| `crates/multiraft-net/src/router.rs` | 261 |
| `crates/multiraft-net/src/runtime/owner.rs` | 290 |
| `crates/multiraft-net/src/runtime/recovery.rs` | 86 |
| `crates/multiraft-net/src/runtime/startup/operation.rs` | 241 |
| `crates/multiraft-net/tests/async_recovery_validation.rs` | 761 |
| `crates/multiraft-net/tests/native_service_cancellation/mod.rs` | 144 |
| `crates/multiraft-store/src/lib.rs` | 44 |
| `crates/multiraft-store/src/sm_bridge.rs` | 493 |
| `crates/multiraft-store/src/sm_bridge/native.rs` | 404 |
| `crates/multiraft-store/src/sm_bridge/native/install.rs` | 152 |
| `crates/multiraft-store/src/sm_bridge/release.rs` | 92 |
| `crates/multiraft-store/src/sm_bridge/validation.rs` | 310 |
| `crates/multiraft-store/src/snapshot_catalog.rs` | 248 |
| `crates/multiraft-store/src/snapshot_catalog/native.rs` | 602 |
| `crates/multiraft-store/src/snapshot_catalog/native/lifecycle.rs` | 249 |
| `crates/multiraft-store/tests/async_validation.rs` | 796 |
| `crates/multiraft-store/tests/native_snapshot_catalog.rs` | 566 |
| `crates/multiraft-store/tests/native_snapshot_staging.rs` | 128 |
| `crates/multiraft-store/tests/startup_provenance.rs` | 136 |
