# 异步恢复校验验证记录

范围：MultiRaft #4 / Ech0 #245 T03 的通用库恢复、native 同伴安装及资源边界。基线提交 `ff52fc841b093d38e7961ed20e8b604ac186c626`；实现位于携带本记录的提交。OpenRaft 仍精确固定为 `=0.10.0-alpha.30`、revision `ea46d0e571f7497f1f549e125254dfcb2ff45e2e`，workspace Cargo 文件未变化。

## 最终源码检查

- `cargo fmt --all --check`：通过。
- `cargo clippy --workspace --all-targets`：通过。
- `RUST_TEST_THREADS=1 cargo test --workspace`：通过；361 项通过、零失败、12 项声明 ignored、82 个结果块。
- `TMPDIR="$PWD/.tmp/issue-4" ACCEPTANCE_DATA="$PWD/.tmp/issue-4/acceptance-final-data" BASE_PORT=32450 ./scripts/acceptance.sh`：最终源码通过，输出 `ACCEPTANCE OK`。使用三个真实 OS 进程，验证领导者停机、已提交值保留、重启 voter 达到逐 Group floor，以及 gRPC 检查。

install/purge/catalog/orphan 的 ignored 子进程函数由启用的父场景实际执行。专用 G1 ENOSPC/RF3 场景和手动 microbench 未执行。本记录不声明 TiKV 事务验证、断电认证、容量资格或远端部署；本库票远端功能验证时间为零。

## 稳定接口证据

`async_validation` 六项测试覆盖候选 read/capture 隔离、apply/install 串行、代次区分、拒绝/panic/deadline/调用者取消/owner 关闭、真实临时文件回收、旧 provider 保留、重复失败与重启，以及无外部证明时跳过准入。

`async_recovery_validation` 四项测试校验已签入 `legacy-native-alpha30` fixture 的文件 hash 和生产者 revision；启动证明看到 snapshot＋committed suffix 后的实际值 15。拒绝、超时和关闭不执行本地 readiness hook。真实独立进程通过公开 catalog API 留下未激活候选；legacy owned startup 在后缀证明前清理该候选。

catalog/staging 共 22 项测试及七项 provenance 测试覆盖同内容 owner、16-slot 部分构造/完整候选、暂停候选 IO 时仍读取旧 active、cleanup debt 和 64 项启动预算。原有 `owned_maintenance_sampler` 保持原测试通过。原有 post-purge/crash 测试在 `staged`、`application_restored`、`activated`、`bridge_updated` 的安装切点通过；active authority 缺失时保留原代文件，支持精确原字节修复。

全部场景通过公开 FSM/store/owner/catalog 或文件接口观察；崩溃 fixture 来自已签入生产者或真实原生写入，不人工补写已 ACK 的业务 SEND。

早期检查发现并修复了候选轮询的新 unavailable 响应、stage IO 阻塞发布锁、继承的安装诊断切点丢失，以及无 active authority 时过早清理原恢复依据。200 ms 回调进入测试在并发冷运行中超时，其精确重跑和最终 serial workspace 均通过。一次中间构建跨越并发编辑，不作为验证记录。上述最终源码检查替代这些中间结果。

日志和源码 SHA-256 保存在 `.tmp/issue-4/`。接口合同见 [async-recovery-validation.zh-CN.md](async-recovery-validation.zh-CN.md)。

## 改动 Rust 文件行数

最大生产文件 602 行；全部生产文件低于 800 行，测试文件低于 1500 行。

| 文件 | 行数 |
| --- | ---: |
| `crates/multiraft-fsm/src/lib.rs` | 93 |
| `crates/multiraft-net/src/fsm_factory.rs` | 85 |
| `crates/multiraft-net/src/multiraft/application.rs` | 308 |
| `crates/multiraft-net/src/multiraft/group_start.rs` | 480 |
| `crates/multiraft-net/src/multiraft/lifecycle.rs` | 124 |
| `crates/multiraft-net/src/multiraft/recovery.rs` | 172 |
| `crates/multiraft-net/src/multiraft/snapshot_runtime.rs` | 92 |
| `crates/multiraft-net/src/runtime/owner.rs` | 290 |
| `crates/multiraft-net/src/runtime/recovery.rs` | 86 |
| `crates/multiraft-net/src/runtime/startup/operation.rs` | 241 |
| `crates/multiraft-net/tests/async_recovery_validation.rs` | 398 |
| `crates/multiraft-net/tests/native_service_cancellation/mod.rs` | 144 |
| `crates/multiraft-store/src/lib.rs` | 44 |
| `crates/multiraft-store/src/sm_bridge.rs` | 493 |
| `crates/multiraft-store/src/sm_bridge/native.rs` | 404 |
| `crates/multiraft-store/src/sm_bridge/native/install.rs` | 136 |
| `crates/multiraft-store/src/sm_bridge/release.rs` | 92 |
| `crates/multiraft-store/src/sm_bridge/validation.rs` | 304 |
| `crates/multiraft-store/src/snapshot_catalog.rs` | 248 |
| `crates/multiraft-store/src/snapshot_catalog/native.rs` | 602 |
| `crates/multiraft-store/src/snapshot_catalog/native/lifecycle.rs` | 249 |
| `crates/multiraft-store/tests/async_validation.rs` | 533 |
| `crates/multiraft-store/tests/native_snapshot_catalog.rs` | 566 |
| `crates/multiraft-store/tests/native_snapshot_staging.rs` | 128 |
| `crates/multiraft-store/tests/startup_provenance.rs` | 136 |
